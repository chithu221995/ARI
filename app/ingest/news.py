# app/ingest/news.py
from __future__ import annotations
import os
import logging
import re
from typing import List, Dict, Any
from datetime import datetime, timedelta

import httpx

log = logging.getLogger("ari.ingest")

# optional tldextract for nicer domain extraction; fallback to urllib.parse
try:
    import tldextract  # type: ignore
except Exception:
    tldextract = None
from urllib.parse import urlparse
from datetime import datetime

def get_source_domain(url: str) -> str:
    if not url:
        return ""
    try:
        if tldextract:
            ext = tldextract.extract(url)
            return ".".join([p for p in [ext.domain, ext.suffix] if p]).lower()
    except Exception:
        pass
    netloc = urlparse(url).netloc.lower()
    return netloc[4:] if netloc.startswith("www.") else netloc

# helpers used to pick best items for LLM summarization
def _is_english(item: dict) -> bool:
    lang = (item.get("lang") or "").lower()
    return lang in ("", "en")  # empty = unknown -> allow

def _has_content(item: dict) -> bool:
    return bool((item.get("translated_text") or "") or (item.get("content") or ""))

def _parse_dt(s: str) -> datetime:
    try:
        # handle ISO like '2025-10-16T14:26:23Z' and naive ISO strings
        return datetime.fromisoformat(s.replace("Z", ""))  # may raise; fallthrough handled
    except Exception:
        return datetime.min

def select_top_news_for_summary(items: list[dict], max_items: int = 5) -> list[dict]:
    """
    Return up to max_items, English-only, with content, sorted by score desc then recency.
    Dedupes by URL.
    """
    keep: list[dict] = []
    seen: set[str] = set()
    for it in items or []:
        k = (it.get("url") or "").strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        if not _is_english(it):
            continue
        if not _has_content(it):
            continue
        keep.append(it)

    # sort primary by _score (descending) then by published_at (descending)
    def _sort_key(x: dict):
        score = float(x.get("_score") or 0.0)
        dt = _parse_dt(x.get("published_at") or "")
        return (score, dt)

    keep.sort(key=_sort_key, reverse=True)
    return keep[:max_items]


NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
NEWS_API_URL = os.getenv("NEWS_API_URL", "https://newsapi.org/v2/everything")


# scoring helper
def _score_news_item(ticker: str, item: Dict[str, Any], aliases: List[str]) -> float:
    title = (item.get("title") or "").lower()
    source = (item.get("source") or "").lower()
    url = item.get("url") or ""
    try:
        host = get_source_domain(url)
    except Exception:
        host = ""
    s = 0.0
    keys = [ticker] + [a.lower() for a in aliases if a]
    for key in keys:
        if key and re.search(rf"\b{re.escape(key)}\b", title):
            s += 2.5
    if host in {
        "reuters.com",
        "bloomberg.com",
        "business-standard.com",
        "economictimes.indiatimes.com",
        "moneycontrol.com",
        "financialexpress.com",
        "livemint.com",
        "thehindubusinessline.com",
        "financialpost.com",
    }:
        s += 1.2
    for key in keys:
        if key and key in source:
            s += 0.6
    if any(x in host for x in {"billboard.com", "pitchfork.com", "rollingstone.com"}):
        s -= 2.0
    return s


# safe resolver fallback
try:
    from app.core.lookup import resolve
except Exception:
    try:
        from app.data.tickers import resolve
    except Exception:
        def resolve(t: str) -> Dict[str, Any]:
            return {"company_name": "", "aliases": []}


async def fetch_news_for_ticker(
    ticker: str,
    *,
    only_en: bool = True,
    max_items: int = 5,
    from_days: int = 7
) -> List[Dict]:
    """
    Async fetch news for a ticker. Returns a list of dicts with keys:
      title, url, published_at, source, (optional) lang, (optional) content_ok

    Parameters:
      ticker: stock ticker or search term
      only_en: apply English-only gate (lang field or fallback detect_language)
      max_items: cap of returned articles
      from_days: how many days back to fetch
    """
    if not ticker:
        return []

    now = datetime.utcnow()
    from_dt = now - timedelta(days=from_days)
    params = {
        "q": f"{ticker} OR \"{ticker} stock\" OR \"{ticker} shares\"",
        "from": from_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pageSize": 20,
        "sortBy": "publishedAt",
        # bias to English at request-level
        "language": "en",
    }

    headers = {}
    if NEWS_API_KEY:
        params["apiKey"] = NEWS_API_KEY

    log.info("ingest.fetch_news: ticker=%s only_en=%s max_items=%s from_days=%s - starting request", ticker, only_en, max_items, from_days)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(NEWS_API_URL, params=params, headers=headers)
            try:
                data = r.json()
            except Exception:
                log.exception("ingest.fetch_news: invalid json response for ticker=%s status=%s", ticker, r.status_code)
                data = {}
    except Exception:
        log.exception("ingest.fetch_news: request failed for ticker=%s", ticker)
        return []

    articles = data.get("articles") or []
    log.info("ingest.fetch_news: api returned=%d articles for ticker=%s", len(articles), ticker)

    # preview first 6 titles + host for quick inspection
    try:
        preview = [(((a.get("title") or "")[:80]), get_source_domain(a.get("url", "") or "")) for a in articles[:6]]
        log.info("ingest.fetch_news: preview=%s", preview)
    except Exception:
        log.exception("ingest.fetch_news: failed to build preview")

    parsed: List[Dict[str, Any]] = []
    for a in articles:
        if len(parsed) >= 20:  # limit parsing to pageSize
            break
        lang = (a.get("language") or a.get("lang") or "").lower()
        title = (a.get("title") or "") or ""
        if only_en:
            if lang and lang != "en":
                continue
            if not lang:
                try:
                    from app.utils.lang import detect_language
                    if detect_language(title) != "en":
                        continue
                except Exception:
                    continue

        url = a.get("url") or ""
        published_at = a.get("publishedAt") or a.get("published_at") or ""
        source = ""
        try:
            src = a.get("source")
            if isinstance(src, dict):
                source = src.get("name") or ""
            elif isinstance(src, str):
                source = src
        except Exception:
            source = ""

        content = (a.get("content") or "") or ""
        content_ok = bool(content and len(content.strip()) > 50)

        item: Dict[str, Any] = {
            "title": title.strip(),
            "url": url,
            "published_at": published_at,
            "source": source,
        }
        if lang:
            item["lang"] = lang
        if content_ok:
            item["content_ok"] = True

        parsed.append(item)

    # scoring + ranking + cap to max_items
    out = parsed
    try:
        meta = resolve(ticker) or {}
    except Exception:
        meta = {"company_name": "", "aliases": []}
    aliases = [meta.get("company_name", "")] + meta.get("aliases", []) if isinstance(meta.get("aliases", []), list) else [meta.get("company_name", "")]
    for it in out:
        try:
            it["_score"] = _score_news_item(ticker, it, aliases)
        except Exception:
            it["_score"] = 0.0
    out.sort(key=lambda x: x.get("_score", 0.0), reverse=True)
    out = out[:max_items]

    log.info("ingest.fetch_news: returning=%d items for ticker=%s (only_en=%s)", len(out), ticker, only_en)
    return out


__all__ = [
    "fetch_news_for_ticker",
]