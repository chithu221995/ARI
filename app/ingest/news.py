# app/ingest/news.py
from __future__ import annotations
import os
import logging
from typing import List, Dict, Any, Optional
import datetime
import httpx
import re

log = logging.getLogger("ari.ingest.news")

# endpoint / constants
NEWS_API_URL = "https://newsapi.org/v2/everything"
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

def build_news_query(ticker: str) -> str:
    """Return a safe query string for newsapi."""
    t = (ticker or "").strip()
    if not t:
        return ""
    return f'{t} OR "{t} stock"'

async def fetch_news_for_ticker(ticker: str, max_items: int = 5, days: int = 7) -> List[Dict[str, Any]]:
    """
    Fetch news for a ticker from NewsAPI.org.
    - uses NEWS_API_URL and NEWS_API_KEY
    - language='en', sortBy='publishedAt', pageSize up to max(5,10)
    - from = days ago (ISO)
    - returns list of dicts: {title, url, published_at, source, content}
    - logs info on start and logs errors on exceptions; never raises NameError for NEWS_API_URL.
    """
    if not ticker:
        return []

    log.info("ingest.news: fetch_news_for_ticker start ticker=%s max_items=%s days=%s", ticker, max_items, days)

    q = build_news_query(ticker)
    now = datetime.datetime.utcnow()
    from_dt = now - datetime.timedelta(days=days)
    from_iso = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    page_size = max(max_items, 10)

    params = {
        "q": q,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "from": from_iso,
    }
    headers = {}
    # prefer header if key present
    if NEWS_API_KEY:
        headers["X-Api-Key"] = NEWS_API_KEY
        params["apiKey"] = NEWS_API_KEY  # fallback; some clients expect param

    out: List[Dict[str, Any]] = []
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(NEWS_API_URL, params=params, headers=headers)
            if r.status_code != 200:
                log.error("ingest.news: newsapi non-200 status=%s text=%s", r.status_code, r.text[:200])
                return []
            data = r.json()
            articles = data.get("articles") or []
            for a in articles:
                if len(out) >= max_items:
                    break
                # prefer explicit publishedAt field
                title = a.get("title") or ""
                url = a.get("url") or ""
                published_at = a.get("publishedAt") or a.get("published_at") or ""
                source = (a.get("source") or {}).get("name") if isinstance(a.get("source"), dict) else a.get("source") or ""
                content = a.get("content") or a.get("description") or ""
                # filter language: NewsAPI should honor language param, but double-check if article carries a language key
                lang = a.get("language") or ""
                if lang and lang.lower() != "en":
                    continue
                out.append({
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "source": source,
                    "content": content,
                })
    except Exception as e:
        log.exception("ingest.news: fetch_news_for_ticker failed for %s: %s", ticker, e)
        return []

    return out

def _parse_published(s: str) -> datetime.datetime:
    if not s:
        return datetime.datetime.fromtimestamp(0)
    try:
        if s.endswith("Z"):
            return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return datetime.datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.datetime.fromtimestamp(0)


def select_top_news_for_summary(items: List[Dict[str, Any]], k: int = 5) -> List[Dict[str, Any]]:
    """
    Return up to k items suitable for summarization:
    - prefer English items (lang == 'en' if present)
    - dedupe by url/title
    - sort by published_at (newest first) when available
    """
    if not items:
        return []

    seen_urls = set()
    seen_titles = set()
    cleaned: List[Dict[str, Any]] = []

    # normalize and filter
    for it in items:
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        lang = (it.get("lang") or "").strip().lower()
        # prefer english if lang present
        if lang and lang != "en":
            continue
        key_url = url.lower()
        key_title = re.sub(r"\s+", " ", title.lower())
        if key_url and key_url in seen_urls:
            continue
        if key_title and key_title in seen_titles:
            continue
        if key_url:
            seen_urls.add(key_url)
        if key_title:
            seen_titles.add(key_title)
        cleaned.append({**it, "published_parsed": _parse_published(it.get("published_at") or it.get("publishedAt") or "")})

    # sort by parsed published date desc, keep original order as fallback
    cleaned.sort(key=lambda x: x.get("published_parsed", datetime.datetime.fromtimestamp(0)), reverse=True)

    # return up to k, strip helper key before returning
    out = []
    for c in cleaned:
        c.pop("published_parsed", None)
        out.append(c)
        if len(out) >= k:
            break
    return out