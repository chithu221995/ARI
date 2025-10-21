from __future__ import annotations
import os
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import httpx

from app.core import settings
from app.core.cache import url_hash

log = logging.getLogger("ari.news")


def _dequery_url(u: str) -> str:
    try:
        p = urlparse((u or "").strip())
        if not p.scheme:
            return u
        # remove common tracking params
        q = parse_qsl(p.query, keep_blank_values=True)
        filtered = [(k, v) for k, v in q if not k.lower().startswith("utm_") and k.lower() not in ("fbclid", "gclid", "icn")]
        new_q = urlencode(filtered, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path or "", p.params or "", new_q, p.fragment or ""))
    except Exception:
        return u


# Try to use a resolve helper if present; otherwise fallback
try:
    from app.core.lookup import resolve  # type: ignore
except Exception:
    def resolve(ticker: str) -> Dict[str, List[str]]:
        # minimal fallback: return ticker as symbol and no aliases
        return {"name": ticker, "aliases": [], "nse": ticker}


async def fetch_news_for_ticker(ticker: str, max_items: int = 20, days: int = 7) -> List[Dict]:
    """
    Fetch news for ticker using NewsAPI /v2/everything.
    Builds q from resolve(ticker) (name + aliases + nse symbol OR-joined).
    Returns list of dicts with keys: title, url, published_at, source, lang='en'
    """
    key = os.getenv("NEWS_API_KEY", "") or getattr(settings, "NEWS_API_KEY", "")
    if not key:
        log.info("news.fetch_news_for_ticker: NEWS_API_KEY missing, skipping NewsAPI for %s", ticker)
        return []

    info = resolve(ticker) or {}
    parts: List[str] = []
    name = info.get("name") or ""
    if name:
        parts.append(f"\"{name}\"" if " " in name else name)
    for a in info.get("aliases", []) or []:
        if a:
            parts.append(f"\"{a}\"" if " " in a else a)
    nse = info.get("nse") or info.get("symbol") or ""
    if nse:
        parts.append(nse)

    q = " OR ".join(parts) if parts else ticker

    now = datetime.utcnow()
    to_iso = now.replace(microsecond=0).isoformat() + "Z"
    from_iso = (now - timedelta(days=days)).replace(microsecond=0).isoformat() + "Z"

    endpoint = "https://newsapi.org/v2/everything"
    params = {
        "q": q,
        "language": "en",
        "sortBy": "relevancy",
        "from": from_iso,
        "to": to_iso,
        "pageSize": 25,
    }
    headers = {"X-Api-Key": key}

    try:
        async with httpx.AsyncClient(timeout=settings.NEWS_TIMEOUT_S or 10) as client:
            r = await client.get(endpoint, params=params, headers=headers)
            if r.status_code != 200:
                log.warning("news.fetch_news_for_ticker: non-200 for %s status=%d text=%s", ticker, r.status_code, r.text[:200])
                return []
            data = r.json()
    except Exception:
        log.exception("news.fetch_news_for_ticker: request failed for %s", ticker)
        return []

    articles = data.get("articles") or []
    out: List[Dict] = []
    seen_hashes = set()
    for a in articles:
        url = (a.get("url") or "").strip()
        if not url:
            continue
        url_norm = _dequery_url(url)
        h = url_hash(url_norm)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        itm = {
            "title": a.get("title") or "",
            "url": url_norm,
            "published_at": a.get("publishedAt") or a.get("published_at") or "",
            "source": (a.get("source") or {}).get("name") if isinstance(a.get("source"), dict) else a.get("source") or "",
            "lang": "en",
        }
        out.append(itm)
        if len(out) >= max_items:
            break

    return out


async def select_top_news_for_summary(ticker: str, max_items: int = 5, days: int = 7) -> List[Dict]:
    """
    Backwards-compatible helper used by other modules.
    Returns the top news items suitable for summarization (delegates to fetch_news_for_ticker).
    """
    try:
        items = await fetch_news_for_ticker(ticker, max_items=max_items, days=days)
        return items[:max_items]
    except Exception:
        log.exception("select_top_news_for_summary failed for %s", ticker)
        return []

# ensure it's exported if __all__ is used
try:
    __all__.append("select_top_news_for_summary")
except Exception:
    __all__ = ["fetch_news_for_ticker", "select_top_news_for_summary"]