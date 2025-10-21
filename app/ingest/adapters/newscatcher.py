from __future__ import annotations
import logging
from typing import List
from datetime import datetime, timedelta

import httpx

from app.core import settings
from .base import NewsItem, normalize_item

log = logging.getLogger("ari.news")


async def fetch(ticker: str, *, days: int, topk: int, timeout_s: int) -> List[NewsItem]:
    """
    Newscatcher v2 search adapter.
    """
    key = settings.NEWSCATCHER_API_KEY
    if not key:
        log.info("newscatcher.fetch: API key missing, skipping")
        return []

    now = datetime.utcnow()
    from_dt = now - timedelta(days=settings.NEWS_DAYS or days or 7)
    now_iso = now.replace(microsecond=0).isoformat() + "Z"
    from_iso = from_dt.replace(microsecond=0).isoformat() + "Z"

    page_size = min(topk, 50)

    url = "https://api.newscatcherapi.com/v2/search"
    headers = {"x-api-key": key}
    params = {
        "q": ticker,
        "lang": settings.NEWS_LANGUAGE or "en",
        "from": from_iso,
        "to": now_iso,
        "page_size": page_size,
    }

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            r = await client.get(url, params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception:
        log.exception("newscatcher.fetch: request failed for %s", ticker)
        return []

    articles = data.get("articles") or []
    items: List[NewsItem] = []
    for a in articles:
        raw = {
            "title": a.get("title", "") or "",
            "url": a.get("link", "") or a.get("url", "") or "",
            "source": (a.get("clean_url") or a.get("rights") or "") or "",
            "published_at": a.get("published_date") or a.get("published_date_precision") or "",
            "lang": "en",
            "content": a.get("summary") or a.get("excerpt") or a.get("description") or "",
        }
        items.append(normalize_item(raw))

    log.info("newscatcher.fetch: adapter returned=%d for %s", len(items), ticker)
    return items[:topk]