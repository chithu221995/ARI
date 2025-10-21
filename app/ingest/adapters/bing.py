from __future__ import annotations
import logging
from typing import List
import httpx

from app.core import settings
from .base import NewsItem, normalize_item

log = logging.getLogger("ari.news")


async def fetch(ticker: str, *, days: int, topk: int, timeout_s: int) -> List[NewsItem]:
    """
    Bing News Search adapter.
    """
    key = settings.BING_NEWS_KEY
    if not key:
        log.info("bing.fetch: API key missing, skipping")
        return []

    endpoint = "https://api.bing.microsoft.com/v7.0/news/search"
    count = min(topk, 50)
    params = {
        "q": f"{ticker}",
        "mkt": "en-IN",
        "freshness": f"{settings.NEWS_DAYS}d",
        "count": count,
        "sortBy": "Date",
        "safeSearch": "Off",
    }
    headers = {"Ocp-Apim-Subscription-Key": key}

    try:
        async with httpx.AsyncClient(timeout=settings.NEWS_TIMEOUT_S or timeout_s) as client:
            r = await client.get(endpoint, params=params, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception:
        log.exception("bing.fetch: request failed for %s", ticker)
        return []

    vals = data.get("value") or []
    items: List[NewsItem] = []
    for i in vals:
        raw = {
            "title": i.get("name", "") or "",
            "url": i.get("url", "") or "",
            "source": (i.get("provider", [{}])[0].get("name", "") or "").lower(),
            "published_at": i.get("datePublished", "") or "",
            "lang": "en",
            "content": i.get("description", "") or "",
        }
        items.append(normalize_item(raw))

    log.info("bing.fetch: adapter returned=%d for %s", len(items), ticker)
    return items[:topk]