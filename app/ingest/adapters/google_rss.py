from __future__ import annotations
import logging
from typing import List

from app.ingest.adapters.base import NewsItem, normalize_item
from app.ingest.google_rss_scrapingdog import search_google_news_scrapingdog
from app.utils.dates import parse_relative_age_to_hours

log = logging.getLogger("ari.ingest.google_rss")


async def fetch(ticker: str, *, days: int, topk: int, timeout_s: int) -> List[NewsItem]:
    """
    Adapter shim that queries ScrapingDog google_news and returns List[NewsItem].
    Now captures news_age in hours from lastUpdated field.
    """
    log.info("google_rss.fetch: start ticker=%s days=%d top_k=%d", ticker, days, topk)

    try:
        raw = await search_google_news_scrapingdog(
            ticker, aliases=[], topk=topk, country="in", timeout_s=timeout_s
        )
        raw_count = len(raw or [])
        log.info("google_rss.fetch: source_call returned=%d entries for %s", raw_count, ticker)
    except Exception:
        log.exception("google_rss.fetch: error fetching google_rss for %s", ticker)
        return []

    items: List[NewsItem] = []
    for r in (raw or [])[:topk]:
        # Parse news age from lastUpdated field
        last_updated = r.get("published_hint") or ""
        news_age_hours = parse_relative_age_to_hours(last_updated) if last_updated else None

        if news_age_hours is not None:
            log.debug(f"Parsed '{last_updated}' -> {news_age_hours} hours")

        ni = {
            "title": r.get("title", "") or "",
            "url": r.get("url", "") or "",
            "source": (r.get("source") or "").strip(),
            "published_at": last_updated,  # Keep original for reference
            "news_age": news_age_hours,  # NEW: Store parsed age in hours
            "lang": "en",
            "content": r.get("snippet") or "",
        }
        items.append(normalize_item(ni))

    kept = len(items)
    log.info("google_rss.fetch: kept=%d for %s", kept, ticker)

    return items