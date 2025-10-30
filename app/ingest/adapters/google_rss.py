from __future__ import annotations
import logging
from typing import List

from app.ingest.adapters.base import NewsItem, normalize_item
from app.ingest.google_rss_scrapingdog import search_google_news_scrapingdog

log = logging.getLogger("ari.ingest.google_rss")


async def fetch(ticker: str, *, days: int, topk: int, timeout_s: int) -> List[NewsItem]:
    """
    Adapter shim that queries ScrapingDog google_news and returns List[NewsItem].
    """
    log.info("google_rss.fetch: start ticker=%s days=%d top_k=%d", ticker, days, topk)
    # try to fetch using scrapingdog helper
    try:
        # aliases not available here; pass empty list (higher-level callers may pre-resolve)
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
        ni = {
            "title": r.get("title", "") or "",
            "url": r.get("url", "") or "",
            "source": (r.get("source") or "").strip(),
            "published_at": r.get("published_hint") or "",
            "lang": "en",
            "content": r.get("snippet") or "",
        }
        items.append(normalize_item(ni))

    kept = len(items)
    dropped = max(0, (len(raw or []) - kept))
    log.info("google_rss.fetch: kept=%d dropped=%d for %s (applied top_k=%d)", kept, dropped, ticker, topk)
    # log one sample item safely truncated
    if items:
        ex = items[0]
        sample_title = (ex.get("title") or "")[:200]
        sample_url = ex.get("url") or ""
        log.info("google_rss.fetch: sample title=%r url=%s", sample_title, sample_url)

    return items