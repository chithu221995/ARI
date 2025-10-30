from __future__ import annotations

import logging
import httpx
from typing import List, Dict, Any

from app.core import settings

log = logging.getLogger("ari.ingest.google_rss_scrapingdog")

async def search_google_news_scrapingdog(
    query: str,
    aliases: list[str],
    topk: int = 10,
    country: str = "us",
    timeout_s: int = 8,
) -> List[Dict[str, Any]]:
    """
    Adapter for ScrapingDog Google News.
    Args:
      query: primary search term
      aliases: optional list of alias terms to include
      topk: maximum results to return (and passed to the API as `results`)
      country: country code (default "in")
      timeout_s: HTTP client timeout in seconds
    Returns list of dicts with keys: title, url, source, published_hint, snippet
    """
    q_terms = [query] + [a for a in (aliases or []) if a]
    # quote multi-word terms for safer queries
    q = " OR ".join([f'"{t}"' if " " in t else t for t in q_terms]) if q_terms else query

    params = {
        "api_key": getattr(settings, "SCRAPINGDOG_API_KEY", "") or "",
        "query": q,
        "results": int(topk or 0),
        "country": country,
    }

    url = "https://api.scrapingdog.com/google_news"
    out: list[dict] = []

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json() or {}
            items = data if isinstance(data, list) else data.get("news_results") or []
            for it in items:
                obj = {
                    "title": it.get("title") or "",
                    "url": it.get("url") or "",
                    "source": it.get("source") or "",
                    "published_hint": it.get("lastUpdated") or it.get("publishedAt") or it.get("published_at") or "",
                    "snippet": it.get("snippet") or "",
                }
                out.append(obj)
                if len(out) >= int(topk or 0):
                    break
    except Exception:
        log.exception("scrapingdog: fetch failed for query=%s", q)

    log.info("scrapingdog: query=%s requested=%d kept=%d", q, int(topk or 0), len(out))
    return out