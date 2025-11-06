from __future__ import annotations

import logging
import httpx
import time
import asyncio
from typing import List, Dict, Any

from app.core import settings
from app.core.metrics import record_vendor_event
from app.core.retry_utils import rate_limited_retry  # ADD THIS

log = logging.getLogger("ari.ingest.google_rss_scrapingdog")

@rate_limited_retry(
    provider="scrapingdog",
    max_retries=2,
    base_delay=2.0,
    max_per_minute=5
)
async def search_google_news_scrapingdog(
    query: str,
    aliases: list[str],
    topk: int = 10,
    country: str = "us",
    timeout_s: int = 15,
    max_retries: int = 1,  # Keep parameter for backward compatibility
) -> List[Dict[str, Any]]:
    """
    Adapter for ScrapingDog Google News with automatic retries and rate limiting.
    
    The @rate_limited_retry decorator handles:
    - Rate limiting (5 calls/minute)
    - Exponential backoff on failures
    - Automatic retries on network errors
    """
    q_terms = [query] + [a for a in (aliases or []) if a]
    q = " OR ".join([f'"{t}"' if " " in t else t for t in q_terms]) if q_terms else query

    params = {
        "api_key": getattr(settings, "SCRAPINGDOG_API_KEY", "") or "",
        "query": q,
        "results": int(topk or 0),
        "country": country,
    }

    url = "https://api.scrapingdog.com/google_news"
    out: list[dict] = []
    start_time = time.time()
    success = False

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            log.info(f"scrapingdog: fetching for query={q}")
            
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
            
            success = True
    
    finally:
        latency_ms = int((time.time() - start_time) * 1000)
        
        record_vendor_event(
            provider="scrapingdog",
            event="fetch",
            ok=success,
            latency_ms=latency_ms
        )
        
        if success:
            log.info(f"scrapingdog: query={q} requested={int(topk or 0)} kept={len(out)} latency={latency_ms}ms")
        else:
            log.warning(f"scrapingdog: query={q} failed, returning empty list")

    return out
