from __future__ import annotations
import os
from typing import List, Dict
import asyncio

# Read flags locally to avoid circular imports
USE_MOCK = os.getenv("USE_MOCK_FILINGS", "false").lower() in {"1", "true", "yes"}
USE_BROWSER = os.getenv("USE_BROWSER_FETCH", "false").lower() in {"1", "true", "yes"}

# Import fetchers according to browser flag
if USE_BROWSER:
    from app.ingest.filings_browser import (
        fetch_bse_announcements_browser as fetch_bse_announcements,
        fetch_nse_announcements_browser as fetch_nse_announcements,
    )
else:
    from app.ingest.filings import fetch_bse_announcements, fetch_nse_announcements

# Import optional helpers from filings (dedupe/filter)
try:
    from app.ingest.filings import filter_for_summary, dedupe_by_url_or_title, generate_mock_filings
except Exception:
    # fallback mock generator if filings module not importable
    def generate_mock_filings(ticker: str) -> list[dict]:
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        return [
            {
                "title": f"{ticker}: Mock announcement",
                "url": "https://example.com/file.pdf",
                "published_at": (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "BSE",
                "category": "ANNOUNCEMENT",
                "summary_allowed": True,
            }
        ]
    # fallback no-op filter/dedupe
    def filter_for_summary(items: List[Dict]) -> List[Dict]:
        return items
    def dedupe_by_url_or_title(items: List[Dict]) -> List[Dict]:
        return items

async def get_filings_for(ticker: str) -> List[Dict]:
    """
    Moved from main.py — returns mock filings if USE_MOCK set,
    otherwise fetches BSE + NSE announcements in parallel, merges,
    applies filter_for_summary + dedupe_by_url_or_title and returns top 5.
    """
    if USE_MOCK:
        return generate_mock_filings(ticker)
    # live path
    bse_task = fetch_bse_announcements(ticker)
    nse_task = fetch_nse_announcements(ticker)
    chunks = await asyncio.gather(bse_task, nse_task, return_exceptions=True)
    filings: List[Dict] = []
    for c in chunks:
        if isinstance(c, list):
            filings.extend(c)
    try:
        filings = dedupe_by_url_or_title(filter_for_summary(filings))
    except Exception:
        pass
    return sorted(filings, key=lambda x: x.get("published_at", ""), reverse=True)[:5]