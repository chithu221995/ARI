from __future__ import annotations
import os
import asyncio
from typing import List, Dict

from app.core.cache import cache_upsert_items

async def fetch_news(ticker: str, *, use_mock: bool = False) -> List[Dict]:
    """
    Fetch latest news items for a ticker.
    Currently delegates to app.ingest.news.fetch_news_for_ticker (sync) and wraps as async.
    """
    from app.ingest.news import fetch_news_for_ticker
    items = fetch_news_for_ticker(ticker) or []
    return items

async def get_filings_for(ticker: str) -> list[dict]:
    """
    Filings are disabled for the prototype — return empty list.
    """
    return []

async def prefetch_all(tickers: List[str], *, use_mock: bool = False, use_browser: bool | None = None) -> None:
    for ticker in tickers:
        news_items = await fetch_news(ticker, use_mock=use_mock)
        await cache_upsert_items(news_items, kind="news")

        filing_items = await get_filings_for(ticker, use_mock=use_mock, use_browser=use_browser)
        await cache_upsert_items(filing_items, kind="filings")