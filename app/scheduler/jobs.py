from __future__ import annotations
import os
import random
import asyncio
from typing import List, Dict, Any

from app.ingest.news import fetch_news_for_ticker
from app.core.services import get_filings_for  # async

# Optional cache helpers (best-effort; ignore if not present)
try:
    from app.core.cache import cache_upsert_items, cache_get_by_ticker
except Exception:
    cache_upsert_items = None
    cache_get_by_ticker = None

STAGGER_BASE_MS = int(os.getenv("STAGGER_BASE_MS", "250"))
STAGGER_JITTER_MS = int(os.getenv("STAGGER_JITTER_MS", "250"))
PREFETCH_CONCURRENCY = int(os.getenv("PREFETCH_CONCURRENCY", "2"))


def parse_cron(expr: str) -> Dict[str, str]:
    """
    Parse a 5-field cron expression: 'm h dom mon dow'
    Returns dict compatible with CronTrigger kwargs.
    """
    parts = (expr or "").split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression '{expr}'. Expected 5 fields.")
    minute, hour, day, month, day_of_week = parts
    return {
        "minute": minute,
        "hour": hour,
        "day": day,
        "month": month,
        "day_of_week": day_of_week,
    }


async def prefetch_brief_and_cache(app: Any, ticker: str) -> Dict[str, Any]:
    """
    Fetch news/filings for `ticker`, upsert into cache, and return counts.
    Imports are local to avoid circular imports.
    """
    # local imports to avoid circulars
    from app.core.cache import cache_upsert_items
    from app.ingest.news import fetch_news_for_ticker as _fetch_news_sync
    from app.core.services import get_filings_for as _get_filings_async

    # fetch news (may be sync) in thread
    try:
        news = await asyncio.to_thread(_fetch_news_sync, ticker) or []
    except Exception as e:
        print(f"[prefetch] error fetching news for {ticker}: {e}")
        news = []

    # fetch filings (async)
    try:
        filings = await _get_filings_async(ticker)
    except Exception as e:
        print(f"[prefetch] error fetching filings for {ticker}: {e}")
        filings = []

    # upsert into cache if available
    if cache_upsert_items:
        try:
            if news:
                await cache_upsert_items(news, kind="news", ticker=ticker)
            if filings:
                await cache_upsert_items(filings, kind="filings", ticker=ticker)
        except Exception as e:
            print(f"[prefetch] cache upsert warn ({ticker}): {e}")

    return {"ticker": ticker, "news": len(news), "filings": len(filings)}


async def run_daily_prefetch(tickers: List[str]) -> None:
    """
    Prefetch news + filings per ticker and optionally upsert into cache.
    Runs under APScheduler. Uses a bounded concurrency semaphore and staggers starts.
    """
    if not tickers:
        print("[scheduler] no tickers configured; skipping")
        return

    sem = asyncio.Semaphore(PREFETCH_CONCURRENCY)
    tasks: List[asyncio.Task] = []

    async def worker(ticker: str) -> None:
        await sem.acquire()
        try:
            delay_ms = STAGGER_BASE_MS + random.randint(0, STAGGER_JITTER_MS)
            await asyncio.sleep(delay_ms / 1000.0)
            try:
                res = await prefetch_brief_and_cache(None, ticker)
                print(f"[prefetch] t={ticker} delay={delay_ms}ms news={res.get('news',0)} filings={res.get('filings',0)}")
            except Exception as e:
                print(f"[prefetch] t={ticker} failed: {e}")
        finally:
            sem.release()

    print(f"[scheduler] prefetch start for {tickers} (concurrency={PREFETCH_CONCURRENCY})")
    for t in tickers:
        tasks.append(asyncio.create_task(worker(t)))
    # wait for all workers
    await asyncio.gather(*tasks)
    print("[scheduler] prefetch done")


async def run_prefetch(app: Any, tickers: List[str] | None = None) -> Dict[str, Dict[str, Any]]:
    """
    Manual/HTTP callable prefetch helper that reuses the scheduled job logic.
    - app: FastAPI app instance (unused but kept for handler signature)
    - tickers: optional list of tickers; if None uses env SCHEDULE_TICKERS

    Returns a dict mapping ticker -> summary info:
      {"TCS": {"ticker":"TCS","news":N,"filings":M,"cached":True}, ...}
    """
    if tickers is None:
        tickers_env = os.getenv("SCHEDULE_TICKERS", "TCS,TATAMOTORS,HEROMOTOCO")
        tickers = [t.strip().upper() for t in tickers_env.split(",") if t.strip()]
    else:
        tickers = [t.strip().upper() for t in tickers if isinstance(t, str) and t.strip()]

    print(f"[run_prefetch] start tickers={tickers}")
    # Reuse the same prefetch routine
    await run_daily_prefetch(tickers)

    results: Dict[str, Dict[str, Any]] = {}
    for t in tickers:
        try:
            if cache_get_by_ticker:
                cached = await cache_get_by_ticker(t, max_age_hours=24)
                news_n = len(cached.get("news", []))
                filings_n = len(cached.get("filings", []))
                results[t] = {"ticker": t, "news": news_n, "filings": filings_n, "cached": True}
            else:
                # If cache layer not available, attempt a lightweight fetch to report counts
                news = fetch_news_for_ticker(t) or []
                filings = await get_filings_for(t)
                results[t] = {"ticker": t, "news": len(news), "filings": len(filings), "cached": False}
        except Exception as e:
            print(f"[run_prefetch] error for {t}: {e}")
            results[t] = {"ticker": t, "news": None, "filings": None, "cached": False, "error": str(e)}

    print(f"[run_prefetch] done tickers={tickers}")
    return results