from __future__ import annotations
from typing import Dict, List, Any
import logging

from app.core.cache import CACHE_DB_PATH
from app.db.users import get_unique_active_tickers, get_user_tickers_map
from app.email.brief import send_brief_email
from app.core.settings import settings

log = logging.getLogger("ari.pipeline")


async def run_daily_fanout(max_items_per_ticker: int = 5) -> Dict[str, Any]:
    """
    1) Collect unique active tickers across users.
    2) Fetch->Extract->Summarize exactly once per ticker.
    3) Fan-out: assemble and email each user's combined brief from cached summaries.
    Returns a small dict with counts for observability.
    """
    # Import here to avoid circular dependency
    from app.api.admin.jobs import job_fetch, job_extract, job_summarize

    # 1) Unique tickers
    tickers = get_unique_active_tickers(CACHE_DB_PATH)
    log.info("fanout: unique tickers=%s", tickers)

    # 2) Per-ticker pipeline (deduped)
    did = {"fetch": 0, "extract": 0, "summarize": 0, "emails": 0}
    for t in tickers:
        try:
            await job_fetch(ticker=t, max_items=getattr(settings, "NEWS_TOPK", 10))
            did["fetch"] += 1
        except Exception:
            log.exception("fanout: fetch failed ticker=%s", t)

        try:
            await job_extract(ticker=t)
            did["extract"] += 1
        except Exception:
            log.exception("fanout: extract failed ticker=%s", t)

        try:
            await job_summarize(tickers=[t])
            did["summarize"] += 1
        except Exception:
            log.exception("fanout: summarize failed ticker=%s", t)

    # 3) Fan-out emails (read per user tickers and send one email each)
    user_map = get_user_tickers_map(CACHE_DB_PATH)
    for email, user_tickers in user_map.items():
        try:
            # send_brief_email already knows how to assemble summaries by ticker
            ok = await send_brief_email(email=email, tickers=user_tickers)
            did["emails"] += 1 if ok else 0
        except Exception:
            log.exception("fanout: email failed email=%s", email)

    log.info("fanout: done %s", did)
    return {"ok": True, "tickers": tickers, **did}