from __future__ import annotations
from typing import Any, Optional, Dict
from fastapi import APIRouter, Depends, Query
import logging
import sqlite3

from app.core.cache import CACHE_DB_PATH
from app.core.metrics import get_daily_summary
from app.db.queries import (
    get_last_ok_by_job_for_ticker,
    get_last_ok_global_email,
    get_distinct_tickers_with_runs,
)

log = logging.getLogger("ari.admin.metrics")
router = APIRouter(prefix="/metrics", tags=["admin:metrics"])


@router.get("/summary")
async def metrics_summary():
    try:
        summary = get_daily_summary()
        return {"ok": True, "summary": summary}
    except Exception:
        log.exception("admin.metrics: failed to fetch summary")
        return {"ok": False, "error": "internal_error"}


def get_db():
    """Dependency to get database connection."""
    conn = sqlite3.connect(CACHE_DB_PATH, timeout=5)
    try:
        yield conn
    finally:
        conn.close()


@router.get("/admin/runs/latest")
def runs_latest(
    ticker: Optional[str] = Query(default=None),
    conn: sqlite3.Connection = Depends(get_db),
) -> Dict[str, Any]:
    """
    Latest successful run timestamps.
    - If `ticker` is provided: return {ticker, fetch, extract, summarize, email, ok}
    - If no ticker: return {"items": [{ticker, fetch, extract, summarize, email, ok}, ...]}
    'ok' is True if fetch/extract/summarize are all present (email is global).
    """
    email_ts = get_last_ok_global_email(conn)

    def _shape_one(t: str) -> Dict[str, Any]:
        per = get_last_ok_by_job_for_ticker(conn, t)
        ok = bool(per["fetch"] and per["extract"] and per["summarize"])
        return {
            "ticker": t,
            "fetch": per["fetch"],
            "extract": per["extract"],
            "summarize": per["summarize"],
            "email": email_ts,
            "ok": ok,
        }

    if ticker:
        return _shape_one(ticker.upper())

    items = [_shape_one(t) for t in get_distinct_tickers_with_runs(conn)]
    return {"items": items}