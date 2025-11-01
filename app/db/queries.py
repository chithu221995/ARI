from __future__ import annotations
import sqlite3
import logging
from typing import List, Dict, Any, Sequence, Tuple, Optional
from datetime import timedelta, datetime

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.db")


def fetch_recent_summaries(
    tickers: Sequence[str],
    hours: int = 36,
    max_per_ticker: int = 3,
    min_relevance: int = 4,
) -> List[Dict[str, Any]]:
    """
    Return up to `max_per_ticker` items per ticker, newest & highest relevance first,
    filtering out items with relevance < min_relevance.
    """
    if not tickers:
        return []

    since = datetime.utcnow() - timedelta(hours=hours)
    params: Tuple[Any, ...] = tuple(tickers) + (since.isoformat(), min_relevance)

    # Pull a superset (e.g., 10 each) then trim in Python to avoid SQLite window funcs.
    per_tkr_cap = max_per_ticker * 3

    sql = f"""
    SELECT
      ticker,
      title,
      url,
      COALESCE(why_it_matters, bullets, '') AS summary,
      sentiment,
      relevance,
      created_at
    FROM summaries
    WHERE
      ticker IN ({",".join("?" for _ in tickers)})
      AND created_at >= ?
      AND relevance >= ?
    ORDER BY ticker, relevance DESC, created_at DESC
    """

    out: List[Dict[str, Any]] = []
    seen_per: Dict[str, int] = {}

    with sqlite3.connect(CACHE_DB_PATH, timeout=5) as conn:
        cur = conn.cursor()
        for row in cur.execute(sql, params):
            tkr = row[0]
            count = seen_per.get(tkr, 0)
            if count >= per_tkr_cap:
                continue
            out.append({
                "ticker": tkr,
                "title": row[1] or "",
                "url": row[2] or "",
                "summary": row[3] or "",
                "sentiment": row[4] or "",
                "relevance": int(row[5] or 0),
                "created_at": row[6],
            })
            seen_per[tkr] = count + 1

    # Final per-ticker trim to max_per_ticker
    final: List[Dict[str, Any]] = []
    seen_per.clear()
    for it in out:
        tkr = it["ticker"]
        n = seen_per.get(tkr, 0)
        if n < max_per_ticker:
            final.append(it)
            seen_per[tkr] = n + 1
    return final


def get_last_ok_by_job_for_ticker(conn, ticker: str) -> Dict[str, Optional[str]]:
    """
    Returns last OK ended_at per job for a specific ticker. Jobs: fetch, extract, summarize.
    Email job is global (ticker may be NULL) and is handled separately.
    """
    sql = """
    SELECT job, MAX(ended_at) AS ts
    FROM runs
    WHERE ok = 1
      AND ticker = ?
      AND job IN ('fetch','extract','summarize')
    GROUP BY job
    """
    out = {"fetch": None, "extract": None, "summarize": None}
    cur = conn.cursor()
    for job, ts in cur.execute(sql, (ticker,)):
        out[job] = ts
    return out


def get_last_ok_global_email(conn) -> Optional[str]:
    """Returns last OK ended_at for email (global fan-out; ticker may be NULL)."""
    sql = """
    SELECT MAX(ended_at) AS ts
    FROM runs
    WHERE ok = 1
      AND job = 'email'
    """
    cur = conn.cursor()
    row = cur.execute(sql).fetchone()
    return row[0] if row and row[0] else None


def get_distinct_tickers_with_runs(conn) -> List[str]:
    """Returns distinct non-NULL tickers that have any runs recorded."""
    sql = """
    SELECT DISTINCT ticker
    FROM runs
    WHERE ticker IS NOT NULL
    ORDER BY ticker
    """
    cur = conn.cursor()
    return [r[0] for r in cur.execute(sql)]


def insert_run(
    job: str,
    ticker: Optional[str],
    ok: int,
    note: str = "",
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
):
    """
    Insert a run record into the runs table.
    
    Args:
        job: Job name (fetch, extract, summarize, email)
        ticker: Ticker symbol (NULL for global jobs like email)
        ok: 1 for success, 0 for failure
        note: Optional note/error message (truncated to 500 chars)
        started_at: ISO timestamp when job started (defaults to now)
        ended_at: ISO timestamp when job ended (defaults to now)
    """
    started_at = started_at or datetime.utcnow().isoformat(timespec="seconds") + "Z"
    ended_at = ended_at or datetime.utcnow().isoformat(timespec="seconds") + "Z"
    
    with sqlite3.connect(CACHE_DB_PATH, timeout=5) as con:
        con.execute(
            "INSERT INTO runs(job,ticker,started_at,ended_at,ok,note) VALUES(?,?,?,?,?,?)",
            (job, ticker, started_at, ended_at, ok, note[:500]),
        )
        con.commit()