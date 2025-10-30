import os
import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("ari.metrics")

# module-level memo to avoid re-running index creation too often
_last_index_run_date: Optional[datetime.date] = None
_METRICS_JSON_SUFFIX = ".metrics.json"
_FALLBACK_CACHE_DB = str(Path(__file__).resolve().parent.parent / "cache.db")


def _resolve_cache_db_path() -> str:
    try:
        # avoid circular import at module import time
        from app.core.cache import CACHE_DB_PATH  # type: ignore
        return CACHE_DB_PATH or _FALLBACK_CACHE_DB
    except Exception:
        return _FALLBACK_CACHE_DB


def ensure_metrics_index() -> None:
    """
    Ensure sqlite metrics table and composite index on (timestamp, event, provider).
    Does not perform any deletion/retention.
    """
    try:
        cache_db = _resolve_cache_db_path()
        if not os.path.exists(cache_db):
            return
        conn = sqlite3.connect(cache_db, timeout=5)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    timestamp TEXT NOT NULL,
                    event TEXT NOT NULL,
                    provider TEXT,
                    latency_ms INTEGER,
                    ok INTEGER
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_metrics_day ON metrics(timestamp, event, provider)"
            )
            conn.commit()
            log.info("metrics: ensured index idx_metrics_day")
        finally:
            conn.close()
    except Exception:
        log.exception("metrics: ensure index failed")


def ensure_metrics_index_once_per_day() -> None:
    """
    Run ensure_metrics_index at most once per UTC day (best-effort memoization).
    """
    global _last_index_run_date
    today = datetime.now(tz=timezone.utc).date()
    if _last_index_run_date == today:
        return
    ensure_metrics_index()
    _last_index_run_date = today


def record_metric(event: str, provider: str, latency_ms: int, ok: bool) -> None:
    """
    Ensure indexes are present once/day, then delegate to the core recorder.
    This keeps index creation close to the first write while avoiding duplication.
    """
    ensure_metrics_index_once_per_day()
    try:
        # delegate to the central recorder (avoid direct import at module load time)
        from app.core.metrics import record_metric as _core_record  # type: ignore

        _core_record(event, provider, latency_ms, ok)
    except Exception:
        log.exception("observability.metrics: failed to delegate record_metric")