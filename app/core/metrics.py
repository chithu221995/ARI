import os
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
from typing import List, Dict, Any

log = logging.getLogger("ari.metrics")

# Do not import app.core.cache at module import time (avoid circular imports).
# Resolve CACHE_DB_PATH lazily inside functions.
_FALLBACK_CACHE_DB = str(Path(__file__).resolve().parent.parent / "cache.db")
_METRICS_JSON_SUFFIX = ".metrics.json"


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _resolve_cache_db_path() -> str:
    try:
        from app.core.cache import CACHE_DB_PATH  # type: ignore
        return CACHE_DB_PATH or _FALLBACK_CACHE_DB
    except Exception:
        return _FALLBACK_CACHE_DB


def record_metric(event: str, provider: str, latency_ms: int, ok: bool) -> None:
    """
    Append one metric row (timestamp UTC). Uses sqlite metrics table if the CACHE_DB_PATH file exists,
    otherwise falls back to appending into a JSON file alongside the cache DB.
    """
    ts = _utc_now_iso()
    ok_int = 1 if ok else 0
    cache_db = _resolve_cache_db_path()
    metrics_json = cache_db + _METRICS_JSON_SUFFIX

    try:
        if os.path.exists(cache_db):
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
                    "INSERT INTO metrics(timestamp, event, provider, latency_ms, ok) VALUES (?, ?, ?, ?, ?)",
                    (ts, event, provider, int(latency_ms or 0), ok_int),
                )
                conn.commit()
                log.info("metrics: recorded sqlite event=%s provider=%s ok=%s latency_ms=%s", event, provider, ok, latency_ms)
            finally:
                conn.close()
            return
    except Exception:
        log.exception("metrics: sqlite record failed, falling back to json")

    # JSON fallback
    try:
        data = []
        if os.path.exists(metrics_json):
            with open(metrics_json, "r", encoding="utf-8") as fh:
                try:
                    data = json.load(fh) or []
                except Exception:
                    data = []
        entry = {"timestamp": ts, "event": event, "provider": provider, "latency_ms": int(latency_ms or 0), "ok": ok_int}
        data.append(entry)
        with open(metrics_json, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        log.info("metrics: recorded json event=%s provider=%s ok=%s latency_ms=%s", event, provider, ok, latency_ms)
    except Exception:
        log.exception("metrics: json record failed")


def record_vendor_event(provider: str, event: str, ok: bool, latency_ms: int) -> None:
    """
    Record a vendor API call to the vendor_metrics table.

    Args:
        provider: Vendor name (e.g., "scrapingdog", "diffbot", "sendgrid")
        event: Event type (e.g., "google_news", "extract_article", "send_email")
        ok: Whether the call succeeded
        latency_ms: Latency in milliseconds
    """
    ts = _utc_now_iso()
    ok_int = 1 if ok else 0
    cache_db = _resolve_cache_db_path()

    log.debug(f"record_vendor_event: provider={provider} event={event} ok={ok} latency_ms={latency_ms} db={cache_db}")

    try:
        if os.path.exists(cache_db):
            conn = sqlite3.connect(cache_db, timeout=5)
            try:
                cur = conn.cursor()
                # Create table if not exists
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS vendor_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        provider TEXT NOT NULL,
                        event TEXT NOT NULL,
                        ok INTEGER NOT NULL,
                        latency_ms INTEGER,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                # Insert the record
                cur.execute(
                    """
                    INSERT INTO vendor_metrics (provider, event, ok, latency_ms, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (provider, event, ok_int, int(latency_ms or 0), ts),
                )
                conn.commit()

                # Verify insert worked
                cur.execute("SELECT COUNT(*) FROM vendor_metrics WHERE provider = ? AND event = ?", (provider, event))
                count = cur.fetchone()[0]
                log.info(
                    f"metrics: recorded vendor_metrics provider={provider} event={event} ok={ok} latency_ms={latency_ms} (total_count={count})"
                )
            finally:
                conn.close()
        else:
            log.error(f"record_vendor_event: cache_db does not exist at {cache_db}")
    except Exception as e:
        log.exception(f"metrics: vendor_metrics record failed for provider={provider} event={event}: {e}")


def get_daily_summary() -> List[Dict[str, Any]]:
    """
    Return totals per event/provider for today (UTC).
    Returns list of dicts: { "event": .., "provider": .., "count": N, "avg_latency_ms": X, "successes": Y }
    """
    today = datetime.now(tz=timezone.utc).date()
    start_iso = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc).isoformat()
    end_iso = (datetime.combine(today, datetime.min.time()) + timedelta(days=1)).replace(tzinfo=timezone.utc).isoformat()

    results: List[Dict[str, Any]] = []

    try:
        cache_db = _resolve_cache_db_path()
        metrics_json = cache_db + _METRICS_JSON_SUFFIX
        if os.path.exists(cache_db):
            conn = sqlite3.connect(cache_db, timeout=5)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT event, provider, COUNT(*) AS cnt, AVG(latency_ms) AS avg_latency, SUM(ok) AS successes
                    FROM metrics
                    WHERE timestamp >= ? AND timestamp < ?
                    GROUP BY event, provider
                    """,
                    (start_iso, end_iso),
                )
                for row in cur.fetchall():
                    results.append(
                        {
                            "event": row[0],
                            "provider": row[1],
                            "count": int(row[2] or 0),
                            "avg_latency_ms": float(row[3]) if row[3] is not None else 0.0,
                            "successes": int(row[4] or 0),
                        }
                    )
                log.info("metrics: sqlite daily summary rows=%d", len(results))
                return results
            finally:
                conn.close()
    except Exception:
        log.exception("metrics: sqlite summary failed, falling back to json")

    # JSON fallback
    try:
        if not os.path.exists(metrics_json):
            log.info("metrics: json summary none (file missing)")
            return []
        with open(metrics_json, "r", encoding="utf-8") as fh:
            data = json.load(fh) or []
        # filter to today's entries
        agg: Dict[tuple, Dict[str, Any]] = {}
        for e in data:
            ts = e.get("timestamp")
            if not ts:
                continue
            if not (start_iso <= ts < end_iso):
                continue
            key = (e.get("event"), e.get("provider"))
            rec = agg.setdefault(key, {"event": key[0], "provider": key[1], "count": 0, "sum_latency": 0, "successes": 0})
            rec["count"] += 1
            rec["sum_latency"] += int(e.get("latency_ms") or 0)
            rec["successes"] += int(e.get("ok") or 0)
        for (k_event, k_provider), v in agg.items():
            cnt = v["count"]
            avg_lat = (v["sum_latency"] / cnt) if cnt else 0.0
            results.append({"event": k_event, "provider": k_provider, "count": cnt, "avg_latency_ms": avg_lat, "successes": v["successes"]})
        log.info("metrics: json daily summary rows=%d", len(results))
        return results
    except Exception:
        log.exception("metrics: json summary failed")
        return []