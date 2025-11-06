from __future__ import annotations
from typing import Any, Optional, Dict
from fastapi import APIRouter, Depends, Query, HTTPException
import logging
from datetime import datetime, timedelta
import time

from app.core.cache import CACHE_DB_PATH
import sqlite3
from app.metrics.aggregates import vendor_performance_summary, vendor_totals
from app.metrics.kpi_aggregates import compute_kpi_aggregates
from app.metrics.series import compute_series  # Changed from time_series
from app.utils.dates import normalize_to_ist_day_start, normalize_to_ist_day_end, enforce_date_range

log = logging.getLogger("ari.admin.metrics")
router = APIRouter(tags=["admin:metrics"])


@router.get("/runs/summary")
async def runs_summary(days: int = Query(default=7, ge=1, le=90)):
    """
    Get run summary for the last N days.
    Returns counts by job type, success rate, etc.
    """
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get run statistics
        cursor.execute(
            """
            SELECT 
                job,
                COUNT(*) as total_runs,
                SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) as successful_runs,
                AVG(CASE WHEN ok = 1 THEN 1.0 ELSE 0.0 END) * 100 as success_rate
            FROM runs
            WHERE datetime(started_at) >= datetime('now', '-' || ? || ' days')
            GROUP BY job
            ORDER BY job
            """,
            (days,),
        )

        job_stats = [dict(row) for row in cursor.fetchall()]

        # Get overall statistics
        cursor.execute(
            """
            SELECT 
                COUNT(*) as total_runs,
                SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) as successful_runs,
                AVG(CASE WHEN ok = 1.0 THEN 1.0 ELSE 0.0 END) * 100 as success_rate
            FROM runs
            WHERE datetime(started_at) >= datetime('now', '-' || ? || ' days')
            """,
            (days,),
        )

        overall = dict(cursor.fetchone())
        conn.close()

        return {"ok": True, "days": days, "overall": overall, "by_job": job_stats}

    except Exception as e:
        log.exception("runs_summary: failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def metrics_summary(
    event: Optional[str] = Query(default=None),
    provider: Optional[str] = Query(default=None),
    days: int = Query(default=7, ge=1, le=90),
):
    """
    Get metrics summary with optional filters.
    Returns latency stats, success rate, etc.
    """
    try:
        conn = sqlite3.connect(CACHE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Build WHERE clause - use 'timestamp' column
        where_parts = ["datetime(timestamp) >= datetime('now', '-' || ? || ' days')"]
        params = [days]

        if event:
            where_parts.append("event = ?")
            params.append(event)

        if provider:
            where_parts.append("provider = ?")
            params.append(provider)

        where_clause = " AND ".join(where_parts)

        # Get metrics statistics
        cursor.execute(
            f"""
            SELECT 
                event,
                provider,
                COUNT(*) as total_calls,
                SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) as successful_calls,
                AVG(CASE WHEN ok = 1.0 THEN 1.0 ELSE 0.0 END) * 100 as success_rate,
                AVG(latency_ms) as avg_latency_ms,
                MIN(latency_ms) as min_latency_ms,
                MAX(latency_ms) as max_latency_ms
            FROM metrics
            WHERE {where_clause}
            GROUP BY event, provider
            ORDER BY event, provider
            """,
            params,
        )

        stats = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return {
            "ok": True,
            "filters": {"days": days, "event": event, "provider": provider},
            "stats": stats,
        }

    except Exception as e:
        log.exception("metrics_summary: failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vendor/summary", summary="Vendor performance and cost KPIs")
async def vendor_summary():
    """
    Get vendor reliability and cost KPIs.

    Returns performance metrics for each provider/event combination:
    - Success rate percentage
    - Total calls and failures
    - Average latency
    - Cost per call and total cost
    """
    try:
        log.info("admin.metrics.vendor_summary: fetching vendor performance data")

        data = await vendor_performance_summary()

        return {"ok": True, "count": len(data), "summary": data}

    except Exception as e:
        log.exception("admin.metrics.vendor_summary: failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vendor/totals", summary="Overall vendor totals")
async def vendor_totals_endpoint():
    """
    Get aggregated totals across all vendors.

    Returns:
    - Total API calls across all vendors
    - Total cost in USD
    - Overall success rate
    - Breakdown by provider
    """
    try:
        log.info("admin.metrics.vendor_totals: computing overall totals")

        data = await vendor_totals()

        return {"ok": True, "totals": data}

    except Exception as e:
        log.exception("admin.metrics.vendor_totals: failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kpi", summary="Compute KPI aggregates")
async def get_kpi_metrics(
    start: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """
    Return aggregated KPI metrics for the specified date range.
    Date range is capped at 365 days and normalized to IST day boundaries.
    """
    try:
        # Parse and normalize dates to IST day boundaries
        start_dt = normalize_to_ist_day_start(datetime.fromisoformat(start))  # 00:00:00
        end_dt = normalize_to_ist_day_end(datetime.fromisoformat(end))        # 23:59:59
        
        # Enforce 365-day maximum
        start_dt, end_dt = enforce_date_range(start_dt, end_dt, max_days=365)
        
        # Convert to ISO strings
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()
        
        log.info(f"Computing KPIs for range {start_iso} to {end_iso}")
        
        results = await compute_kpi_aggregates(start_iso, end_iso)
        
        return {
            "ok": True,
            "start": start_iso,
            "end": end_iso,
            "results": results
        }
    except Exception as e:
        log.error(f"KPI computation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/series")
async def get_time_series(
    start: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end: str = Query(..., description="End date (YYYY-MM-DD)"),
    bucket: str = Query("day", description="Aggregation bucket: day, week, or month"),
):
    """
    Return time-series data for charts.
    Date range is capped at 365 days and normalized to IST day boundaries.
    """
    try:
        # Parse and normalize dates to IST day boundaries
        start_dt = normalize_to_ist_day_start(datetime.fromisoformat(start))
        end_dt = normalize_to_ist_day_end(datetime.fromisoformat(end))
        
        # Enforce 365-day maximum and ensure start <= end
        start_dt, end_dt = enforce_date_range(start_dt, end_dt, max_days=365)
        
        # Convert back to date strings (YYYY-MM-DD format for series.py)
        start_iso = start_dt.date().isoformat()
        end_iso = end_dt.date().isoformat()
        
        # Validate bucket parameter
        if bucket not in ["day", "week", "month"]:
            bucket = "day"
        
        log.info(f"Fetching time series: {start_iso} to {end_iso}, bucket={bucket}")
        
        # Use the compute_series function for each metric
        from app.metrics.series import compute_series
        
        send_success = await compute_series("send_success", start_iso, end_iso, bucket)
        rating = await compute_series("rating", start_iso, end_iso, bucket)
        cost_per_item = await compute_series("cost_per_item", start_iso, end_iso, bucket)
        
        results = {
            "send_success": send_success,
            "rating": rating,
            "cost_per_item": cost_per_item,
        }
        
        log.info(f"Time series: {len(send_success)} send_success, {len(rating)} rating, {len(cost_per_item)} cost points")
        
        return {
            "ok": True,
            "start": start_iso,
            "end": end_iso,
            "bucket": bucket,
            "results": results
        }
    except ValueError as e:
        log.error(f"Invalid date format: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        log.error(f"Time series computation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ticker_age", summary="Ticker-wise average news_age (hours)")
async def ticker_age(
    start: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end: str = Query(..., description="End date (YYYY-MM-DD)"),
    limit: int = Query(200, ge=1, le=1000, description="Max rows to return")
):
    """
    Return per-ticker average news_age (hours) and counts for articles fetched
    in the provided date range. Also returns ticker-wise avg age and count for
    articles that were actually sent to users (joined via summaries.url).
    """
    try:
        # Normalize to IST day boundaries (reuse helpers)
        start_dt = normalize_to_ist_day_start(datetime.fromisoformat(start))
        end_dt = normalize_to_ist_day_end(datetime.fromisoformat(end))
        start_dt, end_dt = enforce_date_range(start_dt, end_dt, max_days=365)
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()

        import sqlite3
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=5)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        sql = """
        WITH art AS (
          SELECT
            COALESCE(ticker, '') AS ticker,
            ROUND(AVG(news_age), 2) AS avg_hours,
            COUNT(*) AS cnt
          FROM articles
          WHERE content IS NOT NULL
            AND LENGTH(content) > 0
            AND created_at BETWEEN ? AND ?
            AND news_age IS NOT NULL
          GROUP BY ticker
        ), sent AS (
          SELECT
            COALESCE(s.ticker, '') AS ticker,
            ROUND(AVG(a.news_age), 2) AS avg_hours_sent,
            ROUND(AVG(s.relevance), 2) AS avg_relevance,
            ROUND(AVG(s.relevance), 2) AS avg_relevance_sent,
            COUNT(DISTINCT s.item_url_hash) AS sent_cnt
          FROM summaries s
          JOIN articles a ON s.url = a.url
          WHERE s.created_at BETWEEN ? AND ?
            AND a.news_age IS NOT NULL
          GROUP BY s.ticker
        )
        SELECT
          a.ticker AS ticker,
          a.avg_hours AS avg_hours,
          a.cnt AS cnt,
          s.avg_hours_sent AS avg_hours_sent,
          s.avg_relevance AS avg_relevance,
          s.avg_relevance_sent AS avg_relevance_sent,
          COALESCE(s.sent_cnt, 0) AS sent_cnt
        FROM art a
        LEFT JOIN sent s ON a.ticker = s.ticker
        ORDER BY a.avg_hours ASC
        LIMIT ?
        """

        cur.execute(sql, (start_iso, end_iso, start_iso, end_iso, limit))
        rows = [dict(r) for r in cur.fetchall()]

        # --- Summary metrics for dashboard (totals / rates) ---
        try:
            # total users
            cur.execute("SELECT COUNT(*) FROM users")
            total_users = int((cur.fetchone() or [0])[0] or 0)

            # total tickers selected (user_tickers rows) and unique tickers
            cur.execute("SELECT COUNT(*) FROM user_tickers")
            total_tickers_selected = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(DISTINCT UPPER(TRIM(ticker))) FROM user_tickers")
            total_unique_tickers_selected = int((cur.fetchone() or [0])[0] or 0)

            # --- Emails: use DISTINCT message identity (email + item_url_hash + date) from email_events ---
            # total delivered events (rows)
            cur.execute(
                "SELECT COUNT(*) FROM email_events WHERE event_type = 'delivered' AND created_at BETWEEN ? AND ?",
                (start_iso, end_iso)
            )
            delivered_events = int((cur.fetchone() or [0])[0] or 0)

            # total bounced/dropped events (rows)
            cur.execute(
                "SELECT COUNT(*) FROM email_events WHERE event_type IN ('dropped','bounced') AND created_at BETWEEN ? AND ?",
                (start_iso, end_iso)
            )
            bounced_events = int((cur.fetchone() or [0])[0] or 0)

            # total open events (rows)
            cur.execute(
                "SELECT COUNT(*) FROM email_events WHERE event_type = 'open' AND created_at BETWEEN ? AND ?",
                (start_iso, end_iso)
            )
            open_events = int((cur.fetchone() or [0])[0] or 0)

            # Count DISTINCT messages (email + item_url_hash + date(created_at)) for delivered / open
            cur.execute(
                """
                SELECT 
                  COUNT(DISTINCT (email || '||' || COALESCE(item_url_hash,'') || '||' || date(created_at))) as delivered_distinct
                FROM email_events
                WHERE event_type = 'delivered' AND created_at BETWEEN ? AND ?
                """,
                (start_iso, end_iso)
            )
            delivered_distinct = int((cur.fetchone() or [0])[0] or 0)

            cur.execute(
                """
                SELECT 
                  COUNT(DISTINCT (email || '||' || COALESCE(item_url_hash,'') || '||' || date(created_at))) as open_distinct
                FROM email_events
                WHERE event_type = 'open' AND created_at BETWEEN ? AND ?
                """,
                (start_iso, end_iso)
            )
            open_distinct = int((cur.fetchone() or [0])[0] or 0)

            # If email_events has no deliveries, fallback to email_items (count distinct recipients / items)
            if delivered_distinct == 0:
                cur.execute(
                    "SELECT COUNT(*) FROM email_items WHERE sent_at BETWEEN ? AND ?",
                    (start_iso, end_iso)
                )
                fallback_sent = int((cur.fetchone() or [0])[0] or 0)
                total_emails_sent = fallback_sent
            else:
                total_emails_sent = delivered_distinct

            # open % using distinct message counts
            pct_opened = round((open_distinct * 100.0 / delivered_distinct), 2) if delivered_distinct > 0 else None

            # Average time between delivery and first opening (minutes), ignore opens > 12 hours
            cur.execute(
                """
                SELECT AVG((julianday(first_open) - julianday(delivered_at)) * 24.0 * 60.0) AS avg_minutes
                FROM (
                  SELECT d.created_at AS delivered_at,
                    (SELECT MIN(o.created_at)
                     FROM email_events o
                     WHERE o.event_type = 'open'
                       AND o.email = d.email
                       AND COALESCE(o.item_url_hash,'') = COALESCE(d.item_url_hash,'')
                       AND o.created_at >= d.created_at
                       AND (julianday(o.created_at) - julianday(d.created_at)) <= 0.5
                    ) AS first_open
                  FROM email_events d
                  WHERE d.event_type = 'delivered'
                    AND d.created_at BETWEEN ? AND ?
                ) sub
                WHERE first_open IS NOT NULL
                """,
                (start_iso, end_iso)
            )
            row_open_avg = cur.fetchone()
            avg_open_minutes = float(row_open_avg[0]) if row_open_avg and row_open_avg[0] is not None else None

            summary = {
                "total_users": total_users,
                "total_tickers_selected": total_tickers_selected,
                "total_unique_tickers_selected": total_unique_tickers_selected,
                "total_emails_sent": total_emails_sent,
                "avg_emails_sent_per_user": round((total_emails_sent / total_users), 2) if total_users > 0 else 0.0,
                "total_bounced_events": bounced_events,
                "total_delivered_events": delivered_events,
                "total_open_events": open_events,
                "total_delivered_distinct": delivered_distinct,
                "total_open_distinct": open_distinct,
                "pct_opened": pct_opened,
                "avg_open_minutes": round(avg_open_minutes, 2) if avg_open_minutes is not None else None,
            }
        except Exception:
            log.exception("ticker_age: failed to compute summary metrics")
            summary = {}

        # Enrich rows with per-ticker user counts using user_tickers.email
        for r in rows:
            ticker_val = (r.get("ticker") or "").strip()
            user_cnt = 0
            ticker_norm = ticker_val.upper()

            try:
                cur2 = conn.cursor()
                # exact normalized match
                cur2.execute(
                    "SELECT COUNT(DISTINCT email) FROM user_tickers WHERE UPPER(TRIM(ticker)) = ?",
                    (ticker_norm,)
                )
                res = cur2.fetchone()
                user_cnt = int(res[0]) if res and res[0] is not None else 0
            except Exception:
                try:
                    # fallback: loose LIKE match
                    cur2 = conn.cursor()
                    cur2.execute(
                        "SELECT COUNT(DISTINCT email) FROM user_tickers WHERE UPPER(ticker) LIKE ?",
                        (f"%{ticker_norm}%",)
                    )
                    res = cur2.fetchone()
                    user_cnt = int(res[0]) if res and res[0] is not None else 0
                except Exception:
                    user_cnt = 0

            r["user_cnt"] = user_cnt

        conn.close()

        return {"ok": True, "start": start_iso, "end": end_iso, "rows": rows, "summary": summary}
    except Exception as e:
        log.exception("ticker_age: failed")
        raise HTTPException(status_code=500, detail=str(e))