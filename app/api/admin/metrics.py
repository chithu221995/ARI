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
            # Total users
            cur.execute("SELECT COUNT(*) FROM users")
            total_users = int((cur.fetchone() or [0])[0] or 0)

            # Total tickers selected (user_tickers rows) and unique tickers
            cur.execute("SELECT COUNT(*) FROM user_tickers")
            total_tickers_selected = int((cur.fetchone() or [0])[0] or 0)
            cur.execute("SELECT COUNT(DISTINCT UPPER(TRIM(ticker))) FROM user_tickers")
            total_unique_tickers_selected = int((cur.fetchone() or [0])[0] or 0)

            # --- Emails: use email_logs as source of truth for sent emails ---
            # Total emails sent (from email_logs where ok=1)
            cur.execute(
                """
                SELECT COUNT(*) 
                FROM email_logs 
                WHERE ok = 1 
                  AND sent_at BETWEEN ? AND ?
                """,
                (start_iso, end_iso)
            )
            total_emails_sent = int((cur.fetchone() or [0])[0] or 0)

            # Average emails per user
            avg_emails_per_user = round((total_emails_sent / total_users), 2) if total_users > 0 else 0.0

            # --- Email engagement from email_events (SendGrid webhooks) ---
            # Count distinct delivered events (unique email + date combinations)
            cur.execute(
                """
                SELECT COUNT(DISTINCT (email || '||' || date(created_at)))
                FROM email_events
                WHERE event_type = 'delivered' 
                  AND created_at BETWEEN ? AND ?
                """,
                (start_iso, end_iso)
            )
            total_delivered = int((cur.fetchone() or [0])[0] or 0)

            # Count distinct bounced/dropped events
            cur.execute(
                """
                SELECT COUNT(DISTINCT (email || '||' || date(created_at)))
                FROM email_events
                WHERE event_type IN ('bounced', 'dropped') 
                  AND created_at BETWEEN ? AND ?
                """,
                (start_iso, end_iso)
            )
            total_bounced = int((cur.fetchone() or [0])[0] or 0)

            # Count distinct open events
            cur.execute(
                """
                SELECT COUNT(DISTINCT (email || '||' || date(created_at)))
                FROM email_events
                WHERE event_type = 'open' 
                  AND created_at BETWEEN ? AND ?
                """,
                (start_iso, end_iso)
            )
            total_opened = int((cur.fetchone() or [0])[0] or 0)

            # Open percentage (opened / delivered)
            pct_opened = round((total_opened * 100.0 / total_delivered), 2) if total_delivered > 0 else None

            # Average time to first open (in minutes)
            cur.execute(
                """
                SELECT AVG((julianday(first_open) - julianday(delivered_at)) * 24.0 * 60.0) AS avg_minutes
                FROM (
                    SELECT 
                        d.created_at AS delivered_at,
                        (SELECT MIN(o.created_at)
                         FROM email_events o
                         WHERE o.event_type = 'open'
                           AND o.email = d.email
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
                "avg_emails_sent_per_user": avg_emails_per_user,
                "total_bounced": total_bounced,
                "total_delivered": total_delivered,
                "total_opened": total_opened,
                "pct_opened": pct_opened,
                "avg_open_minutes": round(avg_open_minutes, 2) if avg_open_minutes is not None else None,
            }
        except Exception as e:
            log.exception("ticker_age: failed to compute summary metrics - %s", e)
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


@router.get("/sendgrid/metrics", summary="SendGrid metrics overview")
async def sendgrid_metrics(
    start: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """
    Get metrics overview for SendGrid API calls, email sends, and webhook events.
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
        
        log.info(f"Fetching SendGrid metrics from {start_iso} to {end_iso}")
        
        conn = sqlite3.connect(CACHE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # 1) vendor API calls for SendGrid
        cur.execute(
            """
            SELECT provider, event, COUNT(*) AS cnt
            FROM vendor_metrics
            WHERE provider = 'sendgrid' AND created_at BETWEEN ? AND ?
            GROUP BY provider, event
            """,
            (start_iso, end_iso)
        )
        vendor_calls = [dict(row) for row in cur.fetchall()]

        # 2) successful email sends recorded (email_logs.ok = 1)
        cur.execute(
            """
            SELECT COUNT(*) FROM email_logs
            WHERE ok = 1 AND sent_at BETWEEN ? AND ?
            """,
            (start_iso, end_iso)
        )
        total_successful_sends = cur.fetchone()[0] or 0

        # 3) webhook events by type
        cur.execute(
            """
            SELECT event_type, COUNT(*) FROM email_events
            WHERE created_at BETWEEN ? AND ?
            GROUP BY event_type
            """,
            (start_iso, end_iso)
        )
        webhook_events = [dict(row) for row in cur.fetchall()]

        # 4) how many email_logs have no matching email_events (best-effort match by email + sent_at)
        cur.execute(
            """
            SELECT el.id, el.to_email, el.sent_at
            FROM email_logs el
            LEFT JOIN email_events ev
              ON ev.email = el.to_email AND ev.email_sent_at IS NOT NULL AND date(ev.email_sent_at) = date(el.sent_at)
            WHERE el.ok = 1
              AND el.sent_at BETWEEN ? AND ?
              AND ev.id IS NULL
            LIMIT 50
            """,
            (start_iso, end_iso)
        )
        unmatched_email_logs = [dict(row) for row in cur.fetchall()]

        conn.close()

        return {
            "ok": True,
            "start": start_iso,
            "end": end_iso,
            "vendor_calls": vendor_calls,
            "total_successful_sends": total_successful_sends,
            "webhook_events": webhook_events,
            "unmatched_email_logs": unmatched_email_logs,
        }
    except Exception as e:
        log.exception("sendgrid_metrics: failed")
        raise HTTPException(status_code=500, detail=str(e))