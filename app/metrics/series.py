"""
Time-series KPIs with day/week/month bucketing (IST).
- rating_ts: avg rating & count
- cost_per_item_ts: total vendor cost / delivered items
- send_success_ts: email send success rate
"""
from __future__ import annotations
from typing import Literal, Dict, Any, List
import aiosqlite
import logging

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.metrics.series")

Bucket = Literal["day", "week", "month"]

# IST offset from UTC
IST_SHIFT = "+5 hours 30 minutes"


def _bucket_expr(bucket: Bucket, col: str) -> str:
    """
    Generate SQL expression for bucketing timestamps into day/week/month.
    
    Simplified version: Use DATE() directly on the column.
    Works for both formats:
    - Plain datetime: "2025-11-03 07:00:13"  
    - ISO with timezone: "2025-11-03T07:00:14+00:00"
    
    Args:
        bucket: Bucketing granularity ("day", "week", or "month")
        col: Column name containing timestamp
        
    Returns:
        SQL expression for bucketing
    """
    # Simple date extraction works for both timestamp formats
    if bucket == "day":
        return f"DATE({col})"
    elif bucket == "week":
        return f"strftime('%Y-W%W', {col})"
    elif bucket == "month":
        return f"strftime('%Y-%m', {col})"
    else:
        raise ValueError(f"Invalid bucket type: {bucket}")


async def rating_ts(
    db: aiosqlite.Connection, 
    start: str, 
    end: str, 
    bucket: Bucket
) -> List[Dict[str, Any]]:
    """
    Get average rating and count over time buckets.
    """
    key = _bucket_expr(bucket, "created_at")
    
    # Check if email_feedback table exists
    try:
        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='email_feedback'")
        has_feedback_table = await cursor.fetchone() is not None
        await cursor.close()
    except Exception:
        has_feedback_table = False
    
    if has_feedback_table:
        sql = f"""
        SELECT 
            {key} AS k, 
            AVG(CAST(rating AS REAL)) AS avg_rating, 
            COUNT(*) AS n
        FROM email_feedback
        WHERE {key} BETWEEN ? AND ?
        GROUP BY k 
        ORDER BY k
        """
        params = (start, end)
    else:
        sql = f"""
        SELECT 
            {key} AS k,
            AVG(CAST(rating AS REAL)) AS avg_rating,
            COUNT(*) AS n
        FROM email_events
        WHERE {key} BETWEEN ? AND ?
          AND event_type = 'feedback'
          AND rating IS NOT NULL
        GROUP BY k 
        ORDER BY k
        """
        params = (start, end)
    
    try:
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        
        result = [
            {
                "bucket": r[0],
                "avg_rating": round(r[1], 2) if r[1] is not None else None,
                "count": r[2]
            }
            for r in rows
        ]
        
        log.info(f"rating_ts: returning {len(result)} buckets")
        return result
    except Exception as e:
        log.error("rating_ts: failed to compute - %s", e, exc_info=True)
        return []


async def cost_per_item_ts(
    db: aiosqlite.Connection, 
    start: str, 
    end: str, 
    bucket: Bucket
) -> List[Dict[str, Any]]:
    """Calculate cost per item over time buckets."""
    k_metrics = _bucket_expr(bucket, "created_at")
    k_emails = _bucket_expr(bucket, "sent_at")
    
    log.debug(f"cost_per_item_ts: querying from {start} to {end}")
    
    # Import VENDOR_COSTS
    try:
        from app.core.config import VENDOR_COSTS
    except ImportError:
        log.warning("cost_per_item_ts: VENDOR_COSTS not found, using defaults")
        VENDOR_COSTS = {
            "scrapingdog": 0.001,
            "diffbot": 0.002,
            "sendgrid": 0.0001,
            "gemini": 0.000085,
            "openai": 0.0067,
        }
    
    # 1) Get vendor costs by bucket
    try:
        cur = await db.execute(
            f"""
            SELECT 
                {k_metrics} AS k, 
                provider, 
                COUNT(*) AS c
            FROM vendor_metrics
            WHERE {k_metrics} BETWEEN ? AND ?
              AND {k_metrics} IS NOT NULL
            GROUP BY k, provider
            ORDER BY k
            """,
            (start, end)
        )
        rows = await cur.fetchall()
        await cur.close()
        
        cost_map: Dict[str, float] = {}
        for k, provider, c in rows:
            if k:  # Skip NULL buckets
                per_call = VENDOR_COSTS.get(provider, 0.0)
                cost_map[k] = cost_map.get(k, 0.0) + (per_call * c)
        
        log.debug(f"cost_per_item_ts: cost_map = {cost_map}")
        
    except Exception as e:
        log.error("cost_per_item_ts: failed to compute costs - %s", e, exc_info=True)
        cost_map = {}
    
    # 2) Get delivered items by bucket
    try:
        cur = await db.execute(
            f"""
            SELECT 
                {k_emails} AS k, 
                COALESCE(SUM(items_count), 0) AS delivered_items
            FROM email_logs
            WHERE ok=1 
              AND {k_emails} BETWEEN ? AND ?
              AND {k_emails} IS NOT NULL
            GROUP BY k 
            ORDER BY k
            """,
            (start, end)
        )
        items_rows = await cur.fetchall()
        await cur.close()
        
        items_map = {k: di for k, di in items_rows if k}  # Skip NULL buckets
        log.debug(f"cost_per_item_ts: items_map = {items_map}")
        
    except Exception as e:
        log.error("cost_per_item_ts: failed to compute delivered items - %s", e, exc_info=True)
        items_map = {}
    
    # 3) Join cost and items - show ALL dates with activity
    all_buckets = sorted(set(list(cost_map.keys()) + list(items_map.keys())))
    out = []
    
    for b in all_buckets:
        total_cost = float(cost_map.get(b, 0.0))
        delivered = int(items_map.get(b, 0))
        
        # Calculate cost per item
        if delivered > 0:
            cost_per_item = round(total_cost / delivered, 4)
        else:
            # No deliveries = can't calculate cost per item, use None for gap
            cost_per_item = None
        
        out.append({
            "bucket": b,
            "total_cost": round(total_cost, 4),
            "delivered_items": delivered,
            "cost_per_item": cost_per_item
        })
    
    log.info(f"cost_per_item_ts: returning {len(out)} buckets: {[o['bucket'] for o in out]} with values: {[o['cost_per_item'] for o in out]}")
    return out


async def send_success_ts(
    db: aiosqlite.Connection, 
    start: str, 
    end: str, 
    bucket: Bucket
) -> List[Dict[str, Any]]:
    """Calculate email send success rate over time buckets."""
    k = _bucket_expr(bucket, "sent_at")
    
    log.debug(f"send_success_ts: querying from {start} to {end}")
    
    try:
        cur = await db.execute(
            f"""
            SELECT 
                {k} AS k,
                COUNT(*) AS total,
                SUM(CASE WHEN ok=1 THEN 1 ELSE 0 END) AS success
            FROM email_logs
            WHERE {k} BETWEEN ? AND ?
              AND {k} IS NOT NULL
            GROUP BY k 
            ORDER BY k
            """,
            (start, end)
        )
        rows = await cur.fetchall()
        await cur.close()
        
        out = []
        for k, total, success in rows:
            if k:  # Skip NULL buckets
                success_rate = round((success / total) * 100.0, 2) if total > 0 else 0.0
                out.append({
                    "bucket": k,
                    "total": total,
                    "success": success,
                    "success_rate": success_rate
                })
        
        log.info(f"send_success_ts: returning {len(out)} buckets: {[o['bucket'] for o in out]}")
        return out
        
    except Exception as e:
        log.error("send_success_ts: failed to compute - %s", e, exc_info=True)
        return []


async def compute_series(
    metric: str, 
    start: str, 
    end: str, 
    bucket: str = "day",
    db_path: str | None = None
) -> list[dict]:
    """
    Compute time-series data for a specific metric.
    
    Args:
        metric: Metric name ("send_success", "rating", or "cost_per_item")
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        bucket: Time bucket granularity ("day", "week", or "month")
        db_path: Path to database (defaults to CACHE_DB_PATH)
        
    Returns:
        List of data points for the metric
    """
    if not db_path:
        db_path = CACHE_DB_PATH
    
    log.info(f"compute_series: metric={metric} from {start} to {end} (bucket={bucket})")
    
    try:
        async with aiosqlite.connect(db_path) as db:
            if metric == "send_success":
                return await send_success_ts(db, start, end, bucket)
            elif metric == "rating":
                return await rating_ts(db, start, end, bucket)
            elif metric == "cost_per_item":
                return await cost_per_item_ts(db, start, end, bucket)
            else:
                log.warning(f"compute_series: unknown metric {metric}")
                return []
    
    except Exception as e:
        log.error(f"compute_series: failed to compute {metric} - %s", e, exc_info=True)
        return []