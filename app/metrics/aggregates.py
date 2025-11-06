"""
Vendor performance and cost aggregation.
Computes KPIs: success rate, total calls, avg latency, total cost.
"""
from __future__ import annotations
import logging
from typing import List, Dict, Any
import aiosqlite

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.metrics.aggregates")

# Import vendor costs
try:
    from app.core.config import VENDOR_COSTS
except ImportError:
    log.warning("aggregates: VENDOR_COSTS not found, using defaults")
    VENDOR_COSTS = {
        "scrapingdog": 0,
        "diffbot": 0,
        "sendgrid": 0,
        "gemini": 0.000085,
        "openai": 0.0067,
    }


async def vendor_performance_summary(db_path: str | None = None) -> List[Dict[str, Any]]:
    """
    Returns success%, total calls, avg latency, and total cost per provider.
    
    Groups by (provider, event) and computes:
    - total_calls: Total number of operations
    - successes: Number of successful operations (ok=1)
    - success_pct: Success rate percentage
    - avg_latency_ms: Average latency in milliseconds
    - cost_per_call: Cost per single operation (from config)
    - total_cost: Total cost = cost_per_call × total_calls
    
    Args:
        db_path: Path to SQLite database (defaults to CACHE_DB_PATH)
        
    Returns:
        List of dictionaries with vendor performance metrics
    """
    if not db_path:
        db_path = CACHE_DB_PATH
    
    log.info("vendor_performance_summary: querying metrics from %s", db_path)
    
    query = """
        SELECT 
            provider,
            event,
            COUNT(*) AS total_calls,
            SUM(CASE WHEN ok=1 THEN 1 ELSE 0 END) AS successes,
            AVG(latency_ms) AS avg_latency
        FROM metrics
        WHERE event IN ('fetch', 'extract', 'summarize', 'email')
        GROUP BY provider, event
        ORDER BY provider, event
    """
    
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(query)
            rows = await cursor.fetchall()
        
        result = []
        for r in rows:
            provider = r["provider"]
            event = r["event"]
            total_calls = r["total_calls"]
            successes = r["successes"]
            avg_latency = r["avg_latency"]
            
            # Get cost per call from config
            cost_per_call = VENDOR_COSTS.get(provider, 0)
            
            # Calculate total cost
            total_cost = cost_per_call * total_calls
            
            # Calculate success percentage
            success_pct = (successes / total_calls * 100) if total_calls > 0 else 0
            
            result.append({
                "provider": provider,
                "event": event,
                "total_calls": total_calls,
                "successes": successes,
                "failures": total_calls - successes,
                "success_pct": round(success_pct, 2),
                "avg_latency_ms": round(avg_latency or 0, 1),
                "cost_per_call": round(cost_per_call, 6),
                "total_cost": round(total_cost, 2),
            })
        
        log.info("vendor_performance_summary: returned %d provider/event combinations", len(result))
        return result
        
    except Exception as e:
        log.exception("vendor_performance_summary: failed to compute summary")
        raise


async def vendor_totals(db_path: str | None = None) -> Dict[str, Any]:
    """
    Aggregate totals across all vendors.
    
    Returns:
        Dictionary with overall totals: calls, cost, success rate
    """
    if not db_path:
        db_path = CACHE_DB_PATH
    
    summary = await vendor_performance_summary(db_path)
    
    total_calls = sum(s["total_calls"] for s in summary)
    total_successes = sum(s["successes"] for s in summary)
    total_cost = sum(s["total_cost"] for s in summary)
    
    overall_success_pct = (total_successes / total_calls * 100) if total_calls > 0 else 0
    
    # Group by provider for provider-level totals
    by_provider: Dict[str, Dict[str, float]] = {}
    for s in summary:
        provider = s["provider"]
        if provider not in by_provider:
            by_provider[provider] = {
                "total_calls": 0,
                "total_cost": 0,
                "successes": 0,
            }
        by_provider[provider]["total_calls"] += s["total_calls"]
        by_provider[provider]["total_cost"] += s["total_cost"]
        by_provider[provider]["successes"] += s["successes"]
    
    # Calculate provider-level success rates
    provider_stats = []
    for provider, stats in by_provider.items():
        success_pct = (stats["successes"] / stats["total_calls"] * 100) if stats["total_calls"] > 0 else 0
        provider_stats.append({
            "provider": provider,
            "total_calls": int(stats["total_calls"]),
            "total_cost": round(stats["total_cost"], 2),
            "success_pct": round(success_pct, 2),
        })
    
    # Sort by cost descending
    provider_stats.sort(key=lambda x: x["total_cost"], reverse=True)
    
    return {
        "total_calls": total_calls,
        "total_successes": total_successes,
        "total_cost": round(total_cost, 2),
        "overall_success_pct": round(overall_success_pct, 2),
        "by_provider": provider_stats,
    }