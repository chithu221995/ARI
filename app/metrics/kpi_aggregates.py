from __future__ import annotations
"""
Compute KPI aggregates for delivery, relevance, freshness, coverage, quality, and vendor performance.
Called by /admin/metrics/kpi route.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import aiosqlite
import time

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.metrics.kpi_aggregates")


async def compute_kpi_aggregates(
    start: str, 
    end: str, 
    db_path: str | None = None
) -> Dict[str, Any]:
    if not db_path:
        db_path = CACHE_DB_PATH
    
    log.info("compute_kpi_aggregates: computing KPIs for %s to %s", start, end)
    log.info(f"kpi_aggregates: START for {start} to {end}")
    t_start = time.time()
    
    try:
        start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
    except ValueError as e:
        log.error("compute_kpi_aggregates: invalid datetime format - %s", e)
        raise ValueError(f"Invalid datetime format. Use ISO format (e.g., '2025-11-01T00:00:00')")
    
    results = {
        "delivery": {},
        "relevance": {},
        "freshness": {},
        "coverage": {},
        "quality": {},
        "vendor": [],
    }

    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # --- Delivery KPIs ---
            log.debug("Computing delivery KPIs")
            cursor = await db.execute(
                """
                SELECT 
                    CAST(SUM(ok) AS REAL) / COUNT(*) AS send_success_rate,
                    AVG(latency_ms) AS avg_latency_ms,
                    MAX(latency_ms) AS max_latency_ms
                FROM metrics 
                WHERE event='email' AND timestamp BETWEEN ? AND ?
                """,
                (start, end)
            )
            delivery_row = await cursor.fetchone()
            
            if delivery_row:
                results["delivery"] = {
                    "send_success_rate": round((delivery_row[0] or 0) * 100, 2),
                    "avg_latency_ms": round(delivery_row[1] or 0, 1),
                    "max_latency_ms": delivery_row[2] or 0,
                }
            else:
                results["delivery"] = {
                    "send_success_rate": None,
                    "avg_latency_ms": None,
                    "max_latency_ms": None,
                }

            # --- Relevance KPIs (rating-based) ---
            log.debug("Computing relevance KPIs (rating-based)")
            
            # Fetch all feedback events with ratings (1-5 stars)
            cursor = await db.execute(
                """
                SELECT rating
                FROM email_events
                WHERE event_type='feedback' 
                  AND created_at BETWEEN ? AND ?
                  AND rating IS NOT NULL
                """,
                (start, end)
            )
            feedback_rows = await cursor.fetchall()
            
            # Extract ratings from rows
            ratings = [row[0] for row in feedback_rows if row[0] is not None]
            
            if ratings:
                total = len(ratings)
                # Positive feedback = ratings >= 4 (4 or 5 stars)
                positive = sum(1 for r in ratings if r >= 4)
                positive_feedback_pct = round((positive / total) * 100, 2)
                avg_rating = round(sum(ratings) / total, 2)
                
                log.info("Relevance: total_feedback=%d, avg_rating=%.2f, positive_pct=%.2f", 
                        total, avg_rating, positive_feedback_pct)
                
                results["relevance"] = {
                    "total_feedback": total,
                    "avg_rating": avg_rating,
                    "positive_feedback_pct": positive_feedback_pct,
                }
            else:
                log.warning("Relevance: no feedback data found")
                results["relevance"] = {
                    "total_feedback": 0,
                    "avg_rating": None,
                    "positive_feedback_pct": None,
                }
            
            # --- Freshness KPIs (existing) ---
            log.debug("Computing freshness KPIs (using news_age at fetch time)")
            
            cursor = await db.execute(
                """
                SELECT 
                    COUNT(*) as total_articles,
                    AVG(news_age) as avg_age_hours,
                    SUM(CASE WHEN news_age IS NOT NULL AND news_age <= 12 THEN 1 ELSE 0 END) as fresh_count
                FROM articles
                WHERE content IS NOT NULL
                  AND LENGTH(content) > 0
                  AND created_at BETWEEN ? AND ?
                  AND news_age IS NOT NULL
                """,
                (start, end)
            )
            
            row = await cursor.fetchone()
            
            if row and (row["total_articles"] or 0) > 0:
                total_articles = int(row["total_articles"] or 0)
                avg_age_hours = float(row["avg_age_hours"]) if row["avg_age_hours"] is not None else None
                fresh_count = int(row["fresh_count"] or 0)
                fresh_pct = round((fresh_count / total_articles) * 100.0, 2) if total_articles > 0 else None

                results["freshness"] = {
                    "source": "articles_news_age",
                    "total_articles": total_articles,
                    "fresh_count": fresh_count,
                    "fresh_pct": fresh_pct,
                    "avg_age_hours": round(avg_age_hours, 2) if avg_age_hours is not None else None,
                }
                
                log.info(
                    f"Freshness: {fresh_count}/{total_articles} articles within 12h "
                    f"(avg age={avg_age_hours:.2f}h at fetch time)"
                )
            else:
                results["freshness"] = {
                    "source": "articles_news_age",
                    "total_articles": 0,
                    "fresh_count": 0,
                    "fresh_pct": None,
                    "avg_age_hours": None,
                }

            # --- NEW: Sent Articles Freshness ---
            log.debug("Computing sent articles freshness (articles actually delivered to users)")
            
            cursor = await db.execute(
                """
                SELECT 
                    COUNT(DISTINCT s.item_url_hash) as total_sent,
                    AVG(a.news_age) as avg_age_hours_sent,
                    SUM(CASE WHEN a.news_age IS NOT NULL AND a.news_age <= 12 THEN 1 ELSE 0 END) as fresh_sent_count,
                    AVG(s.relevance) as avg_relevance_sent
                FROM summaries s
                INNER JOIN articles a ON s.url = a.url
                WHERE s.created_at BETWEEN ? AND ?
                  AND a.news_age IS NOT NULL
                """,
                (start, end)
            )
            
            sent_row = await cursor.fetchone()
            
            if sent_row and (sent_row["total_sent"] or 0) > 0:
                total_sent = int(sent_row["total_sent"] or 0)
                avg_age_sent = float(sent_row["avg_age_hours_sent"]) if sent_row["avg_age_hours_sent"] is not None else None
                fresh_sent = int(sent_row["fresh_sent_count"] or 0)
                fresh_sent_pct = round((fresh_sent / total_sent) * 100.0, 2) if total_sent > 0 else None
                avg_relevance_sent = float(sent_row["avg_relevance_sent"]) if sent_row["avg_relevance_sent"] is not None else None

                results["freshness_sent"] = {
                    "source": "sent_articles",
                    "total_sent": total_sent,
                    "fresh_sent_count": fresh_sent,
                    "fresh_sent_pct": fresh_sent_pct,
                    "avg_age_hours_sent": round(avg_age_sent, 2) if avg_age_sent is not None else None,
                    "avg_relevance_sent": round(avg_relevance_sent, 2) if avg_relevance_sent is not None else None,
                }
                
                log.info(
                    f"Freshness (Sent): {fresh_sent}/{total_sent} sent articles within 12h "
                    f"(avg age={avg_age_sent:.2f}h, avg relevance={avg_relevance_sent:.2f})"
                )
            else:
                log.warning("Freshness (Sent): no sent articles with news_age found")
                results["freshness_sent"] = {
                    "source": "sent_articles",
                    "total_sent": 0,
                    "fresh_sent_count": 0,
                    "fresh_sent_pct": None,
                    "avg_age_hours_sent": None,
                    "avg_relevance_sent": None,
                }

            # --- NEW: Total Summaries Created ---
            log.debug("Computing total summaries created")
            
            cursor = await db.execute(
                """
                SELECT COUNT(*) as total_summaries
                FROM summaries
                WHERE created_at BETWEEN ? AND ?
                """,
                (start, end)
            )
            
            summary_row = await cursor.fetchone()
            total_summaries = int(summary_row["total_summaries"] or 0) if summary_row else 0
            
            results["summaries"] = {
                "total_summaries": total_summaries
            }
            
            log.info(f"Summaries: {total_summaries} created in date range")

            # --- Coverage (unique tickers with summaries vs unique tickers with articles) ---
            log.debug("Computing coverage KPIs")
            
            # Get unique tickers that have articles (no date filter since published_at is NULL)
            cursor = await db.execute(
                """
                SELECT DISTINCT ticker
                FROM articles
                WHERE ticker IS NOT NULL
                  AND ticker != ''
                """
            )
            unique_tickers_with_articles = [row[0] for row in await cursor.fetchall()]
            total_unique_tickers = len(unique_tickers_with_articles)
            
            log.debug(f"Found {total_unique_tickers} unique tickers with articles: {unique_tickers_with_articles}")
            
            # Get tickers that have summaries in the date range
            cursor = await db.execute(
                """
                SELECT DISTINCT ticker
                FROM summaries
                WHERE created_at BETWEEN ? AND ?
                  AND ticker IS NOT NULL
                  AND ticker != ''
                """,
                (start, end)
            )
            covered_tickers = [row[0] for row in await cursor.fetchall()]
            covered_count = len(covered_tickers)
            
            log.debug(f"Found {covered_count} tickers with summaries in range: {covered_tickers}")
            
            # Calculate percentage
            coverage_pct = (covered_count / total_unique_tickers * 100.0) if total_unique_tickers > 0 else 0.0
            
            log.info(f"Coverage: {covered_count}/{total_unique_tickers} unique tickers covered ({coverage_pct:.1f}%)")
            
            results["coverage"] = {
                "total_tickers": total_unique_tickers,
                "covered_tickers": covered_count,
                "coverage_pct": round(coverage_pct, 1)
            }

            # --- Quality Sources ---
            log.debug("Computing quality source KPIs")
            
            from app.core.settings import QUALITY_SOURCES
            
            # Normalize quality sources - strip www. prefix and lowercase
            normalized_quality = set()
            for domain in QUALITY_SOURCES:
                d = domain.lower().strip()
                # Add both with and without www.
                normalized_quality.add(d)
                if d.startswith('www.'):
                    normalized_quality.add(d[4:])  # without www.
                else:
                    normalized_quality.add(f'www.{d}')  # with www.
            
            log.debug(f"Normalized quality sources: {sorted(normalized_quality)}")
            
            # Query with domain normalization in SQL
            cursor = await db.execute(
                """
                SELECT 
                    COUNT(*) AS total_items,
                    SUM(
                        CASE 
                            WHEN LOWER(REPLACE(REPLACE(domain, 'www.', ''), 'WWW.', '')) IN ({placeholders})
                            THEN 1 
                            ELSE 0 
                        END
                    ) AS quality_items
                FROM email_items
                WHERE sent_at BETWEEN ? AND ?
                """.replace('{placeholders}', ','.join('?' * len(normalized_quality))),
                (*[d.replace('www.', '').lower() for d in normalized_quality], start, end)
            )
            quality_row = await cursor.fetchone()
            
            total_items = quality_row[0] or 0
            quality_items = quality_row[1] or 0
            allowlist_pct = (quality_items * 100.0 / total_items) if total_items > 0 else 0.0
            
            log.info(f"Quality: {quality_items}/{total_items} items from quality sources ({allowlist_pct:.1f}%)")
            
            results["quality"] = {
                "total_items": total_items,
                "quality_items": quality_items,
                "allowlist_pct": round(allowlist_pct, 2)
            }

            # --- Coverage (show unique tickers that had articles, not all active tickers) ---
            log.debug("Computing coverage KPIs")
            
            # Get unique tickers that had articles in the date range
            cursor = await db.execute(
                """
                SELECT DISTINCT ticker
                FROM email_items
                WHERE sent_at BETWEEN ? AND ?
                """,
                (start, end)
            )
            unique_tickers_with_articles = [row[0] for row in await cursor.fetchall()]
            total_unique_tickers = len(unique_tickers_with_articles)
            
            # Get tickers that have summaries in the date range
            cursor = await db.execute(
                """
                SELECT DISTINCT ticker
                FROM summaries
                WHERE created_at BETWEEN ? AND ?
                """,
                (start, end)
            )
            covered_tickers = [row[0] for row in await cursor.fetchall()]
            covered_count = len(covered_tickers)
            
            # Calculate percentage (covered / unique tickers with articles)
            coverage_pct = (covered_count / total_unique_tickers * 100.0) if total_unique_tickers > 0 else 0.0
            
            log.info(f"Coverage: {covered_count}/{total_unique_tickers} unique tickers covered ({coverage_pct:.1f}%)")
            
            results["coverage"] = {
                "total_tickers": total_unique_tickers,  # Unique tickers with articles
                "covered_tickers": covered_count,        # Tickers with summaries
                "coverage_pct": round(coverage_pct, 1)
            }

            # --- Vendor Performance ---
            log.debug("Computing vendor performance KPIs")
            
            # Add debug query to check total vendor_metrics count
            cursor = await db.execute(
                "SELECT COUNT(*) FROM vendor_metrics WHERE created_at BETWEEN ? AND ?",
                (start, end)
            )
            total_vendor_rows = (await cursor.fetchone())[0]
            log.debug(f"Total vendor_metrics rows in range: {total_vendor_rows}")
            
            cursor = await db.execute(
                """
                SELECT 
                    provider,
                    event,
                    COUNT(*) as total,
                    SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) as successes,
                    CAST(SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS FLOAT) * 100.0 / COUNT(*) as success_pct,
                    AVG(latency_ms) as avg_latency_ms
                FROM vendor_metrics
                WHERE created_at BETWEEN ? AND ?
                GROUP BY provider, event
                ORDER BY provider, event
                """,
                (start, end)
            )
            vendor_rows = await cursor.fetchall()
            
            log.info(f"compute_kpi_aggregates: found {len(vendor_rows)} vendor metrics (from {total_vendor_rows} total rows)")
            
            vendor_agg = []
            for row in vendor_rows:
                provider, event, total, successes, success_pct, avg_lat = row
                vendor_agg.append({
                    "provider": provider,
                    "event": event,
                    "total": int(total),
                    "successes": int(successes),
                    "success_pct": round(float(success_pct), 2),  # ← Changed to 2 decimals
                    "avg_latency_ms": round(float(avg_lat), 2) if avg_lat else 0  # ← Changed to 2 decimals
                })
            
            results["vendor"] = vendor_agg

            # --- MTTD (Mean Time To Detect) ---
            log.debug("Computing MTTD KPIs")
            
            # Use last 14 days for MTTD calculation — ensure ISO Z suffix
            end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
            mttd_start_dt = end_dt - timedelta(days=14)
            mttd_start = mttd_start_dt.replace(tzinfo=None).isoformat() + "Z"
            mttd_result = await _compute_mttd(db, mttd_start, end)
            
            results["mttd"] = mttd_result
            
            log.info(f"MTTD: {mttd_result.get('avg_minutes')} min avg detection time ({mttd_result.get('failures')} failures, {mttd_result.get('recovered')} recovered)")

            # --- MTTR (Mean Time To Resolve) - NEW ---
            log.debug("Computing MTTR KPIs")
            
            # Use same 14-day window for MTTR — ensure ISO Z suffix
            mttr_start = mttd_start
            mttr_result = await _compute_mttr(db, mttr_start, end)
            
            # Map mttr_result to the requested JSON shape (avg/minor/major)
            results["mttr"] = {
                "avg_minutes": mttr_result.get("avg_minutes"),
                "minor": mttr_result.get("minor", 0),
                "major": mttr_result.get("major", 0),
                "unresolved": mttr_result.get("unresolved", 0),
                "total_incidents": mttr_result.get("total_incidents", 0),
            }
            
            log.info(
                f"MTTR: {results['mttr'].get('avg_minutes')} min avg resolution time "
                f"(minor={results['mttr'].get('minor')}, major={results['mttr'].get('major')}, "
                f"unresolved={results['mttr'].get('unresolved')})"
            )

        total_time = (time.time() - t_start) * 1000
        log.info(f"kpi_aggregates: TOTAL time {total_time:.0f}ms")
        log.info("compute_kpi_aggregates: completed successfully with %d vendor metrics", len(results["vendor"]))
        return {
            "ok": True,
            "start": start,
            "end": end,
            "results": results,
        }
        
    except Exception as e:
        log.error(f"kpi_aggregates: ERROR after {(time.time() - t_start)*1000:.0f}ms: {e}", exc_info=True)
        log.exception("compute_kpi_aggregates: failed")
        raise


async def _compute_avg_age_hours(db: aiosqlite.Connection, start: str, end: str) -> float | None:
    """
    Compute average age of articles in hours (sent_at - published_at).
    Fallback: if DB returns 0 or None, sample recent rows and compute in Python (handles date formats).
    """
    def _parse_iso(s: str) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace('Z', '+00:00'))
        except Exception:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
        return None

    try:
        cursor = await db.execute(
            """
            SELECT AVG((julianday(sent_at) - julianday(published_at)) * 24.0) AS avg_hours,
                   COUNT(*) as cnt
            FROM email_items
            WHERE sent_at BETWEEN ? AND ? 
              AND published_at IS NOT NULL
            """,
            (start, end)
        )
        row = await cursor.fetchone()
        db_avg = row[0] if row else None
        db_cnt = row[1] if row else 0

        # If DB produced a positive avg and there are rows, accept it.
        if db_avg is not None and float(db_avg) > 0 and db_cnt > 0:
            return round(float(db_avg), 2)

        # Fallback: sample up to 200 rows and compute in Python to avoid DB date-format edge cases
        cursor = await db.execute(
            """
            SELECT sent_at, published_at
            FROM email_items
            WHERE sent_at BETWEEN ? AND ?
              AND published_at IS NOT NULL
            ORDER BY sent_at DESC
            LIMIT 200
            """,
            (start, end)
        )
        rows = await cursor.fetchall()
        if not rows:
            return None

        diffs = []
        for r in rows:
            sent = r["sent_at"]
            pub = r["published_at"]
            sdt = _parse_iso(sent)
            pdt = _parse_iso(pub)
            if not sdt or not pdt:
                continue
            # compute positive difference in hours (sent - published)
            diff_h = (sdt - pdt).total_seconds() / 3600.0
            if diff_h >= 0:
                diffs.append(diff_h)

        if not diffs:
            log.debug("_compute_avg_age_hours: sampled rows but no valid diffs found")
            return None

        avg_h = sum(diffs) / len(diffs)
        log.debug("_compute_avg_age_hours: fallback computed avg_hours=%.2f from %d samples", avg_h, len(diffs))
        return round(avg_h, 2)

    except Exception as e:
        log.warning("_compute_avg_age_hours: failed - %s", e)
        return None


async def _compute_mttd(db: aiosqlite.Connection, start: str, end: str) -> dict:
    """
    Compute Mean Time To Detect (MTTD) - average time from vendor failure to next success.
    
    Filters out gaps longer than MTTD_MAX_GAP_MINUTES to avoid skewing by
    overnight/weekend downtime where system wasn't actively running.
    
    Args:
        db: Active database connection
        start: Start datetime string (14 days ago recommended)
        end: End datetime string
        
    Returns:
        Dictionary with avg_minutes, failures, recovered, and excluded_count
    """
    from app.core.settings import MTTD_MAX_GAP_MINUTES
    
    try:
        # Get all failures in the time range
        cursor = await db.execute(
            """
            SELECT id, provider, event, created_at
            FROM vendor_metrics
            WHERE created_at BETWEEN ? AND ?
              AND ok = 0
            ORDER BY provider, event, created_at
            """,
            (start, end)
        )
        failures = await cursor.fetchall()
        
        if not failures:
            log.info("_compute_mttd: no failures found in range")
            return {
                "avg_minutes": None,
                "failures": 0,
                "recovered": 0,
                "excluded": 0,
                "max_gap_minutes": MTTD_MAX_GAP_MINUTES
            }
        
        detection_times = []
        excluded_long_gaps = 0
        
        for failure in failures:
            failure_id, provider, event, failure_time = failure
            
            # Find next success for same provider/event after this failure
            cursor = await db.execute(
                """
                SELECT created_at
                FROM vendor_metrics
                WHERE provider = ?
                  AND event = ?
                  AND created_at > ?
                  AND ok = 1
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (provider, event, failure_time)
            )
            success_row = await cursor.fetchone()
            
            if success_row:
                success_time = success_row[0]
                
                # Parse timestamps and compute difference in minutes
                try:
                    failure_dt = datetime.fromisoformat(failure_time.replace('Z', '+00:00'))
                    success_dt = datetime.fromisoformat(success_time.replace('Z', '+00:00'))
                    diff_minutes = (success_dt - failure_dt).total_seconds() / 60.0
                    
                    # Filter out gaps longer than max (likely system downtime)
                    if diff_minutes <= MTTD_MAX_GAP_MINUTES:
                        detection_times.append(diff_minutes)
                        log.debug(f"_compute_mttd: {provider}/{event} recovered in {diff_minutes:.1f} min ✓")
                    else:
                        excluded_long_gaps += 1
                        log.debug(f"_compute_mttd: {provider}/{event} gap {diff_minutes:.1f} min > {MTTD_MAX_GAP_MINUTES} min (excluded)")
                        
                except Exception as e:
                    log.warning(f"_compute_mttd: failed to parse timestamps: {e}")
                    continue
        
        if detection_times:
            avg_minutes = sum(detection_times) / len(detection_times)
            log.info(
                f"_compute_mttd: avg={avg_minutes:.1f} min from {len(detection_times)} recoveries "
                f"(out of {len(failures)} failures, {excluded_long_gaps} excluded as > {MTTD_MAX_GAP_MINUTES} min)"
            )
            return {
                "avg_minutes": round(avg_minutes, 1),
                "failures": len(failures),
                "recovered": len(detection_times),
                "excluded": excluded_long_gaps,
                "max_gap_minutes": MTTD_MAX_GAP_MINUTES
            }
        else:
            log.warning(
                f"_compute_mttd: {len(failures)} failures but no recoveries within {MTTD_MAX_GAP_MINUTES} min window "
                f"({excluded_long_gaps} excluded as too long)"
            )
            return {
                "avg_minutes": None,
                "failures": len(failures),
                "recovered": 0,
                "excluded": excluded_long_gaps,
                "max_gap_minutes": MTTD_MAX_GAP_MINUTES
            }
            
    except Exception as e:
        log.error(f"_compute_mttd: error: {e}", exc_info=True)
        return {
            "avg_minutes": None,
            "failures": 0,
            "recovered": 0,
            "excluded": 0,
            "max_gap_minutes": MTTD_MAX_GAP_MINUTES
        }


async def _compute_mttr(db: aiosqlite.Connection, start: str, end: str) -> dict:
    """
    Compute MTTR: average resolved duration (minutes) plus counts of minor/major/unresolved.
    Considers incidents where created_at OR resolved_at falls inside [start, end].
    """
    def _parse_iso(s: str) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace('Z', '+00:00'))
        except Exception:
            # Fallback common formats
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except Exception:
                    continue
        return None

    try:
        # ensure table exists
        cur = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='run_errors'")
        exists = await cur.fetchone() is not None
        await cur.close()
        if not exists:
            logging.getLogger("ari.metrics").warning("_compute_mttr: run_errors table not present")
            return {"avg_minutes": None, "minor": 0, "major": 0, "unresolved": 0, "total_incidents": 0}

        # select rows where created_at or resolved_at is in window
        q = """
            SELECT id, created_at, resolved_at, provider, event, job_type
            FROM run_errors
            WHERE (created_at BETWEEN ? AND ?)
               OR (resolved_at BETWEEN ? AND ?)
            ORDER BY created_at DESC
        """
        cur = await db.execute(q, (start, end, start, end))
        rows = await cur.fetchall()
        await cur.close()

        if not rows:
            return {"avg_minutes": None, "minor": 0, "major": 0, "unresolved": 0, "total_incidents": 0}

        total_incidents = len(rows)
        resolved_durations = []
        minor = 0
        major = 0
        unresolved = 0

        for row in rows:
            created_at = row["created_at"]
            resolved_at = row["resolved_at"]

            if not resolved_at:
                unresolved += 1
                continue

            cdt = _parse_iso(created_at)
            rdt = _parse_iso(resolved_at)
            if not cdt or not rdt:
                continue

            duration_minutes = (rdt - cdt).total_seconds() / 60.0
            if duration_minutes < 0:
                # skip bad data
                continue

            resolved_durations.append(duration_minutes)
            if duration_minutes < 30:
                minor += 1
            else:
                major += 1

        if resolved_durations:
            avg_minutes = round(sum(resolved_durations) / len(resolved_durations), 1)
        else:
            avg_minutes = None

        return {
            "avg_minutes": avg_minutes,
            "minor": minor,
            "major": major,
            "unresolved": unresolved,
            "total_incidents": total_incidents
        }

    except Exception as e:
        logging.getLogger("ari.metrics").exception("_compute_mttr error: %s", e)
        return {"avg_minutes": None, "minor": 0, "major": 0, "unresolved": 0, "total_incidents": 0}


def compute_vendor_metrics(start: str, end: str) -> list[dict]:
    """
    Compute vendor performance metrics.
    """
    log.info(f"compute_vendor_metrics: querying from {start} to {end}")
    
    from app.core.cache import CACHE_DB_PATH
    import sqlite3
    
    try:
        with sqlite3.connect(CACHE_DB_PATH, timeout=5) as conn:
            cur = conn.cursor()
            
            # Check what event names are actually in the database
            cur.execute("""
                SELECT DISTINCT provider, event 
                FROM vendor_metrics 
                WHERE created_at >= ? AND created_at <= ?
                ORDER BY provider, event
            """, (start, end))
            
            distinct_events = cur.fetchall()
            log.info(f"compute_vendor_metrics: found distinct provider/event pairs: {distinct_events}")
            
            # Your existing query will work fine - it groups by whatever is in the DB
            cur.execute("""
                SELECT 
                    provider,
                    event,
                    COUNT(*) as total,
                    SUM(ok) as success,
                    ROUND(SUM(ok) * 100.0 / COUNT(*), 2) as success_pct,
                    ROUND(AVG(latency_ms), 1) as avg_latency_ms
                FROM vendor_metrics
                WHERE created_at >= ? AND created_at <= ?
                GROUP BY provider, event
                ORDER BY provider, event
            """, (start, end))
            
            rows = cur.fetchall()
            log.info(f"compute_vendor_metrics: returning {len(rows)} vendor metrics")
            
            results = []
            for row in rows:
                results.append({
                    "provider": row[0],
                    "event": row[1],
                    "total": row[2],
                    "success": row[3],
                    "success_pct": row[4],
                    "avg_latency_ms": row[5]
                })
            
            return results
    except Exception as e:
        log.error(f"compute_vendor_metrics: error: {e}", exc_info=True)
        return []