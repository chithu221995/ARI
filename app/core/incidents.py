"""
Incident tracking helpers for MTTR metrics.
"""
from __future__ import annotations
import logging
from datetime import datetime
from typing import Optional
import aiosqlite

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.incidents")


async def record_incident(
    job_type: str,
    error_message: str,
    provider: Optional[str] = None,
    event: Optional[str] = None,
    ticker: Optional[str] = None,
    db_path: str = CACHE_DB_PATH
) -> int:
    """
    Record a new incident in run_errors table.
    
    Args:
        job_type: Type of job that failed (e.g., "fetch", "extract", "summarize")
        error_message: Description of the error
        provider: Optional provider name (e.g., "diffbot", "gemini")
        event: Optional event type (e.g., "extract", "summarize")
        ticker: Optional ticker symbol context
        db_path: Database path
        
    Returns:
        ID of inserted incident row
    """
    created_at = datetime.utcnow().isoformat() + "Z"
    
    try:
        async with aiosqlite.connect(db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO run_errors 
                (job_type, ticker, provider, event, error_message, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (job_type, ticker, provider, event, error_message, created_at)
            )
            incident_id = cursor.lastrowid
            await db.commit()
        
        log.info(
            f"record_incident: recorded {job_type}/{provider or 'N/A'} "
            f"incident #{incident_id}"
        )
        return incident_id
        
    except Exception as e:
        log.exception("record_incident: failed to record incident")
        return -1


async def resolve_incident(
    job_type: str,
    provider: Optional[str] = None,
    event: Optional[str] = None,
    ticker: Optional[str] = None,
    resolved_by: str = "auto",
    db_path: str = CACHE_DB_PATH
) -> int:
    """
    Mark the most recent unresolved incident as resolved.
    
    Args:
        job_type: Type of job that recovered
        provider: Optional provider name
        event: Optional event type
        ticker: Optional ticker symbol
        resolved_by: How it was resolved (e.g., "auto", "retry", "manual")
        db_path: Database path
        
    Returns:
        Number of incidents resolved (0 or 1)
    """
    resolved_at = datetime.utcnow().isoformat() + "Z"
    
    try:
        async with aiosqlite.connect(db_path) as db:
            # Build WHERE clause based on provided context
            where_parts = ["resolved_at IS NULL", "job_type = ?"]
            params = [job_type]
            
            if provider:
                where_parts.append("provider = ?")
                params.append(provider)
            
            if event:
                where_parts.append("event = ?")
                params.append(event)
            
            if ticker:
                where_parts.append("ticker = ?")
                params.append(ticker)
            
            where_clause = " AND ".join(where_parts)
            
            # Update most recent unresolved incident
            cursor = await db.execute(
                f"""
                UPDATE run_errors
                SET resolved_at = ?, resolved_by = ?
                WHERE id = (
                    SELECT id FROM run_errors
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                """,
                (resolved_at, resolved_by, *params)
            )
            
            resolved_count = cursor.rowcount
            await db.commit()
        
        if resolved_count > 0:
            log.info(
                f"resolve_incident: resolved {job_type}/{provider or 'N/A'} "
                f"incident (method={resolved_by})"
            )
        
        return resolved_count
        
    except Exception as e:
        log.exception("resolve_incident: failed to resolve incident")
        return 0


async def get_unresolved_incidents(
    db_path: str = CACHE_DB_PATH,
    limit: int = 50
) -> list[dict]:
    """
    Get list of unresolved incidents.
    
    Args:
        db_path: Database path
        limit: Maximum number of incidents to return
        
    Returns:
        List of unresolved incident dicts
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute(
                """
                SELECT 
                    id, job_type, ticker, provider, event,
                    error_message, created_at
                FROM run_errors
                WHERE resolved_at IS NULL
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,)
            )
            
            rows = await cursor.fetchall()
            await cursor.close()
        
        incidents = []
        for row in rows:
            created_dt = datetime.fromisoformat(row["created_at"].replace('Z', '+00:00'))
            age_minutes = (datetime.utcnow() - created_dt.replace(tzinfo=None)).total_seconds() / 60.0
            
            incidents.append({
                "id": row["id"],
                "job_type": row["job_type"],
                "ticker": row["ticker"],
                "provider": row["provider"],
                "event": row["event"],
                "error_message": row["error_message"],
                "created_at": row["created_at"],
                "age_minutes": round(age_minutes, 1)
            })
        
        return incidents
        
    except Exception as e:
        log.exception("get_unresolved_incidents: failed")
        return []