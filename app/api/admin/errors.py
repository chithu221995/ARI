"""
Admin endpoint to view recent run errors.
"""
from __future__ import annotations
import logging
from typing import List, Dict, Any
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import aiosqlite

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.admin.errors")
router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/errors", response_class=HTMLResponse)
async def view_errors(request: Request):
    """
    Display recent errors from run_errors table.
    Shows last 50 errors ordered by created_at DESC.
    """
    errors: List[Dict[str, Any]] = []
    debug_info = None
    
    try:
        async with aiosqlite.connect(CACHE_DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            
            # Check if table exists
            cursor = await db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='run_errors'"
            )
            table_exists = await cursor.fetchone() is not None
            await cursor.close()
            
            if not table_exists:
                log.warning("view_errors: run_errors table does not exist")
                return templates.TemplateResponse(
                    "errors.html",
                    {
                        "request": request,
                        "errors": [],
                        "error_message": "run_errors table not yet created. Run migration first.",
                        "debug_info": None
                    }
                )
            
            # Get total count for debug
            cursor = await db.execute("SELECT COUNT(*) as total FROM run_errors")
            row = await cursor.fetchone()
            total_count = row["total"] if row else 0
            await cursor.close()
            
            # Fetch recent errors
            cursor = await db.execute(
                """
                SELECT 
                    id,
                    job_type,
                    provider,
                    event,
                    error_message,
                    created_at,
                    resolved_at,
                    resolved_by
                FROM run_errors
                ORDER BY created_at DESC
                LIMIT 50
                """
            )
            
            rows = await cursor.fetchall()
            await cursor.close()
            
            debug_info = f"Total errors in DB: {total_count}, Showing: {len(rows)}"
            log.info(f"view_errors: {debug_info}")
            
            for row in rows:
                error_msg = row["error_message"] or ""
                # Truncate long messages
                if len(error_msg) > 120:
                    error_msg = error_msg[:120] + "..."
                
                errors.append({
                    "id": row["id"],
                    "job_type": row["job_type"] or "N/A",
                    "provider": row["provider"] or "N/A",
                    "event": row["event"] or "N/A",
                    "message": error_msg,
                    "created_at": row["created_at"],
                    "resolved_at": row["resolved_at"],
                    "resolved_by": row["resolved_by"],
                    "is_resolved": row["resolved_at"] is not None
                })
        
        log.info(f"view_errors: returning {len(errors)} recent errors")
        
    except Exception as e:
        log.exception("view_errors: failed to fetch errors")
        return templates.TemplateResponse(
            "errors.html",
            {
                "request": request,
                "errors": [],
                "error_message": f"Failed to fetch errors: {str(e)}",
                "debug_info": None
            }
        )
    
    return templates.TemplateResponse(
        "errors.html",
        {
            "request": request,
            "errors": errors,
            "error_message": None if errors else "No errors found in database.",
            "debug_info": debug_info
        }
    )