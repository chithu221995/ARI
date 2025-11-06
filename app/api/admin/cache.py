from __future__ import annotations
import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import aiosqlite

from app.core.cache import CACHE_DB_PATH

log = logging.getLogger("ari.admin.cache")
router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/diag", response_class=HTMLResponse)
async def cache_diagnostics(request: Request):
    """
    Display cache diagnostics page with table stats.
    """
    stats = {}
    error_message = None
    
    try:
        async with aiosqlite.connect(CACHE_DB_PATH) as db:
            # Get counts for each table
            tables = ["articles", "summaries", "email_items", "email_events", 
                     "metrics", "vendor_metrics", "run_errors"]
            
            for table in tables:
                try:
                    cursor = await db.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                    row = await cursor.fetchone()
                    stats[table] = row[0] if row else 0
                    await cursor.close()
                except Exception as e:
                    log.warning(f"Could not get count for {table}: {e}")
                    stats[table] = "N/A"
            
            # Get database size
            try:
                cursor = await db.execute(
                    "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
                )
                row = await cursor.fetchone()
                stats["db_size_mb"] = round(row[0] / (1024 * 1024), 2) if row else 0
                await cursor.close()
            except Exception as e:
                log.warning(f"Could not get DB size: {e}")
                stats["db_size_mb"] = "N/A"
    
    except Exception as e:
        log.exception("cache_diagnostics: failed")
        error_message = f"Failed to fetch cache stats: {str(e)}"
    
    return templates.TemplateResponse(
        "cache_diagnostics.html",
        {
            "request": request,
            "stats": stats,
            "error_message": error_message
        }
    )