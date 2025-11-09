from __future__ import annotations
import sys
import logging
from contextlib import asynccontextmanager

import aiosqlite
import os

# Configure logging early to ensure all logs are captured
logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
logging.getLogger().handlers[0].flush = sys.stdout.flush  # ensure flush after each write

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app.core.settings import settings
# Import admin routers
from app.api.admin import jobs as admin_jobs
from app.api.admin import metrics as admin_metrics
from app.api.admin import email as admin_email
from app.api.admin import retry_stats as admin_retry_stats
from app.routes import dashboard_metrics
from app.api.admin.cache import router as cache_router
from app.api.admin.errors import router as errors_router

# DB connection helpers: prefer DATABASE_URL (e.g. postgres) otherwise use local sqlite path
DATABASE_URL = os.getenv("DATABASE_URL")
SQLITE_PATH = os.getenv("SQLITE_PATH", "./ari.db")

# If DATABASE_URL is provided, create an async SQLAlchemy engine for that DB.
# Otherwise continue using aiosqlite throughout the codebase (local dev).
engine = None
if DATABASE_URL:
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine(DATABASE_URL, future=True)
else:
    engine = None

# database initialization helpers (kept)
log = logging.getLogger("ari.cache")

async def _db_path() -> str:
    return os.getenv("SQLITE_PATH", "./ari.db")

async def _table_exists(db: aiosqlite.Connection, name: str) -> bool:
    cur = await db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    )
    row = await cur.fetchone()
    await cur.close()
    return bool(row)

async def count_articles_rows() -> int:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            if not await _table_exists(db, "articles"):
                return 0
            cur = await db.execute("SELECT COUNT(*) FROM articles")
            (n,) = await cur.fetchone()
            await cur.close()
            return int(n or 0)
    except Exception as e:
        log.error("count_articles_rows failed: %s", e)
        return 0

async def count_summaries_rows() -> int:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            if not await _table_exists(db, "summaries"):
                return 0
            cur = await db.execute("SELECT COUNT(*) FROM summaries")
            (n,) = await cur.fetchone()
            await cur.close()
            return int(n or 0)
    except Exception as e:
        log.error("count_summaries_rows failed: %s", e)
        return 0

async def purge_expired(ttl_days: int = 7) -> int:
    """Delete cache rows older than ttl_days from known cache tables; returns total deleted."""
    deleted = 0
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            for tbl, col in [("articles", "created_at"), ("summaries", "created_at")]:
                if not await _table_exists(db, tbl):
                    continue
                q = f"DELETE FROM {tbl} WHERE {col} < datetime('now', ?)"
                cur = await db.execute(q, (f"-{ttl_days} days",))
                deleted += cur.rowcount if cur.rowcount is not None else 0
            await db.commit()
    except Exception as e:
        log.error("purge_expired failed: %s", e)
    return deleted

# make sure these are exported
__all__ = [
    "count_articles_rows",
    "count_summaries_rows",
    "purge_expired",
]

# --- Optional Lightweight Scheduler (guarded by RUN_SCHEDULER env var) ---
import os
from contextlib import suppress

if os.getenv("RUN_SCHEDULER", "0") in {"1", "true", "yes"}:
    # Minimal cron using asyncio.create_task + sleep loops to keep dependencies small
    import asyncio
    from datetime import datetime, time, timedelta, timezone
    from app.pipeline.daily import run_daily_fanout
    import logging
    
    log = logging.getLogger("ari.scheduler")

    async def _sleep_until(ts: datetime):
        now = datetime.now(timezone.utc)
        delay = (ts - now).total_seconds()
        await asyncio.sleep(max(0, delay))

    async def _next_ist(hour: int, minute: int = 0):
        # IST = UTC+5:30
        ist = timezone(timedelta(hours=5, minutes=30))
        now_ist = datetime.now(ist)
        target = datetime.combine(now_ist.date(), time(hour, minute), tzinfo=ist)
        if target <= now_ist:
            target += timedelta(days=1)
        return target.astimezone(timezone.utc)

    async def scheduler_loop():
        while True:
            # Example: 06:00 IST daily
            target_utc = await _next_ist(6, 0)
            log.info("scheduler: next run at %s", target_utc.isoformat())
            await _sleep_until(target_utc)
            with suppress(Exception):
                await run_daily_fanout()

from app.db.migrations.add_run_errors import migrate_add_run_errors
from app.db.migrations.add_news_age_column import migrate_add_news_age_column
from app.db.migrations.link_summaries_to_articles import migrate_link_summaries_to_articles

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    log.info("Application startup")
    
    # Run migrations
    try:
        db_path = os.getenv("SQLITE_PATH", "./ari.db")
        await migrate_add_run_errors(db_path)
        await migrate_add_news_age_column(db_path)
        await migrate_link_summaries_to_articles(db_path)  # NEW
        log.info("Migrations completed successfully")
    except Exception as e:
        log.error(f"Migration failed: {e}")
    
    # Start scheduler if enabled
    if os.getenv("RUN_SCHEDULER", "0") in {"1", "true", "yes"}:
        asyncio.create_task(scheduler_loop())
        log.info("scheduler: started")
    
    yield
    # Shutdown
    log.info("Application shutdown")

# Create FastAPI app
app = FastAPI(
    title="ARI API",
    description="Asset Relevance Intelligence API",
    version="1.0.0",
    lifespan=lifespan
)

# ============================================================================
# CANONICAL ADMIN ROUTES (included in OpenAPI schema)
# ============================================================================
app.include_router(admin_jobs.router, prefix="/admin/jobs", tags=["Admin"])
app.include_router(admin_metrics.router, prefix="/admin/metrics", tags=["Admin"])
app.include_router(admin_email.router, prefix="/admin/email", tags=["Admin"])
app.include_router(admin_retry_stats.router, prefix="/admin/retry", tags=["Admin"])
app.include_router(cache_router, prefix="/admin/cache", tags=["Cache"])
app.include_router(errors_router, prefix="/admin", tags=["Admin"])

# Register dashboard routes
app.include_router(dashboard_metrics.router)

# ============================================================================
# PUBLIC ALIASES (not included in schema to avoid duplicates)
# ============================================================================
# Optional: provide backward-compatible /jobs/* endpoints without schema duplication
app.include_router(admin_jobs.router, prefix="/jobs", include_in_schema=False)

# ============================================================================
# DEBUG & UI ROUTES
# ============================================================================
from app.api import debug as debug_api
app.include_router(debug_api.router, prefix="/debug", tags=["debug"])

from app.api import ui as ui_routes
app.include_router(ui_routes.router, tags=["ui"])

# ============================================================================
# ROOT REDIRECT
# ============================================================================
@app.get("/")
def root():
    """Root endpoint - redirects to docs"""
    return RedirectResponse(url="/docs")

@app.get("/debug/routes")
async def list_routes():
    """Debug endpoint to list all registered routes"""
    routes = []
    for route in app.routes:
        if hasattr(route, "path"):
            routes.append({
                "path": route.path,
                "name": route.name,
                "methods": list(route.methods) if hasattr(route, "methods") else []
            })
    return {"routes": routes}