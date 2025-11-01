from __future__ import annotations
import sys
import logging
from contextlib import asynccontextmanager

# Configure logging early to ensure all logs are captured
logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
logging.getLogger().handlers[0].flush = sys.stdout.flush  # ensure flush after each write

import aiosqlite
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app.core.settings import settings
from app.api import router as api_router
from app.api.admin import jobs as jobs_admin

# create the FastAPI app before mounting routers
app = FastAPI(title="A.R.I. Engine")
app.include_router(api_router)
# app.include_router(admin_pkg)

# v1 routers
# from app.api.v1 import brief as v1_brief
# from app.api.v1 import summary as v1_summary

# mount v1 routers
# app.include_router(v1_brief.router)        # /api/v1/brief
app.include_router(api_router)      # /api/v1/summarize (GET+POST)

# debug API
from app.api import debug as debug_api
# include debug router once under /debug (guard checks healthz specifically)
if not any(getattr(r, "path", "") == "/debug/healthz" for r in app.router.routes):
    app.include_router(debug_api.router, prefix="/debug")

# UI router
from app.api import ui as ui_routes

# Include UI router (place after other includes to avoid duplication)
if not any(getattr(r, "path", "") == "/" for r in app.router.routes if hasattr(r, "path")):
    app.include_router(ui_routes.router)

# database initialization helpers (kept)
import os
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

# keep /debug router if present (already included above)
try:
    from app.api.debug import router as debug_router  # type: ignore
    if not any(getattr(r, "path", "").startswith("/debug") for r in app.router.routes):
        app.include_router(debug_router, prefix="/debug")
except Exception:
    pass

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

    @app.on_event("startup")
    async def _start_scheduler():
        asyncio.create_task(scheduler_loop())
        log.info("scheduler: started")