from __future__ import annotations
import sys
import logging
from contextlib import asynccontextmanager

import aiosqlite
import os

# Configure logging early
logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
logging.getLogger().handlers[0].flush = sys.stdout.flush
log = logging.getLogger("ari")

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app.core.settings import settings
from app.api.admin import jobs as admin_jobs
from app.api.admin import metrics as admin_metrics
from app.api.admin import email as admin_email
from app.api.admin import retry_stats as admin_retry_stats
from app.routes import dashboard_metrics
from app.api.admin.cache import router as cache_router
from app.api.admin.errors import router as errors_router

# =====================================================================
# DATABASE INITIALIZATION (FIXED VERSION)
# =====================================================================

DATABASE_URL = os.getenv("DATABASE_URL")
SQLITE_PATH = os.getenv("SQLITE_PATH", "./ari.db")

from app.db.connection import engine

# =====================================================================
# SQLITE HELPERS
# =====================================================================

async def _db_path() -> str:
    return SQLITE_PATH

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
            return int(n or 0)
    except Exception as e:
        log.error(f"count_articles_rows failed: {e}")
        return 0

async def count_summaries_rows() -> int:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            if not await _table_exists(db, "summaries"):
                return 0
            cur = await db.execute("SELECT COUNT(*) FROM summaries")
            (n,) = await cur.fetchone()
            return int(n or 0)
    except Exception as e:
        log.error(f"count_summaries_rows failed: {e}")
        return 0

async def purge_expired(ttl_days: int = 7) -> int:
    deleted = 0
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            for tbl, col in [("articles", "created_at"), ("summaries", "created_at")]:
                if not await _table_exists(db, tbl):
                    continue
                q = f"DELETE FROM {tbl} WHERE {col} < datetime('now', ?)"
                cur = await db.execute(q, (f"-{ttl_days} days",))
                deleted += cur.rowcount or 0
            await db.commit()
    except Exception as e:
        log.error(f"purge_expired failed: {e}")
    return deleted

__all__ = ["count_articles_rows", "count_summaries_rows", "purge_expired"]

# =====================================================================
# SCHEDULER + MIGRATIONS
# =====================================================================

from contextlib import suppress
from app.db.migrations.add_run_errors import migrate_add_run_errors
from app.db.migrations.add_news_age_column import migrate_add_news_age_column
from app.db.migrations.link_summaries_to_articles import migrate_link_summaries_to_articles

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Application startup")

    # Only run SQLite migrations when we are actually using SQLite
    if not DATABASE_URL:
        try:
            db_path = SQLITE_PATH
            await migrate_add_run_errors(db_path)
            await migrate_add_news_age_column(db_path)
            await migrate_link_summaries_to_articles(db_path)
            log.info("Migrations completed successfully")
        except Exception as e:
            log.error(f"Migration failed: {e}")
    else:
        log.info("DATABASE_URL is set; skipping SQLite migrations (Neon/Postgres mode).")

    yield
    log.info("Application shutdown")

# =====================================================================
# FASTAPI APP + ROUTES
# =====================================================================

app = FastAPI(
    title="ARI API",
    description="Asset Relevance Intelligence API",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(admin_jobs.router, prefix="/admin/jobs", tags=["Admin"])
app.include_router(admin_metrics.router, prefix="/admin/metrics", tags=["Admin"])
app.include_router(admin_email.router, prefix="/admin/email", tags=["Admin"])
app.include_router(admin_retry_stats.router, prefix="/admin/retry", tags=["Admin"])
app.include_router(cache_router, prefix="/admin/cache", tags=["Cache"])
app.include_router(errors_router, prefix="/admin", tags=["Admin"])
app.include_router(dashboard_metrics.router)

from app.api import debug as debug_api
from app.api import ui as ui_routes

app.include_router(debug_api.router, prefix="/debug", tags=["debug"])
app.include_router(ui_routes.router, tags=["ui"])

@app.get("/")
def root():
    return RedirectResponse(url="/docs")

@app.get("/debug/routes")
async def list_routes():
    return {
        "routes": [
            {
                "path": r.path,
                "name": r.name,
                "methods": list(getattr(r, "methods", [])),
            }
            for r in app.routes
        ]
    }