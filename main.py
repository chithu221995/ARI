# filepath: /Users/chitharanjan/ARI/main.py
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import logging
from app.core.cache import init_db
log = logging.getLogger("ari.main")

# (startup handler must be registered after app = FastAPI())
# (moved further down)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from fastapi import FastAPI
import traceback

app = FastAPI(title="A.R.I. Engine")

# register DB init on startup (must run after app exists)
@app.on_event("startup")
async def _startup_init_db() -> None:
    try:
        log.info("startup: initializing cache DB schema")
        await init_db()
        log.info("startup: cache DB initialized")
    except Exception:
        log.exception("startup: init_db failed")

# --- logging bootstrap (append-only) ---
import logging, os, sys
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
log = logging.getLogger("ari.main")
log.info("logging configured")
# --- end logging bootstrap ---

# v1 routers
from app.api.v1 import brief as v1_brief
from app.api.v1 import summary as v1_summary

# admin sub-routers (each router should NOT include "/admin" in its own prefix)
from app.api.admin import email as admin_email
from app.api.admin import cache as admin_cache
from app.api.admin import jobs as admin_jobs
from app.api.admin import cache_diag

# mount routers
app.include_router(v1_brief.router)        # /api/v1/brief
app.include_router(v1_summary.router)      # /api/v1/summarize (GET+POST)

# mount admin subrouters under /admin
app.include_router(admin_email.router, prefix="/admin")   # /admin/email/brief
app.include_router(admin_cache.router, prefix="/admin")   # /admin/cache/...
app.include_router(admin_jobs.router, prefix="/admin")    # /admin/jobs/...
app.include_router(cache_diag.router)  # /admin/cache_diag

# debug API
from app.api import debug as debug_api
# include debug router once under /debug (guard checks healthz specifically)
if not any(getattr(r, "path", "") == "/debug/healthz" for r in app.router.routes):
    app.include_router(debug_api.router, prefix="/debug")

# database initialization
import os
import aiosqlite
import logging
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

# admin jobs are mounted above; removed duplicate remount logic to avoid double-mounting
# (previous try/except remount block deleted)

# keep /debug router if present
try:
    from app.api.debug import router as debug_router  # type: ignore
    if not any(getattr(r, "path", "").startswith("/debug") for r in app.router.routes):
        app.include_router(debug_router, prefix="/debug")
except Exception:
    pass

# include admin routers — do not re-add an "/admin" prefix here because routers already use it
app.include_router(admin_cache.router)
app.include_router(admin_jobs.router)
app.include_router(admin_email.router)
app.include_router(cache_diag.router)

# make sure these are exported
__all__ = [
    "count_articles_rows",
    "count_summaries_rows",
    "purge_expired",
]