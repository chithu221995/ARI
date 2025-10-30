from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from fastapi import FastAPI
from app.api import router as api_router

app = FastAPI(title="ARI")
app.include_router(api_router)

# (startup handler must be registered after app = FastAPI())
# (moved further down)

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from fastapi import FastAPI
import traceback

# import composed admin package (single admin router)
from app.api.admin import admin as admin_pkg
# create the FastAPI app before mounting routers
app = FastAPI(title="A.R.I. Engine")
app.include_router(admin_pkg)

# v1 routers
from app.api.v1 import brief as v1_brief
from app.api.v1 import summary as v1_summary

# mount v1 routers
app.include_router(v1_brief.router)        # /api/v1/brief
app.include_router(v1_summary.router)      # /api/v1/summarize (GET+POST)

# debug API
from app.api import debug as debug_api
# include debug router once under /debug (guard checks healthz specifically)
if not any(getattr(r, "path", "") == "/debug/healthz" for r in app.router.routes):
    app.include_router(debug_api.router, prefix="/debug")

# database initialization helpers (kept)
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