from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
import logging

from app.ingest.extract import extract_via_diffbot

# new imports for DB init
from app.core.cache import ensure_phase4_user_catalog_schemas, CACHE_DB_PATH, load_ticker_catalog_from_file
from app.cache.db_init import init_all_tables

log = logging.getLogger("ari.admin.cache")
router = APIRouter(tags=["admin:cache"])


@router.get("/ping-extract")
async def ping_extract(url: str = Query(..., description="URL to probe with Diffbot extractor"), timeout_s: int = Query(8)):
    """
    Test endpoint to exercise the extractor on a single URL.
    Returns a simple success flag and character count.
    """
    try:
        text = await extract_via_diffbot(url, timeout_s=timeout_s)
        return {"ok": bool(text), "chars": len(text or "")}
    except Exception:
        log.exception("cache_diag.ping_extract failed for url=%s", url)
        return {"ok": False, "chars": 0}


@router.post("/db/init")
async def db_init():
    """
    Initialize all DB schemas including feedback tables.
    """
    import aiosqlite

    try:
        # Initialize all tables including new feedback tables
        await init_all_tables()

        # Keep existing schema initialization for backward compatibility
        async with aiosqlite.connect(CACHE_DB_PATH) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    url_hash TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT,
                    content TEXT,
                    lang TEXT,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS summaries (
                    url_hash TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    summary TEXT,
                    sentiment TEXT,
                    relevance INTEGER,
                    why_it_matters TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job TEXT NOT NULL,
                    ticker TEXT,
                    ok INTEGER NOT NULL,
                    note TEXT,
                    started_at TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS email_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    to_email TEXT NOT NULL,
                    subject TEXT,
                    sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    items_count INTEGER,
                    provider TEXT,
                    ok INTEGER NOT NULL,
                    error TEXT
                )
                """
            )

            # Create indexes
            await db.execute("CREATE INDEX IF NOT EXISTS idx_articles_ticker ON articles(ticker)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_summaries_ticker ON summaries(ticker)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_runs_job_ticker ON runs(job, ticker)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_email_logs_to_sent ON email_logs(to_email, sent_at)")

            await db.commit()

        log.info("db_init: all schemas initialized successfully")
        return {"ok": True, "db": CACHE_DB_PATH}

    except Exception as e:
        log.exception("db_init: failed")
        return {"ok": False, "error": str(e)}


@router.post("/catalog/reload")
async def catalog_reload():
    """
    Reload ticker catalog from data/tickers.json into the ticker_catalog table.
    """
    import os

    json_path = os.path.join(os.getcwd(), "data", "tickers.json")

    try:
        count = await load_ticker_catalog_from_file(json_path, db_path=CACHE_DB_PATH)
        log.info("admin/catalog/reload: loaded %d tickers from %s", count, json_path)
        return {"ok": True, "count": count}
    except Exception as e:
        log.exception("admin/catalog/reload: failed")
        return {"ok": False, "error": str(e)}