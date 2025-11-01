from __future__ import annotations
from fastapi import APIRouter, Query, HTTPException
import logging

from app.ingest.extract import extract_via_diffbot

# new imports for DB init
from app.core.cache import ensure_phase4_user_catalog_schemas, CACHE_DB_PATH, load_ticker_catalog_from_file

log = logging.getLogger("ari.admin.cache_diag")
router = APIRouter(prefix="/metrics", tags=["admin:metrics"])


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
    Ensure phase4 user/catalog schemas are present in the cache DB.
    """
    try:
        await ensure_phase4_user_catalog_schemas(CACHE_DB_PATH)
        log.info("/db/init: phase4 schemas ensured at %s", CACHE_DB_PATH)
        return {"ok": True, "db": CACHE_DB_PATH}
    except Exception:
        log.exception("/db/init: failed")
        raise HTTPException(status_code=500, detail="init_failed")


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