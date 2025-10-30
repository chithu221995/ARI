from __future__ import annotations
from fastapi import APIRouter, Query
import logging

from app.ingest.extract import extract_via_diffbot

log = logging.getLogger("ari.admin.cache_diag")
router = APIRouter(prefix="/admin/metrics", tags=["admin:metrics"])


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