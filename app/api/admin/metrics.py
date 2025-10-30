from __future__ import annotations
from fastapi import APIRouter
import logging

# Import from actual metrics module (adjust path if you placed metrics elsewhere)
from app.core.metrics import get_daily_summary

log = logging.getLogger("ari.admin.metrics")
router = APIRouter(prefix="/metrics", tags=["admin:metrics"])


@router.get("/summary")
async def metrics_summary():
    try:
        summary = get_daily_summary()
        return {"ok": True, "summary": summary}
    except Exception:
        log.exception("admin.metrics: failed to fetch summary")
        return {"ok": False, "error": "internal_error"}