from __future__ import annotations
from fastapi import APIRouter, Request
from typing import Any, Dict, List
import logging

from app.core.cache import get_meta, count_articles_rows, count_summaries_rows

router = APIRouter(tags=["debug"])
log = logging.getLogger("ari.debug")


@router.get("/healthz")
async def healthz() -> Dict[str, Any]:
    """
    Health endpoint reporting scheduler next runs (best-effort), last run times (from meta),
    and counts for articles/summaries (from cache).
    """
    prefetch_next = summarize_next = purge_next = None

    # try to read scheduler next-run times (best-effort)
    try:
        from app.api.admin import jobs as admin_jobs  # type: ignore
        sched = getattr(admin_jobs, "_scheduler", None)
        if sched:
            j = sched.get_job("prefetch")
            s = sched.get_job("summarize")
            p = sched.get_job("ttl_purge")
            prefetch_next = getattr(j, "next_run_time", None).isoformat() if getattr(j, "next_run_time", None) else None
            summarize_next = getattr(s, "next_run_time", None).isoformat() if getattr(s, "next_run_time", None) else None
            purge_next = getattr(p, "next_run_time", None).isoformat() if getattr(p, "next_run_time", None) else None
    except Exception:
        log.debug("healthz: scheduler info not available", exc_info=False)

    # last-run times from meta + counts (DB-backed, safe)
    try:
        last_prefetch = await get_meta("last_prefetch_at")
    except Exception:
        log.exception("healthz: get_meta last_prefetch_at failed")
        last_prefetch = None

    try:
        last_summarize = await get_meta("last_summarize_at")
    except Exception:
        log.exception("healthz: get_meta last_summarize_at failed")
        last_summarize = None

    try:
        articles = await count_articles_rows()
    except Exception:
        log.exception("healthz: count_articles_rows failed")
        articles = 0

    try:
        summaries = await count_summaries_rows()
    except Exception:
        log.exception("healthz: count_summaries_rows failed")
        summaries = 0

    return {
        "ok": True,
        "scheduler": {
            "prefetch_next": prefetch_next,
            "summarize_next": summarize_next,
            "purge_next": purge_next,
        },
        "last_runs": {
            "last_prefetch_at": last_prefetch,
            "last_summarize_at": last_summarize,
        },
        "counts": {"articles": int(articles or 0), "summaries": int(summaries or 0)},
    }


@router.get("/routes")
async def list_routes(request: Request) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in request.app.routes:
        methods = sorted(list(getattr(r, "methods", []) or []))
        out.append({"methods": methods, "path": getattr(r, "path", "")})
    return out