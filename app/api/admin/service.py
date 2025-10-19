from __future__ import annotations
from typing import List, Optional, Dict, Any
import os
import httpx

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from app.core.retry import with_backoff, RETRY_ATTEMPTS

router = APIRouter()


@router.get("/debug/scheduler")
async def debug_scheduler(request: Request) -> Dict[str, Any]:
    sched = getattr(request.app.state, "scheduler", None)
    if not sched:
        return {"enabled": False, "cron": None, "jobs": []}

    cron = None
    try:
        job = sched.get_job("daily_prefetch")
        if job:
            cron = str(job.trigger)
    except Exception:
        cron = None

    jobs = []
    try:
        for j in sched.get_jobs():
            nr = j.next_run_time.isoformat() if j.next_run_time else None
            jobs.append({"id": j.id, "next_run_time": nr})
    except Exception:
        jobs = []

    return {"enabled": True, "cron": cron, "jobs": jobs}


async def prefetch_post(app: Any, payload: Dict[str, Any]) -> Any:
    """
    Shared implementation for prefetch POST handlers.
    Expects payload dict with optional 'tickers'.
    """
    tickers = payload.get("tickers") if isinstance(payload, dict) else None
    try:
        result = await run_daily_prefetch(app, tickers)
        return {"ok": True, **(result or {})}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/admin/run/prefetch")
async def admin_run_prefetch(request: Request) -> Any:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return await prefetch_post(request.app, payload)


@router.post("/admin/run/summarize")
async def admin_run_summarize(request: Request) -> Any:
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    tickers = payload.get("tickers")
    try:
        result = await run_daily_summarize(request.app, tickers)
        return {"ok": True, **(result or {})}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/admin/scheduler/run-now")
async def admin_run_now(request: Request) -> Any:
    # call the same logic used by /admin/run/prefetch with default tickers
    payload = {"tickers": ["TCS", "TATAMOTORS", "HEROMOTOCO"]}
    return await prefetch_post(request.app, payload)


@router.post("/admin/test/retry")
async def admin_test_retry(request: Request) -> Dict[str, Any]:
    # Disabled in production
    if os.getenv("ENV", "").lower() == "prod":
        return {"ok": False, "error": "disabled in prod"}

    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}

    url = (body.get("url") or "https://httpbin.org/status/503").strip()
    attempts_counter = {"n": 0}
    _logger = type("L", (), {"info": print, "warning": print, "error": print})()

    async def _req():
        attempts_counter["n"] += 1
        async with httpx.AsyncClient() as client:
            r = await client.get(url, timeout=15)
            if r.status_code in {429, 500, 502, 503, 504}:
                raise httpx.RequestError(f"status={r.status_code}")
            return r

    try:
        resp = await with_backoff(_req, attempts=RETRY_ATTEMPTS, logger=_logger, label=f"test-retry:{url}")
        return {"ok": True, "attempts": attempts_counter["n"], "status": resp.status_code}
    except Exception as e:
        return {"ok": False, "attempts": attempts_counter["n"], "error": str(e)}