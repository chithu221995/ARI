from __future__ import annotations
import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.cache import purge_expired

log = logging.getLogger("ari.jobs")
router = APIRouter(prefix="/jobs", tags=["admin:jobs"])

_scheduler: Optional[AsyncIOScheduler] = None
_started = False

# track last run times (UTC)
_last_prefetch_time: Optional[datetime] = None
_last_summarize_time: Optional[datetime] = None

# schedule config (env-driven)
SCHEDULE_TICKERS = [t.strip().upper() for t in os.getenv("SCHEDULE_TICKERS", "").split(",") if t.strip()]
CRON_PREFETCH = os.getenv("CRON_PREFETCH", "0 7 * * *")
CRON_SUMMARIZE = os.getenv("CRON_SUMMARIZE", "30 7 * * *")
CRON_PURGE = os.getenv("CRON_PURGE", "15 3 * * *")  # default 03:15 IST


# ---- Job implementations ----
async def job_prefetch(tickers: Optional[list[str]] = None):
    global _last_prefetch_time
    try:
        tickers = tickers or SCHEDULE_TICKERS or []
        log.info("[jobs] prefetch START tickers=%s", tickers)
        # TODO: integrate real prefetch logic
        for t in tickers:
            await asyncio.sleep(0.01)
        _last_prefetch_time = datetime.now(timezone.utc)
        log.info("[jobs] prefetch DONE at=%s", _last_prefetch_time.isoformat())
    except Exception:
        log.exception("[jobs] prefetch FAILED")
        raise


async def job_summarize(tickers: Optional[list[str]] = None):
    global _last_summarize_time
    try:
        tickers = tickers or SCHEDULE_TICKERS or []
        log.info("[jobs] summarize START tickers=%s", tickers)
        # TODO: integrate real summarize logic
        for t in tickers:
            await asyncio.sleep(0.01)
        _last_summarize_time = datetime.now(timezone.utc)
        log.info("[jobs] summarize DONE at=%s", _last_summarize_time.isoformat())
    except Exception:
        log.exception("[jobs] summarize FAILED")
        raise


async def job_purge():
    try:
        log.info("[jobs] purge START")
        a, s, f = await purge_expired()
        log.info("[jobs] purge DONE (articles=%s summaries=%s filings=%s)", a, s, f)
    except Exception:
        log.exception("[jobs] purge FAILED")
        raise


def _get_crons() -> Dict[str, str]:
    return {"prefetch": CRON_PREFETCH, "summarize": CRON_SUMMARIZE, "purge": CRON_PURGE}


def _ensure_scheduler():
    global _scheduler, _started
    if _scheduler is None:
        _scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
    if _started:
        return

    crons = _get_crons()

    # purge prior jobs
    try:
        for j in list(_scheduler.get_jobs()):
            try:
                _scheduler.remove_job(j.id)
            except Exception:
                pass
    except Exception:
        pass

    try:
        _scheduler.add_job(job_prefetch, CronTrigger.from_crontab(crons["prefetch"]), id="prefetch")
        _scheduler.add_job(job_summarize, CronTrigger.from_crontab(crons["summarize"]), id="summarize")
        # schedule daily purge at CRON_PURGE in IST
        _scheduler.add_job(job_purge, CronTrigger.from_crontab(crons["purge"], timezone=ZoneInfo("Asia/Kolkata")), id="job_purge")
    except Exception as e:
        log.exception("[jobs] failed to add cron jobs: %s", e)

    if not _scheduler.running:
        _scheduler.start()

    # log schedule + next runs
    try:
        for job_id in ("prefetch", "summarize", "job_purge"):
            j = _scheduler.get_job(job_id)
            log.info("[jobs] started. job=%s next=%s", job_id, getattr(j, "next_run_time", None))
    except Exception:
        log.exception("[jobs] failed to read scheduler jobs")

    _started = True


@router.on_event("startup")
async def _on_startup():
    try:
        _ensure_scheduler()
    except Exception:
        log.exception("[jobs] scheduler startup failed")


@router.on_event("shutdown")
async def _on_shutdown():
    global _scheduler, _started
    try:
        if _scheduler and _scheduler.running:
            _scheduler.shutdown(wait=False)
    except Exception:
        log.exception("[jobs] scheduler shutdown error")
    _started = False
    _scheduler = None


@router.get("/ping")
async def jobs_ping():
    return {"ok": True, "component": "jobs"}


@router.get("/state")
async def state():
    crons = _get_crons()
    info: Dict[str, Any] = {"running": bool(_scheduler and _scheduler.running), "crons": crons}
    if _scheduler:
        try:
            for j in _scheduler.get_jobs():
                info[j.id] = {"next_run_time": getattr(j, "next_run_time", None)}
        except Exception:
            log.exception("[jobs] failed to read scheduler jobs")
    return info


@router.get("/debug/status")
async def debug_status():
    articles_rows = None
    summaries_rows = None

    # try to import count helpers
    try:
        from app.core.cache import count_articles_rows, count_summaries_rows  # type: ignore
        helpers = [("core.cache", count_articles_rows, count_summaries_rows)]
    except Exception:
        helpers = []

    for label, a_fn, s_fn in helpers:
        try:
            if a_fn:
                if asyncio.iscoroutinefunction(a_fn):
                    articles_rows = await a_fn()
                else:
                    articles_rows = a_fn()
            if s_fn:
                if asyncio.iscoroutinefunction(s_fn):
                    summaries_rows = await s_fn()
                else:
                    summaries_rows = s_fn()
            break
        except Exception:
            log.exception("[jobs] debug.status: count helper %s failed", label)

    scheduled: Dict[str, Optional[str]] = {}
    if _scheduler:
        try:
            for j in _scheduler.get_jobs():
                nrt = getattr(j, "next_run_time", None)
                scheduled[j.id] = nrt.isoformat() if nrt else None
        except Exception:
            log.exception("[jobs] debug.status: failed to enumerate scheduled jobs")

    return {
        "ok": True,
        "last_prefetch": _last_prefetch_time.isoformat() if _last_prefetch_time else None,
        "last_summarize": _last_summarize_time.isoformat() if _last_summarize_time else None,
        "articles_rows": articles_rows,
        "summaries_rows": summaries_rows,
        "scheduled": scheduled,
    }


@router.post("/run/prefetch")
async def run_prefetch_now():
    await job_prefetch()
    return {"ok": True, "ran": "prefetch", "at": datetime.now(timezone.utc).isoformat()}


@router.post("/run/summarize")
async def run_summarize_now():
    await job_summarize()
    return {"ok": True, "ran": "summarize", "at": datetime.now(timezone.utc).isoformat()}


@router.post("/run/purge")
async def run_purge_now():
    await job_purge()
    return {"ok": True, "ran": "purge", "at": datetime.now(timezone.utc).isoformat()}