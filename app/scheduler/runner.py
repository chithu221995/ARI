from __future__ import annotations
import os
import random
import asyncio
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.scheduler.jobs import parse_cron, run_prefetch
from app.scheduler.summarize_job import summarize_cached_and_upsert

_scheduler: Optional[AsyncIOScheduler] = None
_TZ = ZoneInfo("Asia/Kolkata")


def _parse_cron(expr: str) -> Dict[str, str]:
    return parse_cron(expr)


async def schedule_jobs(app: Any) -> None:
    """
    Start AsyncIOScheduler, schedule two daily jobs (prefetch + summarize).
    Stores scheduler on app.state.scheduler.
    """
    global _scheduler
    if _scheduler is not None:
        # already started
        return

    _scheduler = AsyncIOScheduler(timezone=_TZ)

    cron_prefetch = os.getenv("CRON_PREFETCH", "0 7 * * *")
    cron_summarize = os.getenv("CRON_SUMMARIZE", "30 7 * * *")
    jitter_seconds = float(os.getenv("SCHEDULE_JOB_JITTER_SECS", "20"))

    # default tickers for the scheduled prefetch job
    default_tickers = ["TCS", "TATAMOTORS", "HEROMOTOCO"]

    # build triggers
    try:
        p = _parse_cron(cron_prefetch)
        trigger_prefetch = CronTrigger(
            minute=p["minute"],
            hour=p["hour"],
            day=p["day"],
            month=p["month"],
            day_of_week=p["day_of_week"],
            timezone=_TZ,
        )
    except Exception:
        # fallback: run daily at 07:00
        trigger_prefetch = CronTrigger(minute="0", hour="7", timezone=_TZ)

    try:
        s = _parse_cron(cron_summarize)
        trigger_summarize = CronTrigger(
            minute=s["minute"],
            hour=s["hour"],
            day=s["day"],
            month=s["month"],
            day_of_week=s["day_of_week"],
            timezone=_TZ,
        )
    except Exception:
        trigger_summarize = CronTrigger(minute="30", hour="7", timezone=_TZ)

    async def _job_prefetch_wrapper(tickers: Optional[List[str]] = None) -> Dict[str, Any]:
        # small random jitter to avoid thundering starts
        delay = random.uniform(0, jitter_seconds)
        await asyncio.sleep(delay)
        tk = tickers or default_tickers
        # run_prefetch returns a dict per earlier contract
        return await run_prefetch(app, tk)

    async def _job_summarize_wrapper(tickers: Optional[List[str]] = None) -> Dict[str, Any]:
        # small random jitter
        delay = random.uniform(0, jitter_seconds)
        await asyncio.sleep(delay)
        tk = tickers or default_tickers
        results: Dict[str, Any] = {"requested": tk, "ok": [], "failed": [], "counts": {}}
        for t in tk:
            try:
                res = await summarize_cached_and_upsert(app, t)
                results["ok"].append(t)
                results["counts"][t] = int(res.get("summarized", 0))
            except Exception as e:
                results["failed"].append({"symbol": t, "error": str(e)})
                results["counts"][t] = 0
        return results

    # schedule jobs
    _scheduler.add_job(
        _job_prefetch_wrapper,
        trigger=trigger_prefetch,
        args=[None],
        id="scheduled_prefetch",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.add_job(
        _job_summarize_wrapper,
        trigger=trigger_summarize,
        args=[None],
        id="scheduled_summarize",
        replace_existing=True,
        misfire_grace_time=300,
    )

    _scheduler.start()
    # expose on app.state
    try:
        app.state.scheduler = _scheduler
    except Exception:
        pass


def get_scheduler_state() -> Dict[str, Any]:
    """
    Return basic scheduler state: running flag, list of jobs with id/next_run/cron, tz.
    """
    global _scheduler
    if not _scheduler:
        return {"running": False, "jobs": [], "tz": str(_TZ)}

    jobs = []
    try:
        for j in _scheduler.get_jobs():
            nr = j.next_run_time.isoformat() if j.next_run_time else None
            jobs.append({"id": j.id, "next_run": nr, "cron": str(j.trigger)})
    except Exception:
        jobs = []

    return {"running": bool(_scheduler.running), "jobs": jobs, "tz": str(_TZ)}