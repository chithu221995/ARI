from __future__ import annotations
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo
import time
import json
from typing import List, Dict, Any, Optional

import aiosqlite
import httpx
from app.core.cache import (
    CACHE_DB_PATH,
    url_hash,
    cache_upsert_items,
    cache_upsert_summaries,
    now_iso,
    set_meta,
)
from app.summarize.llm import summarize_items
from app.ingest.news import fetch_news_for_ticker

from fastapi import APIRouter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger("ari.jobs")
# ensure router prefix/tag
router = APIRouter(prefix="/jobs", tags=["admin"])

_scheduler: Optional[AsyncIOScheduler] = None
_started = False

# track last run times (UTC)
_last_prefetch_time: Optional[datetime] = None
_last_summarize_time: Optional[datetime] = None

# also keep ISO timestamps of last runs for external APIs / status
LAST_PREFETCH_AT: Optional[str] = None
LAST_SUMMARIZE_AT: Optional[str] = None

# schedule config (env-driven)
SCHEDULE_TICKERS = [t.strip().upper() for t in os.getenv("SCHEDULE_TICKERS", "").split(",") if t.strip()]
CRON_PREFETCH = os.getenv("CRON_PREFETCH", "0 7 * * *")
CRON_SUMMARIZE = os.getenv("CRON_SUMMARIZE", "30 7 * * *")
CRON_PURGE = os.getenv("CRON_PURGE", "0 3 * * *")  # default 03:00 IST


# ---- Job implementations ----
async def job_prefetch(tickers: Optional[list[str]] = None):
    global _last_prefetch_time, LAST_PREFETCH_AT
    start_ts = time.time()
    try:
        tickers = tickers or SCHEDULE_TICKERS or []
        log.info("[jobs] prefetch START tickers=%s", tickers)
        processed = 0

        for t in tickers:
            items_for_ticker: list[dict] = []
            try:
                # fetch recent news for this ticker (7-day window)
                items_for_ticker = await fetch_news_for_ticker(t, max_items=20, days=7)
                processed += 1
            except Exception:
                log.exception("[jobs] prefetch: fetch_news_for_ticker failed for %s", t)
                continue

            # transform fetched items into article rows and upsert (best-effort)
            try:
                rows: List[Dict[str, Any]] = []
                for n in (items_for_ticker or []):
                    url = (n.get("url") or "").strip()
                    rows.append(
                        {
                            "url_hash": url_hash(url),
                            "url": url,
                            "ticker": t,
                            "source": n.get("source") or "",
                            "title": n.get("title") or "",
                            "published_at": n.get("published_at") or n.get("publishedAt") or "",
                            "lang": (n.get("lang") or "en"),
                            "content": n.get("content") or n.get("translated_text") or "",
                            "text_hash": "",
                            "created_at": None,
                        }
                    )
                if rows:
                    inserted = await cache_upsert_items(rows, ticker=t)
                    log.info("[jobs] prefetch: upserted %d articles for %s", int(inserted or len(rows)), t)
            except Exception:
                log.exception("[jobs] failed to upsert collected items into cache for %s", t)
                continue

        _last_prefetch_time = datetime.now(timezone.utc)
        elapsed_ms = int((time.time() - start_ts) * 1000)

        # record last-run in meta and log counts
        try:
            now_iso_val = now_iso()
            await set_meta("last_prefetch_at", now_iso_val)
            arts = await count_articles_rows()
            sums = await count_summaries_rows()
            LAST_PREFETCH_AT = now_iso_val
            log.info("[jobs] prefetch DONE at=%s processed=%d elapsed_ms=%d articles=%s summaries=%s", now_iso_val, processed, elapsed_ms, arts, sums)
        except Exception:
            log.exception("[jobs] post-prefetch bookkeeping failed")
    except Exception:
        log.exception("[jobs] prefetch FAILED")
        raise


async def job_summarize(tickers: Optional[list[str]] = None):
    """
    Summarize recent articles per ticker and persist summaries.
    - loads recent article candidates (last 72h) not already summarized
    - fetches article body when missing
    - calls summarize_items and upserts returned summaries
    """
    global _last_summarize_time, LAST_SUMMARIZE_AT
    start_ts = time.time()
    total_upserted = 0
    tickers = tickers or SCHEDULE_TICKERS or []
    tickers_count = len(tickers)
    try:
        for ticker in tickers:
            t = ticker
            ts_now = datetime.now(timezone.utc)
            cutoff = (ts_now - timedelta(hours=72)).strftime("%Y-%m-%dT%H:%M:%SZ")
            candidates: List[Dict[str, Any]] = []
            # select articles for ticker that are not yet summarized (left join)
            q = """
            SELECT a.url, a.title, a.source, a.published_at, a.translated_text, a.lang, a.url_hash
            FROM articles a
            LEFT JOIN summaries s ON s.item_url_hash = a.url_hash
            WHERE a.ticker = ? AND a.created_at >= ? AND a.lang = 'en' AND s.item_url_hash IS NULL
            ORDER BY a.published_at DESC
            LIMIT 100
            """
            try:
                async with aiosqlite.connect(CACHE_DB_PATH) as db:
                    async with db.execute(q, (t, cutoff)) as cur:
                        rows = await cur.fetchall()
                        for url, title, source, published_at, translated_text, lang, url_h in rows:
                            candidates.append(
                                {
                                    "url": url or "",
                                    "title": title or "",
                                    "source": source or "",
                                    "published_at": published_at or "",
                                    "translated_text": translated_text or "",
                                    "lang": lang or "en",
                                    "url_hash": url_h or "",
                                }
                            )
            except Exception:
                log.exception("[jobs] summarize: DB candidate load failed for %s", t)
                continue

            # enrich with article text when missing
            items_for_llm: List[Dict[str, Any]] = []
            for c in candidates:
                if not (c.get("translated_text") or "").strip():
                    try:
                        art = await fetch_article_text(c.get("url") or "")
                        c["translated_text"] = (art.get("translated_text") or "").strip()
                        c["lang"] = art.get("lang") or c.get("lang")
                    except Exception:
                        c["translated_text"] = c.get("translated_text") or ""
                if not c.get("translated_text"):
                    # skip items with no usable text
                    continue
                items_for_llm.append(
                    {
                        "title": c.get("title") or "",
                        "url": c.get("url") or "",
                        "translated_text": c.get("translated_text") or "",
                        "source": c.get("source") or "",
                        "category": "NEWS",
                        "published_at": c.get("published_at") or "",
                        "summary_allowed": True,
                    }
                )

            # enforce translated_text truthiness and skip empty batches
            items_for_llm = [it for it in items_for_llm if (it.get("translated_text") or "").strip()]
            log.info("[jobs] summarize: %s candidates=%d will_send=%d", t, len(candidates), len(items_for_llm))
            if not items_for_llm:
                log.info("[jobs] summarize: %s no items with translated_text, skipping", t)
                continue

            # call LLM
            try:
                call_start = time.time()
                llm_out = await summarize_items(items_for_llm, ticker=t)
                latency_ms = int((time.time() - call_start) * 1000)
                log.info("[jobs] summarize: LLM finished for %s latency_ms=%d ok=%s", t, latency_ms, bool(llm_out.get("ok", True)))
            except Exception:
                log.exception("[jobs] summarize: LLM call failed for %s", t)
                continue

            # prepare upsert rows; ensure we have at least one item (synthesize fallback if needed)
            rows_to_upsert: List[Dict[str, Any]] = []
            items = llm_out.get("items") or []
            if not items:
                # synthesize a minimal summary from the first candidate's translated_text
                try:
                    src = items_for_llm[0]
                    txt = (src.get("translated_text") or "").strip()
                    excerpt = " ".join(txt.splitlines())[:800].strip()
                    synth_title = (src.get("title") or "").strip() or (excerpt[:120] + "...")
                    log.warning("[jobs] summarize: LLM returned 0 items for %s; synthesizing fallback summary", t)
                    items = [
                        {
                            "url": src.get("url") or "",
                            "title": synth_title,
                            "bullets": [excerpt],
                            "why_it_matters": "",
                            "sentiment": "Neutral",
                        }
                    ]
                except Exception:
                    log.exception("[jobs] summarize: failed to synthesize fallback summary for %s", t)
                    items = []

            for it in items:
                url = (it.get("url") or "").strip()
                try:
                    h = url_hash(url) if url else (it.get("item_url_hash") or "")
                except Exception:
                    h = it.get("item_url_hash") or ""
                bullets = it.get("bullets") or []
                if isinstance(bullets, list):
                    bullets_text = "\n".join([str(b).strip() for b in bullets if b and str(b).strip()])
                else:
                    bullets_text = str(bullets or "")
                rows_to_upsert.append(
                    {
                        "item_url_hash": h,
                        "ticker": t,
                        "title": it.get("title") or "",
                        "url": url,
                        "bullets": bullets_text,
                        "why_it_matters": it.get("why_it_matters") or "",
                        "sentiment": it.get("sentiment") or "Neutral",
                        "created_at": now_iso(),
                    }
                )

            # persist summaries
            try:
                upserted = await cache_upsert_summaries(rows_to_upsert)
                total_upserted += int(upserted or 0)
                log.info("[jobs] summarize: upserted %d summaries for %s", int(upserted or 0), t)
            except Exception:
                log.exception("[jobs] summarize: upsert failed for %s", t)
                continue

        # bookkeeping
        ts = now_iso()
        try:
            await set_meta("last_summarize_at", ts)
            LAST_SUMMARIZE_AT = ts
        except Exception:
            log.exception("[jobs] summarize: failed to set meta last_summarize_at")

        elapsed_ms = int((time.time() - start_ts) * 1000)
        log.info("[jobs] summarize DONE at=%s processed=%d upserted=%d elapsed_ms=%d", ts, tickers_count, total_upserted, elapsed_ms)
    except Exception:
        log.exception("[jobs] summarize FAILED")
        raise


async def job_purge():
    """
    Run TTL purge and record a row in ingest_runs with run_type='ttl_purge'
    """
    started_at = datetime.now(timezone.utc)
    a = s = f = 0
    try:
        log.info("[jobs] purge START")
        res = await purge_expired()
        if isinstance(res, tuple) and len(res) == 3:
            a, s, f = res
        elif isinstance(res, int):
            # old helper returned total deleted; map it to articles and leave others 0
            a = res
        finished_at = datetime.now(timezone.utc)
        log.info("[jobs] purge DONE (articles=%s summaries=%s filings=%s)", a, s, f)
    except Exception:
        log.exception("[jobs] purge FAILED")
        finished_at = datetime.now(timezone.utc)
        a = s = f = 0

    total = (a or 0) + (s or 0) + (f or 0)

    # record run in ingest_runs (best-effort; ignore errors)
    try:
        db_path = os.getenv("SQLITE_PATH", "./ari.db")
        async with aiosqlite.connect(db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute(
                "INSERT INTO ingest_runs (run_type, started_at, finished_at, count, ok) VALUES (?, ?, ?, ?, ?)",
                ("ttl_purge", started_at.isoformat(), finished_at.isoformat(), total, 1),
            )
            await db.commit()
    except Exception:
        log.exception("[jobs] failed to record ingest_runs for purge")


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
        # schedule TTL purge
        _scheduler.add_job(job_purge, CronTrigger.from_crontab(crons["purge"], timezone=ZoneInfo("Asia/Kolkata")), id="ttl_purge")
    except Exception as e:
        log.exception("[jobs] failed to add cron jobs: %s", e)

    if not _scheduler.running:
        _scheduler.start()

    # log schedule + next runs
    try:
        j1 = _scheduler.get_job("prefetch")
        j2 = _scheduler.get_job("summarize")
        j3 = _scheduler.get_job("ttl_purge")
        log.info("[jobs] started. CRON_PREFETCH='%s', next=%s", crons["prefetch"], getattr(j1, "next_run_time", None))
        log.info("[jobs] started. CRON_SUMMARIZE='%s', next=%s", crons["summarize"], getattr(j2, "next_run_time", None))
        log.info("[jobs] started. CRON_PURGE='%s', next=%s", crons["purge"], getattr(j3, "next_run_time", None))
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


@router.get(
    "/state",
    summary="Show scheduler state",
    description="Return scheduler cron configuration, next-run times and last-run timestamps for prefetch/summarize/purge.",
)
async def job_state():
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
    """
    Returns scheduler + last-run + basic counts.
    last_prefetch, last_summarize: ISO timestamps or None
    articles_rows, summaries_rows: integers (0 on error)
    scheduled: mapping job_id -> next_run_time ISO or None
    """
    # fetch counts using the cache helpers
    try:
        articles = await count_articles_rows()
        summaries = await count_summaries_rows()
    except Exception:
        log.exception("[jobs] debug.status: count helpers failed")
        articles = 0
        summaries = 0

    # scheduled next run times
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
        "articles_rows": int(articles or 0),
        "summaries_rows": int(summaries or 0),
        "scheduled": scheduled,
    }


@router.post(
    "/run/prefetch",
    summary="Run prefetch now",
    description=(
        "Fetches the latest English news for configured tickers (7-day window) and writes "
        "articles into the local cache. Use to prime today's data immediately."
    ),
)
async def run_prefetch():
    await job_prefetch()
    return {"ok": True, "ran": "prefetch", "at": datetime.now(timezone.utc).isoformat()}


@router.post(
    "/run/summarize",
    summary="Run summarize now",
    description=(
        "Summarize recent cached articles for configured tickers using the LLM and persist "
        "summary rows into the cache. Useful to regenerate summaries on demand."
    ),
)
async def run_summarize():
    await job_summarize()
    return {"ok": True, "ran": "summarize", "at": datetime.now(timezone.utc).isoformat()}


@router.post(
    "/run/purge",
    summary="Purge old cache rows",
    description="Delete cached articles and summaries older than the configured retention (default: 7 days).",
)
async def run_purge():
    await job_purge()
    return {"ok": True, "ran": "purge", "at": datetime.now(timezone.utc).isoformat()}


import os
import logging
import aiosqlite
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

async def purge_expired(ttl_days: int = 7):
    """
    Delete cache rows older than ttl_days. Return a triple (articles_deleted, summaries_deleted, filings_deleted).
    If a table doesn't exist, count is 0. If 'filings' table was removed from prototype, return 0 for it.
    """
    a = s = f = 0
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            # articles
            if await _table_exists(db, "articles"):
                cur = await db.execute(
                    "DELETE FROM articles WHERE created_at < datetime('now', ?)",
                    (f"-{ttl_days} days",),
                )
                a = cur.rowcount or 0
            # summaries
            if await _table_exists(db, "summaries"):
                cur = await db.execute(
                    "DELETE FROM summaries WHERE created_at < datetime('now', ?)",
                    (f"-{ttl_days} days",),
                )
                s = cur.rowcount or 0
            # filings (optional / may not exist)
            if await _table_exists(db, "filings"):
                cur = await db.execute(
                    "DELETE FROM filings WHERE created_at < datetime('now', ?)",
                    (f"-{ttl_days} days",),
                )
                f = cur.rowcount or 0

            await db.commit()
    except Exception as e:
        log.error("purge_expired failed: %s", e)
    return a, s, f

async def fetch_article_text(url: str) -> dict:
    """
    Minimal text fetcher for summarize job.
    Returns {"translated_text": <string>, "lang": "en"}.
    Keep it simple: download, strip HTML tags crudely, best-effort.
    """
    if not url:
        return {"translated_text": "", "lang": "en"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, follow_redirects=True)
            html = r.text or ""
        # super-simple fallback: remove tags
        import re

        text = re.sub(r"<[^>]+>", " ", html)
        text = " ".join(text.split())
        return {"translated_text": text[:15000], "lang": "en"}
    except Exception:
        return {"translated_text": "", "lang": "en"}


# ensure symbols export
__all__ = [
    "count_articles_rows",
    "count_summaries_rows",
    "purge_expired",
    "fetch_article_text",
]