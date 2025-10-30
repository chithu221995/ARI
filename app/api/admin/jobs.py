from __future__ import annotations
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import time
import json
import aiosqlite
import hashlib

from fastapi import APIRouter, Body, Query, Depends, HTTPException

from app.core import settings
from app.core.cache import cache_upsert_items, url_hash, CACHE_DB_PATH, ensure_summaries_schema
from app.core.dates import now_iso as _now_iso
from app.ingest.news import (
    fetch_news_for_ticker,
    extract_and_cache_bodies,
)
from app.ingest.extract import extract_text, extract_via_diffbot
from app.summarize.llm import summarize_items

log = logging.getLogger("ari.jobs")
router = APIRouter(prefix="/jobs", tags=["jobs"])

# try to import canonical dependency helper; fallback to a safe default
try:
    from app.api.admin.deps import get_tickers_dep  # type: ignore
except Exception:
    async def get_tickers_dep() -> List[str]:
        return getattr(settings, "SCHEDULE_TICKERS", []) or []


# --- Helper: resolve tickers from payload or settings.SCHEDULE_TICKERS ---
def _resolve_tickers(payload: Optional[Dict[str, Any]]) -> List[str]:
    provided = (payload or {}).get("tickers")
    if provided and isinstance(provided, list) and len(provided) > 0:
        return [str(x).strip() for x in provided if str(x).strip()]
    return getattr(settings, "SCHEDULE_TICKERS", []) or []


# --- cache-only summarizer per-ticker (returns upsert count) ---
async def job_summarize(tickers: Optional[List[str]] = None) -> int:
    """
    Summarize recent cached articles for the provided tickers and persist summaries.
    Returns the number of summaries upserted (int) for the tickers handled.
    """
    start_ts = time.time()
    tickers = tickers or getattr(settings, "SCHEDULE_TICKERS", []) or []
    limit = int(getattr(settings, "SUMMARY_TOPK", 5) or 5)
    # process each ticker and sum upserted counts
    total_upserted = 0

    for t in tickers:
        candidates: List[Dict[str, Any]] = []
        # select only rows that have content
        q = f"""
        SELECT url, url_hash, title, content
        FROM articles
        WHERE ticker = ?
          AND content IS NOT NULL
          AND LENGTH(content) > 0
        ORDER BY created_at DESC
        LIMIT ?
        """
        try:
            log.debug("job_summarize: loading candidates ticker=%s limit=%d", t, limit)
            async with aiosqlite.connect(CACHE_DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(q, (t, limit)) as cur:
                    rows = await cur.fetchall()
            log.debug("job_summarize: ticker=%s db_rows_fetched=%d", t, len(rows or []))
        except Exception:
            log.exception("job_summarize: DB candidate load failed for %s", t)
            continue

        for r in (rows or []):
            try:
                rv = dict(r)
            except Exception:
                rv = r
            content = rv.get("content") or ""
            if not content or len(content) < 500:
                continue
            # include url_hash in candidates so we can later enforce allowed set
            candidates.append(
                {
                    "url": rv.get("url") or "",
                    "url_hash": rv.get("url_hash") or "",
                    "title": rv.get("title") or "",
                    "source": rv.get("source") or "",
                    "published_at": rv.get("created_at") or "",
                    "translated_text": content,
                    "lang": rv.get("lang") or "en",
                }
            )

        log.debug("job_summarize: ticker=%s candidates_after_filter=%d", t, len(candidates))
        if not candidates:
            log.info("job_summarize: %s no cached articles with sufficient content, skipping", t)
            continue

        # build allowed set of url_hash values from the candidates we will send
        allowed = { (c.get("url_hash") or "").strip() for c in candidates if c.get("url_hash") }
        sent_count = len(candidates)

        # call summarizer
        try:
            log.debug("job_summarize: ticker=%s sending %d candidates to LLM", t, len(candidates))
            call_start = time.time()
            llm_out = await summarize_items(candidates, ticker=t)
            latency_ms = int((time.time() - call_start) * 1000)
            ok = bool(llm_out.get("ok", True))
            log.info(f"job_summarize: LLM finished for {t} latency_ms={llm_out.get('latency_ms')} ok={ok}")
            # defensive: coerce to list and bail early if empty
            items = llm_out.get("items") or []
            if not items:
                log.warning(f"job_summarize: empty LLM output for {t}, skipping DB upsert")
                continue
        except Exception:
            log.exception("job_summarize: LLM call failed for %s", t)
            continue

        items_raw = (llm_out.get("items") or [])[:limit]
        log.debug("job_summarize: ticker=%s llm_returned=%d", t, len(items_raw))
        # build mapping from candidate positions
        pos_map: Dict[int, Dict[str, Any]] = {i: c for i, c in enumerate(candidates[:limit], start=1)}

        # normalize each returned item with fallbacks to candidate by position/article_number
        items_parsed: List[Dict[str, Any]] = []
        for idx, it in enumerate(items_raw):
            # safe article_number extraction
            art_no = None
            try:
                art_no = int(it.get("article_number")) if it.get("article_number") is not None else None
            except Exception:
                art_no = None

            fallback_cand = pos_map.get(art_no) or pos_map.get(idx + 1) or {}
            url = (it.get("url") or fallback_cand.get("url") or fallback_cand.get("orig_url") or "").strip()
            title_en = (it.get("title") or fallback_cand.get("title") or "").strip()
            why = (it.get("summary") or it.get("why_it_matters") or "").strip()
            sentiment = (it.get("sentiment") or "Neutral").strip()
            sl = sentiment.lower()
            if "neg" in sl or "negative" in sl:
                sentiment = "Negative"
            elif "pos" in sl or "positive" in sl:
                sentiment = "Positive"
            else:
                sentiment = "Neutral"

            rel_raw = it.get("relative_relevance") or it.get("relevance") or it.get("score") or it.get("rank")
            rel_val = None
            if rel_raw is not None:
                try:
                    rel_val = int(str(rel_raw).strip())
                except Exception:
                    rel_val = None

            # heuristic default if model omitted/invalid score
            if rel_val is None:
                txt = f"{title_en} {why}".lower()
                if any(k in txt for k in ["stock pick", "day trading", "outlook for the day", "pr wire", "listicle", "options"]):
                    rel_val = 2
                elif any(k in txt for k in ["layoff", "acquisition", "guidance", "fine", "lawsuit", "contract", "win", "upgrade", "margin", "regulation", "customer"]):
                    rel_val = 8
                else:
                    rel_val = 4

            art_no_final = art_no if isinstance(art_no, int) and 1 <= art_no <= 5 else (idx + 1)

            # preserve any model-provided url_hash/item_url_hash if present
            items_parsed.append({
                 "article_number": art_no_final,
                 "url": url,
                 "url_hash": it.get("url_hash") or it.get("item_url_hash") or "",
                 "title": title_en,
                 "summary": why,
                 "sentiment": sentiment,
                 "relevance": int(rel_val),            # <— add canonical relevance
                 "relative_relevance": int(rel_val),   # keep for backward-compat
             })

        log.debug("job_summarize: ticker=%s parsed_items=%d", t, len(items_parsed))

        # filter / dedupe strictly using allowed url_hash set and derived hashes
        seen = set()
        filtered: List[Dict[str, Any]] = []
        for it in items_parsed:
            h = (it.get("item_url_hash") or it.get("url_hash") or "").strip()
            if not h:
                url = (it.get("url") or "").strip()
                if url:
                    try:
                        h = url_hash(url)
                    except Exception:
                        h = ""
            if not h or (allowed and h not in allowed) or h in seen:
                continue
            seen.add(h)
            # ensure the retained item carries the canonical hash
            it["item_url_hash"] = h
            filtered.append(it)

        # truncate to number of candidates sent
        filtered = filtered[:len(candidates)]
        parsed_count = len(items_parsed)
        kept_count = len(filtered)
        log.debug("job_summarize: ticker=%s sent=%d parsed=%d kept=%d", t, sent_count, parsed_count, kept_count)


        # ensure summaries schema/index then idempotent upsert by URL-hash
        try:
            await ensure_summaries_schema(CACHE_DB_PATH)
        except Exception:
            log.exception("job_summarize: migration check failed; continuing to upsert")

        insert_sql = """
        INSERT INTO summaries
        (ticker, item_url_hash, url, title, why_it_matters, bullets, sentiment, relevance, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_url_hash) DO UPDATE SET
          title=excluded.title,
          why_it_matters=excluded.why_it_matters,
          bullets=excluded.bullets,
          sentiment=excluded.sentiment,
          relevance=excluded.relevance,
          created_at=excluded.created_at
        """
        # Prefer filtered (hash-validated) items; if empty, fall back to the parsed list (not raw LLM output)
        items = filtered if filtered else items_parsed
        log.debug("job_summarize: ticker=%s preparing upsert for %d items (filtered=%d)", t, len(items), len(filtered))
        if not items:
            log.warning("job_summarize: no items to upsert for %s (filtered empty and parsed empty)", t)
            continue

        params = []
        now = _now_iso()
        for it in items:
            url = (it.get("url") or "").strip()
            if not url:
                continue
            try:
                url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
            except Exception:
                log.exception("job_summarize: failed to hash url=%s", url)
                continue

            title = (it.get("title") or "").strip()
            why = (it.get("why_it_matters") or it.get("summary") or "").strip()
            bullets = it.get("bullets") if isinstance(it.get("bullets"), list) else []
            sentiment = (it.get("sentiment") or "Neutral").strip()

            # Cast and clamp relevance from the normalized item (prefer canonical "relevance")
            rel_field = it.get("relevance", None)
            if rel_field is None:
                rel_field = it.get("relative_relevance", None)

            try:
                relevance = int(str(rel_field).strip())
            except Exception:
                relevance = 4  # fallback default

            relevance = max(1, min(10, relevance))

            # Debug what will be written
            log.debug("upsert summary: url=%s rel_raw=%r rel_int=%d sentiment=%s", url, rel_field, relevance, sentiment)

            params.append((
                t, url_hash, url, title, why, json.dumps(bullets),
                sentiment, relevance, now
            ))

        log.debug("job_summarize: ticker=%s upsert params prepared=%d", t, len(params))
        if not params:
            log.info("job_summarize: no valid params to upsert for %s", t)
            continue

        try:
            async with aiosqlite.connect(CACHE_DB_PATH) as db:
                await db.executemany(insert_sql, params)
                await db.commit()
            upserted_i = len(params)
            total_upserted += upserted_i
            log.info("job_summarize: %s upserted %d summaries", t, upserted_i)
        except Exception:
            log.exception("job_summarize: DB upsert failed for %s", t)
            continue

    elapsed = time.time() - start_ts
    log.info("job_summarize: finished tickers=%s total_upserted=%d elapsed=%.3f secs", tickers, total_upserted, elapsed)
    return int(total_upserted)


# --- HTTP endpoints -------------------------------------------------------
@router.post("/run/fetch")
async def run_fetch(payload: Optional[Dict[str, Any]] = Body(None)):
    """
    Fetch endpoint: fetch up to NEWS_TOPK per ticker and persist returned candidate rows.
    Returns per-ticker counts of fetched and upserted rows.
    """
    tickers = _resolve_tickers(payload)
    results: Dict[str, Any] = {}
    fetched_total = 0
    upserted_total = 0

    # request up to NEWS_TOPK items per ticker
    news_topk = int(getattr(settings, "NEWS_TOPK", 10) or 10)
    for t in tickers:
        try:
            rows = await fetch_news_for_ticker(
                t,
                max_items=news_topk,
                days=int(getattr(settings, "NEWS_DAYS", 7) or 7),
            )
            got = len(rows or [])
            # persist all returned candidate metadata (content may be empty)
            upserted_i = int(await cache_upsert_items(rows or []))

            fetched_total += got
            upserted_total += int(upserted_i)
            results[t] = {"got": got, "upserted": int(upserted_i)}
            log.info(
                "run/fetch: ticker=%s requested=%d returned=%d upserted=%d",
                t,
                news_topk,
                got,
                int(upserted_i),
            )

            # quick validation count of recent rows (guarded, remove later)
            try:
                db_path = getattr(settings, "CACHE_DB_PATH", "./ari.db")
                async with aiosqlite.connect(db_path) as db:
                    async with db.execute("SELECT COUNT(*) FROM articles WHERE created_at > datetime('now','-10 minutes')") as cur:
                        row = await cur.fetchone()
                        recent_cnt = int(row[0]) if row and row[0] is not None else 0
                log.info("run/fetch: post-upsert recent rows=%d", recent_cnt)
            except Exception:
                log.exception("run/fetch: post-upsert count failed for %s", t)
        except Exception as e:
            log.exception("run/fetch failed for %s", t)
            results[t] = {"got": 0, "upserted": 0, "error": type(e).__name__}

    return {
        "ok": True,
        "fetched": fetched_total,
        "upserted": upserted_total,
        "tickers": tickers,
        "results": results,
    }


@router.post("/run/extract")
async def run_extract(payload: Optional[Dict[str, Any]] = Body(None)):
    """
    Diffbot-only extraction:
    - Select up to 10 most recent stubs (content NULL/empty) within fresh window.
    - Try the first N = min(EXTRACT_MAX_TRIES, len(cands)) sequentially until we get EXTRACT_TARGET successes.
    - No HTML fallback. Only Diffbot.
    """
    tickers = _resolve_tickers(payload)
    hours = int(getattr(settings, "FRESH_WINDOW_HOURS", 24) or 24)
    target = int(getattr(settings, "EXTRACT_TARGET", 5) or 5)
    max_tries = int(getattr(settings, "EXTRACT_MAX_TRIES", 7) or 7)
    window_expr = f"datetime('now', '-{hours} hours')"

    results: Dict[str, Any] = {}
    db_path = getattr(settings, "CACHE_DB_PATH", "./ari.db")

    for t in tickers:
        # 1) load up to 10 stubs
        q = f"""
        SELECT url, url_hash, title
        FROM articles
        WHERE ticker = ?
          AND (content IS NULL OR LENGTH(content)=0)
          AND created_at > {window_expr}
        ORDER BY created_at DESC
        LIMIT 10
        """
        try:
            async with aiosqlite.connect(db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(q, (t,)) as cur:
                    rows = await cur.fetchall()
        except Exception:
            log.exception("extract: DB load failed for %s", t)
            results[t] = {"tried": 0, "updated": 0, "error": "db_load_failed"}
            continue

        if not rows:
            results[t] = {"tried": 0, "updated": 0}
            continue

        tried = 0
        updated = 0
        # Limit attempts to first `max_tries` records
        for r in (rows or [])[:max_tries]:
            if updated >= target:
                break
            url = (r.get("url") or "").strip() if isinstance(r, dict) else (r["url"] or "").strip()
            if not url:
                continue
            tried += 1
            try:
                # Diffbot only, no fallback
                text, src = await extract_text(url, provider="diffbot", allow_fallback=False)
            except Exception:
                log.exception("extract: diffbot call failed url=%s", url)
                continue

            if not text or len(text) < 500:
                # treat as failure, move to next candidate
                log.debug("extract: insufficient content for url=%s len=%d", url, len(text or ""))
                continue

            # BEFORE building SQL params, define content/title safely
            # r is a sqlite3.Row (mapping-like) — access by keys to avoid .get() on Row
            row_keys = list(r.keys()) if hasattr(r, "keys") else []
            if "text" in row_keys:
                content_raw = r["text"] or ""
            elif "content" in row_keys:
                content_raw = r["content"] or ""
            else:
                content_raw = text or ""
            content = (content_raw or "").strip()
            title = r["title"] if "title" in row_keys else ""
            if not content:
                log.warning("extract: empty content from diffbot; skipping url=%s", url)
                continue

            # upsert body into articles
            try:
                async with aiosqlite.connect(db_path) as db:
                    await db.execute(
                        """
                        UPDATE articles
                           SET content = ?, lang = COALESCE(lang,'en'), title = COALESCE(title, ?)
                         WHERE url = ?
                        """,
                        (content[:15000], title, url),
                    )
                    await db.commit()
                log.info("extract: updated %s (len=%d)", url, len(content))
                updated += 1
            except Exception:
                log.exception("extract: DB update failed url=%s", url)
                continue

        results[t] = {"tried": tried, "updated": updated}
        log.info("run/extract: ticker=%s tried=%d updated=%d", t, tried, updated)

    return {"ok": True, "results": results}


@router.post("/run/summarize")
async def run_summarize(payload: Optional[Dict[str, Any]] = Body(None)):
    """
    Trigger summarization job. Uses job_summarize() defined above.
    """
    tickers = _resolve_tickers(payload)
    summarized: Dict[str, int] = {}
    for t in tickers:
        try:
            n = await job_summarize([t])
            summarized[t] = int(n or 0)
        except Exception:
            log.exception("run_summarize: failed for %s", t)
            summarized[t] = 0
    return {"ok": True, "summarized": summarized}
