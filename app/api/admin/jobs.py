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
from app.pipeline.daily import run_daily_fanout
from datetime import datetime
from app.db.queries import insert_run
from app.ingest.fusion import fetch_news_for_ticker  # Import the updated function
from app.core.cache import cache_upsert_items

log = logging.getLogger("ari.jobs")
router = APIRouter(tags=["admin:jobs"])

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


# --- Reusable job functions (for pipeline use) ---
async def job_fetch(ticker: str, max_items: int = 10) -> int:
    """Fetch news for a single ticker and return upsert count."""
    started = datetime.utcnow().isoformat() + "Z"
    fetched = 0
    upserted = 0
    
    try:
        # Fetch news using company name and aliases from catalog
        rows = await fetch_news_for_ticker(
            ticker,
            max_items=max_items,
            days=int(getattr(settings, "NEWS_DAYS", 7) or 7),
            country="in",  # India for NSE/BSE stocks
        )
        fetched = len(rows or [])
        upserted = await cache_upsert_items(rows or [])
        log.info("job_fetch: ticker=%s fetched=%d upserted=%d", ticker, fetched, upserted)
        
        note = f"fetched={fetched} upserted={upserted}"
        insert_run("fetch", ticker, 1, note, started_at=started)
        return int(upserted)
    except Exception as e:
        log.exception("job_fetch: failed for ticker=%s", ticker)
        insert_run("fetch", ticker, 0, f"err={e}", started_at=started)
        return 0


async def job_extract(ticker: str, force: bool = False) -> dict:
    """
    Extract article content using Diffbot for articles that need extraction.
    Falls back to cached content if Diffbot fails repeatedly.
    """
    from app.db.queries import get_articles_needing_extraction, update_article_content
    from app.ingest.extract import extract_via_diffbot
    from app.core.cache import get_cached_articles  # ADD THIS
    import asyncio
    
    log.info(f"job_extract: starting for ticker={ticker} force={force}")
    
    # Get articles that need extraction
    articles = get_articles_needing_extraction(ticker=ticker, limit=10, force=force)
    
    log.info(f"job_extract: found {len(articles)} articles needing extraction for ticker={ticker}")
    
    if not articles:
        log.info(f"job_extract: no articles need extraction for ticker={ticker}")
        return {"ticker": ticker, "extracted": 0}
    
    extracted_count = 0
    failed_count = 0
    
    for i, article in enumerate(articles):
        url = article.get("url")
        log.info(f"job_extract: extracting content from {url}")
        
        try:
            content = await extract_via_diffbot(url, timeout_s=30)
            
            if content and len(content) > 100:
                update_article_content(url=url, content=content)
                extracted_count += 1
                log.info(f"job_extract: successfully extracted {len(content)} chars from {url}")
            else:
                log.warning(f"job_extract: insufficient content from {url} (got {len(content) if content else 0} chars)")
                failed_count += 1
        
        except Exception as e:
            log.error(f"job_extract: failed to extract from {url}: {e}")
            failed_count += 1
        
        # Wait between Diffbot calls to avoid rate limiting
        if i < len(articles) - 1:
            await asyncio.sleep(2)
    
    # Cache fallback if most extractions failed
    if failed_count > len(articles) // 2 and extracted_count == 0:
        log.warning(f"job_extract: {failed_count}/{len(articles)} extractions failed, attempting cache fallback")
        
        try:
            cached_articles = await get_cached_articles(ticker, max_age_hours=12)
            
            if cached_articles:
                log.warning(f"job_extract: using {len(cached_articles)} cached articles for {ticker}")
                return {
                    "ticker": ticker,
                    "extracted": 0,
                    "cached": len(cached_articles),
                    "fallback": True,
                    "age_hours": round((datetime.utcnow() - datetime.fromisoformat(cached_articles[0]["created_at"].replace("Z", ""))).total_seconds() / 3600, 1)
                }
        except Exception as cache_err:
            log.error(f"job_extract: cache fallback failed for {ticker}: {cache_err}")
    
    log.info(f"job_extract: completed for ticker={ticker}, extracted {extracted_count}/{len(articles)} articles")
    
    return {
        "ticker": ticker,
        "extracted": extracted_count,
        "attempted": len(articles),
        "failed": failed_count
    }


# --- cache-only summarizer per-ticker (returns upsert count) ---
async def job_summarize(tickers: Optional[List[str]] = None) -> int:
    """
    Summarize recent cached articles for the provided tickers and persist summaries.
    Falls back to cached summaries if LLM calls fail repeatedly.
    Returns the number of summaries upserted (int) for the tickers handled.
    """
    from app.core.cache import get_cached_summary  # ADD THIS
    
    started_ts = time.time()
    started = datetime.utcnow().isoformat() + "Z"
    
    try:
        tickers = tickers or getattr(settings, "SCHEDULE_TICKERS", []) or []
        limit = int(getattr(settings, "SUMMARY_TOPK", 5) or 5)
        total_upserted = 0
        llm_failures = 0  # Track consecutive failures

        for t in tickers:
            candidates: List[Dict[str, Any]] = []
            
            # Load candidates from DB
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

            # Build allowed set of url_hash values
            allowed = { (c.get("url_hash") or "").strip() for c in candidates if c.get("url_hash") }
            sent_count = len(candidates)

            # Call summarizer with fallback
            try:
                log.debug("job_summarize: ticker=%s sending %d candidates to LLM", t, len(candidates))
                call_start = time.time()
                llm_out = await summarize_items(candidates, ticker=t)
                latency_ms = int((time.time() - call_start) * 1000)
                ok = bool(llm_out.get("ok", True))
                log.info(f"job_summarize: LLM finished for {t} latency_ms={llm_out.get('latency_ms')} ok={ok}")
                
                items = llm_out.get("items") or []
                if not items:
                    log.warning(f"job_summarize: empty LLM output for {t}, skipping DB upsert")
                    llm_failures += 1
                    
                    # Attempt cache fallback after 2 consecutive failures
                    if llm_failures >= 2:
                        log.warning(f"job_summarize: {llm_failures} LLM failures, attempting cache fallback for {t}")
                        cached = await get_cached_summary(t, max_age_hours=12)
                        
                        if cached and cached.get("items"):
                            log.warning(f"job_summarize: using {len(cached['items'])} cached summaries for {t} (age={cached.get('age_hours')}h)")
                            items = cached["items"]
                            llm_failures = 0  # Reset counter on successful fallback
                        else:
                            log.error(f"job_summarize: no cached summaries available for {t}")
                            continue
                    else:
                        continue
                else:
                    llm_failures = 0  # Reset on success
                    
            except Exception as e:
                log.exception("job_summarize: LLM call failed for %s", t)
                llm_failures += 1
                
                # Attempt cache fallback after 2 consecutive failures
                if llm_failures >= 2:
                    log.warning(f"job_summarize: {llm_failures} LLM failures, attempting cache fallback for {t}")
                    try:
                        cached = await get_cached_summary(t, max_age_hours=12)
                        
                        if cached and cached.get("items"):
                            log.warning(f"job_summarize: using {len(cached['items'])} cached summaries for {t} (age={cached.get('age_hours')}h)")
                            items = cached["items"]
                            llm_failures = 0  # Reset counter on successful fallback
                        else:
                            log.error(f"job_summarize: no cached summaries available for {t}")
                            continue
                    except Exception as cache_err:
                        log.error(f"job_summarize: cache fallback failed for {t}: {cache_err}")
                        continue
                else:
                    continue

            items_raw = items[:limit]
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

                # Ensure url is captured for linking to articles
                url = (it.get("url") or fallback_cand.get("url") or fallback_cand.get("orig_url") or "").strip()
                
                items_parsed.append({
                     "article_number": art_no_final,
                     "url": url,  # IMPORTANT: Preserve URL for linking
                     "url_hash": it.get("url_hash") or it.get("item_url_hash") or "",
                     "title": title_en,
                     "summary": why,
                     "sentiment": sentiment,
                     "relevance": int(rel_val),
                     "relative_relevance": int(rel_val),
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

            # When upserting to summaries table, include url
            insert_sql = """
            INSERT INTO summaries
            (ticker, item_url_hash, url, title, why_it_matters, bullets, sentiment, relevance, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_url_hash) DO UPDATE SET
              url=excluded.url,  -- NEW: Update url on conflict
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
                    t, url_hash, url,  # NEW: Include url in insert
                    title, why, json.dumps(bullets),
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

        elapsed = time.time() - started_ts
        log.info("job_summarize: finished tickers=%s total_upserted=%d elapsed=%.3f secs", tickers, total_upserted, elapsed)
        return int(total_upserted)
        
    except Exception as e:
        log.exception("job_summarize: failed")
        for ticker in (tickers or []):
            insert_run("summarize", ticker, 0, f"err={e}", started_at=started)
        return 0


# --- HTTP endpoints -------------------------------------------------------
@router.post("/run/fetch")
async def run_fetch(payload: Optional[Dict[str, Any]] = Body(None)):
    """
    Fetch endpoint: fetch up to NEWS_TOPK per ticker and persist returned candidate rows.
    Returns per-ticker counts of fetched and upserted rows.
    """
    tickers = _resolve_tickers(payload)
    results: Dict[str, Any] = {}
    upserted_total = 0

    # request up to NEWS_TOPK items per ticker
    news_topk = int(getattr(settings, "NEWS_TOPK", 10) or 10)
    for t in tickers:
        upserted = await job_fetch(t, max_items=news_topk)
        upserted_total += upserted
        results[t] = {"upserted": upserted}

    return {
        "ok": True,
        "upserted": upserted_total,
        "tickers": tickers,
        "results": results,
    }


@router.post("/run/extract")
async def run_extract(payload: Optional[Dict[str, Any]] = Body(None)):
    """Extract content for configured tickers."""
    tickers = _resolve_tickers(payload)

    results: Dict[str, Any] = {}
    for t in tickers:
        updated = await job_extract(t)
        results[t] = {"updated": updated}

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


@router.post("/run/daily", tags=["admin"])
async def run_daily():
    """
    De-duplicated nightly flow:
    unique tickers -> fetch/extract/summarize once each -> email fan-out to users.
    """
    result = await run_daily_fanout()
    return result
