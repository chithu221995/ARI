from __future__ import annotations
import logging
import inspect
from typing import Dict, Any, List

from fastapi import APIRouter, Body
from app.summarize.llm import summarize_items
from app.ingest.news import fetch_news_for_ticker, select_top_news_for_summary

log = logging.getLogger("ari.summary.api")
router = APIRouter()

# optional content fetcher for better LLM input
try:
    from app.fetch.content import fetch_article_text
except Exception:
    fetch_article_text = None


@router.get("/api/v1/summarize")
async def summarize_get(ticker: str):
    # gather items (up to 20) for the ticker
    try:
        if inspect.iscoroutinefunction(fetch_news_for_ticker):
            items = await fetch_news_for_ticker(ticker, only_en=True, max_items=20)
        else:
            items = fetch_news_for_ticker(ticker, only_en=True, max_items=20)
    except Exception:
        log.exception("summary.api: failed to fetch news for ticker=%s", ticker)
        items = []

    # enrich with article text if fetcher available
    enriched: List[Dict[str, Any]] = []
    for a in items or []:
        try:
            if fetch_article_text:
                if inspect.iscoroutinefunction(fetch_article_text):
                    art = await fetch_article_text(a.get("url", "") or "")
                else:
                    art = fetch_article_text(a.get("url", "") or "")
                a["translated_text"] = art.get("translated_text", "") or ""
                a["lang"] = art.get("lang", "") or a.get("lang", "")
            else:
                a["translated_text"] = a.get("content", "") or ""
            enriched.append(a)
        except Exception:
            log.exception("summary.api: article enrichment failed for url=%s", a.get("url"))
            a["translated_text"] = a.get("content", "") or ""
            a["lang"] = a.get("lang", "")
            enriched.append(a)

    # select top N for summarization (english + content + score + recency)
    batch = select_top_news_for_summary(enriched, max_items=5)
    print(f"[summary] batch size={len(batch)} (after english+content filter)")
    log.info("summary.api: calling LLM summarize for ticker=%s n_items=%d", ticker, len(batch or []))
    out: Dict[str, Any] = await summarize_items(batch or [], ticker=ticker)

    if not out.get("ok", True):
        log.info("summary.api: LLM summarize failed for ticker=%s error=%s", ticker, out.get("error"))
        return {
            "ok": False,
            "items": [],
            "error": out.get("error", "llm_failed"),
            "usage": out.get("usage", {}),
            "latency_ms": out.get("latency_ms"),
        }

    log.info("summary.api: LLM summarize succeeded for ticker=%s items=%d latency_ms=%s", ticker, len(out.get("items", [])), out.get("latency_ms"))
    return {"ok": True, **out}


@router.post("/api/v1/summarize")
async def summarize_post(payload: Dict[str, Any] = Body(...)):
    ticker = (payload or {}).get("ticker", "")
    try:
        if inspect.iscoroutinefunction(fetch_news_for_ticker):
            items = await fetch_news_for_ticker(ticker, only_en=True, max_items=20)
        else:
            items = fetch_news_for_ticker(ticker, only_en=True, max_items=20)
    except Exception:
        log.exception("summary.api: failed to fetch news for ticker=%s", ticker)
        items = []

    enriched: List[Dict[str, Any]] = []
    for a in items or []:
        try:
            if fetch_article_text:
                if inspect.iscoroutinefunction(fetch_article_text):
                    art = await fetch_article_text(a.get("url", "") or "")
                else:
                    art = fetch_article_text(a.get("url", "") or "")
                a["translated_text"] = art.get("translated_text", "") or ""
                a["lang"] = art.get("lang", "") or a.get("lang", "")
            else:
                a["translated_text"] = a.get("content", "") or ""
            enriched.append(a)
        except Exception:
            log.exception("summary.api: article enrichment failed for url=%s", a.get("url"))
            a["translated_text"] = a.get("content", "") or ""
            a["lang"] = a.get("lang", "")
            enriched.append(a)

    batch = select_top_news_for_summary(enriched, max_items=5)
    print(f"[summary] batch size={len(batch)} (after english+content filter)")
    log.info("summary.api: calling LLM summarize (POST) for ticker=%s n_items=%d", ticker, len(batch or []))
    out: Dict[str, Any] = await summarize_items(batch or [], ticker=ticker)

    if not out.get("ok", True):
        log.info("summary.api: LLM summarize failed (POST) for ticker=%s error=%s", ticker, out.get("error"))
        return {
            "ok": False,
            "items": [],
            "error": out.get("error", "llm_failed"),
            "usage": out.get("usage", {}),
            "latency_ms": out.get("latency_ms"),
        }

    log.info("summary.api: LLM summarize succeeded (POST) for ticker=%s items=%d latency_ms=%s", ticker, len(out.get("items", [])), out.get("latency_ms"))
    return {"ok": True, **out}