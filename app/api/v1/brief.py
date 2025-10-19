from __future__ import annotations
import asyncio
from inspect import iscoroutinefunction
from typing import Any, Dict, List

from fastapi import APIRouter, Query, HTTPException

from app.ingest.news import fetch_news_for_ticker
from app.summarize.llm import summarize_items

router = APIRouter()


@router.get("/api/v1/brief")
async def brief_get(tickers: str = Query(..., description="Comma-separated tickers"), summarized: bool = Query(False)) -> Any:
    """
    News-only brief endpoint.
    Query:
      tickers: comma-separated list (required)
      summarized: include LLM summary per ticker if true
    Response: { "ok": True, "results": { TICKER: { "news": [...], ("summary": {...}) } } }
    """
    if not tickers:
        raise HTTPException(status_code=400, detail="tickers required")

    ticker_list = [t.strip().upper() for t in tickers.split(",") if t and t.strip()]
    if not ticker_list:
        raise HTTPException(status_code=400, detail="no valid tickers provided")

    results: Dict[str, Dict[str, Any]] = {}

    for t in ticker_list:
        news: List[Dict] = []
        try:
            if iscoroutinefunction(fetch_news_for_ticker):
                news = await fetch_news_for_ticker(t, only_en=True, days=7)
            else:
                def _news_fetch():
                    try:
                        return fetch_news_for_ticker(t, only_en=True, days=7)
                    except TypeError:
                        return fetch_news_for_ticker(t)
                news = await asyncio.to_thread(_news_fetch)
        except Exception:
            news = []

        entry: Dict[str, Any] = {"news": news or []}

        if summarized and news:
            llm_items = []
            for n in news:
                llm_items.append({
                    "title": n.get("title", ""),
                    "url": n.get("url", ""),
                    "translated_text": n.get("content", "") or n.get("translated_text", ""),
                    "published_at": n.get("published_at", "") or n.get("publishedAt", ""),
                })
            try:
                summary = await summarize_items(llm_items, ticker=t)
                entry["summary"] = summary
            except Exception:
                entry["summary"] = {"error": "summarize_items failed"}

        results[t] = entry

    return {"ok": True, "results": results}