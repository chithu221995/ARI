# main.py
from __future__ import annotations
import asyncio
from typing import List, Dict

from fastapi import FastAPI, Query, HTTPException
from dotenv import load_dotenv
load_dotenv()

import os, httpx

USE_MOCK = os.getenv("USE_MOCK_FILINGS", "false").lower() in {"1", "true", "yes"}
USE_BROWSER = os.getenv("USE_BROWSER_FETCH", "false").lower() in {"1", "true", "yes"}

print(f"Filings mode: {'MOCK' if USE_MOCK else ('BROWSER' if USE_BROWSER else 'LIVE')}")

if USE_BROWSER:
    from app.ingest.filings_browser import (
        fetch_bse_announcements_browser as fetch_bse_announcements,
        fetch_nse_announcements_browser as fetch_nse_announcements,
    )
else:
    from app.ingest.filings import fetch_bse_announcements, fetch_nse_announcements, generate_mock_filings
from app.ingest.filings import filter_for_summary, dedupe_by_url_or_title, generate_mock_filings
from app.ingest.news import fetch_news_for_ticker
from app.summarize.llm import summarize_items
from app.ingest.filings_utils import get_filings_for

app = FastAPI(title="A.R.I. Engine")

# Register API v1 summary router
from app.api.v1 import summary
app.include_router(summary.router)

def _cap_latest(items: List[Dict], n: int = 5) -> List[Dict]:
    def _key(it: Dict):
        return it.get("published_at") or ""
    return sorted(items or [], key=_key, reverse=True)[:n]

@app.get("/")
def read_root():
    return {"message": "Hello from the A.R.I. Engine - An Asset Relevance Intelligence Solution for your portfolio tracking!"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/v1/ingest")
def start_ingestion():
    return {"status": "ingestion pipeline started"}

@app.get("/debug/news")
def debug_news(ticker: str):
    items = fetch_news_for_ticker(ticker)
    if not items:
        items = []
    return {"ticker": ticker, "count": len(items), "sample": items[:3]}

@app.get("/debug/filings")
async def debug_filings(ticker: str):
    try:
        items = await get_filings_for(ticker)
        return {
            "ticker": ticker,
            "count": len(items),
            "sample": items[:3],
            "mode": "mock" if USE_MOCK else ("browser" if USE_BROWSER else "live")
        }
    except Exception as e:
        return {
            "ticker": ticker,
            "count": 0,
            "sample": [],
            "error": str(e),
            "mode": "mock" if USE_MOCK else ("browser" if USE_BROWSER else "live")
        }

@app.get("/api/v1/brief")
async def get_news_brief(
    tickers: str = Query(..., description="Comma-separated tickers"),
    summarized: bool = Query(False, description="Return LLM summary")
):
    tickers_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not tickers_list:
        raise HTTPException(status_code=400, detail="tickers query parameter required")

    result: Dict[str, Dict] = {}

    for ticker in tickers_list:
        try:
            news = fetch_news_for_ticker(ticker) or []
            filings = await get_filings_for(ticker)
            news = _cap_latest(news, 5)
            filings = _cap_latest(filings, 5)
            result[ticker] = {"news": news, "filings": filings}
            if summarized:
                # Only items with summary_allowed and not skip
                items_to_summarize = [
                    item for item in (news + filings)
                    if item.get("summary_allowed", True) and not item.get("skip", False)
                ]
                summary = await summarize_items(items_to_summarize, ticker=ticker)
                result[ticker]["summary"] = summary
        except Exception as e:
            result[ticker] = {"news": [], "filings": [], "_error": str(e)}

    return result

@app.get("/debug/lang")
async def debug_lang(url: str):
    from app.fetch.content import fetch_article_text
    data = await fetch_article_text(url)
    return {
        "url": url,
        "lang": data.get("lang"),
        "chars": data.get("chars"),
        "preview": data.get("translated_text", "")[:400]
    }

@app.get("/debug/openai")
async def debug_openai():
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return {"ok": False, "error": "OPENAI_API_KEY not set in env"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type":"application/json"},
            json={"model":"gpt-4o-mini", "messages":[{"role":"user","content":"healthcheck"}], "max_tokens":6}
        )
    body = r.json() if r.headers.get("content-type","").startswith("application/json") else r.text
    return {
        "status": r.status_code,
        "ok": r.status_code == 200,
        "key_len": len(key),
        "key_head": key[:6],
        "key_tail": key[-4:],
        "key_repr": repr(key),
        "body": body,
    }

try:
    from app.ingest.filings import generate_mock_filings
except Exception:
    def generate_mock_filings(ticker: str) -> list[dict]:
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        return [{
            "title": f"{ticker}: Mock announcement",
            "url": "https://example.com/file.pdf",
            "published_at": (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "BSE",
            "category": "ANNOUNCEMENT",
            "summary_allowed": True,
        }]