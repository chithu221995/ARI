# main.py
from fastapi import FastAPI, Query, HTTPException
import asyncio

from app.ingest.news import fetch_news_for_ticker
from app.ingest.filings import (
    fetch_bse_announcements,
    fetch_nse_announcements,
    filter_for_summary,
    dedupe_by_url_or_title,
)

app = FastAPI(title="A.R.I. Engine")

@app.get("/")
def read_root():
    return {"message": "Hello from the A.R.I. Engine - An Asset Relevance Intelligence Solution for your portfolio tracking!"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/v1/ingest")
def start_ingestion():
    return {"status": "ingestion pipeline started"}

@app.get("/api/v1/brief")
async def get_news_brief(tickers: str = Query(..., description="Comma-separated tickers")):
    tickers_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not tickers_list:
        raise HTTPException(status_code=400, detail="tickers query parameter required")

    result = {}
    for ticker in tickers_list:
        # news fetcher can be sync for now
        news = fetch_news_for_ticker(ticker) or []

        # filings are async; run both calls in parallel
        bse_task = fetch_bse_announcements(ticker)
        nse_task = fetch_nse_announcements(ticker)
        filings_raw = await asyncio.gather(bse_task, nse_task, return_exceptions=True)

        filings = []
        for chunk in filings_raw:
            if isinstance(chunk, list):
                filings.extend(chunk)

        # filter + dedupe + cap
        filings = dedupe_by_url_or_title(filter_for_summary(filings))[:5]
        news = news[:5]

        result[ticker] = {"news": news, "filings": filings}

    return result