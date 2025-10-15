from fastapi import FastAPI, Query
from app.ingest.news import fetch_news_for_ticker

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
def get_news_brief(tickers: str = Query(..., description="Comma-separated tickers")):
    tickers_list = [t.strip() for t in tickers.split(",") if t.strip()]
    result = {}
    for ticker in tickers_list:
        result[ticker] = fetch_news_for_ticker(ticker)
    return result