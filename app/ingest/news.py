# app/ingest/news.py
from __future__ import annotations
import os
from typing import List, Dict
from datetime import datetime, timedelta
import httpx

from app.ingest.tickers import resolve

def build_news_query(ticker: str) -> str:
    meta = resolve(ticker)
    parts = [f"\"{meta['company_name']}\""] + meta["aliases"] + [meta["nse_symbol"]]
    # de-dupe while preserving order
    seen = set()
    uniq: List[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    # single OR query (one API call)
    return " OR ".join(uniq)

async def fetch_news_for_ticker(ticker: str) -> List[Dict]:
    """Return list[{title,url,published_at,source}] using NewsAPI. Always returns a list (never None)."""
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key or api_key == "YOUR_API_KEY":
        print("Missing or invalid NEWS_API_KEY")
        return []

    url = f"https://newsapi.org/v2/everything?q={ticker}&apiKey={api_key}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                print(f"News API error {resp.status_code} for {ticker}")
                return []
            data = resp.json()
            articles = data.get("articles", [])
            if not articles:
                return []
            out = []
            for a in articles[:5]:
                out.append({
                    "title": a.get("title", ""),
                    "url": a.get("url", ""),
                    "published_at": a.get("publishedAt", ""),
                    "source": a.get("source", {}).get("name", ""),
                })
            return out
    except Exception as e:
        print(f"News fetch error for {ticker}: {e}")
        return []

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

def fetch_news_for_ticker(ticker: str) -> list:
    if not NEWS_API_KEY or NEWS_API_KEY == "YOUR_API_KEY":
        print("Warning: Missing or invalid NEWS_API_KEY")
        return []
    url = f"https://newsapi.org/v2/everything?q={ticker}&apiKey={NEWS_API_KEY}"
    try:
        resp = httpx.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"News API error {resp.status_code} for {ticker}")
            return []
        data = resp.json()
        articles = data.get("articles", [])
        out = []
        for a in articles[:5]:
            out.append({
                "title": a.get("title", ""),
                "url": a.get("url", ""),
                "published_at": a.get("publishedAt", ""),
                "source": (a.get("source") or {}).get("name", ""),
            })
        return out
    except Exception as e:
        print(f"News fetch error for {ticker}: {e}")
        return []