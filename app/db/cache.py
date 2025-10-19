# app/ingest/news.py
from __future__ import annotations
import os
from typing import List, Dict
from datetime import datetime, timedelta
import httpx

from app.ingest.tickers import resolve

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

def build_news_query(ticker: str) -> str:
    """
    Build a single OR query using company name + aliases + NSE symbol, de-duped.
    Example: "\"Tata Consultancy Services Limited\" OR Tata Consultancy Services OR TCS"
    """
    meta = resolve(ticker)
    parts: List[str] = []

    company = meta.get("company_name") or ""
    if company:
        parts.append(f"\"{company}\"")  # phrase match

    for alias in meta.get("aliases", []):
        if alias:
            parts.append(alias)

    nse = meta.get("nse_symbol") or ""
    if nse:
        parts.append(nse)

    # de-dupe while preserving order
    seen = set()
    uniq: List[str] = []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)

    return " OR ".join(uniq) if uniq else ticker

def fetch_news_for_ticker(ticker: str) -> List[Dict]:
    """
    Return list[{title, url, published_at, source}] using NewsAPI.
    Language is restricted to English and the last 7 days. Synchronous function.
    """
    if not NEWS_API_KEY or NEWS_API_KEY == "YOUR_API_KEY":
        print("Warning: Missing or invalid NEWS_API_KEY")
        return []

    query = build_news_query(ticker)
    params = {
        "q": query,
        "language": "en",  # prefer English to avoid non-English leaks
        "sortBy": "publishedAt",
        "pageSize": 10,
        "from": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    url = "https://newsapi.org/v2/everything"
    headers = {"X-Api-Key": NEWS_API_KEY}

    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=12.0)
        if resp.status_code != 200:
            print(f"News API error {resp.status_code} for {ticker}: {resp.text[:160]}")
            return []
        data = resp.json() or {}
        articles = data.get("articles") or []
        out: List[Dict] = []
        for a in articles[:5]:
            out.append({
                "title": a.get("title", "") or "",
                "url": a.get("url", "") or "",
                "published_at": a.get("publishedAt", "") or "",
                "source": ((a.get("source") or {}).get("name")) or "",
                "summary_allowed": True,  # safe default for news headlines
            })
        return out
    except Exception as e:
        print(f"News fetch error for {ticker}: {e}")
        return []