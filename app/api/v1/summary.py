import asyncio
from fastapi import APIRouter, Query
from app.summarize.llm import summarize_items
from app.fetch.content import fetch_article_text
from app.ingest.news import fetch_news_for_ticker
from app.ingest.filings_utils import get_filings_for

router = APIRouter()

@router.get("/api/v1/summarize")
async def summarize_test(ticker: str):
    # gather items (news + filings)
    news = fetch_news_for_ticker(ticker) or []
    filings = await get_filings_for(ticker)
    all_items = news + filings

    # Fetch article content for each item before summarization
    enriched_items = []
    for item in all_items:
        url = item.get("url")
        if url:
            article_data = await fetch_article_text(url)
            item["translated_text"] = article_data.get("translated_text", "")
            item["lang"] = article_data.get("lang", "")
            print(f"Fetched article for {ticker}: lang={item.get('lang')}, text_len={len(item.get('translated_text',''))}")
        enriched_items.append(item)

    # Keep items that have some text (translated_text/content) and are allowed to summarize.
    candidates = []
    for it in enriched_items:
        if not it.get("summary_allowed", True):
            continue
        txt = (it.get("translated_text") or it.get("content") or "").strip()
        if len(txt) >= 300:  # simple floor to avoid empty pages
            candidates.append(it)
    print(f"[summarize] {ticker}: {len(candidates)} of {len(enriched_items)} items will be sent to LLM")

    print("→ Using real LLM summarizer")
    summary = await summarize_items(candidates, ticker=ticker)
    print("→ summarizer returned")

    return summary