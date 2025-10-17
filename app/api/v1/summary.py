import asyncio
from fastapi import APIRouter, Query
from app.summarize.llm import summarize_items
from app.fetch.content import fetch_article_text
from app.ingest.news import fetch_news_for_ticker
from app.ingest.filings_utils import get_filings_for
from app.core.cache import url_to_hash, cache_get_summary, cache_upsert_summary

router = APIRouter()

@router.get("/api/v1/summarize")
async def summarize_test(ticker: str):
    # gather items (news + filings)
    news = fetch_news_for_ticker(ticker) or []
    filings = await get_filings_for(ticker)
    all_items = news + filings

    # Build candidates (items allowed for summary)
    candidates = [i for i in all_items if i.get("summary_allowed", True)]

    cached_results = []
    to_summarize = []

    # Check cache first; only enrich (fetch article text) for misses
    for it in candidates:
        url = it.get("url", "") or ""
        if not url:
            continue
        hit = await cache_get_summary(url, max_age_hours=24)
        if hit:
            cached_results.append({
                "title": it.get("title", ""),
                "url": url,
                "bullets": hit.get("bullets", []),
                "why_it_matters": hit.get("why_it_matters", ""),
                "sentiment": hit.get("sentiment", "Neutral"),
            })
        else:
            # enrich item with article text before sending to LLM
            article_data = await fetch_article_text(url)
            it["translated_text"] = article_data.get("translated_text", "")
            it["lang"] = article_data.get("lang", "")
            to_summarize.append(it)

    # Build payload that always carries title + url and only includes items with meaningful text
    to_summarize_payload = []
    for it in all_items:
        txt = (it.get("translated_text") or "").strip()
        if len(txt) < 300:
            continue
        to_summarize_payload.append({
            "title": it.get("title", ""),
            "url": it.get("url", ""),
            "text": txt,
            "source": it.get("source", ""),
            "category": it.get("category", ""),
            "published_at": it.get("published_at", ""),
        })
    print(f"[summarize] sending={len(to_summarize_payload)} (english-only ≥300 chars)")

    if not to_summarize_payload and not cached_results:
        print("[summarize] nothing to summarize (no text after extraction/lang filter)")

    summary = {}
    llm_results = []

    if to_summarize_payload:
        print("→ Using real LLM summarizer")
        summary = await summarize_items(to_summarize_payload, ticker=ticker)
        llm_results = summary.get("items", []) or []
        # Only upsert summaries for items that were actually sent to the LLM
        sent_urls = {i.get("url", "") for i in to_summarize_payload}
        for r in llm_results:
            url = r.get("url", "") or ""
            if url not in sent_urls:
                print(f"[cache] skipping upsert for {url}: not sent to LLM")
                continue
            await cache_upsert_summary(
                ticker=ticker,
                url_hash=url_to_hash(url),
                bullets=r.get("bullets", []),
                why_it_matters=r.get("why_it_matters", ""),
                sentiment=r.get("sentiment", "Neutral"),
            )
        print("→ summarizer returned")

    items = cached_results + llm_results

    return {
        "ticker": ticker,
        "items": items,
        "token_usage": summary.get("token_usage", {}),
        "latency_ms": summary.get("latency_ms", 0),
    }