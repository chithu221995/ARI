from __future__ import annotations
import os
import asyncio
from typing import List, Dict, Any, Optional

from app.summarize.llm import summarize_items
from app.core.cache import cache_get_missing_items_for_summary, cache_upsert_summaries

SUMMARIZE_MAX_PER_TICKER = int(os.getenv("SUMMARIZE_MAX_PER_TICKER", "6"))
SUMMARIZE_MIN_CHARS = int(os.getenv("SUMMARIZE_MIN_CHARS", "500"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


async def summarize_cached_and_upsert(app, ticker: str) -> Dict[str, Any]:
    """
    Read missing items for `ticker` from cache, call the LLM in batches,
    and upsert returned summaries into the summaries table.

    Returns: {"ticker": ticker, "summarized": N, "skipped": M}
    """
    max_items = SUMMARIZE_MAX_PER_TICKER
    min_chars = SUMMARIZE_MIN_CHARS
    model = OPENAI_MODEL

    items = await cache_get_missing_items_for_summary(ticker, max_age_hours=24, max_items=max_items, min_chars=min_chars)
    if not items:
        return {"ticker": ticker, "summarized": 0, "skipped": 0}

    # Prepare batches of up to 5
    batch_size = 5
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    total_summarized = 0
    total_skipped = 0
    parsed_upserts: List[Dict[str, Any]] = []

    for idx, batch in enumerate(batches):
        # Build payload for LLM: preserve title + url + text
        payload = []
        url_map = {}
        title_pub_map = {}
        for it in batch:
            title = it.get("title", "") or ""
            url = it.get("url", "") or ""
            text = (it.get("translated_text") or "").strip()
            payload.append({"title": title, "url": url, "text": text})
            url_map[url] = it  # original item
            key = (title.strip(), it.get("published_at") or "")
            title_pub_map[key] = it

        # Call summarizer
        try:
            resp = await summarize_items(payload, ticker=ticker, model=model)
            results = resp.get("items", []) if isinstance(resp, dict) else []
        except Exception as e:
            print(f"[summarize_job] summarize_items failed for {ticker} batch {idx}: {e}")
            results = []

        # Map LLM outputs back to url_hash and prepare upsert payloads
        for r in results:
            # try match by url first
            url = r.get("url", "") or ""
            orig = url_map.get(url)
            if not orig:
                # fallback to title+published_at
                key = (r.get("title", "") or "").strip(), r.get("published_at", "") or ""
                orig = title_pub_map.get(key)
            if not orig:
                # unable to map; skip
                print(f"[summarize_job] unable to map LLM result to original item for ticker={ticker}, title={r.get('title')}")
                total_skipped += 1
                continue

            url_hash = orig.get("url_hash") or orig.get("url_hash") or ""
            if not url_hash:
                print(f"[summarize_job] missing url_hash for item {orig.get('url')}, skipping upsert")
                total_skipped += 1
                continue

            parsed_upserts.append({
                "url_hash": url_hash,
                "title": r.get("title") or orig.get("title", ""),
                "bullets": r.get("bullets", []) or [],
                "why_it_matters": r.get("why_it_matters", "") or "",
                "sentiment": r.get("sentiment", "") or "",
            })
            total_summarized += 1

        # small jitter between batches
        await asyncio.sleep(0.15 * idx)

    # Upsert all parsed summaries
    if parsed_upserts:
        try:
            inserted = await cache_upsert_summaries(ticker, parsed_upserts)
            print(f"[summarize_job] {ticker}: upserted {inserted} summaries")
        except Exception as e:
            print(f"[summarize_job] cache_upsert_summaries failed for {ticker}: {e}")

    return {"ticker": ticker, "summarized": total_summarized, "skipped": total_skipped}