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

    async def _waterfall_refill(ticker: str, all_articles: list[dict], current_items: list[dict], *,
                                 min_usable: int = 3, hard_limit: int = 7) -> tuple[list[dict], int]:
        """
        Ensure we have at least `min_usable` usable summaries (relevance > 1) by fetching
        up to `hard_limit` total articles and summarizing any newly fetched ones.
        Returns (merged_items, fetched_extra_count).
        """
        try:
            usable = [i for i in current_items if int(i.get("relevance", 0) or 0) > 1]
        except Exception:
            usable = []

        if len(usable) >= min_usable:
            return current_items, 0

        if len(all_articles) >= hard_limit:
            return current_items, 0

        extra_needed = min(hard_limit - len(all_articles), hard_limit)  # conservative upper bound
        fetched_extra = []
        try:
            # try to fetch more articles using the fused news helper
            from app.ingest.fusion import fetch_fused_news
            # fetch up to extra_needed (may return duplicates/newer items)
            fetched_extra = await fetch_fused_news(ticker, top_k=extra_needed, days=7)
        except Exception:
            try:
                # best-effort fallback to any other fetcher if available
                from app.ingest.news import fetch as fetch_news  # type: ignore
                fetched_extra = await fetch_news(ticker, days=7, top_k=extra_needed)
            except Exception:
                log.exception("waterfall: failed to fetch extra articles for %s", ticker)
                fetched_extra = []

        # dedupe by URL against existing all_articles
        seen_urls = {a.get("url") for a in all_articles if a.get("url")}
        new_articles = [a for a in (fetched_extra or []) if a.get("url") and a.get("url") not in seen_urls][:extra_needed]
        if not new_articles:
            return current_items, 0

        # run summarizer only on newly fetched articles
        try:
            new_summary_resp = await summarize_items(new_articles, ticker=ticker)
            new_items = new_summary_resp.get("items") if isinstance(new_summary_resp, dict) else []
        except Exception:
            log.exception("waterfall: summarize_items failed for additional articles")
            new_items = []

        # merge keeping original order, avoiding duplicate URLs
        merged: list[dict] = []
        seen = set()
        for it in (current_items or []) + (new_items or []):
            u = (it or {}).get("url") or ""
            if u and u in seen:
                continue
            merged.append(it)
            if u:
                seen.add(u)

        pre = len([x for x in (current_items or []) if int((x.get("relevance") or 0) or 0) > 1])
        post = len([x for x in merged if int((x.get("relevance") or 0) or 0) > 1])
        fetched = len(new_articles)
        log.info("waterfall: pre=%d usable, post=%d usable, fetched=%d extra", pre, post, fetched)

        return merged, fetched

    # ...existing code that calls summarize_items() ...
    # Example integration point (adjust variable names to match your function):
    # assume `all_articles` is the list of article dicts you passed into summarize_items,
    # and `summary_resp` is the dict returned by summarize_items(...)
    try:
        summary_resp = await summarize_items(all_articles, ticker=ticker)
        items = summary_resp.get("items") or []
        # attempt waterfall refill if needed
        merged_items, extra_fetched = await _waterfall_refill(ticker, all_articles, items)
        # write merged_items back into the response structure used by the job
        if isinstance(summary_resp, dict):
            summary_resp["items"] = merged_items
    except Exception:
        log.exception("summarize job: initial summarize_items failed for %s", ticker)