from __future__ import annotations
import logging
import time
from typing import List, Dict, Set, Tuple, Optional
from datetime import datetime

from app.core import settings
from app.core.metrics import record_metric
from app.ingest.adapters import newsapi, google_rss
from app.ingest.adapters.base import NewsItem, domain_from_url

log = logging.getLogger("ari.fusion")
# module import-time log of configured news sources (short, non-secret)
log.info("fusion: NEWS_SOURCES=%s", settings.NEWS_SOURCES)

# map available adapters
source_map = {
    "google_rss": google_rss,
    "newsapi": newsapi,
}


async def _call_adapter(adapter_module, ticker: str, days: int, topk: int, timeout_s: int) -> List[NewsItem]:
    try:
        return await adapter_module.fetch(ticker, days=days, topk=topk, timeout_s=timeout_s)
    except Exception:
        log.exception("adapter %s failed for %s", getattr(adapter_module, "__name__", str(adapter_module)), ticker)
        return []


def _parse_published_at(v: Optional[str]) -> float:
    if not v:
        return 0.0
    try:
        # accept ISO with trailing Z
        s = v.strip()
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except Exception:
        try:
            # fallback: try common format
            dt = datetime.strptime(v.split("T")[0], "%Y-%m-%d")
            return dt.timestamp()
        except Exception:
            return 0.0


async def fetch_fused_news(ticker: str, top_k: int = 10, days: int = 7) -> list[dict]:
    start = time.time()
    log.info("fusion.fetch_fused_news: ticker=%s top_k=%s days=%s", ticker, top_k, days)
    try:
        DAYS = settings.NEWS_DAYS
        TOPK = settings.NEWS_TOPK
        TIMEOUT = settings.NEWS_TIMEOUT_S
        ALLOW = {d.lower() for d in (settings.ALLOWLIST_DOMAINS or [])}
        BLOCK = {d.lower() for d in (settings.BLOCKLIST_DOMAINS or [])}
        KEYWORDS = [k.lower() for k in (settings.BLOCKLIST_KEYWORDS or [])]
        HARD_KEYWORDS = [k.lower() for k in (getattr(settings, "HARD_BLOCK_KEYWORDS", []) or [])]
        LANG = (settings.NEWS_LANGUAGE or "en").lower()
        DEBUG = bool(settings.DEBUG_NEWS_LOG)

        if DEBUG:
            log.info(
                "fusion.debug: LANG=%s ALLOW=%s BLOCK=%s KEYWORDS=%s",
                LANG,
                list(ALLOW),
                list(BLOCK),
                KEYWORDS,
            )

        collected: List[NewsItem] = []
        seen_urls: Set[str] = set()

        # helper: apply filtering/scoring to current collected set and return ranked items (top-K)
        def _filter_and_rank(collected_items: List[NewsItem]) -> List[NewsItem]:
            filtered: List[Tuple[NewsItem, float, int]] = []
            for it in collected_items:
                lang = (it.get("lang") or "en").lower()
                if lang != LANG:
                    if DEBUG:
                        log.info("drop:lang mismatch title=%r lang=%s need=%s", it.get("title"), lang, LANG)
                    continue

                # normalize title once
                title_lc = (it.get("title") or "").lower()

                # HARD block: always drop if any hard phrase matches (case-insensitive substring)
                if any(hk and hk in title_lc for hk in HARD_KEYWORDS):
                    for hk in HARD_KEYWORDS:
                        if hk and hk in title_lc:
                            log.info("drop:hard_kw title=%r kw=%r", it.get("title"), hk)
                    continue

                # normal domain/title filters below
                url = (it.get("url") or "").strip()
                dom = domain_from_url(url)
                dom_l = dom.lower()

                if ALLOW:
                    if dom_l not in ALLOW:
                        if DEBUG:
                            log.info("drop:not-allow title=%r dom=%s", it.get("title"), dom_l)
                        # if allowlist present, drop anything not in it
                        continue
                    domain_boost = 1
                else:
                    if dom_l in BLOCK:
                        if DEBUG:
                            log.info("drop:blocklisted title=%r dom=%s", it.get("title"), dom_l)
                        continue
                    domain_boost = 0

                # BLOCKLIST_KEYWORDS: case-insensitive substring match — always drop (no ticker exception)
                if any(kw and kw in title_lc for kw in KEYWORDS):
                    for kw in KEYWORDS:
                        if kw and kw in title_lc and DEBUG:
                            log.info("drop:keyword title=%r kw=%r", it.get("title"), kw)
                    continue

                score_time = _parse_published_at(it.get("published_at") or it.get("publishedAt"))
                score = score_time + (1000000.0 if domain_boost else 0.0)
                filtered.append((it, score, domain_boost))

                if DEBUG:
                    log.info(
                        "keep:title=%r dom=%s published_at=%r",
                        it.get("title"),
                        dom_l,
                        it.get("published_at") or it.get("publishedAt"),
                    )

            # sort and pick top-K
            def sort_key(tup: Tuple[NewsItem, float, int]) -> Tuple[float, str]:
                item, sc, domain_boost = tup
                src = (item.get("source") or "").lower()
                return (-sc, src)

            filtered.sort(key=sort_key)
            return [it for it, sc, dbst in filtered][:TOPK]

        # fetch in configured order, short-circuit if first source already yields TOPK after filtering
        for src in settings.NEWS_SOURCES:
            mod = source_map.get(src)
            if not mod:
                log.debug("unknown news source '%s' skipped", src)
                continue
            items = await _call_adapter(mod, ticker, days=DAYS, topk=TOPK * 2, timeout_s=TIMEOUT)
            log.info("fusion: source=%s returned=%d for %s", src, len(items), ticker)
            # append raw items (duplicates removed later)
            for it in items:
                url = (it.get("url") or "").strip()
                if not url:
                    continue
                lu = url.lower()
                if lu in seen_urls:
                    continue
                seen_urls.add(lu)
                # tag origin source for tie-breaking if adapter didn't set source
                if not it.get("source"):
                    it["source"] = src
                collected.append(it)

            # short-circuit: if this was the first enabled source, run filters and return if enough
            if len(collected) > 0 and settings.NEWS_SOURCES and settings.NEWS_SOURCES[0] == src:
                prelim = _filter_and_rank(collected)
                if len(prelim) >= TOPK:
                    log.info("fusion: short_circuit source=%s got=%d topk=%d", src, len(prelim), TOPK)
                    return prelim[:TOPK]

        # final filter+rank on all collected sources
        out_items = _filter_and_rank(collected)
        if DEBUG:
            log.info("fusion: after filters for %s collected=%d filtered=%d", ticker, len(collected), len(out_items))

        # final uniqueness by URL (fallback title)
        seen: set[str] = set()
        uniq_out: List[NewsItem] = []
        for it in out_items:
            k = (it.get("url") or "").strip() or (it.get("title") or "").strip()
            if not k or k in seen:
                continue
            seen.add(k)
            uniq_out.append(it)

        log.info(
            "fusion: final for %s returning=%d (unique)",
            ticker,
            len(uniq_out),
        )
        return uniq_out[:TOPK]
    except Exception:
        elapsed_ms = int((time.time() - start) * 1000) if start else 0
        try:
            # success if we have any items, otherwise mark as failed
            if isinstance(items, list) and len(items) > 0:
                record_metric("fetch", "rss", elapsed_ms, True)
            else:
                record_metric("fetch", "rss", elapsed_ms, False)
        except Exception:
            log.exception("metrics: failed to record fetch metric")
        log.exception("fusion: source=%s failed", ticker)
        try:
            record_metric("fetch", "rss", 0, False)
        except Exception:
            log.exception("metrics: failed to record fetch failure metric")
        raise
