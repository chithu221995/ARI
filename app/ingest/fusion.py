from __future__ import annotations
import logging
from typing import List, Dict, Set, Tuple, Optional
from datetime import datetime

from app.core import settings
from app.ingest.adapters import newscatcher, bing, newsapi
from app.ingest.adapters.base import NewsItem, domain_from_url

log = logging.getLogger("ari.news")


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


async def fetch_fused_news(ticker: str) -> List[NewsItem]:
    """
    Fetch from multiple adapters (in order), apply filters, dedupe, score, and return top-K items.
    """
    DAYS = settings.NEWS_DAYS
    TOPK = settings.NEWS_TOPK
    TIMEOUT = settings.NEWS_TIMEOUT_S
    ALLOW = {d.lower() for d in (settings.ALLOWLIST_DOMAINS or [])}
    BLOCK = {d.lower() for d in (settings.BLOCKLIST_DOMAINS or [])}
    KEYWORDS = [k.lower() for k in (settings.BLOCKLIST_KEYWORDS or [])]
    LANG = (settings.NEWS_LANGUAGE or "en").lower()
    DEBUG = bool(settings.DEBUG_NEWS_LOG)

    source_map = {
        "newscatcher": newscatcher,
        "bing": bing,
        "newsapi": newsapi,
    }

    collected: List[NewsItem] = []
    seen_urls: Set[str] = set()

    # Fetch in configured order
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

    # Filtering pipeline
    filtered: List[Tuple[NewsItem, float, int]] = []  # (item, score, allow_boost)
    for it in collected:
        lang = (it.get("lang") or "en").lower()
        if lang != LANG:
            continue

        url = (it.get("url") or "").strip()
        dom = domain_from_url(url)
        dom_l = dom.lower()

        # Domain allow/block logic
        if ALLOW:
            if dom_l not in ALLOW:
                # if allowlist present, drop anything not in it
                continue
            domain_boost = 1
        else:
            # no allowlist: drop only blocked domains
            if dom_l in BLOCK:
                continue
            domain_boost = 0

        # Keyword filters (unless title explicitly mentions ticker)
        title = (it.get("title") or "").lower()
        if ticker and ticker.lower() in title:
            pass  # keep regardless of keyword
        else:
            bad_kw = False
            for kw in KEYWORDS:
                if kw and kw in title:
                    bad_kw = True
                    break
            if bad_kw:
                continue

        # compute base score by published_at
        score_time = _parse_published_at(it.get("published_at") or it.get("publishedAt"))
        # prefer allowlist domains stronger
        score = score_time + (1000000.0 if domain_boost else 0.0)
        filtered.append((it, score, domain_boost))

    if DEBUG:
        log.info("fusion: after filters for %s collected=%d filtered=%d", ticker, len(collected), len(filtered))

    # sort by score desc, then by source name to break ties
    def sort_key(tup: Tuple[NewsItem, float, int]) -> Tuple[float, str]:
        item, sc, domain_boost = tup
        src = (item.get("source") or "").lower()
        # negative score to sort descending
        return (-sc, src)

    filtered.sort(key=sort_key)

    # select top-K
    out_items: List[NewsItem] = []
    for it, sc, dbst in filtered[:TOPK]:
        out_items.append(it)

    if DEBUG:
        log.info("fusion: final for %s returning=%d", ticker, len(out_items))

    return out_items