from __future__ import annotations
import os
import logging
from typing import List, Dict, Any
import aiosqlite
import time
from datetime import datetime, timedelta, timezone
import httpx
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from app.core.cache import url_hash
from app.core.dates import now_iso as _now_iso
from app.ingest.adapters.base import domain_from_url
from app.core import settings
from app.ingest.extract import extract_bodies, extract_via_diffbot  # diffbot single-URL extractor
from app.ingest.fusion import fetch_fused_news
from app.ingest.google_rss_scrapingdog import search_google_news_scrapingdog
from app.observability.metrics import record_metric

log = logging.getLogger("ari.news")


def _dequery_url(u: str) -> str:
    try:
        p = urlparse((u or "").strip())
        if not p.scheme:
            return u
        # remove common tracking params
        q = parse_qsl(p.query, keep_blank_values=True)
        filtered = [(k, v) for k, v in q if not k.lower().startswith("utm_") and k.lower() not in ("fbclid", "gclid", "icn")]
        new_q = urlencode(filtered, doseq=True)
        return urlunparse((p.scheme, p.netloc, p.path or "", p.params or "", new_q, p.fragment or ""))
    except Exception:
        return u

async def fetch_newsapi_everything(q: str, from_iso: str, to_iso: str, page_size: int = 10, api_key: str = "", timeout_s: int = 8):
    raise NotImplementedError


async def fetch_news_for_ticker(ticker: str, max_items: int = 10, days: int = 7, require_content: bool = True) -> list[dict]:
    """
    Fetch candidate news items for `ticker`. If require_content is True we only
    return rows that include non-empty article.content >= 500 chars. If False
    return metadata rows (content may be empty) up to `max_items`.
    """
    key = os.getenv("NEWS_API_KEY", "") or getattr(settings, "NEWS_API_KEY", "")
    # resolve helper
    try:
        from app.core.lookup import resolve  # type: ignore
    except Exception:
        def resolve(t: str) -> Dict[str, List[str]]:
            return {"name": t, "aliases": [], "nse": t}

    info = resolve(ticker) or {}
    aliases = (getattr(settings, "ALIAS_MAP", {}) or {}).get(ticker, []) or info.get("aliases") or []

    # build query terms for ScrapingDog: ticker + aliases, quoting multi-word terms
    terms = [ticker] + [a for a in (aliases or []) if a]
    query_terms = [f'"{t}"' if " " in t else t for t in terms]
    query = " OR ".join(query_terms) or ticker

    # parts used for NewsAPI q construction (reuse quoted terms)
    parts: List[str] = query_terms.copy()

    # block phrases (extend existing keywords)
    extra_block_phrases = ["call options", "outlook for the week", "marathon", "outlook for the day"]
    blocked_keywords = [(k or "").lower() for k in (settings.BLOCKLIST_KEYWORDS or [])] + extra_block_phrases

    out: List[Dict] = []
    seen_hashes = set()
    # ensure we only log keep/drop once per unique URL (by url_hash)
    seen_logged: set[str] = set()

    # collect stub rows (no content/extraction) to return to caller
    rows: List[Dict[str, Any]] = []

    # ScrapingDog / google_rss path (if configured)
    if "google_rss" in (settings.NEWS_SOURCES or []):
        try:
            log.info("news.fetch: ticker=%s aliases=%s max_items=%d", ticker, aliases, max_items)
            t0 = time.time()
            sd_items = []
            try:
                sd_items = await search_google_news_scrapingdog(
                    ticker,
                    aliases=aliases,
                    topk=max_items,
                    country="in",
                    timeout_s=getattr(settings, "NEWS_TIMEOUT_S", 8),
                )
            finally:
                lat_ms = int((time.time() - t0) * 1000)
                try:
                    record_metric("fetch", "scrapingdog", lat_ms, ok=bool(sd_items))
                except Exception:
                    log.exception("metrics: failed to record scrapingdog metric")
            log.info("news: scrapingdog returned=%d for %s", len(sd_items), ticker)

            for r in (sd_items or [])[:max_items]:
                title = (r.get("title") or "").strip()
                url = (r.get("url") or "").strip()
                if not url:
                    continue
                url_norm = _dequery_url(url)
                h = url_hash(url_norm)
                if h in seen_hashes:
                    continue
                # basic title/keyword block check
                tl = title.lower()
                if ticker and ticker.lower() not in tl:
                    bad = False
                    for kw in blocked_keywords:
                        if kw and kw in tl:
                            bad = True
                            break
                    if bad:
                        continue
                seen_hashes.add(h)
                rows.append(
                    {
                        "ticker": ticker,
                        "title": title,
                        "url": url_norm,
                        "url_hash": h,
                        "source": (r.get("source") or domain_from_url(url_norm) or "").strip(),
                        "published_at": (r.get("published_hint") or "").strip(),
                        "lang": "en",
                        "content": "",
                        "created_at": _now_iso(),
                    }
                )
            # also prepare a simple out payload for immediate consumption
            out = [
                {
                    "title": (r.get("title") or "").strip(),
                    "url": (r.get("url") or "").strip(),
                    "published_at": (r.get("published_hint") or "").strip(),
                    "source": (r.get("source") or "") .strip(),
                    "lang": "en",
                    "snippet": (r.get("snippet") or "") if isinstance(r, dict) else "",
                }
                for r in (sd_items or [])[:max_items]
            ]
        except Exception:
            log.exception("news: scrapingdog fetch failed for %s", ticker)

    # If still lacking results, use NewsAPI as before to top-up — but only if configured
    if len(out) < max_items:
        # Only use NewsAPI if it's explicitly enabled in settings.NEWS_SOURCES
        if "newsapi" not in (getattr(settings, "NEWS_SOURCES", []) or []):
            log.info("news.fetch: skipping newsapi (disabled in NEWS_SOURCES)")
            na_items = []
        else:
            # NewsAPI enabled — ensure API key present
            if not key:
                log.info("news.fetch_news_for_ticker: NEWS_API_KEY missing, skipping NewsAPI top-up for %s", ticker)
                na_items = []
            else:
                q = " OR ".join(parts) if parts else ticker
                now = datetime.utcnow()
                to_iso = now.replace(microsecond=0).isoformat() + "Z"
                from_iso = (now - timedelta(days=days)).replace(microsecond=0).isoformat() + "Z"

                na_items = []
                try:
                    t0_na = time.time()
                    r_articles = []
                    try:
                        # call NewsAPI to top-up results (no DB writes here)
                        r_articles = await fetch_newsapi_everything(
                            q,
                            from_iso,
                            to_iso,
                            page_size=max_items,
                            api_key=key,
                            timeout_s=getattr(settings, "NEWS_TIMEOUT_S", 8),
                        )
                        na_items = r_articles or []
                    finally:
                        lat_ms_na = int((time.time() - t0_na) * 1000)
                        try:
                            record_metric("fetch", "newsapi", lat_ms_na, ok=bool(r_articles))
                        except Exception:
                            log.exception("metrics: failed to record newsapi metric")
                    log.info("news: newsapi returned=%d for %s", len(na_items), ticker)

                    for a in na_items:
                        if len(out) >= max_items:
                            break
                        url = (a.get("url") or "").strip()
                        if not url:
                            continue
                        url_norm = _dequery_url(url)
                        h = url_hash(url_norm)
                        if h in seen_hashes:
                            continue
                        title = (a.get("title") or "").strip()
                        tl = title.lower()
                        if ticker and ticker.lower() not in tl:
                            bad = False
                            for kw in blocked_keywords:
                                if kw and kw in tl:
                                    bad = True
                                    break
                            if bad:
                                continue
                        seen_hashes.add(h)
                        row = {
                            "ticker": ticker,
                            "title": title,
                            "url": url_norm,
                            "url_hash": h,
                            "source": (a.get("source") or {}).get("name") if isinstance(a.get("source"), dict) else a.get("source") or domain_from_url(url_norm) or "",
                            "published_at": a.get("publishedAt") or a.get("published_at") or "",
                            "lang": "en",
                            "content": "",
                            "created_at": _now_iso(),
                        }
                        rows.append(row)
                        item = {
                            "title": title,
                            "url": url_norm,
                            "published_at": a.get("publishedAt") or a.get("published_at") or "",
                            "source": (a.get("source") or {}).get("name") if isinstance(a.get("source"), dict) else a.get("source") or "",
                            "lang": "en",
                        }
                        _key = (item.get("url") or "").strip() or (item.get("title") or "").strip()
                        if not _key or _key in seen_logged:
                            continue
                        out.append(item)
                        seen_logged.add(_key)
                except Exception:
                    log.exception("news.fetch_news_for_ticker: NewsAPI request failed for %s", ticker)

    # Normalize/ensure required fields for every returned row and compute stable hash
    rows: List[Dict[str, Any]] = []
    for item in (out or []):
        # ensure url canonical form
        url_raw = (item.get("url") or item.get("link") or "").strip()
        url_norm = _dequery_url(url_raw) if url_raw else ""
        if not url_norm:
            # skip items without a usable URL
            continue

        title = (item.get("title") or item.get("headline") or "").strip()
        # compute full-hex URL hash
        try:
            h = url_hash(url_norm)
        except Exception:
            h = ""

        # source detection
        src = ""
        sfield = item.get("source") or item.get("source_name") or item.get("publisher")
        if isinstance(sfield, dict):
            src = (sfield.get("name") or "").strip()
        else:
            src = (sfield or "").strip()
        if not src:
            try:
                src = domain_from_url(url_norm) or ""
            except Exception:
                src = ""

        published_at = (item.get("publishedAt") or item.get("published_at") or item.get("published") or item.get("pubDate") or "").strip() or ""
        lang = (item.get("lang") or "en") or "en"

        rows.append({
            "ticker": ticker or "",
            "title": title,
            "url": url_norm,
            "url_hash": h,
            "source": src,
            "published_at": published_at,
            "lang": lang,
            "content": "",               # stub for fetch phase
            "created_at": _now_iso(),
        })

    # log one sample row for debugging
    log.info("news.fetch_news_for_ticker: sample=%s", rows[0] if rows else {})
    return rows

async def select_top_news_for_summary(ticker: str, max_items: int = 5, days: int = 7) -> List[Dict]:
    """
    Backwards-compatible helper used by other modules.
    Returns the top news items suitable for summarization (delegates to fetch_news_for_ticker).
    """
    try:
        items = await fetch_news_for_ticker(ticker, max_items=max_items, days=days)
        selected = items[:max_items]
        # Ensure each candidate has English text; extract via Diffbot when missing
        for item in selected:
            try:
                cur_text = (item.get("translated_text") or item.get("content") or "") or ""
                if not cur_text:
                    timeout_s = int(getattr(settings, "NEWS_TIMEOUT_S", 8) or 8)
                    text = await extract_via_diffbot(item.get("url") or "", timeout_s=timeout_s)
                    item["content"] = text or ""
                    item["translated_text"] = text or ""
                    log.info("news: extracted chars=%d for url=%s", len(text or ""), item.get("url") or "")
            except Exception:
                log.exception("news: extract_via_diffbot failed for url=%s", item.get("url") or "")
        return selected
    except Exception:
        log.exception("select_top_news_for_summary failed for %s", ticker)
        return []


async def extract_and_cache_bodies(ticker: str) -> int:
    """
    Find fresh articles for ticker with empty content, run extraction (Diffbot->fallback)
    via extract_bodies(), and update the articles table with extracted content.
    Returns number of articles updated.
    """
    if not ticker:
        return 0

    hours = int(getattr(settings, "FRESH_WINDOW_HOURS", 24) or 24)
    window_expr = f"datetime('now', '-{hours} hours')"
    limit = int(getattr(settings, "NEWS_TOPK", 5) or 5)

    rows: List[Dict[str, Any]] = []
    q = f"""
    SELECT url, url_hash, title
    FROM articles
    WHERE ticker = ? AND created_at >= {window_expr} AND (content IS NULL OR LENGTH(content) = 0)
    ORDER BY created_at DESC
    LIMIT ?
    """

    async with aiosqlite.connect(getattr(settings, "CACHE_DB_PATH", "./ari.db")) as db:
        try:
            async with db.execute(q, (ticker, limit)) as cur:
                fetched = await cur.fetchall()
        except Exception:
            log.exception("extract_and_cache_bodies: DB select failed for %s", ticker)
            return 0

        for url, url_h, cur_title in (fetched or []):
            rows.append({"url": url or "", "url_hash": url_h or "", "title": cur_title or ""})

    if not rows:
        return 0

    # run extraction (no DB writes inside)
    try:
        extracted_rows = await extract_bodies(rows)
    except Exception:
        log.exception("extract_and_cache_bodies: extract_bodies failed for %s", ticker)
        return 0

    updated = 0
    # persist results: update articles with extracted content (limit size)
    db_path = getattr(settings, "CACHE_DB_PATH", "./ari.db")
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        try:
            for r in (extracted_rows or []):
                url_h = (r.get("url_hash") or "").strip()
                content = r.get("content")
                title = r.get("title") or ""
                if not url_h or not content:
                    continue
                if len(content) < 200:  # skip very short extracts
                    continue
                try:
                    await db.execute(
                        """
                        UPDATE articles
                        SET content = ?, title = ?, lang = ?, created_at = ?
                        WHERE url_hash = ?
                        """,
                        (content[:15000], title or "", "en", _now_iso(), url_h),
                    )
                    updated += 1
                except Exception:
                    log.exception("extract_and_cache_bodies: update failed for url_hash=%s", url_h)
            await db.commit()
        except Exception:
            log.exception("extract_and_cache_bodies: DB update loop failed for %s", ticker)

    log.info("extract_and_cache_bodies: ticker=%s updated=%d", ticker, updated)
    return updated