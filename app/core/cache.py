from __future__ import annotations
import os
import json
import hashlib
import datetime
from typing import List, Dict, Optional, Any
import aiosqlite

CACHE_DB_PATH = os.getenv("CACHE_DB_PATH", "ari_cache.db")


"""
Simple async SQLite cache for articles, filings and summaries.

Usage notes:
- Call `await init_db()` once on app startup.
- Cache-first flow (suggested):
  * /api/v1/brief: cached = await cache_get_by_ticker(ticker)
      -> if cached empty: fetch live, await cache_upsert_items(...), return merged result
  * /api/v1/summarize: for each item, check await cache_get_summary(item["url"])
      -> if none: call LLM, then cache_upsert_summary(...)

All functions are async.
"""

async def init_db() -> None:
    print(f"Cache DB path: {CACHE_DB_PATH}")
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS articles(
                id INTEGER PRIMARY KEY,
                url_hash TEXT UNIQUE,
                url TEXT,
                ticker TEXT,
                source TEXT,
                title TEXT,
                published_at TEXT,
                lang TEXT,
                content TEXT,
                text_hash TEXT,
                created_at TEXT
            )"""
        )
        await db.execute(
            """CREATE TABLE IF NOT EXISTS filings(
                id INTEGER PRIMARY KEY,
                url_hash TEXT UNIQUE,
                url TEXT,
                ticker TEXT,
                source TEXT,
                title TEXT,
                category TEXT,
                published_at TEXT,
                lang TEXT,
                content TEXT,
                text_hash TEXT,
                summary_allowed INTEGER DEFAULT 1,
                created_at TEXT
            )"""
        )
        await db.execute(
            """CREATE TABLE IF NOT EXISTS summaries(
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                item_url_hash TEXT,
                bullets TEXT,
                why_it_matters TEXT,
                sentiment TEXT,
                created_at TEXT,
                UNIQUE(ticker, item_url_hash)
            )"""
        )

        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_ticker_created ON articles(ticker, created_at)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_filings_ticker_created ON filings(ticker, created_at)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_summaries_item ON summaries(item_url_hash)"
        )
        await db.commit()
    print("Cache DB initialized.")


def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def sha256_16(s: str) -> str:
    try:
        s2 = (s or "").encode("utf-8")
        h = hashlib.sha256(s2).hexdigest()
        return h[:16]
    except Exception:
        return "0" * 16


def url_to_hash(url: str) -> str:
    return sha256_16(url)


async def cache_upsert_items(items: List[Dict[str, Any]], *, kind: str, ticker: str) -> int:
    """
    Upsert list of items into articles (news) or filings table.
    Returns number of rows upserted.
    """
    if kind not in {"news", "filings"}:
        raise ValueError("kind must be 'news' or 'filings'")
    now = now_iso()
    rows = 0
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        for it in items:
            url = it.get("url", "") or ""
            url_hash = sha256_16(url)
            content = (it.get("translated_text") or it.get("content") or "")[:20000]
            text_hash = sha256_16(content)
            title = it.get("title") or ""
            source = it.get("source") or ""
            published_at = it.get("published_at") or ""
            lang = it.get("lang") or ""
            if kind == "news":
                await db.execute(
                    """INSERT OR REPLACE INTO articles
                       (url_hash,url,ticker,source,title,published_at,lang,content,text_hash,created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (url_hash, url, ticker, source, title, published_at, lang, content, text_hash, now),
                )
                rows += 1
            else:  # filings
                category = it.get("category") or ""
                summary_allowed = 1 if it.get("summary_allowed", True) else 0
                await db.execute(
                    """INSERT OR REPLACE INTO filings
                       (url_hash,url,ticker,source,title,category,published_at,lang,content,text_hash,summary_allowed,created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (url_hash, url, ticker, source, title, category, published_at, lang, content, text_hash, summary_allowed, now),
                )
                rows += 1
        await db.commit()
    print(f"cache_upsert_items: kind={kind} ticker={ticker} upserted={rows}")
    return rows


async def cache_upsert_summary(*, ticker: str, url_hash: str, bullets: List[str], why_it_matters: str, sentiment: str) -> None:
    now = now_iso()
    bullets_json = json.dumps(bullets, ensure_ascii=False)
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO summaries
               (ticker,item_url_hash,bullets,why_it_matters,sentiment,created_at)
               VALUES (?,?,?,?,?,?)""",
            (ticker, url_hash, bullets_json, why_it_matters, sentiment, now),
        )
        await db.commit()
    print(f"cache_upsert_summary: ticker={ticker} url_hash={url_hash} stored")


async def cache_get_by_ticker(ticker: str, *, max_age_hours: int = 24) -> Dict[str, List[Dict[str, Any]]]:
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)).replace(microsecond=0).isoformat() + "Z"
    news_out: List[Dict[str, Any]] = []
    filings_out: List[Dict[str, Any]] = []
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        # News
        async with db.execute(
            "SELECT title,url,published_at,source,lang,content FROM articles WHERE ticker=? AND created_at>=? ORDER BY created_at DESC",
            (ticker, cutoff),
        ) as cur:
            rows = await cur.fetchall()
            if rows:
                print(f"cache_get_by_ticker: news hit for {ticker} rows={len(rows)}")
            else:
                print(f"cache_get_by_ticker: news miss for {ticker}")
            for r in rows:
                title, url, published_at, source, lang, content = r
                news_out.append({
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "source": source,
                    "lang": lang,
                    "content": content,
                })

        # Filings
        async with db.execute(
            "SELECT title,url,published_at,source,category,lang,content,summary_allowed FROM filings WHERE ticker=? AND created_at>=? ORDER BY created_at DESC",
            (ticker, cutoff),
        ) as cur:
            rows = await cur.fetchall()
            if rows:
                print(f"cache_get_by_ticker: filings hit for {ticker} rows={len(rows)}")
            else:
                print(f"cache_get_by_ticker: filings miss for {ticker}")
            for r in rows:
                title, url, published_at, source, category, lang, content, summary_allowed = r
                filings_out.append({
                    "title": title,
                    "url": url,
                    "published_at": published_at,
                    "source": source,
                    "category": category,
                    "lang": lang,
                    "content": content,
                    "summary_allowed": bool(summary_allowed),
                })

    return {"news": news_out, "filings": filings_out}


async def cache_get_summary(url: str, *, max_age_hours: int = 24) -> Optional[Dict[str, Any]]:
    url_hash = sha256_16(url)
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)).replace(microsecond=0).isoformat() + "Z"
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        async with db.execute(
            "SELECT bullets,why_it_matters,sentiment,created_at FROM summaries WHERE item_url_hash=? AND created_at>=?",
            (url_hash, cutoff),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                print(f"cache_get_summary: miss for url_hash={url_hash}")
                return None
            bullets_json, why_it_matters, sentiment, created_at = row
            try:
                bullets = json.loads(bullets_json)
            except Exception:
                bullets = []
            print(f"cache_get_summary: hit for url_hash={url_hash}")
            return {"bullets": bullets, "why_it_matters": why_it_matters, "sentiment": sentiment}


__all__ = [
    "init_db",
    "now_iso",
    "sha256_16",
    "url_to_hash",
    "cache_upsert_items",
    "cache_upsert_summary",
    "cache_get_by_ticker",
    "cache_get_summary",
]