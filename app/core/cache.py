from __future__ import annotations
import os
import hashlib
import aiosqlite
import datetime
from typing import List, Dict, Optional, Any, Tuple

CACHE_DB_PATH = os.getenv("CACHE_DB_PATH", "ari_cache.db")
CACHE_DB = os.getenv("CACHE_DB", "cache.db")
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", "7"))


async def open_db():
    return await aiosqlite.connect(CACHE_DB_PATH)


async def init_db() -> None:
    """
    Initialize DB and run lightweight migrations ensuring expected columns (including 'lang' on articles).
    """
    db_path = CACHE_DB_PATH
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA foreign_keys = ON;")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                url TEXT,
                url_hash TEXT,
                title TEXT,
                source TEXT,
                published_at TEXT,
                content TEXT,
                translated_text TEXT,
                lang TEXT DEFAULT 'en',
                created_at TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT,
                url TEXT,
                url_hash TEXT,
                title TEXT,
                source TEXT,
                category TEXT,
                published_at TEXT,
                content TEXT,
                created_at TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS summaries (
                item_url_hash TEXT PRIMARY KEY,
                ticker TEXT,
                title TEXT,
                bullets_json TEXT,
                why_it_matters TEXT,
                sentiment TEXT,
                created_at TEXT
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS usage_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                model TEXT,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                cost_usd REAL
            )
            """
        )

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_email TEXT,
                ticker TEXT,
                date TEXT,
                news_score INTEGER,
                filings_score INTEGER,
                overall_score INTEGER,
                comments TEXT,
                created_at TEXT
            )
            """
        )

        await db.commit()

        # Ensure 'lang' column exists on articles table (idempotent)
        async with db.execute("PRAGMA table_info(articles);") as cur:
            rows = await cur.fetchall()
            existing = {r[1] for r in rows}
        if "lang" not in existing:
            try:
                await db.execute("ALTER TABLE articles ADD COLUMN lang TEXT DEFAULT 'en';")
                await db.commit()
            except Exception:
                pass

        # Normalize NULL/empty langs to 'en' (best-effort)
        try:
            await db.execute("UPDATE articles SET lang='en' WHERE lang IS NULL OR TRIM(lang) = ''")
            await db.commit()
        except Exception:
            pass

        # ensure indices
        try:
            await db.execute("CREATE INDEX IF NOT EXISTS idx_articles_ticker ON articles(ticker);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_filings_ticker ON filings(ticker);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_summaries_ticker ON summaries(ticker);")
            await db.commit()
        except Exception:
            pass


def now_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def sha256_16(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:16]


def url_hash(url: str) -> str:
    """
    Stable SHA256 hex for a URL (full hex, not truncated).
    """
    u = (url or "").strip()
    return hashlib.sha256(u.encode("utf-8")).hexdigest()


def url_to_hash(url: str) -> str:
    return sha256_16((url or "").strip())


# New helpers for upserting and reading cached items (news/articles and filings)
async def cache_upsert_items(items: List[Dict[str, Any]], *, kind: str = "news", ticker: str) -> int:
    """
    Upsert items into cache. Prototype is news-only:
    - If kind != "news", do nothing (filings removed in prototype).
    """
    if not items:
        return 0
    if kind != "news":
        # filings removed in prototype
        return 0

    inserted = 0
    db_path = CACHE_DB_PATH
    async with aiosqlite.connect(db_path) as db:
        for it in items:
            url = (it.get("url") or "").strip()
            url_hash_val = url_to_hash(url)
            title = it.get("title") or ""
            source = it.get("source") or ""
            published_at = it.get("published_at") or it.get("publishedAt") or ""
            content = it.get("content") or it.get("translated_text") or ""
            lang = it.get("lang") or "en"

            await db.execute(
                """
                INSERT OR REPLACE INTO articles
                (ticker, url, url_hash, title, source, published_at, content, translated_text, lang, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    ticker,
                    url,
                    url_hash_val,
                    title,
                    source,
                    published_at,
                    content,
                    it.get("translated_text") or None,
                    lang,
                    now_iso(),
                ),
            )
            inserted += 1
        await db.commit()
    return inserted


async def cache_get_by_ticker(ticker: str, *, max_age_hours: int = 24) -> Dict[str, Any]:
    """
    Return cached news for a ticker. Filings are removed for prototype.
    Returns {"news": [...]} only.
    """
    out: Dict[str, Any] = {"news": []}
    if not ticker:
        return out
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)).replace(microsecond=0).isoformat() + "Z"
    db_path = CACHE_DB_PATH
    async with aiosqlite.connect(db_path) as db:
        try:
            async with db.execute(
                "SELECT url, title, source, published_at, content, lang FROM articles WHERE ticker = ? AND created_at >= ? AND lang = ? ORDER BY published_at DESC LIMIT 50",
                (ticker, cutoff, "en"),
            ) as cur:
                rows = await cur.fetchall()
                for url, title, source, published_at, content, lang in rows:
                    out["news"].append(
                        {
                            "url": url,
                            "title": title,
                            "source": source,
                            "published_at": published_at,
                            "content": content,
                            "lang": lang or "en",
                        }
                    )
        except Exception:
            out["news"] = []

    out["news"] = out["news"][:5]
    return out


async def cache_upsert_summaries(summaries: List[Dict[str, Any]]) -> int:
    """
    Upsert a list of summaries into the summaries table.
    Returns number of rows inserted/updated.
    """
    if not summaries:
        return 0
    inserted = 0
    db_path = CACHE_DB_PATH
    async with aiosqlite.connect(db_path) as db:
        for it in summaries:
            item_url_hash = (it.get("item_url_hash") or "").strip()
            ticker = it.get("ticker") or ""
            title = it.get("title") or ""
            bullets_json = it.get("bullets_json") or ""
            why_it_matters = it.get("why_it_matters") or ""
            sentiment = it.get("sentiment") or "Neutral"

            await db.execute(
                """
                INSERT OR REPLACE INTO summaries
                (item_url_hash, ticker, title, bullets_json, why_it_matters, sentiment, created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    item_url_hash,
                    ticker,
                    title,
                    bullets_json,
                    why_it_matters,
                    sentiment,
                    now_iso(),
                ),
            )
            inserted += 1
        await db.commit()
    return inserted


async def cache_get_summaries_map(conn: aiosqlite.Connection, url_hashes: List[str]) -> Dict[str, Dict]:
    """
    Return a mapping {item_url_hash: {bullets, why_it_matters, sentiment, created_at}}
    for the given list of url_hashes. If multiple rows exist per hash, keep the latest
    (ORDER BY created_at DESC).

    Notes:
    - Expects an open aiosqlite.Connection object as `conn`.
    - Dedupes incoming hashes to avoid excessive SQL placeholders.
    """
    if not url_hashes:
        return {}

    wanted = list(dict.fromkeys(h for h in url_hashes if h))
    if not wanted:
        return {}

    qmarks = ",".join("?" * len(wanted))
    sql = f"""
    SELECT item_url_hash, bullets_json, why_it_matters, sentiment, created_at
    FROM summaries
    WHERE item_url_hash IN ({qmarks})
    ORDER BY created_at DESC
    """

    out: Dict[str, Dict] = {}
    async with conn.execute(sql, wanted) as cur:
        async for row in cur:
            h, bullets_json, why, sentiment, created_at = row
            if h not in out:
                # normalize bullets: store as list when possible
                bullets = bullets_json or ""
                try:
                    if isinstance(bullets, str):
                        import json as _json
                        bullets_parsed = _json.loads(bullets) if bullets.strip() else []
                    else:
                        bullets_parsed = bullets
                except Exception:
                    bullets_parsed = []
                out[h] = {
                    "bullets": bullets_parsed,
                    "why_it_matters": why or "",
                    "sentiment": (sentiment or "Neutral"),
                    "created_at": created_at,
                }
    return out


async def cache_get_missing_items_for_summary(conn: aiosqlite.Connection, url_hashes: List[str]) -> List[str]:
    """
    Given a list of url_hashes, return only those that are missing in the summaries table.
    Used by summarize_job to find which items still need to be summarized.
    """
    if not url_hashes:
        return []
    wanted = list(dict.fromkeys(h for h in url_hashes if h))
    if not wanted:
        return []
    qmarks = ",".join("?" * len(wanted))
    sql = f"SELECT item_url_hash FROM summaries WHERE item_url_hash IN ({qmarks})"
    existing = set()
    async with conn.execute(sql, wanted) as cur:
        async for row in cur:
            existing.add(row[0])
    missing = [h for h in wanted if h not in existing]
    return missing


async def cache_stats() -> Dict[str, Any]:
    """
    Return counts for tables and latest updated timestamp across tables.
    """
    out = {"articles": 0, "filings": 0, "summaries": 0, "last_updated": None}
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        # counts
        for table, key in [("articles", "articles"), ("filings", "filings"), ("summaries", "summaries")]:
            try:
                async with db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                    row = await cur.fetchone()
                    out[key] = row[0] if row else 0
            except Exception:
                out[key] = 0
        # latest created_at across tables
        latest = None
        for table in ("articles", "filings", "summaries"):
            try:
                async with db.execute(f"SELECT MAX(created_at) FROM {table}") as cur:
                    row = await cur.fetchone()
                    ts = row[0] if row else None
                    if ts and (latest is None or ts > latest):
                        latest = ts
            except Exception:
                pass
        out["last_updated"] = latest
    return out


async def purge_ticker(ticker: str) -> Dict[str, int]:
    """
    Delete all cached rows for a ticker across articles/filings/summaries.
    Note: summaries are keyed by item_url_hash; we purge those whose ticker matches.
    """
    if not ticker:
        return {"articles": 0, "filings": 0, "summaries": 0}
    counts = {"articles": 0, "filings": 0, "summaries": 0}
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        await db.execute("DELETE FROM articles WHERE ticker = ?", (ticker,))
        counts["articles"] = db.total_changes
        await db.execute("DELETE FROM filings WHERE ticker = ?", (ticker,))
        counts["filings"] = db.total_changes - counts["articles"]
        await db.execute("DELETE FROM summaries WHERE ticker = ?", (ticker,))
        counts["summaries"] = db.total_changes - counts["articles"] - counts["filings"]
        await db.commit()
    return counts


async def purge_older_than(iso_cutoff: str) -> Dict[str, int]:
    """
    Delete rows older than cutoff ISO (on created_at).
    """
    counts = {"articles": 0, "filings": 0, "summaries": 0}
    if not iso_cutoff:
        return counts
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        await db.execute("DELETE FROM articles WHERE created_at < ?", (iso_cutoff,))
        counts["articles"] = db.total_changes
        await db.execute("DELETE FROM filings WHERE created_at < ?", (iso_cutoff,))
        counts["filings"] = db.total_changes - counts["articles"]
        await db.execute("DELETE FROM summaries WHERE created_at < ?", (iso_cutoff,))
        counts["summaries"] = db.total_changes - counts["articles"] - counts["filings"]
        await db.commit()
    return counts


async def purge_expired(now_utc: datetime.datetime | None = None) -> Tuple[int,int,int]:
    """
    Delete rows older than TTL from articles, summaries, and (if exists) filings.
    Returns tuple: (articles_deleted, summaries_deleted, filings_deleted)
    """
    cutoff = (now_utc or datetime.datetime.utcnow()) - datetime.timedelta(days=CACHE_TTL_DAYS)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    a = s = f = 0
    async with aiosqlite.connect(CACHE_DB) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        # articles
        try:
            cur = await db.execute("DELETE FROM articles WHERE created_at < ?", (cutoff_iso,))
            a = cur.rowcount or 0
        except Exception:
            a = 0
        # summaries
        try:
            cur = await db.execute("DELETE FROM summaries WHERE created_at < ?", (cutoff_iso,))
            s = cur.rowcount or 0
        except Exception:
            s = 0
        # filings (optional)
        try:
            cur = await db.execute("DELETE FROM filings WHERE created_at < ?", (cutoff_iso,))
            f = cur.rowcount or 0
        except Exception:
            f = 0
        await db.commit()
    return a, s, f


__all__ = [
    "open_db",
    "init_db",
    "now_iso",
    "sha256_16",
    "url_hash",
    "cache_upsert_items",
    "cache_get_by_ticker",
    "cache_upsert_summaries",
    "cache_get_summaries_map",
    "cache_stats",
    "purge_ticker",
    "purge_older_than",
    "cache_get_missing_items_for_summary",
    "purge_expired",
]