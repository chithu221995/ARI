from __future__ import annotations
from typing import List, Dict, Any, Optional

from click import Tuple

from app.core import settings

import os
import aiosqlite
from datetime import datetime, timezone, timedelta
import logging
import hashlib

log = logging.getLogger("ari.news")

# how many days to keep cache rows
CACHE_TTL_DAYS = int(os.getenv("CACHE_TTL_DAYS", "7"))

# canonical DB path used by this module
CACHE_DB_PATH = os.getenv("SQLITE_PATH", "./ari.db")

async def get_db():
    """Return an aiosqlite connection to the canonical DB used by the app."""
    return await aiosqlite.connect(CACHE_DB_PATH)


async def open_db():
    return await aiosqlite.connect(CACHE_DB_PATH)


async def init_db():
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        # ensure tables
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                url_hash TEXT,
                title TEXT,
                source TEXT,
                published_at TEXT,
                lang TEXT,
                content TEXT,
                created_at INTEGER
            )
            """
        )

        # ensure summaries table has the desired schema (idempotent)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS summaries (
                item_url_hash TEXT PRIMARY KEY,
                ticker TEXT NOT NULL,
                title TEXT,
                url TEXT,
                bullets TEXT,
                why_it_matters TEXT,
                sentiment TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now','utc'))
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_summaries_ticker_created ON summaries(ticker, created_at)"
        )
        log.info("cache.init_db: summaries table ensured")

        # --- existing schema patches for articles/summaries (kept as-is) ---
        # enable WAL for safer concurrency
        try:
            await db.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass

        try:
            await db.execute("ALTER TABLE summaries ADD COLUMN title TEXT DEFAULT ''")
            log.info("cache.init_db: added summaries.title column")
        except Exception:
            log.debug("cache.init_db: summaries.title exists or ALTER failed", exc_info=False)

        try:
            await db.execute("ALTER TABLE articles ADD COLUMN translated_text TEXT DEFAULT ''")
            log.info("cache.init_db: added articles.translated_text column")
        except Exception:
            log.debug("cache.init_db: articles.translated_text exists or ALTER failed", exc_info=False)

        # add text_hash if missing (idempotent)
        try:
            await db.execute("ALTER TABLE articles ADD COLUMN text_hash TEXT DEFAULT ''")
            log.info("cache.init_db: added articles.text_hash column")
        except Exception:
            log.debug("cache.init_db: articles.text_hash exists or ALTER failed", exc_info=False)

        # ensure helpful indexes exist
        try:
            await db.execute("CREATE INDEX IF NOT EXISTS idx_articles_ticker_created ON articles(ticker, created_at)")
            # ensure a UNIQUE index on url_hash so ON CONFLICT(url_hash) DO UPDATE works reliably
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url_hash ON articles(url_hash)")
            # informational log so we can see the migration/ensure step
            log.info("cache.init_db: ensured unique idx_articles_url_hash")
        except Exception:
            log.debug("cache.init_db: index creation failed (ignored)", exc_info=False)

        await db.commit()


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


async def ensure_articles_schema(db_path: str) -> None:
    """
    Idempotent: ensure articles table and indexes exist.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS articles (
      id INTEGER PRIMARY KEY,
      ticker TEXT,
      title TEXT,
      url TEXT,
      url_hash TEXT UNIQUE,
      source TEXT,
      published_at TEXT,
      lang TEXT,
      content TEXT,
      created_at TEXT
    );
    """
    create_idx_hash = "CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_url_hash ON articles(url_hash);"
    create_idx_ticker = "CREATE INDEX IF NOT EXISTS idx_articles_ticker_created ON articles(ticker, created_at);"

    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(create_table_sql)
            await db.execute(create_idx_hash)
            await db.execute(create_idx_ticker)
            await db.commit()
            log.debug("ensure_articles_schema: ensured articles schema at %s", db_path)
    except Exception:
        log.exception("ensure_articles_schema: failed for %s", db_path)


async def ensure_summaries_schema(db_path: str) -> None:
    """
    Idempotent: ensure summaries table and indexes exist.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS summaries (
      id INTEGER PRIMARY KEY,
      item_url_hash TEXT UNIQUE,
      ticker TEXT,
      title TEXT,
      why_it_matters TEXT,
      bullets TEXT,
      sentiment TEXT,
      relevance INTEGER,
      created_at TEXT,
      url TEXT
    );
    """
    create_idx_hash = "CREATE UNIQUE INDEX IF NOT EXISTS idx_summaries_item_url_hash ON summaries(item_url_hash);"
    create_idx_ticker = "CREATE INDEX IF NOT EXISTS idx_summaries_ticker_created ON summaries(ticker, created_at);"

    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(create_table_sql)
            # ensure unique index on item_url_hash (idempotent)
            await db.execute(create_idx_hash)
            await db.execute(create_idx_ticker)
            await db.commit()
            log.debug("ensure_summaries_schema: ensured summaries schema at %s", db_path)
    except Exception:
        log.exception("ensure_summaries_schema: failed for %s", db_path)


# New helpers for upserting and reading cached items (news/articles and filings)
async def cache_upsert_items(
    rows: List[Dict[str, Any]],
    kind: str = "news",
    ticker: Optional[str] = None,
    db_path: Optional[str] = None
) -> int:
    """
    Upsert news/filing items into cache.
    Now stores news_age (hours) from fetch time.
    """
    if not db_path:
        db_path = CACHE_DB_PATH
    
    if not rows:
        return 0
    
    now = now_iso()
    params = []
    
    for r in rows:
        url = (r.get("url") or "").strip()
        if not url:
            continue
        
        h = url_hash(url)
        t = (ticker or r.get("ticker") or "").strip().upper()
        source = (r.get("source") or "").strip()
        title = (r.get("title") or "").strip()
        published_at = (r.get("published_at") or "").strip()
        news_age = r.get("news_age")  # NEW: Get news_age in hours
        lang = (r.get("lang") or "en").strip()
        content = (r.get("content") or "").strip()
        translated_text = (r.get("translated_text") or "").strip()
        
        params.append((
            url,
            h,
            t,
            source,
            title,
            published_at,
            news_age,  # NEW: Store age in hours
            lang,
            content,
            translated_text,
            now,
        ))
    
    if not params:
        return 0
    
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.executemany(
                """
                INSERT INTO articles
                (url, url_hash, ticker, source, title, published_at, news_age, lang, content, translated_text, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url_hash) DO UPDATE SET
                    title=excluded.title,
                    source=excluded.source,
                    published_at=excluded.published_at,
                    news_age=excluded.news_age,
                    lang=excluded.lang,
                    content=COALESCE(excluded.content, articles.content),
                    translated_text=COALESCE(excluded.translated_text, articles.translated_text)
                """,
                params
            )
            await db.commit()
        
        log.info(f"cache_upsert_items: upserted {len(params)} rows with news_age")
        return len(params)
        
    except Exception as e:
        log.exception("cache_upsert_items: failed")
        return 0


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


async def cache_upsert_summaries(rows: list[dict]) -> int:
    """
    Upsert summary rows into the summaries table.
    Expects each row to contain keys:
      item_url_hash, ticker, title, why_it_matters, sentiment, relevance, created_at, url
    Returns number of rows actually inserted/updated (computed from sqlite total_changes).
    """
    if not rows:
        return 0

    db_path = getattr(settings, "CACHE_DB_PATH", "./ari.db")
    insert_sql = """
    INSERT INTO summaries
      (item_url_hash, ticker, title, why_it_matters, sentiment, relevance, created_at, url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(item_url_hash) DO UPDATE SET
      ticker = excluded.ticker,
      title = excluded.title,
      why_it_matters = excluded.why_it_matters,
      sentiment = excluded.sentiment,
      relevance = excluded.relevance,
      created_at = excluded.created_at,
      url = excluded.url
    ;
    """

    params = []
    for r in rows:
        # bind provided relevance if numeric; otherwise default to 5
        rel = r.get("relevance")
        try:
            rel_val = int(str(rel).strip()) if rel is not None else 5
        except Exception:
            rel_val = 5

        params.append(
            (
                r.get("item_url_hash"),
                r.get("ticker"),
                r.get("title"),
                r.get("why_it_matters"),
                r.get("sentiment"),
                rel_val,
                r.get("created_at"),
                r.get("url"),
            )
        )

    try:
        async with aiosqlite.connect(db_path) as db:
            # measure total_changes before/after to compute actual writes
            before = getattr(db, "total_changes", 0)
            await db.executemany(insert_sql, params)
            await db.commit()
            after = getattr(db, "total_changes", 0)
            upserted = max(0, int(after) - int(before))
        return upserted
    except Exception:
        log.exception("cache_upsert_summaries: upsert failed")
        return 0


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
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
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


async def _db_path() -> str:
    return os.getenv("SQLITE_PATH", "./ari.db")


async def _table_exists(db: aiosqlite.Connection, name: str) -> bool:
    try:
        cur = await db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
        )
        row = await cur.fetchone()
        await cur.close()
        return bool(row)
    except Exception:
        return False


async def count_articles_rows() -> int:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            if not await _table_exists(db, "articles"):
                return 0
            cur = await db.execute("SELECT COUNT(*) FROM articles")
            row = await cur.fetchone()
            await cur.close()
            return int(row[0] if row else 0)
    except Exception as e:
        log.exception("count_articles_rows failed: %s", e)
        return 0


async def count_summaries_rows() -> int:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            if not await _table_exists(db, "summaries"):
                return 0
            cur = await db.execute("SELECT COUNT(*) FROM summaries")
            row = await cur.fetchone()
            await cur.close()
            return int(row[0] if row else 0)
    except Exception as e:
        log.exception("count_summaries_rows failed: %s", e)
        return 0


META_CREATE_SQL = "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)"

async def set_meta(key: str, value: str) -> None:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            await db.execute(META_CREATE_SQL)
            await db.execute(
                "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (key, value),
            )
            await db.commit()
    except Exception:
        log.exception("set_meta failed for key=%s", key)

async def get_meta(key: str) -> Optional[str]:
    try:
        async with aiosqlite.connect(await _db_path()) as db:
            await db.execute(META_CREATE_SQL)
            cur = await db.execute("SELECT v FROM meta WHERE k=?", (key,))
            row = await cur.fetchone()
            await cur.close()
            return row[0] if row else None
    except Exception:
        log.exception("get_meta failed for key=%s", key)
        return None

# single canonical export list
__all__ = [
    "open_db",
    "init_db",
    "now_iso",
    "sha256_16",
    "url_hash",
    "url_to_hash",
    "cache_upsert_items",
    "cache_get_by_ticker",
    "cache_upsert_summaries",
    "cache_get_summaries_map",
    "cache_get_missing_items_for_summary",
    "cache_stats",
    "purge_ticker",
    "purge_older_than",
    "purge_expired",
    "count_articles_rows",
    "count_summaries_rows",
    "set_meta",
    "get_meta",
    "get_cached_summary",      # NEW
    "get_cached_articles",     # NEW
]


async def _fresh_cutoff_hours() -> int:
    """
    Return configured freshness window in hours (int).
    """
    return int(getattr(settings, "FRESH_WINDOW_HOURS", 24) or 24)


async def has_fresh_urls(db, ticker: str) -> bool:
    """
    Return True if there are articles for ticker newer than the freshness window.
    """
    hours = await _fresh_cutoff_hours()
    q = "SELECT COUNT(*) FROM articles WHERE ticker=? AND created_at > datetime('now', ?)"
    async with db.execute(q, (ticker, f'-{hours} hours')) as cur:
        row = await cur.fetchone()
    return (row[0] or 0) > 0


async def has_fresh_content(db, ticker: str) -> bool:
    """
    Return True if there are articles with non-empty content for ticker newer than the freshness window.
    """
    hours = await _fresh_cutoff_hours()
    q = (
        "SELECT COUNT(*) FROM articles "
        "WHERE ticker=? AND created_at > datetime('now', ?) AND content IS NOT NULL AND LENGTH(content) > 0"
    )
    async with db.execute(q, (ticker, f'-{hours} hours')) as cur:
        row = await cur.fetchone()
    return (row[0] or 0) > 0


async def has_fresh_summaries(db, ticker: str) -> bool:
    """
    Return True if there are summaries for ticker newer than the freshness window.
    """
    hours = await _fresh_cutoff_hours()
    q = "SELECT COUNT(*) FROM summaries WHERE ticker=? AND created_at > datetime('now', ?)"
    async with db.execute(q, (ticker, f'-{hours} hours')) as cur:
        row = await cur.fetchone()
    return (row[0] or 0) > 0


async def ensure_llm_usage_schema(db_path: str) -> None:
    sql_table = """
    CREATE TABLE IF NOT EXISTS llm_usage (
      date TEXT,
      provider TEXT,
      requests INTEGER DEFAULT 0,
      last_minute_count INTEGER DEFAULT 0,
      last_minute_ts INTEGER DEFAULT 0,
      PRIMARY KEY (date, provider)
    );
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(sql_table)
            await db.commit()
            log.debug("ensure_llm_usage_schema: ok db=%s", db_path)
    except Exception:
        log.exception("ensure_llm_usage_schema: failed for %s", db_path)


def _today_ist_str() -> str:
    # IST (UTC+5:30) date key
    ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    return ist.date().isoformat()


async def llm_allow_request(db_path: str, provider: str, rpm_cap: int, daily_cap: int) -> tuple[bool, int, bool]:
    """
    Enforce a simple RPM token limit and daily cap for provider.
    Returns (allowed:bool, wait_ms:int, daily_cap_reached:bool)
    - wait_ms: ms client should wait before retry (0 if allowed)
    - daily_cap_reached: True when daily cap reached (requests >= daily_cap)
    """
    await ensure_llm_usage_schema(db_path)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    today = _today_ist_str()

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = None
        # ensure row exists
        await db.execute(
            "INSERT OR IGNORE INTO llm_usage(date, provider, requests, last_minute_count, last_minute_ts) VALUES (?, ?, 0, 0, ?)",
            (today, provider, 0),
        )
        await db.commit()

        # load current
        async with db.execute("SELECT requests, last_minute_count, last_minute_ts FROM llm_usage WHERE date = ? AND provider = ?", (today, provider)) as cur:
            row = await cur.fetchone()
            if row:
                requests, last_minute_count, last_minute_ts = int(row[0]), int(row[1]), int(row[2])
            else:
                requests, last_minute_count, last_minute_ts = 0, 0, 0

        # daily cap check
        if daily_cap is not None and daily_cap > 0 and requests >= daily_cap:
            return False, 0, True

        # rpm check using last_minute_ts window
        window_ms = 60_000
        elapsed = now_ms - last_minute_ts if last_minute_ts else window_ms + 1
        if elapsed >= window_ms:
            # reset minute bucket
            current_minute_count = 1
            new_last_minute_ts = now_ms
        else:
            current_minute_count = last_minute_count + 1
            new_last_minute_ts = last_minute_ts

        if rpm_cap is not None and rpm_cap > 0 and current_minute_count > rpm_cap:
            # compute wait until minute window expires
            wait_ms = max(0, window_ms - elapsed)
            return False, wait_ms, False

        # permitted: update counters
        await db.execute(
            "UPDATE llm_usage SET requests = requests + 1, last_minute_count = ?, last_minute_ts = ? WHERE date = ? AND provider = ?",
            (current_minute_count, new_last_minute_ts, today, provider),
        )
        await db.commit()
    return True, 0, False


import aiosqlite  # ensure aiosqlite is available for new helpers
from contextlib import asynccontextmanager


@asynccontextmanager
async def _open_db_fk(path: str):
    """
    Async context manager that yields a fresh aiosqlite connection with
    PRAGMA foreign_keys=ON. Use: async with _open_db_fk(path) as db:
    """
    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA foreign_keys=ON;")
        yield db
    finally:
        try:
            await db.close()
        except Exception:
            pass


async def ensure_users_schema(db_path: str) -> None:
    async with _open_db_fk(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                email       TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL
            )
            """
        )
        await db.commit()


async def ensure_user_tickers_schema(db_path: str) -> None:
    async with _open_db_fk(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS user_tickers (
                email         TEXT NOT NULL,
                ticker        TEXT NOT NULL,
                company_name  TEXT NOT NULL,
                aliases_json  TEXT,       -- JSON string or NULL
                rank          INTEGER NOT NULL, -- 1..7
                created_at    TEXT NOT NULL,
                FOREIGN KEY(email) REFERENCES users(email) ON DELETE CASCADE
            )
            """
        )
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_user_tickers_email_rank  ON user_tickers(email, rank)"
        )
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_user_tickers_email_tkr  ON user_tickers(email, ticker)"
        )
        await db.commit()


async def ensure_ticker_catalog_schema(db_path: str) -> None:
    async with _open_db_fk(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS ticker_catalog (
                ticker        TEXT PRIMARY KEY,
                company_name  TEXT NOT NULL,
                aliases_json  TEXT,       -- JSON array of strings or NULL
                active        INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        await db.commit()


async def ensure_runs_schema(db_path: str) -> None:
    async with _open_db_fk(db_path) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job         TEXT NOT NULL,           -- fetch|extract|summarize|email
                ticker      TEXT,                    -- nullable for email fan-out
                started_at  TEXT NOT NULL,
                ended_at    TEXT,
                ok          INTEGER,                 -- 0/1
                note        TEXT
            )
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS ix_runs_job_started ON runs(job, started_at DESC)")
        await db.commit()


async def ensure_email_logs_schema(db_path: str) -> None:
    async with _open_db_fk(db_path) as db:
        # Create table first
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS email_logs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                email        TEXT NOT NULL,
                sent_at      TEXT NOT NULL,
                subject      TEXT NOT NULL,
                ok           INTEGER NOT NULL,       -- 0/1
                error_msg    TEXT,
                items_count  INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await db.commit()
        
        # Now create index (defensive: check if column exists first)
        try:
            await db.execute("CREATE INDEX IF NOT EXISTS ix_email_logs_email_sent ON email_logs(email, sent_at DESC)")
            await db.commit()
        except Exception as e:
            # If index creation fails (e.g., old schema missing column), log but don't crash
            import logging
            logging.getLogger("ari.cache").warning("email_logs index creation skipped: %s", e)


async def ensure_phase4_user_catalog_schemas(db_path: str) -> None:
    await ensure_users_schema(db_path)
    await ensure_user_tickers_schema(db_path)
    await ensure_ticker_catalog_schema(db_path)
    await ensure_runs_schema(db_path)
    await ensure_email_logs_schema(db_path)


async def load_ticker_catalog_from_file(path: str, db_path: str = "./ari.db") -> int:
    """
    Load ticker catalog from a JSON file and upsert into ticker_catalog table.
    
    Expected JSON format:
    [
        {"ticker": "AAPL", "company_name": "Apple Inc.", "aliases": ["Apple", "AAPL"]},
        ...
    ]
    
    Returns the number of rows inserted/updated.
    """
    import json
    
    if not os.path.exists(path):
        log.error("load_ticker_catalog_from_file: file not found: %s", path)
        return 0
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        log.exception("load_ticker_catalog_from_file: failed to parse JSON from %s", path)
        return 0
    
    if not isinstance(data, list):
        log.error("load_ticker_catalog_from_file: expected list, got %s", type(data))
        return 0
    
    upsert_sql = """
    INSERT INTO ticker_catalog (ticker, company_name, aliases_json, active)
    VALUES (?, ?, ?, 1)
    ON CONFLICT(ticker) DO UPDATE SET
        company_name = excluded.company_name,
        aliases_json = excluded.aliases_json,
        active = excluded.active
    """
    
    params = []
    for item in data:
        if not isinstance(item, dict):
            continue
        ticker = item.get("ticker")
        company_name = item.get("company_name")
        aliases = item.get("aliases", [])
        
        if not ticker or not company_name:
            log.warning("load_ticker_catalog_from_file: skipping invalid item (missing ticker or company_name): %s", item)
            continue
        
        # serialize aliases to JSON string
        try:
            aliases_json = json.dumps(aliases) if aliases else None
        except Exception:
            log.exception("load_ticker_catalog_from_file: failed to serialize aliases for ticker=%s", ticker)
            aliases_json = None
        
        params.append((ticker, company_name, aliases_json))
    
    if not params:
        log.info("load_ticker_catalog_from_file: no valid rows to insert from %s", path)
        return 0
    
    try:
        await ensure_ticker_catalog_schema(db_path)
        async with _open_db_fk(db_path) as db:
            before = getattr(db, "total_changes", 0)
            await db.executemany(upsert_sql, params)
            await db.commit()
            after = getattr(db, "total_changes", 0)
            changes = max(0, int(after - before))
        
        log.info("load_ticker_catalog_from_file: loaded=%d rows from %s to %s", changes, path, db_path)
        return changes
    except Exception:
        log.exception("load_ticker_catalog_from_file: upsert failed for %s", path)
        return 0


# Add these new functions at the end of the file (before __all__):

async def get_cached_summary(
    ticker: str, 
    max_age_hours: int = 12,
    db_path: str | None = None
) -> dict[str, Any] | None:
    """
    Retrieve the most recent cached summary for a ticker within max_age_hours.
    
    Args:
        ticker: Stock ticker symbol
        max_age_hours: Maximum age of cached entry in hours (default: 12)
        db_path: Optional database path (defaults to CACHE_DB_PATH)
        
    Returns:
        Dict with summary data if found, None otherwise
    """
    if not db_path:
        db_path = CACHE_DB_PATH
    
    cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat() + "Z"
    
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            query = """
                SELECT 
                    item_url_hash,
                    ticker,
                    title,
                    url,
                    why_it_matters,
                    bullets,
                    sentiment,
                    relevance,
                    created_at
                FROM summaries
                WHERE ticker = ?
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 5
            """
            
            async with db.execute(query, (ticker, cutoff)) as cur:
                rows = await cur.fetchall()
            
            if not rows:
                return None
            
            # Return list of recent summaries
            items = []
            for row in rows:
                bullets_raw = row["bullets"] or ""
                try:
                    import json
                    bullets = json.loads(bullets_raw) if bullets_raw else []
                except Exception:
                    bullets = []
                
                items.append({
                    "url": row["url"] or "",
                    "url_hash": row["item_url_hash"] or "",
                    "title": row["title"] or "",
                    "why_it_matters": row["why_it_matters"] or "",
                    "bullets": bullets,
                    "sentiment": row["sentiment"] or "Neutral",
                    "relevance": row["relevance"] or 5,
                    "created_at": row["created_at"]
                })
            
            age_hours = round((datetime.utcnow() - datetime.fromisoformat(items[0]["created_at"].replace("Z", ""))).total_seconds() / 3600, 1)
            
            log.info(f"get_cached_summary: found {len(items)} summaries for {ticker} (age={age_hours}h)")
            
            return {
                "ok": True,
                "items": items,
                "cached": True,
                "age_hours": age_hours,
                "ticker": ticker
            }
            
    except Exception as e:
        log.exception(f"get_cached_summary: failed for ticker={ticker}")
        return None


async def get_cached_articles(
    ticker: str,
    max_age_hours: int = 12,
    db_path: str | None = None
) -> list[dict[str, Any]] | None:
    """
    Retrieve recent cached articles with content for a ticker.
    
    Args:
        ticker: Stock ticker symbol
        max_age_hours: Maximum age of cached entry in hours (default: 12)
        db_path: Optional database path (defaults to CACHE_DB_PATH)
        
    Returns:
        List of article dicts if found, None otherwise
    """
    if not db_path:
        db_path = CACHE_DB_PATH
    
    cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat() + "Z"
    
    try:
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            query = """
                SELECT 
                    url,
                    url_hash,
                    title,
                    content,
                    source,
                    published_at,
                    lang,
                    created_at
                FROM articles
                WHERE ticker = ?
                  AND created_at >= ?
                  AND content IS NOT NULL
                  AND LENGTH(content) > 500
                ORDER BY created_at DESC
                LIMIT 10
            """
            
            async with db.execute(query, (ticker, cutoff)) as cur:
                rows = await cur.fetchall()
            
            if not rows:
                return None
            
            articles = []
            for row in rows:
                articles.append({
                    "url": row["url"] or "",
                    "url_hash": row["url_hash"] or "",
                    "title": row["title"] or "",
                    "content": row["content"] or "",
                    "translated_text": row["content"] or "",  # For compatibility with summarizer
                    "source": row["source"] or "",
                    "published_at": row["published_at"] or "",
                    "lang": row["lang"] or "en",
                    "created_at": row["created_at"]
                })
            
            age_hours = round((datetime.utcnow() - datetime.fromisoformat(articles[0]["created_at"].replace("Z", ""))).total_seconds() / 3600, 1)
            
            log.info(f"get_cached_articles: found {len(articles)} articles for {ticker} (age={age_hours}h)")
            
            return articles
            
    except Exception as e:
        log.exception(f"get_cached_articles: failed for ticker={ticker}")
        return None