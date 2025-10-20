from __future__ import annotations
import os
import aiosqlite
import datetime
import hashlib
import logging
import json
from typing import List, Dict, Optional, Any, Tuple

log = logging.getLogger("ari.cache")

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
        # existing create table statements for articles
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                url_hash TEXT,
                title TEXT,
                ticker TEXT,
                source TEXT,
                published_at TEXT,
                lang TEXT,
                content TEXT,
                created_at TEXT
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
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_urlhash ON articles(url_hash)")
            log.info("cache.init_db: ensured indexes idx_articles_ticker_created and idx_articles_urlhash")
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


# New helpers for upserting and reading cached items (news/articles and filings)
async def cache_upsert_items(rows: List[Dict[str, Any]], ticker: Optional[str] = None) -> int:
    """
    Upsert article rows into articles table.
    """
    if not rows:
        return 0
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        async with db.execute("BEGIN"):
            stmt = """
            INSERT INTO articles
                (url, url_hash, title, ticker, source, published_at, lang, content, text_hash, created_at, translated_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                 url_hash=excluded.url_hash,
                 title=excluded.title,
                 ticker=excluded.ticker,
                 source=excluded.source,
                 published_at=excluded.published_at,
                 lang=excluded.lang,
                 content=excluded.content,
                 text_hash=excluded.text_hash,
                 created_at=excluded.created_at,
                 translated_text=excluded.translated_text
            """
            cnt = 0
            log.debug("[cache] upserting %d article rows for ticker=%s", len(rows), ticker)
            for r in rows:
                vals = (
                    r.get("url"),
                    r.get("url_hash"),
                    r.get("title", "") or "",
                    r.get("ticker", "") or ticker or "",
                    r.get("source", "") or "",
                    r.get("published_at", "") or "",
                    r.get("lang", "en") or "en",
                    r.get("content", "") or "",
                    r.get("text_hash", "") or "",
                    r.get("created_at") or now_iso(),
                    r.get("translated_text", "") or "",
                )
                try:
                    await db.execute(stmt, vals)
                    cnt += 1
                except Exception as e:
                    log.error("[cache] upsert articles failed: %s", e, exc_info=True)
                    # fallback: INSERT OR REPLACE when conflict-target mismatch or other issues
                    if "ON CONFLICT" in str(e) or "conflict" in str(e).lower():
                        fallback_stmt = """
                          INSERT OR REPLACE INTO articles
                            (url, url_hash, title, ticker, source, published_at, lang, content, text_hash, created_at, translated_text)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        try:
                            await db.execute(fallback_stmt, vals)
                            cnt += 1
                        except Exception as fe:
                            log.error("[cache] fallback REPLACE failed: %s", fe, exc_info=True)
                            raise
                    else:
                        raise
        await db.commit()
    return cnt


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


async def cache_upsert_summaries(rows: List[Dict[str, Any]]) -> int:
    """
    Upsert summary rows using INSERT OR REPLACE.
    Accepts rows with keys: item_url_hash (optional), url (optional), ticker, title, bullets or bullets_json, why_it_matters, sentiment
    """
    if not rows:
        return 0

    stmt = """
    INSERT OR REPLACE INTO summaries
      (item_url_hash, ticker, title, url, bullets, why_it_matters, sentiment, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    upserted = 0
    log.debug("[cache] upserting %d summaries", len(rows))

    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        async with db.execute("BEGIN"):
            for r in rows:
                url = (r.get("url") or "").strip()
                item_hash = r.get("item_url_hash") or url_hash(url)
                bullets_val = r.get("bullets") if "bullets" in r else r.get("bullets_json", "[]")
                try:
                    if isinstance(bullets_val, list):
                        bullets_str = json.dumps(bullets_val)
                    else:
                        bullets_str = bullets_val or "[]"
                except Exception:
                    bullets_str = "[]"

                vals = (
                    item_hash,
                    r.get("ticker") or "",
                    r.get("title") or "",
                    url,
                    bullets_str,
                    r.get("why_it_matters") or "",
                    r.get("sentiment") or "Neutral",
                    r.get("created_at") or now_iso(),
                )

                try:
                    await db.execute(stmt, vals)
                    upserted += 1
                except Exception as e:
                    log.error("[cache] upsert summary failed for url/hash=%s/%s: %s", url or "", item_hash, e, exc_info=True)
                    # continue with next row (do not abort the batch)
                    continue
        await db.commit()

    log.info("[cache] upserted %d summaries", upserted)
    return upserted


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
]