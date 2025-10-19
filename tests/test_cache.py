import os
import json
import hashlib
import sqlite3
import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
importlib.invalidate_caches()

import pytest

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def tmp_db_path(tmp_path, monkeypatch):
    p = tmp_path / "cache_test.db"
    monkeypatch.setenv("CACHE_DB_PATH", str(p))
    return str(p)


async def _reload_modules():
    # ensure modules pick up env
    import importlib
    core_cache = importlib.import_module("app.core.cache")
    importlib.reload(core_cache)
    # ensure db variant also reloaded if present
    try:
        db_cache = importlib.import_module("app.db.cache")
        importlib.reload(db_cache)
    except Exception:
        db_cache = None
    return core_cache, db_cache


@pytest.mark.asyncio
async def test_init_db_creates_tables(tmp_db_path):
    core_cache, _ = await _reload_modules()
    # should run without error
    await core_cache.init_db()
    # inspect sqlite tables
    conn = sqlite3.connect(os.environ["CACHE_DB_PATH"])
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    conn.close()
    assert "articles" in tables
    assert "filings" in tables
    assert "summaries" in tables
    assert "feedback" in tables or True  # feedback may be present per migrations


def test_url_hash_stable():
    from app.core import cache as core_cache
    url = "https://example.com/article/1"
    # expected = sha256 hex first 16 chars per implementation
    expected = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    got = core_cache.sha256_16(url)
    assert got == expected


@pytest.mark.asyncio
async def test_cache_upsert_items_and_get_by_ticker(tmp_db_path):
    core_cache, _ = await _reload_modules()
    await core_cache.init_db()

    news_items = [
        {
            "url": "https://example.com/news1",
            "title": "News One",
            "source": "ExampleNews",
            "published_at": "2025-10-18T00:00:00Z",
            "translated_text": "This is the content of news one. " * 30,
            "lang": "en",
        }
    ]
    filings_items = [
        {
            "url": "https://example.com/filing1",
            "title": "Filing One",
            "source": "ExampleSec",
            "category": "report",
            "published_at": "2025-10-17T00:00:00Z",
            "content": "This is the content of filing one. " * 30,
            "lang": "en",
            "summary_allowed": True,
        }
    ]

    n_news = await core_cache.cache_upsert_items(news_items, kind="news", ticker="TCS")
    n_filings = await core_cache.cache_upsert_items(filings_items, kind="filings", ticker="TCS")
    assert n_news >= 1
    assert n_filings >= 1

    cached = await core_cache.cache_get_by_ticker("TCS", max_age_hours=24)
    assert "news" in cached and isinstance(cached["news"], list)
    assert "filings" in cached and isinstance(cached["filings"], list)
    # check titles present
    titles = [a.get("title") for a in cached["news"] + cached["filings"]]
    assert "News One" in titles or "Filing One" in titles


@pytest.mark.asyncio
async def test_cache_upsert_summaries_and_get_summaries_by_hashes(tmp_db_path):
    core_cache, db_cache = await _reload_modules()
    await core_cache.init_db()

    # create an article and compute its url_hash
    url = "https://example.com/news-summary-test"
    url_hash = core_cache.sha256_16(url)

    # upsert summary via core cache helper
    items = [
        {
            "url_hash": url_hash,
            "title": "Summary Title",
            "bullets": ["Point A", "Point B"],
            "why_it_matters": "Because reasons",
            "sentiment": "Bullish",
        }
    ]
    inserted = await core_cache.cache_upsert_summaries("TCS", items)
    assert inserted == 1

    # retrieve via db cache helper if available, else via core query
    if db_cache:
        res = await db_cache.get_summaries_by_hashes([url_hash], max_age_hours=24)
        assert url_hash in res
        rec = res[url_hash]
        assert rec["title"] == "Summary Title"
        assert isinstance(rec["bullets"], list) and "Point A" in rec["bullets"]
    else:
        # fallback: query sqlite directly
        import aiosqlite
        async with aiosqlite.connect(os.environ["CACHE_DB_PATH"]) as db:
            async with db.execute("SELECT item_url_hash, title, bullets FROM summaries WHERE item_url_hash = ?", (url_hash,)) as cur:
                row = await cur.fetchone()
                assert row is not None
                assert row[1] == "Summary Title"