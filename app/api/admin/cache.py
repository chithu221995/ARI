from __future__ import annotations
import os
import logging
import aiosqlite
from typing import Dict, Any

from fastapi import APIRouter, Body

from app.core.cache import purge_expired, count_articles_rows, count_summaries_rows

log = logging.getLogger("ari.admin.cache")
router = APIRouter(prefix="/cache", tags=["admin"])


@router.get(
    "/stats",
    summary="Cache statistics",
    description="Return counts for articles and summaries in the cache and basic cache statistics.",
)
async def cache_stats() -> Dict[str, Any]:
    articles = await count_articles_rows()
    summaries = await count_summaries_rows()
    return {"ok": True, "articles": int(articles or 0), "summaries": int(summaries or 0)}


@router.post("/purge", summary="Purge old cache rows", description="Delete rows older than TTL days (default 7).")
async def purge(ttl_days: int = Body(7, embed=True)) -> Dict[str, Any]:
    """Run TTL purge using purge_expired(ttl_days)."""
    try:
        res = await purge_expired(ttl_days)
        if isinstance(res, tuple) and len(res) == 3:
            a, s, f = res
            total = (a or 0) + (s or 0) + (f or 0)
            return {"ok": True, "ttl_days": ttl_days, "articles_deleted": a, "summaries_deleted": s, "filings_deleted": f, "total_deleted": total}
        elif isinstance(res, int):
            return {"ok": True, "ttl_days": ttl_days, "total_deleted": res}
        else:
            return {"ok": True, "ttl_days": ttl_days, "result": res}
    except Exception as e:
        log.exception("cache.purge failed")
        return {"ok": False, "error": str(e)}


@router.post(
    "/purge-older",
    summary="Purge older than N days",
    description="Delete rows older than the provided days value (POST body 'days').",
)
async def purge_older(days: int = Body(30, embed=True)) -> Dict[str, Any]:
    try:
        res = await purge_expired(days)
        if isinstance(res, tuple):
            a, s, f = res
            total = (a or 0) + (s or 0) + (f or 0)
            return {"ok": True, "days": days, "articles_deleted": a, "summaries_deleted": s, "filings_deleted": f, "total_deleted": total}
        return {"ok": True, "days": days, "total_deleted": int(res or 0)}
    except Exception as e:
        log.exception("cache.purge_older failed")
        return {"ok": False, "error": str(e)}


@router.post(
    "/purge-non-en",
    summary="Purge non-English articles",
    description="Delete articles whose lang is not 'en' (or is NULL).",
)
async def purge_non_en() -> Dict[str, Any]:
    db_path = os.getenv("SQLITE_PATH", "./ari.db")
    deleted = 0
    try:
        async with aiosqlite.connect(db_path) as db:
            # guard table exists
            cur = await db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='articles' LIMIT 1")
            row = await cur.fetchone()
            await cur.close()
            if not row:
                return {"ok": True, "deleted": 0, "note": "articles table not found"}
            cur = await db.execute("DELETE FROM articles WHERE LOWER(COALESCE(lang, '')) != ? OR COALESCE(lang, '') = ''", ("en",))
            # rowcount may be None depending on sqlite build; attempt to query count as fallback
            deleted = cur.rowcount or 0
            await db.commit()
            if deleted == 0:
                # attempt fallback count via simple select (best-effort)
                # note: rowcount sometimes not supported; this is best-effort
                pass
        return {"ok": True, "deleted": int(deleted)}
    except Exception as e:
        log.exception("cache.purge_non_en failed")
        return {"ok": False, "error": str(e)}