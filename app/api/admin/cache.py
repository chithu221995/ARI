from __future__ import annotations
from typing import Optional, Dict, Any
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
import aiosqlite
import datetime
import logging

# Prefer app.db.cache per prompt, fall back to app.core.cache if unavailable
try:
    from app.db.cache import cache_stats, purge_ticker, purge_older_than, CACHE_DB_PATH
except Exception:
    from app.core.cache import cache_stats, purge_ticker, purge_older_than, CACHE_DB_PATH

router = APIRouter(prefix="/admin/cache", tags=["admin-cache"])


@router.get("/admin/cache/stats")
async def admin_cache_stats(ticker: Optional[str] = None) -> Dict[str, Any]:
    try:
        stats = await cache_stats(ticker)
        return {"ok": True, **stats}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/admin/cache/purge")
async def admin_cache_purge(request: Request) -> Any:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    ticker = payload.get("ticker")
    if not ticker or not isinstance(ticker, str):
        raise HTTPException(status_code=400, detail="ticker required")
    try:
        deleted = await purge_ticker(ticker.strip().upper())
        return {"ok": True, "ticker": ticker.strip().upper(), "deleted": deleted}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/admin/cache/purge-older")
async def admin_cache_purge_older(request: Request) -> Any:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    hours = payload.get("hours")
    if hours is None:
        raise HTTPException(status_code=400, detail="hours required")
    try:
        hours_int = int(hours)
    except Exception:
        raise HTTPException(status_code=400, detail="hours must be integer")
    try:
        deleted = await purge_older_than(hours_int)
        return {"ok": True, "hours": hours_int, "deleted": deleted}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@router.post("/admin/cache/purge-non-en")
async def purge_non_en() -> dict:
    """
    Remove cached articles whose lang is set and is not 'en'.
    Returns {"ok": True, "deleted": <count>}.
    """
    try:
        async with aiosqlite.connect(CACHE_DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(1) FROM articles WHERE lang IS NOT NULL AND lang <> 'en'"
            ) as cur:
                row = await cur.fetchone()
                to_delete = int(row[0] or 0)

            if to_delete:
                await db.execute(
                    "DELETE FROM articles WHERE lang IS NOT NULL AND lang <> 'en'"
                )
                await db.commit()

        return {"ok": True, "deleted": to_delete}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/purge")
async def admin_purge():
    try:
        log.info("admin.cache: purge requested")
        a, s, f = await purge_expired()
        log.info("admin.cache: purge completed articles=%s summaries=%s filings=%s", a, s, f)
        return {"ok": True, "deleted": {"articles": a, "summaries": s, "filings": f}, "ran_at": datetime.datetime.utcnow().isoformat() + "Z"}
    except Exception:
        log.exception("admin.cache: purge failed")
        return {"ok": False, "error": "purge_failed"}