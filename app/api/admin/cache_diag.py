from __future__ import annotations
from fastapi import APIRouter
import aiosqlite
import os
import logging
from typing import Any, Dict

log = logging.getLogger("ari.cache")
router = APIRouter(prefix="/admin/cache", tags=["admin"])


@router.get("/diag", summary="Cache diagnostics", description="Return PRAGMA table/index info for articles and summaries.")
async def cache_diag() -> Dict[str, Any]:
    db_path = os.getenv("SQLITE_PATH", "./ari.db")
    out: Dict[str, Any] = {}
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("PRAGMA table_info(articles)") as cur:
            out["articles_columns"] = await cur.fetchall()
        async with db.execute("PRAGMA index_list(articles)") as cur:
            out["articles_indexes"] = await cur.fetchall()
        async with db.execute("PRAGMA table_info(summaries)") as cur:
            out["summaries_columns"] = await cur.fetchall()
        async with db.execute("PRAGMA index_list(summaries)") as cur:
            out["summaries_indexes"] = await cur.fetchall()
    return out