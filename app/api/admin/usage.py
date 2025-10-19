from __future__ import annotations
from typing import Dict, Any
from fastapi import APIRouter
import aiosqlite
import datetime

from app.summarize import llm
from app.core.cache import CACHE_DB_PATH

router = APIRouter()


@router.get("/admin/usage/openai")
async def admin_usage_openai() -> Dict[str, Any]:
    """
    Return totals from in-memory counters and last 24h aggregates from usage_logs.
    """
    # in-memory totals
    totals = {
        "prompt_tokens_total": getattr(llm, "PROMPT_TOKENS_TOTAL", 0),
        "completion_tokens_total": getattr(llm, "COMPLETION_TOKENS_TOTAL", 0),
        "cost_usd_total": getattr(llm, "COST_USD_TOTAL", 0.0),
    }

    # last 24h sums from DB
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
    last24 = {"prompt_tokens": 0, "completion_tokens": 0, "cost_usd": 0.0}
    try:
        async with aiosqlite.connect(CACHE_DB_PATH) as db:
            async with db.execute(
                "SELECT SUM(prompt_tokens), SUM(completion_tokens), SUM(cost_usd) FROM usage_logs WHERE ts >= ?",
                (cutoff,),
            ) as cur:
                row = await cur.fetchone()
                if row:
                    last24["prompt_tokens"] = int(row[0] or 0)
                    last24["completion_tokens"] = int(row[1] or 0)
                    last24["cost_usd"] = float(row[2] or 0.0)
    except Exception:
        pass

    return {"ok": True, "totals": totals, "last_24h": last24}