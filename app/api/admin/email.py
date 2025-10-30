from __future__ import annotations
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import time

from fastapi import APIRouter, Query, Body, HTTPException
import aiosqlite

from app.core.cache import CACHE_DB_PATH
from app.services.email import send_via_sendgrid, send_via_smtp, _one_para
from app.core.metrics import record_metric

log = logging.getLogger("ari.email")

router = APIRouter(prefix="/email", tags=["/email"]) 

# --- tolerant body formatter (filters rel<=1, sorts desc, top 3) ---
def _format_body(results: Dict[str, Any]) -> str:
    def _to_int(x):
        try:
            return int(str(x).strip())
        except Exception:
            return 0

    def _as_items(seq):
        return [x for x in (seq or []) if isinstance(x, dict)]

    lines: List[str] = []
    tickers = results.get("tickers") or {}

    for ticker, payload in tickers.items():
        # Pull items in a tolerant way
        raw_items = (
            (payload.get("summary") or {}).get("items")
            or payload.get("news")
            or payload.get("items")
            or []
        )
        items = _as_items(raw_items)

        # Filter and sort
        items = [it for it in items if _to_int(it.get("relevance")) >= 4]
        items.sort(key=lambda it: _to_int(it.get("relevance")), reverse=True)
        top_items = items[:3]

        lines.append(f"{ticker}:")
        if not top_items:
            lines.append("(no sufficiently relevant summaries)")
        else:
            for it in top_items:
                lines.append(_one_para(it))
        lines.append("")  # blank line between tickers

    body = "\n".join(lines).strip()
    return body or "No summaries available"

# --- helper: load recent summaries per ticker ---
async def _load_summaries_for_ticker(ticker: str, hours: int = 48) -> List[Dict[str, Any]]:
    sql = (
        """
        SELECT url, title, why_it_matters, sentiment, relevance, created_at
        FROM summaries
        WHERE ticker = ? AND created_at > datetime('now', ?)
        ORDER BY relevance DESC, created_at DESC
        LIMIT 10
        """
    )
    params = [ticker, f"-{hours} hours"]

    rows: List[Dict[str, Any]] = []
    async with aiosqlite.connect(CACHE_DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cur:
            async for r in cur:
                rows.append({
                    "url": r["url"],
                    "title": r["title"],
                    "why_it_matters": r["why_it_matters"],
                    "sentiment": r["sentiment"],
                    "relevance": r["relevance"],
                    "created_at": r["created_at"],
                })
    return rows

# --- shared core for GET/POST ---
async def _build_payload(tickers: List[str], summarized: bool = True) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"tickers": {}}
    for t in tickers:
        items = await _load_summaries_for_ticker(t)
        # shape expected by formatter/senders
        payload["tickers"][t] = {"summary": {"items": items}}
        log.info("ari.email: email.brief: %d items for %s", len(items), t)
    return payload

@router.get("/brief")
async def email_brief_get(
    email: str = Query("you@example.com"),
    tickers: List[str] = Query([]),
    summarized: bool = Query(True),
    dry_run: bool = Query(False),
):
    return await email_brief_post(
        body={
            "email": email,
            "tickers": tickers,
            "summarized": summarized,
            "dry_run": dry_run,
        }
    )

@router.post("/brief")
async def email_brief_post(
    body: Dict[str, Any] = Body(...),
):
    to_email = (body.get("email") or "you@example.com").strip()
    tickers: List[str] = body.get("tickers") or []
    summarized: bool = bool(body.get("summarized", True))
    dry_run: bool = bool(body.get("dry_run", False))

    if not tickers:
        raise HTTPException(status_code=400, detail="tickers required")

    payload_results = await _build_payload(tickers, summarized=summarized)

    # Render a preview body using the robust local formatter
    body_text = _format_body(payload_results)

    # Decide provider
    provider = (getattr(__import__('app.core', fromlist=['settings']).settings, "EMAIL_PROVIDER", "") or os.getenv("EMAIL_PROVIDER", "")).strip().lower()

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "provider": provider or None,
            "tickers": tickers,
            "body_preview": body_text[:600],
        }

    if not provider:
        raise HTTPException(status_code=400, detail="EMAIL_PROVIDER not configured")

    if provider == "sendgrid":
        # send email via sendgrid/smtp (existing call)
        start_send = time.time()
        resp = await send_via_sendgrid(to_email, payload_results, dry_run=False, body_text=body_text)
        send_latency_ms = int((time.time() - start_send) * 1000)
        # record metric on success
        if resp and resp.get("ok"):
            try:
                record_metric("email", "sendgrid", send_latency_ms, True)
            except Exception:
                log.exception("metrics: failed to record email sendgrid metric")
    elif provider == "smtp":
        # SMTP path (existing call)
        start_send = time.time()
        resp = await send_via_smtp(to_email, payload_results, dry_run=False, body_text=body_text)
        send_latency_ms = int((time.time() - start_send) * 1000)
        if resp and resp.get("ok"):
            try:
                record_metric("email", "smtp", send_latency_ms, True)
            except Exception:
                log.exception("metrics: failed to record email smtp metric")
    else:
        raise HTTPException(status_code=400, detail=f"Unknown EMAIL_PROVIDER: {provider}")

    return {
        "ok": bool(resp.get("ok", True) if isinstance(resp, dict) else True),
        "dry_run": False,
        "provider_response": resp,
        "tickers": tickers,
    }
