from __future__ import annotations
import os
import re
import inspect
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, HTTPException
from pydantic import EmailStr
import httpx

from app.ingest.news import fetch_news_for_ticker
from app.summarize.llm import summarize_items
from app.fetch.content import fetch_article_text  # may be None or raise elsewhere; keeping import here

log = logging.getLogger("ari.email")
router = APIRouter(prefix="/email", tags=["admin-email"])

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")


def _subject_for_today() -> str:
    ist = ZoneInfo("Asia/Kolkata")
    return f"Your asset relevance intelligence — {datetime.now(ist).strftime('%b %d, %Y')}"


def _strip_urls(text: str) -> str:
    if not text:
        return ""
    # remove http(s) links and bare www.
    return re.sub(r"https?://\S+|\bwww\.\S+\b", "", text)


def _short_paragraph_from_item(item: Dict[str, Any]) -> str:
    """
    Produce a single-paragraph string:
      Title — <3-4 sentence summary> — Sentiment: X.
    Summary prefers why_it_matters, then bullets, then translated_text/content (first 2-3 sentences).
    URLs are stripped.
    """
    title = (item.get("title") or "").strip()
    sent = (item.get("sentiment") or "Neutral").strip()

    why = (item.get("why_it_matters") or "").strip()
    bullets = item.get("bullets") or []
    translated = (item.get("translated_text") or item.get("content") or "").strip()

    if why:
        summary_raw = why
    elif bullets:
        parts = [b.strip().rstrip(".") for b in bullets if b and b.strip()]
        summary_raw = ". ".join(parts)
    else:
        # take first 2-3 sentences from translated/content
        txt = _strip_urls(translated)
        sentences = re.split(r'(?<=[\.\!\?])\s+', txt)
        summary_raw = " ".join(sentences[:3]).strip()

    summary = _strip_urls(summary_raw).strip()
    if summary and not summary.endswith("."):
        summary += "."

    # assemble single paragraph
    parts = []
    if title:
        parts.append(title)
    if summary:
        parts.append("— " + summary)
    parts.append(f"— Sentiment: {sent}.")
    return " ".join(parts)


async def _send_via_sendgrid(to_email: str, subject: str, body_text: str) -> str:
    if not SENDGRID_API_KEY or not EMAIL_FROM:
        log.error("SendGrid not configured; SENDGRID_API_KEY or EMAIL_FROM missing")
        raise HTTPException(status_code=500, detail="SendGrid not configured")
    url = "https://api.sendgrid.com/v3/mail/send"
    json_payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": EMAIL_FROM},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}],
    }
    headers = {"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(url, headers=headers, json=json_payload)
        if r.status_code not in (200, 202):
            log.error("SendGrid error %s: %s", r.status_code, r.text[:200])
            raise HTTPException(status_code=502, detail=f"SendGrid error {r.status_code}")
        return r.headers.get("X-Message-Id", "")


@router.post("/brief")
async def send_brief_email(
    email: EmailStr = Body(..., embed=True),
    tickers: List[str] = Body(..., embed=True),
    summarized: bool = Body(True, embed=True),
    dry_run: bool = Body(True, embed=True),
):
    if not email:
        raise HTTPException(status_code=400, detail="email required")
    if not tickers:
        raise HTTPException(status_code=400, detail="tickers required")

    log.info("email.brief: start email=%s tickers=%s summarized=%s", email, tickers, summarized)
    errors: List[Dict[str, str]] = []
    results: Dict[str, Dict[str, Any]] = {}

    # fetch news per ticker
    for t in [x.strip().upper() for x in tickers if x.strip()]:
        try:
            if inspect.iscoroutinefunction(fetch_news_for_ticker):
                news = await fetch_news_for_ticker(t, only_en=True, max_items=5)
            else:
                news = fetch_news_for_ticker(t, only_en=True, max_items=5)
            news = news or []
            results[t] = {"news": news}
        except Exception as e:
            log.error("email brief: ticker %s failed: %s", t, e, exc_info=True)
            errors.append({"ticker": t, "error": str(e)})
            continue

    # summarize if requested (enrich with article text first)
    if summarized:
        for t, data in list(results.items()):
            try:
                items = data.get("news", []) or []
                items_for_llm = items[:5]

                # enrich with article text (if fetcher available)
                enriched_for_llm: List[Dict[str, Any]] = []
                for n in items_for_llm:
                    try:
                        art = await fetch_article_text(n.get("url", "") or "")
                        n["translated_text"] = art.get("translated_text", "") or ""
                        n["lang"] = art.get("lang", "") or n.get("lang", "")
                    except Exception:
                        n["translated_text"] = n.get("content", "") or ""
                        n["lang"] = n.get("lang", "")
                    enriched_for_llm.append(n)
                items_for_llm = enriched_for_llm

                llm_out = await summarize_items(items_for_llm, ticker=t)
                if not llm_out.get("ok", True):
                    log.error("email brief: summarize LLM failed for %s: %s", t, llm_out.get("error"))
                    errors.append({"ticker": t, "error": str(llm_out.get("error"))})
                    # keep original items
                    continue

                summaries_by_title = {s.get("title", ""): s for s in llm_out.get("items", [])}
                enriched = []
                for n in items:
                    s = summaries_by_title.get(n.get("title", ""))
                    if s:
                        n = {**n, "summary": s}
                    enriched.append(n)
                results[t]["news"] = enriched
            except Exception as e:
                log.error("email brief: summarize failed for ticker %s: %s", t, e, exc_info=True)
                errors.append({"ticker": t, "error": str(e)})
                continue

    # compose single email body for all tickers
    subject = _subject_for_today()
    paragraphs: List[str] = []
    total_items = 0
    for ticker, payload in results.items():
        items = (payload.get("summary") or {}).get("items") or payload.get("news") or []
        for it in items[:5]:
            para = _short_paragraph_from_item(it)
            paragraphs.append(para)
            total_items += 1

    body = "\n\n".join(paragraphs).strip()

    log.info("email: composing %d items for %s subject=%s", total_items, email, subject)

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "email": str(email),
            "tickers": tickers,
            "summarized": summarized,
            "preview_first_400": body[:400],
            "total_items": total_items,
            "errors": errors,
        }

    message_id = await _send_via_sendgrid(str(email), subject, body)
    log.info("email: sent to=%s provider=sendgrid message_id=%s", email, message_id or "")
    return {
        "ok": True,
        "dry_run": False,
        "email": str(email),
        "tickers": tickers,
        "summarized": summarized,
        "provider": "sendgrid",
        "message_id": message_id or None,
        "errors": errors,
    }
