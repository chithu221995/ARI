from __future__ import annotations
import os
import re
import inspect
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Body, HTTPException
from pydantic import EmailStr
import httpx

from app.ingest.news import fetch_news_for_ticker
from app.ingest.news import select_top_news_for_summary  # new: pick top items for summarization
from app.core.cache import url_to_hash, cache_upsert_summaries  # new: prepare & persist summary rows
from app.summarize.llm import summarize_items

log = logging.getLogger("ari.email")
router = APIRouter(prefix="/email", tags=["admin"])

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
    Produce a single compact paragraph per item:
      "{title}. {summary} Sentiment: {sentiment}."
    - Title only (no URLs).
    - Summary prefers why_it_matters, then bullets, then translated_text/content (first 2-3 sentences).
    - All URLs removed.
    - No bullets or link placeholders.
    """
    title = _strip_urls((item.get("title") or "").strip())
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
        txt = _strip_urls(translated)
        sentences = re.split(r'(?<=[\.\!\?])\s+', txt)
        summary_raw = " ".join(sentences[:3]).strip()

    summary = _strip_urls(summary_raw).strip()
    if summary and not summary.endswith("."):
        summary += "."

    parts = []
    if title:
        # ensure title ends with a period
        t = title.rstrip(".")
        parts.append(f"{t}.")
    if summary:
        parts.append(summary)
    parts.append(f"Sentiment: {sent}.")

    # join with single space, produce one compact paragraph
    return " ".join(p for p in parts if p).strip()


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


def ist_today() -> str:
    """Return today's date in IST formatted like 'Mon 20, 2025'."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%a %d, %Y")


def render_brief_email(ticker_items: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Build subject and plain-text body: one compact paragraph per item (no raw links).
    Each ticker section looks like:

    === TICKER ===

    Para 1

    Para 2
    """
    # subject (keep existing style if helper ist_today is available)
    try:
        subject = f"Your asset relevance intelligence — {ist_today()}"
    except Exception:
        subject = "Your asset relevance intelligence"

    def _mk_para(it: Dict[str, Any]) -> str:
        # strip URLs from title and why text using existing helper if present
        title = (it.get("title") or "").strip()
        try:
            title = _strip_urls(title)
        except Exception:
            # fallback: crude removal of http/https
            import re
            title = re.sub(r"https?://\S+", "", title).strip()
        title = title.rstrip(" .")

        why = (it.get("why_it_matters") or "").strip()
        if not why:
            bullets = it.get("bullets") or []
            try:
                why = " ".join([str(b).strip() for b in bullets if str(b).strip()])[:240].strip()
            except Exception:
                why = ""
        try:
            why = _strip_urls(why)
        except Exception:
            import re
            why = re.sub(r"https?://\S+", "", (why or "")).strip()

        sent = (it.get("sentiment") or "Neutral").strip().capitalize()

        if title and why:
            return f"{title}. {why} | Sentiment: {sent}."
        if title and not why:
            return f"{title}. Sentiment: {sent}."
        if why and not title:
            return f"{why} | Sentiment: {sent}."
        return ""

    sections: List[str] = []
    for ticker, payload in ticker_items.items():
        items = (payload.get("news") or [])[:5]
        paras: List[str] = []
        for it in items:
            p = _mk_para(it)
            if p:
                paras.append(p)
        if not paras:
            sections.append(f"=== {ticker} ===\n\nNo items found.")
        else:
            sections.append(f"=== {ticker} ===\n\n" + "\n\n".join(paras))

    body_text = "\n\n".join(sections).strip()
    body_html = None  # keep HTML generation separate if needed

    return {"subject": subject, "text": body_text, "html": body_html}


@router.post(
    "/brief",
    summary="Build and optionally send daily brief email",
    description=(
        "Build a consolidated daily brief for the provided tickers and email address. "
        "When summarized=true, this will fetch news, call the summarization model to produce "
        "compact paragraphs and optionally persist summaries. If dry_run is true the assembled "
        "subject/body are returned for inspection instead of sending."
    ),
)
async def send_brief_email(
    email: EmailStr = Body(..., embed=True),
    tickers: List[str] = Body(..., embed=True),
    summarized: bool = Body(True, embed=True),
    dry_run: bool = Body(True, embed=True),
):
    """
    Compose and (optionally) send a consolidated daily brief.
    - subject is built using IST date (weekday abbrev).
    - body contains one compact paragraph per item (titles only, URLs removed).
    - if dry_run is true the assembled subject/body are returned for inspection.
    """
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
            # use new signature: max_items, days
            if inspect.iscoroutinefunction(fetch_news_for_ticker):
                news = await fetch_news_for_ticker(t, max_items=10, days=7)
            else:
                news = fetch_news_for_ticker(t, max_items=10, days=7)
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
                # select top items for summary
                items_for_llm = select_top_news_for_summary(items, k=5)

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

                # call LLM
                try:
                    log.info("email.brief: calling LLM summarize for %s items=%d", t, len(items_for_llm))
                    llm_out = await summarize_items(items_for_llm, ticker=t)
                except Exception as e:
                    log.exception("email.brief: summarize_items call failed for %s", t)
                    errors.append({"ticker": t, "error": str(e)})
                    continue

                if not llm_out.get("ok", True):
                    log.error("email.brief: summarize LLM returned error for %s: %s", t, llm_out.get("error"))
                    errors.append({"ticker": t, "error": str(llm_out.get("error"))})
                    continue

                # upsert summaries into cache (rows shaped for cache_upsert_summaries)
                try:
                    rows = []
                    for it in (llm_out.get("items") or []):
                        url = (it.get("url") or "").strip()
                        rows.append(
                            {
                                "item_url_hash": url_to_hash(url),
                                "ticker": t,
                                "title": it.get("title", "") or "",
                                "bullets_json": json.dumps(it.get("bullets") or []),
                                "why_it_matters": it.get("why_it_matters", "") or "",
                                "sentiment": it.get("sentiment", "Neutral") or "Neutral",
                            }
                        )
                    if rows:
                        inserted = await cache_upsert_summaries(rows)
                        log.info("email.brief: upserted %d summaries for %s", inserted, t)
                except Exception:
                    log.exception("email.brief: failed to upsert summaries for %s", t)

                # attach summaries back to results for rendering
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
    else:
        # summarized == False: keep headlines only (no LLM)
        for t, data in list(results.items()):
            items = data.get("news", []) or []
            results[t]["news"] = [{"title": (it.get("title") or "").strip()} for it in items]

    # render subject/body using helper (ensures body is available even on dry_run)
    rendered = render_brief_email(results)
    subject = rendered["subject"]
    body_text = rendered["text"]
    body_html = rendered["html"]

    log.info("email: composing %d items for %s subject=%s", sum(len(v.get("news") or []) for v in results.values()), email, subject)

    provider_name = "sendgrid"

    if dry_run:
        return {
            "ok": True,
            "provider": provider_name,
            "email": str(email),
            "tickers": tickers,
            "summarized": summarized,
            "dry_run": True,
            "subject": subject,
            "preview": body_text,
            "body_text": body_text,
            "errors": errors,
        }

    # perform real send
    message_id = await _send_via_sendgrid(str(email), subject, body_text)
    log.info("email: sent to=%s provider=%s message_id=%s", email, provider_name, message_id or "")
    return {
        "ok": True,
        "provider": provider_name,
        "email": str(email),
        "tickers": tickers,
        "summarized": summarized,
        "dry_run": False,
        "subject": subject,
        "message_id": message_id or None,
        "errors": errors,
    }
