from __future__ import annotations
import json
import os
from email.message import EmailMessage
from email.utils import formataddr
import logging
from typing import Dict, Any, Optional
import httpx
import re
import asyncio
import smtplib
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List
from app.core import settings
import time
from app.core.metrics import record_metric



log = logging.getLogger("ari.email")
ist = ZoneInfo("Asia/Kolkata")

try:
    import aiosmtplib
except Exception:
    aiosmtplib = None
    log.warning("aiosmtplib not installed; SMTP async helper unavailable, falling back to sync send")

# Local tolerant formatter (kept here to avoid circular imports with app.api.admin.email)
def _format_body(results: Dict[str, Any]) -> str:
    def _to_int(x):
        try:
            return int(str(x).strip())
        except Exception:
            return 0

    def _as_items(seq):
        return [x for x in (seq or []) if isinstance(x, dict)]

    lines: list[str] = []
    tickers = results.get("tickers") or {}
    for ticker, payload in tickers.items():
        raw_items = (
            (payload.get("summary") or {}).get("items")
            or payload.get("news")
            or payload.get("items")
            or []
        )
        items = _as_items(raw_items)
        items = [it for it in items if _to_int(it.get("relevance")) >= 2]
        items.sort(key=lambda it: _to_int(it.get("relevance")), reverse=True)
        top_items = items[:3]
        lines.append(f"{ticker}:")
        if not top_items:
            lines.append("(no sufficiently relevant summaries)")
        else:
            for it in top_items:
                # Expecting _one_para in this module already; fallback to simple formatting
                try:
                    lines.append(_one_para(it))
                except Exception:
                    title = it.get("title") or ""
                    why = it.get("why_it_matters") or it.get("summary") or ""
                    sent =  it.get("sentiment") or ""
                    lines.append(f"{title} /\n {why} /\n {sent}")
        lines.append("")  # blank line
    body = "\n".join(lines).strip()
    return body or "No summaries available"


async def send_via_sendgrid(
    to_email: str,
    results: Dict[str, Any],
    subject: str = f"Your Asset Relevant Intelligence - {datetime.now(ist).strftime('%b %d, %Y')}",
    dry_run: bool = True,
    body_text: Optional[str] = None,
) -> Dict[str, Any]:
    # Ensure we have an API key and source address
    api_key = os.getenv("SENDGRID_API_KEY") or getattr(settings, "SENDGRID_API_KEY", None)
    from_email = os.getenv("EMAIL_FROM", "ari@example.com")
    # Build body from provided preview or locally render from results (fallback)
    body = body_text if body_text is not None else _format_body(results)

    if not api_key:
        log.error("sendgrid: SENDGRID_API_KEY missing")
        return {"ok": False, "error": "no_api_key"}

    payload = {
        "personalizations": [{"to": [{"email": to_email}], "subject": subject}],
        "from": {"email": from_email, "name": "A.R.I"},
        "content": [{"type": "text/html", "value": body}],
    }

    if dry_run:
        log.info("sendgrid.dry_run: to=%s subject=%s body_len=%d", to_email, subject, len(body or ""))
        return {"ok": True, "dry_run": True, "body_preview": (body or "")[:800]}

    start_send = time.time()
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
            )
            elapsed_ms = int((time.time() - start_send) * 1000)
            if 200 <= r.status_code < 300:
                try:
                    record_metric("email", "sendgrid", elapsed_ms, True)
                except Exception:
                    log.exception("metrics: failed to record email sendgrid success metric")
                return {"ok": True, "status": r.status_code}
            # non-2xx
            try:
                record_metric("email", "sendgrid", elapsed_ms, False)
            except Exception:
                log.exception("metrics: failed to record email sendgrid failure metric")
            log.error("sendgrid failed status=%s text=%s", r.status_code, r.text[:500])
            return {"ok": False, "status": r.status_code, "text": r.text}
    except Exception:
        elapsed_ms = int((time.time() - start_send) * 1000)
        try:
            record_metric("email", "sendgrid", elapsed_ms, False)
        except Exception:
            log.exception("metrics: failed to record email sendgrid exception metric")
        log.exception("sendgrid: exception sending mail")
        return {"ok": False, "error": "exception"}


async def send_via_smtp(
    to_email: str,
    results: Dict[str, Any],
    subject: str = f"Your Asset Relevant Intelligence - {datetime.now(ist).strftime('%b %d, %Y')}",
    dry_run: bool = True,
    body_text: Optional[str] = None,
) -> Dict[str, Any]:
    from_email = os.getenv("EMAIL_FROM", "ari@example.com")
    # Build body from provided preview or locally render from results
    body = body_text if body_text is not None else _format_body(results)

    msg = EmailMessage()
    msg["From"] = formataddr(("A.R.I", from_email))
    msg["To"] = to_email
    msg["Subject"] = subject or f"Your Asset Relevant Intelligence - {datetime.now(ist).strftime('%b %d, %Y')}"
    msg.set_content(body)

    if dry_run:
        log.info("smtp.dry_run: to=%s subject=%s body_len=%d", to_email, subject or "", len(body or ""))
        return {"ok": True, "dry_run": True, "body_preview": (body or "")[:800]}

    start_send = time.time()
    try:
        if aiosmtplib is None:
            # fallback to sync smtplib send (existing code path)
            with smtplib.SMTP(host=smtp_host, port=smtp_port, timeout=30) as s:
                s.starttls()
                s.login(smtp_user, smtp_pass)
                s.sendmail(from_email, [to_email], message.as_string())
            elapsed_ms = int((time.time() - start_send) * 1000)
            try:
                record_metric("email", "smtp", elapsed_ms, True)
            except Exception:
                log.exception("metrics: failed to record email smtp success metric")
            return {"ok": True}
        else:
            await aiosmtplib.send(message, hostname=smtp_host, port=smtp_port, username=smtp_user, password=smtp_pass)
            elapsed_ms = int((time.time() - start_send) * 1000)
            try:
                record_metric("email", "smtp", elapsed_ms, True)
            except Exception:
                log.exception("metrics: failed to record email smtp success metric")
            return {"ok": True}
    except Exception:
        elapsed_ms = int((time.time() - start_send) * 1000)
        try:
            record_metric("email", "smtp", elapsed_ms, False)
        except Exception:
            log.exception("metrics: failed to record email smtp failure metric")
        log.exception("smtp: exception sending mail")
        return {"ok": False, "error": "exception"}


def _subject_for_today() -> str:
    ist = ZoneInfo("Asia/Kolkata")
    return f"Your Asset Relevant Intelligence - {datetime.now(ist).strftime('%b %d, %Y')}"


def _strip_urls(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"https?://\S+|\bwww\.\S+\b", "", text)


def _summary_text(item: Dict[str, Any]) -> str:
    """
    Build only the summary text (no title, no sentiment), preferring:
    1) why_it_matters
    2) bullets joined into sentences
    3) first 2 sentences from content snippet
    URLs are removed.
    """
    why = (item.get("why_it_matters") or "").strip()
    if why:
        return _strip_urls(why).strip()

    bullets = item.get("bullets") or []
    parts = [b.strip().rstrip(".") for b in bullets if b and b.strip()]
    if parts:
        return _strip_urls(". ".join(parts)).strip() + "."

    content_snippet = (item.get("content") or "").strip()
    if content_snippet:
        clean = _strip_urls(content_snippet)
        sentences = re.split(r'(?<=[\.\!\?])\s+', clean)
        return " ".join(sentences[:2]).strip()

    return ""

def _one_para(item: Dict[str, Any]) -> str:
    """
    Render a single paragraph for an item:
      <Headline> — <summary>. Sentiment: <...>.
    Prefer why_it_matters, fall back to bullets joined into sentences, then content snippet.
    No URLs are included.
    """
    title = (item.get("title") or "").strip()
    # summary sources
    why = (item.get("why_it_matters") or "").strip()
    bullets = item.get("bullets") or []
    content_snippet = (item.get("content") or "").strip()
    # build summary text
    if why:
        summary = why
    elif bullets:
        parts = [b.strip().rstrip(".") for b in bullets if b and b.strip()]
        summary = ". ".join(parts)
    else:
        # fall back to a short content snippet (first 2 sentences)
        if content_snippet:
            # remove URLs and collapse whitespace
            clean = _strip_urls(content_snippet)
            sentences = re.split(r'(?<=[\.\!\?])\s+', clean)
            summary = " ".join(sentences[:2]).strip()
        else:
            summary = ""

    summary = _strip_urls(summary).strip()
    if summary and not summary.endswith("."):
        summary += "."

    sentiment = (item.get("sentiment") or "Neutral").strip()
    return (
        f"<br><b><u>{title}</u></b><br>"
        f"{summary}<br>"
        f"Sentiment: <b>{sentiment}</b><br><br>"
    )


def render_plain_email(results: Dict[str, Any]) -> str:
    """
    Compose one plain-text email for all requested tickers.
    results = {"tickers": {"TICKER": [items...]}, ...}  # or {"TICKER": {"news":[...], "summary": {...}}}
    """
    lines: list[str] = []
    total = 0
    for ticker, payload in results.items():
        lines.append(f"{ticker}:")
        items = (payload.get("summary") or {}).get("items") or payload.get("news") or []
        for it in items[:5]:
            lines.append(_one_para(it))
            total += 1
        lines.append("")  # blank line between tickers
    body = "\n".join(lines).strip()
    return body, total


def _strip_meta_lead(text: str) -> str:
    """
    Remove leading meta phrases like:
      "this article", "the article", "this report", "the report",
      "according to the article", "according to the report"
    followed by optional punctuation/whitespace. Return cleaned text with initial cap.
    """
    if not isinstance(text, str):
        return ""
    s = text.strip()
    s = re.sub(
        r'^(?:\s*(?:this|the)\s+(?:article|report)\b(?:\s*[:\-–—,]*)|'
        r'\s*according\s+to\s+(?:the\s+)?(?:article|report)\b(?:\s*[:\-–—,]*))',
        '',
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r'\s{2,}', ' ', s).strip()
    if s:
        s = s[0].upper() + s[1:]
    return s


# Export names for importers
# Export names for importers
__all__ = ["send_via_sendgrid", "send_via_smtp"]