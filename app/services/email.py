from __future__ import annotations
import os
import re
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

import httpx

log = logging.getLogger("ari.email")

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")


def _subject_for_today() -> str:
    ist = ZoneInfo("Asia/Kolkata")
    return f"Your asset relevance intelligence for the day - {datetime.now(ist).strftime('%b %d, %Y')}"


def _strip_urls(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"https?://\S+|\bwww\.\S+\b", "", text)


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
    parts = []
    if title:
        parts.append(title)
    if summary:
        parts.append("— " + summary)
    parts.append(f"Sentiment: {sentiment}.")
    return " ".join(p for p in parts if p).strip()


def render_plain_email(results: Dict[str, Any]) -> str:
    """
    Compose one plain-text email for all requested tickers.
    results = {"TICKER": {"news":[...], "summary": {...}}, ...}
    """
    lines: List[str] = []
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


async def send_via_sendgrid(to_email: str, results: Dict[str, Any], dry_run: bool = True) -> Dict[str, Optional[str]]:
    """
    Send plain text email via SendGrid. Returns dict with subject, preview and message_id (if sent).
    """
    subject = _subject_for_today()
    body, total_items = render_plain_email(results)

    log.info("email: composing %d items for %s", total_items, to_email)

    if dry_run:
        return {"subject": subject, "preview": body[:400], "message_id": None}

    if not SENDGRID_API_KEY or not EMAIL_FROM:
        log.error("SendGrid not configured; SENDGRID_API_KEY or EMAIL_FROM missing")
        raise RuntimeError("SendGrid not configured (missing SENDGRID_API_KEY or EMAIL_FROM)")

    url = "https://api.sendgrid.com/v3/mail/send"
    json_payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": EMAIL_FROM},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }
    headers = {"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(url, headers=headers, json=json_payload)
            if r.status_code not in (200, 202):
                log.error("SendGrid error %s: %s", r.status_code, r.text[:200])
                raise RuntimeError(f"SendGrid error {r.status_code}: {r.text[:200]}")
            msg_id = r.headers.get("X-Message-Id")
            log.info("email: sent to=%s provider=sendgrid message_id=%s", to_email, msg_id)
            return {"subject": subject, "preview": body[:400], "message_id": msg_id}
    except Exception as e:
        log.error("email send failed: %s", e, exc_info=True)
        raise