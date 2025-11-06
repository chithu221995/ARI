from __future__ import annotations
from email import message
import json
import os
from email.message import EmailMessage
from email.utils import formataddr
import logging
import httpx
from typing import Dict, Any, Optional
import re
import asyncio
import smtplib
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List
from app.core import settings
import time
from app.core.metrics import record_metric
from app.core.retry_utils import rate_limited_retry



log = logging.getLogger("ari.services.email")
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


@rate_limited_retry(
    provider="sendgrid",
    max_retries=2,
    base_delay=2.0,
    max_per_minute=10
)
async def send_via_sendgrid(
    to_email: str,
    payload_results: Dict[str, Any],
    dry_run: bool = False,
    body_text: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send email via SendGrid API with automatic retries and rate limiting.
    
    Returns:
        Dict with keys: ok (bool), provider_message_id (str|None), error (str|None), 
        status_code (int|None), response_body (str|None)
    """
    import os
    
    sendgrid_key = os.getenv("SENDGRID_API_KEY", "")
    from_email = os.getenv("EMAIL_FROM", "noreply@onthesubjectofmoney.com")
    
    if not sendgrid_key:
        error_msg = "SENDGRID_API_KEY not configured"
        log.error("send_via_sendgrid: %s", error_msg)
        return {
            "ok": False,
            "error": error_msg,
            "provider_message_id": None,
            "status_code": None,
            "response_body": None
        }
    
    if dry_run:
        log.info("send_via_sendgrid: dry_run=True, skipping actual send to %s", to_email)
        return {
            "ok": True,
            "dry_run": True,
            "provider_message_id": None,
            "status_code": None,
            "response_body": None
        }
    
    # Build email content
    subject = "Your Daily ARI Brief"
    html_content = f"<html><body><pre>{body_text or 'No content'}</pre></body></html>"
    text_content = body_text or "No content"
    
    payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": text_content},
            {"type": "text/html", "value": html_content}
        ]
    }
    
    log.info("send_via_sendgrid: sending to %s via SendGrid", to_email)
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {sendgrid_key}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=20
        )
    
    status_code = response.status_code
    response_body = response.text
    provider_message_id = response.headers.get("X-Message-Id")
    ok = 200 <= status_code < 300
    
    log.info(
        "sendgrid_result",
        extra={
            "status": status_code,
            "body": response_body[:400],
            "ok": ok,
            "to": to_email,
            "msg_id": provider_message_id
        }
    )
    
    if not ok:
        error_msg = f"SendGrid returned {status_code}: {response_body[:500]}"
        log.error("send_via_sendgrid: %s", error_msg)
        # Raise for retry if it's a transient error
        if status_code == 429 or 500 <= status_code < 600:
            response.raise_for_status()
        return {
            "ok": False,
            "error": error_msg,
            "provider_message_id": provider_message_id,
            "status_code": status_code,
            "response_body": response_body[:1000]
        }
    
    log.info("send_via_sendgrid: successfully sent to %s (status=%d, msg_id=%s)", 
             to_email, status_code, provider_message_id)
    
    return {
        "ok": True,
        "provider_message_id": provider_message_id,
        "error": None,
        "status_code": status_code,
        "response_body": response_body[:1000]
    }


async def send_via_smtp(
    to_email: str,
    payload_results: Dict[str, Any],
    dry_run: bool = False,
    body_text: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send email via SMTP.
    
    Returns:
        Dict with keys: ok (bool), error (str|None)
    """
    import os
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    smtp_host = os.getenv("SMTP_HOST", "")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    from_email = os.getenv("EMAIL_FROM", "noreply@onthesubjectofmoney.com")
    
    if not all([smtp_host, smtp_user, smtp_pass]):
        error_msg = "SMTP credentials not fully configured"
        log.error("send_via_smtp: %s", error_msg)
        return {"ok": False, "error": error_msg}
    
    if dry_run:
        log.info("send_via_smtp: dry_run=True, skipping actual send to %s", to_email)
        return {"ok": True, "dry_run": True}
    
    try:
        # Build email
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Your Daily ARI Brief"
        msg["From"] = from_email
        msg["To"] = to_email
        
        text_part = MIMEText(body_text or "No content", "plain")
        html_part = MIMEText(f"<html><body><pre>{body_text or 'No content'}</pre></body></html>", "html")
        
        msg.attach(text_part)
        msg.attach(html_part)
        
        # Send via SMTP
        log.info("send_via_smtp: sending to %s via SMTP (%s:%d)", to_email, smtp_host, smtp_port)
        
        with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        
        log.info("send_via_smtp: successfully sent to %s", to_email)
        return {"ok": True, "error": None}
        
    except smtplib.SMTPException as exc:
        error_msg = f"SMTPException: {exc}"
        log.exception("send_via_smtp: SMTP error sending to %s", to_email)
        return {"ok": False, "error": error_msg}
        
    except Exception as exc:
        error_msg = f"{exc.__class__.__name__}: {exc}"
        log.exception("send_via_smtp: unexpected error sending to %s", to_email)
        return {"ok": False, "error": error_msg}


def _one_para(item: Dict[str, Any]) -> str:
    """Format a single summary item as a paragraph."""
    title = item.get("title", "Untitled")
    url = item.get("url", "")
    summary = item.get("why_it_matters", "") or item.get("summary", "")
    relevance = item.get("relevance", "")
    sentiment = item.get("sentiment", "")
    
    parts = [f"• {title}"]
    if summary:
        parts.append(f"  {summary[:300]}")
    if relevance or sentiment:
        meta = []
        if relevance:
            meta.append(f"Relevance: {relevance}")
        if sentiment:
            meta.append(f"Sentiment: {sentiment}")
        parts.append(f"  [{' | '.join(meta)}]")
    if url:
        parts.append(f"  {url}")
    
    return "\n".join(parts)