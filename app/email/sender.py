from __future__ import annotations
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from typing import List, Dict, Optional

# SendGrid optional imports
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
except Exception:
    SendGridAPIClient = None
    Mail = None

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER or "no-reply@example.com")


def _render_brief_body(ticker: str, news: List[Dict], summaries_map: Optional[Dict[str, Dict]] = None) -> str:
    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    lines: List[str] = []
    lines.append(f"Brief: {ticker} — {now}")
    lines.append("")
    lines.append("News (max 5):")
    if news:
        for n in news[:5]:
            title = n.get("title", "") or "<no title>"
            src = n.get("source", "") or ""
            url = n.get("url", "") or ""
            lines.append(f"- {title} ({src})")
            if url:
                lines.append(f"  {url}")
    else:
        lines.append("- (none)")

    if summaries_map:
        lines.append("")
        lines.append("Summaries:")
        for s in summaries_map.values() if isinstance(summaries_map, dict) else summaries_map:
            title = s.get("title") or ""
            bullets = s.get("bullets") or []
            why = s.get("why_it_matters") or ""
            sentiment = s.get("sentiment") or ""
            lines.append(f"- {title} (sentiment: {sentiment})")
            for b in bullets:
                lines.append(f"  • {b}")
            if why:
                lines.append(f"  Why: {why}")
            lines.append("")

    return "\n".join(lines) + "\n"


def render_combined_body(sections: List[Dict]) -> str:
    lines: List[str] = []
    for sec in sections:
        t = (sec.get("ticker") or "").upper()
        lines.append(f"=== {t} ===")
        # News
        if sec.get("news"):
            lines.append("News:")
            for it in sec["news"][:5]:
                src = it.get("source", "")
                lines.append(f"- {it.get('title','')} — {src}")
        # Summaries
        if sec.get("summaries"):
            lines.append("")
            lines.append("Summaries:")
            for s in sec["summaries"][:5]:
                sent = s.get("sentiment","Neutral")
                lines.append(f"- {s.get('title','')} [{sent}]")
                for b in (s.get("bullets") or [])[:3]:
                    lines.append(f"  • {b}")
                wim = s.get("why_it_matters","")
                if wim:
                    lines.append(f"  Why it matters: {wim}")
        lines.append("")  # spacer
    return "\n".join(lines).strip() + "\n"


def _send_via_sendgrid(to_email: str, from_email: str, subject: str, body: str, api_key: str) -> Optional[str]:
    if not SendGridAPIClient or not Mail:
        raise RuntimeError("sendgrid package not installed")
    sg = SendGridAPIClient(api_key)
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        plain_text_content=body,
    )
    resp = sg.send(message)
    hdrs = getattr(resp, "headers", {}) or {}
    return hdrs.get("X-Message-Id") or hdrs.get("X-Message-ID") or None


def send_combined_brief(to_email: str, sections: List[Dict], *, subject: Optional[str] = None) -> Optional[str]:
    provider = (os.getenv("EMAIL_PROVIDER", "smtp") or "smtp").lower()
    from_email = os.getenv("EMAIL_FROM", EMAIL_FROM)
    if not subject:
        tickers_str = ", ".join([sec.get("ticker","").upper() for sec in sections if sec.get("ticker")])
        subject = f"ARI Brief: {tickers_str} — {datetime.utcnow().strftime('%Y-%m-%d')}"
    body = render_combined_body(sections)

    if provider == "sendgrid":
        api_key = os.getenv("SENDGRID_API_KEY") or ""
        if not SendGridAPIClient or not Mail:
            raise RuntimeError("sendgrid package not installed")
        if not api_key:
            raise RuntimeError("SENDGRID_API_KEY not configured")
        return _send_via_sendgrid(to_email, from_email, subject, body, api_key)

    # fallback: send single combined email via SMTP if configured
    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST not configured")
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        try:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
        except Exception:
            pass
        if SMTP_USER and SMTP_PASS:
            smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)
    return None


def send_brief_email(to_email: str, ticker: str, news: List[Dict], summaries: Optional[Dict[str, Dict]] = None) -> Optional[str]:
    """
    Backwards-compatible single-ticker send. Keeps previous behavior.
    """
    provider = os.getenv("EMAIL_PROVIDER", "smtp").lower()
    subject = f"ARI brief: {ticker} — {datetime.utcnow().date().isoformat()}"
    body = _render_brief_body(ticker, news, summaries)

    if provider == "sendgrid":
        api_key = os.getenv("SENDGRID_API_KEY", "")
        if not api_key:
            raise RuntimeError("SENDGRID_API_KEY not configured")
        return _send_via_sendgrid(to_email, os.getenv("EMAIL_FROM", EMAIL_FROM), subject, body, api_key)

    if not SMTP_HOST:
        raise RuntimeError("SMTP_HOST not configured")
    msg = EmailMessage()
    msg["From"] = os.getenv("EMAIL_FROM", EMAIL_FROM)
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        try:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
        except Exception:
            pass
        if SMTP_USER and SMTP_PASS:
            smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)
    return None


__all__ = ["send_brief_email", "_render_brief_body", "render_combined_body", "send_combined_brief"]