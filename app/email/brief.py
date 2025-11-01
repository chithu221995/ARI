from __future__ import annotations
import os
import logging
import time
from typing import List, Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

from app.db.queries import fetch_recent_summaries

log = logging.getLogger("ari.email")


def _provider():
    """Get configured email provider."""
    return (os.getenv("EMAIL_PROVIDER") or "").strip().lower()


def build_subject() -> str:
    """Build email subject line with IST date."""
    ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    # Portable day without leading zero:
    day = str(ist.day)
    return f"Your Asset Relevance Intelligence for the day - {ist:%a}, {ist:%b} {day}, {ist:%Y}"


def _assemble_html_body(items: List[Dict[str, Any]], tickers: List[str]) -> str:
    """Assemble HTML email body from summary items."""
    html_parts = [
        "<html><body style='font-family:Arial,sans-serif;'>",
        "<h1 style='color:#333;'>Your Daily ARI Brief</h1>",
        f"<p style='color:#666;'>Latest insights for: <strong>{', '.join(tickers)}</strong></p>",
        "<hr style='border:none;border-top:1px solid #ddd;margin:20px 0;'>"
    ]
    
    if not items:
        html_parts.append("<p><em>No recent summaries found for your tickers.</em></p>")
    else:
        # Group by ticker
        by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            ticker = item.get("ticker", "")
            by_ticker.setdefault(ticker, []).append(item)
        
        for ticker in tickers:
            ticker_items = by_ticker.get(ticker, [])
            if not ticker_items:
                continue
            
            html_parts.append(f"<h2 style='color:#0066cc;margin-top:30px;'>{ticker}</h2>")
            html_parts.append("<ul style='list-style:none;padding:0;'>")
            
            for it in ticker_items:
                title = it.get("title", "Untitled")
                url = it.get("url", "#")
                summary = it.get("summary", "")[:700]  # Safe length limit
                rel = it.get("relevance")
                sent = (it.get("sentiment") or "").strip()
                
                # Build badges
                badges = []
                if sent:
                    badges.append(
                        f"<span style='font-size:12px;padding:2px 6px;border:1px solid #ddd;"
                        f"border-radius:4px;margin-left:6px;background:#f5f5f5;'>Sentiment: {sent}</span>"
                    )
                badges_html = " ".join(badges)
                
                html_parts.append(f"""
                <li style="margin-bottom:20px;border-bottom:1px solid #eee;padding-bottom:15px;">
                  <a href="{url}" target="_blank" style="color:#0066cc;font-weight:bold;text-decoration:none;">{title}</a><br/>
                  <div style="margin:8px 0;color:#555;line-height:1.5;">{summary}</div>
                  <div style="margin-top:8px;">{badges_html}</div>
                </li>
                """)
            
            html_parts.append("</ul>")
    
    html_parts.append("</body></html>")
    return "".join(html_parts)


def _assemble_text_body(items: List[Dict[str, Any]], tickers: List[str]) -> str:
    """Assemble plain text email body from summary items."""
    text_parts = [
        "Your Daily ARI Brief",
        "=" * 50,
        f"Latest insights for: {', '.join(tickers)}",
        ""
    ]
    
    if not items:
        text_parts.append("No recent summaries found for your tickers.")
    else:
        # Group by ticker
        by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            ticker = item.get("ticker", "")
            by_ticker.setdefault(ticker, []).append(item)
        
        for ticker in tickers:
            ticker_items = by_ticker.get(ticker, [])
            if not ticker_items:
                continue
            
            text_parts.extend([
                "",
                f"=== {ticker} ===",
                ""
            ])
            
            for it in ticker_items:
                title = it.get("title", "Untitled")
                url = it.get("url", "#")
                summary = it.get("summary", "")[:700]  # Safe length limit
                rel = it.get("relevance")
                sent = (it.get("sentiment") or "").strip()
                
                text_parts.extend([
                    f"• {title}",
                    f"  {summary}",
                ])
                
                # Add metadata
                meta = []
                if isinstance(rel, int):
                    meta.append(f"Relevance: {rel}")
                if sent:
                    meta.append(f"Sentiment: {sent}")
                if meta:
                    text_parts.append(f"  [{' | '.join(meta)}]")
                
                text_parts.append(f"  Read more: {url}")
                text_parts.append("")
    
    return "\n".join(text_parts)


async def send_brief_email(email: str, tickers: List[str]) -> bool:
    """
    Assemble and send a daily brief email to a user containing summaries
    for their selected tickers.

    Args:
        email: Recipient email address
        tickers: List of ticker symbols for this user (ordered by rank)

    Returns:
        True if email sent successfully, False otherwise
    """
    try:
        # Normalize ticker case
        tickers = [(t or "").upper() for t in tickers if t]
        
        # Log what we're about to do
        log.info(
            "email.brief: to=%s tickers=%s provider=%s",
            email, tickers, _provider()
        )

        # Check provider is configured
        prov = _provider()
        if prov != "sendgrid":
            log.error(
                "email.brief: EMAIL_PROVIDER is '%s' (expected 'sendgrid'); not sending",
                prov
            )
            return False

        # Load summaries from cache for the given tickers
        items = fetch_recent_summaries(
            tickers, hours=12, max_per_ticker=3, min_relevance=4
        )
        
        log.info(
            "email.brief: to=%s tickers=%s provider=%s items=%d",
            email, tickers, prov, len(items)
        )
        
        log.info(
            "email.brief: per-user assembled items=%d (%s)",
            len(items), ",".join(tickers)
        )

        # Assemble email body from summaries
        html_body = _assemble_html_body(items, tickers)
        text_body = _assemble_text_body(items, tickers)
        subject = build_subject()

        # Send via SendGrid
        import httpx
        from app.core.metrics import record_metric

        sendgrid_key = os.getenv("SENDGRID_API_KEY", "")
        from_email = {"email": os.getenv("EMAIL_FROM", "ari@onthesubjectofmoney.com"),
              "name": "A.R.I Brief"}

        if not sendgrid_key:
            log.error("email.brief: SENDGRID_API_KEY not configured")
            return False

        payload = {
            "personalizations": [{"to": [{"email": email}]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [
                {
                    "type": "text/plain",
                    "value": text_body
                },
                {
                    "type": "text/html",
                    "value": html_body
                }
            ]
        }

        start_time = time.perf_counter()
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={
                    "Authorization": f"Bearer {sendgrid_key}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=20
            )
        
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        log.info("email.brief: sendgrid status=%s", resp.status_code)

        ok = (resp.status_code == 202)
        record_metric("email", "sendgrid", latency_ms, ok)

        if resp.status_code >= 300:
            body = (await resp.aread()).decode('utf-8', errors='replace')
            log.error("email.brief: sendgrid error body=%s", body[:500])
            return False

        return True

    except Exception:
        log.exception("email.brief: failed to send email to %s", email)
        return False

