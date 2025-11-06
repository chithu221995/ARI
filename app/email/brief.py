from __future__ import annotations
import os
import logging
import time
from typing import List, Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
import hashlib
import sqlite3

from app.db.queries import fetch_recent_summaries, insert_run, insert_email_log
from app.email.feedback import make_feedback_token
from app.core import settings

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
                    # Determine sentiment color
                    sent_lower = sent.lower()
                    if "positive" in sent_lower:
                        sentiment_color = "#006400"  # Dark green for contrast
                        sentiment_text = "Positive"
                    elif "negative" in sent_lower:
                        sentiment_color = "#DC143C"  # Crimson red
                        sentiment_text = "Negative"
                    else:
                        sentiment_color = "#555"  # Neutral gray
                        sentiment_text = sent
                    
                    badges.append(
                        f"<span style='font-size:12px;padding:2px 6px;border:1px solid #ddd;"
                        f"border-radius:4px;margin-left:6px;background:#f5f5f5;'>"
                        f"Sentiment: <strong><u style='color:{sentiment_color};'>{sentiment_text}</u></strong></span>"
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


def _hash16(url: str) -> str:
    """Generate a 16-char hash from URL for deduplication."""
    return hashlib.sha256((url or "").encode("utf-8")).hexdigest()[:16]


def _parse_published(timestamp: str) -> str | None:
    """Parse published timestamp to ISO format for SQLite."""
    if not timestamp:
        return None
    try:
        # Handle ISO format with or without 'Z'
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        log.warning(f"Failed to parse published timestamp '{timestamp}': {e}")
        return None


def normalize_domain(url: str) -> str:
    """Extract and normalize domain from URL (strip www. prefix)."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Strip www. prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return ""


def _insert_email_items(email_log_id: int, items: List[Dict[str, Any]]):
    """
    Insert email_items records for tracking what was sent.
    
    Args:
        email_log_id: ID of the email_logs entry
        items: List of summary items that were sent
    """
    from app.core.cache import CACHE_DB_PATH

    # Guard: bail out if email_log_id is falsy
    if not email_log_id:
        log.error("Failed to insert email_items: email_log_id is None")
        return

    try:
        with sqlite3.connect(CACHE_DB_PATH, timeout=5) as conn:
            for item in items:
                url = item.get("url") or item.get("link") or ""
                ticker = item.get("ticker") or item.get("symbol") or ""
                
                # DEBUG: Log what fields are available
                log.debug(f"email_items: item keys = {list(item.keys())}")
                
                # The problem: summaries might have different field names
                # Let's check ALL possible timestamp fields
                published_hint = (
                    item.get("published_hint") or 
                    item.get("published_at") or 
                    item.get("pubDate") or 
                    item.get("pub_date") or
                    item.get("created_at") or
                    item.get("date") or  # ADD THIS
                    item.get("timestamp") or  # ADD THIS
                    item.get("published") or  # ADD THIS
                    ""
                )
                
                # ADD DEBUG
                if not published_hint:
                    log.warning(f"email_items: NO published_hint found for item with keys: {list(item.keys())}")

                domain = normalize_domain(item.get("url", ""))
                url_hash = _hash16(url)
                published_at = _parse_published(published_hint)
                
                # ADD DEBUG
                if not published_at:
                    log.warning(f"email_items: failed to parse published_hint='{published_hint}' for url={url}")

                conn.execute("""
                    INSERT INTO email_items 
                    (email_log_id, ticker, item_url, item_url_hash, domain, published_at, sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    email_log_id,
                    ticker,
                    url,
                    url_hash,
                    domain,
                    published_at
                ))
            conn.commit()
            log.info(f"Inserted {len(items)} email_items for email_log_id={email_log_id}")
    except Exception as e:
        log.error(f"Failed to insert email_items for log_id={email_log_id}: {e}", exc_info=True)


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
    started = datetime.utcnow().isoformat() + "Z"
    provider_message_id = None
    items_count = 0
    subject = ""
    email_log_id = None
    
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
            error_msg = f"EMAIL_PROVIDER is '{prov}' (expected 'sendgrid')"
            log.error("email.brief: %s; not sending", error_msg)
            insert_run("email", None, 0, f"to={email} err=provider_not_sendgrid", started_at=started)
            insert_email_log(email, "N/A", 0, prov or "none", False, error_msg)
            log.info(f"email_logs: to={email} items=0 ok=False provider={prov} msg_id=None")
            return False

        # Load summaries from cache for the given tickers
        items = fetch_recent_summaries(
            tickers, hours=12, max_per_ticker=3, min_relevance=4
        )
        items_count = len(items)
        
        log.info(
            "email.brief: to=%s tickers=%s provider=%s items=%d",
            email, tickers, prov, items_count
        )
        
        log.info(
            "email.brief: per-user assembled items=%d (%s)",
            items_count, ",".join(tickers)
        )

        # Create email log entry BEFORE sending to get the ID
        subject = build_subject()
        sent_at = datetime.utcnow().isoformat() + "Z"
        
        # Try to insert via the helper function
        email_log_id = insert_email_log(
            email, 
            subject, 
            items_count, 
            "sendgrid", 
            False,  # Will update to True on success
            None
        )

        # If insert_email_log returns None, create the log entry directly
        if not email_log_id:
            from app.core.cache import CACHE_DB_PATH
            log.warning("email.brief: insert_email_log returned None, creating log entry directly")
            try:
                with sqlite3.connect(CACHE_DB_PATH, timeout=5) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO email_logs (to_email, subject, items_count, provider, ok, sent_at)
                        VALUES (?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
                        """,
                        (email, subject, items_count, "sendgrid")
                    )
                    email_log_id = cur.lastrowid
                    conn.commit()
                    log.info("email.brief: created email_log_id=%s directly", email_log_id)
            except Exception as e:
                log.error("email.brief: failed to create email_log entry: %s", e)

        # Insert email_items only if we have a valid email_log_id
        if email_log_id:
            _insert_email_items(email_log_id, items)
        else:
            log.error("email.brief: skipping email_items insert because email_log_id is None")

        # Generate feedback token and link
        app_base_url = getattr(settings, "APP_BASE_URL", None) or os.getenv("APP_BASE_URL", "http://localhost:8000")
        feedback_token = make_feedback_token(email, email_log_id, sent_at)
        feedback_link = f"{app_base_url}/admin/email/f/{feedback_token}"

        log.info("email.brief: generated feedback link for log_id=%s", email_log_id)

        # Assemble email body from summaries
        html_body = _assemble_html_body(items, tickers)
        text_body = _assemble_text_body(items, tickers)

        # Add feedback section to HTML body
        html_body += f"""
        <hr style="margin: 30px 0; border: none; border-top: 1px solid #ddd;">
        <div style="text-align: center; padding: 20px; background: #f9f9f9; border-radius: 8px;">
            <p style="margin: 0 0 10px 0; color: #666; font-size: 14px;">
                📊 How useful was this email?
            </p>
            <a href="{feedback_link}" 
               style="display: inline-block; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                      color: white; text-decoration: none; padding: 10px 24px; border-radius: 6px; 
                      font-weight: 600; font-size: 14px;">
                Leave Feedback ⭐
            </a>
        </div>
        """
        
        # Add feedback section to plain text body
        text_body += f"""
+
+━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
+
+📊 How useful was this email?
+
+Leave feedback: {feedback_link}
+
+Your input helps us make ARI Brief better!
+"""

        # Send via SendGrid
        import httpx
        from app.core.metrics import record_vendor_event

        sendgrid_key = os.getenv("SENDGRID_API_KEY", "")
        from_email = os.getenv("EMAIL_FROM", "ari@onthesubjectofmoney.com")

        if not sendgrid_key:
            error_msg = "SENDGRID_API_KEY not configured"
            log.error("email.brief: %s", error_msg)
            insert_run("email", None, 0, f"to={email} err=no_api_key", started_at=started)
            # Update existing log entry
            _update_email_log(email_log_id, False, error_msg, None)
            log.info(f"email_logs: to={email} items={items_count} ok=False provider=sendgrid msg_id=None")
            return False

        # Clean and validate API key
        api_key = sendgrid_key.strip().strip('"').strip("'")
        
        # Debug log for API key sanity check (don't log the full key)
        log.info(
            "sendgrid.debug",
            extra={
                "key_len": len(api_key),
                "key_prefix": api_key[:3] if len(api_key) >= 3 else "TOO_SHORT",
                "has_bearer_prefix": api_key.lower().startswith("bearer "),
            }
        )

        payload = {
            "personalizations": [{"to": [{"email": email}]}],
            "from": {
                "email": from_email,
                "name": "A.R.I Brief"
            },
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
            ],
            "custom_args": {
                "email_sent_at": sent_at,
                "email_log_id": str(email_log_id)
            }
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        start_time = time.perf_counter()
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers=headers,
                json=payload,
                timeout=20
            )
        
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        log.info("email.brief: sendgrid status=%s", resp.status_code)

        # Try to extract message ID from response headers
        provider_message_id = resp.headers.get("X-Message-Id")

        ok = (resp.status_code == 202)
        record_vendor_event(
            provider="sendgrid",
            event="email",
            ok=ok,
            latency_ms=latency_ms
        )

        if resp.status_code >= 300:
            body = (await resp.aread()).decode('utf-8', errors='replace')
            error_msg = f"status={resp.status_code} body={body[:500]}"
            log.error("email.brief: sendgrid error body=%s", body[:500])
            insert_run("email", None, 0, f"to={email} status={resp.status_code}", started_at=started)
            # Update existing log entry
            _update_email_log(email_log_id, False, error_msg, provider_message_id)
            log.info(f"email_logs: to={email} items={items_count} ok=False provider=sendgrid msg_id={provider_message_id}")
            return False

        # Success path
        note = f"to={email} items={items_count} tickers={','.join(tickers)}"
        insert_run("email", None, 1, note, started_at=started)
        # Update existing log entry
        _update_email_log(email_log_id, True, None, provider_message_id)
        log.info(f"email_logs: to={email} items={items_count} ok=True provider=sendgrid msg_id={provider_message_id}")
        return True

    except Exception as e:
        error_msg = str(e)
        log.exception("email.brief: failed to send email to %s", email)
        insert_run("email", None, 0, f"to={email} err={e}", started_at=started)
        # Update existing log entry if we have one
        if email_log_id:
            _update_email_log(email_log_id, False, error_msg, provider_message_id)
        else:
            insert_email_log(email, subject or "N/A", items_count, "sendgrid", False, error_msg, provider_message_id)
        log.info(f"email_logs: to={email} items={items_count} ok=False provider=sendgrid msg_id={provider_message_id}")
        return False


def _update_email_log(log_id: int, ok: bool, error: str | None, provider_message_id: str | None):
    """
    Update an existing email_logs entry with send status.
    
    Args:
        log_id: ID of the email_logs entry
        ok: Whether send was successful
        error: Error message if failed
        provider_message_id: SendGrid message ID
    """
    import sqlite3
    from app.core.cache import CACHE_DB_PATH
    
    try:
        with sqlite3.connect(CACHE_DB_PATH, timeout=5) as conn:
            conn.execute(
                """
                UPDATE email_logs 
                SET ok = ?, error = ?, provider_message_id = ?
                WHERE id = ?
                """,
                (1 if ok else 0, error, provider_message_id, log_id)
            )
            conn.commit()
    except Exception as e:
        log.error("_update_email_log: failed to update log_id=%d - %s", log_id, e)

