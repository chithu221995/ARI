from __future__ import annotations
import logging
import os
import time
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Query, HTTPException, Request, Form
from fastapi.responses import HTMLResponse
import httpx
import sqlite3
import aiosqlite
from datetime import datetime
from itsdangerous import URLSafeSerializer, BadSignature

from app.email.brief import send_brief_email
from app.core.cache import CACHE_DB_PATH
from app.core import settings
from app.email.feedback import make_feedback_token, parse_feedback_token

log = logging.getLogger("ari.admin.email")
router = APIRouter(tags=["admin:email"])

# ============================================================================
# EMAIL SENDING ENDPOINTS
# ============================================================================

@router.post("/test", summary="Send test email")
async def send_test_email(to: str = Query(..., description="Recipient email")):
    """
    Send a test email to a single recipient with sample ticker (TCS).
    Returns success status and provider message ID or error details.
    """
    try:
        log.info("admin.email.test: sending to %s", to)
        ok = await send_brief_email(email=to, tickers=["TCS"])
        
        if ok:
            return {
                "ok": True,
                "message": f"Test email sent to {to}",
                "provider": "sendgrid"
            }
        else:
            return {
                "ok": False,
                "error": "Email send failed (check logs for details)",
                "provider": "sendgrid"
            }
    except Exception as e:
        log.exception("admin.email.test: failed")
        return {
            "ok": False,
            "error": str(e),
            "provider": "sendgrid"
        }


@router.get("/brief", summary="Send brief emails (GET)")
@router.post("/brief", summary="Send brief emails (POST)")
async def send_brief(to: Optional[str] = Query(None, description="Optional: send to specific email only")):
    """
    Send daily brief emails.
    - If `to` is provided: sends only to that recipient
    - If `to` is None: sends to all eligible pilot users (fan-out)
    
    Returns summary of sends with success count and any errors.
    """
    try:
        errors = []
        sent_count = 0
        
        if to:
            # Single recipient mode
            log.info("admin.email.brief: sending to single recipient %s", to)
            # For testing, we'll use a sample ticker list
            # In production, you'd fetch the user's actual ticker preferences
            ok = await send_brief_email(email=to, tickers=["TCS", "INFY"])
            
            if ok:
                sent_count = 1
            else:
                errors.append({"email": to, "error": "Send failed"})
            
            return {
                "ok": sent_count > 0,
                "sent_count": sent_count,
                "errors": errors,
                "mode": "single"
            }
        else:
            # Fan-out mode: send to all eligible users
            log.info("admin.email.brief: fan-out mode (all eligible users)")
            
            # TODO: Fetch all pilot users from database
            # For now, this is a placeholder
            # In production, you'd do something like:
            # users = get_pilot_users_with_tickers()
            # for user in users:
            #     ok = await send_brief_email(user.email, user.tickers)
            #     if ok: sent_count += 1
            #     else: errors.append({"email": user.email, "error": "..."})
            
            log.warning("admin.email.brief: fan-out not yet implemented, use ?to=email for testing")
            
            return {
                "ok": False,
                "sent_count": 0,
                "errors": [],
                "mode": "fanout",
                "message": "Fan-out mode not yet implemented. Use ?to=email parameter for testing."
            }
            
    except Exception as e:
        log.exception("admin.email.brief: failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/config", summary="Check email configuration")
async def email_config():
    """
    Check email configuration and verify SendGrid API key by testing scopes endpoint.
    
    Returns:
        Configuration status including provider, credentials set, and SendGrid API key validation.
    """
    try:
        provider = (os.getenv("EMAIL_PROVIDER") or "").strip().lower()
        from_email = os.getenv("EMAIL_FROM", "")
        sendgrid_key = os.getenv("SENDGRID_API_KEY", "")
        
        api_key_set = bool(sendgrid_key)
        from_set = bool(from_email)
        
        # Verify SendGrid API key by calling scopes endpoint
        scopes_ok = False
        scopes_status = None
        scopes_error = None
        
        if api_key_set and provider == "sendgrid":
            try:
                # Clean API key (same logic as send function)
                api_key = sendgrid_key.strip().strip('"').strip("'")
                
                # Build headers exactly as we do for sending
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                # Test the key by calling scopes endpoint
                log.info("admin.email.config: verifying SendGrid API key via /v3/scopes")
                
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        "https://api.sendgrid.com/v3/scopes",
                        headers=headers,
                        timeout=8
                    )
                
                scopes_status = resp.status_code
                scopes_ok = (resp.status_code == 200)
                
                if not scopes_ok:
                    scopes_error = f"SendGrid returned {resp.status_code}"
                    if resp.status_code == 401:
                        scopes_error += " (Unauthorized - invalid API key)"
                    elif resp.status_code == 403:
                        scopes_error += " (Forbidden - key lacks permissions)"
                
                log.info(
                    "admin.email.config: scopes check status=%d ok=%s",
                    scopes_status, scopes_ok
                )
                
            except httpx.TimeoutException:
                scopes_error = "Request timeout (8s)"
                log.warning("admin.email.config: scopes check timed out")
            except httpx.RequestError as e:
                scopes_error = f"Request error: {e}"
                log.warning("admin.email.config: scopes check failed - %s", e)
            except Exception as e:
                scopes_error = f"Unexpected error: {e}"
                log.exception("admin.email.config: scopes check failed")
        
        return {
            "provider": provider,
            "from_set": from_set,
            "from_email": from_email if from_set else None,
            "api_key_set": api_key_set,
            "scopes_ok": scopes_ok,
            "scopes_status": scopes_status,
            "scopes_error": scopes_error,
            "ready": (provider == "sendgrid" and from_set and api_key_set and scopes_ok)
        }
        
    except Exception as e:
        log.exception("admin.email.config: failed")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# SENDGRID WEBHOOK ENDPOINT
# ============================================================================

@router.post("/events/sendgrid", summary="Receive SendGrid webhook events")
async def receive_sendgrid_events(request: Request):
    """
    Receive SendGrid Event Webhook payloads (JSON list of events).
    Each event has: event, email, timestamp, sg_message_id, url, etc.
    Logs events (processed, delivered, open, click, bounce, dropped, spamreport).
    Inserts into email_events table for analytics.
    Return {"ok": True, "received": len(events), "inserted": count}.
    """
    try:
        # Parse JSON body (SendGrid sends a list of events)
        try:
            body = await request.json()
        except Exception as e:
            log.error("admin.email.events: invalid JSON body - %s", e)
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        
        # Ensure it's a list
        if not isinstance(body, list):
            log.error("admin.email.events: body is not a list, type=%s", type(body).__name__)
            raise HTTPException(status_code=400, detail="Expected JSON array of events")
        
        events: List[Dict[str, Any]] = body
        received_count = len(events)
        inserted_count = 0
        
        log.info("admin.email.events: received %d events from SendGrid", received_count)
        
        # Get database path
        db_path = getattr(settings, "SQLITE_PATH", None) or CACHE_DB_PATH
        
        # Process each event
        async with aiosqlite.connect(db_path) as db:
            for event_data in events:
                try:
                    # Extract fields (SendGrid event structure)
                    event_type = event_data.get("event", "")
                    email = event_data.get("email", "")
                    timestamp = event_data.get("timestamp")  # Unix timestamp (when event occurred)
                    sg_message_id = event_data.get("sg_message_id", "")
                    url = event_data.get("url", "")  # For clicks
                    ip = event_data.get("ip", "")  # User's IP
                    useragent = event_data.get("useragent", "")  # User's browser/client
                    
                    # Extract custom args that contain email_sent_at (if we set it when sending)
                    # SendGrid passes custom arguments in the webhook payload
                    email_sent_at = None
                    custom_args = event_data.get("custom_args", {})
                    if isinstance(custom_args, dict):
                        email_sent_at = custom_args.get("email_sent_at")
                    
                    # If not in custom_args, try to look it up from email_logs table
                    # Note: email_logs may not have sg_message_id stored, so this is best-effort
                    if not email_sent_at and sg_message_id and email:
                        try:
                            cursor = await db.execute(
                                """
                                SELECT sent_at 
                                FROM email_logs 
                                WHERE email = ? 
                                  AND sent_at >= datetime('now', '-7 days')
                                ORDER BY sent_at DESC 
                                LIMIT 1
                                """,
                                (email,)
                            )
                            row = await cursor.fetchone()
                            if row:
                                email_sent_at = row[0]
                        except Exception as lookup_error:
                            log.debug("Could not lookup email_sent_at from email_logs: %s", lookup_error)
                    
                    # Convert timestamp to ISO format
                    created_at = None
                    if timestamp:
                        try:
                            created_at = datetime.utcfromtimestamp(int(timestamp)).isoformat() + "Z"
                        except Exception:
                            created_at = datetime.utcnow().isoformat() + "Z"
                    else:
                        created_at = datetime.utcnow().isoformat() + "Z"
                    
                    # Log event (concise format)
                    log.info(
                        "sendgrid: %s %s sg_id=%s url=%s sent=%s",
                        event_type,
                        email,
                        sg_message_id[:20] if sg_message_id else "none",
                        url[:50] if url else "",
                        email_sent_at[:19] if email_sent_at else "unknown"
                    )
                    
                    # Insert into email_events table (ignore duplicates based on unique constraint)
                    await db.execute(
                        """
                        INSERT OR IGNORE INTO email_events 
                        (email, event_type, item_url_hash, ip, email_sent_at, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            email,
                            event_type,
                            url or None,  # Store clicked URL in item_url_hash for now
                            ip or None,
                            email_sent_at,  # When the original email was sent
                            created_at
                        )
                    )
                    inserted_count += 1
                    
                except Exception as e:
                    log.warning(
                        "admin.email.events: failed to process event - %s",
                        e,
                        extra={"event": event_data}
                    )
                    continue
            
            await db.commit()
        
        log.info(
            "admin.email.events: processed %d events, inserted %d",
            received_count,
            inserted_count
        )
        
        return {
            "ok": True,
            "received": received_count,
            "inserted": inserted_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.exception("admin.email.events: failed to process webhook")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# FEEDBACK ROUTES
# ============================================================================

@router.get("/f/{token}", response_class=HTMLResponse, summary="Feedback form")
async def feedback_page(token: str):
    """
    Renders a simple rating + comment form for email feedback.
    Users access this via a link in their email with a signed token.
    """
    try:
        # Parse and verify token
        data = parse_feedback_token(token)
        email = data.get("e", "unknown")
        
        log.info("feedback_page: token parsed for email=%s", email)
        
        # Render simple HTML form
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>ARI Brief Feedback</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                    text-align: center;
                    padding: 40px 20px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    margin: 0;
                }}
                .container {{
                    background: white;
                    border-radius: 12px;
                    padding: 40px;
                    max-width: 500px;
                    margin: 0 auto;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.1);
                }}
                h3 {{
                    color: #333;
                    margin-bottom: 30px;
                }}
                .stars {{
                    font-size: 40px;
                    margin: 20px 0;
                    display: flex;
                    justify-content: center;
                    gap: 5px;
                }}
                .star-wrapper {{
                    display: inline-block;
                }}
                .stars label {{
                    cursor: pointer;
                    display: inline-block;
                    transition: all 0.2s;
                    filter: grayscale(100%);
                    opacity: 0.3;
                }}
                .stars label.selected {{
                    filter: grayscale(0%);
                    opacity: 1;
                }}
                .stars label.hovering {{
                    filter: grayscale(0%);
                    opacity: 1;
                    transform: scale(1.2);
                }}
                .stars input {{
                    display: none;
                }}
                textarea {{
                    width: 100%;
                    padding: 12px;
                    border: 2px solid #e0e0e0;
                    border-radius: 8px;
                    font-family: inherit;
                    font-size: 14px;
                    resize: vertical;
                    box-sizing: border-box;
                }}
                textarea.error {{
                    border-color: #dc3545;
                }}
                .error-message {{
                    color: #dc3545;
                    font-size: 13px;
                    margin-top: 5px;
                    display: none;
                }}
                .rating-error {{
                    color: #dc3545;
                    font-size: 13px;
                    margin-top: 10px;
                    display: none;
                }}
                button {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border: none;
                    padding: 12px 40px;
                    border-radius: 8px;
                    font-size: 16px;
                    font-weight: 600;
                    cursor: pointer;
                    margin-top: 20px;
                    transition: transform 0.2s;
                }}
                button:hover {{
                    transform: translateY(-2px);
                }}
            </style>
            <script>
                document.addEventListener('DOMContentLoaded', function() {{
                    const starInputs = document.querySelectorAll('.stars input[type="radio"]');
                    const starLabels = document.querySelectorAll('.stars label');
                    const form = document.querySelector('form');
                    const textarea = document.querySelector('textarea');
                    const commentError = document.querySelector('.error-message');
                    const ratingError = document.querySelector('.rating-error');
                    
                    // Function to update star highlighting (for selection)
                    function updateStars(selectedValue) {{
                        starLabels.forEach((label, index) => {{
                            if (index < selectedValue) {{
                                label.classList.add('selected');
                            }} else {{
                                label.classList.remove('selected');
                            }}
                        }});
                    }}
                    
                    // Function to reset all stars to default (unselected) state
                    function resetStars() {{
                        starLabels.forEach(label => {{
                            label.classList.remove('selected', 'hovering');
                            label.style.filter = '';
                            label.style.opacity = '';
                            label.style.transform = '';
                        }});
                    }}
                    
                    // Add change event to each star
                    starInputs.forEach((input, index) => {{
                        input.addEventListener('change', function() {{
                            if (this.checked) {{
                                const value = parseInt(this.value);
                                resetStars();  // Clear any hover effects first
                                updateStars(value);
                                // Hide rating error when star is selected
                                ratingError.style.display = 'none';
                            }}
                        }});
                        
                        // Add click event to labels for better UX
                        starLabels[index].addEventListener('click', function() {{
                            starInputs[index].checked = true;
                            const value = parseInt(starInputs[index].value);
                            resetStars();  // Clear any hover effects first
                            updateStars(value);
                            ratingError.style.display = 'none';
                        }});
                    }});
                    
                    // Hover effect for stars
                    starLabels.forEach((label, index) => {{
                        label.addEventListener('mouseenter', function() {{
                            // Remove 'selected' class and add 'hovering' class for all stars up to and including hovered
                            starLabels.forEach((lbl, idx) => {{
                                if (idx <= index) {{
                                    lbl.classList.add('hovering');
                                }} else {{
                                    lbl.classList.remove('hovering');
                                }}
                            }});
                        }});
                    }});
                    
                    // Reset to selected state on mouse leave
                    document.querySelector('.stars').addEventListener('mouseleave', function() {{
                        // Remove all hover effects
                        starLabels.forEach(label => {{
                            label.classList.remove('hovering');
                        }});
                        
                        // Restore selected state if any star is checked
                        const checkedInput = document.querySelector('.stars input[type="radio"]:checked');
                        if (checkedInput) {{
                            updateStars(parseInt(checkedInput.value));
                        }} else {{
                            // No selection - ensure all stars are in default state
                            resetStars();
                        }}
                    }});
                    
                    // Form validation
                    form.addEventListener('submit', function(e) {{
                        let hasError = false;
                        
                        // Check if rating is selected
                        const ratingSelected = document.querySelector('.stars input[type="radio"]:checked');
                        if (!ratingSelected) {{
                            e.preventDefault();
                            ratingError.style.display = 'block';
                            ratingError.textContent = 'Please select a rating before feedback submission';
                            hasError = true;
                        }}
                        
                        // Check comment word count
                        const comment = textarea.value.trim();
                        const wordCount = comment.split(/\\s+/).filter(w => w.length > 0).length;
                        
                        if (wordCount < 5) {{
                            e.preventDefault();
                            textarea.classList.add('error');
                            commentError.style.display = 'block';
                            commentError.textContent = wordCount === 0 
                                ? 'Please provide feedback (minimum 5 words)' 
                                : `Please add at least ${{5 - wordCount}} more word(s)`;
                            hasError = true;
                        }}
                        
                        if (hasError) {{
                            return false;
                        }}
                    }});
                    
                    // Remove comment error on typing
                    textarea.addEventListener('input', function() {{
                        textarea.classList.remove('error');
                        commentError.style.display = 'none';
                    }});
                }});
            </script>
        </head>
        <body>
            <div class="container">
                <h3>📊 How useful was this email?</h3>
                <form method="POST" action="/admin/email/f/{token}/submit">
                    <div class="stars">
                        {''.join(f'<div class="star-wrapper"><input type="radio" name="stars" value="{i}" id="star{i}"><label for="star{i}">⭐</label></div>' for i in range(1, 6))}
                    </div>
                    <div class="rating-error"></div>
                    <p>
                        <textarea name="comment" rows="4" placeholder="Your feedback helps us improve! (minimum 5 words)"></textarea>
                        <div class="error-message"></div>
                    </p>
                    <button type="submit">Submit Feedback</button>
                </form>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(content=html, status_code=200)
        
    except BadSignature:
        log.warning("feedback_page: invalid or expired token")
        return HTMLResponse(
            content="<html><body style='font-family:sans-serif;text-align:center;padding:40px'><h3>Invalid or expired feedback link</h3></body></html>",
            status_code=400
        )
    except Exception as e:
        log.exception("feedback_page: failed")
        raise HTTPException(status_code=500, detail="Failed to load feedback form")


@router.post("/f/{token}/submit", response_class=HTMLResponse, summary="Submit feedback")
async def feedback_submit(token: str, stars: int = Form(...), comment: str = Form("")):
    """
    Stores rating + comment into email_events table as a single row.
    """
    try:
        # Parse and verify token
        data = parse_feedback_token(token)
        email = data.get("e", "unknown")
        email_log_id = data.get("log")
        email_sent_at = data.get("sent")  # ISO timestamp when email was sent
        
        log.info("feedback_submit: email=%s stars=%d comment_length=%d sent_at=%s", 
                 email, stars, len(comment.strip()), email_sent_at)
        
        # Get database path
        db_path = getattr(settings, "SQLITE_PATH", None) or CACHE_DB_PATH
        
        # Store feedback in email_events table (single row with rating AND comment)
        async with aiosqlite.connect(db_path) as db:
            # Insert single feedback event with rating and comment
            await db.execute(
                """
                INSERT INTO email_events 
                (email, event_type, rating, comment, item_url_hash, email_sent_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    email, 
                    "feedback",  # Single event type for all feedback
                    stars, 
                    comment.strip()[:500] if comment.strip() else None,  # Store comment directly
                    str(email_log_id) if email_log_id else None,
                    email_sent_at
                )
            )
            
            await db.commit()
        
        log.info("feedback_submit: stored feedback for email=%s rating=%d", email, stars)
        
        # Return thank you page
        return HTMLResponse(
            content="""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>Thank You!</title>
                <style>
                    body {
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                        text-align: center;
                        padding: 40px 20px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        min-height: 100vh;
                        margin: 0;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                    }
                    .container {
                        background: white;
                        border-radius: 12px;
                        padding: 60px 40px;
                        max-width: 500px;
                        box-shadow: 0 10px 40px rgba(0,0,0,0.1);
                    }
                    h3 {
                        color: #333;
                        font-size: 28px;
                        margin: 20px 0;
                    }
                    .emoji {
                        font-size: 64px;
                        margin-bottom: 20px;
                    }
                    p {
                        color: #666;
                        font-size: 16px;
                    }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="emoji">💌</div>
                    <h3>Thanks for your feedback!</h3>
                    <p>Your input helps us make ARI Brief better for everyone.</p>
                </div>
            </body>
            </html>
            """,
            status_code=200
        )
        
    except BadSignature:
        log.warning("feedback_submit: invalid or expired token")
        return HTMLResponse(
            content="<html><body style='font-family:sans-serif;text-align:center;padding:40px'><h3>Invalid or expired feedback link</h3></body></html>",
            status_code=400
        )
    except Exception as e:
        log.exception("feedback_submit: failed")
        raise HTTPException(status_code=500, detail="Failed to submit feedback")
