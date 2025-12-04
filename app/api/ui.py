from __future__ import annotations
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.db.pg import pg_fetch_all

log = logging.getLogger("ari.ui")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


# Email validation regex
EMAIL_RX = re.compile(r".+@.+\..+")


def _valid_email(s: str) -> bool:
    """Simple email validation using regex."""
    return bool(EMAIL_RX.match(s or ""))


async def _get_active_tickers():
    """
    Load all tickers from Postgres 'tickers' table.
    Return list of dicts: { "ticker": "...", "name": "..."}
    """
    rows = await pg_fetch_all(
        "SELECT ticker, name FROM tickers ORDER BY name;"
    )
    return [{"ticker": r["ticker"], "name": r["name"]} for r in rows]


async def _load_user_tickers(email: str) -> list[dict]:
    """Load current user_tickers for given email from Postgres, ordered by rank."""
    try:
        rows = await pg_fetch_all(
            f"SELECT ticker, company_name, rank FROM user_tickers WHERE email='{email}' ORDER BY rank"
        )
        return [{"ticker": r["ticker"], "company_name": r["company_name"], "rank": r["rank"]} for r in rows]
    except Exception:
        log.exception("_load_user_tickers: failed for email=%s", email)
        return []


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Simple landing page with link to dashboard."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_get(request: Request, email: Optional[str] = None):
    """
    Render dashboard form with 7 ticker slots.
    If email query param provided, pre-populate with existing selections.
    """
    ticker_list = await _get_active_tickers()
    tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
    
    selected = {}
    email = ""
    
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "tickers": tickers,
            "email": email or "",
            "selected": selected,
            "error": None,
        }
    )


@router.post("/dashboard", response_class=HTMLResponse)
async def dashboard_post(
    request: Request,
    email: str = Form(...),
    slot1_ticker: str = Form(""),
    slot2_ticker: str = Form(""),
    slot3_ticker: str = Form(""),
    slot4_ticker: str = Form(""),
    slot5_ticker: str = Form(""),
    slot6_ticker: str = Form(""),
    slot7_ticker: str = Form(""),
    slot1_clear: Optional[str] = Form(None),
    slot2_clear: Optional[str] = Form(None),
    slot3_clear: Optional[str] = Form(None),
    slot4_clear: Optional[str] = Form(None),
    slot5_clear: Optional[str] = Form(None),
    slot6_clear: Optional[str] = Form(None),
    slot7_clear: Optional[str] = Form(None),
):
    """
    Handle dashboard form submission.
    Validate and save user ticker selections.
    """
    
    # Validate email with helper
    if not _valid_email(email):
        ticker_list = await _get_active_tickers()
        tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "tickers": tickers,
                "email": email,
                "selected": {},
                "error": "Invalid email address.",
            }
        )
    
    # Collect selected tickers (ignore cleared slots)
    slot_tickers = [
        (1, slot1_ticker if not slot1_clear else ""),
        (2, slot2_ticker if not slot2_clear else ""),
        (3, slot3_ticker if not slot3_clear else ""),
        (4, slot4_ticker if not slot4_clear else ""),
        (5, slot5_ticker if not slot5_clear else ""),
        (6, slot6_ticker if not slot6_clear else ""),
        (7, slot7_ticker if not slot7_clear else ""),
    ]
    
    # Filter out empty slots and validate uniqueness
    selected = [(rank, ticker.strip()) for rank, ticker in slot_tickers if ticker.strip()]
    
    # Debug: log raw selections
    log.info("dashboard: email=%s raw selections=%s", email, selected)

    if len(selected) < 3:
        ticker_list = await _get_active_tickers()
        tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
        selected_dict = {rank: ticker for rank, ticker in slot_tickers if ticker.strip()}
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "tickers": tickers,
                "email": email,
                "selected": selected_dict,
                "error": "Please pick at least 3 tickers.",
            }
        )
    
    unique_tickers = set(ticker for _, ticker in selected)
    
    if len(selected) != len(unique_tickers):
        ticker_list = await _get_active_tickers()
        tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
        selected_dict = {rank: ticker for rank, ticker in slot_tickers if ticker.strip()}
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "tickers": tickers,
                "email": email,
                "selected": selected_dict,
                "error": "Duplicate tickers selected. Please choose unique tickers (max 7).",
            }
        )
    
    if len(selected) > 7:
        ticker_list = await _get_active_tickers()
        tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
        selected_dict = {rank: ticker for rank, ticker in slot_tickers if ticker.strip()}
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "tickers": tickers,
                "email": email,
                "selected": selected_dict,
                "error": "Maximum 7 tickers allowed.",
            }
        )
    
    # Save to database (Postgres)
    try:
        from app.db.pg import engine
        from sqlalchemy import text
        
        # Check if user already exists
        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT 1 FROM users WHERE email = :email"),
                {"email": email}
            )
            exists = result.fetchone()
            
            if exists:
                ticker_list = await _get_active_tickers()
                tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
                return templates.TemplateResponse(
                    "dashboard.html",
                    {
                        "request": request,
                        "tickers": tickers,
                        "email": email,
                        "selected": {},
                        "error": "This email has already been registered. Editing is disabled for the pilot.",
                    }
                )
            
            # Upsert user
            from datetime import datetime, timezone

            now_dt = datetime.now(timezone.utc).isoformat()
            await conn.execute(
                text("INSERT INTO users(email, created_at) VALUES (:email, :created_at) ON CONFLICT(email) DO NOTHING"),
                {"email": email, "created_at": now_dt}
            )
            # Clear existing user_tickers
            await conn.execute(
                text("DELETE FROM user_tickers WHERE email = :email"),
                {"email": email}
            )
            
            # Insert new selections
            saved_tickers = []
            for rank, ticker in selected:
                # Lookup from tickers table
                result = await conn.execute(
                    text("SELECT name, aliases FROM tickers WHERE ticker = :ticker"),
                    {"ticker": ticker}
                )
                row = result.fetchone()
                
                if not row:
                    log.warning("dashboard: ticker=%s not found in tickers table for email=%s", ticker, email)
                    continue
                
                company_name = row[0]
                aliases_json = row[1] if len(row) > 1 else ""
                
                await conn.execute(
                    text("""
                        INSERT INTO user_tickers(email, ticker, company_name, aliases_json, rank, created_at)
                        VALUES (:email, :ticker, :company_name, :aliases_json, :rank, :created_at)
                    """),
                    {
                        "email": email,
                        "ticker": ticker,
                        "company_name": company_name,
                        "aliases_json": aliases_json,
                        "rank": rank,
                        "created_at": now_dt
                    }
                )
                saved_tickers.append({"rank": rank, "ticker": ticker, "company_name": company_name})
            
            log.info("dashboard: upserted %d tickers for email=%s", len(saved_tickers), email)
            
            # Render success page
            return templates.TemplateResponse(
                "dashboard_success.html",
                {
                    "request": request,
                    "email": email,
                    "saved_tickers": saved_tickers,
                }
            )
            
    except Exception:
        log.exception("dashboard: save failed for email=%s", email)
        ticker_list = await _get_active_tickers()
        tickers = [(t["ticker"], f"{t['ticker']} - {t['name']}") for t in ticker_list]
        selected_dict = {rank: ticker for rank, ticker in slot_tickers if ticker.strip()}
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "tickers": tickers,
                "email": email,
                "selected": selected_dict,
                "error": "Failed to save selections. Please try again.",
            }
        )