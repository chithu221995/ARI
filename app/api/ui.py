from __future__ import annotations
import os
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import aiosqlite

log = logging.getLogger("ari.ui")
router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


# Email validation regex
EMAIL_RX = re.compile(r".+@.+\..+")


def _valid_email(s: str) -> bool:
    """Simple email validation using regex."""
    return bool(EMAIL_RX.match(s or ""))


def _utc_now_iso() -> str:
    """Return current UTC timestamp as ISO string without microseconds."""
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


async def _get_active_tickers(db_path: str) -> list[tuple[str, str]]:
    """
    Load active tickers from ticker_catalog.
    Returns list of (ticker, "TICKER — Company Name") tuples sorted by company_name.
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                "SELECT ticker, company_name FROM ticker_catalog WHERE active=1 ORDER BY company_name"
            )
            rows = await cur.fetchall()
            await cur.close()
            return [(row[0], f"{row[0]} — {row[1]}") for row in rows]
    except Exception:
        log.exception("_get_active_tickers: failed")
        return []


async def _load_user_tickers(db_path: str, email: str) -> list[dict]:
    """Load current user_tickers for given email, ordered by rank."""
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute(
                "SELECT ticker, company_name, rank FROM user_tickers WHERE email=? ORDER BY rank",
                (email,)
            )
            rows = await cur.fetchall()
            await cur.close()
            return [{"ticker": r[0], "company_name": r[1], "rank": r[2]} for r in rows]
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
    db_path = os.getenv("SQLITE_PATH", "./ari.db")
    tickers = await _get_active_tickers(db_path)
    
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
    db_path = os.getenv("SQLITE_PATH", "./ari.db")
    
    # Validate email with helper
    if not _valid_email(email):
        tickers = await _get_active_tickers(db_path)
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
        tickers = await _get_active_tickers(db_path)
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
        tickers = await _get_active_tickers(db_path)
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
        tickers = await _get_active_tickers(db_path)
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
    
    # Save to database
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute("PRAGMA foreign_keys=ON;")
            
            # Prevent editing for existing users (pilot restriction)
            cur = await db.execute("SELECT 1 FROM users WHERE email=?", (email,))
            exists = await cur.fetchone()
            await cur.close()
            if exists:
                tickers = await _get_active_tickers(db_path)
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
            now_iso = _utc_now_iso()
            await db.execute(
                "INSERT INTO users(email, created_at) VALUES (?, ?) ON CONFLICT(email) DO NOTHING",
                (email, now_iso)
            )
            
            # Clear existing user_tickers
            await db.execute("DELETE FROM user_tickers WHERE email=?", (email,))
            
            # Insert new selections with catalog lookup
            saved_tickers = []
            for rank, ticker in selected:
                # Lookup from catalog to get authoritative company_name and aliases
                cur = await db.execute(
                    "SELECT company_name, aliases_json FROM ticker_catalog WHERE ticker=? AND active=1",
                    (ticker,)
                )
                row = await cur.fetchone()
                await cur.close()
                
                if not row:
                    log.warning("dashboard: ticker=%s not found in catalog for email=%s", ticker, email)
                    continue
                
                company_name, aliases_json = row
                await db.execute(
                    """
                    INSERT INTO user_tickers(email, ticker, company_name, aliases_json, rank, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (email, ticker, company_name, aliases_json, rank, now_iso)
                )
                saved_tickers.append({"rank": rank, "ticker": ticker, "company_name": company_name})
            
            await db.commit()
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
        tickers = await _get_active_tickers(db_path)
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