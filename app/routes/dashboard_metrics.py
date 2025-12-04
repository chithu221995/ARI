"""
Metrics dashboard route - displays KPI cards and charts.
"""
from __future__ import annotations
from typing import Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import logging

from app.db.pg import pg_fetch_all

log = logging.getLogger("ari.dashboard.metrics")

router = APIRouter(prefix="/dashboard", tags=["dashboard"])
templates = Jinja2Templates(directory="templates")


@router.get("/metrics", response_class=HTMLResponse, summary="KPI Metrics Dashboard")
async def show_metrics_dashboard(
    request: Request,
    start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """
    Render the KPI metrics dashboard with date pickers and summary cards.
    
    Shows:
    - Date range selector
    - KPI summary cards (delivery, relevance, freshness, etc.)
    - Time-series charts placeholders
    - Vendor performance tables
    """
    try:
        # Default date range: last 30 days
        today = datetime.utcnow().date()
        if not end:
            end = today.isoformat()
        if not start:
            start = (today - timedelta(days=30)).isoformat()
        
        log.info("dashboard.metrics: rendering dashboard for %s to %s", start, end)
        
        return templates.TemplateResponse(
            "metrics_dashboard.html",
            {
                "request": request,
                "start": start,
                "end": end,
                "title": "KPI Metrics Dashboard",
            }
        )
        
    except Exception as e:
        log.exception("dashboard.metrics: failed to render")
        return HTMLResponse(
            content=f"""
            <html>
            <head><title>Error</title></head>
            <body style="font-family: sans-serif; padding: 40px; text-align: center;">
                <h2>⚠️ Dashboard Error</h2>
                <p>Failed to load metrics dashboard: {str(e)}</p>
                <p><a href="/dashboard/metrics">Try again</a></p>
            </body>
            </html>
            """,
            status_code=500
        )


@router.get("/dashboard", response_class=HTMLResponse)
async def show_dashboard(request: Request):
    """
    Display user dashboard for managing ticker selections.
    """
    # Load ticker catalog from Postgres
    tickers = []
    try:
        rows = await pg_fetch_all("SELECT ticker, name FROM tickers ORDER BY ticker;")
        tickers = [(r["ticker"], f"{r['ticker']} - {r['name']}") for r in rows]
    except Exception as e:
        log.warning(f"Could not load tickers from Postgres: {e}")

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "tickers": tickers,
            "email": None,
            "selected": {}
        }
    )