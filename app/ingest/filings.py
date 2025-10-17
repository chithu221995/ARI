# app/ingest/filings.py
from __future__ import annotations
from typing import List, Dict
from datetime import datetime
import httpx
import asyncio
from datetime import datetime, timedelta

from app.ingest.tickers import resolve
from app.ingest.filings_browser import (
    fetch_bse_announcements_browser,
    fetch_nse_announcements_browser,
)

BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
NSE_API_URL = "https://www.nseindia.com/api/corporate-announcements?symbol={symbol}"

BSE_HEADERS = {
    "Origin": "https://www.bseindia.com",
    "Referer": "https://www.bseindia.com/corporates/ann.aspx",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
    "Host": "api.bseindia.com",
}

NSE_HEADERS = {
    "referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "accept": "application/json, text/plain, */*",
    "connection": "keep-alive",
    "cache-control": "no-cache",
    "host": "www.nseindia.com",
}

EXCLUDE_KEYWORDS = ["ANNUAL REPORT", "QUARTERLY RESULTS", "TRANSCRIPT"]

def _iso(ts: str) -> str:
    """Safely convert any date string to ISO8601Z format."""
    if not ts:
        return ""
    try:
        # Try parsing common formats
        dt = None
        for fmt in ("%d %b %Y %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(ts, fmt)
                break
            except Exception:
                continue
        if not dt:
            dt = datetime.fromisoformat(ts)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ts

def _should_exclude(cat: str) -> bool:
    """Skip announcements containing certain keywords."""
    if not cat:
        return False
    cat_upper = cat.upper()
    return any(kw in cat_upper for kw in EXCLUDE_KEYWORDS)

def filter_for_summary(items: List[Dict]) -> List[Dict]:
    """Adds summary_allowed flag based on category exclusion."""
    for item in items:
        item["summary_allowed"] = not _should_exclude(item.get("category", ""))
    return items

def dedupe_by_url_or_title(items: List[Dict]) -> List[Dict]:
    """Removes duplicate announcements by url or title."""
    seen = set()
    result = []
    for item in items:
        key = (item.get("url") or "") + "|" + (item.get("title") or "")
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result

async def fetch_bse_announcements(ticker: str) -> List[Dict]:
    info = resolve(ticker)
    bse_code = info.get("bse_code", "")
    if not bse_code:
        print("BSE: missing bse_code for", ticker)
        return []
    payload = {
        "strCat": "Company",
        "strPrevDate": (datetime.utcnow() - timedelta(days=7)).strftime("%d-%m-%Y"),
        "strScrip": bse_code,
    }
    tries = 3
    rows = []
    async with httpx.AsyncClient(timeout=15) as client:
        for i in range(tries):
            # Warm-up GET before each POST
            await client.get("https://www.bseindia.com/corporates/ann.aspx", headers=BSE_HEADERS)
            try:
                resp = await client.post(BSE_API_URL, json=payload, headers=BSE_HEADERS)
                if resp.status_code in (301, 302, 403) or "error_Bse.html" in resp.text:
                    print(f"BSE blocked (try {i+1}/3) → retrying after 1s")
                    await asyncio.sleep(1)
                    continue
                resp.raise_for_status()
                data = resp.json()
                rows = data.get("Table", []) if isinstance(data, dict) else []
                break
            except Exception as e:
                if i == tries - 1:
                    print(f"BSE fetch error for {ticker}: {e}")
                    return []
                await asyncio.sleep(1)
        else:
            print("BSE blocked completely for", ticker)
            return []
    items = []
    for row in rows:
        title = row.get("SUBJECT") or row.get("HEADLINE") or ""
        url = row.get("ATTACHMENTURL") or row.get("DETAILSURL") or ""
        if url.startswith("//"):
            url = "https:" + url
        elif url and url.startswith("/"):
            url = "https://www.bseindia.com" + url
        elif url and not url.startswith("http"):
            url = "https://www.bseindia.com/" + url
        published_at = _iso(row.get("NEWS_DT") or row.get("DT_TM") or "")
        category = row.get("HEADLINE") or "ANNOUNCEMENT"
        items.append({
            "title": title.strip(),
            "url": url.strip(),
            "published_at": published_at,
            "source": "BSE",
            "category": category.strip(),
        })
    items = dedupe_by_url_or_title(filter_for_summary(items))
    items = sorted(items, key=lambda x: x.get("published_at", ""), reverse=True)[:10]
    print(f"BSE total={len(rows)} kept={len(items)} for {ticker} (code={bse_code})")
    return items

async def fetch_nse_announcements(ticker: str) -> List[Dict]:
    meta = resolve(ticker)
    wanted = meta.get("nse_symbol", "").upper()
    aliases = [meta.get("company_name", "")] + meta.get("aliases", [])
    tries = 3
    rows = []
    async with httpx.AsyncClient(timeout=20, headers=NSE_HEADERS, cookies=httpx.Cookies(), follow_redirects=True) as client:
        await client.get("https://www.nseindia.com/", headers=NSE_HEADERS)
        await asyncio.sleep(0.5)
        for i in range(tries):
            resp = await client.get(NSE_API_URL.format(symbol=wanted), headers=NSE_HEADERS)
            ctype = resp.headers.get("content-type", "")
            if resp.status_code != 200 or "text/html" in ctype:
                print(f"NSE API blocked or HTML for {ticker} (try {i+1}/3), status={resp.status_code}")
                await asyncio.sleep(1.5)
                continue
            try:
                data = resp.json()
            except Exception as e:
                print(f"NSE JSON error for {ticker}: {e}")
                return []
            if isinstance(data, dict):
                rows = data.get("data", [])
            elif isinstance(data, list):
                rows = data
            else:
                rows = []
            break
        else:
            print("NSE permanently blocked for", ticker)
            return []
    items = []
    for row in rows:
        symbol = row.get("symbol", "")
        company_name = row.get("companyName", "")
        if symbol:
            if not symbol.upper().startswith(wanted):
                continue
        elif company_name:
            if not any(alias.upper() in company_name.upper() for alias in aliases):
                continue
        title = row.get("sm_desc") or row.get("headline") or ""
        url = row.get("pdfUrl") or row.get("announcementUrl") or ""
        if url.startswith("//"):
            url = "https:" + url
        elif url and url.startswith("/"):
            url = "https://www.nseindia.com" + url
        elif url and not url.startswith("http"):
            url = "https://www.nseindia.com/" + url
        published_at = _iso(row.get("announcementDate") or row.get("date") or row.get("dissemDate") or "")
        category = row.get("category") or "ANNOUNCEMENT"
        items.append({
            "title": title.strip(),
            "url": url.strip(),
            "published_at": published_at,
            "source": "NSE",
            "category": category.strip(),
        })
    items = dedupe_by_url_or_title(filter_for_summary(items))
    items = sorted(items, key=lambda x: x.get("published_at", ""), reverse=True)[:10]
    print(f"NSE total={len(rows)} kept={len(items)} for {ticker} symbol={wanted}")
    return items

async def fetch_filings_with_browser(ticker: str) -> list:
    # Try API first
    nse = await fetch_nse_announcements(ticker)
    bse = await fetch_bse_announcements(ticker)
    filings = (nse or []) + (bse or [])
    if filings:
        return sorted(filings, key=lambda x: x.get("published_at", ""), reverse=True)[:5]
    # Fallback to Playwright scraping
    print(f"Browser fallback triggered for {ticker}")
    nse_browser = await fetch_nse_announcements_browser(ticker)
    bse_browser = await fetch_bse_announcements_browser(ticker)
    filings_browser = (nse_browser or []) + (bse_browser or [])
    return sorted(filings_browser, key=lambda x: x.get("published_at", ""), reverse=True)[:5]

def generate_mock_filings(ticker: str) -> list[dict]:
    now = datetime.utcnow()
    return [
        {
            "title": f"{ticker}: Board meeting outcome (mock)",
            "url": "https://example.com/bse.pdf",
            "published_at": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "BSE",
            "category": "BOARD MEETING",
            "summary_allowed": True,
        },
        {
            "title": f"{ticker}: Press release (mock)",
            "url": "https://example.com/nse.pdf",
            "published_at": (now - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "NSE",
            "category": "PRESS RELEASE",
            "summary_allowed": True,
        },
    ]