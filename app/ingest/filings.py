import os, httpx
from typing import List, Dict
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

EXCLUDE_CATEGORIES = {"ANNUAL REPORT", "QUARTERLY RESULTS", "EARNINGS CALL TRANSCRIPT", "TRANSCRIPT"}
from typing import List, Dict

EXCLUDE_CATEGORIES = {"ANNUAL REPORT", "QUARTERLY RESULTS", "EARNINGS CALL TRANSCRIPT", "TRANSCRIPT"}

def filter_for_summary(items: List[Dict]) -> List[Dict]:
    """
    Backward-compat layer: ensure each item has `summary_allowed`.
    If fetchers already set it, we keep it; otherwise compute from category/title.
    """
    out = []
    for it in items:
        if "summary_allowed" not in it:
            cat_or_title = (it.get("category") or it.get("title") or "").upper()
            it["summary_allowed"] = not any(key in cat_or_title for key in EXCLUDE_CATEGORIES)
        out.append(it)
    return out

def dedupe_by_url_or_title(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for it in items:
        key = (it.get("url") or "").strip().lower() or (it.get("title") or "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            out.append(it)
    return out
def _iso(ts: str) -> str:
    try:
        return datetime.fromisoformat(ts.replace("Z","")).isoformat() + "Z"
    except Exception:
        return datetime.utcnow().isoformat() + "Z"

def _should_exclude(cat: str) -> bool:
    cat = (cat or "").upper()
    return any(x in cat for x in EXCLUDE_CATEGORIES)

async def fetch_bse_announcements(ticker: str) -> List[Dict]:
    """Fetch latest filings for a given symbol from BSE RSS feed."""
    url = "https://www.bseindia.com/xml-data/corpfiling/CorpFiling.xml"
    items: List[Dict] = []
    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return items
            root = ET.fromstring(r.text)
            for entry in root.findall(".//Item"):
                company = entry.findtext("Company")
                headline = entry.findtext("Headline")
                attachment = entry.findtext("Attachment")
                date = entry.findtext("Date")
                if company and ticker.upper() not in company.upper():
                    continue
                items.append({
                    "title": headline or company or "",
                    "url": attachment or "",
                    "published_at": _iso(date or ""),
                    "source": "BSE",
                    "category": "ANNOUNCEMENT",
                    "summary_allowed": not _should_exclude(headline or ""),
                })
    except Exception as e:
        print("BSE error:", e)
    return items[:10]  # limit

async def fetch_nse_announcements(ticker: str) -> List[Dict]:
    """Fetch latest filings for a given symbol from NSE JSON feed."""
    url = "https://www.nseindia.com/api/corporate-announcements?index=equities"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
        "Origin": "https://www.nseindia.com",
    }
    items: List[Dict] = []
    try:
        async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
            await client.get("https://www.nseindia.com/")  # warm cookies
            r = await client.get(url)
            if r.status_code != 200:
                return items
            data = r.json() or {}
            for row in data.get("data", []):
                sym = (row.get("symbol") or "").upper()
                if sym != ticker.upper():
                    continue
                title = (row.get("sm_desc") or row.get("headline") or "").strip()
                link  = (row.get("pdfUrl") or row.get("attachement") or "").strip()
                cat   = (row.get("category") or "").strip().upper()
                dt    = _iso(row.get("date") or row.get("dissemDate") or "")
                items.append({
                    "title": title,
                    "url": link,
                    "published_at": dt,
                    "source": "NSE",
                    "category": cat or "ANNOUNCEMENT",
                    "summary_allowed": not _should_exclude(cat),
                })
    except Exception as e:
        print("NSE error:", e)
    return items[:10]