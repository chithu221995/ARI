import os
from typing import List, Dict
import httpx
from datetime import datetime, timedelta

EXCLUDE_CATEGORIES = {"ANNUAL REPORT", "QUARTERLY RESULTS", "EARNINGS CALL TRANSCRIPT", "TRANSCRIPT"}

def _iso(ts: str) -> str:
    try:
        return datetime.fromisoformat(ts.replace("Z","")).isoformat() + "Z"
    except Exception:
        return datetime.utcnow().isoformat() + "Z"

def _use_mock() -> bool:
    return (os.getenv("USE_MOCK_FILINGS", "false").lower() in {"1","true","yes"})

async def fetch_bse_announcements(ticker_or_bse_code: str) -> List[Dict]:
    """Real BSE fetch with safe headers; returns mock if USE_MOCK_FILINGS=true."""
    if _use_mock():
        return [{
            "title": f"Sample BSE announcement for {ticker_or_bse_code}",
            "url": "https://www.bseindia.com/",
            "published_at": "2025-10-14T10:00:00Z",
            "source": "BSE",
            "category": "GENERAL",
            "summary_allowed": True,
        }]

    date_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate={date_str}&strScrip={ticker_or_bse_code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.bseindia.com/",
        "Origin": "https://www.bseindia.com",
    }
    items: List[Dict] = []
    async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
        # retry up to 3 times
        for _ in range(3):
            r = await client.get(url)
            if r.status_code == 200:
                data = r.json() if "application/json" in r.headers.get("content-type","") else []
                for row in data or []:
                    title = (row.get("HEADLINE") or row.get("ATTACHMENTNAME") or "").strip()
                    link  = (row.get("ATTACHMENT") or row.get("MORE") or row.get("FilePath") or "").strip()
                    cat   = (row.get("CATEGORY") or "").strip().upper()
                    dt    = _iso(row.get("NEWS_DT") or row.get("DT_TM") or "")
                    items.append({
                        "title": title,
                        "url": link,
                        "published_at": dt,
                        "source": "BSE",
                        "category": cat or "ANNOUNCEMENT",
                    })
                break
        return items

async def fetch_nse_announcements(nse_symbol: str) -> List[Dict]:
    """Real NSE fetch with cookie warm-up + headers; returns mock if USE_MOCK_FILINGS=true."""
    if _use_mock():
        return [{
            "title": f"Sample NSE announcement for {nse_symbol}",
            "url": "https://www.nseindia.com/",
            "published_at": "2025-10-14T11:00:00Z",
            "source": "NSE",
            "category": "CORPORATE ANNOUNCEMENT",
            "summary_allowed": True,
        }]

    base_api = "https://www.nseindia.com/api/corporate-announcements?index=equities"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nseindia.com/",
        "Origin": "https://www.nseindia.com",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    results: List[Dict] = []
    async with httpx.AsyncClient(timeout=20.0, headers=headers, follow_redirects=True) as client:
        # 1) warm up cookies by hitting homepage (prevents 403)
        await client.get("https://www.nseindia.com/")

        # 2) call API with up to 3 retries
        for _ in range(3):
            r = await client.get(base_api)
            if r.status_code == 200:
                data = r.json() or {}
                for row in data.get("data", []):
                    sym = (row.get("symbol") or "").upper()
                    if sym != nse_symbol.upper():
                        continue
                    title = (row.get("sm_desc") or row.get("headline") or "").strip()
                    link  = (row.get("pdfUrl") or row.get("attachement") or "").strip()
                    cat   = (row.get("category") or "").strip().upper()
                    dt    = _iso(row.get("date") or row.get("dissemDate") or "")
                    results.append({
                        "title": title,
                        "url": link,
                        "published_at": dt,
                        "source": "NSE",
                        "category": cat or "ANNOUNCEMENT",
                    })
                break
        return results

def filter_for_summary(items: List[Dict]) -> List[Dict]:
    cleaned = []
    for it in items:
        cat = (it.get("category") or "").upper()
        it["summary_allowed"] = not any(key in cat for key in EXCLUDE_CATEGORIES)
        cleaned.append(it)
    return cleaned

def dedupe_by_url_or_title(items: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for it in items:
        key = (it.get("url") or "").strip().lower() or (it.get("title") or "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            out.append(it)
    return out