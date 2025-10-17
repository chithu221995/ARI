import asyncio
from typing import List, Dict
from playwright.async_api import async_playwright
from app.ingest.tickers import resolve

async def _with_browser(run):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ))
        page = await ctx.new_page()
        try:
            return await run(page)
        finally:
            await ctx.close()
            await browser.close()

async def fetch_nse_announcements_browser(ticker: str) -> List[Dict]:
    meta = resolve(ticker)
    wanted = (meta["nse_symbol"] or ticker).upper()
    async def run(page):
        # Warm homepage to set cookies
        await page.goto("https://www.nseindia.com/", wait_until="domcontentloaded")
        # Fetch corporate announcements via browser context (NSE accepts this)
        await page.goto("https://www.nseindia.com/companies-listing/corporate-filings-announcements", wait_until="domcontentloaded")
        # The page calls the API; we can fetch it ourselves with cookies via evaluate
        js = """
        fetch("https://www.nseindia.com/api/corporate-announcements?index=equities",{
          headers: {
            "accept":"application/json,text/plain,*/*",
            "referer":"https://www.nseindia.com/",
          },
          credentials: "include"
        }).then(r=>r.json()).catch(()=>[]);
        """
        data = await page.evaluate(js)
        rows = data if isinstance(data, list) else data.get("data", [])
        out=[]
        for row in rows or []:
            sym = (row.get("symbol") or "").upper()
            if sym != wanted:
                continue
            out.append({
                "title": (row.get("sm_desc") or row.get("headline") or "").strip(),
                "url": (row.get("pdfUrl") or row.get("attachement") or "").strip(),
                "published_at": (row.get("date") or row.get("dissemDate") or ""),
                "source": "NSE",
                "category": (row.get("category") or "").strip().upper() or "ANNOUNCEMENT",
            })
        return out[:10]
    return await _with_browser(run)

async def fetch_bse_announcements_browser(ticker: str) -> List[Dict]:
    meta = resolve(ticker)
    aliases = [meta["company_name"]] + meta["aliases"]

    def _matches(company: str) -> bool:
        c = (company or "").upper()
        return any(a.upper() in c for a in aliases)

    async def run(page):
        # Use RSS page and parse links present in HTML (BSE blocks raw RSS often)
        await page.goto("https://www.bseindia.com/corporates/ann.aspx", wait_until="domcontentloaded")
        # pull the table rows text + links
        rows = await page.locator("table tr").all()
        out=[]
        for r in rows[:200]:
            txt = (await r.inner_text()).strip()
            hrefs = await r.locator("a").evaluate_all("els => els.map(e => e.href)")
            if not txt or not hrefs:
                continue
            # simple heuristic: keep rows that mention the company name
            if not _matches(txt):
                continue
            out.append({
                "title": txt[:180],
                "url": hrefs[0],
                "published_at": "",  # can be improved by parsing columns
                "source": "BSE",
                "category": "ANNOUNCEMENT",
            })
        return out[:10]
    return await _with_browser(run)