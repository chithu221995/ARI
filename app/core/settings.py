from __future__ import annotations
import os
from typing import List

def _b(s: str, default=False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "yes", "y", "on"}

def _i(s: str, default: int) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default

def _split_csv(s: str) -> List[str]:
    return [p.strip() for p in (s or "").split(",") if p.strip()]

# ---- News knobs (Phase 2) ----
NEWS_DAYS        = _i(os.getenv("NEWS_DAYS", "7"), 7)                     # lookback window (days)
NEWS_TOPK        = _i(os.getenv("NEWS_TOPK", "5"), 5)                     # keep top-K per ticker
NEWS_TIMEOUT_S   = _i(os.getenv("NEWS_TIMEOUT_S", "8"), 8)                # per-source http timeout (seconds)
DEBUG_NEWS_LOG   = _b(os.getenv("DEBUG_NEWS_LOG", "1"), True)             # verbose adapter/filter logs

# Which sources to query, in order (we’ll implement newscatcher + bing; keep newsapi as baseline)
NEWS_SOURCES     = _split_csv(os.getenv("NEWS_SOURCES", "newscatcher,bing,newsapi"))

# Language + domain rules
NEWS_LANGUAGE    = os.getenv("NEWS_LANGUAGE", "en")

ALLOWLIST_DOMAINS = _split_csv(os.getenv(
    "ALLOWLIST_DOMAINS",
    "economictimes.indiatimes.com,livemint.com,thehindubusinessline.com,"
    "moneycontrol.com,bqprime.com,ndtvprofit.com,business-standard.com,"
    "financialexpress.com,thehindu.com"
))

BLOCKLIST_DOMAINS = _split_csv(os.getenv(
    "BLOCKLIST_DOMAINS",
    "globenewswire.com,prnewswire.com"
))

# Drop if title contains these (unless hard-matched to company/aliases)
BLOCKLIST_KEYWORDS = _split_csv(os.getenv(
    "BLOCKLIST_KEYWORDS",
    "marathon,road closures,waterfront marathon"
))

# Safety limits
MAX_ITEMS_PER_SOURCE = _i(os.getenv("MAX_ITEMS_PER_SOURCE", "25"), 25)

# API keys for adapters
NEWSCATCHER_API_KEY: str = os.getenv("NEWSCATCHER_API_KEY", "")
BING_NEWS_KEY: str = os.getenv("BING_NEWS_KEY", "")

# Export a simple “show” for quick debug
def as_dict() -> dict:
    return {
        "NEWS_DAYS": NEWS_DAYS,
        "NEWS_TOPK": NEWS_TOPK,
        "NEWS_TIMEOUT_S": NEWS_TIMEOUT_S,
        "DEBUG_NEWS_LOG": DEBUG_NEWS_LOG,
        "NEWS_SOURCES": NEWS_SOURCES,
        "NEWS_LANGUAGE": NEWS_LANGUAGE,
        "ALLOWLIST_DOMAINS": ALLOWLIST_DOMAINS,
        "BLOCKLIST_DOMAINS": BLOCKLIST_DOMAINS,
        "BLOCKLIST_KEYWORDS": BLOCKLIST_KEYWORDS,
        "MAX_ITEMS_PER_SOURCE": MAX_ITEMS_PER_SOURCE,
        "NEWSCATCHER_API_KEY": NEWSCATCHER_API_KEY,
        "BING_NEWS_KEY": BING_NEWS_KEY,
    }