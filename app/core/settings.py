from __future__ import annotations
import encodings
import os
from typing import List
import os
from dotenv import load_dotenv, find_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import json
import re
from typing import List, Union

# Load .env early and override any preexisting/empty values
load_dotenv(
    os.getenv("ENV_FILE") or find_dotenv(usecwd=True) or ".env",
    override=True,
)

# ensure NEWS_TOPK default = 10
NEWS_TOPK = int(os.getenv("NEWS_TOPK", "10"))

class Settings(BaseSettings):
    # read .env with case-insensitive keys
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",   # ← add this
    )
    # LLM provider selection: "gemini" or "openai"
    LLM_PROVIDER: str = Field("openai", description="gemini|openai")

    # Default news sources
    # Accept either JSON array or comma/space-separated string from env
    NEWS_SOURCES: List[str] = Field(default_factory=lambda: ["google_rss"])

    @field_validator("NEWS_SOURCES", mode="before")
    @classmethod
    def _parse_news_sources(cls, v):
        # None -> default factory will apply
        if v is None:
            return None
        if isinstance(v, (list, tuple)):
            return list(v)
        s = str(v).strip()
        if not s:
            return None
        # try JSON array first
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if x]
        except Exception:
            pass
        # fallback: split on commas/spaces
        parts = [p.strip() for p in re.split(r"[,\s]+", s) if p.strip()]
        return parts or None

    # Optional provider-specific settings (kept here for convenience)
    GEMINI_API_KEY: str | None = Field(None)
    GEMINI_MODEL: str = Field("gemini-2.5-pro")
    OPENAI_MODEL: str = Field("gpt-4o-mini")

# module-level settings instance

settings = Settings()

import logging
logging.getLogger("ari.settings").info(
    "ari.settings: LLM_PROVIDER=%s NEWS_SOURCES=%s SUMMARY_MAX_TOKENS=%s SUMMARY_TEMPERATURE=%s",
    getattr(settings, "LLM_PROVIDER", None),
    getattr(settings, "NEWS_SOURCES", None),
    getattr(settings, "SUMMARY_MAX_TOKENS", None),
    getattr(settings, "SUMMARY_TEMPERATURE", None),
)

# Back-compat: expose validated NEWS_SOURCES at module level for callers importing the module
NEWS_SOURCES = settings.NEWS_SOURCES

def _b(s: str, default=False) -> bool:
    if s is None:
        return default
    return s.strip().lower() in {"1", "true", "yes", "y", "on"}

def _i(s: str, default: int) -> int:
    try:
        return int(str(s).strip())
    except Exception:
        return default

def _split_csv(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split(",") if x.strip()]

def _as_int(val, default):
    try:
        return int(val)
    except Exception:
        return default

# ---- News knobs (Phase 2) ----
NEWS_DAYS        = _i(os.getenv("NEWS_DAYS", "7"), 7)                     # lookback window (days)
NEWS_TOPK        = _i(os.getenv("NEWS_TOPK", "10"), 10)                   # keep top-K per ticker (default 10)
NEWS_TIMEOUT_S   = _i(os.getenv("NEWS_TIMEOUT_S", "8"), 8)                # per-source http timeout (seconds)
DEBUG_NEWS_LOG   = _b(os.getenv("DEBUG_NEWS_LOG", "1"), True)             # verbose adapter/filter logs

# Which sources to query: use validated settings.NEWS_SOURCES (module-level env-derived CSV removed)
# (module-level NEWS_SOURCES removed to avoid double-parsing by pydantic-settings)

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

# Hard block phrases (case-insensitive) — extra relevance controls
HARD_BLOCK_KEYWORDS = [p.lower() for p in _split_csv(os.getenv(
    "HARD_BLOCK_KEYWORDS",
    "stocks to buy,outlook for the week,outlook for the day,call options"
))]

# Safety limits
MAX_ITEMS_PER_SOURCE = _i(os.getenv("MAX_ITEMS_PER_SOURCE", "25"), 25)

# API keys for adapters
NEWSCATCHER_API_KEY: str = os.getenv("NEWSCATCHER_API_KEY", "")
BING_NEWS_KEY: str = os.getenv("BING_NEWS_KEY", "")
SCRAPINGDOG_API_KEY: str = os.getenv("SCRAPINGDOG_API_KEY", "")
DIFFBOT_TOKEN: str = os.getenv("DIFFBOT_TOKEN", "")

# freshness window (hours)
FRESH_WINDOW_HOURS = int(os.getenv("FRESH_WINDOW_HOURS", "24"))

# schedule tickers (comma-separated env, default to "TCS")
SCHEDULE_TICKERS = _split_csv(os.getenv("SCHEDULE_TICKERS", "TCS"))

# LLM summary knobs
SUMMARY_MAX_TOKENS = int(os.getenv("SUMMARY_MAX_TOKENS", "900"))   # try 900; can raise to 1200–1500
SUMMARY_TEMPERATURE = float(os.getenv("SUMMARY_TEMPERATURE", "0.2"))

# Email / delivery settings
EMAIL_PROVIDER: str | None = None        # 'sendgrid' | 'smtp'
EMAIL_FROM: str | None = None
EMAIL_TO: str | None = None
EMAIL_CC: str | None = None
SENDGRID_API_KEY: str | None = None
SMTP_HOST: str | None = None
SMTP_PORT: int | None = None
SMTP_USER: str | None = None
SMTP_PASS: str | None = None
# control flags
# scheduler / summarizer control
SUMMARY_DRY_RUN: bool = True

# ensure pydantic-settings reads the env file (idempotent)
# Export a simple "show" for quick debug
def as_dict() -> dict:
    # return a plain dict snapshot of current module-level settings
    return {
        "NEWS_DAYS": NEWS_DAYS,
        "NEWS_TOPK": NEWS_TOPK,
        "NEWS_TIMEOUT_S": NEWS_TIMEOUT_S,
        "DEBUG_NEWS_LOG": DEBUG_NEWS_LOG,
        "NEWS_SOURCES": settings.NEWS_SOURCES,
        "NEWS_LANGUAGE": NEWS_LANGUAGE,
        "ALLOWLIST_DOMAINS": ALLOWLIST_DOMAINS,
        "BLOCKLIST_DOMAINS": BLOCKLIST_DOMAINS,
        "BLOCKLIST_KEYWORDS": BLOCKLIST_KEYWORDS,
        "HARD_BLOCK_KEYWORDS": HARD_BLOCK_KEYWORDS,
        "MAX_ITEMS_PER_SOURCE": MAX_ITEMS_PER_SOURCE,
        "FRESH_WINDOW_HOURS": FRESH_WINDOW_HOURS,
        "SCHEDULE_TICKERS": SCHEDULE_TICKERS,
        "SCRAPINGDOG_API_KEY": SCRAPINGDOG_API_KEY,
        "DIFFBOT_TOKEN": DIFFBOT_TOKEN,
        "SUMMARY_MAX_TOKENS": SUMMARY_MAX_TOKENS,
        "SUMMARY_TEMPERATURE": SUMMARY_TEMPERATURE,
    }
