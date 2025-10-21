from __future__ import annotations
from typing import TypedDict, List, Optional, Dict, Protocol
from urllib.parse import urlparse


class NewsItem(TypedDict, total=False):
    title: str
    url: str
    source: str
    published_at: str
    lang: str  # prefer 'en' if missing
    content: str  # optional article body


class Adapter(Protocol):
    async def fetch(
        self,
        ticker: str,
        *,
        days: int,
        topk: int,
        timeout_s: int,
    ) -> List[NewsItem]:
        """
        Fetch recent news items for the given ticker.

        Returns a list of NewsItem dicts. Implementations should not raise for
        expected network/provider errors (they may return an empty list).
        """
        ...


def normalize_item(raw: Dict) -> NewsItem:
    """
    Normalize a raw provider item to NewsItem:
    - trim strings
    - ensure lang is present (default 'en')
    - ensure required keys exist (empty string fallback)
    """
    def _trim(v: Optional[object]) -> str:
        if v is None:
            return ""
        return str(v).strip()

    item: NewsItem = {
        "title": _trim(raw.get("title") or raw.get("headline") or ""),
        "url": _trim(raw.get("url") or raw.get("link") or ""),
        "source": _trim(raw.get("source") or raw.get("publisher") or ""),
        "published_at": _trim(raw.get("published_at") or raw.get("publishedAt") or ""),
        "lang": _trim(raw.get("lang") or "en") or "en",
    }
    content = raw.get("content") or raw.get("summary") or raw.get("description") or ""
    if content is not None:
        item["content"] = _trim(content)
    return item


def domain_from_url(url: str) -> str:
    """
    Return the hostname portion of a URL lowercased (no port).
    """
    try:
        p = urlparse((url or "").strip())
        host = (p.hostname or "").lower()
        return host
    except Exception:
        return ""
        

__all__ = ["NewsItem", "Adapter", "normalize_item", "domain_from_url"]