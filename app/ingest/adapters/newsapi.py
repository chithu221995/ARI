from __future__ import annotations
import logging
from typing import List
from .base import NewsItem, normalize_item

log = logging.getLogger("ari.news")


async def fetch(ticker: str, *, days: int, topk: int, timeout_s: int) -> List[NewsItem]:
    """
    NewsAPI adapter (placeholder).
    TODO: implement real fetch using NewsAPI.org.
    """
    log.info("newsapi.fetch called for %s days=%d topk=%d timeout_s=%d", ticker, days, topk, timeout_s)

    # Example return shape (uncomment and adapt when implementing):
    # return [
    #     normalize_item({
    #         "title": "NewsAPI example",
    #         "url": "https://newsapi-example.com/article",
    #         "source": "newsapi-example.com",
    #         "published_at": "2025-10-20T08:30:00Z",
    #         "lang": "en",
    #         "content": "",
    #     })
    # ][:topk]

    return []