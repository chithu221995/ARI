from __future__ import annotations
import logging
from typing import Optional, Dict
import httpx
import time
from app.observability.metrics import record_metric

from app.core import settings

log = logging.getLogger("ari.news")


async def extract_with_diffbot(url: str, timeout_s: Optional[int] = None) -> Optional[Dict[str, str]]:
    """
    Call Diffbot API and record timing/ok metric (provider 'diffbot_api').
    Wraps existing logic to ensure metrics fire even on exceptions/early returns.
    """
    t0 = time.time()
    ok = False
    result = None
    try:
        # existing Diffbot HTTP logic goes here and returns `result` (dict or None)
        result = await _call_diffbot_api(url, timeout_s=timeout_s)  # keep your existing call
        ok = bool(result)
        return result
    except Exception:
        # re-raise after metrics are recorded in finally
        raise
    finally:
        try:
            lat_ms = int((time.time() - t0) * 1000)
            record_metric("extract", "diffbot_api", lat_ms, ok=ok)
        except Exception:
            import logging

            logging.getLogger("ari.metrics").exception("metrics: failed to record diffbot_api metric")


async def _call_diffbot_api(url: str, timeout_s: Optional[int] = None) -> Optional[Dict[str, str]]:
    """
    Extract article text/title/lang using Diffbot Analyze API.
    Returns dict {'text': ..., 'title': ..., 'lang': ...} or None on error/no-content.
    """
    if not url:
        return None

    token = getattr(settings, "DIFFBOT_TOKEN", "") or ""
    if not token:
        log.info("diffbot: DIFFBOT_TOKEN missing, skipping for %s", url)
        return None

    timeout = float(timeout_s) if timeout_s is not None else float(getattr(settings, "NEWS_TIMEOUT_S", 8) or 8)
    endpoint = "https://api.diffbot.com/v3/analyze"
    params = {"token": token, "url": url}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(endpoint, params=params)
            if r.status_code != 200:
                log.info("diffbot: non-200 for %s status=%d", url, r.status_code)
                return None
            data = r.json()
    except Exception as e:
        log.info("diffbot: request failed for %s: %s", url, e)
        return None

    objs = data.get("objects") or []
    if not objs:
        log.info("diffbot: no objects returned for %s", url)
        return None

    obj = objs[0] or {}
    text = obj.get("text") or ""
    if not text:
        log.info("diffbot: empty text for %s", url)
        return None

    title = obj.get("title") or obj.get("pageTitle") or ""
    lang = obj.get("language") or data.get("language") or None

    return {"text": text, "title": title, "lang": lang}