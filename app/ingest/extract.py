from __future__ import annotations
import asyncio
import logging
import os
import httpx
import time
from typing import Optional, Tuple, List, Dict, Any
from app.observability.metrics import record_metric

import trafilatura

from app.core import settings

log = logging.getLogger("ari.extract")

# Transient error used to indicate retryable HTTP statuses (e.g. 429, 5xx)
class _TransientError(Exception):
    def __init__(self, status: int):
        super().__init__(f"Transient HTTP error: {status}")
        self.status = status


async def extract_via_diffbot(url: str, timeout_s: Optional[int] = None) -> Optional[str]:
    """
    Run Diffbot extraction and ensure a metric is recorded even on early returns/exceptions.
    """
    t0 = time.time()
    ok = False
    text = None
    try:
        ok_diffbot, text, _title = await extract_with_diffbot(url, timeout_s=timeout_s)
        ok = bool(ok_diffbot and text)
        return text
    except Exception:
        raise
    finally:
        try:
            lat_ms = int((time.time() - t0) * 1000)
            log.info("extract: metric fired for %s ok=%s", url, ok)
            record_metric("extract", "diffbot", lat_ms, ok=ok)
        except Exception:
            import logging
            logging.getLogger("ari.metrics").exception("metrics: failed to record diffbot extract metric")


async def extract_with_diffbot(url: str, timeout_s: Optional[int] = None) -> Tuple[bool, str, str]:
    """
    Call Diffbot Analyze API. Return (ok, text, title).
    Raises _TransientError on retryable HTTP statuses (429, 5xx).
    """
    if not url:
        return False, "", ""

    token = getattr(settings, "DIFFBOT_TOKEN", "") or ""
    if not token:
        log.info("diffbot.extract: no token, skipping for %s", url)
        return False, "", ""

    timeout = float(timeout_s) if timeout_s is not None else float(getattr(settings, "NEWS_TIMEOUT_S", 8) or 8)
    endpoint = "https://api.diffbot.com/v3/analyze"
    params = {"token": token, "url": url}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(endpoint, params=params)
            status = r.status_code
            if status == 200:
                data = r.json()
                objs = data.get("objects") or []
                if not objs:
                    log.info("diffbot.extract: no objects for %s", url)
                    return False, "", ""
                obj = objs[0] or {}
                text = obj.get("text") or ""
                if not text:
                    log.info("diffbot.extract: empty text for %s", url)
                    return False, "", ""
                title = obj.get("title") or obj.get("pageTitle") or ""
                lang = obj.get("language") or data.get("language") or None
                # do not perform language enforcement here; caller may decide
                return True, text, title or ""
            if status == 429 or 500 <= status < 600:
                # retryable
                raise _TransientError(status)
            # non-retryable failure
            log.info("diffbot.extract: non-200 %d for %s", status, url)
            return False, "", ""
    except _TransientError:
        raise
    except Exception as e:
        log.info("diffbot.extract: request failed for %s: %s", url, e)
        return False, "", ""


async def extract_with_fallback(url: str, timeout_s: Optional[int] = None) -> Tuple[bool, str, str]:
    """
    Fetch HTML and extract main text/title using trafilatura. Return (ok, text, title).
    """
    if not url:
        return False, "", ""

    timeout = float(timeout_s) if timeout_s is not None else float(getattr(settings, "NEWS_TIMEOUT_S", 8) or 8)
    headers = {"User-Agent": "ARI-NewsFetcher/1.0"}
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            r = await client.get(url)
            if r.status_code != 200:
                log.info("fallback.extract: non-200 %d for %s", r.status_code, url)
                return False, "", ""
            ctype = r.headers.get("content-type", "") or ""
            if "text/html" not in ctype.lower():
                log.info("fallback.extract: content-type not html for %s", url)
                return False, "", ""

            limit = 1_500_000
            chunks = []
            size = 0
            async for chunk in r.aiter_bytes():
                if not chunk:
                    break
                need = limit - size
                if need <= 0:
                    break
                if len(chunk) > need:
                    chunks.append(chunk[:need])
                    size += need
                    break
                chunks.append(chunk)
                size += len(chunk)
            raw = b"".join(chunks)
            try:
                html = raw.decode("utf-8", errors="replace")
            except Exception:
                html = raw.decode("latin1", errors="replace")
    except Exception as e:
        log.info("fallback.extract: request failed for %s: %s", url, e)
        return False, "", ""

    try:
        text = trafilatura.extract(html, include_comments=False, include_tables=False, include_formatting=False) or ""
    except Exception:
        text = ""

    if not text:
        # crude fallback: strip tags
        import re
        text = re.sub(r"<[^>]+>", " ", html)
        text = " ".join(text.split())

    if not text:
        return False, "", ""

    # try to find a title from the HTML (trafilatura may include it)
    title = ""
    try:
        # attempt a simple <title> parse
        import re
        m = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
        if m:
            title = m.group(1).strip()
    except Exception:
        title = ""

    return True, text, title


async def extract_text(url: str, timeout_s: Optional[int] = None, provider: str = "diffbot", allow_fallback: bool = True) -> tuple[str, Optional[str]]:
    """
    High-level extraction wrapper.

    - provider="diffbot" will prefer Diffbot and, if allow_fallback is False,
      WILL NOT try any HTML/manual fallback (returns ("", None) on failure).
    - If provider != "diffbot", we still prefer Diffbot first, then fall back
      to HTML/manual extraction only when allow_fallback is True.

    Returns (text, source) where source is "diffbot" or "html" (or None on failure).
    """
    if not url:
        return "", None

    # Try Diffbot first (best quality)
    try:
        text = await extract_via_diffbot(url, timeout_s=timeout_s)
    except Exception:
        log.exception("extract_text: diffbot call raised for %s", url)
        text = None

    if text:
        text = text.strip()
        if text:
            return text, "diffbot"

    # If caller requested strict Diffbot-only, return failure now
    if provider == "diffbot" and not allow_fallback:
        return "", None

    # Otherwise, attempt HTML/manual fallback only if allowed
    if allow_fallback:
        # try common html extractor if available
        try:
            # import lazily to avoid circular imports when module not present
            from .html_extract import extract_via_html  # optional helper
        except Exception:
            extract_via_html = None

        if extract_via_html:
            try:
                html_text = await extract_via_html(url, timeout_s=timeout_s)
                if html_text:
                    html_text = html_text.strip()
                    if html_text:
                        return html_text, "html"
            except Exception:
                log.exception("extract_text: html fallback failed for %s", url)

    # nothing succeeded
    return "", None


async def extract_bodies(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Given a list of dicts with at least {'url', 'url_hash', ...}, try to extract content for each URL.
    - Try Diffbot (via extract_text) and on transient 429 retry once.
    - Fallback to HTML extraction (extract_text should implement that fallback).
    - No DB writes here; return rows with 'content' set (or None on failure) and optional extract metadata.
    """
    out: List[Dict[str, Any]] = []
    timeout_s = getattr(settings, "NEWS_TIMEOUT_S", 8)

    for r in (rows or []):
        url = (r.get("url") or "").strip()
        out_row = dict(r)
        out_row["content"] = None
        out_row.setdefault("extract_method", None)
        out_row.setdefault("extract_text_len", 0)

        if not url:
            out.append(out_row)
            continue

        try:
            # attempt up to 2 tries (retry once on potential rate limit)
            attempt = 0
            last_ok = False
            last_text = None
            last_method = None
            while attempt < 2:
                attempt += 1
                try:
                    text, method = await extract_text(url, timeout_s=timeout_s)
                except Exception as e:
                    log.debug("extract_bodies: extract_text exception attempt=%d url=%s err=%s", attempt, url, e)
                    text = None
                    method = None

                last_ok = bool(text)
                last_text = text
                last_method = method

                if last_ok:
                    break
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                break

            if last_ok and last_text:
                out_row["content"] = last_text
                out_row["extract_method"] = last_method or "unknown"
                out_row["extract_text_len"] = len(last_text or "")
            else:
                out_row["content"] = None
                out_row["extract_method"] = last_method or "failed"
                out_row["extract_text_len"] = len(last_text or "")
        except Exception:
            log.exception("extract_bodies: unexpected failure for url=%s", url)
            out_row["content"] = None
            out_row["extract_method"] = "error"
            out_row["extract_text_len"] = 0

        out.append(out_row)

    return out