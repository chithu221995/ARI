from __future__ import annotations
import os
import logging
import json
import re
from typing import Any
import httpx
import time

from app.core.metrics import record_vendor_event
from app.core.retry_utils import rate_limited_retry, RetryExhausted  # ADD THIS

log = logging.getLogger("ari.summarize.gemini")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or ""
GEMINI_MODEL = os.getenv("GEMINI_MODEL") or "gemini-2.5-pro"
GEMINI_BASE = os.getenv("GEMINI_API_BASE", "https://generativelanguage.googleapis.com")

# If you use an API key (recommended for server-to-server), send via x-goog-api-key header.
# If you use OAuth access tokens, send via Authorization: Bearer <token>.

def _auth_headers(api_key: str) -> dict[str, str]:
    # Heuristic: API keys are long base64-ish strings that do NOT start with 'ya29.'; oauth tokens often do.
    if not api_key:
        return {}
    if api_key.startswith("ya29."):
        return {"Authorization": f"Bearer {api_key}"}
    # Default to API key header
    return {"x-goog-api-key": api_key}

class GeminiRateLimitError(Exception):
    pass

@rate_limited_retry(
    provider="gemini",
    max_retries=3,
    base_delay=1.0,
    max_per_minute=5
)
async def summarize(
    payload_text: str,
    system_prompt: str,
    _unused: str,
    max_tokens: int = 1024,
    temperature: float = 0.2,
) -> str:
    """
    Call Gemini REST with automatic retries and rate limiting.
    """
    if not GEMINI_API_KEY:
        log.error("gemini.summarize: GEMINI_API_KEY not configured")
        raise ValueError("GEMINI_API_KEY not configured")

    url = f"{GEMINI_BASE.rstrip('/')}/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"Content-Type": "application/json"}
    headers.update(_auth_headers(GEMINI_API_KEY))

    body = {
        "system_instruction": {
            "parts": [{"text": system_prompt or ""}]
        },
        "contents": [
            {
                "parts": [{"text": payload_text or ""}]
            }
        ],
        "generationConfig": {
            "temperature": float(temperature or 0.0),
            "maxOutputTokens": int(max_tokens or 1024)
        }
    }

    start_time = time.perf_counter()
    ok = False
    result = ""

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(url, json=body, headers=headers)
            
            # Let retry decorator handle 429
            if r.status_code == 429:
                log.warning("gemini.summarize: rate limited (429)")
                r.raise_for_status()  # Will be caught and retried
            
            r.raise_for_status()
            j = r.json()

            try:
                cands = j.get("candidates") or []
                if cands and isinstance(cands, list):
                    content = (cands[0].get("content") or {})
                    parts = content.get("parts") or []
                    if parts and isinstance(parts, list):
                        txt = (parts[0].get("text") or "").strip()

                        m = re.search(r'```(?:json)?\s*(.*?)\s*```', txt, flags=re.S | re.I)
                        if m:
                            txt = m.group(1).strip()

                        result = txt or ""
                        ok = True
                        return result
            except Exception:
                pass

            fallback = (j.get("output", "") or j.get("text", "") or "").strip()
            m2 = re.search(r'```(?:json)?\s*(.*?)\s*```', fallback, flags=re.S | re.I)
            if m2:
                fallback = m2.group(1).strip()
            result = fallback or ""
            ok = bool(result)
            
            if not ok:
                raise ValueError("Empty response from Gemini")
            
            return result

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            raise  # Let retry decorator handle
        log.error("gemini.summarize: HTTP error %d", e.response.status_code)
        ok = False
        raise
    except Exception as e:
        log.exception("gemini.summarize: request failed: %s", e)
        ok = False
        raise
    finally:
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        record_vendor_event(
            provider="gemini",
            event="summarize",
            ok=ok,
            latency_ms=latency_ms
        )