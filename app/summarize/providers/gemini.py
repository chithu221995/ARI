from __future__ import annotations
import os
import logging
import json
import re
from typing import Any
import httpx

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

async def summarize(
    payload_text: str,
    system_prompt: str,
    _unused: str,
    max_tokens: int = 1024,
    temperature: float = 0.2,
) -> str:
    """
    Call Gemini REST (v1beta) generateContent.
    - Auth: x-goog-api-key header when GEMINI_API_KEY is an API key; otherwise treat it as OAuth Bearer.
    - Body: uses system_instruction + contents (no legacy "prompt" field).
    - Returns assistant text (empty string on failure).
    - Raises GeminiRateLimitError on 429 / quota responses so callers can fallback to OpenAI.
    """
    if not GEMINI_API_KEY:
        log.error("gemini.summarize: GEMINI_API_KEY not configured")
        return ""

    # Endpoint & headers
    url = f"{GEMINI_BASE.rstrip('/')}/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"Content-Type": "application/json"}
    headers.update(_auth_headers(GEMINI_API_KEY))

    # Request payload: put rules in system_instruction; article bundle in user content
    prompt = (system_prompt or "").strip() + "\n\n" + (payload_text or "").strip()

    # guarded debug: indicate whether a system_instruction was provided and a short safe snippet (no secrets)
    if log.isEnabledFor(logging.DEBUG):
        sys_text = (system_prompt or "").strip()
        sys_present = bool(sys_text)
        sys_snippet = sys_text[:80] if sys_text else ""
        log.debug("gemini.summarize: system_instruction_present=%s system_snippet=%r", sys_present, sys_snippet)

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

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(url, json=body, headers=headers)
            if r.status_code == 429:
                # Trigger caller fallback path
                log.warning("gemini.summarize: rate limited (429); body=%s", r.text[:400])
                raise GeminiRateLimitError("gemini rate limited (429)")
            if not (200 <= r.status_code < 300):
                # Log the first ~400 chars of the error for diagnosis
                log.error("gemini.summarize: HTTP %s; body=%s", r.status_code, r.text[:400])
            r.raise_for_status()
            j = r.json()
            # Happy path: candidates[0].content.parts[0].text
            try:
                cands = j.get("candidates") or []
                if cands and isinstance(cands, list):
                    content = (cands[0].get("content") or {})
                    parts = content.get("parts") or []
                    if parts and isinstance(parts, list):
                        txt = (parts[0].get("text") or "").strip()

                        # strip fenced json blocks ```json ... ``` or ``` ... ```
                        m = re.search(r'```(?:json)?\s*(.*?)\s*```', txt, flags=re.S | re.I)
                        if m:
                            txt = m.group(1).strip()

                        # verify JSON-ish (best-effort); warn if not valid but still return raw string
                        try:
                            json.loads(txt)
                        except Exception:
                            log.warning("gemini.summarize: response not valid JSON after stripping fences")
                        return txt or ""
            except Exception:
                # Fall through to alternative fields below
                pass

            # Secondary fallbacks for older/atypical responses; strip fences if present
            fallback = (j.get("output", "") or j.get("text", "") or "")
            fallback = (fallback or "").strip()
            m2 = re.search(r'```(?:json)?\s*(.*?)\s*```', fallback, flags=re.S | re.I)
            if m2:
                fallback = m2.group(1).strip()
            return fallback or ""
    except GeminiRateLimitError:
        raise
    except Exception as e:
        log.exception("gemini.summarize: request failed: %s", e)
        return ""