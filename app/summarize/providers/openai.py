from __future__ import annotations
import os
import logging
from typing import Optional
import time
from app.core.metrics import record_vendor_event
from app.core.retry_utils import rate_limited_retry  # ADD THIS

import httpx

log = logging.getLogger("ari.summarize.openai")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or ""
OPENAI_MODEL = os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
OPENAI_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com")

@rate_limited_retry(
    provider="openai",
    max_retries=3,
    base_delay=1.0,
    max_per_minute=15
)
async def summarize(
    payload_text: str,
    system_prompt: str,
    output_instructions: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """
    Call OpenAI Chat Completions with automatic retries and rate limiting.
    """
    if not OPENAI_API_KEY:
        log.error("openai.summarize: OPENAI_API_KEY not configured")
        return ""

    url = f"{OPENAI_BASE.rstrip('/')}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": payload_text},
    ]
    body = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "temperature": float(temperature or 0.0),
        "max_tokens": int(max_tokens or 512),
    }

    start_time = time.perf_counter()
    ok = False
    result = ""

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(url, json=body, headers=headers)
            r.raise_for_status()
            j = r.json()
            
            choices = j.get("choices") or []
            if choices:
                msg = choices[0].get("message") or {}
                text = msg.get("content") or ""
                result = text or ""
                ok = True
                return result
            
            result = j.get("text", "") or ""
            ok = bool(result)
            return result
            
    except Exception as e:
        log.exception("openai.summarize: request failed: %s", e)
        ok = False
        return ""
    finally:
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        record_vendor_event(
            provider="openai",
            event="summarize",
            ok=ok,
            latency_ms=latency_ms
        )