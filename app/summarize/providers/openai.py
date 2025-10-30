from __future__ import annotations
import os
import logging
from typing import Optional

import httpx

log = logging.getLogger("ari.summarize.openai")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or ""
OPENAI_MODEL = os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
OPENAI_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com")

async def summarize(
    payload_text: str,
    system_prompt: str,
    output_instructions: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """
    Call OpenAI Chat Completions API (async) and return the assistant text (str).
    Returns empty string on error.
    """
    if not OPENAI_API_KEY:
        log.error("openai.summarize: OPENAI_API_KEY not configured")
        return ""

    url = f"{OPENAI_BASE.rstrip('/')}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    # single system message + user payload
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

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(url, json=body, headers=headers)
            r.raise_for_status()
            j = r.json()
            # standard path: choices[0].message.content
            choices = j.get("choices") or []
            if choices:
                msg = choices[0].get("message") or {}
                text = msg.get("content") or ""
                return text or ""
            # fallback: top-level text
            return j.get("text", "") or ""
    except Exception as e:
        log.exception("openai.summarize: request failed: %s", e)
        return ""