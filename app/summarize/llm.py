from __future__ import annotations
import os
import time
import json
import logging
from typing import List, Dict, Any

import httpx

log = logging.getLogger("ari.summarize")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # adjust as needed
OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")

if not OPENAI_API_KEY:
    log.warning("[summary] OPENAI_API_KEY missing — summarizer will return empty output")

SYSTEM_PROMPT = (
    "You are an assistant that ingests a small list of news items and produces concise actionable summaries. "
    "For each input item produce a short paragraph (3-4 sentences) that explains the gist and why it matters for the asset. "
    "Must include a line starting with exactly 'Sentiment: ' followed by one of Positive, Neutral, or Negative. "
    "Output a JSON object with a top-level key 'items' that is an array of objects. Each object should include at least: "
    "'title' (string), 'bullets' (array of short bullet strings -- optional), 'why_it_matters' (string), and 'sentiment' (string). "
    "You may wrap the JSON in markdown code fences but ensure the JSON is parseable. Keep responses concise and factual."
)


async def summarize_items(batch: List[Dict[str, Any]], *, ticker: str | None = None) -> Dict[str, Any]:
    """
    Send batch to LLM and return parsed {'items': [...], 'ok': True, 'latency_ms': ...}
    On error return {'items': [], 'ok': False, 'error': '...'}
    """
    if not batch:
        return {"ok": True, "items": []}

    # Guard: if no API key, bail early with clear error
    if not OPENAI_API_KEY:
        return {"ok": False, "items": [], "error": "no_api_key", "latency_ms": 0}

    log.info("[summary] sending %d items to LLM for ticker=%s", len(batch), ticker)
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}

    # prepare messages
    user_payload = {
        "ticker": ticker or "",
        "items": [
            {"title": (i.get("title") or ""), "url": (i.get("url") or ""), "content": (i.get("translated_text") or i.get("content") or "")}
            for i in batch
        ],
        "instructions": "Return JSON with key 'items' as described in system prompt."
    }

    body = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)}
        ],
        "temperature": 0.2,
        "max_tokens": 900,
    }

    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(OPENAI_API_URL, headers=headers, json=body)
            latency_ms = int((time.time() - start) * 1000)
            log.info("[summary] LLM response status=%s latency_ms=%d", resp.status_code, latency_ms)
            # Guard non-200 explicitly
            if resp.status_code != 200:
                return {"ok": False, "items": [], "error": f"llm_http_{resp.status_code}", "latency_ms": latency_ms}
            try:
                data = resp.json()
            except Exception:
                log.exception("[summary] invalid json from LLM")
                data = {}
    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        log.exception("[summary] LLM request failed (latency_ms=%d)", latency_ms)
        return {"ok": False, "error": str(e), "items": [], "latency_ms": latency_ms}

    # extract assistant text (support chat completions shape)
    output_text = ""
    try:
        choices = data.get("choices") or []
        if choices:
            first = choices[0]
            msg = first.get("message") or first.get("text") or {}
            if isinstance(msg, dict):
                output_text = msg.get("content", "") or ""
            else:
                output_text = str(msg or "")
    except Exception:
        output_text = ""

    if not output_text.strip():
        log.info("[summary] empty assistant content")
    else:
        log.info("[summary] raw assistant content (first 400 chars): %s", output_text[:400])

    # permissive JSON parsing with fallbacks
    parsed: List[Dict[str, Any]] = []
    try:
        obj = json.loads(output_text)
        if isinstance(obj, dict) and isinstance(obj.get("items"), list):
            parsed = obj["items"]
        elif isinstance(obj, list):
            parsed = obj
    except Exception as e:
        # try stripping fences and common wrappers
        try:
            txt = output_text.strip()
            for fence in ("```json", "```"):
                if txt.startswith(fence):
                    txt = txt[len(fence):].strip()
            if txt.endswith("```"):
                txt = txt[:-3].strip()
            obj = json.loads(txt)
            if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                parsed = obj["items"]
            elif isinstance(obj, list):
                parsed = obj
        except Exception:
            # last resort: slice from first '{' to last '}' and retry
            try:
                start_i = output_text.find("{")
                end_i = output_text.rfind("}")
                if start_i != -1 and end_i != -1 and end_i > start_i:
                    obj = json.loads(output_text[start_i:end_i + 1])
                    if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                        parsed = obj["items"]
            except Exception:
                log.exception("[summary] parse FAILED")
                parsed = []

    # If parsed empty, keep existing behavior: return ok=False and include assistant preview
    if not parsed:
        log.info("[summary] parsed items empty; assistant preview (first 400): %s", output_text[:400])
        return {"ok": False, "error": "empty_parsed", "items": [], "latency_ms": int((time.time() - start) * 1000)}

    # Final normalization: ensure sentiment field exists and short paragraph present
    normalized = []
    for it in parsed:
        title = it.get("title") or ""
        why = it.get("why_it_matters") or it.get("summary") or ""
        bullets = it.get("bullets") or []
        sentiment = (it.get("sentiment") or "").strip()
        if not sentiment:
            # try to extract from raw text sentiment line
            s = ""
            if isinstance(why, str) and "Sentiment:" in why:
                try:
                    s = why.split("Sentiment:")[-1].strip().split()[0]
                except Exception:
                    s = ""
            sentiment = s or "Neutral"
        normalized.append({
            "title": title,
            "bullets": bullets if isinstance(bullets, list) else [],
            "why_it_matters": why,
            "sentiment": sentiment,
        })

    total_latency = int((time.time() - start) * 1000)
    return {"ok": True, "items": normalized, "latency_ms": total_latency}