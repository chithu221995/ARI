from __future__ import annotations

import os
import json
import logging
import time
import asyncio
import re
import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from app.core import settings
from app.summarize.providers import get_provider
from app.core.cache import llm_allow_request, CACHE_DB_PATH
from app.summarize.prompts import SYSTEM_PROMPT, _parse_plain_fallback
from app.core.metrics import record_metric

# compiled sanitizer to remove leading meta-language like "this article ...", "the article ..."
_META_LEAD_RE = re.compile(
    r'^(?:\s*(?:this|the)\s+(?:article|report|piece|story)\s+(?:reports?|covers|details?|explains?|discusses|is\s+about|according\s+to)\s*[:\-–—]*\s*)',
    flags=re.IGNORECASE,
)


log = logging.getLogger("ari.summarize.llm")

# Read caps from env (defaults)
LLM_RPM_CAP = int(os.getenv("LLM_RPM_CAP", "5"))
LLM_DAILY_CAP = int(os.getenv("LLM_DAILY_CAP", "100"))
DEFAULT_PROVIDER = (os.getenv("LLM_PROVIDER") or "openai").strip().lower()

# In-memory, best-effort Gemini quota tracker (process-local)
GEMINI_LIMITS = {"max_per_day": int(getattr(settings, "GEMINI_MAX_PER_DAY", 100)), "max_per_minute": int(getattr(settings, "GEMINI_MAX_PER_MINUTE", 5))}
_gemini_usage = {"calls": [], "daily": []}

# Helper to persist lightweight per-process Gemini usage bookkeeping
def _record_gemini_call() -> None:
    """
    Best-effort, process-local Gemini quota bookkeeping to help with local decisioning.
    Safe to call even if limits dicts are empty.
    """
    try:
        now = time.time()
        # per-minute sliding window
        _gemini_usage["calls"] = [t for t in _gemini_usage["calls"] if now - t < 60.0]
        _gemini_usage["calls"].append(now)
        # per-day sliding window (24h)
        _gemini_usage["daily"] = [t for t in _gemini_usage["daily"] if now - t < 86400.0]
        _gemini_usage["daily"].append(now)
    except Exception:
        log.exception("gemini usage bookkeeping failed")

# helper to call provider with quota enforcement and fallback
async def call_llm_with_quota(payload_text: str, system_prompt: str, max_tokens: int = 1024, temperature: float = 0.0) -> tuple[str, str]:
    """
    Returns (response_text, provider_used).
    Will enforce RPM and daily caps using CACHE_DB_PATH.
    If Gemini signals daily cap (or is over cap), will fall back to OpenAI once.
    """
    provider_name = _resolve_provider()
    log.info(
        "llm.call: provider=%s (env=%r, settings=%r)",
        provider_name,
        os.getenv("LLM_PROVIDER"),
        getattr(settings, "LLM_PROVIDER", None),
    )
    provider_fn = get_provider()

    # check allowance
    allowed, wait_ms, daily_reached = await llm_allow_request(CACHE_DB_PATH, provider_name, LLM_RPM_CAP, LLM_DAILY_CAP if provider_name == "gemini" else 0)
    if not allowed:
        if daily_reached and provider_name == "gemini":
            # fallback to openai for this request
            log.info("llm: gemini daily cap reached, falling back to openai for this request")
            provider_name = "openai"
            provider_fn = get_provider()  # get_provider will return openai when env says so; ensure explicit import below
            # check openai allowance (no daily cap enforced here)
            allowed2, wait_ms2, _ = await llm_allow_request(CACHE_DB_PATH, provider_name, LLM_RPM_CAP, 0)
            if not allowed2:
                log.info("llm: openai rpm limited, wait_ms=%d", wait_ms2)
                await asyncio.sleep(wait_ms2 / 1000.0)
        else:
            # RPM-limited: wait then proceed
            log.info("llm: provider=%s rpm limited wait_ms=%d", provider_name, wait_ms)
            await asyncio.sleep(wait_ms / 1000.0)

    # ensure we call the actual provider module (respect fallback override)
    if provider_name == "openai":
        provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize
    elif provider_name == "gemini":
        provider_fn = __import__("app.summarize.providers.gemini", fromlist=[""]).summarize
    else:
        provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize

    # call provider
    start = time.time()
    try:
        resp = await provider_fn(payload_text, system_prompt, "", max_tokens, temperature)
        elapsed_ms = int((time.time() - start) * 1000)
        log.info("llm.call: provider=%s elapsed_ms=%d", provider_name, elapsed_ms)
        return resp or "", provider_name
    except Exception as e:
        # Auto-fallback when Gemini signals rate-limit/quota exceeded.
        msg = str(e).lower()
        is_rate = "gemini_rate_limit" in msg or "429" in msg or "rate limit" in msg or "quota" in msg
        log.exception("llm.call: provider=%s failed: %s", provider_name, e)
        if provider_name == "gemini" and is_rate:
            log.info("llm.call: detected gemini rate/quota condition -> falling back to openai for this request")
            try:
                provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize
                resp = await provider_fn(payload_text, system_prompt, "", max_tokens, temperature)
                return resp or "", "openai"
            except Exception:
                log.exception("llm.call: openai fallback also failed")
                return "", provider_name
        # existing behavior: if gemini failed for other reasons, attempt openai as before
        if provider_name == "gemini":
            try:
                provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize
                resp = await provider_fn(payload_text, system_prompt, "", max_tokens, temperature)
                return resp or "", "openai"
            except Exception:
                log.exception("llm.call: openai fallback failed")
        return "", provider_name

# Example usage wrapper used elsewhere in this module:
async def summarize_items_via_llm(candidates: List[dict], ticker: str) -> dict:
    """
    Build payload_text and call LLM via call_llm_with_quota.
    Returns dict-like with 'ok' and 'items' if parseable.
    """
    payload_text = "\n\n".join(
        f"Article {i+1}:\nTitle: {row.get('title','')}\nURL: {row.get('url','')}\nText: {row.get('translated_text') or row.get('content','')[:3000]}"
        for i, row in enumerate(candidates)
    )
    system_prompt = SYSTEM_PROMPT
    resp_text, prov = await call_llm_with_quota(payload_text, system_prompt, max_tokens=1024, temperature=0.0)
    log.debug("summarize_items_via_llm: provider=%s resp_len=%d", prov, len(resp_text or ""))
    # parse JSON safely (previous parse helper)
    parsed_items = []
    try:
        import json
        parsed = json.loads(resp_text or "{}")
        parsed_items = parsed.get("items") if isinstance(parsed, dict) else []
    except Exception:
        log.exception("summarize_items_via_llm: parse failed")

    return {"ok": True, "items": parsed_items or [], "provider": prov}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")

if not OPENAI_API_KEY:
    log.warning("[summary] OPENAI_API_KEY missing — summarizer will return empty output")



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
    log.info("summarize: batch_size=%d for %s", len(batch), ticker or "")

    # knobs
    max_tokens = int(getattr(settings, "SUMMARY_MAX_TOKENS", 900))
    temperature = float(getattr(settings, "SUMMARY_TEMPERATURE", 0.2))

    # Build plain-text payload
    parts: List[str] = []
    for i in batch:
        title = (i.get("title") or "").strip()
        url = (i.get("url") or "").strip()
        content = (i.get("translated_text") or i.get("content") or "").strip()
        parts.append(f"Title: {title}\nURL: {url}\n\n{content}")
    payload_text = "\n\n---\n\n".join(parts)

    # Provider selection with local Gemini quota check and possible fallback to OpenAI
    provider = _resolve_provider()
    log.info(
        "llm.call: provider=%s (env=%r, settings=%r)",
        provider,
        os.getenv("LLM_PROVIDER"),
        getattr(settings, "LLM_PROVIDER", None),
    )
    if provider == "gemini" and not _can_use_gemini():
        log.warning("Gemini quota reached → falling back to OpenAI")
        provider = "openai"

    # Resolve provider function
    if provider == "openai":
        provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize
    elif provider == "gemini":
        provider_fn = __import__("app.summarize.providers.gemini", fromlist=[""]).summarize
    else:
        provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize

    # log chosen provider before making the external API call
    log.info("llm.call: provider=%s", provider)
    
    start = time.time()
    try:
        output_text = await provider_fn(payload_text, SYSTEM_PROMPT, "", max_tokens, temperature)
        provider_used = provider
        latency_ms = int((time.time() - start) * 1000)
        log.info("[summary] provider=%s latency_ms=%d", provider_used, latency_ms)
        # record success metric (best-effort)
        try:
            record_metric("summarize", provider_used, int(latency_ms or 0), True)
        except Exception:
            log.exception("metrics: failed to record summarize success metric")
 
    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        log.exception("[summary] LLM request failed (latency_ms=%d) error=%s", latency_ms, e)
        # record failure for initial provider
        try:
            record_metric("summarize", provider, int(latency_ms or 0), False)
        except Exception:
            log.exception("metrics: failed to record summarize failure metric")

        # if Gemini failed, try OpenAI as a fallback once
        if provider == "gemini":
            try:
                provider_fn = __import__("app.summarize.providers.openai", fromlist=[""]).summarize
                start2 = time.time()
                output_text = await provider_fn(payload_text, SYSTEM_PROMPT, "", max_tokens, temperature)
                provider_used = "openai"
                latency_ms = int((time.time() - start2) * 1000)
                log.info("[summary] fallback provider=openai latency_ms=%d", latency_ms)
                # record fallback success
                try:
                    record_metric("summarize", "openai", int(latency_ms or 0), True)
                except Exception:
                    log.exception("metrics: failed to record summarize fallback success metric")
            except Exception as e2:
                latency_ms = int((time.time() - start) * 1000)
                log.exception("[summary] openai fallback failed (latency_ms=%d) error=%s", latency_ms, e2)
                # record fallback failure
                try:
                    record_metric("summarize", "openai", int(latency_ms or 0), False)
                except Exception:
                    log.exception("metrics: failed to record summarize fallback failure metric")
                return {"ok": False, "error": str(e2), "items": [], "latency_ms": latency_ms}
        else:
            return {"ok": False, "error": str(e), "items": [], "latency_ms": latency_ms}

    if not (output_text or "").strip():
        log.info("[summary] empty assistant content for %s", ticker)
    else:
        log.info("[summary] raw assistant content (first 400 chars): %s", (output_text or "").replace("\n", " ")[:400])

    # permissive JSON parsing with fallbacks
    parsed: List[Dict[str, Any]] = []

    # 1) try direct JSON parse
    try:
        obj = json.loads(output_text)
        if isinstance(obj, dict) and isinstance(obj.get("items"), list):
            parsed = obj["items"]
        elif isinstance(obj, list):
            parsed = obj
    except Exception:
        parsed = []

    # 2) if direct parse failed, try to extract fenced code blocks (```json or ```)
    if not parsed:
        try:
            txt = output_text or ""
            m = re.search(r"```json(.*?)```", txt, flags=re.S | re.I)
            if not m:
                m = re.search(r"```(.*?)```", txt, flags=re.S)
            if m:
                candidate = m.group(1).strip()
                try:
                    obj = json.loads(candidate)
                    if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                        parsed = obj["items"]
                    elif isinstance(obj, list):
                        parsed = obj
                except Exception:
                    # try to strip leading/trailing fences/braces and retry
                    cand2 = candidate.strip()
                    if cand2.startswith("json"):
                        cand2 = cand2[4:].strip()
                    try:
                        obj = json.loads(cand2)
                        if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                            parsed = obj["items"]
                        elif isinstance(obj, list):
                            parsed = obj
                    except Exception:
                        parsed = []
        except Exception:
            parsed = []

    # 3) try to find the first {...} JSON object in the assistant text
    if not parsed:
        try:
            start_i = output_text.find("{")
            end_i = output_text.rfind("}")
            if start_i != -1 and end_i != -1 and end_i > start_i:
                obj = json.loads(output_text[start_i:end_i + 1])
                if isinstance(obj, dict) and isinstance(obj.get("items"), list):
                    parsed = obj["items"]
                elif isinstance(obj, list):
                    parsed = obj
        except Exception:
            parsed = []

    # 4) final fallback: treat assistant text as plain-text blocks and parse
    if not parsed:
        try:
            plain = _parse_plain_fallback(output_text or "")
            if plain:
                # Map plain fallback blocks to the original batch by index order (1..N)
                parsed = []
                count = min(len(batch or []), len(plain))
                for i in range(count):
                    src = batch[i]
                    p = plain[i]
                    title = p.get("title") or src.get("title") or ""
                    summary = p.get("summary") or ""
                    sentiment = p.get("sentiment") or "Neutral"
                    # accept provided relevance if present, else apply lightweight heuristic
                    rel = p.get("relevance")
                    try:
                        if rel is None:
                            relevance = None
                        elif isinstance(rel, int):
                            relevance = rel
                        elif isinstance(rel, str):
                            srel = rel.strip()
                            # try integer-ish strings first, then floats
                            if srel.isdigit():
                                relevance = int(srel)
                            else:
                                try:
                                    relevance = int(float(srel))
                                except Exception:
                                    relevance = None
                        else:
                            # attempt a best-effort numeric coercion
                            relevance = int(rel)
                    except Exception:
                        relevance = None

                    # lightweight heuristic only when relevance is missing/invalid
                    if relevance is None or not (1 <= relevance <= 10):
                        text_for_infer = f"{title} {summary}".lower()
                        low_indicators = ["stock pick", "day trading", "outlook for the day", "pr wire", "listicle"]
                        high_indicators = ["layoff", "acquisition", "guidance", "fine", "lawsuit", "contract", "win", "upgrade", "margin", "regulation", "customer"]
                        if any(k in text_for_infer for k in low_indicators):
                            relevance = 2
                        elif any(k in text_for_infer for k in high_indicators):
                            relevance = 8
                        else:
                            relevance = 4

                    # final safe clamp/cast
                    try:
                        relevance = int(relevance)
                    except Exception:
                        relevance = 4
                    relevance = max(1, min(10, relevance))

                    parsed.append(
                        {
                            "article_number": i + 1,
                            "url": src.get("url") or "",
                            "title": title,
                            "summary": summary,
                            "sentiment": sentiment,
                            "relevance": relevance,
                        }
                    )
        except Exception:
            log.exception("summarize_items: fallback parse error")

    # post-process: ensure we have a non-empty summary for each item (if possible)
    for i, row in enumerate(parsed):
        if not row.get("summary") and i < len(batch):
            # fallback to original content (truncated) if available
            src_content = (batch[i].get("translated_text") or batch[i].get("content") or "")[:300]
            row["summary"] = f"(no summary provided by model, source content first 300 chars: {src_content})"

    return {"ok": True, "items": parsed, "latency_ms": latency_ms}

def _resolve_provider() -> str:
    """
    Resolve provider with precedence: environment -> settings -> default.
    Returns 'gemini' or 'openai' (defaults to openai for anything else).
    """
    val = (os.getenv("LLM_PROVIDER") or getattr(settings, "LLM_PROVIDER", "") or "openai").strip().lower()
    return val if val in {"gemini", "openai"} else "openai"

def _can_use_gemini() -> bool:
    """
    Check if Gemini can be used based on current quota limits.
    """
    if getattr(settings, "GEMINI_MAX_PER_DAY", 100) <= 0 or getattr(settings, "GEMINI_MAX_PER_MINUTE", 5) <= 0:
        return False
    return True