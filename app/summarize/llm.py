import os
import time
import json
from typing import List, Dict
from dotenv import load_dotenv
import httpx

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

SYSTEM_PROMPT = (
    "You are a financial assistant. Summarize provided article texts. "
    "Return STRICT JSON (no markdown, no prose) with this schema: "
    '{"items":[{"title":str,"url_id":str,"bullets":[str,str,str],"why_it_matters":str,"sentiment":"Positive|Neutral|Negative"}]} '
    "Do not invent URLs or titles—reuse exactly what is provided. Use url_id tokens exactly as given."
)

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-4o-mini"

def _estimate_tokens(text: str) -> int:
    # Rough estimate: 1 token ≈ 4 chars
    return max(1, len(text) // 4)

async def summarize_items(items: List[Dict], *, ticker: str) -> Dict:
    """
    Expect each item to have: title, url, text (text is the extracted/translated content).
    Returns strict JSON-shaped dict: { ticker, items: [...], token_usage, latency_ms }
    """
    start = time.time()
    token_usage = {"input": 0, "output": 0, "total": 0}
    results: List[Dict] = []

    if not OPENAI_API_KEY:
        print("Missing OPENAI_API_KEY, returning fallback summaries.")
        out = [
            {"title": i.get("title", ""), "url": i.get("url", ""), "bullets": [], "why_it_matters": "", "sentiment": "Neutral"}
            for i in items
        ]
        return {"ticker": ticker, "items": out, "token_usage": token_usage, "latency_ms": int((time.time() - start) * 1000)}

    # Build payload items; do NOT send raw URLs — send url_id tokens and keep mapping locally.
    payload_items = []
    placeholder_map = {}  # url_id -> real url
    for idx, i in enumerate(items):
        uid = f"URL#{idx+1}"
        placeholder_map[uid] = i.get("url", "") or ""
        payload_items.append({
            "title": i.get("title", "") or "",
            "url_id": uid,
            "text": (i.get("text") or i.get("translated_text") or i.get("content") or "")[:8000]
        })

    # Compact JSON user payload
    user_json = json.dumps({"items": payload_items}, ensure_ascii=False)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_json},
    ]

    req = {
        "model": MODEL,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 700,
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}

    print(f"Summarizing {len(payload_items)} items …")
    print(f"Calling OpenAI for {len(payload_items)} items...")
    print(f"API key starts with: {os.getenv('OPENAI_API_KEY')[:10]}...")

    input_tokens = _estimate_tokens(user_json) + _estimate_tokens(SYSTEM_PROMPT)
    output_text = ""
    parsed_items = []

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(OPENAI_URL, json=req, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"OpenAI API error {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        output_text = data["choices"][0]["message"]["content"]

        # Try strict JSON parse
        try:
            obj = json.loads(output_text)
            parsed_items = obj.get("items", []) if isinstance(obj, dict) else []
        except Exception:
            print("LLM JSON parse failed; will fall back to neutral entries while preserving urls/titles")
            parsed_items = []

        # token accounting estimates
        output_tokens = _estimate_tokens(output_text)
        token_usage["input"] += input_tokens
        token_usage["output"] += output_tokens
        token_usage["total"] += input_tokens + output_tokens

        # If OpenAI returns usage, log it
        usage = data.get("usage")
        if usage:
            print(f"OpenAI usage: input={usage.get('prompt_tokens')}, output={usage.get('completion_tokens')}")
        print(f"{ticker}: {input_tokens + output_tokens} tokens, est ${(input_tokens + output_tokens) / 1_000_000 * 0.15:.5f}")

    except Exception as e:
        print(f"OpenAI call failed: {e}")
        # keep parsed_items as empty to trigger neutral fallbacks below
        parsed_items = []
        # conservative token accounting: count input only
        token_usage["input"] += input_tokens
        token_usage["output"] += 0
        token_usage["total"] += input_tokens

    # Post-process: produce 1:1 output in input order, preserving original title/url when model misses them
    out: List[Dict] = []
    for idx, it in enumerate(items):
        p = parsed_items[idx] if idx < len(parsed_items) and isinstance(parsed_items[idx], dict) else {}
        title = p.get("title") or it.get("title", "")
        url_id = p.get("url_id") or p.get("url") or ""  # accept url_id or url fields returned
        url = placeholder_map.get(url_id) if url_id in placeholder_map else (it.get("url", "") or "")
        bullets = p.get("bullets") or []
        why = p.get("why_it_matters") or ""
        sentiment = (p.get("sentiment") or "").capitalize()
        sentiment = sentiment if sentiment in {"Positive", "Neutral", "Negative"} else "Neutral"
        out.append({
            "title": title,
            "url": url,
            "bullets": bullets[:3],
            "why_it_matters": why,
            "sentiment": sentiment
        })

    latency_ms = int((time.time() - start) * 1000)
    return {"ticker": ticker, "items": out, "token_usage": token_usage, "latency_ms": latency_ms}