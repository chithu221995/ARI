import os
import time
import json
from typing import List, Dict
from dotenv import load_dotenv
import httpx

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

SYSTEM_PROMPT = (
    "You are a financial assistant. All inputs are now in English. Produce concise bullets and sentiment; do not restate headlines verbatim. "
    "For each announcement, generate 2–3 concise bullet points, a 1–2 sentence explanation of why it matters for investors, and a sentiment (Positive, Neutral, Negative). "
    "Be brief and factual. Use only the provided info."
)

OPENAI_URL = "https://api.openai.com/v1/chat/completions"
MODEL = "gpt-4o-mini"

def _estimate_tokens(text: str) -> int:
    # Rough estimate: 1 token ≈ 4 chars
    return max(1, len(text) // 4)

async def summarize_items(items: List[Dict], *, ticker: str) -> Dict:
    import asyncio
    start = time.time()
    results = []
    token_usage = {"input": 0, "output": 0, "total": 0}
    if not OPENAI_API_KEY:
        print("Missing OPENAI_API_KEY, returning fallback summaries.")
        for item in items:
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "bullets": [],
                "why_it_matters": "",
                "sentiment": "Neutral"
            })
        return {
            "ticker": ticker,
            "items": results,
            "token_usage": token_usage,
            "latency_ms": int((time.time() - start) * 1000)
        }

    # Only summarize items with summary_allowed (default True when missing)
    to_summarize = [item for item in items if item.get("summary_allowed", True)]
    print(f"Summarizing {len(to_summarize)} items …")
    batches = [to_summarize[i:i+5] for i in range(0, len(to_summarize), 5)]
    async with httpx.AsyncClient(timeout=30) as client:
        for batch in batches:
            user_content = "\n".join(
                f"Text: {item.get('translated_text') or item.get('content') or ''}\nSource: {item.get('source','')}\nCategory: {item.get('category','')}\nPublished: {item.get('published_at','')}\nURL: {item.get('url','')}"
                for item in batch if item.get('translated_text') or item.get('content')
            )
            if not user_content.strip():
                continue

            messages = [
                {"role": "system", "content":
                 "You are a financial assistant. Return ONLY valid JSON with this exact shape: {\"items\": [{\"title\": str, \"bullets\": [str, str, str], \"why_it_matters\": str, \"sentiment\": \"Positive\"|\"Neutral\"|\"Negative\"}]}. No prose, no extra keys."
                },
                {"role": "user", "content":
                 "Summarize each input item into 2-3 bullets, a brief 'why_it_matters', and sentiment. Use only the provided text. Input items follow:\n\n" + user_content
                }
            ]

            payload = {
                "model": MODEL,
                "messages": messages,
                "temperature": 0.2,
                "max_tokens": 700,
                "response_format": {"type": "json_object"}
            }
            headers = {
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            }

            print(f"Calling OpenAI for {len(batch)} items...")
            print(f"API key starts with: {os.getenv('OPENAI_API_KEY')[:10]}...")
            # estimate input tokens (we'll add to totals after getting output size)
            input_tokens = _estimate_tokens(str(messages))
            try:
                resp = await client.post(OPENAI_URL, json=payload, headers=headers)
                elapsed = int((time.time() - start) * 1000)
                if resp.status_code != 200:
                    raise Exception(f"OpenAI API error {resp.status_code}")
                data = resp.json()
                output_text = data["choices"][0]["message"]["content"]

                # Try to parse strict JSON returned by the model
                try:
                    obj = json.loads(output_text)
                    raw_items = obj.get("items", []) if isinstance(obj, dict) else []
                except Exception:
                    raw_items = []

                # compute token usage now that we have output_text
                output_tokens = _estimate_tokens(output_text)
                token_usage["input"] += input_tokens
                token_usage["output"] += output_tokens
                token_usage["total"] += input_tokens + output_tokens

                # If OpenAI returns usage in payload, print it
                usage = data.get("usage")
                if usage:
                    print(f"OpenAI usage: input={usage.get('prompt_tokens')}, output={usage.get('completion_tokens')}")
                print(f"{ticker}: {input_tokens + output_tokens} tokens, est ${(input_tokens + output_tokens) / 1_000_000 * 0.15:.5f}")

                # Build lookup maps by normalized title/url to preserve urls
                def norm(s): return (s or "").strip().lower()
                by_title = {norm(i.get("title")): i for i in batch}
                by_url = {norm(i.get("url")): i for i in batch}

                parsed = []
                for ri in raw_items:
                    t = ri.get("title", "")
                    bullets = [b for b in (ri.get("bullets") or []) if isinstance(b, str)]
                    why = ri.get("why_it_matters", "")
                    sent = (ri.get("sentiment", "Neutral") or "").capitalize()
                    sent = sent if sent in {"Positive", "Neutral", "Negative"} else "Neutral"
                    url = ""
                    m = by_title.get(norm(t))
                    if m:
                        url = m.get("url", "")
                    parsed.append({
                        "title": t,
                        "url": url,
                        "bullets": bullets[:3],
                        "why_it_matters": why,
                        "sentiment": sent
                    })

                # If parsing produced nothing, fallback will be applied below
                results.extend(parsed)

            except Exception as e:
                print(f"LLM summary error: {e}")
                for item in batch:
                    results.append({
                        "title": item.get("title", ""),
                        "url": item.get("url", ""),
                        "bullets": [],
                        "why_it_matters": "",
                        "sentiment": "Neutral"
                    })

    summarized_titles = {r["title"] for r in results}
    for item in items:
        if item.get("title", "") not in summarized_titles:
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "bullets": [],
                "why_it_matters": "",
                "sentiment": "Neutral"
            })
    latency_ms = int((time.time() - start) * 1000)
    return {
        "ticker": ticker,
        "items": results,
        "token_usage": token_usage,
        "latency_ms": latency_ms
    }