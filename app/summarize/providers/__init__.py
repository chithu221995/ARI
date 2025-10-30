from __future__ import annotations
import os
import importlib
from typing import Callable

# Read configured provider name (lowercase) and map to module
def get_provider() -> Callable[..., str]:
    """
    Returns a callable `summarize(payload_text, system_prompt, output_instructions, max_tokens, temperature)`
    from the configured provider module. Falls back to 'openai' if unknown.
    """
    prov = (os.getenv("LLM_PROVIDER") or "openai").strip().lower()
    if prov not in {"gemini", "openai"}:
        prov = "openai"
    try:
        mod = importlib.import_module(f"app.summarize.providers.{prov}")
        return getattr(mod, "summarize")
    except Exception:
        # best-effort fallback to openai
        mod = importlib.import_module("app.summarize.providers.openai")
        return getattr(mod, "summarize")