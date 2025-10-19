import httpx
import trafilatura
from datetime import datetime
from typing import Dict, Any

from app.utils.lang import detect_language


def fetch_article_text(url: str) -> Dict[str, Any]:
    """
    Fetch page and extract text using trafilatura.
    Always return a dict with keys:
      - lang: detected language (or "unknown")
      - translated_text: text only if detected as English, else ""
      - content: raw extracted text or ""
      - chars: length of extracted text
      - fetched_at: ISO timestamp (UTC)
    Do not default non-English to "en".
    """
    text = ""
    try:
        resp = httpx.get(url, timeout=10)
        if resp is not None and resp.status_code == 200:
            html = resp.text or ""
            try:
                extracted = trafilatura.extract(html) or ""
            except Exception:
                extracted = ""
            text = extracted or ""
    except Exception:
        text = ""

    try:
        lang = detect_language(text or "")
    except Exception:
        lang = "unknown"

    translated = text if lang == "en" else ""

    return {
        "lang": lang,
        "translated_text": translated,
        "content": text or "",
        "chars": len(text or ""),
        "fetched_at": datetime.utcnow().isoformat() + "Z",
    }