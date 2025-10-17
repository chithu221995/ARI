from langdetect import detect

def detect_language(text: str) -> str:
    try:
        return detect(text or "") or "en"
    except Exception:
        return "en"

def translate_to_english(text: str, src_lang: str) -> str:
    # No-op for prototype; skipping translation by design.
    return text or ""