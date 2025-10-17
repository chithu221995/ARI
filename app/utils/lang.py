from langdetect import detect

def detect_language(text: str) -> str:
    try:
        return detect(text or "") or "en"
    except Exception:
        return "en"

def translate_to_english(text: str, src_lang: str) -> str:
    # Prototype: we do NOT perform translation in this build.
    # Only English input is allowed downstream; non-English inputs return empty string
    # to force callers to skip or handle them. Replace with real translator later.
    return text if (src_lang or "").lower().startswith("en") else ""