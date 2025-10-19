from langdetect import detect

def detect_language(text: str) -> str:
    try:
        return detect(text or "") or "en"
    except Exception:
        return "en"