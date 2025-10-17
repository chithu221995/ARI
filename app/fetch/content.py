import trafilatura
from datetime import datetime
from app.utils.lang import detect_language, translate_to_english

async def fetch_article_text(url: str) -> dict:
    try:
        downloaded = trafilatura.fetch_url(url)
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False, include_formatting=False) if downloaded else ""
        text = text or ""
    except Exception:
        text = ""

    lang = detect_language(text)
    # Skipping actual translation for now — keep translated only if already English
    translated = text if lang == "en" else ""
    return {
        "lang": lang,
        "translated_text": translated,
        "chars": len(translated),
        "fetched_at": datetime.utcnow().isoformat() + "Z"
    }