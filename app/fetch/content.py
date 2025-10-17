import trafilatura
from datetime import datetime
from app.utils.lang import detect_language

async def fetch_article_text(url: str) -> dict:
    try:
        downloaded = trafilatura.fetch_url(url)
        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            include_formatting=False,
            no_fallback=True
        ) if downloaded else ""
        text = text or ""
    except Exception:
        text = ""

    lang = detect_language(text)
    print(f"[content] {url} -> lang={lang}, chars={len(text)}")

    # Cheap guard: treat very short extracts as empty so callers skip them.
    # Prototype: block non-English content by returning empty translated_text.
    if len(text) < 300:
        translated = ""
    elif lang != "en":
        translated = ""  # block non-English in prototype
    else:
        translated = text

    return {
        "lang": lang,
        "translated_text": translated,
        "chars": len(translated),
        "fetched_at": datetime.utcnow().isoformat() + "Z"
    }