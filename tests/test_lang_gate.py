import pytest
from app.utils.lang import detect_language

def _english_gate(items):
    out = []
    for it in items:
        lang = (it.get("lang") or "").lower()
        title = it.get("title", "")
        # Block if explicitly non-English
        if lang and lang != "en":
            continue
        # Fallback detect from title if lang missing
        if not lang:
            try:
                if detect_language(title) != "en":
                    continue
            except Exception:
                continue
        out.append(it)
    return out

def test_english_gate_pass_and_block():
    items = [
        {"title": "TCS expands in AP", "lang": "en"},
        {"title": "比亚迪发布新车型", "lang": "zh"},
        {"title": "H-1B fee hike analysis", "lang": ""},  # falls back to title detection
    ]
    filtered = _english_gate(items)
    titles = [i["title"] for i in filtered]
    assert "TCS expands in AP" in titles
    assert "比亚迪发布新车型" not in titles
    # third item should not crash and likely passes as English