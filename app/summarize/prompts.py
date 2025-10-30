import logging
import re as _re

log = logging.getLogger("ari.summarize.prompts")

__all__ = ["SYSTEM_PROMPT", "_parse_plain_fallback"]

# Strong system prompt for the LLM (UTC/explicit output contract)
SYSTEM_PROMPT = (
    "You are a news summarization engine for busy portfolio holders. "
    "Write short, impactful summaries with no meta language. Do NOT use phrases like "
    "'this article', 'the article', 'this report', 'according to the article', or similar meta-prefaces. "
    "Lead with the key takeaway and keep tone factual and concise.\n\n"
    "You will receive multiple articles for a single company. For each article, output a JSON item with:\n"
    '- "article_number": index starting from 1\n'
    '- "url": same as provided\n'
    '- "title": concise English title\n'
    '- "summary": one cohesive 4–5 sentence paragraph on concrete business impact (no bullets)\n'
    '- "sentiment": exactly one of {Positive, Neutral, Negative}\n'
    '- "relevance": integer 1–10, relative importance within THIS batch (10 = most impactful to shareholder value). '
    "No two items share the same score EXCEPT that multiple items may be scored 1 when not meaningfully related.\n\n"
    "Rules:\n"
    "- Focus on operations, strategy, financials, products, customers, regulation, leadership. Ignore PR fluff and stock tips.\n"
    "- Return one item per input article (no skipping/merging), preserve input order.\n"
    "- If an article is thin on text, infer from title/URL and assign lower relevance.\n"
    "- If an article is not meaningfully related to the company and therefore shareholder value, assign relevance = 1.\n"
    "- The entire output MUST be a SINGLE valid JSON object with no preface, no trailing text, and no code fences. "
    '  Example: { "items": [ { "article_number": 1, "url": "...", "title": "...", "summary": "...", "sentiment": "Positive", "relevance": 7 }, ... ] }\n'
    "- No text before or after the JSON."
)

def _parse_plain_fallback(text: str):
    """Very permissive plain-text fallback parser.
    Expected loose blocks like:
      Article 1: ...\nTitle: ...\nSummary: ...\nSentiment: ...\nRelevance: ...
    Returns a list of dicts with keys: title, summary, sentiment, relevance.
    """
    if not text:
        return []
    items = []
    # Split on double newlines or article markers
    blocks = [b.strip() for b in _re.split(r"\n\s*\n+|(?=\bArticle\s+\d+:)" , text) if b.strip()]
    for b in blocks:
        title = None
        summary = None
        sentiment = None
        relevance = None
        m = _re.search(r"^\s*Title:\s*(.+)$", b, flags=_re.I | _re.M)
        if m:
            title = m.group(1).strip()
        m = _re.search(r"^\s*Summary:\s*(.+?)(?:\n\w+:|$)", b, flags=_re.I | _re.S)
        if m:
            summary = m.group(1).strip()
        m = _re.search(r"^\s*Sentiment:\s*(Positive|Neutral|Negative)\b", b, flags=_re.I | _re.M)
        if m:
            sentiment = m.group(1).capitalize()
        m = _re.search(r"^\s*Relevance:\s*([0-9]+)\b", b, flags=_re.I | _re.M)
        if m:
            try:
                relevance = int(m.group(1))
            except Exception:
                relevance = None
        if any([title, summary, sentiment, relevance is not None]):
            items.append({
                "title": title or "",
                "summary": summary or "",
                "sentiment": sentiment or "Neutral",
                "relevance": relevance if relevance is not None else None,
            })
    return items