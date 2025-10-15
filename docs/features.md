# Features — A.R.I.

## Prototype
- Input tickers (≤10) and email through a simple form.
- Fetch recent headlines via NewsAPI + selected RSS feeds.
- Fetch corporate filings from BSE / NSE.
- AI-generated summaries: 3 bullets + “Why it matters” + sentiment tag.
- Daily plain-text email with separate *News* and *Filings* sections.
- Basic logging of token usage, latency, and email status.

## MVP
- Web dashboard: select tickers, enter email, choose delivery time (09 : 30 – 21 : 30 IST).
- Validation: no duplicate emails; max 10 tickers per user.
- Cached summaries shared across users to control API costs.
- Improved summarization (topic de-duplication + sentiment accuracy).
- HTML email template with feedback links.
- Feedback form: 0–5 stars for News / Filings / Overall + comments.
- Daily and weekly digests.
- KPI logging (cost, engagement, reliability).
- Admin dashboard: Cost | User | Reliability tabs with hide toggles.

## Post-MVP
- Enhanced filings filters (exclude AR / QR / transcripts).
- Advanced email design and sentiment icons.
- Error-alerting (API failures > 5 %, latency spikes).
- Visual KPI dashboard with graphs.
- Optional migration to Postgres + cloud deployment.
- Broker integrations (Zerodha / Angel / Groww) and analytics dashboard.
