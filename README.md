# A.R.I. — Asset Relevance Intelligence
A small service that fetches news, summarizes relevance per asset, and emails a daily brief.

## Quickstart
1. Python 3.11+ recommended
2. Create venv and install:
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
3. Copy and populate env:
   cp .env.example .env
   edit .env and fill keys (see Environment section)
4. Run dev server:
   uvicorn main:app --reload

## Endpoints
- GET /health
- GET /debug/routes
- POST /api/v1/brief
- GET/POST /api/v1/summarize
- POST /admin/email/brief
- POST /admin/run/prefetch
- POST /admin/run/summarize
- GET /admin/cache/stats
- GET /admin/jobs/debug/status

## Scheduler
- Scheduler reads CRON_PREFETCH and CRON_SUMMARIZE (interpreted in Asia/Kolkata / IST).
- Jobs start on FastAPI startup and stop on shutdown.

## Email
- Uses SendGrid when EMAIL_PROVIDER=sendgrid.
- Requires SENDGRID_API_KEY and EMAIL_FROM (verified sender).
- Dry-run option available to preview email body.

## Testing
Run tests:
pytest -q

## Environment
Copy .env.example to .env and fill in secrets before running. Key env vars:
- NEWS_API_KEY: News API key for fetching articles.
- OPENAI_API_KEY: OpenAI (or compatible) key for summarization.
- SCHEDULE_TICKERS: Comma-separated tickers to schedule.
- PREFETCH_CONCURRENCY / STAGGER_*: Prefetch pacing configuration.
- RETRY_*: Retry/backoff configuration.
- CRON_PREFETCH / CRON_SUMMARIZE: Cron schedules (IST) for jobs.
- SUMMARY_DRY_RUN: Whether summaries default to dry-run.
- EMAIL_PROVIDER / SENDGRID_API_KEY / EMAIL_FROM / EMAIL_TO: Email configuration.
- CACHE_DB / CACHE_TTL_DAYS: Local cache DB and TTL.
