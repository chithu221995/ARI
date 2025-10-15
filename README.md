# A.R.I. — Asset Relevance Intelligence

A.R.I. is a personalized financial-intelligence platform. Users add their portfolio tickers and receive daily/weekly emails with concise, AI-generated summaries of **news** and **exchange filings** (BSE/NSE), including a quick **sentiment** tag.

## Goals
- Cut portfolio-monitoring time by ~60%
- Deliver only **relevant** updates (holdings-aware)
- Provide clear, scannable briefs with links to sources

## Current Status
- ✅ Phase 0: Environment setup (FastAPI, venv, `.env`, Git)
- ✅ Phase 1: Product docs + roadmap + issues
- ⏳ Phase 2: Prototype (in progress)

## Tech
- **API:** FastAPI (Python)
- **Data:** SQLite (MVP; Postgres later)
- **LLM:** OpenAI API
- **News sources:** NewsAPI + RSS (ET, Moneycontrol)
- **Filings:** BSE/NSE public announcement endpoints
- **Email:** SendGrid (MVP)

## Run Locally
```bash
# 1) Clone and enter
git clone https://github.com/<your-username>/ARI.git
cd ARI

# 2) Python venv
python3 -m venv venv
source venv/bin/activate

# 3) Install deps
pip install -r requirements.txt

# 4) Env vars
cp .env.example .env
# add your OPENAI_API_KEY and NEWS_API_KEY

# 5) Run API
uvicorn main:app --reload
# http://127.0.0.1:8000/health
