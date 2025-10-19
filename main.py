# filepath: /Users/chitharanjan/ARI/main.py
from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from fastapi import FastAPI
import traceback

app = FastAPI(title="A.R.I. Engine")

# v1 routers
from app.api.v1 import brief as v1_brief
from app.api.v1 import summary as v1_summary

# admin sub-routers (each router should NOT include "/admin" in its own prefix)
from app.api.admin import email as admin_email
from app.api.admin import cache as admin_cache
from app.api.admin import jobs as admin_jobs

# mount routers
app.include_router(v1_brief.router)        # /api/v1/brief
app.include_router(v1_summary.router)      # /api/v1/summarize (GET+POST)

# mount admin subrouters under /admin
app.include_router(admin_email.router, prefix="/admin")   # /admin/email/brief
app.include_router(admin_cache.router, prefix="/admin")   # /admin/cache/...
app.include_router(admin_jobs.router, prefix="/admin")    # /admin/jobs/...

@app.get("/debug/routes")
def debug_routes():
    return [{"methods": list(r.methods), "path": r.path} for r in app.router.routes]

@app.get("/")
async def root():
    return {"status": "ok"}

@app.get("/health")
async def health():
    return {"status": "ok"}