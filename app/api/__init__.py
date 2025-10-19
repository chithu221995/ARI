from fastapi import APIRouter

router = APIRouter()

# include admin sub-routers (optional imports to avoid hard failures at import time)
try:
    from app.api.admin import email as email_admin
    router.include_router(email_admin.router, prefix="/email", tags=["admin:email"])
except Exception as e:
    print("[api] failed to include admin.email router:", e)

try:
    from app.api.admin import cache as cache_admin
    router.include_router(cache_admin.router, prefix="/cache", tags=["admin:cache"])
except Exception as e:
    print("[api] failed to include admin.cache router:", e)

try:
    from app.api.admin import jobs as jobs_admin
    router.include_router(jobs_admin.router, prefix="/jobs", tags=["admin:jobs"])
except Exception as e:
    print("[api] failed to include admin.jobs router:", e)

# makes package importable
__all__ = ["admin"]