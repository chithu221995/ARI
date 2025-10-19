from fastapi import APIRouter

router = APIRouter()

# include subrouters (each subrouter should NOT use '/admin' in its own prefix)
try:
    from .email import router as email_router
    router.include_router(email_router)
except Exception as e:
    print(f"[api] failed to include admin.email router: {e}")

try:
    from .jobs import router as jobs_router
    router.include_router(jobs_router)
except Exception as e:
    print(f"[api] failed to include admin.jobs router: {e}")