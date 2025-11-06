from __future__ import annotations
from fastapi import APIRouter

# import child routers (these should define `router` as APIRouter)
from . import jobs, cache_diag, metrics, email  # ensure all modules are imported

admin = APIRouter(prefix="/admin", tags=["admin"])

# child routers have their own prefixes:
# jobs.router -> /jobs, email.router -> /email, cache_diag.router -> /cache
admin.include_router(jobs.router)
admin.include_router(email.router)
admin.include_router(cache_diag.router)
admin.include_router(metrics.router)

__all__ = ["admin"]