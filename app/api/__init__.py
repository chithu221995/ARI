from __future__ import annotations
from fastapi import APIRouter
from app.api.admin import jobs as jobs_admin
from app.api.admin import metrics as admin_metrics

router = APIRouter()

# Include admin routers
router.include_router(jobs_admin.router)
router.include_router(admin_metrics.router)

# Admin namespaces
router.include_router(jobs_admin.router, prefix="/admin", tags=["admin:jobs"])
router.include_router(admin_metrics.router, prefix="/admin", tags=["admin:metrics"])

# Public (non-admin) aliases
router.include_router(jobs_admin.router, prefix="", tags=["jobs"])        # /jobs/...
router.include_router(admin_metrics.router, prefix="", tags=["metrics"])  # /metrics/...