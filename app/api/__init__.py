from fastapi import APIRouter
from app.api.admin import jobs as jobs_admin
from app.api.admin import email as email_admin

router = APIRouter()
# Admin namespaces
router.include_router(jobs_admin.router, prefix="/admin", tags=["admin:jobs"])
router.include_router(email_admin.router, prefix="/admin", tags=["admin:email"])

# Public (non-admin) aliases
router.include_router(jobs_admin.router, prefix="", tags=["jobs"])        # /jobs/...
router.include_router(email_admin.router, prefix="", tags=["email"])      # /email/...