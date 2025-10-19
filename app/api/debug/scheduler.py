from fastapi import APIRouter
from app.scheduler.runner import get_scheduler_state

router = APIRouter()

@router.get("/debug/scheduler")
async def debug_scheduler():
    return await get_scheduler_state()