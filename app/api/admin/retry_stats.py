"""Admin endpoint for retry and rate limiter statistics."""
from fastapi import APIRouter
from app.core.retry_utils import get_rate_limiter_stats, reset_rate_limiter

router = APIRouter(tags=["admin", "monitoring"])


@router.get("/stats")
async def get_retry_stats(provider: str | None = None):
    """
    Get rate limiter statistics for providers.
    
    Args:
        provider: Optional provider name. If not provided, returns stats for all.
        
    Returns:
        Rate limiter statistics
    """
    stats = get_rate_limiter_stats(provider)
    return {
        "ok": True,
        "stats": stats
    }


@router.post("/reset")
async def reset_retry_stats(provider: str | None = None):
    """
    Reset rate limiter state.
    
    Args:
        provider: Optional provider name. If not provided, resets all.
        
    Returns:
        Confirmation message
    """
    await reset_rate_limiter(provider)
    return {
        "ok": True,
        "message": f"Rate limiter reset for {provider or 'all providers'}"
    }