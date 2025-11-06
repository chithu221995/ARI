"""
Generic async retry decorator with rate limiting and exponential backoff.

Features:
- In-memory rate limiting using sliding window (calls per minute)
- Exponential backoff with jitter on failures
- Configurable max retries and delays
- Detailed logging per provider
- Thread-safe using asyncio primitives
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict, deque
from functools import wraps
from typing import Any, Callable, TypeVar, ParamSpec

import httpx
from app.core.incidents import record_incident, resolve_incident

log = logging.getLogger("ari.retry")

# Type hints for decorator
P = ParamSpec('P')
T = TypeVar('T')

# Global rate limiter state (per provider)
_rate_limiters: dict[str, deque[float]] = defaultdict(deque)
_rate_limiter_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


class RetryExhausted(Exception):
    """Raised when all retry attempts are exhausted."""
    pass


async def _wait_for_rate_limit(
    provider: str,
    max_per_minute: int,
    window_seconds: int = 60
) -> None:
    """
    Enforce rate limiting using sliding window algorithm.
    
    Args:
        provider: Provider name for tracking
        max_per_minute: Maximum calls allowed per minute
        window_seconds: Time window in seconds (default: 60)
    """
    async with _rate_limiter_locks[provider]:
        now = time.time()
        window_start = now - window_seconds
        
        # Remove timestamps outside the window
        queue = _rate_limiters[provider]
        while queue and queue[0] < window_start:
            queue.popleft()
        
        # If at limit, wait until oldest call expires
        if len(queue) >= max_per_minute:
            sleep_time = queue[0] - window_start
            if sleep_time > 0:
                log.warning(
                    f"Rate limit reached for {provider}: "
                    f"{len(queue)}/{max_per_minute} calls. "
                    f"Sleeping {sleep_time:.2f}s"
                )
                await asyncio.sleep(sleep_time)
                # Recurse to clean up and check again
                await _wait_for_rate_limit(provider, max_per_minute, window_seconds)
                return
        
        # Record this call
        queue.append(now)


def rate_limited_retry(
    provider: str = "unknown",
    max_retries: int = 3,
    base_delay: float = 1.5,
    max_delay: float = 60.0,
    max_per_minute: int = 5,
    jitter: float = 0.5,
    retry_on: tuple[type[Exception], ...] = (
        httpx.RequestError,
        httpx.TimeoutException,
        TimeoutError,
        ConnectionError,
    )
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator for async functions with rate limiting and exponential backoff.
    
    Args:
        provider: Provider name for logging and rate limiting
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Base delay in seconds for exponential backoff (default: 1.5)
        max_delay: Maximum delay between retries in seconds (default: 60)
        max_per_minute: Maximum calls per minute (default: 5)
        jitter: Random jitter to add/subtract in seconds (default: ±0.5)
        retry_on: Tuple of exception types to retry on
    
    Returns:
        Decorated async function with retry logic
        
    Example:
        @rate_limited_retry(provider="scrapingdog", max_retries=3)
        async def fetch_data(url: str) -> dict:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.json()
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None
            incident_recorded = False
            
            for attempt in range(max_retries + 1):
                try:
                    # Enforce rate limiting before each call
                    await _wait_for_rate_limit(provider, max_per_minute)
                    
                    # Execute the function
                    result = await func(*args, **kwargs)
                    
                    # Success - log and return
                    if attempt > 0:
                        log.info(
                            f"✓ {provider}: {func.__name__} succeeded on attempt {attempt + 1}"
                        )
                        
                        # Resolve any open incident for this provider
                        if incident_recorded:
                            await resolve_incident(
                                job_type=func.__name__,
                                provider=provider,
                                resolved_by="retry"
                            )
                    
                    return result
                    
                except retry_on as e:
                    last_exception = e
                    
                    # Record incident on first failure
                    if not incident_recorded:
                        await record_incident(
                            job_type=func.__name__,
                            error_message=f"{type(e).__name__}: {str(e)}",
                            provider=provider
                        )
                        incident_recorded = True
            
                    # If this was the last attempt, raise
                    if attempt >= max_retries:
                        log.error(
                            f"✗ {provider}: {func.__name__} failed after {max_retries + 1} attempts. "
                            f"Final error: {type(e).__name__}: {str(e)}"
                        )
                        raise RetryExhausted(
                            f"{provider}: All {max_retries + 1} attempts failed"
                        ) from e
                    
                    # Calculate exponential backoff with jitter
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    jitter_value = random.uniform(-jitter, jitter)
                    actual_delay = max(0, delay + jitter_value)
                    
                    log.warning(
                        f"⚠ {provider}: {func.__name__} failed on attempt {attempt + 1}/{max_retries + 1}. "
                        f"Error: {type(e).__name__}: {str(e)}. "
                        f"Retrying in {actual_delay:.2f}s..."
                    )
                    
                    await asyncio.sleep(actual_delay)
                    
                except Exception as e:
                    # Non-retryable exception - log and raise immediately
                    log.error(
                        f"✗ {provider}: {func.__name__} failed with non-retryable error: "
                        f"{type(e).__name__}: {str(e)}"
                    )
                    
                    # Record incident for non-retryable errors too
                    if not incident_recorded:
                        await record_incident(
                            job_type=func.__name__,
                            error_message=f"Non-retryable: {type(e).__name__}: {str(e)}",
                            provider=provider
                        )
                    
                    raise
    
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
            raise RuntimeError(f"{provider}: Unexpected retry loop exit")
        
        return wrapper
    return decorator


def get_rate_limiter_stats(provider: str | None = None) -> dict[str, Any]:
    """
    Get current rate limiter statistics.
    
    Args:
        provider: Optional provider name. If None, returns stats for all providers.
        
    Returns:
        Dictionary with rate limiter statistics
    """
    if provider:
        queue = _rate_limiters.get(provider, deque())
        now = time.time()
        recent_calls = sum(1 for ts in queue if ts > now - 60)
        
        return {
            "provider": provider,
            "calls_last_minute": recent_calls,
            "total_tracked_calls": len(queue),
            "oldest_call_age_seconds": now - queue[0] if queue else None
        }
    
    # Return stats for all providers
    stats = {}
    now = time.time()
    for prov, queue in _rate_limiters.items():
        recent_calls = sum(1 for ts in queue if ts > now - 60)
        stats[prov] = {
            "calls_last_minute": recent_calls,
            "total_tracked_calls": len(queue),
            "oldest_call_age_seconds": now - queue[0] if queue else None
        }
    
    return stats


async def reset_rate_limiter(provider: str | None = None) -> None:
    """
    Reset rate limiter state for a provider or all providers.
    
    Args:
        provider: Optional provider name. If None, resets all providers.
    """
    if provider:
        async with _rate_limiter_locks[provider]:
            _rate_limiters[provider].clear()
            log.info(f"Rate limiter reset for {provider}")
    else:
        for prov in list(_rate_limiters.keys()):
            async with _rate_limiter_locks[prov]:
                _rate_limiters[prov].clear()
        log.info("Rate limiters reset for all providers")


# Convenience decorators for common providers
def scrapingdog_retry(**kwargs: Any) -> Callable:
    """Retry decorator configured for Scrapingdog API."""
    return rate_limited_retry(
        provider="scrapingdog",
        max_retries=kwargs.get("max_retries", 3),
        base_delay=kwargs.get("base_delay", 2.0),
        max_per_minute=kwargs.get("max_per_minute", 5),
        **{k: v for k, v in kwargs.items() if k not in ["max_retries", "base_delay", "max_per_minute"]}
    )


def diffbot_retry(**kwargs: Any) -> Callable:
    """Retry decorator configured for Diffbot API."""
    return rate_limited_retry(
        provider="diffbot",
        max_retries=kwargs.get("max_retries", 3),
        base_delay=kwargs.get("base_delay", 1.5),
        max_per_minute=kwargs.get("max_per_minute", 5),  # Changed from 10 to 5
        **{k: v for k, v in kwargs.items() if k not in ["max_retries", "base_delay", "max_per_minute"]}
    )


def gemini_retry(**kwargs: Any) -> Callable:
    """Retry decorator configured for Gemini API."""
    return rate_limited_retry(
        provider="gemini",
        max_retries=kwargs.get("max_retries", 3),
        base_delay=kwargs.get("base_delay", 1.0),
        max_per_minute=kwargs.get("max_per_minute", 5),  # Changed from 15 to 5
        **{k: v for k, v in kwargs.items() if k not in ["max_retries", "base_delay", "max_per_minute"]}
    )


def openai_retry(**kwargs: Any) -> Callable:
    """Retry decorator configured for OpenAI API."""
    return rate_limited_retry(
        provider="openai",
        max_retries=kwargs.get("max_retries", 3),
        base_delay=kwargs.get("base_delay", 1.0),
        max_per_minute=kwargs.get("max_per_minute", 15),
        **{k: v for k, v in kwargs.items() if k not in ["max_retries", "base_delay", "max_per_minute"]}
    )


def sendgrid_retry(**kwargs: Any) -> Callable:
    """Retry decorator configured for SendGrid API."""
    return rate_limited_retry(
        provider="sendgrid",
        max_retries=kwargs.get("max_retries", 2),
        base_delay=kwargs.get("base_delay", 2.0),
        max_per_minute=kwargs.get("max_per_minute", 10),
        **{k: v for k, v in kwargs.items() if k not in ["max_retries", "base_delay", "max_per_minute"]}
    )