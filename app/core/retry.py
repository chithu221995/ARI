from __future__ import annotations
import os
import asyncio
import random
from typing import Callable, Tuple, Type, Any, Optional
import httpx

# Configurable defaults from environment
RETRY_ATTEMPTS: int = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_BASE_MS: int = int(os.getenv("RETRY_BASE_MS", "400"))
RETRY_FACTOR: float = float(os.getenv("RETRY_FACTOR", "2.0"))
RETRY_JITTER_MS: int = int(os.getenv("RETRY_JITTER_MS", "200"))

async def with_backoff(
    coro_fn: Callable[[], Any],
    *,
    attempts: int = RETRY_ATTEMPTS,
    base_ms: int = RETRY_BASE_MS,
    factor: float = RETRY_FACTOR,
    jitter_ms: int = RETRY_JITTER_MS,
    retry_on: Tuple[Type[BaseException], ...] = (httpx.RequestError,),
    logger: Optional[Any] = None,
    label: str = "",
):
    """
    Run `coro_fn()` with exponential backoff retries.

    Args:
        coro_fn: Zero-arg callable that returns an awaitable.
        attempts: Total attempts (first try + retries).
        base_ms: Base sleep in milliseconds.
        factor: Exponential growth factor per retry.
        jitter_ms: Max random jitter in milliseconds added to sleep.
        retry_on: Tuple of exception types that should trigger a retry.
        logger: Optional logger with .info/.warning/.error methods.
        label: Short label included in logs.

    Raises:
        The last exception if all attempts fail.
    """
    if logger:
        logger.info(f"[retry] start {label} attempts={attempts} base_ms={base_ms} factor={factor} jitter_ms={jitter_ms}")

    last_exc: Optional[BaseException] = None

    for attempt in range(1, max(1, attempts) + 1):
        try:
            return await coro_fn()
        except Exception as exc:
            last_exc = exc
            # If exception type not in retry_on, re-raise immediately
            if not isinstance(exc, retry_on):
                if logger:
                    logger.error(f"[retry] {label} non-retriable error on attempt {attempt}: {exc}")
                raise
            # If last attempt, re-raise
            if attempt >= attempts:
                if logger:
                    logger.error(f"[retry] {label} failed after {attempt} attempts: {exc}")
                raise
            # compute backoff with jitter
            backoff_ms = base_ms * (factor ** (attempt - 1))
            jitter = random.uniform(0, jitter_ms)
            sleep_s = (backoff_ms + jitter) / 1000.0
            if logger:
                logger.warning(f"[retry] {label} attempt={attempt} failed: {exc}; retrying in {sleep_s:.2f}s")
            await asyncio.sleep(sleep_s)

    # Should not reach here, but re-raise last exception if it does
    if last_exc:
        raise last_exc