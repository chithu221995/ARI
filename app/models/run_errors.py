"""
Run errors and incidents tracking for MTTR metrics.
Tracks failures and their resolution for incident duration analysis.
"""
from __future__ import annotations
from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.ext.hybrid import hybrid_property


class RunError:
    """
    Track job/vendor failures and their resolution for MTTR metrics.

    Lifecycle:
    1. created_at: When failure first detected
    2. resolved_at: When successfully recovered (next success for same job/provider)
    3. duration_minutes: Computed from (resolved_at - created_at)

    Classification:
    - Minor: duration < 30 minutes
    - Major: duration >= 30 minutes
    - Unresolved: resolved_at is NULL
    """

    __tablename__ = "run_errors"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # What failed
    job_type = Column(String, nullable=False)  # e.g., "fetch", "extract", "summarize"
    ticker = Column(String, nullable=True)      # Optional ticker context
    provider = Column(String, nullable=True)    # e.g., "diffbot", "gemini", "scrapingdog"
    event = Column(String, nullable=True)       # e.g., "extract", "summarize", "fetch"

    # Failure details
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime, nullable=False)  # When failure occurred

    # Resolution tracking
    resolved_at = Column(DateTime, nullable=True)  # When recovered (NULL = unresolved)
    resolved_by = Column(String, nullable=True)    # What resolved it (e.g., "retry", "manual", "auto")

    @hybrid_property
    def duration_minutes(self) -> Optional[float]:
        """
        Compute incident duration in minutes.
        Returns None if not yet resolved.
        """
        if self.resolved_at is None or self.created_at is None:
            return None

        delta = self.resolved_at - self.created_at
        return delta.total_seconds() / 60.0

    @hybrid_property
    def severity(self) -> str:
        """
        Classify incident severity based on duration.
        - "unresolved": Not yet fixed
        - "minor": < 30 minutes
        - "major": >= 30 minutes
        """
        if self.resolved_at is None:
            return "unresolved"

        duration = self.duration_minutes
        if duration is None:
            return "unresolved"

        return "minor" if duration < 30 else "major"

    def __repr__(self) -> str:
        status = f"resolved in {self.duration_minutes:.1f}m" if self.resolved_at else "unresolved"
        return f"<RunError({self.job_type}/{self.provider or 'N/A'} - {status})>"