"""Feedback token utilities for email tracking."""
from __future__ import annotations
import time
import os
from typing import Dict, Any
from itsdangerous import URLSafeSerializer, BadSignature

from app.core import settings

# Initialize serializer for signing feedback tokens
_feedback_secret = getattr(settings, "FEEDBACK_SIGNING_SECRET", None) or os.getenv("FEEDBACK_SIGNING_SECRET", "change-me-in-production")
serializer = URLSafeSerializer(_feedback_secret, salt="feedback")


def make_feedback_token(email: str, email_log_id: int, email_sent_at: str) -> str:
    """
    Create a signed token containing email and log ID for feedback tracking.

    Args:
        email: User's email address
        email_log_id: ID from email_logs table
        email_sent_at: ISO timestamp when email was sent

    Returns:
        Signed token string
    """
    return serializer.dumps({
        "e": email,
        "log": email_log_id,
        "sent": email_sent_at,
        "t": int(time.time())
    })


def parse_feedback_token(token: str) -> Dict[str, Any]:
    """
    Parse and verify a feedback token.

    Args:
        token: Signed token string

    Returns:
        Dictionary with email, log, sent timestamp, and token creation timestamp

    Raises:
        BadSignature: If token is invalid or tampered with
    """
    return serializer.loads(token)