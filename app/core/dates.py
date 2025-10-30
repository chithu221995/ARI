from datetime import datetime, timezone

def now_iso() -> str:
    """UTC ISO8601 without microseconds, Z-suffix."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")