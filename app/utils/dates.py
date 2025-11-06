"""
Date utilities for IST timezone handling and date range validation.
"""

from datetime import datetime, timedelta, timezone
import re

# Indian Standard Time (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


def normalize_to_ist_day(dt: datetime) -> datetime:
    """
    Snap any UTC datetime to the same day boundary in IST (00:00).
    
    Args:
        dt: Datetime to normalize (if naive, assumes UTC)
        
    Returns:
        Datetime at midnight IST for the same calendar day
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    ist_dt = dt.astimezone(IST)
    return ist_dt.replace(hour=0, minute=0, second=0, microsecond=0)


def normalize_to_ist_day_start(dt: datetime) -> datetime:
    """
    Snap any UTC datetime to the START of the same day in IST (00:00).
    Use this for 'start' dates.
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    ist_dt = dt.astimezone(IST)
    return ist_dt.replace(hour=0, minute=0, second=0, microsecond=0)


def normalize_to_ist_day_end(dt: datetime) -> datetime:
    """
    Snap any UTC datetime to the END of the same day in IST (23:59:59).
    Use this for 'end' dates.
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    ist_dt = dt.astimezone(IST)
    return ist_dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def enforce_date_range(
    start: datetime, 
    end: datetime, 
    max_days: int = 365
) -> tuple[datetime, datetime]:
    """
    Cap the date range to max_days and ensure start <= end.
    
    Args:
        start: Start datetime
        end: End datetime
        max_days: Maximum allowed days in range (default: 365)
        
    Returns:
        Tuple of (start, end) with validated range
        
    Example:
        >>> start = datetime(2024, 1, 1)
        >>> end = datetime(2025, 12, 31)  # 730 days
        >>> start, end = enforce_date_range(start, end, max_days=365)
        >>> # Returns (2024-01-01, 2025-01-01) - capped to 365 days
    """
    # Swap if start > end
    if start > end:
        start, end = end, start
    
    # Calculate days between
    delta = (end - start).days
    
    # Cap to max_days if exceeded
    if delta > max_days:
        end = start + timedelta(days=max_days)
    
    return start, end


def parse_date_param(
    date_str: str | None, 
    default: datetime | None = None
) -> datetime | None:
    """
    Parse date string (YYYY-MM-DD) to datetime with IST timezone.
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD)
        default: Default value if parsing fails or date_str is None
        
    Returns:
        Datetime with IST timezone at midnight, or default
        
    Example:
        >>> dt = parse_date_param("2025-11-03")
        >>> # Returns datetime(2025, 11, 3, 0, 0, 0, tzinfo=IST)
    """
    if not date_str:
        return default
    
    try:
        # Parse as naive datetime
        dt = datetime.fromisoformat(date_str)
        # Set to IST timezone at midnight
        return dt.replace(tzinfo=IST, hour=0, minute=0, second=0, microsecond=0)
    except (ValueError, TypeError):
        return default


def get_default_date_range(days: int = 30) -> tuple[datetime, datetime]:
    """
    Get default date range in IST timezone.
    
    Args:
        days: Number of days to look back (default: 30)
        
    Returns:
        Tuple of (start, end) datetimes in IST at midnight
        
    Example:
        >>> start, end = get_default_date_range(7)  # Last 7 days
    """
    now = datetime.now(IST)
    end = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    return start, end


def parse_relative_timestamp(relative_time: str, reference_time: datetime = None) -> datetime | None:
    """
    Convert relative timestamps like "6 hours ago", "2 days ago" to datetime.
    
    Args:
        relative_time: String like "6 hours ago", "2 days ago", etc.
        reference_time: Reference datetime (defaults to now)
        
    Returns:
        Datetime object or None if parsing fails
    """
    if not relative_time:
        return None
    
    if reference_time is None:
        reference_time = datetime.utcnow()
    
    # Normalize input
    text = relative_time.lower().strip()
    
    # Pattern: <number> <unit> ago
    # Examples: "6 hours ago", "2 days ago", "1 week ago"
    match = re.match(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago', text)
    
    if not match:
        return None
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # Convert to timedelta
    if unit == 'second':
        delta = timedelta(seconds=value)
    elif unit == 'minute':
        delta = timedelta(minutes=value)
    elif unit == 'hour':
        delta = timedelta(hours=value)
    elif unit == 'day':
        delta = timedelta(days=value)
    elif unit == 'week':
        delta = timedelta(weeks=value)
    elif unit == 'month':
        delta = timedelta(days=value * 30)  # Approximate
    elif unit == 'year':
        delta = timedelta(days=value * 365)  # Approximate
    else:
        return None
    
    return reference_time - delta


import re
from datetime import datetime

def parse_relative_age_to_hours(relative_time: str) -> float | None:
    """
    Convert relative timestamps like "6 hours ago", "2 days ago", "30 minutes ago" to hours.
    
    Args:
        relative_time: String like "6 hours ago", "2 days ago", "30 minutes ago", "45 seconds ago"
        
    Returns:
        Age in hours as float, or None if parsing fails
        
    Examples:
        "6 hours ago" -> 6.0
        "2 days ago" -> 48.0
        "30 minutes ago" -> 0.5
        "45 seconds ago" -> 0.0125
    """
    if not relative_time:
        return None
    
    # Normalize input
    text = relative_time.lower().strip()
    
    # Pattern: <number> <unit> ago
    match = re.match(r'(\d+(?:\.\d+)?)\s+(second|minute|hour|day|week|month|year)s?\s+ago', text)
    
    if not match:
        return None
    
    value = float(match.group(1))
    unit = match.group(2)
    
    # Convert everything to hours
    if unit == 'second':
        return round(value / 3600.0, 4)  # seconds to hours
    elif unit == 'minute':
        return round(value / 60.0, 4)  # minutes to hours
    elif unit == 'hour':
        return round(value, 2)
    elif unit == 'day':
        return round(value * 24.0, 2)  # days to hours
    elif unit == 'week':
        return round(value * 24.0 * 7, 2)  # weeks to hours
    elif unit == 'month':
        return round(value * 24.0 * 30, 2)  # approximate month to hours
    elif unit == 'year':
        return round(value * 24.0 * 365, 2)  # approximate year to hours
    
    return None