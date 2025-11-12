"""
Common utility functions for MCP tools
"""

import math
import pytz
from datetime import datetime
from typing import Optional

""

def sanitize_json_compat(value):
    """Recursively replace NaN/Inf floats with None for JSON compatibility"""
    try:
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {k: sanitize_json_compat(v) for k, v in value.items()}
        if isinstance(value, list):
            return [sanitize_json_compat(v) for v in value]
        if isinstance(value, tuple):
            return [sanitize_json_compat(v) for v in value]
        return value
    except Exception:
        return None


def sanitize_json_compat(value):
    """Recursively replace NaN/Inf floats with None for JSON compatibility"""
    try:
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {k: sanitize_json_compat(v) for k, v in value.items()}
        if isinstance(value, list):
            return [sanitize_json_compat(v) for v in value]
        if isinstance(value, tuple):
            return [sanitize_json_compat(v) for v in value]
        return value
    except Exception:
        return None


def get_utc_timestamp():
    """Get current UTC timestamp in ISO format"""
    return datetime.now(pytz.UTC).isoformat() 

def duration_from_time_range(start_time_iso: Optional[str], end_time_iso: Optional[str]) -> Optional[str]:
    """
    Convert ISO start/end times into a duration string
    
    Args:
        start_time_iso: Start time in ISO format (e.g., "2025-01-01T00:00:00Z")
        end_time_iso: End time in ISO format (e.g., "2025-01-01T01:00:00Z")
    
    Returns:
        Duration string (e.g., "1h", "30m", "1h30m") or None if invalid
    """
    try:
        if not start_time_iso or not end_time_iso:
            return None
        
        start_dt = datetime.fromisoformat(start_time_iso.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time_iso.replace("Z", "+00:00"))
        
        if end_dt <= start_dt:
            return None
        
        total_seconds = int((end_dt - start_dt).total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        
        if hours > 0 and minutes > 0:
            return f"{hours}h{minutes}m"
        if hours > 0:
            return f"{hours}h"
        if minutes > 0:
            return f"{minutes}m"
        return "1m"
    except Exception:
        return None    