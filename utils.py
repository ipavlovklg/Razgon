from datetime import datetime
import os


def to_iso(timestamp: float|None) -> str|None:
    """Конвертируем timestamp в строку ISO 8601."""
    return datetime.fromtimestamp(timestamp).isoformat() if timestamp else None

def format_bytes(bytes_value: int) -> str:
    bytes_value_f = float(bytes_value)
    """Converts bytes to a human-readable string (KB, MB, GB)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value_f < 1024.0:
            return f"{bytes_value_f:.2f} {unit}"
        bytes_value_f /= 1024
    return f"{bytes_value_f:.2f} PB"