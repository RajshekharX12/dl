import math
import os
from datetime import timedelta

def fmt_bytes(n: float) -> str:
    if n is None:
        return "?"
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"

def fmt_eta(seconds) -> str:
    if seconds is None:
        return "?"
    if seconds < 0:
        seconds = 0
    return str(timedelta(seconds=int(seconds)))

def safe_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    return name[:200]

def file_too_large(path: str, max_mb: int) -> bool:
    try:
        size = os.path.getsize(path)
        return size > max_mb * 1024 * 1024
    except FileNotFoundError:
        return False

def looks_like_video_ext(ext: str) -> bool:
    return (ext or "").lower() in {"mp4", "mkv", "webm", "mov", "m4v"}
