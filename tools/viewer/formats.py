"""
Display-formatting helpers (no I/O, no tkinter).
"""

import datetime
from .constants import GFS_DAILY, GFS_WEEKLY, GFS_MONTHLY, GFS_YEARLY


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def fmt_time(ts: int) -> str:
    if ts == 0:
        return "—"
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def fmt_mode(mode: int) -> str:
    return f"{mode:04o}"


def hex_dump(data: bytes, max_bytes: int = 4096) -> str:
    """Standard hex dump: offset  hex (16/line)  |ascii|"""
    lines = []
    data = data[:max_bytes]
    for i in range(0, len(data), 16):
        chunk = data[i:i + 16]
        left  = " ".join(f"{b:02x}" for b in chunk[:8])
        right = " ".join(f"{b:02x}" for b in chunk[8:])
        text  = "".join(chr(b) if 32 <= b < 127 else "." for b in chunk)
        lines.append(f"{i:08x}  {left:<23}  {right:<23}  |{text}|")
    return "\n".join(lines)


def gfs_flags_str(flags: int) -> str:
    parts = []
    if flags & GFS_DAILY:   parts.append("daily")
    if flags & GFS_WEEKLY:  parts.append("weekly")
    if flags & GFS_MONTHLY: parts.append("monthly")
    if flags & GFS_YEARLY:  parts.append("yearly")
    return ", ".join(parts) if parts else "none"
