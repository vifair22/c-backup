import os
import tkinter as tk
from tkinter import ttk

from ..parsers import parse_snap_header, parse_pack_dat
from ..formats import fmt_size, fmt_time, gfs_flags_str
from ..widgets import make_text_widget, set_text, PAD, FONT_BOLD


class OverviewTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Overview")
        self._text = make_text_widget(self._frame)

    def populate(self, scan: dict) -> None:
        lines = [
            f"Repository : {scan['repo_path']}",
            f"HEAD snap  : {scan['head_id'] if scan['head_id'] else '—'}",
            f"Snapshots  : {len(scan['snapshots'])}",
            f"Pack files : {len(scan['pack_dat'])} .dat  /  {len(scan['pack_idx'])} .idx",
            f"Loose objs : {len(scan['loose'])}",
            f"Tags       : {len(scan['tags'])}",
            "",
        ]

        fmt_path = os.path.join(scan["repo_path"], "format")
        if os.path.exists(fmt_path):
            lines.insert(1, f"Format     : {open(fmt_path).read().strip()}")

        if scan["snapshots"]:
            lines.append("── Snapshots ──────────────────────────────────────────────────────────")
            lines.append(f"  {'ID':>8}  {'Created':>19}  {'Nodes':>7}  {'GFS':>25}  New bytes")
            for path in scan["snapshots"]:
                try:
                    s = parse_snap_header(path)
                    gfs = gfs_flags_str(s["gfs_flags"])
                    lines.append(
                        f"  {s['snap_id']:>8}  {fmt_time(s['created_sec']):>19}"
                        f"  {s['node_count']:>7}  {gfs:>25}  {fmt_size(s['phys_new_bytes'])}"
                    )
                except Exception as e:
                    lines.append(f"  {os.path.basename(path):30}  ERROR: {e}")

        if scan["pack_dat"]:
            lines += ["", "── Pack .dat files ────────────────────────────────────────────────────"]
            lines.append(f"  {'File':30}  {'Ver':>3}  {'Objects':>7}  {'Size':>10}")
            for path in scan["pack_dat"]:
                try:
                    d = parse_pack_dat(path)
                    sz = os.path.getsize(path)
                    lines.append(
                        f"  {os.path.basename(path):30}  v{d['version']:>2}  {d['count']:>7}  {fmt_size(sz):>10}"
                    )
                except Exception as e:
                    lines.append(f"  {os.path.basename(path):30}  ERROR: {e}")

        set_text(self._text, "\n".join(lines))
