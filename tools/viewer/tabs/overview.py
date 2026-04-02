import threading
import tkinter as tk
from tkinter import ttk

from ..rpc import call, RPCError
from ..formats import fmt_size, fmt_time, gfs_flags_str
from ..widgets import make_text_widget, set_text, PAD, FONT_BOLD


class OverviewTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Overview")
        self._text = make_text_widget(self._frame)

    def populate(self, repo_path: str) -> None:
        set_text(self._text, "Loading…")

        def _worker() -> None:
            try:
                scan = call(repo_path, "scan")
                snap_list = call(repo_path, "list")
            except RPCError as e:
                msg = f"Error: {e}"
                self._frame.after(0, lambda: set_text(self._text, msg))
                return

            snaps = snap_list.get("snapshots", [])
            head  = snap_list.get("head")
            packs = scan.get("packs", [])

            lines = [
                f"Repository : {repo_path}",
                f"HEAD snap  : {head if head else '—'}",
                f"Snapshots  : {scan.get('snapshot_files', 0)}",
                f"Pack files : {len(packs)} .dat",
                f"Loose objs : {scan.get('loose_objects', 0)}",
                f"Tags       : {scan.get('tag_count', 0)}",
                "",
            ]

            fmt_str = scan.get("format")
            if fmt_str:
                lines.insert(1, f"Format     : {fmt_str}")

            if snaps:
                lines.append("── Snapshots ──────────────────────────────────────────────────────────")
                lines.append(f"  {'ID':>8}  {'Created':>19}  {'Nodes':>7}  {'GFS':>25}  New bytes")
                for s in snaps:
                    gfs = gfs_flags_str(int(s.get("gfs_flags", 0)))
                    lines.append(
                        f"  {s['id']:>8}  {fmt_time(int(s['created_sec'])):>19}"
                        f"  {int(s.get('node_count', 0)):>7}  {gfs:>25}"
                        f"  {fmt_size(int(s.get('phys_new_bytes', 0)))}"
                    )

            if packs:
                lines += ["", "── Pack .dat files ────────────────────────────────────────────────────"]
                lines.append(f"  {'File':30}  {'Size':>10}")
                for pk in packs:
                    lines.append(
                        f"  {pk['name']:30}  {fmt_size(int(pk.get('size', 0))):>10}"
                    )

            self._frame.after(0, lambda: set_text(self._text, "\n".join(lines)))

        threading.Thread(target=_worker, daemon=True).start()
