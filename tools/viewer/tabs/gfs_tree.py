"""GFS Tree tab — visual Grandfather-Father-Son retention hierarchy."""
import datetime
from tkinter import ttk
import tkinter as tk

from ..rpc import call, RPCError
from ..formats import fmt_size, fmt_time, gfs_flags_str
from ..constants import GFS_DAILY, GFS_WEEKLY, GFS_MONTHLY, GFS_YEARLY
from ..widgets import PAD, FONT_MONO, FONT_BOLD

# Highest-priority tier wins the row colour.
_TIER_ORDER = [GFS_YEARLY, GFS_MONTHLY, GFS_WEEKLY, GFS_DAILY]
_TAG_FOR_FLAG = {
    GFS_YEARLY:  "yearly",
    GFS_MONTHLY: "monthly",
    GFS_WEEKLY:  "weekly",
    GFS_DAILY:   "daily",
}
_TIER_LABELS = [
    ("Yearly",   "#7B4A00", "#FFF3CD"),
    ("Monthly",  "#003087", "#CCE5FF"),
    ("Weekly",   "#155724", "#D4EDDA"),
    ("Daily",    "#0C5460", "#D1ECF1"),
    ("Untagged", "#444444", "#F0F0F0"),
]


def _snap_tag(gfs_flags: int) -> str:
    for flag in _TIER_ORDER:
        if gfs_flags & flag:
            return _TAG_FOR_FLAG[flag]
    return "untagged"


class GFSTreeTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="GFS Tree")
        self._build()

    # ------------------------------------------------------------------ build

    def _build(self) -> None:
        # Summary line
        self._summary = tk.Label(self._frame, text="No repository loaded.",
                                 anchor="w", font=FONT_MONO)
        self._summary.pack(fill=tk.X, padx=PAD, pady=(PAD, 2))

        # Legend
        legend = tk.Frame(self._frame)
        legend.pack(fill=tk.X, padx=PAD, pady=(0, PAD // 2))
        tk.Label(legend, text="Tier:", font=FONT_BOLD).pack(side=tk.LEFT, padx=(0, 6))
        for label, fg, bg in _TIER_LABELS:
            tk.Label(legend, text=f" {label} ", fg=fg, bg=bg,
                     relief=tk.RIDGE, padx=4, pady=1).pack(side=tk.LEFT, padx=3)

        # Separator
        ttk.Separator(self._frame, orient=tk.HORIZONTAL).pack(fill=tk.X, padx=PAD, pady=2)

        # Treeview + scrollbars
        tree_frame = tk.Frame(self._frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=PAD, pady=(0, PAD))

        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        hsb = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL)

        cols = ("snap", "datetime", "flags", "nodes", "size")
        self._tree = ttk.Treeview(
            tree_frame, columns=cols, show="tree headings",
            yscrollcommand=vsb.set, xscrollcommand=hsb.set,
        )
        vsb.config(command=self._tree.yview)
        hsb.config(command=self._tree.xview)

        # Column layout
        self._tree.heading("#0",       text="Period")
        self._tree.column("#0",        width=200, stretch=False, minwidth=160)
        self._tree.heading("snap",     text="Snap #")
        self._tree.column("snap",      width=70,  stretch=False, anchor="center")
        self._tree.heading("datetime", text="Date / Time")
        self._tree.column("datetime",  width=155, stretch=False)
        self._tree.heading("flags",    text="GFS Tiers")
        self._tree.column("flags",     width=190, stretch=True)
        self._tree.heading("nodes",    text="Nodes")
        self._tree.column("nodes",     width=80,  stretch=False, anchor="e")
        self._tree.heading("size",     text="Phys New")
        self._tree.column("size",      width=90,  stretch=False, anchor="e")

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

        # Colour tags for snap rows
        self._tree.tag_configure("yearly",   background="#FFF3CD", foreground="#7B4A00")
        self._tree.tag_configure("monthly",  background="#CCE5FF", foreground="#003087")
        self._tree.tag_configure("weekly",   background="#D4EDDA", foreground="#155724")
        self._tree.tag_configure("daily",    background="#D1ECF1", foreground="#0C5460")
        self._tree.tag_configure("untagged", background="#F0F0F0", foreground="#444444")
        # Group rows (year / month headers)
        self._tree.tag_configure("year_hdr",  background="#DDDDDD", foreground="#111111",
                                  font=FONT_BOLD)
        self._tree.tag_configure("month_hdr", background="#EEEEEE", foreground="#333333",
                                  font=FONT_BOLD)

    # --------------------------------------------------------------- populate

    def populate(self, repo_path: str) -> None:
        self._tree.delete(*self._tree.get_children())

        try:
            data = call(repo_path, "list")
        except RPCError:
            self._summary.config(text="Error loading snapshot list.")
            return

        snaps = data.get("snapshots", [])
        if not snaps:
            self._summary.config(text="No snapshots found.")
            return

        snaps.sort(key=lambda s: s["id"])

        # Counters
        n_yearly   = sum(1 for s in snaps if int(s["gfs_flags"]) & GFS_YEARLY)
        n_monthly  = sum(1 for s in snaps if int(s["gfs_flags"]) & GFS_MONTHLY)
        n_weekly   = sum(1 for s in snaps if int(s["gfs_flags"]) & GFS_WEEKLY)
        n_daily    = sum(1 for s in snaps if int(s["gfs_flags"]) & GFS_DAILY)
        n_untagged = sum(1 for s in snaps if not int(s["gfs_flags"]))

        self._summary.config(
            text=(
                f"Total: {len(snaps)}  |  "
                f"Yearly: {n_yearly}   Monthly: {n_monthly}   "
                f"Weekly: {n_weekly}   Daily: {n_daily}   "
                f"Untagged: {n_untagged}"
            )
        )

        # Group: year_str → month_str → [snap], newest first
        year_map: dict[str, dict[str, list[dict]]] = {}
        for s in snaps:
            dt = datetime.datetime.fromtimestamp(int(s["created_sec"]))
            y  = dt.strftime("%Y")
            m  = dt.strftime("%B %Y")          # e.g. "March 2026"
            year_map.setdefault(y, {}).setdefault(m, []).append(s)

        for year in sorted(year_map, reverse=True):
            months   = year_map[year]
            yr_total = sum(len(v) for v in months.values())

            year_node = self._tree.insert(
                "", "end",
                text=f"  {year}",
                values=(f"{yr_total} snapshots", "", "", "", ""),
                tags=("year_hdr",),
                open=True,
            )

            for month in sorted(
                months,
                key=lambda m: datetime.datetime.strptime(m, "%B %Y"),
                reverse=True,
            ):
                month_snaps = months[month]
                mo_node = self._tree.insert(
                    year_node, "end",
                    text=f"  {month}",
                    values=(f"{len(month_snaps)} snapshots", "", "", "", ""),
                    tags=("month_hdr",),
                    open=True,
                )

                for s in sorted(month_snaps, key=lambda x: x["id"], reverse=True):
                    flags     = int(s["gfs_flags"])
                    flags_str = gfs_flags_str(flags) if flags else "—"
                    tag       = _snap_tag(flags)

                    self._tree.insert(
                        mo_node, "end",
                        text="",
                        values=(
                            f"#{int(s['id']):04d}",
                            fmt_time(int(s["created_sec"])),
                            flags_str,
                            f"{int(s.get('node_count', 0)):,}",
                            fmt_size(int(s.get("phys_new_bytes", 0))),
                        ),
                        tags=(tag,),
                    )
