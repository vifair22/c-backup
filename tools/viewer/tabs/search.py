import tkinter as tk
from tkinter import ttk

from ..parsers import parse_snap, find_paths_matching
from ..formats import fmt_size, fmt_time, hex_hash
from ..constants import NODE_TYPE_NAMES
from ..widgets import PAD, FONT_MONO


class SearchTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="File Search")
        self._scan: dict | None = None
        self._navigate_cb = None
        self._build()

    def set_navigate_callback(self, cb) -> None:
        self._navigate_cb = cb

    def _build(self) -> None:
        bar = tk.Frame(self._frame)
        bar.pack(fill=tk.X, padx=PAD, pady=PAD)
        tk.Label(bar, text="Filename contains:").pack(side=tk.LEFT)
        self._var = tk.StringVar()
        entry = tk.Entry(bar, textvariable=self._var, font=FONT_MONO, width=40)
        entry.pack(side=tk.LEFT, padx=4)
        entry.bind("<Return>", lambda _: self._search())
        tk.Button(bar, text="Search", command=self._search).pack(side=tk.LEFT)
        self._count_label = tk.Label(bar, text="")
        self._count_label.pack(side=tk.LEFT, padx=8)

        cols = [
            ("snap_id",  60, False),
            ("created", 140, False),
            ("path",    440, True),
            ("type",     72, False),
            ("size",     90, False),
            ("hash",    200, False),
        ]
        col_ids = [c[0] for c in cols]
        self._tree = ttk.Treeview(self._frame, columns=col_ids,
                                  show="headings", selectmode="browse")
        for name, width, stretch in cols:
            self._tree.heading(name, text=name.replace("_", " ").title())
            self._tree.column(name, width=width, stretch=stretch)
        sb_y = ttk.Scrollbar(self._frame, orient=tk.VERTICAL,
                              command=self._tree.yview)
        sb_x = ttk.Scrollbar(self._frame, orient=tk.HORIZONTAL,
                              command=self._tree.xview)
        self._tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.pack(side=tk.RIGHT,  fill=tk.Y)
        sb_x.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)
        self._tree.bind("<Double-1>", self._on_result_dbl)
        tk.Label(self._frame,
                 text="Double-click a result → jump to snapshot dir tree",
                 fg="gray").pack(anchor="w", padx=PAD)

    def populate(self, scan: dict) -> None:
        self._scan = scan

    def _search(self) -> None:
        fragment = self._var.get().strip()
        if not fragment or not self._scan:
            return
        for row in self._tree.get_children():
            self._tree.delete(row)
        total = 0
        for snap_path in self._scan.get("snapshots", []):
            try:
                s = parse_snap(snap_path)
            except Exception:
                continue
            for path, node in find_paths_matching(s, fragment):
                if node is None:
                    continue
                total += 1
                self._tree.insert("", tk.END, values=(
                    s["snap_id"],
                    fmt_time(s["created_sec"]),
                    path,
                    NODE_TYPE_NAMES.get(node["type"], str(node["type"])),
                    fmt_size(node["size"]),
                    hex_hash(node["content_hash"])[:20] + "…",
                ))
        self._count_label.config(text=f"{total} result(s)")

    def _on_result_dbl(self, _event) -> None:
        sel = self._tree.selection()
        if not sel or not self._navigate_cb:
            return
        row = self._tree.item(sel[0])["values"]
        snap_id   = int(row[0])
        full_path = row[2]
        self._navigate_cb(snap_id, full_path)
