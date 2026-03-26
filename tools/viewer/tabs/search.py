import threading
import tkinter as tk
from tkinter import ttk

from ..parsers import parse_snap, find_paths_matching, ParseError
from ..formats import fmt_size, fmt_time, hex_hash
from ..constants import NODE_TYPE_NAMES
from ..widgets import PAD, FONT_MONO

_RESULT_CAP = 5000


class SearchTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="File Search")
        self._scan: dict | None = None
        self._navigate_cb = None
        self._search_gen: int = 0
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
        self._search_btn = tk.Button(bar, text="Search", command=self._search)
        self._search_btn.pack(side=tk.LEFT)
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
        rows = self._tree.get_children()
        if rows:
            self._tree.delete(*rows)

        self._search_gen += 1
        gen = self._search_gen
        self._count_label.config(text="Searching…")
        self._search_btn.config(state=tk.DISABLED)

        snap_paths = list(self._scan.get("snapshots", []))

        def _worker() -> None:
            results: list[tuple] = []
            for snap_path in snap_paths:
                try:
                    s = parse_snap(snap_path)
                except ParseError:
                    continue
                for path, node in find_paths_matching(s, fragment):
                    if node is None:
                        continue
                    results.append((
                        s["snap_id"],
                        fmt_time(s["created_sec"]),
                        path,
                        NODE_TYPE_NAMES.get(node["type"], str(node["type"])),
                        fmt_size(node["size"]),
                        hex_hash(node["content_hash"])[:20] + "…",
                    ))
                    if len(results) >= _RESULT_CAP:
                        break
                if len(results) >= _RESULT_CAP:
                    break
            self._frame.after(0, lambda: self._finish_search(results, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _finish_search(self, results: list[tuple], gen: int) -> None:
        if gen != self._search_gen:
            return
        self._search_btn.config(state=tk.NORMAL)
        # Group by path so the same filename across snapshots appears together,
        # secondary sort by snap_id within each group.
        results.sort(key=lambda r: (r[2], r[0]))
        for vals in results:
            self._tree.insert("", tk.END, values=vals)
        total = len(results)
        if total >= _RESULT_CAP:
            self._count_label.config(text=f"{total}+ results (capped at {_RESULT_CAP})")
        else:
            self._count_label.config(text=f"{total} result(s)")

    def _on_result_dbl(self, _event) -> None:
        sel = self._tree.selection()
        if not sel or not self._navigate_cb:
            return
        row = self._tree.item(sel[0])["values"]
        snap_id   = int(row[0])
        full_path = row[2]
        self._navigate_cb(snap_id, full_path)
