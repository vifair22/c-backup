import tkinter as tk
from tkinter import ttk, messagebox

from ..parsers import parse_snap, parse_snap_header, build_path_map
from ..formats import fmt_size, fmt_time, hex_hash
from ..widgets import PAD, FONT_MONO


class DiffTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Diff")
        self._snap_paths: list[str] = []
        self._build()

    def _build(self) -> None:
        bar = tk.Frame(self._frame)
        bar.pack(fill=tk.X, padx=PAD, pady=PAD)

        tk.Label(bar, text="Snap A:").pack(side=tk.LEFT)
        self._a_var = tk.StringVar()
        self._a_combo = ttk.Combobox(bar, textvariable=self._a_var,
                                     width=32, state="readonly")
        self._a_combo.pack(side=tk.LEFT, padx=4)

        tk.Label(bar, text="→  Snap B:").pack(side=tk.LEFT, padx=(8, 0))
        self._b_var = tk.StringVar()
        self._b_combo = ttk.Combobox(bar, textvariable=self._b_var,
                                     width=32, state="readonly")
        self._b_combo.pack(side=tk.LEFT, padx=4)

        tk.Button(bar, text="Compare", command=self._compare).pack(side=tk.LEFT, padx=8)
        self._status = tk.Label(bar, text="")
        self._status.pack(side=tk.LEFT)

        cols = [
            ("status",   80, False),
            ("path",    480, True),
            ("old_size",  90, False),
            ("new_size",  90, False),
            ("old_hash", 140, False),
            ("new_hash", 140, False),
        ]
        col_ids = [c[0] for c in cols]
        self._tree = ttk.Treeview(self._frame, columns=col_ids,
                                  show="headings", selectmode="browse")
        for name, width, stretch in cols:
            self._tree.heading(name, text=name.replace("_", " ").title())
            self._tree.column(name, width=width, stretch=stretch)

        self._tree.tag_configure("added",    foreground="#006400")
        self._tree.tag_configure("deleted",  foreground="#8B0000")
        self._tree.tag_configure("modified", foreground="#8B4500")
        self._tree.tag_configure("meta",     foreground="#00008B")

        sb_y = ttk.Scrollbar(self._frame, orient=tk.VERTICAL,
                              command=self._tree.yview)
        sb_x = ttk.Scrollbar(self._frame, orient=tk.HORIZONTAL,
                              command=self._tree.xview)
        self._tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.pack(side=tk.RIGHT,  fill=tk.Y)
        sb_x.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

    def populate(self, scan: dict) -> None:
        self._snap_paths = scan["snapshots"]
        labels = []
        for path in self._snap_paths:
            try:
                h = parse_snap_header(path)
                labels.append(f"{h['snap_id']:>6}  {fmt_time(h['created_sec'])}")
            except Exception:
                import os
                labels.append(os.path.basename(path))
        self._a_combo["values"] = labels
        self._b_combo["values"] = labels
        if len(labels) >= 2:
            self._a_combo.current(len(labels) - 2)
            self._b_combo.current(len(labels) - 1)
        elif labels:
            self._a_combo.current(0)
            self._b_combo.current(0)

    def _compare(self) -> None:
        ai = self._a_combo.current()
        bi = self._b_combo.current()
        if ai < 0 or bi < 0:
            messagebox.showwarning("Select snaps", "Select both snapshots.")
            return
        try:
            snap_a = parse_snap(self._snap_paths[ai])
            snap_b = parse_snap(self._snap_paths[bi])
        except Exception as e:
            messagebox.showerror("Parse error", str(e))
            return

        map_a = build_path_map(snap_a)
        map_b = build_path_map(snap_b)
        all_paths = sorted(set(map_a) | set(map_b))

        for row in self._tree.get_children():
            self._tree.delete(row)

        counts = {"added": 0, "deleted": 0, "modified": 0, "meta": 0}
        for path in all_paths:
            na, nb = map_a.get(path), map_b.get(path)
            if na is None:
                status, tag = "added",    "added"
                old_sz, new_sz = "—", fmt_size(nb["size"])
                old_h,  new_h  = "—", hex_hash(nb["content_hash"])[:14]
            elif nb is None:
                status, tag = "deleted",  "deleted"
                old_sz, new_sz = fmt_size(na["size"]), "—"
                old_h,  new_h  = hex_hash(na["content_hash"])[:14], "—"
            elif na["content_hash"] != nb["content_hash"]:
                status, tag = "modified", "modified"
                old_sz, new_sz = fmt_size(na["size"]), fmt_size(nb["size"])
                old_h,  new_h  = hex_hash(na["content_hash"])[:14], hex_hash(nb["content_hash"])[:14]
            elif (na["mode"] != nb["mode"] or na["uid"] != nb["uid"] or
                  na["gid"] != nb["gid"] or na["mtime_sec"] != nb["mtime_sec"]):
                status, tag = "meta-only", "meta"
                old_sz, new_sz = fmt_size(na["size"]), fmt_size(nb["size"])
                old_h,  new_h  = hex_hash(na["content_hash"])[:14], "(same)"
            else:
                continue  # unchanged

            counts[tag if tag != "meta" else "meta"] += 1
            self._tree.insert("", tk.END,
                              values=(status, path, old_sz, new_sz, old_h, new_h),
                              tags=(tag,))

        self._status.config(
            text=(f"snap {snap_a['snap_id']} → {snap_b['snap_id']}:  "
                  f"+{counts['added']} added  "
                  f"-{counts['deleted']} deleted  "
                  f"~{counts['modified']} modified  "
                  f"{counts['meta']} meta-only")
        )
