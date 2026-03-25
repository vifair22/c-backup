import difflib
import threading
import tkinter as tk
from tkinter import ttk, messagebox

from ..parsers import (parse_snap, parse_snap_header, build_path_map,
                       find_object, decompress_payload, HAS_LZ4)
from ..formats import fmt_size, fmt_time, hex_hash
from ..widgets import PAD, FONT_MONO, make_text_widget
from ..constants import COMPRESS_NONE, UI_SIZE_LIMIT

_DIFF_ROW_CAP = 5000


class DiffTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Diff")
        self._snap_paths: list[str] = []
        self._snap_a = None
        self._snap_b = None
        self._map_a: dict | None = None
        self._map_b: dict | None = None
        self._scan: dict = {}
        self._compare_gen: int = 0
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

        self._compare_btn = tk.Button(bar, text="Compare", command=self._compare)
        self._compare_btn.pack(side=tk.LEFT, padx=8)
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
        self._tree.bind("<Double-1>", self._on_dbl)

        sb_y = ttk.Scrollbar(self._frame, orient=tk.VERTICAL,
                              command=self._tree.yview)
        sb_x = ttk.Scrollbar(self._frame, orient=tk.HORIZONTAL,
                              command=self._tree.xview)
        self._tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.pack(side=tk.RIGHT,  fill=tk.Y)
        sb_x.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

    def populate(self, scan: dict) -> None:
        self._scan = scan
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

        self._compare_gen += 1
        gen = self._compare_gen
        self._status.config(text="Comparing…")
        self._compare_btn.config(state=tk.DISABLED)
        rows = self._tree.get_children()
        if rows:
            self._tree.delete(*rows)

        path_a = self._snap_paths[ai]
        path_b = self._snap_paths[bi]

        def _worker() -> None:
            try:
                snap_a = parse_snap(path_a)
                snap_b = parse_snap(path_b)
            except Exception as e:
                err = str(e)
                self._frame.after(0, lambda: self._on_compare_error(err, gen))
                return

            map_a = build_path_map(snap_a)
            map_b = build_path_map(snap_b)
            all_paths = sorted(set(map_a) | set(map_b))

            counts = {"added": 0, "deleted": 0, "modified": 0, "meta": 0}
            result_rows: list[tuple] = []    # (values_tuple, tag)

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
                    continue

                counts[tag if tag != "meta" else "meta"] += 1
                if len(result_rows) < _DIFF_ROW_CAP:
                    result_rows.append(((status, path, old_sz, new_sz, old_h, new_h), tag))

            self._frame.after(0, lambda: self._finish_compare(
                snap_a, snap_b, map_a, map_b, counts, result_rows, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _on_compare_error(self, err: str, gen: int) -> None:
        if gen != self._compare_gen:
            return
        self._compare_btn.config(state=tk.NORMAL)
        self._status.config(text="")
        messagebox.showerror("Parse error", err)

    def _finish_compare(self, snap_a, snap_b, map_a, map_b,
                        counts, result_rows, gen) -> None:
        if gen != self._compare_gen:
            return
        self._compare_btn.config(state=tk.NORMAL)
        self._snap_a = snap_a
        self._snap_b = snap_b
        self._map_a  = map_a
        self._map_b  = map_b

        for vals, tag in result_rows:
            self._tree.insert("", tk.END, values=vals, tags=(tag,))

        total_changes = sum(counts.values())
        cap_note = ""
        if total_changes > _DIFF_ROW_CAP:
            cap_note = f"  (showing {len(result_rows)} of {total_changes:,})"

        self._status.config(
            text=(f"snap {snap_a['snap_id']} → {snap_b['snap_id']}:  "
                  f"+{counts['added']:,} added  "
                  f"-{counts['deleted']:,} deleted  "
                  f"~{counts['modified']:,} modified  "
                  f"{counts['meta']:,} meta-only"
                  f"{cap_note}")
        )

    # ------------------------------------------------------------------ #
    # Double-click → file diff popup                                       #
    # ------------------------------------------------------------------ #

    def _on_dbl(self, _event) -> None:
        sel = self._tree.selection()
        if not sel or not self._snap_a or not self._snap_b:
            return
        vals  = self._tree.item(sel[0])["values"]
        status, path = str(vals[0]), str(vals[1])

        # Use cached path maps from the compare (avoid rebuilding)
        if self._map_a is None or self._map_b is None:
            self._map_a = build_path_map(self._snap_a)
            self._map_b = build_path_map(self._snap_b)

        self._show_file_diff(path, status, self._map_a.get(path), self._map_b.get(path))

    def _show_file_diff(self, path: str, status: str, na: dict | None, nb: dict | None) -> None:
        win = tk.Toplevel(self._frame)
        win.title(f"diff  {path}")
        win.geometry("980x640")
        win.resizable(True, True)

        bar = tk.Frame(win)
        bar.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))
        color = {"added": "#006400", "deleted": "#8B0000",
                 "modified": "#8B4500", "meta-only": "#00008B"}.get(status, "black")
        tk.Label(bar, text=f"[{status}]  {path}",
                 font=FONT_MONO, fg=color).pack(side=tk.LEFT)

        text = make_text_widget(win)
        text.tag_configure("add",    foreground="#005500", background="#e6ffec")
        text.tag_configure("rem",    foreground="#8B0000", background="#ffebe9")
        text.tag_configure("hunk",   foreground="#0000CD",
                           font=(FONT_MONO[0], FONT_MONO[1], "bold"))
        text.tag_configure("header", font=(FONT_MONO[0], FONT_MONO[1], "bold"))
        text.tag_configure("meta",   foreground="#00008B")

        lines = self._build_diff_lines(path, status, na, nb)

        text.config(state=tk.NORMAL)
        for line, tag in lines:
            text.insert(tk.END, line, (tag,) if tag else ())
        text.config(state=tk.DISABLED)

    def _load_content(self, node: dict | None) -> bytes | None:
        """Load and decompress a node's content object. Returns b"" for empty files.
        Raises RuntimeError if lz4 is required but not installed."""
        if node is None:
            return None
        h = node["content_hash"]
        if h == bytes(32):
            return b""
        result = find_object(h, self._scan)
        if not result:
            return None
        _, _, _, comp, uncomp_sz, _, payload = result
        if comp != COMPRESS_NONE and not HAS_LZ4:
            raise RuntimeError(
                "lz4 Python package is not installed — cannot decompress this object.\n"
                "Install it with:  pip install lz4"
            )
        return decompress_payload(payload, comp, uncomp_sz) if comp != COMPRESS_NONE else payload

    def _build_diff_lines(self, path: str, status: str,
                          na: dict | None, nb: dict | None) -> list:
        """Return a list of (text, tag) pairs for the diff popup."""
        if status == "meta-only":
            return self._meta_diff_lines(na, nb)

        try:
            old_data = self._load_content(na)
            new_data = self._load_content(nb)
        except RuntimeError as exc:
            return [(str(exc) + "\n", "meta")]

        old_label = f"a{path}"
        new_label = f"b{path}"
        if na is None:
            old_label = "/dev/null"
        if nb is None:
            new_label = "/dev/null"

        if old_data is None and new_data is None:
            return [("(could not load object content — object missing from repo)\n", "meta")]

        old_bytes = old_data if old_data is not None else b""
        new_bytes = new_data if new_data is not None else b""

        if len(old_bytes) > UI_SIZE_LIMIT or len(new_bytes) > UI_SIZE_LIMIT:
            big = max(len(old_bytes), len(new_bytes))
            return [(f"File too large to diff inline  "
                     f"({fmt_size(big)} > {fmt_size(UI_SIZE_LIMIT)})\n", "meta")]

        # Try text diff first
        try:
            old_lines = old_bytes.decode("utf-8").splitlines(keepends=True)
            new_lines = new_bytes.decode("utf-8").splitlines(keepends=True)
        except UnicodeDecodeError:
            # Binary
            if old_bytes == new_bytes:
                return [("Binary files identical\n", "meta")]
            return [
                (f"Binary files {old_label} and {new_label} differ\n", "header"),
                (f"  old: {fmt_size(len(old_bytes))}  ({len(old_bytes):,} bytes)\n", "rem"),
                (f"  new: {fmt_size(len(new_bytes))}  ({len(new_bytes):,} bytes)\n", "add"),
            ]

        chunks = list(difflib.unified_diff(
            old_lines, new_lines,
            fromfile=old_label, tofile=new_label,
            lineterm="",
        ))
        if not chunks:
            return [("(no differences in text content)\n", "meta")]

        result = []
        for line in chunks:
            if line.startswith("---") or line.startswith("+++"):
                result.append((line + "\n", "header"))
            elif line.startswith("@@"):
                result.append((line + "\n", "hunk"))
            elif line.startswith("+"):
                result.append((line + "\n", "add"))
            elif line.startswith("-"):
                result.append((line + "\n", "rem"))
            else:
                result.append((line + "\n", ""))
        return result

    @staticmethod
    def _meta_diff_lines(na: dict, nb: dict) -> list:
        lines = []
        for field, fmt in (("mode",     lambda n: f"{n['mode']:o}"),
                           ("uid",      lambda n: str(n["uid"])),
                           ("gid",      lambda n: str(n["gid"])),
                           ("mtime",    lambda n: str(n["mtime_sec"]))):
            va, vb = fmt(na), fmt(nb)
            if va != vb:
                lines.append((f"- {field}: {va}\n", "rem"))
                lines.append((f"+ {field}: {vb}\n", "add"))
        return lines or [("(no metadata differences found)\n", "meta")]
