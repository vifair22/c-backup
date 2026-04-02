import base64
import difflib
import threading
import tkinter as tk
from tkinter import ttk, messagebox

from ..rpc import call, RPCError
from ..formats import fmt_size, fmt_time
from ..widgets import PAD, FONT_MONO, make_text_widget
from ..constants import UI_SIZE_LIMIT, ZERO_HASH

_DIFF_ROW_CAP = 5000


class DiffTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Diff")
        self._repo_path: str | None = None
        self._snap_list: list[dict] = []
        self._diff_changes: list[dict] = []
        self._compare_gen: int = 0
        self._snap_a_id: int | None = None
        self._snap_b_id: int | None = None
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

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path
        try:
            data = call(repo_path, "list")
            self._snap_list = data.get("snapshots", [])
        except RPCError:
            self._snap_list = []

        labels = []
        for s in self._snap_list:
            labels.append(f"{int(s['id']):>6}  {fmt_time(int(s['created_sec']))}")
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

        id_a = int(self._snap_list[ai]["id"])
        id_b = int(self._snap_list[bi]["id"])
        self._snap_a_id = id_a
        self._snap_b_id = id_b
        repo_path = self._repo_path

        def _worker() -> None:
            try:
                result = call(repo_path, "diff", id1=id_a, id2=id_b)
            except RPCError as e:
                err = str(e)
                self._frame.after(0, lambda: self._on_compare_error(err, gen))
                return

            changes = result.get("changes", [])
            counts = {"added": 0, "deleted": 0, "modified": 0, "meta": 0}
            result_rows: list[tuple] = []

            for c in changes:
                change = c["change"]
                path   = c["path"]
                old_n  = c.get("old_node")
                new_n  = c.get("new_node")

                if change == "A":
                    status, tag = "added", "added"
                    old_sz, new_sz = "—", fmt_size(int(new_n["size"])) if new_n else "—"
                    old_h,  new_h  = "—", new_n["content_hash"][:14] if new_n else "—"
                elif change == "D":
                    status, tag = "deleted", "deleted"
                    old_sz, new_sz = fmt_size(int(old_n["size"])) if old_n else "—", "—"
                    old_h,  new_h  = old_n["content_hash"][:14] if old_n else "—", "—"
                elif change == "M":
                    status, tag = "modified", "modified"
                    old_sz = fmt_size(int(old_n["size"])) if old_n else "—"
                    new_sz = fmt_size(int(new_n["size"])) if new_n else "—"
                    old_h  = old_n["content_hash"][:14] if old_n else "—"
                    new_h  = new_n["content_hash"][:14] if new_n else "—"
                elif change == "m":
                    status, tag = "meta-only", "meta"
                    old_sz = fmt_size(int(old_n["size"])) if old_n else "—"
                    new_sz = fmt_size(int(new_n["size"])) if new_n else "—"
                    old_h  = old_n["content_hash"][:14] if old_n else "—"
                    new_h  = "(same)"
                else:
                    continue

                counts[tag if tag != "meta" else "meta"] += 1
                if len(result_rows) < _DIFF_ROW_CAP:
                    result_rows.append(((status, path, old_sz, new_sz, old_h, new_h), tag))

            self._frame.after(0, lambda: self._finish_compare(
                changes, counts, result_rows, id_a, id_b, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _on_compare_error(self, err: str, gen: int) -> None:
        if gen != self._compare_gen:
            return
        self._compare_btn.config(state=tk.NORMAL)
        self._status.config(text="")
        messagebox.showerror("Parse error", err)

    def _finish_compare(self, changes, counts, result_rows, id_a, id_b, gen) -> None:
        if gen != self._compare_gen:
            return
        self._compare_btn.config(state=tk.NORMAL)
        self._diff_changes = changes

        for vals, tag in result_rows:
            self._tree.insert("", tk.END, values=vals, tags=(tag,))

        total_changes = sum(counts.values())
        cap_note = ""
        if total_changes > _DIFF_ROW_CAP:
            cap_note = f"  (showing {len(result_rows)} of {total_changes:,})"

        self._status.config(
            text=(f"snap {id_a} → {id_b}:  "
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
        if not sel or not self._repo_path:
            return
        vals  = self._tree.item(sel[0])["values"]
        status, path = str(vals[0]), str(vals[1])

        # Find the matching change entry
        change_entry = None
        for c in self._diff_changes:
            if c["path"] == path:
                change_entry = c
                break
        if not change_entry:
            return

        self._show_file_diff(path, status,
                             change_entry.get("old_node"),
                             change_entry.get("new_node"))

    def _show_file_diff(self, path: str, status: str,
                        old_n: dict | None, new_n: dict | None) -> None:
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

        lines = self._build_diff_lines(path, status, old_n, new_n)

        text.config(state=tk.NORMAL)
        for line, tag in lines:
            text.insert(tk.END, line, (tag,) if tag else ())
        text.config(state=tk.DISABLED)

    def _load_content(self, node: dict | None) -> bytes | None:
        if node is None or not self._repo_path:
            return None
        h = node.get("content_hash", ZERO_HASH)
        if h == ZERO_HASH:
            return b""
        try:
            cr = call(self._repo_path, "object_content", hash=h,
                      max_bytes=UI_SIZE_LIMIT)
            return base64.b64decode(cr.get("content_base64", ""))
        except RPCError:
            return None

    def _build_diff_lines(self, path: str, status: str,
                          old_n: dict | None, new_n: dict | None) -> list:
        if status == "meta-only":
            return self._meta_diff_lines(old_n, new_n)

        old_data = self._load_content(old_n)
        new_data = self._load_content(new_n)

        old_label = f"a{path}"
        new_label = f"b{path}"
        if old_n is None:
            old_label = "/dev/null"
        if new_n is None:
            new_label = "/dev/null"

        if old_data is None and new_data is None:
            return [("(could not load object content — object missing from repo)\n", "meta")]

        old_bytes = old_data if old_data is not None else b""
        new_bytes = new_data if new_data is not None else b""

        if len(old_bytes) > UI_SIZE_LIMIT or len(new_bytes) > UI_SIZE_LIMIT:
            big = max(len(old_bytes), len(new_bytes))
            return [(f"File too large to diff inline  "
                     f"({fmt_size(big)} > {fmt_size(UI_SIZE_LIMIT)})\n", "meta")]

        try:
            old_lines = old_bytes.decode("utf-8").splitlines(keepends=True)
            new_lines = new_bytes.decode("utf-8").splitlines(keepends=True)
        except UnicodeDecodeError:
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
    def _meta_diff_lines(old_n: dict, new_n: dict) -> list:
        lines = []
        for field, fmt in (("mode",     lambda n: f"{int(n['mode']):o}"),
                           ("uid",      lambda n: str(int(n["uid"]))),
                           ("gid",      lambda n: str(int(n["gid"]))),
                           ("mtime",    lambda n: str(int(n["mtime_sec"])))):
            va, vb = fmt(old_n), fmt(new_n)
            if va != vb:
                lines.append((f"- {field}: {va}\n", "rem"))
                lines.append((f"+ {field}: {vb}\n", "add"))
        return lines or [("(no metadata differences found)\n", "meta")]
