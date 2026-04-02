import base64
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..rpc import call, RPCError
from ..formats import fmt_size, hex_dump
from ..constants import (
    OBJECT_TYPE_NAMES, COMPRESS_NAMES,
    OBJECT_TYPE_SPARSE, UI_SIZE_LIMIT, ZERO_HASH,
)
from ..widgets import (make_text_widget, set_text, make_sparse_canvas,
                       update_sparse_map, parse_sparse_regions,
                       PAD, FONT_MONO, FONT_BOLD)

_PREVIEW_LIMIT = UI_SIZE_LIMIT


class LooseTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Loose Objects")
        self._repo_path: str | None = None
        self._objects: list[dict] = []
        self._current_hash: str | None = None
        self._current_data: bytes | None = None
        self._build()

    def _build(self) -> None:
        pane = ttk.PanedWindow(self._frame, orient=tk.HORIZONTAL)
        pane.pack(fill=tk.BOTH, expand=True)

        # Left: object list
        left = ttk.Frame(pane)
        pane.add(left, weight=1)
        tk.Label(left, text="Loose objects", font=FONT_BOLD).pack(anchor="w", padx=PAD)
        self._list = tk.Listbox(left, font=FONT_MONO, width=22)
        sb = ttk.Scrollbar(left, command=self._list.yview)
        self._list.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._list.pack(fill=tk.BOTH, expand=True)
        self._list.bind("<<ListboxSelect>>", self._on_select)

        # Right: sub-notebook + export button
        right = ttk.Frame(pane)
        pane.add(right, weight=4)

        btn_bar = tk.Frame(right)
        btn_bar.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))
        tk.Button(btn_bar, text="Export payload…", command=self._export).pack(side=tk.LEFT)

        self._nb = ttk.Notebook(right)
        self._nb.pack(fill=tk.BOTH, expand=True, pady=(PAD, 0))

        # Tab 1: Info
        info_frame = ttk.Frame(self._nb)
        self._nb.add(info_frame, text="Info")
        self._info_text = make_text_widget(info_frame)

        # Tab 2: Content Preview
        prev_frame = ttk.Frame(self._nb)
        self._nb.add(prev_frame, text="Content Preview")
        self._prev_text = make_text_widget(prev_frame)

        # Tab 3: Sparse Map
        sparse_frame = ttk.Frame(self._nb)
        self._nb.add(sparse_frame, text="Sparse Map")
        self._sparse_canvas, self._sparse_info = make_sparse_canvas(sparse_frame)

    # ---- populate ----

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path
        self._list.delete(0, tk.END)
        try:
            data = call(repo_path, "loose_list")
            self._objects = data.get("objects", [])
        except RPCError as e:
            self._objects = []
            set_text(self._info_text, f"Error loading loose objects: {e}")
            return
        for obj in self._objects:
            h = obj.get("hash", "")
            self._list.insert(tk.END, h[:2] + "/" + h[2:])

    # ---- selection ----

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel or not self._repo_path:
            return
        obj = self._objects[sel[0]]
        hash_hex   = obj["hash"]
        otype      = int(obj["type"])
        comp       = int(obj["compression"])
        uncomp_sz  = int(obj["uncompressed_size"])
        comp_sz    = int(obj["compressed_size"])

        self._current_hash = hash_hex

        # Load content via RPC
        self._current_data = None
        max_bytes = min(uncomp_sz, _PREVIEW_LIMIT) if uncomp_sz > 0 else _PREVIEW_LIMIT
        try:
            cr = call(self._repo_path, "object_content", hash=hash_hex,
                      max_bytes=max_bytes)
            self._current_data = base64.b64decode(cr.get("content_base64", ""))
        except RPCError:
            pass

        self._populate_info(hash_hex, otype, comp, uncomp_sz, comp_sz)
        self._populate_preview(uncomp_sz)
        self._populate_sparse(otype, uncomp_sz)

    def _populate_info(self, hash_hex, otype, comp, uncomp_sz, comp_sz) -> None:
        ratio = f"{comp_sz / uncomp_sz:.3f}" if uncomp_sz and comp_sz else "—"
        lines = [
            f"Hash (in header)   : {hash_hex}",
            f"Object type        : {OBJECT_TYPE_NAMES.get(otype, otype)}",
            f"Compression        : {COMPRESS_NAMES.get(comp, comp)}",
            f"Uncompressed size  : {fmt_size(uncomp_sz)} ({uncomp_sz} bytes)",
            f"Compressed size    : {fmt_size(comp_sz)} ({comp_sz} bytes)",
            f"Ratio              : {ratio}",
            "",
        ]
        set_text(self._info_text, "\n".join(lines))

    def _populate_preview(self, uncomp_sz) -> None:
        if self._current_data is None:
            set_text(self._prev_text, "(could not load object content)")
            return

        data = self._current_data
        truncated = uncomp_sz > len(data)
        lines = [f"Decompressed size: {fmt_size(uncomp_sz)}"
                 + (f"  (showing first {fmt_size(len(data))})" if truncated else ""),
                 ""]

        try:
            text = data.decode("utf-8")
            lines += ["── UTF-8 text ──────────────────────────────────────────", text[:4096]]
            if len(text) > 4096:
                lines.append(f"\n… ({len(text) - 4096} more chars)")
        except UnicodeDecodeError:
            pass

        lines += ["", "── Hex dump ────────────────────────────────────────────",
                  hex_dump(data)]
        if len(data) > 4096:
            lines.append(f"\n… ({len(data) - 4096} more bytes not shown)")

        set_text(self._prev_text, "\n".join(lines))

    def _populate_sparse(self, otype, uncomp_sz) -> None:
        self._sparse_canvas.delete("all")
        self._sparse_info.config(text="")

        if otype != OBJECT_TYPE_SPARSE:
            self._sparse_info.config(text="(only available for SPARSE objects)")
            return

        if self._current_data is None:
            self._sparse_info.config(text="(could not load content)")
            return

        regions = parse_sparse_regions(self._current_data)
        if regions is None:
            self._sparse_info.config(text="(could not parse sparse header)")
            return

        update_sparse_map(self._sparse_canvas, self._sparse_info, regions, uncomp_sz)

    # ---- export ----

    def _export(self) -> None:
        if not self._current_hash or not self._repo_path:
            messagebox.showinfo("Nothing selected", "Select an object first.")
            return
        # Load full content (no size limit) for export
        try:
            cr = call(self._repo_path, "object_content", hash=self._current_hash)
            data = base64.b64decode(cr.get("content_base64", ""))
        except RPCError as e:
            messagebox.showerror("Error", f"Could not load object: {e}")
            return

        dest = filedialog.asksaveasfilename(
            title="Save decompressed payload",
            initialfile=f"{self._current_hash[:16]}.bin",
        )
        if not dest:
            return
        try:
            with open(dest, "wb") as f:
                f.write(data)
            messagebox.showinfo("Saved", f"Wrote {fmt_size(len(data))} to:\n{dest}")
        except OSError as e:
            messagebox.showerror("Write error", str(e))
