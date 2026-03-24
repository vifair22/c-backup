import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..parsers import load_loose_object, parse_sparse_regions, decompress_payload, HAS_LZ4
from ..formats import fmt_size, hex_hash, hex_dump
from ..constants import (
    OBJECT_TYPE_NAMES, COMPRESS_NAMES,
    OBJECT_TYPE_SPARSE, COMPRESS_NONE, UI_SIZE_LIMIT,
)
from ..widgets import (make_text_widget, set_text, make_sparse_canvas,
                       update_sparse_map, PAD, FONT_MONO, FONT_BOLD)

_PREVIEW_LIMIT = UI_SIZE_LIMIT


class LooseTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Loose Objects")
        self._loose_paths: list[str] = []
        self._current: tuple | None = None   # (otype, comp, uncomp_sz, comp_sz, h, payload)
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

    def populate(self, scan: dict) -> None:
        self._loose_paths = scan["loose"]
        self._list.delete(0, tk.END)
        for path in self._loose_paths:
            parts = path.split(os.sep)
            self._list.insert(tk.END, parts[-2] + parts[-1])

    # ---- selection ----

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel:
            return
        path = self._loose_paths[sel[0]]
        try:
            otype, comp, uncomp_sz, comp_sz, h, payload = load_loose_object(path)
        except Exception as e:
            messagebox.showerror("Parse error", str(e))
            return
        self._current = (otype, comp, uncomp_sz, comp_sz, h, payload)
        self._populate_info(path, otype, comp, uncomp_sz, comp_sz, h, payload)
        self._populate_preview(comp, uncomp_sz, payload)
        self._populate_sparse(otype, comp, uncomp_sz, payload)

    def _populate_info(self, path, otype, comp, uncomp_sz, comp_sz, h, payload) -> None:
        ratio = f"{comp_sz / uncomp_sz:.3f}" if uncomp_sz and comp_sz else "—"
        lines = [
            f"File               : {path}",
            f"Hash (in header)   : {hex_hash(h)}",
            f"Object type        : {OBJECT_TYPE_NAMES.get(otype, otype)}",
            f"Compression        : {COMPRESS_NAMES.get(comp, comp)}",
            f"Uncompressed size  : {fmt_size(uncomp_sz)} ({uncomp_sz} bytes)",
            f"Compressed size    : {fmt_size(comp_sz)} ({comp_sz} bytes)",
            f"Ratio              : {ratio}",
            "",
        ]
        if not HAS_LZ4 and comp != COMPRESS_NONE:
            lines.append("Install the 'lz4' Python package to decompress payload.")
        set_text(self._info_text, "\n".join(lines))

    def _populate_preview(self, comp, uncomp_sz, payload) -> None:
        """Hex dump + text decode for small objects."""
        if not HAS_LZ4 and comp != COMPRESS_NONE:
            set_text(self._prev_text, "(lz4 not installed — cannot decompress)")
            return
        if uncomp_sz > _PREVIEW_LIMIT:
            set_text(self._prev_text,
                     f"Object too large to preview ({fmt_size(uncomp_sz)}).\n"
                     f"Use Export to save the decompressed payload to a file.")
            return

        data = decompress_payload(payload, comp, uncomp_sz)
        if data is None:
            set_text(self._prev_text, "(decompression failed)")
            return

        lines = [f"Decompressed size: {fmt_size(len(data))}", ""]

        # Attempt UTF-8 text decode
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

    def _populate_sparse(self, otype, comp, uncomp_sz, payload) -> None:
        self._sparse_canvas.delete("all")
        self._sparse_info.config(text="")

        if otype != OBJECT_TYPE_SPARSE:
            self._sparse_info.config(text="(only available for SPARSE objects)")
            return

        sparse_raw = payload if comp == COMPRESS_NONE else decompress_payload(payload, comp, uncomp_sz)
        if sparse_raw is None:
            self._sparse_info.config(text="(decompression failed — cannot parse sparse map)")
            return

        regions = parse_sparse_regions(sparse_raw)
        if regions is None:
            self._sparse_info.config(text="(could not parse sparse header)")
            return

        update_sparse_map(self._sparse_canvas, self._sparse_info, regions, uncomp_sz)

    # ---- export ----

    def _export(self) -> None:
        if not self._current:
            messagebox.showinfo("Nothing selected", "Select an object first.")
            return
        otype, comp, uncomp_sz, comp_sz, h, payload = self._current
        dest = filedialog.asksaveasfilename(
            title="Save decompressed payload",
            initialfile=f"{hex_hash(h)}.bin",
        )
        if not dest:
            return
        if comp != COMPRESS_NONE:
            if not HAS_LZ4:
                messagebox.showerror("Error", "lz4 not installed — cannot decompress.")
                return
            data = decompress_payload(payload, comp, uncomp_sz)
            if data is None:
                messagebox.showerror("Error", "Decompression failed.")
                return
        else:
            data = payload
        try:
            with open(dest, "wb") as f:
                f.write(data)
            messagebox.showinfo("Saved", f"Wrote {fmt_size(len(data))} to:\n{dest}")
        except OSError as e:
            messagebox.showerror("Write error", str(e))
