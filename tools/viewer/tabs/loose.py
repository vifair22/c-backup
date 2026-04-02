import base64
import bisect
import math
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..rpc import call, RPCError
from ..formats import fmt_size, hex_dump
from ..constants import (
    OBJECT_TYPE_NAMES, COMPRESS_NAMES,
    OBJECT_TYPE_SPARSE, UI_SIZE_LIMIT, ZERO_HASH,
    COMPRESS_NONE, COMPRESS_LZ4, COMPRESS_LZ4_FRAME,
)
from ..widgets import (make_text_widget, set_text, ui_call,
                       PAD, FONT_MONO, FONT_BOLD)

_PREVIEW_LIMIT = UI_SIZE_LIMIT

# Segment colours — reuse pack map palette
_C_OBJ_HDR       = "#4a4a4a"   # dark gray — object header
_C_NONE           = "#5ba3d0"   # blue — uncompressed payload
_C_LZ4            = "#f28e2b"   # orange — lz4-block payload
_C_LZ4_FRAME      = "#e15759"   # red — lz4-frame payload
_C_HDR_PARITY     = "#b07aa1"   # purple — XOR header parity
_C_RS_PARITY      = "#59a14f"   # green — RS parity
_C_PAR_CRC        = "#edc948"   # gold — payload CRC + rs_data_len
_C_PAR_FOOTER     = "#4e79a7"   # steel blue — parity footer
_C_UNKNOWN        = "#cccccc"   # light gray — unknown

_COMP_COLOR = {
    COMPRESS_NONE:      _C_NONE,
    COMPRESS_LZ4:       _C_LZ4,
    COMPRESS_LZ4_FRAME: _C_LZ4_FRAME,
}

_KIND_COLOR = {
    "header":          _C_OBJ_HDR,
    "payload":         None,       # resolved from compression type
    "hdr_parity":      _C_HDR_PARITY,
    "rs_parity":       _C_RS_PARITY,
    "par_crc":         _C_PAR_CRC,
    "par_footer":      _C_PAR_FOOTER,
    "trailer_unknown": _C_UNKNOWN,
}

_KIND_LABEL = {
    "header":          "Object header  —  magic / version / type / "
                       "compression / sizes / hash",
    "payload":         "Payload",
    "hdr_parity":      "XOR header parity  —  CRC32C + stride-256 "
                       "interleaved parity",
    "rs_parity":       "RS parity  —  RS(255,239) × 64 interleave",
    "par_crc":         "Payload CRC32C + RS data length",
    "par_footer":      "Parity footer  —  magic / version / trailer_size",
    "trailer_unknown": "Parity trailer  (unknown layout)",
}

_LEGEND_ROW1 = [
    (_C_OBJ_HDR,      "Header"),
    (_C_NONE,          "Payload — none"),
    (_C_LZ4,           "Payload — lz4"),
    (_C_LZ4_FRAME,     "Payload — lz4-frame"),
]

_LEGEND_ROW2 = [
    (_C_HDR_PARITY,    "XOR parity"),
    (_C_RS_PARITY,     "RS parity"),
    (_C_PAR_CRC,       "Payload CRC"),
    (_C_PAR_FOOTER,    "Parity footer"),
]


class LooseTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Loose Objects")
        self._repo_path: str | None = None
        self._objects: list[dict] = []
        self._current_hash: str | None = None
        self._current_data: bytes | None = None
        self._map_segments: list[tuple] = []
        self._seg_starts: list[int] = []
        self._map_total_bytes: int = 0
        self._display_segments: list[tuple] = []
        self._display_starts: list[int] = []
        self._display_total: int = 0
        self._build()

    def _build(self) -> None:
        pane = ttk.PanedWindow(self._frame, orient=tk.HORIZONTAL)
        pane.pack(fill=tk.BOTH, expand=True)

        # Left: object list
        left = ttk.Frame(pane)
        pane.add(left, weight=1)
        tk.Label(left, text="Loose objects", font=FONT_BOLD).pack(
            anchor="w", padx=PAD)
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
        tk.Button(btn_bar, text="Export payload…",
                  command=self._export).pack(side=tk.LEFT)

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

        # Tab 3: Object Map
        self._map_frame = ttk.Frame(self._nb)
        self._nb.add(self._map_frame, text="Object Map")
        self._build_object_map()

    def _build_object_map(self) -> None:
        legend_box = tk.Frame(self._map_frame)
        legend_box.pack(side=tk.TOP, fill=tk.X, padx=PAD, pady=(PAD, 4))
        for row_items in (_LEGEND_ROW1, _LEGEND_ROW2):
            row = tk.Frame(legend_box)
            row.pack(fill=tk.X, pady=(0, 2))
            for color, label in row_items:
                swatch = tk.Frame(row, bg=color, width=18, height=18,
                                  relief=tk.GROOVE, bd=1)
                swatch.pack(side=tk.LEFT, padx=(0, 4))
                tk.Label(row, text=label,
                         font=FONT_BOLD).pack(side=tk.LEFT, padx=(0, 12))

        self._map_info_label = tk.Label(self._map_frame, text="",
                                        font=FONT_MONO, fg="gray")
        self._map_info_label.pack(anchor="w", padx=PAD)

        detail_frame = tk.Frame(self._map_frame, bd=1, relief=tk.SUNKEN)
        detail_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=PAD, pady=PAD)
        self._map_detail = tk.Label(
            detail_frame,
            text="Select an object to view its file layout",
            font=FONT_MONO, fg="gray", anchor="w")
        self._map_detail.pack(fill=tk.X, padx=4, pady=3)

        scroll_outer = tk.Frame(self._map_frame)
        scroll_outer.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=PAD)

        vsb = ttk.Scrollbar(scroll_outer, orient=tk.VERTICAL)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self._map_canvas = tk.Canvas(scroll_outer, bg="#1e1e1e",
                                     highlightthickness=0,
                                     yscrollcommand=vsb.set,
                                     cursor="crosshair")
        vsb.configure(command=self._map_canvas.yview)
        self._map_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self._map_canvas.bind("<Configure>", lambda _: self._redraw_map())
        self._map_canvas.bind("<Motion>", self._on_map_motion)
        self._map_canvas.bind("<Leave>",
                              lambda _: self._map_detail.config(
                                  text="Hover for details", fg="gray"))
        self._map_canvas.bind("<MouseWheel>",
                              lambda e: self._map_canvas.yview_scroll(
                                  int(-1 * (e.delta / 120)), "units"))
        self._map_canvas.bind("<Button-4>",
                              lambda _: self._map_canvas.yview_scroll(
                                  -3, "units"))
        self._map_canvas.bind("<Button-5>",
                              lambda _: self._map_canvas.yview_scroll(
                                  3, "units"))

    # ---- populate ----

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path
        try:
            data = call(repo_path, "loose_list")
        except RPCError as e:
            self._objects = []
            set_text(self._info_text,
                     f"Error loading loose objects: {e}")
            return
        self._apply_list(data)

    def populate_from_summary(self, repo_path: str, summary: dict) -> None:
        self._repo_path = repo_path
        data = summary.get("loose_list") or {"objects": []}
        self._apply_list(data)

    def _apply_list(self, data: dict) -> None:
        self._list.delete(0, tk.END)
        self._objects = data.get("objects", [])
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
        file_sz    = int(obj.get("file_size", 0))

        self._current_hash = hash_hex

        # Load content via RPC
        self._current_data = None
        max_bytes = (min(uncomp_sz, _PREVIEW_LIMIT) if uncomp_sz > 0
                     else _PREVIEW_LIMIT)
        try:
            cr = call(self._repo_path, "object_content", hash=hash_hex,
                      max_bytes=max_bytes)
            self._current_data = base64.b64decode(
                cr.get("content_base64", ""))
        except RPCError:
            pass

        self._populate_info(hash_hex, otype, comp, uncomp_sz, comp_sz,
                            file_sz)
        self._populate_preview(uncomp_sz)
        self._load_object_layout(hash_hex, comp)

    def _populate_info(self, hash_hex, otype, comp, uncomp_sz, comp_sz,
                       file_sz) -> None:
        ratio = (f"{comp_sz / uncomp_sz:.3f}"
                 if uncomp_sz and comp_sz else "—")
        hdr_sz = 56
        trailer_sz = file_sz - hdr_sz - comp_sz if file_sz > 0 else 0
        lines = [
            f"Hash (in header)   : {hash_hex}",
            f"Object type        : {OBJECT_TYPE_NAMES.get(otype, otype)}",
            f"Compression        : {COMPRESS_NAMES.get(comp, comp)}",
            f"Uncompressed size  : {fmt_size(uncomp_sz)}"
            f" ({uncomp_sz} bytes)",
            f"Compressed size    : {fmt_size(comp_sz)}"
            f" ({comp_sz} bytes)",
            f"Ratio              : {ratio}",
            f"File size on disk  : {fmt_size(file_sz)}"
            f" ({file_sz} bytes)",
            (f"Parity trailer     : {fmt_size(trailer_sz)}"
             f" ({trailer_sz} bytes)"
             if trailer_sz > 0
             else "Parity trailer     : none (v1 object)"),
            "",
        ]
        set_text(self._info_text, "\n".join(lines))

    def _populate_preview(self, uncomp_sz) -> None:
        if self._current_data is None:
            set_text(self._prev_text,
                     "(could not load object content)")
            return

        data = self._current_data
        truncated = uncomp_sz > len(data)
        lines = [
            f"Decompressed size: {fmt_size(uncomp_sz)}"
            + (f"  (showing first {fmt_size(len(data))})"
               if truncated else ""),
            "",
        ]

        try:
            text = data.decode("utf-8")
            lines += [
                "── UTF-8 text ─────────────────────────────────"
                "─────────", text[:4096],
            ]
            if len(text) > 4096:
                lines.append(f"\n… ({len(text) - 4096} more chars)")
        except UnicodeDecodeError:
            pass

        lines += [
            "",
            "── Hex dump ───────────────────────────────────"
            "─────────",
            hex_dump(data),
        ]
        if len(data) > 4096:
            lines.append(
                f"\n… ({len(data) - 4096} more bytes not shown)")

        set_text(self._prev_text, "\n".join(lines))

    # ---- object map (data from C endpoint) ----

    def _load_object_layout(self, hash_hex: str, comp: int) -> None:
        if not self._repo_path:
            return
        repo_path = self._repo_path

        def _worker() -> None:
            try:
                d = call(repo_path, "object_layout", hash=hash_hex)
            except RPCError:
                ui_call(self._clear_map)
                return
            ui_call(lambda: self._apply_layout(d, comp))

        threading.Thread(target=_worker, daemon=True).start()

    def _clear_map(self) -> None:
        self._map_segments = []
        self._seg_starts = []
        self._map_total_bytes = 0
        self._map_canvas.delete("all")
        self._map_info_label.config(text="")

    def _apply_layout(self, d: dict, comp: int) -> None:
        file_sz = int(d.get("file_size", 0))
        segments = d.get("segments", [])
        segs: list[tuple] = []
        for seg in segments:
            kind = seg["kind"]
            off  = int(seg["offset"])
            sz   = int(seg["size"])

            # Resolve color
            if kind == "payload":
                color = _COMP_COLOR.get(comp, _C_NONE)
            else:
                color = _KIND_COLOR.get(kind, _C_UNKNOWN)

            # Build description
            label = _KIND_LABEL.get(kind, kind)
            text = f"{label}  ({fmt_size(sz)})"

            segs.append((off, off + sz, color, text))

        self._map_segments = segs
        self._seg_starts = [s[0] for s in segs]
        self._map_total_bytes = file_sz

        # Build display coords (log scale)
        if not segs or not file_sz:
            self._display_segments = []
            self._display_starts = []
            self._display_total = 0
            self._map_canvas.delete("all")
            return

        SCALE = 10_000_000
        log_sizes = [math.log2(1 + (end - start))
                     for start, end, _, _ in segs]
        total_log = sum(log_sizes) or 1.0

        cumulative = 0
        d_segs: list[tuple] = []
        d_starts: list[int] = []
        for i, (_, _, color, text) in enumerate(segs):
            visual = max(1, int(log_sizes[i] / total_log * SCALE))
            d_starts.append(cumulative)
            d_segs.append((cumulative, cumulative + visual, color, text))
            cumulative += visual

        self._display_segments = d_segs
        self._display_starts = d_starts
        self._display_total = cumulative

        self._redraw_map()

    def _redraw_map(self) -> None:
        self._map_canvas.delete("all")
        if not self._display_segments or not self._display_total:
            return

        W = self._map_canvas.winfo_width() or 700
        total_display = self._display_total
        bpr = max(1, total_display // 500)
        row_h = 8
        n_rows = (total_display + bpr - 1) // bpr
        content_h = n_rows * row_h
        self._map_content_h = content_h
        self._map_bpr = bpr
        self._map_row_h = row_h

        self._map_canvas.configure(scrollregion=(0, 0, W, content_h))
        self._map_info_label.config(
            text=f"{n_rows} rows  ·  log scale  ·  "
                 f"{fmt_size(self._map_total_bytes)} total on disk")

        for d_start, d_end, color, _ in self._display_segments:
            self._draw_segment(d_start, d_end, color, bpr, W, row_h)

    def _draw_segment(self, start: int, end: int, color: str,
                      bpr: int, W: int, row_h: int) -> None:
        if start >= end:
            return
        first_row = start // bpr
        last_row = (end - 1) // bpr
        border = "#1e1e1e"

        def _x(offset_in_row: int) -> int:
            return int(W * offset_in_row / bpr)

        if first_row == last_row:
            x0 = _x(start % bpr)
            x1 = max(x0 + 1, _x((end - 1) % bpr + 1))
            y0 = first_row * row_h
            self._map_canvas.create_rectangle(
                x0, y0, x1, y0 + row_h, fill=color, outline=border)
            return

        s_x0 = _x(start % bpr)
        e_x1 = max(1, _x((end - 1) % bpr + 1))

        y0 = first_row * row_h
        self._map_canvas.create_rectangle(
            s_x0, y0, W, y0 + row_h, fill=color, outline="")

        if last_row > first_row + 1:
            y_mid0 = (first_row + 1) * row_h
            y_mid1 = last_row * row_h
            self._map_canvas.create_rectangle(
                0, y_mid0, W, y_mid1, fill=color, outline="")

        y0 = last_row * row_h
        self._map_canvas.create_rectangle(
            0, y0, e_x1, y0 + row_h, fill=color, outline="")

        fy0 = first_row * row_h
        fy1 = fy0 + row_h
        ly0 = last_row * row_h
        ly1 = ly0 + row_h

        self._map_canvas.create_line(s_x0, fy0, W, fy0, fill=border)
        self._map_canvas.create_line(W, fy0, W, ly0, fill=border)
        if s_x0 > 0:
            self._map_canvas.create_line(
                s_x0, fy0, s_x0, fy1, fill=border)
            self._map_canvas.create_line(
                0, fy1, s_x0, fy1, fill=border)
        self._map_canvas.create_line(0, fy1, 0, ly1, fill=border)
        self._map_canvas.create_line(
            0, ly1, e_x1, ly1, fill=border)
        if e_x1 < W:
            self._map_canvas.create_line(
                e_x1, ly0, e_x1, ly1, fill=border)
            self._map_canvas.create_line(
                e_x1, ly0, W, ly0, fill=border)

    # ---- map interaction ----

    def _on_map_motion(self, event: tk.Event) -> None:
        d_segs = getattr(self, "_display_segments", None)
        if not d_segs:
            return
        W = self._map_canvas.winfo_width() or 700
        bpr = getattr(self, "_map_bpr", 1)
        row_h = getattr(self, "_map_row_h", 8)
        content_h = getattr(self, "_map_content_h", 0)
        if content_h > 0:
            y0_frac = self._map_canvas.yview()[0]
            cy = event.y + y0_frac * content_h
        else:
            cy = float(self._map_canvas.canvasy(event.y))
        row = max(0, int(cy / row_h))
        col = max(0.0, min(1.0, event.x / W))
        pos = min(row * bpr + int(col * bpr),
                  max(self._display_total - 1, 0))

        idx = bisect.bisect_right(self._display_starts, pos) - 1
        if 0 <= idx < len(d_segs):
            seg = d_segs[idx]
            if seg[0] <= pos < seg[1]:
                real = self._map_segments[idx]
                real_sz = real[1] - real[0]
                self._map_detail.config(
                    text=f"{seg[3]}  [{real[0]:,}–{real[1]:,}]  "
                         f"{fmt_size(real_sz)}",
                    fg="black")
                return
        self._map_detail.config(text="", fg="gray")

    # ---- export ----

    def _export(self) -> None:
        if not self._current_hash or not self._repo_path:
            messagebox.showinfo("Nothing selected",
                                "Select an object first.")
            return
        try:
            cr = call(self._repo_path, "object_content",
                      hash=self._current_hash)
            data = base64.b64decode(cr.get("content_base64", ""))
        except RPCError as e:
            messagebox.showerror("Error",
                                 f"Could not load object: {e}")
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
            messagebox.showinfo("Saved",
                                f"Wrote {fmt_size(len(data))} to:\n{dest}")
        except OSError as e:
            messagebox.showerror("Write error", str(e))
