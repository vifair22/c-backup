import bisect
import math
import os
import struct
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..parsers import (
    parse_pack_dat, parse_pack_idx,
    DAT_HDR_FMT, DAT_HDR_SIZE,
    DAT_ENTRY_V2_FMT, DAT_ENTRY_V2_SIZE,
    DAT_ENTRY_V1_FMT, DAT_ENTRY_V1_SIZE,
    decompress_payload, HAS_LZ4,
)
from ..formats import fmt_size, hex_hash
from ..constants import (OBJECT_TYPE_NAMES, COMPRESS_NAMES,
                         PACK_VERSION_V1, COMPRESS_NONE,
                         COMPRESS_LZ4, COMPRESS_LZ4_FRAME)
from ..widgets import make_text_widget, set_text, PAD, FONT_MONO, FONT_BOLD

_ENTRY_PAGE = 500

# Segment colours
_C_FILE_HDR  = "#4a4a4a"
_C_ENTRY_HDR = "#a0a0a0"
_C_NONE      = "#5ba3d0"
_C_LZ4       = "#f28e2b"
_C_LZ4_FRAME      = "#e15759"
_C_LZ4_FRAME_FLAT = "#f0c060"   # lz4-frame with ratio ≥ 1.0 (no compression benefit)
_C_UNKNOWN        = "#cccccc"

_COMP_COLOR = {
    COMPRESS_NONE:      _C_NONE,
    COMPRESS_LZ4:       _C_LZ4,
    COMPRESS_LZ4_FRAME: _C_LZ4_FRAME,
}

_LEGEND = [
    (_C_FILE_HDR,       "File header"),
    (_C_ENTRY_HDR,      "Entry header"),
    (_C_NONE,           "Payload — uncompressed"),
    (_C_LZ4,            "Payload — lz4-block"),
    (_C_LZ4_FRAME,      "Payload — lz4-frame"),
    (_C_LZ4_FRAME_FLAT, "Payload — lz4-frame (ratio ≥ 0.99)"),
]

_BPR_PRESETS = [
    ("auto",    0),
    ("512 B",   512),
    ("1 KB",    1024),
    ("4 KB",    4096),
    ("16 KB",   16384),
    ("64 KB",   65536),
    ("256 KB",  262144),
    ("1 MB",    1048576),
]
_BPR_LABELS = [p[0] for p in _BPR_PRESETS]

_RH_PRESETS = [4, 6, 8, 12, 16]


class PacksTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Pack Files")
        self._pack_paths: list[str] = []
        self._current_dat: str | None = None
        self._entries: list[dict] = []
        # (byte_start, byte_end, color, hover_text, entry_index|None)
        self._map_segments: list[tuple] = []
        self._seg_starts:   list[int]   = []   # parallel list for bisect
        self._map_total_bytes: int = 0
        # Display-space coordinates (log or linear depending on scale mode)
        self._display_segments: list[tuple] = []
        self._display_starts:   list[int]   = []
        self._display_total: int = 0
        self._load_generation: int = 0         # discard stale threaded results
        self._build()

    # ---- build ----

    def _build(self) -> None:
        pane = ttk.PanedWindow(self._frame, orient=tk.HORIZONTAL)
        pane.pack(fill=tk.BOTH, expand=True)

        # Left list
        left = ttk.Frame(pane)
        pane.add(left, weight=1)
        tk.Label(left, text="Pack files", font=FONT_BOLD).pack(anchor="w", padx=PAD)
        self._list = tk.Listbox(left, font=FONT_MONO, width=28)
        sb = ttk.Scrollbar(left, command=self._list.yview)
        self._list.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._list.pack(fill=tk.BOTH, expand=True)
        self._list.bind("<<ListboxSelect>>", self._on_select)

        # Right sub-notebook
        right = ttk.Frame(pane)
        pane.add(right, weight=4)
        self._nb = ttk.Notebook(right)
        self._nb.pack(fill=tk.BOTH, expand=True)

        self._hdr_text    = self._make_text_tab("Header")
        self._entry_frame = ttk.Frame(self._nb)
        self._nb.add(self._entry_frame, text="Entries (.dat)")
        self._idx_frame   = ttk.Frame(self._nb)
        self._nb.add(self._idx_frame, text="Index (.idx)")
        self._map_frame   = ttk.Frame(self._nb)
        self._nb.add(self._map_frame, text="Pack Map")

        self._build_entry_tree()
        self._build_idx_tree()
        self._build_pack_map()

    def _make_text_tab(self, label: str) -> tk.Text:
        frame = ttk.Frame(self._nb)
        self._nb.add(frame, text=label)
        return make_text_widget(frame)

    def _build_entry_tree(self) -> None:
        bar = tk.Frame(self._entry_frame)
        bar.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))
        self._entry_count_label = tk.Label(bar, text="", font=FONT_MONO)
        self._entry_count_label.pack(side=tk.LEFT)
        self._dat_loading_label = tk.Label(bar, text="", fg="gray", font=FONT_MONO)
        self._dat_loading_label.pack(side=tk.LEFT, padx=8)

        cols = [
            ("hash",          260, True),
            ("type",           60, False),
            ("compression",    80, False),
            ("uncompressed",  100, False),
            ("compressed",    100, False),
            ("ratio",          55, False),
        ]
        col_ids = [c[0] for c in cols]
        self._entry_tree = ttk.Treeview(self._entry_frame, columns=col_ids,
                                        show="headings", selectmode="browse")
        for name, width, stretch in cols:
            self._entry_tree.heading(name, text=name.replace("_", " ").title())
            self._entry_tree.column(name, width=width, stretch=stretch)
        sb_y = ttk.Scrollbar(self._entry_frame, orient=tk.VERTICAL,
                              command=self._entry_tree.yview)
        sb_x = ttk.Scrollbar(self._entry_frame, orient=tk.HORIZONTAL,
                              command=self._entry_tree.xview)
        self._entry_tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.pack(side=tk.RIGHT, fill=tk.Y)
        sb_x.pack(side=tk.BOTTOM, fill=tk.X)
        self._entry_tree.pack(fill=tk.BOTH, expand=True)

        self._ctx_menu = tk.Menu(self._entry_tree, tearoff=0)
        self._ctx_menu.add_command(label="Export payload…", command=self._export_selected)
        self._entry_tree.bind("<Button-3>", self._on_right_click)

    def _build_idx_tree(self) -> None:
        idx_bar = tk.Frame(self._idx_frame)
        idx_bar.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))
        self._idx_count_label = tk.Label(idx_bar, text="", font=FONT_MONO)
        self._idx_count_label.pack(side=tk.LEFT)

        self._idx_tree = ttk.Treeview(self._idx_frame,
                                      columns=("hash", "dat_offset"),
                                      show="headings", selectmode="browse")
        self._idx_tree.heading("hash",       text="Hash")
        self._idx_tree.heading("dat_offset", text=".dat offset")
        self._idx_tree.column("hash",       width=340)
        self._idx_tree.column("dat_offset", width=120, stretch=False)
        sb = ttk.Scrollbar(self._idx_frame, orient=tk.VERTICAL,
                           command=self._idx_tree.yview)
        self._idx_tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._idx_tree.pack(fill=tk.BOTH, expand=True)

    def _build_pack_map(self) -> None:
        # ── Legend ────────────────────────────────────────────────────────
        legend = tk.Frame(self._map_frame)
        legend.pack(side=tk.TOP, fill=tk.X, padx=PAD, pady=(PAD, 4))
        for color, label in _LEGEND:
            swatch = tk.Frame(legend, bg=color, width=18, height=18,
                              relief=tk.GROOVE, bd=1)
            swatch.pack(side=tk.LEFT, padx=(0, 4))
            tk.Label(legend, text=label,
                     font=FONT_BOLD).pack(side=tk.LEFT, padx=(0, 16))

        # ── Controls ──────────────────────────────────────────────────────
        ctrl = tk.Frame(self._map_frame)
        ctrl.pack(side=tk.TOP, fill=tk.X, padx=PAD, pady=(0, 4))

        tk.Label(ctrl, text="Scale:").pack(side=tk.LEFT)
        self._scale_var = tk.StringVar(value="Log")
        scale_box = ttk.Combobox(ctrl, textvariable=self._scale_var,
                                 values=["Linear", "Log"], width=7, state="readonly")
        scale_box.pack(side=tk.LEFT, padx=4)
        scale_box.bind("<<ComboboxSelected>>", lambda _: self._on_scale_change())

        tk.Label(ctrl, text="  Row width:").pack(side=tk.LEFT)
        self._bpr_var = tk.StringVar(value="auto")
        self._bpr_combo = ttk.Combobox(ctrl, textvariable=self._bpr_var,
                                       values=_BPR_LABELS, width=9, state="disabled")
        self._bpr_combo.pack(side=tk.LEFT, padx=4)
        self._bpr_combo.bind("<<ComboboxSelected>>", lambda _: self._redraw_pack_map())

        tk.Label(ctrl, text="  Row height:").pack(side=tk.LEFT)
        self._rh_var = tk.StringVar(value="8")
        rh_box = ttk.Combobox(ctrl, textvariable=self._rh_var,
                              values=[str(v) for v in _RH_PRESETS],
                              width=5, state="readonly")
        rh_box.pack(side=tk.LEFT, padx=4)
        rh_box.bind("<<ComboboxSelected>>", lambda _: self._redraw_pack_map())

        self._map_info_label = tk.Label(ctrl, text="", font=FONT_MONO, fg="gray")
        self._map_info_label.pack(side=tk.LEFT, padx=(16, 0))

        # ── Hover detail bar ──────────────────────────────────────────────
        detail_frame = tk.Frame(self._map_frame, bd=1, relief=tk.SUNKEN)
        detail_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=PAD, pady=PAD)
        self._map_detail = tk.Label(detail_frame,
                                    text="Hover for details  ·  double-click to open object detail",
                                    font=FONT_MONO, fg="gray", anchor="w")
        self._map_detail.pack(fill=tk.X, padx=4, pady=3)

        # ── Scrollable canvas ─────────────────────────────────────────────
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

        self._map_canvas.bind("<Configure>", lambda _: self._redraw_pack_map())
        self._map_canvas.bind("<Motion>",    self._on_map_motion)
        self._map_canvas.bind("<Double-1>",  self._on_map_click)
        self._map_canvas.bind("<Leave>",
                              lambda _: self._map_detail.config(
                                  text="Hover for details  ·  double-click to open object detail",
                                  fg="gray"))
        # Mousewheel — Linux Button-4/5, Windows/Mac delta
        self._map_canvas.bind("<MouseWheel>",
                              lambda e: self._map_canvas.yview_scroll(
                                  int(-1 * (e.delta / 120)), "units"))
        self._map_canvas.bind("<Button-4>",
                              lambda _: self._map_canvas.yview_scroll(-3, "units"))
        self._map_canvas.bind("<Button-5>",
                              lambda _: self._map_canvas.yview_scroll(3, "units"))

    # ---- populate ----

    def populate(self, scan: dict) -> None:
        self._pack_paths = scan["pack_dat"]
        self._scan = scan
        self._list.delete(0, tk.END)
        for path in self._pack_paths:
            self._list.insert(tk.END, os.path.basename(path))

    # ---- events ----

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel:
            return
        dat_path = self._pack_paths[sel[0]]
        self.load_dat(dat_path)
        idx_path = dat_path.replace(".dat", ".idx")
        if os.path.exists(idx_path):
            self.load_idx(idx_path)

    def load_dat(self, path: str) -> None:
        """Parse the .dat file in a background thread to avoid freezing the UI."""
        self._current_dat = path
        self._load_generation += 1
        gen = self._load_generation
        self._dat_loading_label.config(text="Loading…")
        self._entry_count_label.config(text="")

        def _worker() -> None:
            try:
                d   = parse_pack_dat(path)
                fsz = os.path.getsize(path)
            except Exception as e:
                err = str(e)
                self._entry_frame.after(0,
                    lambda: self._on_dat_error(err, gen))
                return
            self._entry_frame.after(0,
                lambda: self._finish_load_dat(path, d, fsz, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _on_dat_error(self, err: str, gen: int) -> None:
        if gen != self._load_generation:
            return
        self._dat_loading_label.config(text="")
        messagebox.showerror("Parse error", err)

    def _finish_load_dat(self, path: str, d: dict, file_size: int, gen: int) -> None:
        if gen != self._load_generation:
            return
        self._dat_loading_label.config(text="")
        lines = [
            f"File    : {path}",
            f"Size    : {fmt_size(file_size)}",
            f"Magic   : 0x42504B44 (BPKD)",
            f"Version : {d['version']}",
            f"Objects : {d['count']}",
        ]
        self._entries = d["entries"]
        set_text(self._hdr_text, "\n".join(lines))

        # Paginate treeview
        rows = self._entry_tree.get_children()
        if rows:
            self._entry_tree.delete(*rows)
        shown = self._entries[:_ENTRY_PAGE]
        total = len(self._entries)
        for e in shown:
            ratio = "—"
            if e["uncompressed_size"] > 0 and e["compressed_size"] > 0:
                ratio = f"{e['compressed_size'] / e['uncompressed_size']:.3f}"
            self._entry_tree.insert("", tk.END, values=(
                hex_hash(e["hash"]),
                OBJECT_TYPE_NAMES.get(e["type"], str(e["type"])),
                COMPRESS_NAMES.get(e["compression"], str(e["compression"])),
                fmt_size(e["uncompressed_size"]),
                fmt_size(e["compressed_size"]),
                ratio,
            ))
        label = (f"Showing {_ENTRY_PAGE} of {total} entries"
                 if total > _ENTRY_PAGE else f"{total} entries")
        self._entry_count_label.config(text=label)

        self._build_map_segments(d["version"], file_size)
        self._compute_display_coords()
        self._map_canvas.after(50, self._redraw_pack_map)

    def load_idx(self, path: str) -> None:
        try:
            idx = parse_pack_idx(path)
        except Exception as e:
            messagebox.showerror("Parse error", str(e))
            return
        rows = self._idx_tree.get_children()
        if rows:
            self._idx_tree.delete(*rows)
        entries = idx["entries"]
        total   = len(entries)
        shown   = entries[:_ENTRY_PAGE]
        for e in shown:
            self._idx_tree.insert("", tk.END, values=(
                hex_hash(e["hash"]),
                f"0x{e['dat_offset']:X}",
            ))
        label = (f"Showing {_ENTRY_PAGE} of {total} entries"
                 if total > _ENTRY_PAGE else f"{total} entries")
        self._idx_count_label.config(text=label)

    # ---- pack map segments ----

    def _build_map_segments(self, version: int, file_size: int) -> None:
        entry_hdr_size = (DAT_ENTRY_V1_SIZE if version == PACK_VERSION_V1
                          else DAT_ENTRY_V2_SIZE)
        segs: list[tuple] = []

        segs.append((0, DAT_HDR_SIZE, _C_FILE_HDR,
                     f"Pack file header  ({DAT_HDR_SIZE} B)  —  magic / version / count",
                     None))

        cursor = DAT_HDR_SIZE
        for i, e in enumerate(self._entries):
            h_short   = e["hash"].hex()[:16]
            type_name = OBJECT_TYPE_NAMES.get(e["type"],      str(e["type"]))
            comp_name = COMPRESS_NAMES.get(e["compression"],  str(e["compression"]))
            comp_sz   = e["compressed_size"]
            uncomp_sz = e["uncompressed_size"]

            hdr_end = cursor + entry_hdr_size
            segs.append((cursor, hdr_end, _C_ENTRY_HDR,
                         f"Entry {i}  struct header ({entry_hdr_size} B)  —  "
                         f"{h_short}…  {type_name}  {comp_name}",
                         i))
            cursor = hdr_end

            payload_end = cursor + comp_sz
            color = _COMP_COLOR.get(e["compression"], _C_UNKNOWN)
            if (e["compression"] == COMPRESS_LZ4_FRAME
                    and uncomp_sz > 0 and comp_sz / uncomp_sz >= 0.99):
                color = _C_LZ4_FRAME_FLAT
            ratio_str = (f"  ratio {comp_sz / uncomp_sz:.3f}"
                         if uncomp_sz and comp_sz else "")
            segs.append((cursor, payload_end, color,
                         f"Entry {i}  payload  —  {h_short}…  "
                         f"{type_name}  {comp_name}  "
                         f"{fmt_size(comp_sz)} compressed → "
                         f"{fmt_size(uncomp_sz)} uncompressed{ratio_str}",
                         i))
            cursor = payload_end

        # Cover any trailing bytes not accounted for by the entry list
        # (e.g. a pack footer, alignment padding, or a size mismatch).
        if cursor < file_size:
            segs.append((cursor, file_size, _C_UNKNOWN,
                         f"Trailing / unaccounted  "
                         f"({fmt_size(file_size - cursor)})  "
                         f"bytes {cursor}–{file_size}",
                         None))

        self._map_segments  = segs
        self._seg_starts    = [s[0] for s in segs]
        self._map_total_bytes = file_size

    # ---- display coordinate mapping ----

    def _compute_display_coords(self) -> None:
        """Build the display-space coordinate system for the pack map.

        In Linear mode, display coords == byte offsets (identity).
        In Log mode, each segment's visual width is proportional to
        log2(1 + byte_size).  This compresses huge objects and enlarges
        tiny ones so that every segment is visible — like Windows Disk
        Management does for partition sizes.
        """
        if not self._map_segments:
            self._display_segments = []
            self._display_starts   = []
            self._display_total    = 0
            return

        if self._scale_var.get() == "Linear":
            self._display_segments = self._map_segments
            self._display_starts   = self._seg_starts
            self._display_total    = self._map_total_bytes
            return

        # Log-compressed scale.
        # Compute log2(1 + size) for each segment, then scale the results
        # into a large integer space so the existing grid math (which uses
        # integer // and %) works without modification.
        SCALE = 10_000_000
        log_sizes = [math.log2(1 + (end - start))
                     for start, end, _, _, _ in self._map_segments]
        total_log = sum(log_sizes) or 1.0

        cumulative = 0
        display_segs:   list[tuple] = []
        display_starts: list[int]   = []
        for i, (_, _, color, text, idx) in enumerate(self._map_segments):
            visual = max(1, int(log_sizes[i] / total_log * SCALE))
            display_starts.append(cumulative)
            display_segs.append((cumulative, cumulative + visual, color, text, idx))
            cumulative += visual

        self._display_segments = display_segs
        self._display_starts   = display_starts
        self._display_total    = cumulative

    def _on_scale_change(self) -> None:
        is_log = self._scale_var.get() != "Linear"
        self._bpr_combo.config(state="disabled" if is_log else "readonly")
        if is_log:
            self._bpr_var.set("auto")
        self._compute_display_coords()
        self._redraw_pack_map()

    # ---- drawing ----

    def _get_display_bpr(self) -> int:
        """Return the display-units-per-row value for the current settings."""
        if self._scale_var.get() != "Linear":
            # Log mode: always auto (byte-based presets are meaningless)
            return max(1, self._display_total // 500)
        val = self._bpr_var.get()
        if val == "auto":
            return max(512, self._map_total_bytes // 500)
        for label, bpr in _BPR_PRESETS:
            if label == val:
                return bpr
        return max(512, self._map_total_bytes // 500)

    def _get_row_height(self) -> int:
        try:
            return int(self._rh_var.get())
        except ValueError:
            return 8

    def _redraw_pack_map(self) -> None:
        self._map_canvas.delete("all")
        if not self._display_segments or not self._display_total:
            return
        W     = self._map_canvas.winfo_width() or 700
        bpr   = self._get_display_bpr()
        row_h = self._get_row_height()
        total = self._display_total
        n_rows    = (total + bpr - 1) // bpr
        content_h = n_rows * row_h
        self._map_content_h = content_h

        self._map_canvas.configure(scrollregion=(0, 0, W, content_h))

        if self._scale_var.get() == "Linear":
            self._map_info_label.config(
                text=f"{n_rows} rows  ·  {fmt_size(bpr)}/row  ·  "
                     f"{fmt_size(self._map_total_bytes)} total")
        else:
            self._map_info_label.config(
                text=f"{n_rows} rows  ·  log scale  ·  "
                     f"{fmt_size(self._map_total_bytes)} total")

        for d_start, d_end, color, _, _ in self._display_segments:
            self._draw_segment(d_start, d_end, color, bpr, W, row_h)

    @staticmethod
    def _guide_color(hex_color: str) -> str:
        """Blend a segment color 35% toward the canvas background (#1e1e1e)."""
        r = int(hex_color[1:3], 16)
        g = int(hex_color[3:5], 16)
        b = int(hex_color[5:7], 16)
        # bg = 0x1e = 30
        r = int(r + (30 - r) * 0.35)
        g = int(g + (30 - g) * 0.35)
        b = int(b + (30 - b) * 0.35)
        return f"#{r:02x}{g:02x}{b:02x}"

    def _draw_segment(self, start: int, end: int, color: str,
                      bpr: int, W: int, row_h: int) -> None:
        """Draw one segment onto the 2D grid.

        Sub-rectangles (first partial row, middle full rows, last partial row)
        are drawn WITHOUT outlines so there is no solid line at internal row
        wraps.  Instead, dashed guides mark internal wraps and segment
        boundaries are visible through colour differences between neighbours.
        Single-row segments keep a thin border for clarity.
        """
        if start >= end:
            return
        first_row = start // bpr
        last_row  = (end - 1) // bpr
        border    = "#1e1e1e"   # == canvas bg

        def _x(offset_in_row: int) -> int:
            return int(W * offset_in_row / bpr)

        if first_row == last_row:
            x0 = _x(start % bpr)
            x1 = max(x0 + 1, _x((end - 1) % bpr + 1))
            y0 = first_row * row_h
            self._map_canvas.create_rectangle(
                x0, y0, x1, y0 + row_h, fill=color, outline=border)
            return

        # Multi-row: draw sub-rectangles with NO outline so internal
        # shared edges don't produce solid lines.
        s_x0 = _x(start % bpr)
        e_x1 = max(1, _x((end - 1) % bpr + 1))

        # First partial row
        y0 = first_row * row_h
        self._map_canvas.create_rectangle(
            s_x0, y0, W, y0 + row_h, fill=color, outline="")

        # All full middle rows as one rectangle
        if last_row > first_row + 1:
            y_mid0 = (first_row + 1) * row_h
            y_mid1 = last_row * row_h
            self._map_canvas.create_rectangle(
                0, y_mid0, W, y_mid1, fill=color, outline="")

        # Last partial row
        y0 = last_row * row_h
        self._map_canvas.create_rectangle(
            0, y0, e_x1, y0 + row_h, fill=color, outline="")

        # Outer perimeter — thin border only on the segment's exterior edges.
        fy0 = first_row * row_h
        fy1 = fy0 + row_h
        ly0 = last_row * row_h
        ly1 = ly0 + row_h

        # Top edge of first row
        self._map_canvas.create_line(s_x0, fy0, W, fy0, fill=border)
        # Right edge (full height)
        self._map_canvas.create_line(W, fy0, W, ly0, fill=border)
        # Left step at first_row → first_row+1 (if first row is partial)
        if s_x0 > 0:
            self._map_canvas.create_line(s_x0, fy0, s_x0, fy1, fill=border)
            self._map_canvas.create_line(0, fy1, s_x0, fy1, fill=border)
        # Left edge of middle + last rows
        self._map_canvas.create_line(0, fy1, 0, ly1, fill=border)
        # Bottom edge of last row
        self._map_canvas.create_line(0, ly1, e_x1, ly1, fill=border)
        # Right step at last_row (if last row is partial)
        if e_x1 < W:
            self._map_canvas.create_line(e_x1, ly0, e_x1, ly1, fill=border)
            self._map_canvas.create_line(e_x1, ly0, W, ly0, fill=border)

        # Dashed row guides at every internal row wrap, clipped to the
        # segment's horizontal extent so they don't overlap the solid
        # perimeter on rows shared with other segments.
        n_guides = last_row - first_row
        if n_guides <= 2000:
            guide = self._guide_color(color)
            for row in range(first_row + 1, last_row + 1):
                y = row * row_h
                gx0 = s_x0 if row == first_row + 1 else 0
                gx1 = e_x1 if row == last_row else W
                self._map_canvas.create_line(
                    gx0, y, gx1, y, fill=guide, dash=(6, 2))

    # ---- interaction ----

    def _display_offset_at(self, event: tk.Event) -> int:
        """Convert a canvas mouse event to a display-space offset.

        Uses explicit yview fraction × content-height rather than canvasy() to
        avoid floating-point precision issues with very tall scroll regions.
        """
        W         = self._map_canvas.winfo_width() or 700
        bpr       = self._get_display_bpr()
        row_h     = self._get_row_height()
        content_h = getattr(self, "_map_content_h", 0)
        if content_h > 0:
            y0_frac = self._map_canvas.yview()[0]
            cy = event.y + y0_frac * content_h
        else:
            cy = float(self._map_canvas.canvasy(event.y))
        row = max(0, int(cy / row_h))
        col = max(0.0, min(1.0, event.x / W))
        return min(row * bpr + int(col * bpr), max(self._display_total - 1, 0))

    def _segment_at(self, display_pos: int):
        """O(log n) lookup of the display segment covering display_pos."""
        idx = bisect.bisect_right(self._display_starts, display_pos) - 1
        if 0 <= idx < len(self._display_segments):
            seg = self._display_segments[idx]
            if seg[0] <= display_pos < seg[1]:
                return seg
        return None

    def _on_map_motion(self, event: tk.Event) -> None:
        if not self._display_segments or not self._display_total:
            return
        seg = self._segment_at(self._display_offset_at(event))
        if seg:
            self._map_detail.config(text=seg[3], fg="black")
        else:
            self._map_detail.config(text="", fg="gray")

    def _on_map_click(self, event: tk.Event) -> None:
        if not self._display_segments or not self._display_total:
            return
        seg = self._segment_at(self._display_offset_at(event))
        if seg and seg[4] is not None:
            entry_idx = seg[4]
            if entry_idx < len(self._entries):
                from ..widgets import show_object_preview
                show_object_preview(self._map_frame,
                                    self._entries[entry_idx]["hash"],
                                    self._scan)

    # ---- right-click export ----

    def _on_right_click(self, event: tk.Event) -> None:
        item = self._entry_tree.identify_row(event.y)
        if item:
            self._entry_tree.selection_set(item)
            self._ctx_menu.tk_popup(event.x_root, event.y_root)

    def _export_selected(self) -> None:
        sel = self._entry_tree.selection()
        if not sel or not self._current_dat:
            return
        row = self._entry_tree.item(sel[0])["values"]
        hash_hex = row[0]
        try:
            raw_hash = bytes.fromhex(hash_hex)
        except ValueError:
            return

        try:
            with open(self._current_dat, "rb") as f:
                magic, version, count = struct.unpack(DAT_HDR_FMT, f.read(DAT_HDR_SIZE))
                payload   = None
                comp      = COMPRESS_NONE
                uncomp_sz = 0
                for _ in range(count):
                    if version == PACK_VERSION_V1:
                        h, otype, c, u, csz = struct.unpack(
                            DAT_ENTRY_V1_FMT, f.read(DAT_ENTRY_V1_SIZE))
                    else:
                        h, otype, c, u, csz = struct.unpack(
                            DAT_ENTRY_V2_FMT, f.read(DAT_ENTRY_V2_SIZE))
                    data = f.read(csz)
                    if h == raw_hash:
                        payload   = data
                        comp      = c
                        uncomp_sz = u
                        break
        except OSError as e:
            messagebox.showerror("Read error", str(e))
            return

        if payload is None:
            messagebox.showerror("Not found", "Entry not found in .dat file.")
            return

        if comp != COMPRESS_NONE:
            if not HAS_LZ4:
                messagebox.showerror("Error", "lz4 not installed — cannot decompress.")
                return
            out_data = decompress_payload(payload, comp, uncomp_sz)
            if out_data is None:
                messagebox.showerror("Error", "Decompression failed.")
                return
        else:
            out_data = payload

        dest = filedialog.asksaveasfilename(
            title="Save decompressed payload",
            initialfile=f"{hash_hex[:16]}.bin",
        )
        if not dest:
            return
        try:
            with open(dest, "wb") as f:
                f.write(out_data)
            messagebox.showinfo("Saved",
                                f"Wrote {fmt_size(len(out_data))} to:\n{dest}")
        except OSError as e:
            messagebox.showerror("Write error", str(e))
