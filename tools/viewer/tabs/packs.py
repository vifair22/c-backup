import base64
import bisect
import math
import os
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..rpc import call, RPCError
from ..formats import fmt_size
from ..constants import (OBJECT_TYPE_NAMES, COMPRESS_NAMES,
                         PACK_DAT_HDR_SIZE,
                         COMPRESS_NONE, COMPRESS_LZ4, COMPRESS_LZ4_FRAME)
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
        self._repo_path: str | None = None
        self._pack_names: list[str] = []
        self._current_dat: str | None = None
        self._entries: list[dict] = []
        self._map_segments: list[tuple] = []
        self._seg_starts:   list[int]   = []
        self._map_total_bytes: int = 0
        self._display_segments: list[tuple] = []
        self._display_starts:   list[int]   = []
        self._display_total: int = 0
        self._load_generation: int = 0
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
        self._gidx_frame  = ttk.Frame(self._nb)
        self._nb.add(self._gidx_frame, text="Global Index")

        self._build_entry_tree()
        self._build_idx_tree()
        self._build_pack_map()
        self._build_global_index()

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
        legend = tk.Frame(self._map_frame)
        legend.pack(side=tk.TOP, fill=tk.X, padx=PAD, pady=(PAD, 4))
        for color, label in _LEGEND:
            swatch = tk.Frame(legend, bg=color, width=18, height=18,
                              relief=tk.GROOVE, bd=1)
            swatch.pack(side=tk.LEFT, padx=(0, 4))
            tk.Label(legend, text=label,
                     font=FONT_BOLD).pack(side=tk.LEFT, padx=(0, 16))

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

        detail_frame = tk.Frame(self._map_frame, bd=1, relief=tk.SUNKEN)
        detail_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=PAD, pady=PAD)
        self._map_detail = tk.Label(detail_frame,
                                    text="Hover for details  ·  double-click to open object detail",
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

        self._map_canvas.bind("<Configure>", lambda _: self._redraw_pack_map())
        self._map_canvas.bind("<Motion>",    self._on_map_motion)
        self._map_canvas.bind("<Double-1>",  self._on_map_click)
        self._map_canvas.bind("<Leave>",
                              lambda _: self._map_detail.config(
                                  text="Hover for details  ·  double-click to open object detail",
                                  fg="gray"))
        self._map_canvas.bind("<MouseWheel>",
                              lambda e: self._map_canvas.yview_scroll(
                                  int(-1 * (e.delta / 120)), "units"))
        self._map_canvas.bind("<Button-4>",
                              lambda _: self._map_canvas.yview_scroll(-3, "units"))
        self._map_canvas.bind("<Button-5>",
                              lambda _: self._map_canvas.yview_scroll(3, "units"))

    def _build_global_index(self) -> None:
        # Header info
        self._gidx_hdr_text = make_text_widget(
            self._gidx_frame, height=6)
        self._gidx_hdr_text.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))

        # Fanout bar chart canvas
        fan_lbl = tk.Label(self._gidx_frame, text="Fanout distribution",
                           font=FONT_BOLD)
        fan_lbl.pack(anchor="w", padx=PAD, pady=(PAD, 0))
        self._fan_canvas = tk.Canvas(self._gidx_frame, bg="white",
                                     height=120, highlightthickness=0)
        self._fan_canvas.pack(fill=tk.X, padx=PAD, pady=(0, PAD))
        self._fan_canvas.bind("<Configure>", lambda _: self._redraw_fanout())
        self._fan_canvas.bind("<Motion>", self._on_fan_motion)
        self._fan_data: list[int] = []

        # Entries tree (paginated)
        bar = tk.Frame(self._gidx_frame)
        bar.pack(fill=tk.X, padx=PAD, pady=(0, 0))
        self._gidx_count_label = tk.Label(bar, text="", font=FONT_MONO)
        self._gidx_count_label.pack(side=tk.LEFT)
        self._gidx_loading = tk.Label(bar, text="", fg="gray", font=FONT_MONO)
        self._gidx_loading.pack(side=tk.LEFT, padx=8)

        cols = [
            ("hash",         260, True),
            ("pack_num",      80, False),
            ("dat_offset",   110, False),
            ("pack_version",  80, False),
            ("entry_index",   80, False),
        ]
        col_ids = [c[0] for c in cols]
        self._gidx_tree = ttk.Treeview(self._gidx_frame, columns=col_ids,
                                        show="headings", selectmode="browse")
        for name, width, stretch in cols:
            self._gidx_tree.heading(name, text=name.replace("_", " ").title())
            self._gidx_tree.column(name, width=width, stretch=stretch)
        sb = ttk.Scrollbar(self._gidx_frame, orient=tk.VERTICAL,
                            command=self._gidx_tree.yview)
        self._gidx_tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._gidx_tree.pack(fill=tk.BOTH, expand=True)

    def _load_global_index(self) -> None:
        if not self._repo_path:
            return
        self._gidx_loading.config(text="Loading…")
        repo_path = self._repo_path

        def _worker() -> None:
            try:
                d = call(repo_path, "global_pack_index")
            except RPCError:
                self._gidx_frame.after(0, lambda: self._gidx_loading.config(
                    text="No global index available"))
                return
            self._gidx_frame.after(0, lambda: self._finish_gidx(d))

        threading.Thread(target=_worker, daemon=True).start()

    def _finish_gidx(self, d: dict) -> None:
        self._gidx_loading.config(text="")
        hdr = d.get("header", {})
        lines = [
            f"Magic       : 0x{int(hdr.get('magic', 0)):08X}",
            f"Version     : {hdr.get('version', '?')}",
            f"Entry count : {hdr.get('entry_count', '?'):,}",
            f"Pack count  : {hdr.get('pack_count', '?'):,}",
        ]
        set_text(self._gidx_hdr_text, "\n".join(lines))

        # Fanout
        self._fan_data = [int(x) for x in d.get("fanout", [])]
        self._redraw_fanout()

        # Entries
        rows = self._gidx_tree.get_children()
        if rows:
            self._gidx_tree.delete(*rows)
        entries = d.get("entries", [])
        total = int(hdr.get("entry_count", len(entries)))
        shown = entries[:_ENTRY_PAGE]
        for e in shown:
            self._gidx_tree.insert("", tk.END, values=(
                e["hash"],
                e["pack_num"],
                f"0x{int(e['dat_offset']):X}",
                e["pack_version"],
                e["entry_index"],
            ))
        label = (f"Showing {min(_ENTRY_PAGE, len(shown))} of {total:,} entries"
                 if total > _ENTRY_PAGE else f"{total:,} entries")
        self._gidx_count_label.config(text=label)

    def _redraw_fanout(self) -> None:
        c = self._fan_canvas
        c.delete("all")
        if not self._fan_data or len(self._fan_data) < 256:
            return
        W = c.winfo_width() or 700
        H = c.winfo_height() or 120
        # Compute bucket sizes (fanout is cumulative)
        buckets = []
        for i in range(256):
            prev = self._fan_data[i - 1] if i > 0 else 0
            buckets.append(self._fan_data[i] - prev)
        mx = max(buckets) if buckets else 1
        if mx == 0:
            mx = 1
        bar_w = W / 256.0
        for i, count in enumerate(buckets):
            h = int(H * count / mx) if mx else 0
            x0 = int(i * bar_w)
            x1 = int((i + 1) * bar_w)
            c.create_rectangle(x0, H - h, x1, H, fill="#5ba3d0", outline="")

    def _on_fan_motion(self, event: tk.Event) -> None:
        if not self._fan_data or len(self._fan_data) < 256:
            return
        W = self._fan_canvas.winfo_width() or 700
        idx = min(255, max(0, int(event.x * 256 / W)))
        prev = self._fan_data[idx - 1] if idx > 0 else 0
        count = self._fan_data[idx] - prev
        self._fan_canvas.delete("tip")
        self._fan_canvas.create_text(
            event.x + 10, max(10, event.y - 10),
            text=f"0x{idx:02X}: {count:,} entries",
            anchor="w", font=FONT_MONO, tag="tip")

    # ---- populate ----

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path
        self._pack_names = []
        self._list.delete(0, tk.END)

        try:
            scan = call(repo_path, "scan")
            for pk in sorted(scan.get("packs", []), key=lambda p: p["name"]):
                self._pack_names.append(pk["name"])
                self._list.insert(tk.END, pk["name"])
        except RPCError:
            pass

        self._load_global_index()

    # ---- events ----

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel or not self._repo_path:
            return
        dat_name = self._pack_names[sel[0]]
        self.load_dat(dat_name)
        idx_name = dat_name.replace(".dat", ".idx")
        self.load_idx(idx_name)

    def load_dat(self, dat_name: str) -> None:
        if not self._repo_path:
            return
        self._current_dat = dat_name
        self._load_generation += 1
        gen = self._load_generation
        self._dat_loading_label.config(text="Loading…")
        self._entry_count_label.config(text="")

        repo_path = self._repo_path

        def _worker() -> None:
            try:
                d = call(repo_path, "pack_entries", name=dat_name)
                fsz = int(d.get("file_size", 0))
            except RPCError as e:
                err = str(e)
                self._entry_frame.after(0,
                    lambda: self._on_dat_error(err, gen))
                return
            self._entry_frame.after(0,
                lambda: self._finish_load_dat(dat_name, d, fsz, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _on_dat_error(self, err: str, gen: int) -> None:
        if gen != self._load_generation:
            return
        self._dat_loading_label.config(text="")
        messagebox.showerror("Parse error", err)

    def _finish_load_dat(self, dat_name: str, d: dict, file_size: int, gen: int) -> None:
        if gen != self._load_generation:
            return
        self._dat_loading_label.config(text="")

        dat_path = os.path.join(self._repo_path, "packs", dat_name) if self._repo_path else dat_name
        lines = [
            f"File    : {dat_path}",
            f"Size    : {fmt_size(file_size)}",
            f"Magic   : 0x42504B44 (BPKD)",
            f"Version : {d.get('version', '?')}",
            f"Objects : {d.get('count', '?')}",
        ]
        self._entries = d.get("entries", [])
        set_text(self._hdr_text, "\n".join(lines))

        # Paginate treeview
        rows = self._entry_tree.get_children()
        if rows:
            self._entry_tree.delete(*rows)
        shown = self._entries[:_ENTRY_PAGE]
        total = len(self._entries)
        for e in shown:
            uncomp = int(e["uncompressed_size"])
            comp   = int(e["compressed_size"])
            ratio = "—"
            if uncomp > 0 and comp > 0:
                ratio = f"{comp / uncomp:.3f}"
            self._entry_tree.insert("", tk.END, values=(
                e["hash"],
                OBJECT_TYPE_NAMES.get(int(e["type"]), str(e["type"])),
                COMPRESS_NAMES.get(int(e["compression"]), str(e["compression"])),
                fmt_size(uncomp),
                fmt_size(comp),
                ratio,
            ))
        label = (f"Showing {_ENTRY_PAGE} of {total} entries"
                 if total > _ENTRY_PAGE else f"{total} entries")
        self._entry_count_label.config(text=label)

        self._build_map_segments(file_size)
        self._compute_display_coords()
        self._map_canvas.after(50, self._redraw_pack_map)

    def load_idx(self, idx_name: str) -> None:
        if not self._repo_path:
            return
        try:
            idx = call(self._repo_path, "pack_index", name=idx_name)
        except RPCError as e:
            messagebox.showerror("Parse error", str(e))
            return
        rows = self._idx_tree.get_children()
        if rows:
            self._idx_tree.delete(*rows)
        entries = idx.get("entries", [])
        total   = len(entries)
        shown   = entries[:_ENTRY_PAGE]
        for e in shown:
            self._idx_tree.insert("", tk.END, values=(
                e["hash"],
                f"0x{int(e['dat_offset']):X}",
            ))
        label = (f"Showing {_ENTRY_PAGE} of {total} entries"
                 if total > _ENTRY_PAGE else f"{total} entries")
        self._idx_count_label.config(text=label)

    # ---- pack map segments ----

    def _build_map_segments(self, file_size: int) -> None:
        segs: list[tuple] = []

        segs.append((0, PACK_DAT_HDR_SIZE, _C_FILE_HDR,
                     f"Pack file header  ({PACK_DAT_HDR_SIZE} B)  —  magic / version / count",
                     None))

        # Derive entry header boundaries from payload_offset
        for i, e in enumerate(self._entries):
            h_short   = e["hash"][:16]
            type_name = OBJECT_TYPE_NAMES.get(int(e["type"]),      str(e["type"]))
            comp_name = COMPRESS_NAMES.get(int(e["compression"]),  str(e["compression"]))
            comp_sz   = int(e["compressed_size"])
            uncomp_sz = int(e["uncompressed_size"])
            pay_off   = int(e["payload_offset"])

            # Entry header: from previous end to payload_offset
            prev_end = segs[-1][1] if segs else PACK_DAT_HDR_SIZE
            if pay_off > prev_end:
                hdr_size = pay_off - prev_end
                segs.append((prev_end, pay_off, _C_ENTRY_HDR,
                             f"Entry {i}  struct header ({hdr_size} B)  —  "
                             f"{h_short}…  {type_name}  {comp_name}",
                             i))

            # Payload
            payload_end = pay_off + comp_sz
            comp_code = int(e["compression"])
            color = _COMP_COLOR.get(comp_code, _C_UNKNOWN)
            if (comp_code == COMPRESS_LZ4_FRAME
                    and uncomp_sz > 0 and comp_sz / uncomp_sz >= 0.99):
                color = _C_LZ4_FRAME_FLAT
            ratio_str = (f"  ratio {comp_sz / uncomp_sz:.3f}"
                         if uncomp_sz and comp_sz else "")
            segs.append((pay_off, payload_end, color,
                         f"Entry {i}  payload  —  {h_short}…  "
                         f"{type_name}  {comp_name}  "
                         f"{fmt_size(comp_sz)} compressed → "
                         f"{fmt_size(uncomp_sz)} uncompressed{ratio_str}",
                         i))

        # Trailing bytes
        last_end = segs[-1][1] if segs else PACK_DAT_HDR_SIZE
        if last_end < file_size:
            segs.append((last_end, file_size, _C_UNKNOWN,
                         f"Trailing / unaccounted  "
                         f"({fmt_size(file_size - last_end)})  "
                         f"bytes {last_end}–{file_size}",
                         None))

        self._map_segments  = segs
        self._seg_starts    = [s[0] for s in segs]
        self._map_total_bytes = file_size

    # ---- display coordinate mapping ----

    def _compute_display_coords(self) -> None:
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
        if self._scale_var.get() != "Linear":
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
        r = int(hex_color[1:3], 16)
        g = int(hex_color[3:5], 16)
        b = int(hex_color[5:7], 16)
        r = int(r + (30 - r) * 0.35)
        g = int(g + (30 - g) * 0.35)
        b = int(b + (30 - b) * 0.35)
        return f"#{r:02x}{g:02x}{b:02x}"

    def _draw_segment(self, start: int, end: int, color: str,
                      bpr: int, W: int, row_h: int) -> None:
        if start >= end:
            return
        first_row = start // bpr
        last_row  = (end - 1) // bpr
        border    = "#1e1e1e"

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
            self._map_canvas.create_line(s_x0, fy0, s_x0, fy1, fill=border)
            self._map_canvas.create_line(0, fy1, s_x0, fy1, fill=border)
        self._map_canvas.create_line(0, fy1, 0, ly1, fill=border)
        self._map_canvas.create_line(0, ly1, e_x1, ly1, fill=border)
        if e_x1 < W:
            self._map_canvas.create_line(e_x1, ly0, e_x1, ly1, fill=border)
            self._map_canvas.create_line(e_x1, ly0, W, ly0, fill=border)

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
        if not self._display_segments or not self._display_total or not self._repo_path:
            return
        seg = self._segment_at(self._display_offset_at(event))
        if seg and seg[4] is not None:
            entry_idx = seg[4]
            if entry_idx < len(self._entries):
                from ..widgets import show_object_preview
                show_object_preview(self._map_frame,
                                    self._entries[entry_idx]["hash"],
                                    self._repo_path)

    # ---- right-click export ----

    def _on_right_click(self, event: tk.Event) -> None:
        item = self._entry_tree.identify_row(event.y)
        if item:
            self._entry_tree.selection_set(item)
            self._ctx_menu.tk_popup(event.x_root, event.y_root)

    def _export_selected(self) -> None:
        sel = self._entry_tree.selection()
        if not sel or not self._repo_path:
            return
        row = self._entry_tree.item(sel[0])["values"]
        hash_hex = row[0]

        try:
            cr = call(self._repo_path, "object_content", hash=hash_hex)
            data = base64.b64decode(cr.get("content_base64", ""))
        except RPCError as e:
            messagebox.showerror("Error", f"Could not load object: {e}")
            return

        dest = filedialog.asksaveasfilename(
            title="Save decompressed payload",
            initialfile=f"{hash_hex[:16]}.bin",
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
