"""
Shared tkinter widget helpers used by every tab.
"""

import tkinter as tk
from tkinter import ttk

FONT_MONO = ("Courier", 10)
FONT_BOLD = ("TkDefaultFont", 10, "bold")
PAD = 4


def make_text_widget(parent) -> tk.Text:
    """Scrollable, read-only monospace Text widget that fills its parent."""
    t = tk.Text(parent, font=FONT_MONO, wrap=tk.NONE, state=tk.DISABLED)
    sb_y = ttk.Scrollbar(parent, orient=tk.VERTICAL,   command=t.yview)
    sb_x = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=t.xview)
    t.config(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
    sb_y.pack(side=tk.RIGHT,  fill=tk.Y)
    sb_x.pack(side=tk.BOTTOM, fill=tk.X)
    t.pack(fill=tk.BOTH, expand=True)
    return t


def make_text_tab(nb: ttk.Notebook, label: str) -> tk.Text:
    """Add a new tab to a Notebook containing a single read-only Text widget."""
    frame = ttk.Frame(nb)
    nb.add(frame, text=label)
    return make_text_widget(frame)


def set_text(widget: tk.Text, text: str) -> None:
    """Replace all content in a read-only Text widget."""
    widget.config(state=tk.NORMAL)
    widget.delete("1.0", tk.END)
    widget.insert(tk.END, text)
    widget.config(state=tk.DISABLED)


from .constants import UI_SIZE_LIMIT as _PREVIEW_LIMIT
_COLOR_DATA     = "#4e79a7"
_COLOR_HOLE     = "#e0e0e0"


def make_sparse_canvas(parent) -> tuple:
    """Pack a legend, a canvas bar, and a stats label into parent.
    Returns (canvas, stats_label).
    """
    legend = tk.Frame(parent)
    legend.pack(anchor="w", padx=PAD, pady=(PAD, 2))
    for color, lbl in ((_COLOR_DATA, "Data"), (_COLOR_HOLE, "Hole")):
        tk.Label(legend, bg=color, width=3, relief=tk.GROOVE).pack(side=tk.LEFT, padx=(0, 2))
        tk.Label(legend, text=f"{lbl}   ").pack(side=tk.LEFT)
    canvas = tk.Canvas(parent, height=52, bg="white", highlightthickness=0)
    canvas.pack(fill=tk.X, padx=PAD)
    stats = tk.Label(parent, text="", font=FONT_MONO, justify=tk.LEFT)
    stats.pack(anchor="w", padx=PAD, pady=(PAD, 0))
    return canvas, stats


def update_sparse_map(canvas: tk.Canvas, stats: tk.Label,
                      regions: list, file_size: int) -> None:
    """Draw sparse regions onto canvas and update the stats label."""
    from .formats import fmt_size
    canvas.delete("all")
    if not regions or not file_size:
        stats.config(text="(no regions)")
        return
    total_data = sum(ln for _, ln in regions)
    total_hole = file_size - total_data
    pct = 100.0 * total_data / file_size
    stats.config(
        text=(f"{len(regions)} region(s)  |  "
              f"data: {fmt_size(total_data)}  "
              f"holes: {fmt_size(total_hole)}  "
              f"({pct:.1f}% data)")
    )

    def _draw(_event=None, _r=regions, _sz=file_size):
        canvas.delete("all")
        W = canvas.winfo_width() or 700
        canvas.create_rectangle(0, 4, W, 48, fill=_COLOR_HOLE, outline="#aaa")
        for offset, length in _r:
            x0 = int(W * offset / _sz)
            x1 = max(x0 + 1, int(W * (offset + length) / _sz))
            canvas.create_rectangle(x0, 4, x1, 48, fill=_COLOR_DATA, outline="")

    canvas.bind("<Configure>", _draw)
    canvas.after(80, _draw)


def show_object_preview(parent_widget, raw_hash: bytes, scan: dict,
                        filename: str | None = None,
                        decode_as_symlink: bool = False) -> None:
    """Open a tabbed Toplevel with Info / Content / Sparse Map for an object."""
    from .parsers import find_object, decompress_payload, HAS_LZ4, parse_sparse_regions
    from .formats import fmt_size, hex_dump, hex_hash
    from .constants import (OBJECT_TYPE_NAMES, COMPRESS_NAMES,
                            COMPRESS_NONE, OBJECT_TYPE_SPARSE)

    win = tk.Toplevel(parent_widget)
    win.title(filename if filename else f"Object  {raw_hash.hex()[:20]}…")
    win.geometry("860x580")
    win.resizable(True, True)

    nb = ttk.Notebook(win)
    nb.pack(fill=tk.BOTH, expand=True, padx=PAD, pady=PAD)

    # ── resolve object ────────────────────────────────────────────────────
    result = find_object(raw_hash, scan)

    # ── Info tab ──────────────────────────────────────────────────────────
    info_outer = ttk.Frame(nb)
    nb.add(info_outer, text="Info")

    if result is None:
        tk.Label(info_outer, text="Object not found in repository.",
                 fg="red", font=FONT_BOLD).pack(padx=PAD * 2, pady=PAD, anchor="w")
        tk.Label(info_outer, text=raw_hash.hex(),
                 font=FONT_MONO).pack(padx=PAD * 2, anchor="w")
        return

    kind, location, otype, comp, uncomp_sz, comp_sz, payload = result
    ratio_str = f"{comp_sz / uncomp_sz:.4f}" if uncomp_sz and comp_sz else "—"

    # Decode symlink target from the already-loaded payload — no extra I/O.
    symlink_target = None
    if decode_as_symlink:
        try:
            raw_data = decompress_payload(payload, comp, uncomp_sz) if comp != COMPRESS_NONE else payload
            if raw_data:
                symlink_target = raw_data.rstrip(b"\x00").decode("utf-8", errors="replace")
        except Exception:
            pass

    # For loose objects, read pack_skip_ver from the header for display.
    pack_skip_ver = None
    if kind == "loose":
        from .parsers import parse_loose_object
        from .constants import PROBER_VERSION
        try:
            meta = parse_loose_object(location)
            pack_skip_ver = meta.get("pack_skip_ver")
        except Exception:
            pass

    rows = []
    if filename:
        rows.append(("Filename", filename))
    if symlink_target is not None:
        rows.append(("Symlink target", symlink_target))
    rows += [
        ("Hash",              raw_hash.hex()),
        ("Location",          f"{kind}  —  {location}"),
        ("Object type",       OBJECT_TYPE_NAMES.get(otype, str(otype))),
        ("Compression",       COMPRESS_NAMES.get(comp, str(comp))),
        ("Uncompressed size", f"{fmt_size(uncomp_sz)}  ({uncomp_sz:,} bytes)"),
        ("Compressed size",   f"{fmt_size(comp_sz)}  ({comp_sz:,} bytes)"),
        ("Ratio",             ratio_str),
    ]
    if pack_skip_ver is not None:
        if pack_skip_ver == 0:
            skip_str = "0  (unevaluated)"
        elif pack_skip_ver == PROBER_VERSION:
            skip_str = f"{pack_skip_ver}  (skip — incompressible)"
        else:
            skip_str = f"{pack_skip_ver}  (stale skip marker)"
        rows.append(("Pack skip ver", skip_str))

    # card-style container
    card = ttk.LabelFrame(info_outer, text="Object metadata", padding=(PAD * 2, PAD))
    card.pack(fill=tk.X, padx=PAD * 3, pady=PAD * 3, anchor="n")

    for r, (label, value) in enumerate(rows):
        tk.Label(card, text=label, anchor="e", width=18,
                 font=FONT_BOLD).grid(row=r, column=0, sticky="e", pady=3, padx=(0, PAD))
        ent = tk.Entry(card, font=FONT_MONO, relief=tk.FLAT, bd=0,
                       highlightthickness=0)
        ent.insert(0, value)
        ent.config(state="readonly")
        ent.grid(row=r, column=1, sticky="ew", pady=3)

    card.columnconfigure(1, weight=1)

    # ── Text / Hex tabs ───────────────────────────────────────────────────
    def _labeled_text_tab(label: str):
        """Tab with a status bar above a clean text widget."""
        frame = ttk.Frame(nb)
        nb.add(frame, text=label)
        bar = tk.Frame(frame, bd=1, relief=tk.FLAT)
        bar.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))
        status = tk.Label(bar, text="", font=FONT_MONO, fg="gray", anchor="w")
        status.pack(side=tk.LEFT)
        widget = make_text_widget(frame)
        return widget, status

    text_widget, text_status = _labeled_text_tab("Text")
    hex_widget,  hex_status  = _labeled_text_tab("Hex")

    if not HAS_LZ4 and comp != COMPRESS_NONE:
        msg = "Install the 'lz4' Python package to decompress this object."
        set_text(text_widget, msg)
        set_text(hex_widget,  msg)
    elif uncomp_sz > _PREVIEW_LIMIT:
        msg = f"Object too large to preview ({fmt_size(uncomp_sz)}, limit {fmt_size(_PREVIEW_LIMIT)})."
        set_text(text_widget, msg)
        set_text(hex_widget,  msg)
    else:
        data = decompress_payload(payload, comp, uncomp_sz)
        if data is None:
            set_text(text_widget, "(decompression failed)")
            set_text(hex_widget,  "(decompression failed)")
        else:
            size_str = f"{fmt_size(len(data))}  ({len(data):,} bytes)"
            hex_status.config(text=size_str)

            # Text tab — raw decoded content only
            try:
                decoded = data.decode("utf-8")
                body = decoded[:4096]
                suffix = (f"\n… {len(decoded) - 4096:,} more chars not shown"
                          if len(decoded) > 4096 else "")
                text_status.config(text=f"{size_str}  —  UTF-8")
                set_text(text_widget, body + suffix)
            except UnicodeDecodeError:
                text_status.config(text=f"{size_str}  —  binary")
                set_text(text_widget, "(binary data — not valid UTF-8)")

            # Hex tab — raw hex dump only
            set_text(hex_widget, hex_dump(data))

    # ── Sparse Map tab (any SPARSE object) ────────────────────────────────
    if otype == OBJECT_TYPE_SPARSE:
        sparse_frame = ttk.Frame(nb)
        nb.add(sparse_frame, text="Sparse Map")
        sp_canvas, sp_stats = make_sparse_canvas(sparse_frame)

        # Decompress if needed before parsing the region list
        sparse_raw = payload if comp == COMPRESS_NONE else decompress_payload(payload, comp, uncomp_sz)
        if sparse_raw is None:
            sp_stats.config(text="(decompression failed — cannot parse sparse map)")
        else:
            regions = parse_sparse_regions(sparse_raw)
            if regions is None:
                sp_stats.config(text="(could not parse sparse header)")
            else:
                update_sparse_map(sp_canvas, sp_stats, regions, uncomp_sz)


def make_tree(parent, columns: list[tuple], height: int | None = None) -> ttk.Treeview:
    """
    Create a Treeview with scrollbars.
    columns: list of (name, width, stretch) tuples.
    Returns the Treeview; callers pack/grid their own container.
    """
    col_ids = [c[0] for c in columns]
    kw = {"columns": col_ids, "show": "headings", "selectmode": "browse"}
    if height:
        kw["height"] = height
    tree = ttk.Treeview(parent, **kw)
    for name, width, stretch in columns:
        tree.heading(name, text=name.replace("_", " ").title())
        tree.column(name, width=width, stretch=stretch)
    sb_y = ttk.Scrollbar(parent, orient=tk.VERTICAL,   command=tree.yview)
    sb_x = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=tree.xview)
    tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
    sb_y.pack(side=tk.RIGHT,  fill=tk.Y)
    sb_x.pack(side=tk.BOTTOM, fill=tk.X)
    tree.pack(fill=tk.BOTH, expand=True)
    return tree
