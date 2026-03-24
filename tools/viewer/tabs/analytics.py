import tkinter as tk
from tkinter import ttk

from ..parsers import parse_pack_dat, parse_loose_object
from ..formats import fmt_size
from ..constants import (
    OBJECT_TYPE_NAMES,
    OBJECT_TYPE_FILE, OBJECT_TYPE_SPARSE,
    OBJECT_TYPE_XATTR, OBJECT_TYPE_ACL,
)
from ..widgets import make_text_widget, set_text, PAD, FONT_MONO, FONT_BOLD

_TYPES = [OBJECT_TYPE_FILE, OBJECT_TYPE_SPARSE, OBJECT_TYPE_XATTR, OBJECT_TYPE_ACL]
_COLORS = {
    OBJECT_TYPE_FILE:   "#4e79a7",
    OBJECT_TYPE_SPARSE: "#f28e2b",
    OBJECT_TYPE_XATTR:  "#59a14f",
    OBJECT_TYPE_ACL:    "#e15759",
}


class AnalyticsTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Analytics")
        self._chart_data: list = []
        self._build()

    def _build(self) -> None:
        self._text = make_text_widget(self._frame)
        # Fixed height — canvas gets the rest
        self._text.config(height=12)

        tk.Label(self._frame, text="Uncompressed size by type",
                 font=FONT_BOLD).pack(anchor="w", padx=PAD, pady=(PAD, 0))
        self._canvas = tk.Canvas(self._frame, height=168, bg="white",
                                 highlightthickness=0)
        self._canvas.pack(fill=tk.X, padx=PAD, pady=(0, PAD))
        self._canvas.bind("<Configure>", lambda _: self._redraw())

    def populate(self, scan: dict) -> None:
        stats = {t: {"count": 0, "uncomp": 0, "comp": 0} for t in _TYPES}

        for dat_path in scan.get("pack_dat", []):
            try:
                d = parse_pack_dat(dat_path)
            except Exception:
                continue
            for e in d["entries"]:
                t = e["type"]
                if t in stats:
                    stats[t]["count"]  += 1
                    stats[t]["uncomp"] += e["uncompressed_size"]
                    stats[t]["comp"]   += e["compressed_size"]

        for loose_path in scan.get("loose", []):
            try:
                obj = parse_loose_object(loose_path)
            except Exception:
                continue
            t = obj["type"]
            if t in stats:
                stats[t]["count"]  += 1
                stats[t]["uncomp"] += obj["uncompressed_size"]
                stats[t]["comp"]   += obj["compressed_size"]

        total_uncomp = sum(s["uncomp"] for s in stats.values()) or 1
        total_comp   = sum(s["comp"]   for s in stats.values()) or 1
        total_count  = sum(s["count"]  for s in stats.values())
        savings      = total_uncomp - total_comp

        lines = [
            f"{'Type':<8}  {'Count':>7}  {'Uncompressed':>14}  {'Compressed':>12}  {'Ratio':>6}",
            "─" * 58,
        ]
        for t in _TYPES:
            s = stats[t]
            ratio = f"{s['comp'] / s['uncomp']:.3f}" if s["uncomp"] else "—"
            lines.append(
                f"{OBJECT_TYPE_NAMES.get(t, t):<8}  {s['count']:>7}"
                f"  {fmt_size(s['uncomp']):>14}  {fmt_size(s['comp']):>12}  {ratio:>6}"
            )
        lines += [
            "─" * 58,
            f"{'TOTAL':<8}  {total_count:>7}  {fmt_size(total_uncomp):>14}"
            f"  {fmt_size(total_comp):>12}  {total_comp / total_uncomp:.3f}",
            "",
            f"Space saved : {fmt_size(savings)}"
            f"  ({100 * (1 - total_comp / total_uncomp):.1f}% reduction)",
        ]
        set_text(self._text, "\n".join(lines))

        self._chart_data = [(t, stats[t]["uncomp"]) for t in _TYPES]
        self._redraw()

    def _redraw(self) -> None:
        self._canvas.delete("all")
        if not self._chart_data:
            return
        W   = self._canvas.winfo_width() or 600
        bar_h, gap     = 28, 10
        margin_l, margin_r = 72, 120

        total = sum(v for _, v in self._chart_data) or 1
        bar_w = W - margin_l - margin_r

        for i, (t, val) in enumerate(self._chart_data):
            y      = 8 + i * (bar_h + gap)
            filled = int(bar_w * val / total)
            color  = _COLORS.get(t, "#aaa")
            label  = OBJECT_TYPE_NAMES.get(t, str(t))

            self._canvas.create_text(margin_l - 6, y + bar_h // 2,
                                     anchor="e", text=label, font=FONT_MONO)
            self._canvas.create_rectangle(margin_l, y,
                                          margin_l + bar_w, y + bar_h,
                                          fill="#eee", outline="#ccc")
            if filled > 0:
                self._canvas.create_rectangle(margin_l, y,
                                              margin_l + filled, y + bar_h,
                                              fill=color, outline="")
            self._canvas.create_text(margin_l + bar_w + 8, y + bar_h // 2,
                                     anchor="w", text=fmt_size(val),
                                     font=FONT_MONO)
