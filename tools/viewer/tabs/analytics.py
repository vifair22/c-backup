import math
import threading
import tkinter as tk
from tkinter import ttk

from ..rpc import call, RPCError
from ..formats import fmt_size
from ..constants import (
    OBJECT_TYPE_NAMES,
    OBJECT_TYPE_FILE, OBJECT_TYPE_SPARSE,
    OBJECT_TYPE_XATTR, OBJECT_TYPE_ACL,
    PROBER_VERSION,
)
from ..widgets import make_text_widget, set_text, ui_call, PAD, FONT_MONO, FONT_BOLD

_TYPES = [OBJECT_TYPE_FILE, OBJECT_TYPE_SPARSE, OBJECT_TYPE_XATTR, OBJECT_TYPE_ACL]
_COLORS = {
    OBJECT_TYPE_FILE:   "#4e79a7",
    OBJECT_TYPE_SPARSE: "#f28e2b",
    OBJECT_TYPE_XATTR:  "#59a14f",
    OBJECT_TYPE_ACL:    "#e15759",
}
_COLOR_PACK       = "#4e79a7"
_COLOR_LOOSE      = "#f28e2b"
_COLOR_LOOSE_SKIP = "#e15759"
_COLOR_COMPRESS   = "#59a14f"   # green  – compressible
_COLOR_HIRATIO    = "#f28e2b"   # orange – high-ratio pack


class AnalyticsTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Analytics")
        self._chart_data:    list = []
        self._loc_data:      list = []
        self._compress_data: dict = {}
        self._build()

    def _build(self) -> None:
        self._text = make_text_widget(self._frame)
        self._text.config(height=18)

        tk.Label(self._frame, text="Compressibility  (object counts)",
                 font=FONT_BOLD).pack(anchor="w", padx=PAD, pady=(PAD, 0))
        self._compress_canvas = tk.Canvas(self._frame, height=84, bg="white",
                                          highlightthickness=0)
        self._compress_canvas.pack(fill=tk.X, padx=PAD, pady=(0, PAD))
        self._compress_canvas.bind("<Configure>", lambda _: self._redraw_compress())

        tk.Label(self._frame, text="Uncompressed size by type  (object counts)",
                 font=FONT_BOLD).pack(anchor="w", padx=PAD, pady=(PAD, 0))
        self._canvas = tk.Canvas(self._frame, height=168, bg="white",
                                 highlightthickness=0)
        self._canvas.pack(fill=tk.X, padx=PAD, pady=(0, PAD))
        self._canvas.bind("<Configure>", lambda _: self._redraw())

        tk.Label(self._frame, text="Pack vs loose  (object counts)",
                 font=FONT_BOLD).pack(anchor="w", padx=PAD, pady=(PAD, 0))
        self._loc_canvas = tk.Canvas(self._frame, height=108, bg="white",
                                     highlightthickness=0)
        self._loc_canvas.pack(fill=tk.X, padx=PAD, pady=(0, PAD))
        self._loc_canvas.bind("<Configure>", lambda _: self._redraw_loc())

    def populate(self, repo_path: str) -> None:
        set_text(self._text, "Loading…")

        def _worker() -> None:
            try:
                pack_data = call(repo_path, "all_pack_entries")
            except RPCError:
                pack_data = {"entries": []}
            try:
                loose_data = call(repo_path, "loose_list")
            except RPCError:
                loose_data = {"objects": []}
            ui_call(lambda: self._compute_and_display(pack_data, loose_data))

        threading.Thread(target=_worker, daemon=True).start()

    def populate_from_summary(self, repo_path: str, summary: dict) -> None:
        pack_data = summary.get("all_pack_entries") or {"entries": []}
        loose_data = summary.get("loose_list") or {"objects": []}
        self._compute_and_display(pack_data, loose_data)

    def _compute_and_display(self, pack_data: dict, loose_data: dict) -> None:
        stats = {t: {"count": 0, "uncomp": 0, "comp": 0} for t in _TYPES}

        pack    = {"count": 0, "uncomp": 0, "comp": 0}
        loose   = {"count": 0, "uncomp": 0, "comp": 0}
        skip    = {"count": 0, "uncomp": 0, "comp": 0}
        hiratio = {"count": 0, "uncomp": 0, "comp": 0}

        for e in pack_data.get("entries", []):
            t = int(e["type"])
            uncomp = int(e["uncompressed_size"])
            comp   = int(e["compressed_size"])
            if t in stats:
                stats[t]["count"]  += 1
                stats[t]["uncomp"] += uncomp
                stats[t]["comp"]   += comp
            pack["count"]  += 1
            pack["uncomp"] += uncomp
            pack["comp"]   += comp
            if uncomp > 0 and comp / uncomp >= 0.90:
                hiratio["count"]  += 1
                hiratio["uncomp"] += uncomp
                hiratio["comp"]   += comp

        for obj in loose_data.get("objects", []):
            t      = int(obj["type"])
            uncomp = int(obj["uncompressed_size"])
            comp   = int(obj["compressed_size"])
            if t in stats:
                stats[t]["count"]  += 1
                stats[t]["uncomp"] += uncomp
                stats[t]["comp"]   += comp
            loose["count"]  += 1
            loose["uncomp"] += uncomp
            loose["comp"]   += comp
            if int(obj.get("pack_skip_ver", 0)) == PROBER_VERSION:
                skip["count"]  += 1
                skip["uncomp"] += uncomp
                skip["comp"]   += comp

        total_uncomp = sum(s["uncomp"] for s in stats.values()) or 1
        total_comp   = sum(s["comp"]   for s in stats.values()) or 1
        total_count  = sum(s["count"]  for s in stats.values())
        savings      = total_uncomp - total_comp

        lines = [
            f"{'Type':<8}  {'Count':>7}  {'Uncompressed':>14}  {'Compressed':>12}  {'Ratio':>6}",
            "\u2500" * 58,
        ]
        for t in _TYPES:
            s = stats[t]
            ratio = f"{s['comp'] / s['uncomp']:.3f}" if s["uncomp"] else "\u2014"
            lines.append(
                f"{OBJECT_TYPE_NAMES.get(t, t):<8}  {s['count']:>7}"
                f"  {fmt_size(s['uncomp']):>14}  {fmt_size(s['comp']):>12}  {ratio:>6}"
            )
        lines += [
            "\u2500" * 58,
            f"{'TOTAL':<8}  {total_count:>7}  {fmt_size(total_uncomp):>14}"
            f"  {fmt_size(total_comp):>12}  {total_comp / total_uncomp:.3f}",
            "",
            f"Space saved : {fmt_size(savings)}"
            f"  ({100 * (1 - total_comp / total_uncomp):.1f}% reduction)",
        ]

        total_all        = pack["count"]  + loose["count"]
        total_all_uncomp = pack["uncomp"] + loose["uncomp"]
        incomp_count     = skip["count"]  + hiratio["count"]
        incomp_uncomp    = skip["uncomp"] + hiratio["uncomp"]
        comp_count       = total_all        - incomp_count
        comp_uncomp      = total_all_uncomp - incomp_uncomp

        lines += [
            "",
            f"{'Category':<20}  {'Count':>7}  {'Uncompressed':>14}",
            "\u2500" * 46,
            f"{'Compressible':<20}  {comp_count:>7}  {fmt_size(comp_uncomp):>14}",
            f"{'Incompressible':<20}  {incomp_count:>7}  {fmt_size(incomp_uncomp):>14}",
            f"{'  skip-marked':<20}  {skip['count']:>7}  {fmt_size(skip['uncomp']):>14}",
            f"{'  high-ratio pack':<20}  {hiratio['count']:>7}  {fmt_size(hiratio['uncomp']):>14}",
        ]

        chart_data = [(t, stats[t]["uncomp"], stats[t]["count"]) for t in _TYPES]
        loose_only_count = loose["count"] - skip["count"]
        loose_only_bytes = loose["uncomp"] - skip["uncomp"]
        loc_data = [
            ("Pack",         pack["uncomp"],       pack["count"],       _COLOR_PACK),
            ("Loose",        loose_only_bytes,      loose_only_count,    _COLOR_LOOSE),
            ("Loose (skip)", skip["uncomp"],        skip["count"],       _COLOR_LOOSE_SKIP),
        ]
        compress_data = {
            "total":          total_all,
            "comp_count":     comp_count,
            "comp_uncomp":    comp_uncomp,
            "skip_count":     skip["count"],
            "skip_uncomp":    skip["uncomp"],
            "hiratio_count":  hiratio["count"],
            "hiratio_uncomp": hiratio["uncomp"],
            "incomp_count":   incomp_count,
            "incomp_uncomp":  incomp_uncomp,
        }

        self._display(lines, chart_data, loc_data, compress_data)

    def _display(self, lines, chart_data, loc_data, compress_data) -> None:
        set_text(self._text, "\n".join(lines))
        self._chart_data    = chart_data
        self._loc_data      = loc_data
        self._compress_data = compress_data
        self._redraw()
        self._redraw_loc()
        self._redraw_compress()

    def _redraw(self) -> None:
        self._canvas.delete("all")
        if not self._chart_data:
            return
        W   = self._canvas.winfo_width() or 600
        bar_h, gap     = 28, 10
        margin_l, margin_r = 104, 84

        total = sum(v for _, v, _ in self._chart_data) or 1
        bar_w = W - margin_l - margin_r

        for i, (t, val, count) in enumerate(self._chart_data):
            y      = 8 + i * (bar_h + gap)
            filled = int(bar_w * val / total)
            color     = _COLORS.get(t, "#aaa")
            label     = OBJECT_TYPE_NAMES.get(t, str(t))
            pct       = 100.0 * val / total
            bar_label = f"{count:,} ({pct:.1f}%)"

            self._canvas.create_text(margin_l - 6, y + bar_h // 2,
                                     anchor="e", text=label, font=FONT_MONO)
            self._canvas.create_rectangle(margin_l, y,
                                          margin_l + bar_w, y + bar_h,
                                          fill="#eee", outline="#ccc")
            if filled > 0:
                self._canvas.create_rectangle(margin_l, y,
                                              margin_l + filled, y + bar_h,
                                              fill=color, outline="")
            if filled > 40:
                self._canvas.create_text(margin_l + filled // 2, y + bar_h // 2,
                                         anchor="center", text=bar_label,
                                         font=FONT_MONO, fill="white")
            else:
                self._canvas.create_text(margin_l + filled + 6, y + bar_h // 2,
                                         anchor="w", text=bar_label,
                                         font=FONT_MONO, fill="black")
            self._canvas.create_text(margin_l + bar_w + 8, y + bar_h // 2,
                                     anchor="w", text=fmt_size(val),
                                     font=FONT_MONO)

    def _redraw_loc(self) -> None:
        self._loc_canvas.delete("all")
        if not self._loc_data:
            return
        W = self._loc_canvas.winfo_width() or 600
        bar_h, gap = 24, 8
        margin_l, margin_r = 104, 84

        total = sum(v for _, v, _, _ in self._loc_data) or 1
        bar_w = max(W - margin_l - margin_r, 1)

        for i, (label, val, count, color) in enumerate(self._loc_data):
            y         = 8 + i * (bar_h + gap)
            filled    = int(bar_w * val / total)
            pct       = 100.0 * val / total
            bar_label = f"{count:,} ({pct:.1f}%)"

            self._loc_canvas.create_text(margin_l - 6, y + bar_h // 2,
                                         anchor="e", text=label, font=FONT_MONO)
            self._loc_canvas.create_rectangle(margin_l, y,
                                              margin_l + bar_w, y + bar_h,
                                              fill="#eee", outline="#ccc")
            if filled > 0:
                self._loc_canvas.create_rectangle(margin_l, y,
                                                  margin_l + filled, y + bar_h,
                                                  fill=color, outline="")
            if filled > 40:
                self._loc_canvas.create_text(margin_l + filled // 2, y + bar_h // 2,
                                             anchor="center", text=bar_label,
                                             font=FONT_MONO, fill="white")
            else:
                self._loc_canvas.create_text(margin_l + filled + 6, y + bar_h // 2,
                                             anchor="w", text=bar_label,
                                             font=FONT_MONO, fill="black")
            self._loc_canvas.create_text(margin_l + bar_w + 6, y + bar_h // 2,
                                         anchor="w", text=fmt_size(val),
                                         font=FONT_MONO)

    @staticmethod
    def _log_widths(counts: list[int], total_px: int) -> list[int]:
        logs = [math.log2(1 + c) for c in counts]
        log_total = sum(logs) or 1
        widths = [int(total_px * v / log_total) for v in logs]
        for i, c in enumerate(counts):
            if c > 0 and widths[i] < 4:
                widths[i] = 4
        overflow = sum(widths) - total_px
        if overflow > 0:
            biggest = max(range(len(widths)), key=lambda j: widths[j])
            widths[biggest] = max(4, widths[biggest] - overflow)
        return widths

    def _redraw_compress(self) -> None:
        self._compress_canvas.delete("all")
        if not self._compress_data:
            return
        d = self._compress_data
        total = d["total"] or 1
        W = self._compress_canvas.winfo_width() or 600
        bar_h, gap = 28, 10
        margin_l, margin_r = 104, 84
        bar_w = max(W - margin_l - margin_r, 1)

        rows = [
            ("Compressible", d["comp_count"],   d["comp_uncomp"],
             [(d["comp_count"],    _COLOR_COMPRESS)]),
            ("Incompress.",  d["incomp_count"],  d["incomp_uncomp"],
             [(d["skip_count"],    _COLOR_LOOSE_SKIP),
              (d["hiratio_count"], _COLOR_HIRATIO)]),
        ]

        for i, (label, count, uncomp, segments) in enumerate(rows):
            y         = 8 + i * (bar_h + gap)
            filled    = int(bar_w * count / total)
            pct       = 100.0 * count / total
            bar_label = f"{count:,} ({pct:.1f}%)"

            self._compress_canvas.create_text(margin_l - 6, y + bar_h // 2,
                                              anchor="e", text=label, font=FONT_MONO)
            self._compress_canvas.create_rectangle(margin_l, y,
                                                   margin_l + bar_w, y + bar_h,
                                                   fill="#eee", outline="#ccc")

            seg_counts = [sc for sc, _ in segments]
            seg_colors = [sc for _, sc in segments]
            seg_widths = self._log_widths(seg_counts, filled)

            x = margin_l
            multi = len(segments) > 1
            for sw, seg_color, seg_count in zip(seg_widths, seg_colors, seg_counts):
                if sw > 0:
                    self._compress_canvas.create_rectangle(
                        x, y, x + sw, y + bar_h,
                        fill=seg_color, outline="")
                    if multi and seg_count > 0 and sw > 40:
                        spct = 100.0 * seg_count / total
                        stxt = f"{seg_count:,} ({spct:.1f}%)"
                        self._compress_canvas.create_text(
                            x + sw // 2, y + bar_h // 2,
                            anchor="center", text=stxt,
                            font=FONT_MONO, fill="white")
                    x += sw

            if not multi:
                if filled > 40:
                    self._compress_canvas.create_text(
                        margin_l + filled // 2, y + bar_h // 2,
                        anchor="center", text=bar_label, font=FONT_MONO, fill="white")
                else:
                    self._compress_canvas.create_text(
                        margin_l + filled + 6, y + bar_h // 2,
                        anchor="w", text=bar_label, font=FONT_MONO, fill="black")

            self._compress_canvas.create_text(margin_l + bar_w + 6, y + bar_h // 2,
                                              anchor="w", text=fmt_size(uncomp),
                                              font=FONT_MONO)
