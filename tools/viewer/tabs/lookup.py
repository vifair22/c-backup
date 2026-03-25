import os
import threading
import tkinter as tk
from tkinter import ttk

from ..parsers import parse_snap, idx_bisect, dat_read_entry_at, load_loose_object
from ..formats import fmt_size, hex_hash
from ..constants import OBJECT_HASH_SIZE, OBJECT_TYPE_NAMES, COMPRESS_NAMES
from ..widgets import make_text_widget, set_text, FONT_MONO, PAD


class LookupTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Hash Lookup")
        self._scan: dict | None = None
        self._lookup_gen: int = 0
        self._build()

    def _build(self) -> None:
        top = tk.Frame(self._frame)
        top.pack(fill=tk.X, padx=PAD, pady=PAD)
        tk.Label(top, text="Hash (hex):").pack(side=tk.LEFT)
        self._var = tk.StringVar()
        entry = tk.Entry(top, textvariable=self._var, font=FONT_MONO, width=66)
        entry.pack(side=tk.LEFT, padx=4)
        entry.bind("<Return>", lambda _: self._lookup())
        self._lookup_btn = tk.Button(top, text="Look up", command=self._lookup)
        self._lookup_btn.pack(side=tk.LEFT)
        self._text = make_text_widget(self._frame)

    def populate(self, scan: dict) -> None:
        self._scan = scan

    def _lookup(self) -> None:
        if not self._scan:
            set_text(self._text, "No repository open.")
            return

        hex_val = self._var.get().strip().lower()
        if len(hex_val) != OBJECT_HASH_SIZE * 2:
            set_text(self._text, f"Hash must be {OBJECT_HASH_SIZE * 2} hex chars.")
            return
        try:
            raw_hash = bytes.fromhex(hex_val)
        except ValueError:
            set_text(self._text, "Invalid hex string.")
            return

        # Phase 1: loose + pack lookups are fast (O(1) + O(log n)) — run immediately
        lines = [f"Hash: {hex_val}", ""]

        prefix, suffix = hex_val[:2], hex_val[2:]
        loose_path = os.path.join(self._scan["repo_path"], "objects", prefix, suffix)
        if os.path.exists(loose_path):
            lines.append("Found: loose object")
            lines.append(f"  Path       : {loose_path}")
            lines.append(f"  Size       : {fmt_size(os.path.getsize(loose_path))}")
            try:
                otype, comp, uncomp_sz, comp_sz, h, _ = load_loose_object(loose_path)
                lines += [
                    f"  Type       : {OBJECT_TYPE_NAMES.get(otype, otype)}",
                    f"  Compression: {COMPRESS_NAMES.get(comp, comp)}",
                    f"  Uncomp size: {fmt_size(uncomp_sz)} ({uncomp_sz} bytes)",
                    f"  Comp size  : {fmt_size(comp_sz)} ({comp_sz} bytes)",
                ]
            except Exception as e:
                lines.append(f"  (parse error: {e})")
        else:
            lines.append("Not found in loose object store.")

        lines.append("")

        found_pack = False
        for dat_path in self._scan.get("pack_dat", []):
            idx_path = dat_path.replace(".dat", ".idx")
            if not os.path.exists(idx_path):
                continue
            try:
                dat_offset = idx_bisect(idx_path, raw_hash)
                if dat_offset is None:
                    continue
                e = dat_read_entry_at(dat_path, dat_offset)
            except Exception:
                continue
            found_pack = True
            lines += [
                f"Found in pack: {os.path.basename(dat_path)}",
                f"  Pack version  : {e['pack_version']}",
                f"  Type          : {OBJECT_TYPE_NAMES.get(e['type'], e['type'])}",
                f"  Compression   : {COMPRESS_NAMES.get(e['compression'], e['compression'])}",
                f"  Uncompressed  : {fmt_size(e['uncompressed_size'])} ({e['uncompressed_size']} bytes)",
                f"  Compressed    : {fmt_size(e['compressed_size'])} ({e['compressed_size']} bytes)",
                f"  Payload offset: 0x{e['payload_offset']:X} in {os.path.basename(dat_path)}",
            ]
            if e["uncompressed_size"] > 0:
                ratio = e["compressed_size"] / e["uncompressed_size"]
                lines.append(f"  Ratio         : {ratio:.4f}")
        if not found_pack:
            lines.append("Not found in any pack file.")

        lines += ["", "Scanning snapshots for references…"]
        set_text(self._text, "\n".join(lines))

        # Phase 2: snapshot scan is slow (full parse each) — thread it
        self._lookup_gen += 1
        gen = self._lookup_gen
        self._lookup_btn.config(state=tk.DISABLED)
        fast_lines = list(lines[:-1])   # drop the "Scanning…" line
        snap_paths = list(self._scan.get("snapshots", []))

        def _worker() -> None:
            referencing: list[tuple] = []
            for snap_path in snap_paths:
                try:
                    s = parse_snap(snap_path)
                except Exception:
                    continue
                for nd in s["nodes"]:
                    for field, key in (("content", "content_hash"),
                                       ("xattr",   "xattr_hash"),
                                       ("acl",     "acl_hash")):
                        if nd[key] == raw_hash:
                            referencing.append((s["snap_id"], nd["node_id"], field))
            self._frame.after(0, lambda: self._finish_lookup(
                fast_lines, referencing, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _finish_lookup(self, fast_lines: list[str],
                       referencing: list[tuple], gen: int) -> None:
        if gen != self._lookup_gen:
            return
        self._lookup_btn.config(state=tk.NORMAL)
        lines = fast_lines + [""]
        if referencing:
            lines.append(f"Referenced by {len(referencing)} node(s):")
            for snap_id, node_id, field in referencing:
                lines.append(f"  snap {snap_id:>6}  node {node_id:>10}  ({field} hash)")
        else:
            lines.append("Not referenced by any loaded snapshot.")
        set_text(self._text, "\n".join(lines))
