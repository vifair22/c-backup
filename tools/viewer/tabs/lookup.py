import os
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
        self._build()

    def _build(self) -> None:
        top = tk.Frame(self._frame)
        top.pack(fill=tk.X, padx=PAD, pady=PAD)
        tk.Label(top, text="Hash (hex):").pack(side=tk.LEFT)
        self._var = tk.StringVar()
        entry = tk.Entry(top, textvariable=self._var, font=FONT_MONO, width=66)
        entry.pack(side=tk.LEFT, padx=4)
        entry.bind("<Return>", lambda _: self._lookup())
        tk.Button(top, text="Look up", command=self._lookup).pack(side=tk.LEFT)
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

        lines = [f"Hash: {hex_val}", ""]

        # Check loose object store (objects/<xx>/<62-hex>)
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

        # Fix 3: binary search each paired .idx instead of scanning every .dat entry
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

        lines.append("")

        # Which snapshots reference this hash?
        referencing = []
        for snap_path in self._scan.get("snapshots", []):
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

        if referencing:
            lines.append(f"Referenced by {len(referencing)} node(s):")
            for snap_id, node_id, field in referencing:
                lines.append(f"  snap {snap_id:>6}  node {node_id:>10}  ({field} hash)")
        else:
            lines.append("Not referenced by any loaded snapshot.")

        set_text(self._text, "\n".join(lines))
