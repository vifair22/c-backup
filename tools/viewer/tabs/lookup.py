import threading
import tkinter as tk
from tkinter import ttk

from ..rpc import call, RPCError
from ..formats import fmt_size
from ..constants import OBJECT_HASH_SIZE, OBJECT_TYPE_NAMES
from ..widgets import make_text_widget, set_text, ui_call, FONT_MONO, PAD


class LookupTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Hash Lookup")
        self._repo_path: str | None = None
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

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path

    def _lookup(self) -> None:
        if not self._repo_path:
            set_text(self._text, "No repository open.")
            return

        hex_val = self._var.get().strip().lower()
        if len(hex_val) != OBJECT_HASH_SIZE * 2:
            set_text(self._text, f"Hash must be {OBJECT_HASH_SIZE * 2} hex chars.")
            return
        try:
            bytes.fromhex(hex_val)
        except ValueError:
            set_text(self._text, "Invalid hex string.")
            return

        # Phase 1: locate object via RPC (fast)
        lines = [f"Hash: {hex_val}", ""]

        try:
            loc = call(self._repo_path, "object_locate", hash=hex_val)
        except RPCError as e:
            set_text(self._text, f"Error: {e}")
            return

        if loc.get("found"):
            otype = int(loc.get("type", 0))
            uncomp = int(loc.get("uncompressed_size", 0))
            lines += [
                "Found in repository",
                f"  Type       : {OBJECT_TYPE_NAMES.get(otype, otype)}",
                f"  Uncomp size: {fmt_size(uncomp)} ({uncomp} bytes)",
            ]
        else:
            lines.append("Not found in repository.")

        lines += ["", "Scanning snapshots for references…"]
        set_text(self._text, "\n".join(lines))

        # Phase 2: snapshot reference scan via object_refs (single RPC call)
        self._lookup_gen += 1
        gen = self._lookup_gen
        self._lookup_btn.config(state=tk.DISABLED)
        fast_lines = list(lines[:-1])   # drop the "Scanning…" line
        repo_path = self._repo_path

        def _worker() -> None:
            referencing: list[tuple] = []
            try:
                refs = call(repo_path, "object_refs", hash=hex_val)
                for r in refs.get("refs", []):
                    referencing.append((
                        int(r["snap_id"]),
                        int(r["node_id"]),
                        r["field"],
                    ))
            except RPCError:
                pass
            ui_call(lambda: self._finish_lookup(
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
