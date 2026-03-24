import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..parsers import parse_snap, find_object, decompress_payload, HAS_LZ4
from ..formats import fmt_size, fmt_time, fmt_mode, gfs_flags_str, hex_hash
from ..constants import (NODE_TYPE_NAMES, NODE_TYPE_REG, NODE_TYPE_DIR,
                         NODE_TYPE_SYMLINK, NODE_TYPE_CHR, NODE_TYPE_BLK,
                         COMPRESS_NONE)
from ..widgets import make_text_widget, make_text_tab, set_text, PAD, FONT_MONO, FONT_BOLD

# Cap visible rows; filter narrows the window before this limit matters
NODE_PAGE_SIZE = 500
# Sentinel text used as a placeholder child for lazy dir-tree expansion
_DIR_SENTINEL = "\x00"


class SnapshotsTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Snapshots")
        self._snap_paths: list[str] = []
        self._snap_id_to_path: dict[int, str] = {}
        self._current_snap: dict | None = None
        self._scan: dict | None = None
        self._all_nodes: list[dict] = []
        self._dir_children: dict[int, list] = {}
        self._node_type_map: dict[int, int] = {}
        self._empty_node_ids: set[int] = set()
        self._build()

    # ---- build ----

    def _build(self) -> None:
        pane = ttk.PanedWindow(self._frame, orient=tk.HORIZONTAL)
        pane.pack(fill=tk.BOTH, expand=True)

        # Left: snapshot list
        left = ttk.Frame(pane)
        pane.add(left, weight=1)
        tk.Label(left, text="Snapshots", font=FONT_BOLD).pack(anchor="w", padx=PAD)
        self._list = tk.Listbox(left, font=FONT_MONO, width=26)
        sb = ttk.Scrollbar(left, command=self._list.yview)
        self._list.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._list.pack(fill=tk.BOTH, expand=True)
        self._list.bind("<<ListboxSelect>>", self._on_select)

        # Right: sub-notebook
        right = ttk.Frame(pane)
        pane.add(right, weight=4)
        self._snap_nb = ttk.Notebook(right)
        self._snap_nb.pack(fill=tk.BOTH, expand=True)

        self._hdr_text   = make_text_tab(self._snap_nb, "Header")
        self._node_frame = ttk.Frame(self._snap_nb)
        self._snap_nb.add(self._node_frame, text="Nodes")
        self._dir_frame  = ttk.Frame(self._snap_nb)
        self._snap_nb.add(self._dir_frame, text="Directory tree")

        self._build_node_tree()
        self._build_dir_tree()

    def _build_node_tree(self) -> None:
        bar = tk.Frame(self._node_frame)
        bar.pack(fill=tk.X, padx=PAD, pady=(PAD, 0))
        tk.Label(bar, text="Filter (hash prefix):").pack(side=tk.LEFT)
        self._filter_var = tk.StringVar()
        self._filter_var.trace_add("write", lambda *_: self._apply_filter())
        tk.Entry(bar, textvariable=self._filter_var, font=FONT_MONO, width=36).pack(
            side=tk.LEFT, padx=4)
        self._node_count_label = tk.Label(bar, text="")
        self._node_count_label.pack(side=tk.LEFT, padx=8)
        tk.Label(bar, text="Double-click row → jump to dir tree", fg="gray").pack(
            side=tk.RIGHT, padx=PAD)

        cols = [
            ("node_id",      80, False),
            ("type",         70, False),
            ("mode",         60, False),
            ("uid",          50, False),
            ("gid",          50, False),
            ("size",         90, False),
            ("mtime",       145, False),
            ("content_hash", 210, True),
        ]
        col_ids = [c[0] for c in cols]
        self._node_tree = ttk.Treeview(self._node_frame, columns=col_ids,
                                       show="headings", selectmode="browse")
        for name, width, stretch in cols:
            self._node_tree.heading(name, text=name.replace("_", " ").title())
            self._node_tree.column(name, width=width, stretch=stretch)
        sb_y = ttk.Scrollbar(self._node_frame, orient=tk.VERTICAL,
                              command=self._node_tree.yview)
        sb_x = ttk.Scrollbar(self._node_frame, orient=tk.HORIZONTAL,
                              command=self._node_tree.xview)
        self._node_tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        sb_y.pack(side=tk.RIGHT, fill=tk.Y)
        sb_x.pack(side=tk.BOTTOM, fill=tk.X)
        self._node_tree.pack(fill=tk.BOTH, expand=True)
        self._node_tree.bind("<<TreeviewSelect>>", self._on_node_select)
        self._node_tree.bind("<Double-1>",         self._on_node_dbl)

        self._node_detail = tk.Text(self._node_frame, font=FONT_MONO,
                                    height=9, state=tk.DISABLED)
        self._node_detail.pack(fill=tk.X)

    def _build_dir_tree(self) -> None:
        hint = tk.Label(self._dir_frame,
                        text="Double-click a file → view object content",
                        fg="gray")
        hint.pack(anchor="w", padx=PAD, pady=(PAD, 0))

        self._dir_tree = ttk.Treeview(self._dir_frame, show="tree headings",
                                      columns=("kind", "node_id"),
                                      selectmode="browse")
        self._dir_tree.heading("#0",      text="Name")
        self._dir_tree.heading("kind",    text="Type")
        self._dir_tree.heading("node_id", text="Node ID")
        self._dir_tree.column("#0",      width=440)
        self._dir_tree.column("kind",    width=80,  stretch=False)
        self._dir_tree.column("node_id", width=100, stretch=False)

        # Visual tags: directories bold+blue, symlinks grey-italic, others default
        self._dir_tree.tag_configure("dir",     foreground="#1a5fb4",
                                     font=(*FONT_MONO[:1], FONT_MONO[1], "bold"))
        self._dir_tree.tag_configure("symlink", foreground="#888888")
        self._dir_tree.tag_configure("empty",   foreground="#aaaaaa",
                                     font=(*FONT_MONO[:1], FONT_MONO[1], "italic"))

        sb = ttk.Scrollbar(self._dir_frame, orient=tk.VERTICAL,
                           command=self._dir_tree.yview)
        self._dir_tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._dir_tree.pack(fill=tk.BOTH, expand=True)
        self._dir_tree.bind("<<TreeviewOpen>>", self._on_dir_open)
        self._dir_tree.bind("<Double-1>",       self._on_dir_dbl)
        self._dir_tree.bind("<Button-3>",       self._on_dir_right_click)

        self._dir_ctx = tk.Menu(self._dir_tree, tearoff=0)
        self._dir_ctx.add_command(label="Export file…", command=self._export_dir_item)

    # ---- populate ----

    def populate(self, scan: dict) -> None:
        self._scan = scan
        self._snap_paths = scan["snapshots"]
        self._head_id    = scan.get("head_id")
        self._snap_id_to_path = {}
        self._list.delete(0, tk.END)
        for path in self._snap_paths:
            try:
                s = parse_snap(path)
                label = f"{s['snap_id']:>6}  {fmt_time(s['created_sec'])}"
                if s["snap_id"] == self._head_id:
                    label += " [HEAD]"
                self._snap_id_to_path[s["snap_id"]] = path
            except Exception:
                import os
                label = os.path.basename(path) + " [ERR]"
            self._list.insert(tk.END, label)

    # ---- events ----

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel:
            return
        self._load(self._snap_paths[sel[0]])

    def load_path(self, path: str) -> None:
        self._load(path)

    def _load(self, path: str) -> None:
        try:
            s = parse_snap(path)
        except Exception as e:
            messagebox.showerror("Parse error", str(e))
            return
        self._current_snap = s
        self._all_nodes = s["nodes"]
        self._filter_var.set("")          # clears and triggers _apply_filter
        self._populate_header(s)
        self._populate_dir_tree(s)

    def _populate_header(self, s: dict) -> None:
        lines = [
            f"Snap ID        : {s['snap_id']}",
            f"Created        : {fmt_time(s['created_sec'])}",
            f"New bytes      : {fmt_size(s['phys_new_bytes'])}",
            f"Node count     : {s['node_count']}",
            f"Dirent count   : {s['dirent_count']}",
            f"GFS flags      : {gfs_flags_str(s['gfs_flags'])} (0x{s['gfs_flags']:02X})",
            f"Snap flags     : 0x{s['snap_flags']:08X}",
        ]
        set_text(self._hdr_text, "\n".join(lines))

    # filter + paginate node tree
    def _apply_filter(self) -> None:
        prefix = self._filter_var.get().strip().lower()
        if prefix:
            visible = [nd for nd in self._all_nodes
                       if nd["content_hash"].hex().startswith(prefix)]
        else:
            visible = self._all_nodes

        shown = visible[:NODE_PAGE_SIZE]
        total = len(visible)

        for row in self._node_tree.get_children():
            self._node_tree.delete(row)
        for nd in shown:
            self._node_tree.insert("", tk.END, values=(
                nd["node_id"],
                NODE_TYPE_NAMES.get(nd["type"], str(nd["type"])),
                fmt_mode(nd["mode"]),
                nd["uid"],
                nd["gid"],
                fmt_size(nd["size"]),
                fmt_time(nd["mtime_sec"]),
                hex_hash(nd["content_hash"])[:16] + "…",
            ))

        if total > NODE_PAGE_SIZE:
            self._node_count_label.config(
                text=f"Showing {NODE_PAGE_SIZE} of {total}  (narrow filter to see more)")
        else:
            self._node_count_label.config(text=f"{total} node(s)")

    def _on_node_select(self, _event) -> None:
        sel = self._node_tree.selection()
        if not sel or not self._current_snap:
            return
        node_id = int(self._node_tree.item(sel[0])["values"][0])
        nd = next((n for n in self._current_snap["nodes"] if n["node_id"] == node_id), None)
        if not nd:
            return
        lines = [
            f"Node ID        : {nd['node_id']}",
            f"Type           : {NODE_TYPE_NAMES.get(nd['type'], nd['type'])}",
            f"Mode           : {fmt_mode(nd['mode'])}",
            f"UID/GID        : {nd['uid']}/{nd['gid']}",
            f"Size           : {fmt_size(nd['size'])} ({nd['size']} bytes)",
            f"Mtime          : {fmt_time(nd['mtime_sec'])}.{nd['mtime_nsec']:09d}",
            f"Link count     : {nd['link_count']}",
            f"Inode identity : {nd['inode_identity']}",
            f"Content hash   : {hex_hash(nd['content_hash'])}",
            f"Xattr hash     : {hex_hash(nd['xattr_hash'])}",
            f"ACL hash       : {hex_hash(nd['acl_hash'])}",
        ]
        if nd["type"] == NODE_TYPE_SYMLINK:
            lines.append(f"Target len     : {nd['union_a']}")
        elif nd["type"] in (NODE_TYPE_CHR, NODE_TYPE_BLK):
            lines.append(f"Device         : {nd['union_a']}:{nd['union_b']}")
        set_text(self._node_detail, "\n".join(lines))

    def _on_node_dbl(self, _event) -> None:
        """Double-click on a node: switch to dir tree and reveal it."""
        sel = self._node_tree.selection()
        if not sel or not self._current_snap:
            return
        node_id = int(self._node_tree.item(sel[0])["values"][0])
        self._snap_nb.select(2)   # Directory tree tab
        self._frame.after(50, lambda nid=node_id: self._reveal_by_node_id(nid))

    def _on_dir_dbl(self, _event) -> None:
        """Double-click on a dir tree item: show object content preview."""
        sel = self._dir_tree.selection()
        if not sel or not self._current_snap or not self._scan:
            return
        item = sel[0]
        node_id  = int(self._dir_tree.item(item)["values"][1])
        filename = self._dir_tree.item(item)["text"]
        nd = next((n for n in self._current_snap["nodes"] if n["node_id"] == node_id), None)
        if not nd:
            return
        raw_hash = nd["content_hash"]
        if raw_hash == bytes(32):
            if nd["type"] == NODE_TYPE_REG and nd["size"] == 0:
                messagebox.showinfo("Empty file",
                                    f"{filename}\n\nThis file is empty (0 bytes).")
            return   # directory / special file — no content object

        from ..widgets import show_object_preview
        is_symlink = nd["type"] == NODE_TYPE_SYMLINK
        show_object_preview(self._frame, raw_hash, self._scan,
                            filename=filename, decode_as_symlink=is_symlink)

    def _on_dir_right_click(self, event: tk.Event) -> None:
        item = self._dir_tree.identify_row(event.y)
        if not item:
            return
        self._dir_tree.selection_set(item)
        node_id = int(self._dir_tree.item(item)["values"][1])
        nd = next((n for n in (self._current_snap or {}).get("nodes", [])
                   if n["node_id"] == node_id), None)
        has_content = nd is not None and nd["content_hash"] != bytes(32)
        self._dir_ctx.entryconfig("Export file…",
                                  state=tk.NORMAL if has_content else tk.DISABLED)
        self._dir_ctx.tk_popup(event.x_root, event.y_root)

    def _export_dir_item(self) -> None:
        sel = self._dir_tree.selection()
        if not sel or not self._current_snap or not self._scan:
            return
        item = sel[0]
        node_id  = int(self._dir_tree.item(item)["values"][1])
        filename = self._dir_tree.item(item)["text"]
        nd = next((n for n in self._current_snap["nodes"]
                   if n["node_id"] == node_id), None)
        if nd is None or nd["content_hash"] == bytes(32):
            messagebox.showinfo("No content", "This item has no file content to export.")
            return

        result = find_object(nd["content_hash"], self._scan)
        if result is None:
            messagebox.showerror("Not found", "Object not found in repository.")
            return
        _, _, _, comp, uncomp_sz, _, payload = result

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

        dest = filedialog.asksaveasfilename(
            title="Export file", initialfile=filename)
        if not dest:
            return
        try:
            with open(dest, "wb") as f:
                f.write(data)
            messagebox.showinfo("Saved", f"Wrote {fmt_size(len(data))} to:\n{dest}")
        except OSError as e:
            messagebox.showerror("Write error", str(e))

    # ---- dir tree population ----

    def _dir_tag(self, node_id: int) -> str:
        ntype = self._node_type_map.get(node_id)
        if ntype == NODE_TYPE_DIR:
            return "dir"
        if ntype == NODE_TYPE_SYMLINK:
            return "symlink"
        if node_id in self._empty_node_ids:
            return "empty"
        return ""

    def _kind_label(self, node_id: int) -> str:
        ntype = self._node_type_map.get(node_id)
        return NODE_TYPE_NAMES.get(ntype, "") if ntype else ""

    def _populate_dir_tree(self, s: dict) -> None:
        for row in self._dir_tree.get_children():
            self._dir_tree.delete(row)

        self._dir_children = {}
        for d in s["dirents"]:
            self._dir_children.setdefault(d["parent_node"], []).append(d)

        self._node_type_map  = {n["node_id"]: n["type"] for n in s["nodes"]}
        self._empty_node_ids = {n["node_id"] for n in s["nodes"]
                                if n["type"] == NODE_TYPE_REG and n["size"] == 0}

        for d in sorted(self._dir_children.get(0, []), key=lambda x: x["name"]):
            tag = self._dir_tag(d["node_id"])
            item = self._dir_tree.insert("", tk.END,
                                         text=d["name"],
                                         values=(self._kind_label(d["node_id"]),
                                                 d["node_id"]),
                                         tags=(tag,) if tag else ())
            if d["node_id"] in self._dir_children:
                self._dir_tree.insert(item, tk.END, text=_DIR_SENTINEL)

    def _on_dir_open(self, _event) -> None:
        item = self._dir_tree.focus()
        children = self._dir_tree.get_children(item)
        if not (len(children) == 1 and
                self._dir_tree.item(children[0])["text"] == _DIR_SENTINEL):
            return
        self._expand_item(item)

    def _expand_item(self, item: str) -> None:
        """Expand a dir tree item if it still has a sentinel placeholder."""
        children = self._dir_tree.get_children(item)
        if (len(children) == 1 and
                self._dir_tree.item(children[0])["text"] == _DIR_SENTINEL):
            self._dir_tree.delete(children[0])
            node_id = int(self._dir_tree.item(item)["values"][1])
            for d in sorted(self._dir_children.get(node_id, []), key=lambda x: x["name"]):
                tag = self._dir_tag(d["node_id"])
                child = self._dir_tree.insert(item, tk.END,
                                              text=d["name"],
                                              values=(self._kind_label(d["node_id"]),
                                                      d["node_id"]),
                                              tags=(tag,) if tag else ())
                if d["node_id"] in self._dir_children:
                    self._dir_tree.insert(child, tk.END, text=_DIR_SENTINEL)
        self._dir_tree.item(item, open=True)

    # ---- navigation helpers ----

    def navigate_to_path(self, snap_id: int, full_path: str) -> None:
        """Load the given snapshot and reveal full_path in the dir tree."""
        snap_path = self._snap_id_to_path.get(snap_id)
        if not snap_path:
            return
        if snap_path in self._snap_paths:
            idx = self._snap_paths.index(snap_path)
            self._list.selection_clear(0, tk.END)
            self._list.selection_set(idx)
            self._list.see(idx)
        if not self._current_snap or self._current_snap.get("snap_id") != snap_id:
            self._load(snap_path)
        self._snap_nb.select(2)
        self._frame.after(50, lambda p=full_path: self._reveal_by_path(p))

    def _reveal_by_path(self, full_path: str) -> None:
        """Walk the dir tree by name components and select the matching leaf."""
        parts = [p for p in full_path.split("/") if p]
        if not parts:
            return
        parent_item = ""
        last_item   = None
        for name in parts:
            if parent_item != "":
                self._expand_item(parent_item)
            item = None
            for child in self._dir_tree.get_children(parent_item):
                if self._dir_tree.item(child)["text"] == name:
                    item = child
                    break
            if item is None:
                break
            parent_item = item
            last_item   = item
        if last_item:
            self._dir_tree.selection_set(last_item)
            self._dir_tree.see(last_item)

    def _reveal_by_node_id(self, target_node_id: int) -> None:
        """Expand the dir tree to show the item with the given node_id."""
        if not self._current_snap:
            return
        # Build node_id → parent_node_id lookup from dirents
        parent_of: dict[int, int] = {}
        for d in self._current_snap["dirents"]:
            parent_of[d["node_id"]] = d["parent_node"]

        # Trace ancestry upward from target until we hit root (parent == 0)
        chain: list[int] = []
        nid = target_node_id
        visited: set[int] = set()
        while nid in parent_of and nid not in visited:
            visited.add(nid)
            chain.append(nid)
            nid = parent_of[nid]
        chain.reverse()   # root-child → … → target

        parent_item = ""
        last_item   = None
        for node_id in chain:
            if parent_item != "":
                self._expand_item(parent_item)
            item = None
            for child in self._dir_tree.get_children(parent_item):
                vals = self._dir_tree.item(child)["values"]
                if vals and int(vals[1]) == node_id:
                    item = child
                    break
            if item is None:
                break
            parent_item = item
            last_item   = item

        if last_item:
            self._dir_tree.selection_set(last_item)
            self._dir_tree.see(last_item)
