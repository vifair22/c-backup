import base64
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from ..rpc import call, RPCError
from ..formats import fmt_size, fmt_time, fmt_mode, gfs_flags_str
from ..constants import (NODE_TYPE_NAMES, NODE_TYPE_REG, NODE_TYPE_DIR,
                         NODE_TYPE_SYMLINK, NODE_TYPE_CHR, NODE_TYPE_BLK,
                         ZERO_HASH)
from ..widgets import make_text_widget, make_text_tab, set_text, ui_call, PAD, FONT_MONO, FONT_BOLD

# Cap visible rows; filter narrows the window before this limit matters
NODE_PAGE_SIZE = 500
# Sentinel text used as a placeholder child for lazy dir-tree expansion
_DIR_SENTINEL = "\x00"


class SnapshotsTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Snapshots")
        self._repo_path: str | None = None
        self._snap_ids: list[int] = []
        self._snap_headers: list[dict] = []
        self._current_snap_id: int | None = None
        self._current_header: dict | None = None
        # Legacy full-snap data (loaded on demand for Nodes tab)
        self._all_nodes: list[dict] = []
        self._node_map: dict[int, dict] = {}
        self._nodes_loaded_for: int | None = None
        self._load_generation: int = 0
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
        self._loading_label = tk.Label(left, text="", fg="gray", font=FONT_MONO)
        self._loading_label.pack(anchor="w", padx=PAD)

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

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path
        try:
            data = call(repo_path, "list")
        except RPCError:
            data = {}
        self._apply_list(data)

    def populate_from_summary(self, repo_path: str, summary: dict) -> None:
        self._repo_path = repo_path
        self._apply_list(summary.get("list") or {})

    def _apply_list(self, data: dict) -> None:
        self._snap_ids = []
        self._snap_headers = []
        self._list.delete(0, tk.END)

        head_id = data.get("head")
        for s in data.get("snapshots", []):
            sid = int(s["id"])
            self._snap_ids.append(sid)
            self._snap_headers.append(s)
            label = f"{sid:>6}  {fmt_time(int(s['created_sec']))}"
            if sid == head_id:
                label += " [HEAD]"
            self._list.insert(tk.END, label)

    # ---- events ----

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel:
            return
        snap_id = self._snap_ids[sel[0]]
        self._load(snap_id)

    def load_path(self, path: str) -> None:
        """Compatibility — load by snap file path (extract ID from filename)."""
        import os
        name = os.path.basename(path)
        try:
            snap_id = int(name.split(".")[0])
            self._load(snap_id)
        except (ValueError, IndexError):
            messagebox.showerror("Error", f"Cannot determine snap ID from: {name}")

    def _load(self, snap_id: int) -> None:
        if not self._repo_path:
            return
        self._load_generation += 1
        gen = self._load_generation
        self._loading_label.config(text="Loading…")
        self._list.config(state=tk.DISABLED)

        repo_path = self._repo_path

        def _worker() -> None:
            try:
                hdr = call(repo_path, "snap_header", id=snap_id)
            except RPCError as e:
                err = str(e)
                ui_call(lambda: self._on_load_error(err, gen))
                return
            # Also fetch root children for the dir tree
            try:
                root = call(repo_path, "snap_dir_children",
                            id=snap_id, parent_node=0)
            except RPCError:
                root = {"children": []}
            ui_call(lambda: self._finish_load(snap_id, hdr, root, gen))

        threading.Thread(target=_worker, daemon=True).start()

    def _on_load_error(self, err: str, gen: int) -> None:
        if gen != self._load_generation:
            return
        self._loading_label.config(text="")
        self._list.config(state=tk.NORMAL)
        messagebox.showerror("Parse error", err)

    def _finish_load(self, snap_id: int, hdr: dict,
                     root: dict, gen: int) -> None:
        if gen != self._load_generation:
            return
        self._loading_label.config(text="")
        self._list.config(state=tk.NORMAL)
        self._current_snap_id = snap_id
        self._current_header = hdr
        self._all_nodes = []
        self._node_map = {}
        self._nodes_loaded_for = None
        self._filter_var.set("")
        self._populate_header(hdr)
        self._populate_dir_tree(root)

    def _populate_header(self, s: dict) -> None:
        lines = [
            f"Snap ID        : {s.get('snap_id', '?')}",
            f"Version        : {s.get('version', '?')}",
            f"Created        : {fmt_time(int(s.get('created_sec', 0)))}",
            f"New bytes      : {fmt_size(int(s.get('phys_new_bytes', 0)))}",
            f"Node count     : {s.get('node_count', 0)}",
            f"Dirent count   : {s.get('dirent_count', 0)}",
            f"GFS flags      : {gfs_flags_str(int(s.get('gfs_flags', 0)))} (0x{int(s.get('gfs_flags', 0)):02X})",
            f"Snap flags     : 0x{int(s.get('snap_flags', 0)):08X}",
        ]
        set_text(self._hdr_text, "\n".join(lines))

    # ---- Nodes tab (lazy: loads full snap only when needed) ----

    def _ensure_nodes_loaded(self) -> None:
        """Load the full snap (nodes only) if not already loaded for current snap."""
        if (self._nodes_loaded_for == self._current_snap_id
                and self._all_nodes):
            return
        if not self._repo_path or not self._current_snap_id:
            return
        try:
            s = call(self._repo_path, "snap", id=self._current_snap_id)
            self._all_nodes = s.get("nodes", [])
            self._node_map = {int(n["node_id"]): n for n in self._all_nodes}
            self._nodes_loaded_for = self._current_snap_id
        except RPCError:
            self._all_nodes = []
            self._node_map = {}

    def _apply_filter(self) -> None:
        self._ensure_nodes_loaded()
        prefix = self._filter_var.get().strip().lower()
        if prefix:
            visible = [nd for nd in self._all_nodes
                       if nd["content_hash"].startswith(prefix)]
        else:
            visible = self._all_nodes

        shown = visible[:NODE_PAGE_SIZE]
        total = len(visible)

        rows = self._node_tree.get_children()
        if rows:
            self._node_tree.delete(*rows)
        for nd in shown:
            self._node_tree.insert("", tk.END, values=(
                int(nd["node_id"]),
                NODE_TYPE_NAMES.get(int(nd["type"]), str(nd["type"])),
                fmt_mode(int(nd["mode"])),
                int(nd["uid"]),
                int(nd["gid"]),
                fmt_size(int(nd["size"])),
                fmt_time(int(nd["mtime_sec"])),
                nd["content_hash"][:16] + "…",
            ))

        if total > NODE_PAGE_SIZE:
            self._node_count_label.config(
                text=f"Showing {NODE_PAGE_SIZE} of {total}  (narrow filter to see more)")
        else:
            self._node_count_label.config(text=f"{total} node(s)")

    def _on_node_select(self, _event) -> None:
        sel = self._node_tree.selection()
        if not sel or not self._current_snap_id:
            return
        node_id = int(self._node_tree.item(sel[0])["values"][0])
        nd = self._node_map.get(node_id)
        if not nd:
            return
        lines = [
            f"Node ID        : {int(nd['node_id'])}",
            f"Type           : {NODE_TYPE_NAMES.get(int(nd['type']), nd['type'])}",
            f"Mode           : {fmt_mode(int(nd['mode']))}",
            f"UID/GID        : {int(nd['uid'])}/{int(nd['gid'])}",
            f"Size           : {fmt_size(int(nd['size']))} ({int(nd['size'])} bytes)",
            f"Mtime          : {fmt_time(int(nd['mtime_sec']))}.{int(nd.get('mtime_nsec', 0)):09d}",
            f"Link count     : {int(nd.get('link_count', 0))}",
            f"Inode identity : {int(nd.get('inode_identity', 0))}",
            f"Content hash   : {nd['content_hash']}",
            f"Xattr hash     : {nd.get('xattr_hash', '')}",
            f"ACL hash       : {nd.get('acl_hash', '')}",
        ]
        ntype = int(nd["type"])
        if ntype == NODE_TYPE_SYMLINK:
            lines.append(f"Target len     : {int(nd.get('union_a', 0))}")
        elif ntype in (NODE_TYPE_CHR, NODE_TYPE_BLK):
            lines.append(f"Device         : {int(nd.get('union_a', 0))}:{int(nd.get('union_b', 0))}")
        set_text(self._node_detail, "\n".join(lines))

    def _on_node_dbl(self, _event) -> None:
        sel = self._node_tree.selection()
        if not sel or not self._current_snap_id:
            return
        node_id = int(self._node_tree.item(sel[0])["values"][0])
        self._snap_nb.select(2)   # Directory tree tab
        self._frame.after(50, lambda nid=node_id: self._reveal_by_node_id(nid))

    def _on_dir_dbl(self, _event) -> None:
        sel = self._dir_tree.selection()
        if not sel or not self._current_snap_id or not self._repo_path:
            return
        item = sel[0]
        node_id  = int(self._dir_tree.item(item)["values"][1])
        filename = self._dir_tree.item(item)["text"]

        # Get content hash from dir tree stored data or via node lookup
        content_hash = self._dir_tree.item(item).get("tags", [""])[0] if False else None
        # We need to look up the node — use snap_dir_children data or call object_locate
        self._ensure_nodes_loaded()
        nd = self._node_map.get(node_id)
        if not nd:
            return
        content_hash = nd["content_hash"]
        if content_hash == ZERO_HASH:
            if int(nd["type"]) == NODE_TYPE_REG and int(nd["size"]) == 0:
                messagebox.showinfo("Empty file",
                                    f"{filename}\n\nThis file is empty (0 bytes).")
            return

        from ..widgets import show_object_preview
        is_symlink = int(nd["type"]) == NODE_TYPE_SYMLINK
        show_object_preview(self._frame, content_hash, self._repo_path,
                            filename=filename, decode_as_symlink=is_symlink)

    def _on_dir_right_click(self, event: tk.Event) -> None:
        item = self._dir_tree.identify_row(event.y)
        if not item:
            return
        self._dir_tree.selection_set(item)
        node_id = int(self._dir_tree.item(item)["values"][1])
        self._ensure_nodes_loaded()
        nd = self._node_map.get(node_id)
        has_content = nd is not None and nd["content_hash"] != ZERO_HASH
        self._dir_ctx.entryconfig("Export file…",
                                  state=tk.NORMAL if has_content else tk.DISABLED)
        self._dir_ctx.tk_popup(event.x_root, event.y_root)

    def _export_dir_item(self) -> None:
        sel = self._dir_tree.selection()
        if not sel or not self._current_snap_id or not self._repo_path:
            return
        item = sel[0]
        node_id  = int(self._dir_tree.item(item)["values"][1])
        filename = self._dir_tree.item(item)["text"]
        self._ensure_nodes_loaded()
        nd = self._node_map.get(node_id)
        if nd is None or nd["content_hash"] == ZERO_HASH:
            messagebox.showinfo("No content", "This item has no file content to export.")
            return

        try:
            cr = call(self._repo_path, "object_content", hash=nd["content_hash"])
            data = base64.b64decode(cr.get("content_base64", ""))
        except RPCError as e:
            messagebox.showerror("Error", f"Could not load object: {e}")
            return

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

    # ---- dir tree (lazy via snap_dir_children RPC) ----

    def _dir_tag_from_child(self, child: dict) -> str:
        ntype = int(child.get("type", 0))
        if ntype == NODE_TYPE_DIR:
            return "dir"
        if ntype == NODE_TYPE_SYMLINK:
            return "symlink"
        if ntype == NODE_TYPE_REG and int(child.get("size", 1)) == 0:
            return "empty"
        return ""

    def _kind_label_from_child(self, child: dict) -> str:
        ntype = int(child.get("type", 0))
        return NODE_TYPE_NAMES.get(ntype, "")

    def _populate_dir_tree(self, root_data: dict) -> None:
        rows = self._dir_tree.get_children()
        if rows:
            self._dir_tree.delete(*rows)

        for child in sorted(root_data.get("children", []),
                            key=lambda x: x.get("name", "")):
            nid = int(child["node_id"])
            tag = self._dir_tag_from_child(child)
            item = self._dir_tree.insert(
                "", tk.END,
                text=child.get("name", "?"),
                values=(self._kind_label_from_child(child), nid),
                tags=(tag,) if tag else ())
            if child.get("has_children"):
                self._dir_tree.insert(item, tk.END, text=_DIR_SENTINEL)

    def _on_dir_open(self, _event) -> None:
        item = self._dir_tree.focus()
        children = self._dir_tree.get_children(item)
        if not (len(children) == 1 and
                self._dir_tree.item(children[0])["text"] == _DIR_SENTINEL):
            return
        self._expand_item(item)

    def _expand_item(self, item: str) -> None:
        children = self._dir_tree.get_children(item)
        if not (len(children) == 1 and
                self._dir_tree.item(children[0])["text"] == _DIR_SENTINEL):
            return
        self._dir_tree.delete(children[0])

        node_id = int(self._dir_tree.item(item)["values"][1])
        if not self._repo_path or not self._current_snap_id:
            return

        try:
            data = call(self._repo_path, "snap_dir_children",
                        id=self._current_snap_id, parent_node=node_id)
        except RPCError:
            return

        for child in sorted(data.get("children", []),
                            key=lambda x: x.get("name", "")):
            nid = int(child["node_id"])
            tag = self._dir_tag_from_child(child)
            new_item = self._dir_tree.insert(
                item, tk.END,
                text=child.get("name", "?"),
                values=(self._kind_label_from_child(child), nid),
                tags=(tag,) if tag else ())
            if child.get("has_children"):
                self._dir_tree.insert(new_item, tk.END, text=_DIR_SENTINEL)

        self._dir_tree.item(item, open=True)

    # ---- navigation helpers ----

    def navigate_to_path(self, snap_id: int, full_path: str) -> None:
        if snap_id not in self._snap_ids:
            return
        idx = self._snap_ids.index(snap_id)
        self._list.selection_clear(0, tk.END)
        self._list.selection_set(idx)
        self._list.see(idx)
        if self._current_snap_id != snap_id:
            self._load(snap_id)
        self._snap_nb.select(2)
        self._frame.after(50, lambda p=full_path: self._reveal_by_path(p))

    def _reveal_by_path(self, full_path: str) -> None:
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
        """Navigate to a node by ID — requires loading full snap for path resolution."""
        if not self._current_snap_id or not self._repo_path:
            return

        # Need the full snap to build parent chain
        try:
            s = call(self._repo_path, "snap", id=self._current_snap_id)
        except RPCError:
            return

        parent_of: dict[int, int] = {}
        name_of: dict[int, str] = {}
        for d in s.get("dirents", []):
            nid = int(d["node_id"])
            parent_of[nid] = int(d["parent_node"])
            name_of[nid] = d.get("name", "?")

        chain: list[int] = []
        nid = target_node_id
        visited: set[int] = set()
        while nid in parent_of and nid not in visited:
            visited.add(nid)
            chain.append(nid)
            nid = parent_of[nid]
        chain.reverse()

        # Build path and use _reveal_by_path
        path = "/".join(name_of.get(n, "?") for n in chain)
        self._reveal_by_path(path)
