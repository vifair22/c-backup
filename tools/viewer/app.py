import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog

from .rpc import (RPCError, is_remote, parse_remote, ssh_connect,
                  open_session, close_session, call,
                  VIEWER_VERSION, BACKUP_BIN, get_binary_version)
from .widgets import PAD, install_poll, ui_call
from .tabs import (
    OverviewTab, SnapshotsTab, PacksTab,
    LooseTab, TagsTab, PolicyTab, LookupTab,
    DiffTab, AnalyticsTab, SearchTab, GFSTreeTab,
)

# Map tab key → which repo_summary sub-keys it needs
_SUMMARY_KEYS = {
    "overview":  ("scan", "list"),
    "analytics": ("repo_stats",),
    "search":    ("list",),
    "diff":      ("list",),
    "snapshots": ("list",),
    "loose":     ("loose_stats",),
    "packs":     ("scan", "global_pack_index"),
    "tags":      ("tags",),
    "policy":    ("scan", "policy"),
    "lookup":    (),
    "gfs_tree":  ("list",),
}


class ViewerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("c-backup Repository Viewer")
        self.geometry("1200x850")
        self.repo_path: str | None = None
        install_poll(self)
        self._build_menu()
        self._build_layout()

    # ---- menu ----

    def _build_menu(self) -> None:
        mb = tk.Menu(self)
        file_menu = tk.Menu(mb, tearoff=0)
        file_menu.add_command(label="Open Repository…", command=self._open_repo)
        file_menu.add_separator()
        file_menu.add_command(label="Quit", command=self.quit)
        mb.add_cascade(label="File", menu=file_menu)

        help_menu = tk.Menu(mb, tearoff=0)
        help_menu.add_command(label="About", command=self._show_about)
        mb.add_cascade(label="Help", menu=help_menu)

        self.config(menu=mb)

    def _show_about(self) -> None:
        bin_ver = get_binary_version(self.repo_path)

        dlg = tk.Toplevel(self)
        dlg.title("About c-backup Viewer")
        dlg.resizable(False, False)
        dlg.transient(self)
        dlg.grab_set()

        frame = ttk.Frame(dlg, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        # Title
        ttk.Label(frame, text="c-backup Repository Viewer",
                  font=("TkDefaultFont", 14, "bold")).pack(anchor="w")

        ttk.Separator(frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(10, 8))

        # Viewer section
        ttk.Label(frame, text="Viewer",
                  font=("TkDefaultFont", 11, "bold")).pack(anchor="w")
        detail = ttk.Frame(frame)
        detail.pack(anchor="w", padx=(12, 0), pady=(2, 8))
        ttk.Label(detail, text=f"Version:  {VIEWER_VERSION}",
                  font=("TkDefaultFont", 9)).pack(anchor="w")

        ttk.Separator(frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=(0, 8))

        # Binary section
        ttk.Label(frame, text="Backup Binary",
                  font=("TkDefaultFont", 11, "bold")).pack(anchor="w")
        detail2 = ttk.Frame(frame)
        detail2.pack(anchor="w", padx=(12, 0), pady=(2, 8))
        ttk.Label(detail2, text=f"Version:  {bin_ver}",
                  font=("TkDefaultFont", 9)).pack(anchor="w")
        ttk.Label(detail2, text=f"Path:     {BACKUP_BIN}",
                  font=("TkDefaultFont", 9)).pack(anchor="w")

        # Close button
        ttk.Button(frame, text="Close", command=dlg.destroy).pack(pady=(8, 0))

        # Center on parent
        dlg.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() - dlg.winfo_width()) // 2
        y = self.winfo_y() + (self.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")

    # ---- layout ----

    def _build_layout(self) -> None:
        top = tk.Frame(self, bd=1, relief=tk.SUNKEN)
        top.pack(fill=tk.X, padx=PAD, pady=PAD)
        tk.Label(top, text="Repository:").pack(side=tk.LEFT)
        self._repo_var = tk.StringVar(value="(none)")
        tk.Label(top, textvariable=self._repo_var, anchor="w").pack(side=tk.LEFT, padx=4)

        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=PAD, pady=(0, PAD))

        self._tabs = {
            "overview":   OverviewTab(nb),
            "analytics":  AnalyticsTab(nb),
            "search":     SearchTab(nb),
            "diff":       DiffTab(nb),
            "snapshots":  SnapshotsTab(nb),
            "loose":      LooseTab(nb),
            "packs":      PacksTab(nb),
            "tags":       TagsTab(nb),
            "lookup":     LookupTab(nb),
            "policy":     PolicyTab(nb),
            "gfs_tree":   GFSTreeTab(nb),
        }
        self._nb = nb
        self._wire_navigation()

    def _wire_navigation(self) -> None:
        snaps_tab  = self._tabs["snapshots"]
        search_tab = self._tabs["search"]
        gfs_tab    = self._tabs["gfs_tree"]
        snaps_idx  = list(self._tabs.keys()).index("snapshots")

        def nav(snap_id: int, full_path: str) -> None:
            self._nb.select(snaps_idx)
            snaps_tab.navigate_to_path(snap_id, full_path)

        def nav_snap(snap_id: int) -> None:
            self._nb.select(snaps_idx)
            snaps_tab.navigate_to_path(snap_id, "")

        search_tab.set_navigate_callback(nav)
        gfs_tab.set_navigate_callback(nav_snap)

    # ---- SSH auth ----

    def _ensure_ssh(self, host: str) -> bool:
        """Establish SSH connection, prompting for password if needed."""
        # Try key auth first
        if ssh_connect(host):
            return True

        # Key auth failed — ask for password
        password = simpledialog.askstring(
            "SSH Authentication",
            f"Password for {host}:",
            show="*",
            parent=self)
        if not password:
            return False

        if ssh_connect(host, password):
            return True

        messagebox.showerror(
            "SSH Error",
            f"Authentication failed for {host}",
            parent=self)
        return False

    # ---- open repo ----

    def _open_repo(self) -> None:
        path = filedialog.askdirectory(title="Select repository directory")
        if path:
            self.load_repo(path)

    def load_repo(self, path: str) -> None:
        if is_remote(path):
            host, _ = parse_remote(path)
            if not self._ensure_ssh(host):
                return
        elif not os.path.isdir(path):
            messagebox.showerror("Error", f"Not a directory: {path}")
            return

        # Close any previous session
        if self.repo_path:
            close_session(self.repo_path)

        self.repo_path = path
        self._repo_var.set(path)
        self.title(f"c-backup Viewer — {path}")

        # Show loading state on all tabs
        for tab in self._tabs.values():
            if hasattr(tab, "set_loading"):
                tab.set_loading()

        self._rpc_cache: dict[str, object] = {}
        self._populated_tabs: set[str] = set()

        def _load():
            open_session(path)

            # Priority 1: overview (user sees this immediately)
            self._fetch_and_populate(path, "overview")

            # Priority 2: snapshots (common first click, uses cached "list")
            self._fetch_and_populate(path, "snapshots")

            # Priority 3: cheap deterministic data (tags, policy)
            for key in ("tags", "policy"):
                if key not in self._rpc_cache:
                    try:
                        self._rpc_cache[key] = call(path, key)
                    except RPCError:
                        self._rpc_cache[key] = None

            # Priority 4: heavier stats (repo_stats, loose_stats,
            # global_pack_index) — these scan idx files and object dirs,
            # so they go last to avoid blocking the UI on large repos
            for key in ("repo_stats", "loose_stats", "global_pack_index"):
                if key not in self._rpc_cache:
                    try:
                        self._rpc_cache[key] = call(path, key)
                    except RPCError:
                        self._rpc_cache[key] = None

            # Deliver all tabs whose data is now fully cached
            for tab_key in self._tabs:
                if tab_key not in self._populated_tabs:
                    needed = _SUMMARY_KEYS.get(tab_key, ())
                    if all(k in self._rpc_cache for k in needed):
                        self._deliver_tab(path, tab_key)

        threading.Thread(target=_load, daemon=True).start()
        self._nb.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    def _active_tab_key(self) -> str:
        """Return the key of the currently selected notebook tab."""
        try:
            idx = self._nb.index(self._nb.select())
        except Exception:
            idx = 0
        keys = list(self._tabs.keys())
        return keys[idx] if idx < len(keys) else keys[0]

    def _on_tab_changed(self, _event=None) -> None:
        path = self.repo_path
        if not path:
            return
        key = self._active_tab_key()
        if key in self._populated_tabs:
            return
        # Data may not be cached yet if background load is still running
        threading.Thread(
            target=self._fetch_and_populate, args=(path, key),
            daemon=True).start()

    def _fetch_and_populate(self, path: str, tab_key: str) -> None:
        """Fetch any missing RPC keys for a tab, then deliver to the UI."""
        needed = _SUMMARY_KEYS.get(tab_key, ())
        for sub_key in needed:
            if sub_key not in self._rpc_cache:
                try:
                    self._rpc_cache[sub_key] = call(path, sub_key)
                except RPCError as e:
                    print(f"[RPC] error fetching {sub_key}: {e}", flush=True)
                    self._rpc_cache[sub_key] = None
        self._deliver_tab(path, tab_key)

    def _deliver_tab(self, path: str, tab_key: str) -> None:
        """Push cached data to a tab's UI. Must be called from any thread."""
        needed = _SUMMARY_KEYS.get(tab_key, ())
        sub = {k: self._rpc_cache.get(k) for k in needed}
        self._populated_tabs.add(tab_key)

        tab = self._tabs[tab_key]
        if hasattr(tab, "populate_from_summary"):
            ui_call(lambda t=tab, p=path, s=sub: t.populate_from_summary(p, s))
        else:
            ui_call(lambda t=tab, p=path: t.populate(p))
