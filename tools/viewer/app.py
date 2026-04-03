import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog

from .rpc import (RPCError, is_remote, parse_remote, ssh_connect,
                  open_session, close_session, call)
from .widgets import PAD, install_poll, ui_call
from .tabs import (
    OverviewTab, SnapshotsTab, PacksTab,
    LooseTab, TagsTab, PolicyTab, LookupTab,
    DiffTab, AnalyticsTab, SearchTab, GFSTreeTab,
)

# Map tab key → which repo_summary sub-keys it needs
_SUMMARY_KEYS = {
    "overview":  ("scan", "list"),
    "analytics": ("all_pack_entries", "loose_list"),
    "search":    ("list",),
    "diff":      ("list",),
    "snapshots": ("list",),
    "loose":     ("loose_list",),
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
        self.config(menu=mb)

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

        # Open session + fetch summary in background
        def _load():
            # Try to open a persistent session
            open_session(path)

            # Try consolidated summary
            try:
                summary = call(path, "repo_summary")
            except RPCError:
                summary = None

            if summary:
                ui_call(lambda: self._apply_summary(path, summary))
            else:
                # Fallback: per-tab populate
                ui_call(lambda: self._populate_all(path))

        threading.Thread(target=_load, daemon=True).start()

    def _apply_summary(self, path: str, summary: dict) -> None:
        """Distribute pre-fetched summary data to each tab."""
        for key, tab in self._tabs.items():
            needed = _SUMMARY_KEYS.get(key, ())
            sub = {k: summary.get(k) for k in needed}
            if hasattr(tab, "populate_from_summary"):
                tab.populate_from_summary(path, sub)
            else:
                tab.populate(path)

    def _populate_all(self, path: str) -> None:
        """Legacy fallback: each tab fetches its own data."""
        for tab in self._tabs.values():
            tab.populate(path)
