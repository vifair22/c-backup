import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from .rpc import RPCError
from .widgets import PAD
from .tabs import (
    OverviewTab, SnapshotsTab, PacksTab,
    LooseTab, TagsTab, PolicyTab, LookupTab,
    DiffTab, AnalyticsTab, SearchTab, GFSTreeTab,
)


class ViewerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("c-backup Repository Viewer")
        self.geometry("1200x850")
        self.repo_path: str | None = None
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
        snaps_idx  = list(self._tabs.keys()).index("snapshots")

        def nav(snap_id: int, full_path: str) -> None:
            self._nb.select(snaps_idx)
            snaps_tab.navigate_to_path(snap_id, full_path)

        search_tab.set_navigate_callback(nav)

    # ---- open repo ----

    def _open_repo(self) -> None:
        path = filedialog.askdirectory(title="Select repository directory")
        if path:
            self.load_repo(path)

    def load_repo(self, path: str) -> None:
        if not os.path.isdir(path):
            messagebox.showerror("Error", f"Not a directory: {path}")
            return
        self.repo_path = path
        self._repo_var.set(path)
        for tab in self._tabs.values():
            tab.populate(path)
        self.title(f"c-backup Viewer — {path}")
