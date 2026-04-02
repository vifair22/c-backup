import tkinter as tk
from tkinter import ttk, messagebox

from ..rpc import call, RPCError
from ..widgets import make_text_widget, set_text, PAD, FONT_MONO, FONT_BOLD


class TagsTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Tags")
        self._repo_path: str | None = None
        self._tags: list[dict] = []
        self._build()

    def _build(self) -> None:
        pane = ttk.PanedWindow(self._frame, orient=tk.HORIZONTAL)
        pane.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(pane)
        pane.add(left, weight=1)
        tk.Label(left, text="Tags", font=FONT_BOLD).pack(anchor="w", padx=PAD)
        self._list = tk.Listbox(left, font=FONT_MONO, width=22)
        sb = ttk.Scrollbar(left, command=self._list.yview)
        self._list.config(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self._list.pack(fill=tk.BOTH, expand=True)
        self._list.bind("<<ListboxSelect>>", self._on_select)

        right = ttk.Frame(pane)
        pane.add(right, weight=4)
        self._text = make_text_widget(right)

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path
        self._list.delete(0, tk.END)
        try:
            data = call(repo_path, "tags")
            self._tags = data.get("tags", [])
        except RPCError as e:
            self._tags = []
            set_text(self._text, f"Error loading tags: {e}")
            return
        for tag in self._tags:
            self._list.insert(tk.END, tag.get("name", "?"))

    def populate_from_summary(self, repo_path: str, summary: dict) -> None:
        self._repo_path = repo_path
        self._list.delete(0, tk.END)
        data = summary.get("tags") or {}
        self._tags = data.get("tags", [])
        for tag in self._tags:
            self._list.insert(tk.END, tag.get("name", "?"))

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel:
            return
        tag = self._tags[sel[0]]
        lines = [f"Name     : {tag.get('name', '?')}"]
        for k, v in tag.items():
            if k != "name":
                lines.append(f"{k:8} : {v}")
        set_text(self._text, "\n".join(lines))
