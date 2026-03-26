import os
import tkinter as tk
from tkinter import ttk, messagebox

from ..parsers import parse_tag, ParseError
from ..widgets import make_text_widget, set_text, PAD, FONT_MONO, FONT_BOLD


class TagsTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Tags")
        self._tag_paths: list[str] = []
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

    def populate(self, scan: dict) -> None:
        self._tag_paths = scan["tags"]
        self._list.delete(0, tk.END)
        for path in self._tag_paths:
            self._list.insert(tk.END, os.path.basename(path))

    def _on_select(self, _event) -> None:
        sel = self._list.curselection()
        if not sel:
            return
        path = self._tag_paths[sel[0]]
        try:
            tag = parse_tag(path)
        except ParseError as e:
            messagebox.showerror("Parse error", str(e))
            return
        lines = [f"File     : {path}", f"Name     : {os.path.basename(path)}"]
        for k, v in tag.items():
            lines.append(f"{k:8} : {v}")
        set_text(self._text, "\n".join(lines))
