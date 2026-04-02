import tkinter as tk
from tkinter import ttk, messagebox

from ..rpc import call, RPCError
from ..widgets import PAD, FONT_MONO, FONT_BOLD

_BOOL_FIELDS = [
    ("auto_pack",    "Pack loose objects after each backup"),
    ("auto_gc",      "Remove unreferenced objects after each backup"),
    ("auto_prune",   "Apply retention policy after each backup"),
    ("verify_after", "Verify object existence after each backup"),
    ("strict_meta",  "Strict metadata mode (xattr + ACL, slower)"),
]
_INT_FIELDS = [
    ("keep_snaps",   "Minimum snapshots to keep"),
    ("keep_daily",   "Keep one per day  (N days)"),
    ("keep_weekly",  "Keep one per week (N weeks)"),
    ("keep_monthly", "Keep one per month (N months)"),
    ("keep_yearly",  "Keep one per year (N years)"),
]
_DEFAULTS = {
    "auto_pack": True, "auto_gc": True, "auto_prune": True,
    "verify_after": False, "strict_meta": False,
    "keep_snaps": 1, "keep_daily": 0, "keep_weekly": 0,
    "keep_monthly": 0, "keep_yearly": 0,
    "paths": [], "exclude": [],
}


class PolicyTab:
    def __init__(self, nb: ttk.Notebook):
        self._frame = ttk.Frame(nb)
        nb.add(self._frame, text="Policy")
        self._repo_path: str | None = None
        self._bool_vars: dict[str, tk.BooleanVar] = {}
        self._int_vars:  dict[str, tk.StringVar]  = {}
        self._build()

    def _build(self) -> None:
        canvas = tk.Canvas(self._frame, highlightthickness=0)
        sb = ttk.Scrollbar(self._frame, orient=tk.VERTICAL, command=canvas.yview)
        canvas.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        inner = ttk.Frame(canvas)
        win_id = canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>",
                   lambda _: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
                    lambda e: canvas.itemconfig(win_id, width=e.width))

        row = 0

        self._status_label = tk.Label(inner, text="", fg="gray")
        self._status_label.grid(row=row, column=0, columnspan=3,
                                sticky="w", padx=PAD, pady=(PAD, 0))
        row += 1

        # Paths
        tk.Label(inner, text="Paths to back up", font=FONT_BOLD).grid(
            row=row, column=0, sticky="w", padx=PAD, pady=(PAD, 2))
        row += 1
        self._paths_text = tk.Text(inner, font=FONT_MONO, height=4, width=60)
        self._paths_text.grid(row=row, column=0, columnspan=2, sticky="ew", padx=PAD)
        row += 1
        tk.Label(inner, text="One absolute path per line", fg="gray").grid(
            row=row, column=0, sticky="w", padx=PAD)
        row += 1

        # Exclude
        tk.Label(inner, text="Exclude paths", font=FONT_BOLD).grid(
            row=row, column=0, sticky="w", padx=PAD, pady=(PAD, 2))
        row += 1
        self._exclude_text = tk.Text(inner, font=FONT_MONO, height=4, width=60)
        self._exclude_text.grid(row=row, column=0, columnspan=2, sticky="ew", padx=PAD)
        row += 1
        tk.Label(inner, text="One absolute path per line", fg="gray").grid(
            row=row, column=0, sticky="w", padx=PAD)
        row += 1

        # Retention
        tk.Label(inner, text="Retention (GFS)", font=FONT_BOLD).grid(
            row=row, column=0, sticky="w", padx=PAD, pady=(PAD, 2))
        row += 1
        for key, label in _INT_FIELDS:
            var = tk.StringVar(value="0")
            self._int_vars[key] = var
            tk.Label(inner, text=label).grid(row=row, column=0, sticky="w",
                                             padx=(PAD * 3, 0))
            tk.Entry(inner, textvariable=var, width=8, font=FONT_MONO).grid(
                row=row, column=1, sticky="w", padx=4)
            row += 1

        # Automation
        tk.Label(inner, text="Automation", font=FONT_BOLD).grid(
            row=row, column=0, sticky="w", padx=PAD, pady=(PAD, 2))
        row += 1
        for key, label in _BOOL_FIELDS:
            var = tk.BooleanVar(value=_DEFAULTS[key])
            self._bool_vars[key] = var
            ttk.Checkbutton(inner, text=label, variable=var).grid(
                row=row, column=0, columnspan=2, sticky="w", padx=(PAD * 3, 0))
            row += 1

        # Save button
        btn_frame = tk.Frame(inner)
        btn_frame.grid(row=row, column=0, columnspan=2, sticky="w",
                       padx=PAD, pady=(PAD * 2, PAD))
        tk.Button(btn_frame, text="Save policy.toml", command=self._save).pack(side=tk.LEFT)
        self._save_status = tk.Label(btn_frame, text="", fg="green")
        self._save_status.pack(side=tk.LEFT, padx=8)

        inner.columnconfigure(1, weight=1)

    def populate(self, repo_path: str) -> None:
        self._repo_path = repo_path

        fmt_str = ""
        try:
            scan = call(repo_path, "scan")
            fmt_val = scan.get("format")
            if fmt_val:
                fmt_str = f"Format: {fmt_val}  |  "
        except RPCError:
            pass

        # Load policy via RPC
        try:
            data = call(repo_path, "policy")
        except RPCError:
            data = None

        if data is None or data == {}:
            self._status_label.config(
                text=f"{fmt_str}No policy.toml — defaults shown. Save to create.",
                fg="gray")
            data = dict(_DEFAULTS)
        else:
            self._status_label.config(
                text=f"{fmt_str}policy.toml loaded via RPC", fg="gray")

        self._paths_text.delete("1.0", tk.END)
        self._paths_text.insert("1.0", "\n".join(data.get("paths", [])))

        self._exclude_text.delete("1.0", tk.END)
        self._exclude_text.insert("1.0", "\n".join(data.get("exclude", [])))

        for key, _ in _INT_FIELDS:
            self._int_vars[key].set(str(data.get(key, _DEFAULTS.get(key, 0))))

        for key, _ in _BOOL_FIELDS:
            self._bool_vars[key].set(bool(data.get(key, _DEFAULTS.get(key, False))))

        self._save_status.config(text="")

    def _save(self) -> None:
        if not self._repo_path:
            messagebox.showinfo("No repo", "Open a repository first.")
            return

        paths   = [p.strip() for p in
                   self._paths_text.get("1.0", tk.END).splitlines() if p.strip()]
        exclude = [p.strip() for p in
                   self._exclude_text.get("1.0", tk.END).splitlines() if p.strip()]

        params: dict = {
            "paths": paths,
            "exclude": exclude,
        }
        for key, _ in _INT_FIELDS:
            try:
                params[key] = int(self._int_vars[key].get())
            except ValueError:
                params[key] = 0
        for key, _ in _BOOL_FIELDS:
            params[key] = bool(self._bool_vars[key].get())

        try:
            call(self._repo_path, "save_policy", **params)
            self._save_status.config(text="Saved.", fg="green")
            self._status_label.config(
                text="policy.toml saved via RPC", fg="gray")
        except RPCError as e:
            messagebox.showerror("Save error", str(e))
