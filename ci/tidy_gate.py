#!/usr/bin/env python3
"""clang-tidy gate: run the curated .clang-tidy over src/ and fail only on
warnings the recorded baseline does not already hold.

Why a baseline: the tree predates the gate, and the checks found a few
hundred existing warnings. Fixing them all first would hold the gate back
indefinitely; a gate that fails on every one would be switched off. Holding
the count where it is and failing on anything new keeps the next one out.

Baseline keys (ci/tidy-baseline.txt) are "<file> <check> <message>" with a
count, never a line number, so an edit that moves code does not churn the
baseline. Digits in the message are replaced with N for the same reason.
A warning is new when its file, check and message occur more often than the
baseline allows. A header's warning is reported once however many
translation units include it (deduplicated by its exact location).

Changed-files mode (--changed-from REF): only the .c files under src/ that
differ from REF are analysed. Any change to a header under src/, to
.clang-tidy, the baseline, this script or the Makefile falls back to the
full run, because those change what every translation unit sees. Branch
pipelines and `make tidy` always run in full.

clang-tidy's own errors (a file that does not compile under its flags) fail
the gate outright: a translation unit clang cannot parse is one it is not
checking.
"""

import argparse
import collections
import concurrent.futures
import os
import re
import shlex
import subprocess
import sys
import time

DIAG = re.compile(r"^(?P<file>[^:\s][^:]*):(?P<line>\d+):(?P<col>\d+): "
                  r"(?P<sev>warning|error): (?P<msg>.*?)(?: \[(?P<check>[^\]]+)\])?$")
FULL_RUN_TRIGGERS = (".clang-tidy", "ci/tidy-baseline.txt", "ci/tidy_gate.py", "Makefile")


def all_sources(root):
    out = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(root, "src")):
        dirnames.sort()
        for name in sorted(filenames):
            if name.endswith(".c"):
                out.append(os.path.relpath(os.path.join(dirpath, name), root))
    return out


def changed_sources(root, ref):
    """The .c files to check against ref, or None for a full run."""
    diff = subprocess.run(["git", "-C", root, "diff", "--name-only", ref, "HEAD"],
                          capture_output=True, text=True, check=False)
    if diff.returncode != 0:
        print(f"tidy: cannot diff against {ref} ({diff.stderr.strip()}); running in full")
        return None
    files = [p for p in diff.stdout.split("\n") if p]
    for p in files:
        if p in FULL_RUN_TRIGGERS or (p.startswith("src/") and p.endswith(".h")):
            print(f"tidy: {p} changed; running in full")
            return None
    return [p for p in files if p.startswith("src/") and p.endswith(".c")
            and os.path.exists(os.path.join(root, p))]


def norm_msg(msg):
    return re.sub(r"\d+", "N", msg.strip())


def run_one(tidy, root, path, flags):
    proc = subprocess.run([tidy, "-quiet", path, "--"] + flags, cwd=root,
                          capture_output=True, text=True, check=False)
    return path, proc.returncode, proc.stdout + proc.stderr


def parse(root, output):
    """(file, line, col, check, msg, sev) for diagnostics inside src/."""
    out = []
    for line in output.split("\n"):
        m = DIAG.match(line)
        if not m:
            continue
        path = os.path.normpath(os.path.relpath(
            os.path.join(root, m.group("file")), root))
        if not path.startswith("src" + os.sep):
            continue
        out.append((path, int(m.group("line")), int(m.group("col")),
                    m.group("check") or "clang-diagnostic-error",
                    m.group("msg"), m.group("sev")))
    return out


def read_baseline(path):
    base = collections.Counter()
    if not os.path.exists(path):
        return base
    with open(path, encoding="utf-8") as f:
        for num, raw in enumerate(f, 1):
            line = raw.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 4 or not parts[0].isdigit():
                sys.exit(f"{path}:{num}: expected 'count<TAB>file<TAB>check<TAB>message'")
            base[(parts[1], parts[2], parts[3])] = int(parts[0])
    return base


def write_baseline(path, counts, version):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("# clang-tidy warnings the gate tolerates: count, file, check, message.\n")
        f.write("# Regenerate with `make tidy-baseline` (full run). Lower is the only\n")
        f.write("# direction it should move; a new entry is a reviewer's decision.\n")
        f.write(f"# {version}\n")
        for key in sorted(counts):
            f.write(f"{counts[key]}\t{key[0]}\t{key[1]}\t{key[2]}\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=".")
    ap.add_argument("--tidy", default=os.environ.get("CLANG_TIDY", "clang-tidy"))
    ap.add_argument("--baseline", default=None,
                    help="default: <root>/ci/tidy-baseline.txt")
    ap.add_argument("--flags", default=os.environ.get("TIDY_FLAGS", ""),
                    help="compiler flags for every file (default: $TIDY_FLAGS)")
    ap.add_argument("--virt-flags", default=os.environ.get("TIDY_VIRT_FLAGS", ""),
                    help="extra flags for src/virt/ (default: $TIDY_VIRT_FLAGS); "
                         "without them src/virt/ is skipped, as the build skips it")
    ap.add_argument("--changed-from", default=None, metavar="REF")
    ap.add_argument("--update", action="store_true", help="rewrite the baseline")
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 1)
    ap.add_argument("files", nargs="*", help="check these instead (testing)")
    args = ap.parse_args(argv)

    root = os.path.abspath(args.root)
    baseline_path = args.baseline or os.path.join(root, "ci", "tidy-baseline.txt")
    flags = shlex.split(args.flags)
    virt_flags = shlex.split(args.virt_flags)

    full = True
    if args.files:
        files = args.files
        full = False
    else:
        files = None
        if args.changed_from and not args.update:
            files = changed_sources(root, args.changed_from)
            full = files is None
        if files is None:
            files = all_sources(root)
    if not virt_flags and any(p.startswith("src/virt/") for p in files):
        if args.update:
            sys.exit("tidy: --update needs libvirt's flags, or src/virt/ drops out of the baseline")
        print("tidy: no libvirt flags; skipping src/virt/, as the build does")
        files = [p for p in files if not p.startswith("src/virt/")]
    if not files:
        print("tidy: no C sources changed; nothing to check")
        return 0

    version = subprocess.run([args.tidy, "--version"], capture_output=True,
                             text=True, check=False).stdout
    version = " ".join(l.strip() for l in version.split("\n") if "version" in l)
    print(f"tidy: {len(files)} file(s), {args.jobs} job(s), {version}")

    t0 = time.monotonic()
    seen = set()
    crashed = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futs = [pool.submit(run_one, args.tidy, root, p,
                            flags + (virt_flags if p.startswith("src/virt/") else []))
                for p in files]
        for fut in concurrent.futures.as_completed(futs):
            path, rc, out = fut.result()
            diags = parse(root, out)
            seen.update(diags)
            if rc != 0 and not any(d[5] == "error" for d in diags):
                crashed.append((path, out.strip().split("\n")[-1:]))
    elapsed = time.monotonic() - t0

    errors = sorted(d for d in seen if d[5] == "error")
    counts = collections.Counter((d[0], d[3], norm_msg(d[4]))
                                 for d in seen if d[5] == "warning")
    print(f"tidy: {sum(counts.values())} warning(s) in {elapsed:.1f} s")

    for path, tail in crashed:
        print(f"tidy: FAIL clang-tidy exited non-zero on {path}: {tail}")
    for d in errors:
        print(f"tidy: FAIL {d[0]}:{d[1]}:{d[2]}: error: {d[4]} [{d[3]}]")
    if crashed or errors:
        print("tidy: FAIL (a file that does not parse is not being checked)")
        return 1

    if args.update:
        if not full:
            sys.exit("tidy: --update needs a full run")
        write_baseline(baseline_path, counts, version)
        print(f"tidy: baseline rewritten, {sum(counts.values())} warning(s)")
        return 0

    base = read_baseline(baseline_path)
    new = []
    for key, n in sorted(counts.items()):
        if n > base.get(key, 0):
            new.append((key, n, base.get(key, 0)))
    if new:
        for key, n, allowed in new:
            locs = sorted(f"{d[0]}:{d[1]}:{d[2]}" for d in seen
                          if d[5] == "warning" and (d[0], d[3], norm_msg(d[4])) == key)
            print(f"tidy: NEW {key[1]}: {key[2]} ({n} found, baseline {allowed})")
            for loc in locs:
                print(f"    {loc}")
        print(f"tidy: FAIL ({len(new)} new warning key(s); fix them, or if one is a "
              "false positive say why with a NOLINT(<check>) comment)")
        return 1

    if full:
        fixed = sum(n - counts.get(key, 0) for key, n in base.items()
                    if n > counts.get(key, 0))
        if fixed:
            print(f"tidy: note {fixed} baseline warning(s) are gone; "
                  "`make tidy-baseline` locks that in")
    print("tidy: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
