#!/usr/bin/env python3
"""File-size ratchet: no first-party C source or header may pass a ceiling
of code lines, and a file already past it may not grow.

Why a ratchet and not a flat limit: pack.c and a few others were already
well past any sensible ceiling when the gate went in, and splitting them is
1.1 work. A flat limit would start red and be overridden until it meant
nothing; a ceiling that holds every file at its current size catches the
next 400-line addition to a 3,000-line file, which is how they got there.

How a line is counted (code_lines below): a physical line counts when it
still holds a non-whitespace character after comments are removed. The
lexer knows /* */ (across lines), // (including a backslash-continued //
comment), and string and character literals, so "//" or "/*" inside a
literal is code, not a comment. Preprocessor lines count. Blank lines, lines
holding only a comment, and lines holding only the inside or the closing of a
block comment do not. The count depends on the file's bytes alone, so it is
the same on every machine.

Scope: every *.c and *.h under src/, tests/ and bench/. Nothing there is
vendored (cJSON comes from c-utils, outside the tree); ui/ is JavaScript and
out of scope. build/ is never scanned.

The baseline (ci/size-baseline.txt) lists each file that is over the
ceiling, with the count it may not exceed. --update lowers entries to the
current counts and drops files that are gone or back under the ceiling; it
never raises an entry or adds one, so a file can only get there by editing
the baseline by hand, which a reviewer sees.
"""

import argparse
import os
import sys

CEILING = 2000
SCAN_DIRS = ("src", "tests", "bench")
SUFFIXES = (".c", ".h")


def code_lines(text):
    """Count lines holding code once comments are removed."""
    count = 0
    in_block = False      # inside /* */
    in_line_cmt = False   # inside a // comment continued by a backslash
    for line in text.split("\n"):
        has_code = False
        i = 0
        n = len(line)
        if in_line_cmt:
            in_line_cmt = line.endswith("\\")
            continue
        quote = None
        while i < n:
            c = line[i]
            if in_block:
                if line.startswith("*/", i):
                    in_block = False
                    i += 2
                else:
                    i += 1
                continue
            if quote:
                has_code = True
                if c == "\\":
                    i += 2
                    continue
                if c == quote:
                    quote = None
                i += 1
                continue
            if line.startswith("/*", i):
                in_block = True
                i += 2
                continue
            if line.startswith("//", i):
                in_line_cmt = line.endswith("\\")
                break
            if c in "\"'":
                quote = c
                has_code = True
            elif not c.isspace():
                has_code = True
            i += 1
        if has_code:
            count += 1
    return count


def scan(root):
    """Map of repo-relative path -> code lines, for every file in scope."""
    out = {}
    for top in SCAN_DIRS:
        base = os.path.join(root, top)
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames.sort()
            for name in sorted(filenames):
                if not name.endswith(SUFFIXES):
                    continue
                path = os.path.join(dirpath, name)
                rel = os.path.relpath(path, root)
                with open(path, "r", encoding="utf-8",
                          errors="surrogateescape") as f:
                    out[rel] = code_lines(f.read())
    return out


def read_baseline(path):
    base = {}
    if not os.path.exists(path):
        return base
    with open(path, encoding="utf-8") as f:
        for num, raw in enumerate(f, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 2 or not parts[0].isdigit():
                sys.exit(f"{path}:{num}: expected '<count> <path>', got {raw!r}")
            base[parts[1]] = int(parts[0])
    return base


def write_baseline(path, base):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Files over the code-line ceiling and the count each may not exceed.\n")
        f.write("# Lower only: `make size-ratchet` rewrites this after a file shrinks.\n")
        f.write("# See ci/size_check.py for how lines are counted.\n")
        for rel in sorted(base):
            f.write(f"{base[rel]} {rel}\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=".")
    ap.add_argument("--baseline", default=None,
                    help="default: <root>/ci/size-baseline.txt")
    ap.add_argument("--update", action="store_true",
                    help="lower the baseline to the current counts")
    ap.add_argument("--init", action="store_true",
                    help="record every file now over the ceiling (first run only)")
    ap.add_argument("--report", action="store_true",
                    help="also list the largest files under the ceiling")
    args = ap.parse_args(argv)

    baseline_path = args.baseline or os.path.join(args.root, "ci", "size-baseline.txt")
    counts = scan(args.root)
    base = read_baseline(baseline_path)

    if args.init:
        if base:
            sys.exit("size: --init refuses to overwrite an existing baseline")
        new = {rel: n for rel, n in counts.items() if n > CEILING}
        write_baseline(baseline_path, new)
        print(f"size: baseline recorded, {len(new)} file(s) over {CEILING}")
        return 0

    fail = False
    slack = []
    for rel, n in sorted(counts.items()):
        limit = base.get(rel)
        if limit is None:
            if n > CEILING:
                print(f"size: FAIL {rel}: {n} code lines, ceiling {CEILING}. "
                      "Split it rather than growing it.")
                fail = True
        elif n > limit:
            print(f"size: FAIL {rel}: {n} code lines, may not grow past its "
                  f"recorded {limit} (ci/size-baseline.txt)")
            fail = True
        elif n < limit:
            slack.append((rel, n, limit))

    stale = sorted(rel for rel in base
                   if rel not in counts or counts[rel] <= CEILING)

    if args.update:
        new = {rel: min(base[rel], counts[rel]) for rel in base
               if rel in counts and counts[rel] > CEILING}
        write_baseline(baseline_path, new)
        print(f"size: baseline rewritten, {len(new)} file(s) over {CEILING}")
        return 1 if fail else 0

    for rel, n, limit in slack:
        print(f"size: note {rel} is down to {n} from {limit}; "
              "`make size-ratchet` locks that in")
    for rel in stale:
        print(f"size: note {rel} is no longer over the ceiling; "
              "`make size-ratchet` drops its entry")
    if args.report:
        under = sorted(((n, rel) for rel, n in counts.items() if rel not in base),
                       reverse=True)[:5]
        for n, rel in under:
            print(f"size: largest under the ceiling: {n} {rel}")
    if fail:
        print("size: FAIL")
        return 1
    print(f"size: OK ({len(counts)} files, {len(base)} held at their baseline)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
