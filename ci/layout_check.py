#!/usr/bin/env python3
"""Every directory under src/ has a row in docs/layout.md, and every row
names a directory that exists.

A row is a Markdown table line whose first cell starts with a backticked
path, `src/<dir>/`. Anything after the closing backtick in that cell (a
qualifier like "(databases)") is ignored, so one directory may have several
rows. A directory counts when it holds at least one file of its own.
"""

import argparse
import os
import re
import sys

ROW = re.compile(r"^\|\s*`(src(?:/[^`]*)?)`")


def src_dirs(root):
    out = set()
    for dirpath, _dirnames, filenames in os.walk(os.path.join(root, "src")):
        if filenames:
            rel = os.path.relpath(dirpath, root).replace(os.sep, "/")
            out.add(rel.rstrip("/") + "/")
    return out


def listed(doc):
    out = set()
    with open(doc, encoding="utf-8") as f:
        for line in f:
            m = ROW.match(line)
            if m:
                out.add(m.group(1).rstrip("/") + "/")
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=".")
    ap.add_argument("--doc", default=None, help="default: <root>/docs/layout.md")
    args = ap.parse_args(argv)
    doc = args.doc or os.path.join(args.root, "docs", "layout.md")
    if not os.path.exists(doc):
        print(f"layout: FAIL {doc} is missing")
        return 1
    have = src_dirs(args.root)
    rows = listed(doc)
    fail = False
    for d in sorted(have - rows):
        print(f"layout: FAIL {d} has no row in {os.path.relpath(doc, args.root)}")
        fail = True
    for d in sorted(rows - have):
        print(f"layout: FAIL {os.path.relpath(doc, args.root)} lists {d}, "
              "which does not exist")
        fail = True
    if fail:
        print("layout: FAIL")
        return 1
    print(f"layout: OK ({len(have)} directories)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
