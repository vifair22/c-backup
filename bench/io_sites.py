#!/usr/bin/env python3
"""io_sites.py counters.N: symbolise the fsync/fdatasync sites (addr2line -f over each mapped object)."""
import sys, subprocess, re
maps = []
lines = open(sys.argv[1]).read().splitlines()
for l in lines:
    if l.startswith("map "):
        f = l[4:].split()
        if len(f) < 6: continue
        lo, hi = (int(x, 16) for x in f[0].split("-"))
        maps.append((lo, hi, int(f[2], 16), f[5]))
def sym(a):
    for lo, hi, off, path in maps:
        if lo <= a < hi:
            rel = a - lo + off
            try:
                out = subprocess.run(["addr2line", "-f", "-C", "-e", path, hex(rel - 1)], capture_output=True, text=True).stdout.split("\n")[0]
            except Exception:
                out = "?"
            if out in ("??", ""):
                out = path.rsplit("/", 1)[-1] + "+" + hex(rel)
            return out
    return hex(a)
for l in lines:
    if not l.startswith("site "): continue
    head, frames = l.split(" :", 1)
    fr = [int(x, 16) for x in frames.split()]
    names = [sym(a) for a in fr[1:7]]
    names = [n for n in names if not n.startswith("libsqlite3")][:5]
    print(head[5:], "<-", " <- ".join(names))
