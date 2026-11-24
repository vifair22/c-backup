#!/usr/bin/env python3
"""io_threads.py threads.raw: CPU seconds per thread name over the run (first to last sample)."""
import sys, collections
first, last, names = {}, {}, {}
t0 = t1 = None
for line in open(sys.argv[1]):
    ts, rest = line.split(" ", 1)
    ts = float(ts)
    tid = rest.split(" ", 1)[0]
    name = rest[rest.index("(") + 1: rest.rindex(")")]
    f = rest[rest.rindex(")") + 2:].split()
    cpu = (int(f[11]) + int(f[12])) / 100.0   # utime, stime (fields 14, 15)
    first.setdefault(tid, cpu); last[tid] = cpu; names[tid] = name
    t0 = ts if t0 is None else t0; t1 = ts
agg = collections.defaultdict(lambda: [0.0, 0])
for tid in last:
    agg[names[tid]][0] += last[tid] - first[tid]; agg[names[tid]][1] += 1
print(f"span {t1 - t0:.2f}s")
for n, (c, k) in sorted(agg.items(), key=lambda x: -x[1][0]):
    if c > 0.005: print(f"{c:8.2f}s  x{k:<3d} {n}")
