#!/usr/bin/env python3
"""Turn bench/prof_shim.c's samples into a profile, one per segment.

    prof_report.py PROF_OUT [--top N] [--seg NAME] [--folded FILE]

For each segment (a step bench_phases marked) it prints:

  self       the innermost frame of each sample, as lib:function. On a wall
             clock a sample inside a system call wrapper is time spent
             waiting in the kernel.
  ours       the innermost frame inside the program's own binary, so time in
             SQLite, libc or liblz4 is charged to the c-backup function that
             called it. This is the column that says which of our code paths
             to change.
  site       as ours, but skipping c-utils' db_execute/db_exec_raw wrappers,
             so a statement is charged to the function that issued it.
  inclusive  every function on the stack, counted once per sample.

Symbols come from nm on each mapped file (the full symbol table where there
is one, the dynamic one otherwise), so a stripped library's internal function
shows as the nearest exported symbol before it: read SQLite's rows as "inside
SQLite, near X", and use the ours column to say why it was called. With LTO
most static helpers are inlined into their callers and do not appear.
"""

import bisect
import collections
import subprocess
import sys


def load_maps(path):
    maps = []
    bases = {}
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 6 or not parts[5].startswith('/'):
                continue
            lo, hi = (int(x, 16) for x in parts[0].split('-'))
            off = int(parts[2], 16)
            name = parts[5]
            maps.append((lo, hi, off, name))
            if off == 0 and name not in bases:
                bases[name] = lo
    maps.sort()
    return maps, bases


# c-utils' database wrappers and their helpers: the site column skips them so
# a statement's cost lands on the function that issued it.
GENERIC = ('db_execute', 'db_exec_raw', 'bind_params', 'result_append',
           '_set_error')

_symcache = {}


def symbols(path):
    if path in _symcache:
        return _symcache[path]
    out = []
    for args in (['nm', '-n', '--defined-only'], ['nm', '-D', '-n', '--defined-only']):
        try:
            txt = subprocess.run(args + [path], capture_output=True, text=True,
                                 check=False).stdout
        except OSError:
            txt = ''
        for line in txt.splitlines():
            p = line.split()
            if len(p) >= 3 and p[1] in 'tTwWiI':
                out.append((int(p[0], 16), p[2]))
        if out:
            break
    out.sort()
    addrs = [a for a, _ in out]
    _symcache[path] = (addrs, out)
    return _symcache[path]


def short(path):
    base = path.rsplit('/', 1)[-1]
    for pre in ('libsqlite3', 'libc.so', 'liblz4', 'libcrypto', 'libc-utils',
                'libacl', 'libgcc_s', 'ld-linux'):
        if base.startswith(pre):
            return pre.replace('.so', '')
    return base


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 2
    out = args[0]
    top = 25
    only = None
    folded = None
    i = 1
    while i < len(args):
        if args[i] == '--top':
            top = int(args[i + 1]); i += 2
        elif args[i] == '--seg':
            only = args[i + 1]; i += 2
        elif args[i] == '--folded':
            folded = args[i + 1]; i += 2
        else:
            print('unknown argument ' + args[i]); return 2

    maps, bases = load_maps(out + '/maps.txt')
    los = [m[0] for m in maps]
    segs = {}
    main_bin = None
    for m in maps:
        if m[2] == 0 and 'prof_shim' not in m[3]:
            main_bin = m[3]
            break

    resolved = {}

    def resolve(addr, leaf):
        key = (addr, leaf)
        if key in resolved:
            return resolved[key]
        a = addr if leaf else addr - 1
        k = bisect.bisect_right(los, a) - 1
        if k < 0 or a >= maps[k][1]:
            r = ('?', '0x%x' % addr)
        else:
            name = maps[k][3]
            vma = a - bases.get(name, maps[k][0] - maps[k][2])
            saddrs, syms = symbols(name)
            j = bisect.bisect_right(saddrs, vma) - 1
            fn = syms[j][1] if j >= 0 else '0x%x' % vma
            r = (name, fn)
        resolved[key] = r
        return r

    per_seg = collections.defaultdict(lambda: {
        'n': 0, 'self': collections.Counter(), 'ours': collections.Counter(),
        'site': collections.Counter(),
        'incl': collections.Counter(), 'fold': collections.Counter()})
    header = ''
    with open(out + '/samples.txt') as f:
        for line in f:
            if line.startswith('# seg '):
                _, _, n, label = line.rstrip('\n').split(' ', 3)
                segs[int(n)] = label
                continue
            if line.startswith('#'):
                header = line.strip()
                continue
            parts = line.split()
            seg_s, _, w_s = parts[0].partition(':')
            seg = int(seg_s)
            w = int(w_s) if w_s else 1
            pcs = [int(x, 16) for x in parts[1:]]
            frames = []
            for idx, pc in enumerate(pcs):
                name, fn = resolve(pc, idx == 0)
                frames.append((name, fn))
            # Drop the handler and the signal trampoline: the shim's frames,
            # then one more (libc's __restore_rt, which has no dynamic symbol
            # and would resolve to whatever precedes it).
            cut = 0
            while cut < len(frames) and 'prof_shim' in frames[cut][0]:
                cut += 1
            if cut < len(frames) and frames[cut][1] == '__restore_rt':
                cut += 1
            elif cut > 0:
                cut += 1
            frames = frames[cut:]
            # The interrupted pc is exact, not a return address.
            if cut < len(pcs) and frames:
                frames[0] = resolve(pcs[cut], True)
            if not frames:
                continue
            # Our own shim's wrappers sit between the program and libc.
            frames = [fr for fr in frames if 'prof_shim' not in fr[0]] or frames
            d = per_seg[seg]
            d['n'] += w
            leaf = frames[0]
            d['self']['%s:%s' % (short(leaf[0]), leaf[1])] += w
            ours = next((fr for fr in frames if fr[0] == main_bin), None)
            if ours is not None:
                tag = ours[1]
                if leaf[0] != main_bin:
                    tag += '  [in %s]' % short(leaf[0])
                d['ours'][tag] += w
            else:
                d['ours']['(outside the binary)'] += w
            site = next((fr for fr in frames if fr[0] == main_bin and
                         not fr[1].startswith(GENERIC)), None)
            d['site'][site[1] if site else '(outside the binary)'] += w
            seen = set()
            for fr in frames:
                k = '%s:%s' % (short(fr[0]), fr[1])
                if k not in seen:
                    seen.add(k)
                    d['incl'][k] += w
            d['fold'][';'.join('%s:%s' % (short(fr[0]), fr[1])
                               for fr in reversed(frames))] += w

    print(header)
    fold_out = open(folded, 'w') if folded else None
    for seg in sorted(per_seg):
        label = segs.get(seg, str(seg))
        if only and only != label:
            continue
        d = per_seg[seg]
        n = d['n']
        print('\n=== %s: %d ticks ===' % (label, n))
        for col in ('self', 'ours', 'site', 'incl'):
            print('  -- %s --' % {'self': 'self', 'ours': 'ours (innermost c-backup frame)',
                                  'site': 'site (ours, skipping the c-utils db wrappers)',
                                  'incl': 'inclusive'}[col])
            for k, v in d[col].most_common(top):
                print('  %6d %5.1f%%  %s' % (v, 100.0 * v / n, k))
        if fold_out:
            for k, v in d['fold'].items():
                fold_out.write('%s;%s %d\n' % (label.replace(' ', '_'), k, v))
    if fold_out:
        fold_out.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
