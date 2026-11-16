# c-backup Performance Journal

Tracking benchmark results across commits to catch regressions and measure optimizations.

**System:** AMD Ryzen 9 5900XT (16C/32T), GCC 15.2.1, Linux 6.18.18, `-O3 -flto -march=znver3`

---

## 2026-11-16 - NBD reads: pipelining and request size (VM disks)

`make bench-nbd` (`bench/nbd.c`): qemu-nbd 11.1.3 over a 1 GiB image of
incompressible data, raw and qcow2 (the same bytes), read whole by the
native client in 32 MiB calls, page cache warm (the image is read once
first, so this is the protocol and qemu-nbd, not the disk). Best of three,
vfairfield-dt, bench.slice at nice 19, unix socket.

| format | depth | request | MiB/s |
|--------|------:|--------:|------:|
| raw    | 1 | 4096 KiB | 3,603 |
| raw    | 8 | 4096 KiB | 3,857 |
| raw    | 1 | 1024 KiB | 3,565 |
| raw    | 8 | 1024 KiB | 3,905 |
| raw    | 1 |  512 KiB | 3,229 |
| raw    | 8 |  512 KiB | 7,926 |
| raw    | 16 | 512 KiB | 7,717 |
| raw    | 8 |  256 KiB | 8,819 |
| qcow2  | 1 | 4096 KiB | 3,930 |
| qcow2  | 8 | 4096 KiB | 3,818 |
| qcow2  | 1 | 1024 KiB | 4,046 |
| qcow2  | 8 | 1024 KiB | 4,008 |
| qcow2  | 1 |  512 KiB | 3,634 |
| qcow2  | 8 |  512 KiB | 5,356 |
| qcow2  | 16 | 512 KiB | 4,663 |
| qcow2  | 8 |  256 KiB | 8,450 |

Depth alone does nothing with large requests: at 4 MiB, eight in flight
read no faster than one, because qemu-nbd copies each large request
through on its own and the time goes there, not in round trips. Small
requests in flight are what helps: depth 8 at 256 KiB reads 2.3x what one
4 MiB request at a time does (8.8 against 3.6-3.9 GiB/s raw, 8.5 against
3.9 qcow2). So the defaults are `vm.nbd_queue_depth` 8 and
`vm.nbd_request_kb` 256, not the 4096 the design guessed; both are
configurable. The block source asks for 4 MiB windows, so each window is
sixteen requests in flight. At these rates a full read of a 78 GB raw
disk (Noah MC) is bound by the disk and the chunker, not the NBD path.
Measured on a desktop over a unix socket; the online export (QEMU's own
NBD server in a running VM) is to be measured in UAT.

## 2026-11-16 - OWN-44 on v3 after the 1.0.0 merge (c-utils 1.7.1)

After merging origin/v3 e6dd246 (the 1.0.0 merge, V3-4 ports, OWN-45-LIVE,
many-small-files work), same comparison, v6 = origin/v3 e6dd246, five
rounds, `bench_phases -n 0 -l 48 -t 0 -m 16 -R` and `-n 20000 -l 0 -t 0 -R`:

| Shape | v6 first backup | v7 first backup | v6 mean | v7 mean |
|---|---|---|---:|---:|
| large, 768 MiB | 1.226, 1.102, 1.040, 1.048, 1.109 s | 1.153, 0.990, 0.981, 1.033, 1.026 s | 1.105 s | **1.037 s** |
| 20,000 small | 0.560, 0.557, 0.575, 0.590, 0.559 s | 0.554, 0.556, 0.552, 0.553, 0.551 s | 0.568 s | **0.553 s** |

## 2026-11-16 - OWN-44 on v3 with small-file workers and the claim set

After merging origin/v3 b59db42 (small files on workers, OWN-46's claim set
and gc reclaim), the same comparison, v6 = origin/v3 b59db42, five rounds,
`bench_phases -n 0 -l 48 -t 0 -m 16 -R` and `-n 20000 -l 0 -t 0 -R`:

| Shape | v6 first backup | v7 first backup | v6 mean | v7 mean |
|---|---|---|---:|---:|
| large, 768 MiB | 1.131, 1.086, 1.098, 1.061, 1.066 s | 0.994, 1.002, 1.003, 0.978, 0.990 s | 1.088 s | **0.993 s** |
| 20,000 small | 1.708, 1.570, 1.602, 1.544, 1.541 s | 1.555, 1.539, 1.556, 1.519, 1.540 s | 1.593 s | **1.542 s** |

The small-file workers took both builds' small-file backup from about 1.9 s
to about 1.6 s; v7 stays ahead with them, its workers computing each entry's
CRC and AEAD and no parity.

## 2026-11-16 - Pack writers within the buffer budget, one encoder pool (OWN-44)

Review of the encode-as-you-write change found writer memory unbudgeted: a
backup with sixteen file workers could open seventeen writers, about
311 MiB of parity and buffers and 68 encoder threads. Each writer now takes
its encoder and write buffer (26.3 MiB) from `jobs.read_budget_mb` when it
opens, never waiting and never past one object's room left for readers, and
a backup plans its file workers to what fits; the encoder threads are one
process-wide pool of min(4, CPUs). Same benchmark as below, rerun on this
build (bench_phases sets no budget, so every writer encodes; at the default
256 MiB a backup gets up to nine), v6 = origin/v3 d4658fb, five rounds:

| Shape | v6 first backup | v7 first backup | v6 mean | v7 mean |
|---|---|---|---:|---:|
| large, 768 MiB | 1.067, 1.066, 1.047, 1.087, 1.065 s | 0.978, 0.969, 1.039, 0.966, 0.996 s | 1.066 s | **0.990 s** |
| 20,000 small | 1.897, 1.952, 1.923, 1.963, 1.986 s | 1.908, 1.913, 1.947, 1.964, 1.899 s | 1.944 s | **1.926 s** |

One shared pool instead of four threads per writer made the large-file
case faster, not slower: the three packs of that run no longer each start
and join a pool of their own.

## 2026-11-16 - Stripe parity encoded as packs are written (OWN-44)

The first cut of striped parity encoded each pack's stripe at the seal, on
one thread, reading the body back: 3-4% slower than v6 on large files, and
it would have been worse with content-defined chunking feeding 256 MiB packs
from parallel workers. The plaintext writer now hands each 15,296-byte group
to a small pool (up to four threads) as soon as appends complete it, with at
most eight groups of body in flight; the chunk workers compute each entry's
payload CRC with its encryption, and the writer extends it by the 51-byte
header's CRC (`crc32c_combine`, a few hundred operations). The seal encodes
only the last partial group and group 0 (its count is patched) and reads
nothing back. gc's rewrite, coalescing and the stored writer still seal by
reading back; none is on a backup's path.

`bench_phases -n 0 -l 48 -t 0 -m 16 -R` (48 incompressible 16 MiB files,
768 MiB, three packs) and `-n 20000 -l 0 -t 0 -R` (20,000 files averaging
4 KiB), btrfs over LVM on NVMe, v6 = origin/v3 d4658fb (CDC, per-entry
parity on the chunk workers), v7 = this branch, alternating, five rounds:

| Shape | v6 first backup | v7 first backup | v6 mean | v7 mean |
|---|---|---|---:|---:|
| large, 768 MiB | 1.137, 1.100, 1.114, 1.130, 1.123 s | 1.085, 1.056, 1.129, 1.054, 1.079 s | 1.121 s | **1.081 s** |
| 20,000 small | 1.932, 1.966, 2.113, 1.970, 2.138 s | 1.911, 1.995, 1.897, 1.949, 1.946 s | 2.024 s | **1.940 s** |

Parity written: large 52.0 MiB (v6) and 51.4 MiB (v7); small 25.1 MiB (v6)
and 3.8 MiB (v7). The incremental with no change is unchanged within noise
(0.78-0.88 s both). A first measurement with the CRC combine done by
repeated matrix squaring per entry put v7 4% behind v6 on small files;
precomputing the 64 zero-byte operators once made it the 4% ahead above.

## 2026-11-16 - Striped pack parity (OWN-44)

Pack version 7 stripes Reed-Solomon over each pack body as one stream, with
an 8-byte CRC slot per entry, instead of giving every entry its own parity
rounded up to whole groups and a 260-byte header record
(`docs/pack-parity.md`). Parity against data, from
`tests/test_pack_parity_ratio.c` (random content; "data" is the pack body):

| Shape | Objects | v6 parity / data | v7 parity / data | v7 x RS rate |
|---|---:|---:|---:|---:|
| small files, 100 B to 2 KiB | 12,000 | 1.1606 | **0.0746** | 1.12 |
| mixed tree, 64 B to 1 MiB, 1% 4 MiB | 1,000 | 0.0734 | **0.0670** | 1.00 |
| chunks, 512 KiB to 8 MiB | 64 | 0.0674 | **0.0670** | 1.00 |

Four of the small-file case's eight bytes an entry are the header CRC that
lets a walk check every entry header without reading payload (B119); with
a 4-byte slot it was 0.0709.

`bench_phases -n 20000 -l 3 -t 0 -m 16` (20,000 small files averaging 4 KiB
plus three 16 MiB files) on btrfs over LVM on NVMe, base = origin/v3
015fee8, alternating, two rounds:

| Build | First backup | Written | Parity | Incremental | Restore |
|---|---|---:|---:|---|---|
| v6 | 2.237 s, 2.140 s | 130.1 MiB | 28.3 MiB | 0.959 s, 0.913 s | 2.587 s, 2.430 s |
| v7 | 2.122 s, 2.089 s | 108.8 MiB | 7.0 MiB | 0.986 s, 0.942 s | 2.532 s, 2.479 s |

Writing 75% less parity makes the small-file first backup a little faster,
not slower, although the seal now reads each pack back to compute it.
Incremental and restore are unchanged within noise: a clean read touches no
parity in either version.

Large incompressible files (`-n 0 -l 24 -t 0 -m 16 -R`, 384 MiB, two rounds):
v6 0.630 s and 0.633 s, v7 0.651 s and 0.659 s, both writing 25.7 MiB of
parity. The 3-4% is the seal: v6 computed each entry's parity on the pack
writer's worker threads as it went, v7 encodes the whole body at the seal,
on one thread, from the page cache. Moving the stripe encode back onto the
workers (each window of groups is independent) would recover it if it ever
matters; at 600 MiB/s of backup it does not today.
## 2026-11-16 - Many small files: state.db settings, restore batches, one LZ4 pass

Three of the proposals in the profile two entries below, built (`perf/many-small-fixes`):

- **state.db settings** (src/common/state_db.c): `synchronous=NORMAL`,
  `cache_size` 64 MiB and `wal_autocheckpoint` 16,384 pages on the daemon's
  handle, read back after setting, and config keys `db.synchronous`,
  `db.cache_mb` and `db.wal_autocheckpoint`. A thread syncs the WAL once a
  second when it has changed, so NORMAL loses at most about a second of
  edits on a power cut rather than the kernel's 30-second writeback; at
  every start the daemon reindexes, as a job, any repository whose index
  lost snapshots the repository holds (src/meta/index_reconcile.c, Manual
  17.5). bench_phases now opens state.db the way the daemon does, flusher
  included; earlier entries ran on SQLite's defaults.
- **Restore** syncs every 4,096 files or 1 GiB (was 512 / 256 MiB), and
  `make_parents` skips a file whose parent it made last (per restore).
- **One LZ4 pass** for an object no larger than the 64 KiB probe: the
  probe's output is the result.

The numbers below were measured before the small-file workers (next entry)
merged; the two changes are independent (database and restore against the
backup thread's read and hash), and their combined effect is not yet
measured.

The statement cache (proposal 1) is not in this: it is going into c-utils
1.7.0 separately, and these numbers are against c-utils 1.6.0.

`bench_phases -n 100000 -l 0 -t 0`, before = `feat/v3-cdc` 4269bc1 plus the
profiling tools, after = this branch, both linked against c-utils 1.6.0.
Three interleaved rounds, medians, range in brackets; NVMe is `~/.cache`
(btrfs on LVM on LUKS, shared), tmpfs `/dev/shm`, page cache warm, bench.slice
at nice 19.

| | NVMe first | NVMe incr | NVMe restore | tmpfs first | tmpfs incr | tmpfs restore |
|---|---:|---:|---:|---:|---:|---:|
| Before | 12.46 [12.23-12.51] | 5.33 [5.32-5.63] | 12.41 [12.14-13.60] | 8.12 [8.10-8.29] | 2.96 [2.93-2.98] | 5.80 [5.71-6.05] |
| After | 8.51 [8.41-9.81] | 3.37 [3.33-3.47] | 10.39 [10.16-11.09] | 7.27 [7.24-7.54] | 2.80 [2.74-2.87] | 5.53 [5.51-5.53] |
| Change | -32% | -37% | -16% | -10% | -5% | -5% |

/proc/self/io write_bytes on NVMe: first backup 1,485 to 1,071 MiB (3.80x
to 2.74x content), incremental 875 to 520 MiB (2.24x to 1.33x).

### From the shim (`prof_shim.so`, one NVMe run each)

| | Before | After |
|---|---:|---:|
| First backup: fdatasync | 356 (2.74 s) | 31 (0.61 s) |
| First backup: COMMIT and BEGIN (`sqlite3_exec`) | 2.90 s | 1.03 s |
| First backup: state.db pread / pwrite | 729k / 784k | 139k / 303k |
| Incremental: fdatasync | 206 (1.88 s) | 22 (0.44 s) |
| Incremental: state.db pread / pwrite | 513k / 444k | 40k / 234k |
| Restore: syncfs | 196 (9.16 s) | 25 (3.55 s) |
| Restore: mkdir | 1,414,071 (0.49 s) | 15,084 (0.01 s) |
| First backup: LZ4 samples (self) | 706 ms | 325 ms |

A syncfs costs more when it flushes more (142 ms a call against 47 ms), but
far less in total. The fdatasyncs that remain are checkpoints, the flusher's
once-a-second sync, and the pack and snapshot record syncs. The flusher's
cost is in the numbers: it runs in the benchmark as in the daemon.

The profiled runs' totals (13.7 / 6.0 / 17.9 s before, 10.5 / 3.8 / 11.4 s
after) are above the matrix because the shim times every call and samples
the stack; their proportions are what the table uses.

### Where a first backup goes now (NVMe, median run, phase timers)

read+hash+write 2.56 s, pack-seal 1.20, index-nodes 1.18, index-put 1.09,
txn-commit 0.84, cache-put 0.72, cache-lookup 0.48, scan 0.26. The
incremental: index-nodes 1.23 s, cache-lookup 0.96, txn-commit 0.76, scan
0.24. Most of what is left is statement preparation (806,267 prepares in a
first backup, 3.6 s under the shim), which is the c-utils statement cache's
to remove; the profile entry's prototype of both together measured 4.9 s
first and 2.3 s incremental on NVMe.

## 2026-11-16 - Small files on workers (many-small profile, proposal 5)

Small files were stored one at a time on the backup thread: open, read,
SHA-256, the LZ4 probe and compression, parity, then the pack append and
the rows. Files of at most 1 MiB the cache cannot vouch for now go to up to
four workers in batches of 256 files or 4 MiB (`src/ops/small_files.c`),
which read, chunk and prepare them; the backup thread appends their objects
in file order and writes the rows and cache entries, as before.

`feat/v3-cdc` at 4269bc1 plus this change, c-utils v1.6.0 (the pinned
version: no statement cache, no pragmas), `bench_phases -n 100000 -l 0 -t 0
-R` (100,000 files of 2-6 KiB), `-w 1` (serial, as before) against `-w 0`
(automatic: 21 chunk workers on this machine, 4 small-file workers),
alternating, three pairs each, in `bench.slice`. NVMe is `~/.cache` with
`-c` (source evicted, trailing sync timed); tmpfs is `/dev/shm`, warm.

| First backup | `-w 1` | `-w 0` | Change |
|---|---|---|---:|
| tmpfs | 7.99 / 8.04 / 8.10 s | 6.13 / 6.31 / 6.12 s | -24% |
| NVMe, cold | 17.60 / 18.26 / 17.00 s | 10.57 / 11.93 / 10.58 s | -38% |

The read+hash+write row on the backup thread, which is now the append
alone, falls from 2.5 s to 0.93 s on tmpfs and from 7.1 s to 1.3 s on NVMe;
the workers are busy 1.8 s together on tmpfs (5.8 s on NVMe, where they
wait on the disk in parallel instead of the backup thread waiting in
series) and the backup thread waits for them for 0.0-0.1 s in all. What is
left on the backup thread is the database: pack-seal 1.3 s, index-nodes
1.2 s, index-put 1.0 s, cache-put 0.7 s, cache-lookup 0.35 s on tmpfs,
which the statement cache and pragmas (proposals 1 and 2) address.

Two things tried and not kept:

- Asking the index whether an object exists on the workers rather than in
  the append, to take that statement off the backup thread: every worker
  then queued on the one database handle's mutex behind the backup
  thread's own statements, the backup thread's statements slowed by about
  70%, and the first backup went from 6.2 to 6.9 s.
- One small-file worker per chunk worker (21 here): no faster than four,
  since the backup thread is the limit; four it is (`SMALL_POOL_MAX`).

## 2026-11-16 - Where a many-small-files backup goes (format 3, profile)

The many-small preset barely moved between 1.0.0 and format 3 (NVMe 21.7 s
against 21.4 s, entry below), and real trees are mostly small files. This
is the analysis: no product code changed. Two prototypes were built outside
the tree to prove numbers, and are described, not committed.

Branch `feat/v3-cdc` at e93d6be, `bench_phases -n 100000 -l 0 -t 0` (100,000
files of 2-6 KiB, 390.5 MiB, 100 per directory), restore included. NVMe is
`~/.cache` (btrfs on LVM on LUKS, shared with other work); tmpfs is
`/dev/shm`. Every run in `bench.slice` at nice 19, page cache warm (no
`-c`). Medians of three runs, range in brackets.

### Tools

perf, strace, bpftrace and valgrind are not installed on this machine and
perf_event_paranoid is 2, so two harness pieces were added:

- `bench/prof_shim.c` (`build/bins/prof_shim.so`), an LD_PRELOAD shim that
  counts and times libc calls through the PLT (open, fstat, pread, pwrite,
  fdatasync, syncfs, xattr calls, mkdir, rename, fcntl, fwrite and more),
  counts SQLite prepares, steps and resets per statement text with
  `sqlite3_stmt_status` at finalize, and samples the main thread's stack
  on a wall-clock timer (`PROF_CLOCK=cpu` for its CPU clock), weighting a
  sample by the timer overrun so time asleep in fdatasync is not
  undercounted. Counters are reported per step: `bench_phases` now calls
  `prof_mark` (found with dlsym, so nothing changes without the shim).
- `bench/prof_report.py`, which symbolises the samples with nm and prints
  self, innermost-c-backup-frame and inclusive tables per step. The site
  column skips c-utils' `db_execute` wrappers, so a statement's cost lands
  on the function that issued it.
- `BENCH_DB_PRAGMAS` in `bench_common.h`: SQL run once on the fresh
  state.db handle, for trying connection settings the daemon does not
  apply. Unset, the handle is the daemon's.

```
LD_PRELOAD=build/bins/prof_shim.so PROF_OUT=DIR bench_phases -n 100000 -l 0 -t 0
python3 bench/prof_report.py DIR
```

### Time breakdown, as shipped (phase timers, NVMe, median run)

| Phase | First backup | No-change incremental |
|---|---:|---:|
| read+hash+write (open, CDC, SHA-256, LZ4, parity, pack append) | 2.92 s | - |
| txn-commit (WAL write and sync at COMMIT) | 3.20 s | 2.27 s |
| index-put (staged `objects` row per file) | 1.69 s | 0.01 s |
| index-nodes (`nodes` + `node_fts` row per entry) | 1.66 s | 1.96 s |
| pack-seal (mostly `pack_objects` and `objects` rows per object) | 1.64 s | 0 |
| cache-put | 0.78 s | - |
| cache-lookup (two SELECTs per file) | 0.50 s | 0.99 s |
| scan | 0.25 s | 0.24 s |
| trees, snapshot, sweep, other | 0.22 s | 0.23 s |
| **Total** | **12.86 s** | **5.67 s** |

Restore has no phase timers; from the shim on NVMe (13.8 s run): syncfs
196 calls 6.32 s, statement prepares 2.18 s, rename 0.90 s, mkdir 1.41M
calls 0.49 s, fcntl 600k calls 0.21 s.

### Syscalls and statements per file

From the shim, NVMe, per file over 100,000 files:

| | First backup | Incremental | Restore |
|---|---:|---:|---:|
| open / fstat / fstatfs / close | 1 / 1 / 1 / 1 | 0 | 0 / 3 / 0 / 1 |
| lstat, llistxattr, getxattr (scan) | 1, 1, 1 | 1, 1, 1 | 1 (lstat) |
| pread (source and state.db) | 7.3 | 5.1 | 1.8 |
| pwrite (state.db WAL and checkpoint) | 7.8 | 4.4 | 1.0 |
| fwrite (pack, stdio) | 7.1 | 0 | 0 |
| mkdir / rename / utimensat | 0 | 0 | 14.1 / 1 / 1 |
| SQLite prepares | 8.1 | 4.0 | 3.0 |
| SQLite steps | 11.1 | 9.1 | 7.0 |
| fdatasync, whole run | 356 (3.30 s) | 208 (2.36 s) | 196 syncfs (6.32 s) |

The kernel-side work a small file needs is about 5 us (lstat 0.7, listxattr
0.8, getxattr 0.5, open 1.7, fstat 0.3, pread 0.8, close 0.4); the
filesystem calls together are under 0.7 s of a 13 s backup. The time is in
state.db.

### The sinks, with evidence

**1. Every statement is prepared on every call.** c-utils' `db_execute` and
`db_execute_non_query` call `sqlite3_prepare_v2` and finalize each time.
806,267 prepares in a first backup cost 3.4-3.6 s (4.2-4.4 us each),
404,242 in the incremental 1.1 s, 300,001 in the restore 2.0-2.2 s at 6.7
us each, because the restore's location lookup is a JOIN and the JOIN is
the most expensive statement to plan. On tmpfs, prepares are 40% of the
first backup and 34% of the restore. Per statement, whole run, tmpfs
(prepare ms / step ms):

| Statement | Calls | Prepare | Step |
|---|---:|---:|---:|
| `pack_objects JOIN packs` (db_locate: dedup check, restore twice per file) | 402,014 | 3,124 | 991 |
| `INSERT INTO objects ... ON CONFLICT` (staged row, then the seal's upsert) | 202,014 | 1,249 | 1,326 |
| `INSERT INTO nodes` | 202,012 | 1,017 | 796 |
| `SELECT ... FROM file_cache` | 200,000 | 781 | 181 |
| `INSERT INTO file_cache` | 100,000 | 737 | 243 |
| `INSERT INTO node_fts` | 202,012 | 229 | 371 |
| `INSERT OR REPLACE INTO pack_objects` | 101,007 | 212 | 211 |
| `SELECT 1 FROM objects` (cache hit check) | 100,000 | 204 | 152 |

**2. state.db runs on SQLite defaults.** Nothing sets a pragma, so it is
`synchronous=FULL` (a WAL sync per commit), a 2 MB page cache (7 preads and
8 pwrites per file even on tmpfs) and a checkpoint every 1,000 pages: a
first backup writes 1.45 GB through /proc/self/io for 390 MB of content
(3.7x), a no-change incremental 850 MB (2.2x), nearly all of it WAL and
checkpoint. Lowering synchronous alone does little, because the 4 MB
checkpoint threshold still syncs twice per checkpoint: 249 fdatasyncs
remain. Raising the threshold is what removes them (21 left).

**3. Every snapshot re-indexes every entry.** `index_nodes` writes a
`nodes` row and a trigram `node_fts` row per entry whether or not anything
changed, so the no-change incremental spends 4.2 s of 5.7 s (index-nodes
plus its commits) re-recording 101,007 unchanged entries. The manifest
trees already dedup (1,008 of 1,008 reused); the index does not.

**4. Restore syncs the whole filesystem every 512 files.** 196 syncfs at
32 ms each are 46% of an NVMe restore. syncfs is a btrfs transaction
commit for the whole filesystem, so its cost is per call and grows with
whatever else is dirty on the disk.

**5. Small files are stored serially on the backup thread**, and per file
carry two LZ4 passes over the same bytes (the 64 KiB probe covers the
whole of a file under 64 KiB, then the file is compressed again), an RS
parity encode into a malloc'd buffer (3.3 us; OWN-44 is changing parity),
SHA-256 (2.9 us) and 7 stdio writes. read+hash+write is 16-19 us per file
once the database stops dominating.

**6. Smaller items.** `make_parents` mkdirs every component of every
restored file's path (14 per file, all EEXIST, 0.3-0.5 s); restore loads
every node of the snapshot into one `db_execute` result; each object row
is written twice (staged, then at seal); hashes are bound as 64-character
hex text into BLOB columns, doubling every hash index.

### Prototypes

Built from a scratch copy of c-utils v1.6.0 and a scratch restore.c,
linked into separate bench binaries; not committed.

- **Statement cache**: 64 prepared statements per handle, keyed by SQL text
  (FNV hash, then strcmp), `sqlite3_prepare_v3(SQLITE_PREPARE_PERSISTENT)`,
  `sqlite3_reset` after use. The first version also called
  `sqlite3_clear_bindings`, and the restore got *slower* (5.7 to 7.3 s on
  tmpfs): clear_bindings marks a statement expired when its plan depends
  on a bound value, and the locate JOIN's does, so it was re-prepared on
  every one of 402,013 executions (`SQLITE_STMTSTATUS_REPREPARE`). Every
  parameter is rebound before each step, so the clear is unnecessary.
  Without it there are no reprepares.
- **Pragmas** via `BENCH_DB_PRAGMAS`: `synchronous=NORMAL;
  cache_size=-65536; wal_autocheckpoint=16384`.
- **Restore**: syncfs batches of 4,096 files or 1 GiB (from 512 / 256 MiB),
  and `make_parents` skipping a path whose parent it made last.

| Config | NVMe first | NVMe incr | NVMe restore | tmpfs first | tmpfs incr | tmpfs restore |
|---|---:|---:|---:|---:|---:|---:|
| As shipped | 12.62 [12.44-12.77] | 5.39 [5.36-5.53] | 12.74 [12.40-13.05] | 8.21 [8.10-8.26] | 2.93 [2.92-3.08] | 5.68 [5.66-5.79] |
| synchronous + cache_size | 12.08 [11.98-17.75] | 5.51 [4.93-5.84] | 12.52 [12.47-13.02] | 7.59 [7.52-7.84] | 2.86 [2.77-3.00] | 5.83 [5.76-5.92] |
| all three pragmas | 8.73 [8.67-8.82] | 3.39 [3.34-3.49] | 12.02 [11.83-12.50] | 7.54 [7.48-7.55] | 2.75 [2.74-2.75] | 5.66 [5.65-5.73] |
| statement cache | 8.85 [8.75-8.88] | 4.42 [4.27-4.47] | 9.61 [9.05-10.29] | 4.26 [4.21-4.31] | 1.75 [1.73-1.77] | 3.19 [3.17-3.26] |
| statement cache + all three pragmas | 4.87 [4.84-4.89] | 2.27 [2.25-2.44] | 9.02 [8.94-9.23] | 3.87 [3.83-3.91] | 1.64 [1.61-1.64] | 3.16 [3.14-3.16] |
| ... + restore batch and parent cache | 4.83 [4.81-4.84] | 2.25 [2.19-2.25] | 7.21 [6.95-7.25] | 3.83 [3.83-3.83] | 1.60 [1.58-1.61] | 2.79 [2.79-2.82] |

The first two rows come from one interleaved round, the next from a later
one; the shipped binary measured 12.86 / 5.67 / 12.77 s on NVMe in the
first round, so rounds agree within about 3%. Together the cache and the
pragmas take the NVMe first backup from 12.6 to 4.9 s (2.6x), the
incremental from 5.4 to 2.3 s (2.4x), and the restore from 12.7 to 7.2 s
with the restore changes. With them, NVMe write_bytes fall from 1.45 GB to
1.07 GB (first) and from 850 to 520 MB (incremental).

Where the time goes after the cache and the pragmas (NVMe, median run):
first backup read+hash+write 1.78 s, txn-commit 0.89, pack-seal 0.57,
index-nodes 0.52, index-put 0.37, scan 0.25, cache-put 0.21; incremental
txn-commit 0.82, index-nodes 0.59, cache-lookup 0.47, scan 0.24.

### Baselines, same tree, same disk

restic and borg are not installed (not installed for this, by rule).

| Tool | Time |
|---|---:|
| `tar cf` (to one file) | 0.39-0.40 s (+0.11 s sync) |
| `cp -a` | 1.23-1.25 s (+0.10-0.12 s sync) |
| `rsync -a` (fresh) | 2.53-2.68 s (+0.21-0.33 s sync) |
| `rsync -a` (no change) | 0.26-0.28 s |
| `find -printf '%s %T@'` (stat walk) | 0.08-0.09 s |
| `tar xf` (restore-side) | 2.32-2.58 s (+0.13-0.41 s sync) |

### The floor

Per file, measured: about 5 us of system calls (above); CPU of 2.9 us
SHA-256, 3.3 us LZ4 once, 3.3 us parity and about 2 us of CRC and stdio,
roughly 12 us; and with prepared statements about 10 us of SQLite steps
across the rows a file writes today (cache, object, pack_objects, node,
fts). On one thread that is about 27 us a file, 2.7 s for this tree, and
the cache plus pragmas build is at 3.8 s on tmpfs. A deduplicating backup
also has to hash, compress and index, so `tar cf` at 0.4 s is not the
target; `cp -a` at 1.2 s is a fair one for a first backup. The
incremental floor is the stat walk plus a cache lookup per file: rsync
does it in 0.27 s, against 2.25 s here, nearly all of which is
re-indexing unchanged nodes.

### Proposals, by expected gain

| # | Change | Expected gain (this preset) | Risk |
|---|---|---|---|
| 1 | Statement cache in c-utils `db_execute*` (prototype above; no `clear_bindings`) | NVMe first -30%, incr -18%, restore -25%; tmpfs first -48%, incr -40%, restore -44% | Low. Cached statements must be reset on every path (they are, via cleanup); `db_close` finalizes; a DDL change re-prepares on its own. Needs a c-utils release and the version pin |
| 2 | Set `synchronous=NORMAL`, a larger `cache_size` and `wal_autocheckpoint` on the daemon's handle | With 1: NVMe first 8.9 to 4.9 s, incr 4.4 to 2.3 s | Medium: NORMAL in WAL mode can lose the last commits on power loss, never corrupt. Safe here because the repository is the source of truth and the snapshot row lands last; each needs a sentence in Manual.md. The cache is per handle memory (64 MB) |
| 3 | Do not re-index unchanged subtrees: key node rows by tree hash, or fault snapshots in on first browse (`nodes_materialized`) | Incremental 2.25 s to about 0.8 s (index-nodes and its commits are 1.4 s of it); first backup 0.5-1 s | High: schema, browse, search and restore queries, FTS by snapshot. Fault-in does not exist yet |
| 4 | Restore: larger syncfs batches, a parent-directory cache in `make_parents` | NVMe restore 9.0 to 7.2 s, tmpfs 3.16 to 2.79 s | Low; more verified temp files pending at a crash, which the next run already sweeps. The prototype's cache is a static and must become per-restore state |
| 5 | Store small files on workers: read, CDC, hash, compress and seal in parallel, append and index on the backup thread | read+hash+write 1.8 s is the largest remaining first-backup row; across 4 workers it could fall to about 0.5 s | Medium: the pack append order and the batch must stay on one thread; the existing large-file pool is the model |
| 6 | One LZ4 pass for objects under 64 KiB (reuse the probe's output) | About 0.3 s of 0.66 s LZ4 on this tree | Low |
| 7 | Bind hashes as 32-byte blobs, not hex text; write an object row once at seal rather than staged and then upserted | Unmeasured: halves every hash index and removes one statement per new object | Medium to high: a schema-level change to stored values, and staged rows exist for a reason that has to be re-argued |
| 8 | OWN-44 (parity, other branch) | Per entry parity is 3.3 us a file, about 4% of the tmpfs first backup once 1 and 2 are in; a cheaper per-entry parity helps in proportion | As that branch |

1 and 2 together are most of what this preset can gain without design
work, and both are small changes. 3 is where the incremental's remaining
time is.

## 2026-11-16 - Tree building is linear in entries (OWN-43)

`build_one_tree` found a directory's children by scanning every scan entry,
twice (count, then copy), so the trees phase was O(directories x entries).
The ordering pass in front of it had already been made linear; this was the
same shape one step later. `tree_build` (src/ops/tree_build.c) now buckets
every entry under its parent's scan index once, and each tree reads its own
bucket. Tree bytes are unchanged: `tests/test_tree_build.c` pins every tree
name of a synthetic scan to values captured from the old code, and a
no-change incremental reuses the same 25,005 of 25,008 trees before and
after.

`bench_phases -d ~/.cache/own43bench -n 250000 -s 64 -D 10 -l 0 -t 0 -R`
(25,000 directories of 10 files, 25,007 directories in all), btrfs on
NVMe, single run each:

| Build | First backup | trees phase | No-change incremental | trees phase |
|---|---:|---:|---:|---:|
| Before | 49.09 s | 7.6 s | 24.42 s | 5.6 s |
| OWN-43 | 36.71 s | 1.5 s | 16.93 s | 0.3 s |

The first backup's remaining 1.5 s is mostly hashing and staging 25,008 new
trees and their 13 commits (0.55 s of it). The rest of the gap in the totals
is run-to-run noise in the store and index phases, which this change does
not touch.

The tree phase alone, `build_trees` over a synthetic scan with the store
replaced by `object_name` (no I/O), old and new code built the same way.
The old cost quadruples per doubling, the new one doubles:

| Directories x 10 files | Entries | Before | OWN-43 |
|---|---:|---:|---:|
| 3,125 | 34,376 | 0.102 s | 0.013 s |
| 6,250 | 68,751 | 0.344 s | 0.021 s |
| 12,500 | 137,501 | 1.399 s | 0.042 s |
| 25,000 | 275,001 | 5.449 s | 0.084 s |
| 1 x 250,000 | 250,002 | 0.077 s | 0.087 s |

Root tree names were identical between the two builds at every size. One
wide directory was never the problem: the rescan costs entries per
directory, so with one directory it is a single pass either way.
## 2026-11-16 - Format 3: content-defined chunking, first backup

The format 3 writer (docs/repo-format.md, "Format 3") against the 1.0.0 base
it replaces (`origin/fix/infra1-test-scratch`, 7822d17), `bench_phases` on
this machine (Linux 7.3.3, load average about 3 from other work), one run per
cell unless stated, restore skipped (`-R`). tmpfs is `/dev/shm`; NVMe is
`~/.cache` (btrfs on LVM on LUKS), with `-c` (source evicted, trailing sync
timed). Presets: incompressible `-n 0 -l 4 -t 0 -m 512` (4 x 512 MiB
random), compressible `-n 0 -l 0 -t 0 -T 4 -m 512` (4 x 512 MiB distinct
text), mixed (the default: 10,000 small files, 3 x 64 MiB random, 2 x 64 MiB
identical text), many-small `-n 100000 -l 0 -t 0`.

| Preset | Medium | 1.0.0 s | format 3 s | 1.0.0 written / content | format 3 written / content |
|---|---|---:|---:|---:|---:|
| incompressible | tmpfs | 0.410 | 0.429 | 1.067 | 1.068 |
| compressible | tmpfs | 0.760 | 0.396 | 1.076 | 0.004 |
| mixed | tmpfs | 0.800 | 0.830 | 1.062 | 0.682 |
| many-small | tmpfs | 8.084 | 8.419 | 1.010 | 1.010 |
| incompressible | NVMe | 4.430 | 4.129 | 1.067 | 1.068 |
| compressible | NVMe | 2.084 | 0.854 | 1.076 | 0.004 |
| mixed | NVMe | 3.466 | 4.304 | 1.062 | 0.682 |
| many-small | NVMe | 21.683 | 21.448 | 1.011 | 1.011 |

The NVMe disk is shared with other work and swings by 3x between runs of
the same binary (incompressible 1.4 to 4.4 s for 1.0.0 across the session).
Mixed on NVMe, three alternating pairs (1.0.0 / format 3): 2.65 / 2.52,
3.75 / 4.24, 1.60 / 1.60 s; the earlier table cell is inside that spread.
Many-small on tmpfs measured 7.99 / 8.02 s in an earlier round of the same
build.

Written per content byte falls wherever content repeats or compresses:
the compressible preset's generated text repeats at chunk scale, so it
deduplicates almost entirely (9 MiB stored for 2 GiB), and the mixed
preset's two identical text files and compressible chunks bring it to
0.68x. Nothing is written loose and the pack pass writes nothing: 1.0.0
wrote large files loose, uncompressed, and rewrote the compressible ones in
the pack pass (B086).

### What it took to reach the base

The first wired version was far slower; each step below is one change, with
the cell it moved:

| Change | Cell | Before | After |
|---|---|---:|---:|
| Seal (encrypt, CRC, RS parity) on the chunk worker, not under the pack writer's lock | tmpfs incompressible | 1.26 s | (with the next) |
| Several large files at once, each worker cutting into its own pack writer | tmpfs incompressible | 1.26 s | 0.44 s |
| One index row per small file, not two (the content root was recorded twice) | tmpfs many-small | 8.76 s | 8.02 s |
| Writeback started every 8 MiB appended (`sync_file_range`), as the format 2 loose writer did | NVMe incompressible | 4.05 s | 1.60-1.73 s (1.59 s base) |
| A ring of read-ahead slots filled by its own thread, for spans of 16 MiB on a disk | NVMe incompressible | per file 206 MB/s | overlapping |
| Pack commit creates missing object rows; file workers write none (was a WAL sync per file) | NVMe mixed | 6.76 s | 4.30 s |
| No read-ahead on tmpfs or ramfs (a copy for nothing) | tmpfs incompressible | 0.57 s | 0.43 s |

### Per-file CPU ceiling (`bench_micro`)

| | MiB/s |
|---|---:|
| `cdc_scan_16M` (boundaries only) | 4,688 |
| `sha256_oneshot_1M` (one core: format 2's per-file ceiling) | 2,071 |
| `cdc_store_256M_8workers` (one file, chunk SHA-256 and probe) | 2,880 |

One file goes at 1.4 times what one core's SHA-256 allowed; a tree of
several large files at the rate of the file workers together.

## 2026-11-15 - Restore renames verified files in synced batches (OWN-36)

Restore now writes each file to a temporary file and renames it into place
only once its object has verified, and the data has to be on disk before
the name is, or a power failure leaves a right-named file of zeros.

`bench_phases -d ~/.cache/own36bench -n 10000 -l 1 -t 0 -m 64` (10,000
small files averaging 4 KiB plus one 64 MiB file) on btrfs over LVM on
NVMe, restore phase only, two rounds each:

| Build | Restore | Files/s |
|---|---|---|
| Before (writes in place, never syncs) | 0.735 s, 0.735 s | 13,600 |
| Temp file + rename, no sync (not shipped) | 0.879 s, 0.866 s | 11,500 |
| Temp file + `fdatasync` per file + rename | 56.99 s, 53.53 s | 175-187 |
| Temp file + one `syncfs` per 512 files or 256 MiB + rename (shipped) | 1.615 s, 1.601 s | 6,200 |

The temporary file and rename cost about 18%. Syncing per file costs a
btrfs log commit per file regardless of size, 70 times slower; batching
pays about 20 syncs for this tree. Most of the remaining difference from
"before" is writeback the old restore never waited for, which the shipped
build pays inside the timing.

## 2026-11-15 - The pack pass bails out inside an entry (OWN-27)

The pack-pass half of the streaming bail-out proposed below ("Until the
write-once path exists, the same window helps today's pack pass"), built.
`pw_write_large` reads the 64 KiB the probe judged first, then tracks LZ4's
output over the trailing 8 MiB of input (`PACK_BAIL_WINDOW`); at a ratio of
0.90 or more (`PACK_RATIO_THRESHOLD`) it abandons the entry: truncates the
staging pack back to the entry's start, skip-marks the loose object and
leaves it loose. Output is held in the worker's idle 16 MiB read buffer
until it would overflow, so an object abandoned before then writes nothing
to the pack. A new write counter, `bytes_written_pack_abandoned`, counts
what a later abandon truncates.

Holding only until the first window passed was the first version, and
`-H` showed why it was not enough: bench_phases' 1 MiB head is one line
repeated, which LZ4 takes to almost nothing, so the first window holds it
at 7.06 / 8 = 0.88, under the threshold, and the verdict came one read
later with the window already written: 58.6 MiB truncated over 8 files.

Same machine and command as the `-H 8` row below (`-n 0 -l 0 -t 0 -H 8
-m 1024 -c -R`, repository on /home, NVMe), single run:

| Case | Wall | Written / content | /proc write_bytes / content | pack-sweep |
|---|---:|---:|---:|---:|
| `-H 8` head-text, before | 20.97 s | 2.133 | 2.142 | 14.47 s |
| `-H 8` head-text, OWN-27 | **7.36 s** | **1.067** | 1.068 | **0.13 s** |

1.067x is the loose copy and its parity, nothing else: the same as `-l 8`
random. The 0.13 s is LZ4 over each file's first 8 MiB or so and nothing
written; the next backup's sweep skips all eight on the mark (0.1 ms).

## 2026-11-15 - What a first backup writes, by path (B086, reopened)

UAT FS2 (15.24 GB, 1,605 files over NFS; repository and state.db on btrfs
RAID10 of DRAM-less SATA SSDs, `cp` about 80 MB/s): first backup 388 s
serial, 398 s on 8 workers, against 188 s for `cp`. The pool gives nothing,
which says the run is bound by something the workers share, and on that
host that is write bandwidth. The profile also stopped 70-100 s short of
the job: the pack pass ran after it.

### Instrumented

- The pack pass now runs inside `backup_run` (`pack_after`, set by the job
  from `maintenance.auto_pack`), under `pack-sweep` with `sweep:scan+probe`
  and `sweep:pack` beneath it. Profile total equals job wall time.
- The store counts bytes written into the repository by category; the job
  result carries `bytes_written` and its parts (docs/api.md), and the
  backup logs them after the profile.

### Bytes written per source byte, from the code

r(P) is RS parity, 1,024 bytes per 15,296-byte group: 0.0669 P.

| Path | Code | Written |
|---|---|---|
| File <= 16 MiB | `pack_backup_add` -> `pb_compress` (keeps LZ4 only if smaller) -> backup pack, parity in memory, sealed at 256 MiB | 51 + C + 280 + r(C) per object, once |
| File > 16 MiB | `write_object_file_stream`: tmp/obj.*, trailer, `async_writeback`, rename into objects/ | 64 + P + 280 + r(P), always uncompressed |
| ...its parity over the cap | `rs_parity_stream` spill to tmp/, replayed into the trailer | the excess twice; cap 256 MiB serial, 16-256 MiB per pool worker (no spill below about 239 MiB-3.7 GiB files) |
| Duplicate large file | staged in full, found present at `object_exists` or the rename | 64 + P, discarded |
| Pack pass, probe says incompressible | 64 KiB read, skip mark (1-byte pwrite + header parity) | ~0 |
| Pack pass, probe says compressible | `pw_write_large`: re-read P, LZ4F the whole object with no bail-out, parity recomputed over the output, own pack if over 256 MiB, fsync, rename, then the loose copy unlinked | 51 + C + 280 + r(C), and P read again |
| Trees | backup pack | small |
| state.db | WAL | measured below at under 0.1% of content for large files |

The probe reads only the first 64 KiB (threshold: LZ4 ratio < 0.90). A file
whose head compresses and body does not - container tables, metadata or a
thumbnail in front of compressed media - is rewritten whole for nothing.

So, for large files with compression ratio c = C/P:

- incompressible: W = 1.067 P
- probe-compressible: W = 1.067 (1 + c) P, and 2P read
- head-compressible, body not (c ~ 1): W = 2.13 P

For FS2's 15.24 GB, the code predicts 16.3 GB written if every large file
were incompressible (about 203 s at 80 MB/s, 1.08x `cp`), and
1.067 x (1 + c) x 15.24 GB if they compress.

UAT then measured it (serial first backup, uat-ddd87f4f): loose
`bytes_stored` 15.75 GB, repository after the pack pass 7.77 GB (c ~ 0.49),
host-wide writes about 23.5 GB = 15.75 loose + 7.77 packed, and the pack
pass 63 s of 383 s. That is the second row: large compressible files
written uncompressed and loose, then read back and rewritten compressed.
Written once, compressed, they would put about 7.8 GB on disk, less than
`cp` writes. FS2 is the reference; the local runs below only check the
counters and the head-probe case.

### Measured

Machine: the header's, Linux 7.3.3; repository on /home (btrfs on LVM on
LUKS on NVMe), `-c` (source evicted, trailing sync timed), 8 x 1 GiB files,
auto workers (8). Single runs. The 80 MB/s cap is
`systemd-run --user --scope -p "IOWriteBandwidthMax=/dev/mapper/vg0-sys 80M"`,
which the user cgroup honours (dd: 403 MB/s uncapped, 75.5 MB/s capped).

| Case | Disk | Wall | Written / content | /proc write_bytes / content | pack-sweep |
|---|---|---:|---:|---:|---:|
| `-l 8` random | NVMe | 4.60 s | 1.067 | 1.068 | 0.00 s |
| `-l 8` random | slow sync 5 ms | 4.67 s | 1.067 | 1.068 | 0.00 s |
| `-l 8` random | slow sync 20 ms | 5.89 s | 1.067 | 1.068 | 0.00 s |
| `-l 8` random | 80 MB/s cap | 115.3 s | 1.067 | 1.068 | 0.00 s |
| `-H 8` head-text | NVMe | 20.97 s | **2.133** | 2.142 | **14.47 s** |

Random, capped: 9.16 GB in 115.3 s is 79.4 MB/s, so the backup runs at the
cap and its time is its bytes written over the bandwidth; 1.067x is all
parity. Head-text: the pack pass rewrote all 8 GiB to save 0.1%
(8,184.6 MiB of pack payload for 8,192 MiB loose), 69% of the run.

Not run: the head-text case under slow sync and the cap, the `-T`,
default and FS2-shaped mixes, and the `cp` comparisons. The machine was
needed, and FS2's own measurement (above) made them unnecessary. To
repeat: `bench_phases -d DIR ARGS -c -k -R` once, then `-S` on the kept
source under each condition.

### Proposal (not implemented): compressible large files straight into packs

Status: a design for the user's decision. Nothing here is built.

**What changes.** A file over 16 MiB that the probe calls compressible is
stored by its pool worker (or the backup thread, serially) as an LZ4F entry
in a pack, compressed and with parity, instead of as a loose file. A file
the probe calls incompressible is stored loose exactly as now and
skip-marked at write time, so the pack pass has nothing to probe. Sparse
files stay loose. The pack pass stays, for those and for anything an
interrupted run left.

**Bytes.** Per compressible file of P bytes, from 1.067 (1 + c) P written
and 2P read to 1.067 c P written and P read. FS2: from about 23.5 GB to
about 8.3 GB (7.77 GB plus parity), and no 63 s pack pass.

#### Format

No repository format change. Everything the new path writes already exists
on disk today, because the pack pass writes it: LZ4F entries
(`COMPRESS_LZ4_FRAME`) of any size, per-entry parity over the stored bytes,
and single-entry packs larger than the 256 MiB seal (`pw_write_large` gives
any entry that does not fit an open pack its own pack; OWN-19 made every
reader take an oversized pack). The repository a backup produces is the one
the pack pass would have produced, minus the loose stage.

Two limits in the code assume more than the format does, and both bite
today's pack pass already:

- `OBJECT_SIZE_MAX` (128 GiB, pack.c) is enforced on pack entries by
  `write_dat_parity`, coalescing and gc's rewrite, which treat a bigger
  entry as corrupt. Loose objects have no cap. A compressible file whose
  stored size exceeds it must stay loose (or the constant must rise; it is
  a sanity bound, not a format field). Today the pack pass will pack such
  a file and the next gc will call its pack corrupt.
- Nothing else checks an entry against the seal: the seal is a writer-side
  rollover rule (`pack_backup_full`, `rw->body + entry_total`), and
  readers, verify and gc take any size.

#### Write path

1. The worker reads the first pipeline chunk (1 MiB) and runs the existing
   probe (LZ4 over the first 64 KiB, ratio < 0.90 means compressible)
   before writing anything. Incompressible: the loose path, with
   `pack_skip_ver = PROBER_VERSION` in the header it writes. Compressible:
   below.
2. Entry header with placeholder hash and sizes into the worker's pack,
   staged in `tmp/` (`open_new_pack`). Read -> hash -> LZ4F -> encrypt ->
   write -> CRC + parity, as the existing three-stage pipeline, with LZ4F
   in the writer stage. When the stream ends, patch hash, sizes and the
   header parity, as `pw_write_large` already patches `compressed_size`.
3. Placement: one pack writer per worker, sealed at 256 MiB like the pack
   pass's workers; an entry that does not fit an open pack gets its own.
   A pack per object instead would leave every file whose stored size is
   under 64 MiB (`PACK_COALESCE_SMALL_BYTES`) as a coalescing candidate,
   and the next gc would rewrite them: the second write moved from backup
   to gc, not removed.
4. Seal, install, announce: `seal_pack_file` (fsync), rename, then the
   commit callback, exactly as `pw_finalize`. Every worker pack is sealed
   before the snapshot record, as the backup's own pack is now.

**Probe says compressible, tail is not.** The decision is made on 64 KiB,
before the rest exists. Without anything more, LZ4F stores each block that
does not shrink raw, so the worst case is the file stored at about its own
size inside a pack, written once: no worse than loose, and never rewritten.
It still spends LZ4 CPU on every block of a body that will not compress,
against design constraint 4. The streaming bail-out below stops that.

#### Crash safety

- Staging and install: the pack lives in `tmp/` until sealed and fsynced,
  then a single rename installs it, then its rows are committed (the OWN-11
  order: install first, rows second, so an interruption leaves objects
  invisible, never advertised and absent). `install_pack_file` refuses to
  overwrite. Pack numbers come from one atomic counter shared by the
  workers and the backup's own writer, as `repo_pack`'s workers share one.
- A killed run leaves staging files that `store_sweep_tmp_before` removes
  at the next backup (the backup holds the write lock), and staged rows
  that `store_index_drop_staged` removes at its start. A cancelled run
  discards every open worker pack and its staged rows as it now discards
  the backup pack (`pack_backup_abort`).
- OWN-20 (replace packs only once their replacements are installed)
  concerns gc and coalescing; the backup only adds packs, so it does not
  apply, and gc's later rewrites follow it unchanged.
- The snapshot never names an unresolvable object: the backup thread
  records a file only after its worker returns, and the worker returns
  after its entry is staged; every worker pack seals and announces before
  `write_snapshot_record`. Today the worker returns after a rename into
  objects/, which resolves at once, so this is a new ordering rule and the
  one most worth a test: a staged-but-unsealed entry must count as present
  for the existence checks in the same run (as `pack_backup_holds` does
  for the backup's pack), and must not be visible to any other job.
- Commits from workers: the pack commit callback would run on store
  workers while the backup thread may hold a batch transaction across a
  pool wait (B107, OWN-16). The worker's `db_txn_begin` queues,
  `db_txn_waiters()` makes the backup thread yield at its next 50 ms
  pool-wait slice, and the order (pack path's lock, then transaction) is
  the one `db_pack_commit` already takes. Alternatively the workers hand
  sealed staging files to the backup thread to install and announce,
  keeping every commit on one thread; simpler to reason about, at the cost
  of a queue.

#### Dedup

- The hash is known only at the end. Already in the repository (found at
  the end, as `object_exists` finds it now): ftruncate the worker's pack
  back to the entry's start and drop its parity. It costs the file's
  stored bytes written once, as the discarded loose copy costs now
  (`bytes_written_loose_discarded`), but compressed.
- Two workers storing the same object: today the second rename hits
  EEXIST. With per-worker packs both entries could be staged, so the
  check-and-claim must be atomic across workers: a shared set of hashes
  staged this run under a mutex, consulted at the end of each file; the
  loser truncates. Without it a pack pair holds one hash twice, which
  every reader tolerates (first match wins) but gc would count as dead
  weight.
- Avoiding the duplicate write entirely needs the hash before the write,
  which means reading the source twice. Not proposed.

#### Memory

Streaming only; no file is ever held whole. Per worker: the existing
8 x 1 MiB pipeline chunks, an LZ4F context and its 64 KiB-block output
buffer, and the RS parity stream with the pool's cap (16-256 MiB). An
entry's parity cannot be written until the pack's trailer, so it is held
until the seal: for entries that share a pack, at most about 17 MiB for
256 MiB of body; for an oversized entry, the capped stream spilling to
`tmp/` beyond the cap (`bytes_written_parity_spill`), as the loose path
does now. Spill only starts above about 239 MiB of stored bytes at the
16 MiB floor, 3.7 GiB at 256 MiB.

#### Worker pool

The workers do more per byte (LZ4F at roughly 600 MB/s a thread, against
SHA-256 at about 2 GB/s) but write half as much on FS2-like data, and the
pool exists for exactly this: CPU work spread over threads while the disk
is the limit. The pool's memory plan gains the LZ4F state and the
open-pack parity per worker (`store_pool_plan`'s per-worker budget). The
pack pass's single threaded LZ4F at the end of the job disappears for
these files.

#### Effect on the readers

- Restore, grep, FUSE: `pack_object_load_stream` streams LZ4F entries of any
  size now. No change.
- Verify: reads them through the same stream path. No change in what it
  can check.
- **Repair: a regression to fix first.** `object_repair` corrects a loose
  object of any size group by group, reading parity with `pread`.
  `pack_object_repair` only repairs entries up to 16 MiB, and the streaming
  read of an LZ4F entry only detects a CRC mismatch ("not repairable").
  So every large object the pack pass packs today has already lost repair,
  and this design would keep that. Porting `object_repair`'s group loop to
  pack entries (parity at a known offset in the trailer) restores it; RS
  parity covers the stored, compressed bytes, so correcting them in place
  needs no LZ4 at all. Not a format change.
- **Reading parity: a memory bound to fix first.** `read_entry_parity`
  mallocs an entry's whole RS parity, 6.7% of its stored size, on every
  load of a packed object: 3.3 GiB for a 50 GiB entry, against design
  constraint 2. Same exposure today for anything the pass packed; return
  the parity's offset and stream it instead.
- gc: rewrites a pack only when it has dead entries and deletes it when
  all are dead, so a single-entry pack is never rewritten, only kept or
  deleted. The OWN-19 split rolls over between entries, so an oversized
  entry passes through as its own output. Coalescing only takes packs
  under 64 MiB, which is why the placement above shares packs.
- Reindex: rebuilds `pack_objects` from the packs themselves, whatever
  they hold. No change.
- Replicate: moves stored bytes, so it is indifferent to loose or packed.
  But `object_read_stored` mallocs each object's whole stored payload
  (and reencrypt a second copy), loose or packed, which breaks design
  constraint 2 for large files today, independent of this design.

#### Streaming bail-out inside an entry

Design constraint 4 asks for a fast incompressible bailout. The 64 KiB
probe gives it at the head of a file; this gives it for the rest. The
writer tracks what LZ4 achieves as the entry is written, and once
compression has stopped paying it stores the remaining blocks of that same
entry uncompressed. A media file with a compressible header then costs LZ4
time for its first few MiB only, and is written once.

**Already in the frame format, already in today's repositories.** An LZ4
frame is a sequence of blocks, each led by a 4-byte size whose high bit
marks the block as stored uncompressed (LZ4 frame format, block size
field). `LZ4F_compressUpdate` already writes such blocks on its own,
whenever a 64 KiB block does not shrink, so mixed frames are what the pack
pass produces today for a file like the `-H` case above (8,184.6 MiB of
payload for 8,192 MiB of input: mostly raw blocks). Every reader decodes
with `LZ4F_decompress` (`pack_object_load_stream`, the prefix reader at
pack.c ~1060, verify's stream read), which
takes compressed and uncompressed blocks in any order, as the frame
specification requires. The entry stays `COMPRESS_LZ4_FRAME`, its header
fields keep their meaning, and nothing outside the writer changes. No
format change.

What the bail-out adds is not a new kind of block but skipping the attempt:
today every block is compressed and then thrown away if it did not shrink.

**How the writer emits raw blocks.** Two ways, both producing the same
bytes:

- `LZ4F_compressUpdate` with `LZ4F_compressOptions_t.uncompressed = 1`.
  That field exists from lz4 1.11 (installed here). Debian trixie, the
  Docker image's base, ships 1.10, which has only the static-only
  `LZ4F_uncompressedUpdate` (1.9.4+), whose export from the shared library
  is not something to depend on.
- Write the blocks directly: `LZ4F_flush` the context, then for each 64 KiB
  of input emit a little-endian `size | 0x80000000` and the bytes, and
  finish the frame with the 4-byte zero EndMark instead of
  `LZ4F_compressEnd`. This is valid because the writer's frames carry no
  content checksum, no block checksum and no content size (the frame
  preferences are the library defaults, NULL), so the context holds
  nothing the raw blocks would have to update. Portable across every lz4
  version and recommended. The emitted bytes go through the same path as
  compressed output (encrypt in place, write, CRC, RS parity).

**N and the threshold.**

- Ratio per block, 64 KiB (the frame's block size), accumulated over a
  window: switch to raw when the last 8 MiB (128 blocks) compressed to
  0.90 or more of their input, the same `PACK_RATIO_THRESHOLD` the probe
  uses, so "compressible" means one thing everywhere.
- 8 MiB, because a container header or embedded thumbnail is usually well
  under that, so the window sees the body before deciding; and 8 MiB of
  LZ4 is milliseconds, so deciding later saves little. N need not be
  exact: an entry that switched late is merely a little slower to write,
  never larger.
- The window starts after the first 64 KiB probe region, which has
  already been judged.

**Re-probing.** Once raw, every 256 MiB the writer compresses one 64 KiB
block into scratch without emitting it (`LZ4_compress_default`, as the
probe does). Below the threshold, it goes back to compressing. That costs
one block of LZ4 per 256 MiB, a 0.02% duty cycle, and catches a file
whose compressible part follows an incompressible one (a disk image, a tar
of mixed content).

Going back to compressed blocks after raw ones needs care with linked
blocks: a compressed block may reference the previous 64 KiB, which the
context never saw if the writer emitted it by hand. Either write these
frames with independent blocks (`LZ4F_blockIndependent`, chosen per frame
in its descriptor, so readers need nothing; costs LZ4 a little ratio,
typically a few percent at 64 KiB blocks), or make the switch one-way: no
re-probe, and the rest of the entry stays raw. Recommended first step: the
one-way switch, which needs no change to the frame preferences; add
re-probing with independent blocks only if a real tree shows files that
compress after an incompressible stretch.

**Stored size and accounting.** `compressed_size` stays what it is: the
stored bytes, now including 4 bytes per raw 64 KiB block (0.006%) and the
frame header. `uncompressed_size`, the pack row's `comp_size` and
`orig_size`, `bytes_stored` and `bytes_written_pack` all keep their
meaning; a bailed-out entry is simply nearly as large as its input. An
entry can end up larger than its input by those few bytes per block and
the frame overhead: possible today too, harmless, and capped at about
0.01%.

**Parity.** RS parity and the CRC cover the stored bytes, whatever they
are; raw blocks are stored bytes like any other. Nothing changes, and
repair (once pack entries over 16 MiB can be repaired, above) corrects
raw and compressed blocks alike, without decoding.

**Dedup.** The hash is over the plaintext file content, never the stored
form, so it is unaffected. The switch depends only on the content, so two
copies of a file produce identical entries; dedup does not rely on that
in any case, since stored forms already differ between writers (backup
pack, pack pass, replica).

**The probe and the skip mark.**

- The 64 KiB head probe stays the first gate. Incompressible at the head:
  the file goes loose with the skip mark set at write time and no LZ4 at
  all. Compressible at the head: the pack path, with the bail-out
  watching the rest. The probe catches the common case for free; the
  bail-out catches the header-in-front-of-media case the probe misjudges.
- The skip mark exists so that a loose object is not re-probed by every
  pack run. A packed entry needs none: nothing re-probes a pack entry
  (`pb_compress`'s comment), so the bail-out's verdict is durable by being
  in the frame.
- Until the write-once path exists, the same window helps today's pack
  pass differently: there the loose copy already exists, so finishing the
  entry raw would still be a full rewrite for nothing. The pass should
  instead abandon the entry (ftruncate the pack back to the entry's
  start, drop its parity), set the skip mark on the loose object, and
  leave it loose. The mark keeps its meaning, "a prober said leave this
  loose", so `PROBER_VERSION` need not change and existing marks stay
  valid. That alone would have saved the whole 14.5 s and 8 GiB of the
  `-H` run, without a format change.

#### Alternatives considered

- Probe more than the head (several 64 KiB windows): cheaper than the
  streaming bail-out but blind between its samples. The bail-out reads
  every block anyway, as part of writing it, so it sees the whole file
  for the cost of a ratio check. Neither helps FS2, whose data really
  compresses; both matter for media.
- Compressed loose objects, so the pass only renames: every loose reader
  refuses a codec other than `COMPRESS_NONE`. Allowing one is a reader and
  format change, and is not needed given the above.

Recommendation: fix pack-entry repair and the parity read first (both are
bugs in today's repository, not in this design), then build the write path
above behind the existing pool. None of it needs a format change, so it
does not have to land before the freeze; the two fixes arguably should.

## 2026-11-15 - Batches commit only when another writer waits (OWN-16)

UAT measured r8 against pre-r8 on FS2 (1,605 files, 15.24 GB, source over
NFS, state.db and repositories on btrfs RAID10 over LUKS on DRAM-less SATA
SSDs): 335.7 s before, 402.9 s after. Reading, hashing and writing were no
slower; about 60 s per run was in no phase at all, and the trees phase took
16-19 s.

**Unattributed time.** B107 commits the store phase's batch every 200 ms and
before every file of 4 MiB or more, whether or not anybody is waiting for the
lock. That is one WAL sync per such file. r8 predates the store-commit row
(B086 added it), so those commits were "elsewhere": about 1,600 of them at
the host's ~37 ms per sync. The job's progress UPDATEs, issued between
batches, were commits of their own too.

**Trees.** Every new tree recorded its staged row with no transaction open,
so each directory was a commit and a sync. That is the same in pre-r8: here
pre-r8 and post-r8 spend identical time in trees at every sync delay (9.0-10.0
s at 20 ms for 305 directories). UAT's 0.5 s pre-r8 figure is not reproduced
by any code difference between fc511b9 and r8; the likeliest reading is that
those runs reused their trees. Either way the per-tree sync was the cost.

**Fix.** db_txn_waiters() says whether anybody is queued for the lock. A
batch ends at 2,000 entries, or at the next check point after a writer
queues (each entry, each 1 MiB read of a file being stored, each 50 ms slice
of a pool wait, each tree); once a waiter is seen it follows B107's rules for
1 s, so two persistent writers alternate every 200 ms rather than once per
entry. Trees are recorded in a batch. Transaction time is now attributed:
txn-wait, txn-commit (with commit:store/trees/index/pack), progress-cb,
pack-seal, setup and snapshot-record, and "elsewhere" is 0.01 s.

### Method

Machine: the header's (Ryzen 9 5900XT, 32 threads), Linux 7.3.3, GCC 15.
`bench_phases -n 1500 -s 4194304 -D 5 -l 105 -t 0 -m 24 -j`: 1,605 files,
8.3 GiB, 305 directories; 1,500 files of 2-6 MiB (packed, half over the old
4 MiB line) and 105 of 24 MiB (to the pool), with the daemon's job-progress
writes. On the NVMe disk (/home, btrfs on LVM), slow sync through
`build/bins/slow_sync.so` with `SLOW_SYNC_DIRS` set to the working
directory, which holds both state.db and the repository, as on the UAT
host. Builds: pre-r8 = fc511b9, post-r8 = origin/perf/b086-pool (r8 plus the
large-file pool), fixed = this branch. Single runs; where a row was repeated
both are shown.

### First backup, seconds

| sync delay | pre-r8 | post-r8 | fixed |
|---|---:|---:|---:|
| none (real fsync, ~6.6 ms a commit) | 18.84 | 22.50 | 15.61, 16.56 |
| 5 ms | 20.85 | 28.50 | 16.04, 21.52 |
| 20 ms | 28.28, 25.54 | 52.41, 47.13 | 16.58, 18.33 |
| 50 ms | 41.21 | 96.93 | 17.88, 19.78 |

The regression reproduces and grows with sync cost (post-r8 is 2.4x pre-r8 at
50 ms); the fix is flat across delays and faster than pre-r8 at every one,
since pre-r8 still synced once per tree and had no pool. The 21.52 s fixed run
at 5 ms is disk noise: its commits took 0.64 s, and the repeat read 16.04 s.

### Where the time and syncs went (first backup)

| | pre-r8 | post-r8 | fixed |
|---|---:|---:|---:|
| commits (store phase / all) | - | 777-793 store-commit | 10 / 22 |
| syncs delayed by the shim | 362-426 | 1,221-1,460 | 63-65 |
| store-commit / txn-commit at 20 ms | - | 21.1-24.4 s | 0.67-0.83 s |
| store-commit / txn-commit at 50 ms | - | 45.6 s | 1.40-1.58 s |
| trees at 5 / 20 / 50 ms | 3.6 / 9.0-10.0 / 22.9 s | 3.8 / 8.9-10.0 / 23.0 s | 0.01 s |
| txn-wait | - | - | 0.00 s |
| elsewhere (unattributed) | 0.57-0.77 s | 0.44-0.69 s | 0.01 s |

Fixed, the 22 commits are 10 store batches (one before each of nine pack
seals, one at the end), 10 pack commits, one trees batch and one index_nodes
batch. A sync count of 63 is those plus the pack and snapshot record fsyncs.

### No-change incremental, seconds

| sync delay | pre-r8 | post-r8 | fixed |
|---|---:|---:|---:|
| none | 0.105 | 0.112 | 0.103-0.105 |
| 5 ms | 0.157 | 0.154 | 0.145-0.173 |
| 20 ms | 0.263-0.286 | 0.263-0.285 | 0.264-0.285 |
| 50 ms | 0.529 | 0.523 | 0.531-0.614 |

Unchanged: three or four commits either way (store, trees, index, snapshot).

### tmpfs

`-n 1555 -s 1048576 -D 5 -l 50 -t 0 -m 20 -j` on /tmp (2.5 GiB; the restore
step ran out of tmpfs space and is not reported). First backup: pre-r8 4.11 s,
post-r8 3.62 s (63 store commits), fixed 3.60-3.64 s (8 commits). No-change
incremental 0.06 s for all three. On tmpfs a commit is 0.2 ms and none of
this shows, which is how B107 passed review.

### Contention

A writer that queues once a millisecond during a 6,000-file backup
(test_a_persistent_writer_does_not_cost_a_commit_per_file): 4 store commits
in 430 ms. With the contended window disabled the same run made 250. The
stress test (4 writers against a backup) keeps its max write wait at
15-140 ms against the 2 s bound.

## 2026-11-15 - Large files stored on a worker pool (B086, part 2)

UAT's next report after the pipelined writer: 16 GiB of 8 x 2 GiB random
files on a 12-thread Ryzen, source in page cache, repository on tmpfs, took
86.3 s against `cp`'s 20.5 s. The files were stored strictly one after
another at about 203 MB/s each, 1.3 cores busy. A file's SHA-256 is serial,
so the backup now stores several files at once: files over 16 MiB that the
cache cannot vouch for go to a pool of `backup.store_workers` threads (auto:
two thirds of the CPUs, bounded by memory and by the number of such files).
The snapshot and every object are identical to a serial run's.

Machine: the header's (Ryzen 9 5900XT, 32 threads), Linux 7.3.3, GCC 15.
`bench_phases` gained `-w N` (store workers; `-w 1` is the old serial path)
and `-p many-large` (8 x 2 GiB incompressible, nothing else). The machine was
shared with other jobs (load average 15-29 during the disk runs), so disk
numbers are three interleaved rounds and the spread is shown.

### tmpfs (`/dev/shm`), 8 x 384 MiB

The full 8 x 2 GiB preset does not fit: source, repository and restore
together need 48 GiB of a 16 GiB tmpfs. Same file count, smaller files.

| first backup | serial (`-w 1`) | pool (auto, 8 workers) | `cp -r` |
|---|---:|---:|---:|
| time | 1.78 s (1.770, 1.775) | **0.66 s** (0.664, 0.662) | 0.87-0.90 s |
| throughput | 1,733 MiB/s | **4,633 MiB/s** | |
| ratio to `cp` | 2.0x | **0.75x** | |

Each file takes 652 ms on a worker against 221 ms alone: eight writers
share memory bandwidth (every byte is read, hashed, copied, parity-encoded
and written). Wall time is the slowest file, because eight files on eight
workers is one round.

Mixed default preset (10,000 small files, 3 x 64 MiB random, 2 x 64 MiB
text), two rounds: first backup 1.12-1.14 s serial, 0.96-0.99 s on 5
workers. The small-file path is unchanged; the gain is the five large files.

### Real disk (`/home`: btrfs on NVMe), 8 x 2 GiB, `-c`

Source evicted before each backup and a trailing `sync` in every timing.
`cp -r --reflink=never` with the same eviction and sync for reference (plain
`cp` on btrfs reflinks and finishes in 15 ms).

| first backup | serial (`-w 1`) | pool (auto, 8 workers) | `cp -r` |
|---|---:|---:|---:|
| median | 17.25 s | **13.38 s** | 12.64 s |
| rounds | 16.96, 17.25, 17.36 | 13.26, 13.38, 14.14 | 12.60, 12.64, 13.13 |
| ratio to `cp` | 1.36x | **1.06x** | |

Two runs before the interleaved rounds read 12.6 s serial and 14.5 s pooled
at lower load; `-w 2` read 15.1 s and `-w 4` 13.6 s. On this disk the
pool's ceiling is the storage itself: it now runs at `cp`'s speed, and
reading, hashing and writing concurrently is what closed the gap.

### Store-phase commits (r8's 200 ms batch rule)

The new `store-commit` phase counts and times the store phase's cache batch
commits (one WAL sync each), to see whether the 200 ms bound B107 added
makes slow storage commit much more often.

| run | commits | per commit | share of backup |
|---|---:|---:|---:|
| tmpfs, 8 x 384 MiB, serial / pool | 9 / 9 | 21 / 36 us | 0.0% |
| tmpfs, 10k small mixed, serial / pool | 11 / 10 | 0.4-0.6 ms | 0.5-0.7% |
| disk, 8 x 2 GiB, serial | 9 | 117-124 ms | 6.1-6.6% |
| disk, 8 x 2 GiB, pool | 5-8 | 268-472 ms | 12-20% |
| disk, 100k small (`-n 100000 -c`), serial | 78 | 31 ms | 8.2% |
| disk, 100k small, pool (5 workers) | 67 | 31 ms | 8.5% |

Large files commit once per file (the batch is let go before each one) plus
once at the end, whatever the storage. A 100k-file tree commits 67-78 times
where 2,000-entry batches alone would give 50: the time bound adds about a
third more syncs on this disk, at 31 ms each. Under the pool a commit queues
behind eight files' writeback and takes 0.3-0.5 s, but it runs on the backup
thread while the workers carry on, so it is mostly off the critical path
(the backup thread spends most of a pooled run in large-file-wait anyway).

### Residual limits

- **One file is still one core.** SHA-256 over the whole plaintext is the
  object identity; splitting it needs a tree hash, which changes the format
  and every object's identity. Here io:hash runs at about 2.2 GB/s per core
  (7.8 s CPU for 16 GiB serial), so a single 2 GiB file cannot store faster
  than about 1 s on this machine, and a tree of one huge file gains nothing
  from the pool. On UAT's machine the per-file rate was 203 MB/s, so its
  ceiling per file is about ten times lower and the pool's gain is bounded
  by how many large files there are and by its CPU count.
- **The pool only takes files over 16 MiB.** Smaller files go through the
  backup pack on the backup thread, as before.
- **On storage slower than the CPU** the pool can only overlap I/O; at
  `cp`'s speed there is nothing left to win.
- **Memory**: the budget is an eighth of RAM or of the cgroup's memory.max,
  whichever is smaller (256 MiB to 2 GiB, never above a quarter); RS parity
  past each worker's share spills to the repository's tmp/.

## 2026-11-15 - One-snapshot search reads the index newest first

Machine: i7-1265U laptop, `/tmp` on tmpfs. `make bench-browse` defaults
(100k files, 4 snapshots).

A search restricted to one snapshot applied the snapshot filter after the
trigram match, and read matches in index rowid order, which is the order
snapshots were indexed: oldest first. A page from the newest snapshot - the
one people search - walked every older snapshot's matches first, so its cost
grew with retention. The route now reads newest first unless the snapshot is
in the older half of the repository's snapshots. All-snapshot search reads
newest first too, which is also the copy a collapsed result keeps.

| Query | Before | After |
|---|---:|---:|
| search_one_snap_common (newest) | 210,873 us | 364 us |
| search_one_snap_common_oldest | 251 us | 257 us |
| search_one_snap_rare | 188 us | 345 us |
| search_all_snaps_common | 1,624 us | 1,944 us |
| search_all_snaps_rare | 188 us | 338 us |

The rare-term and all-snapshot rows cost about 0.15-0.3 ms more reading
descending; the common case they trade against was 580x slower. A snapshot
reindexed after later ones is written at the high end of the index, so it is
found more slowly by this rule, never wrongly.

## 2026-11-15 - Benchmarks rebuilt on the daemon engine

`make bench` had not built since the daemon rewrite. `BENCH_CFLAGS` kept its
own `-I` list, which lacked `src/meta`, `src/scheduler`, `src/config`,
`src/daemon`, `src/api/routes` and the c-utils headers, and four harnesses
included headers the rewrite removed (`snapshot.h`, `ls.h`). `BENCH_CFLAGS`
now uses `$(SRC_INCLUDES)` and the daemon's warning set, and every harness
compiles clean under it.

| Harness | Fate |
|---------|------|
| `bench_micro` | Unchanged apart from `-Wpedantic` fixes |
| `bench_phases` | Rewritten: first backup, no-change incremental and restore through `backup_run`, `repo_pack` and `restore_run`, with the daemon's own phase profile |
| `bench_browse` | Rewritten: the routes' SQL against the `nodes` table and `node_fts`, on a snapshot made by `backup_run` |
| `bench_browse_sqlite` | Retired |
| `bench_manifest` | Retired |
| `bench_fuse_xfer` | Unchanged; built on demand, not part of `make bench` |

**Why two were retired rather than ported.** `bench_browse_sqlite` raced the
pre-rewrite `.snap` browse path against a prototype SQLite node table, and
`bench_manifest` measured manifest-tree dedup and node-table size before
either existed. Both questions were decided and are now the shipped design:
the pre-rewrite format they compared against no longer exists in this tree,
and the SQLite side of the comparison is what `bench_browse` now measures
directly. Their results stay where they were recorded, in
"2026-08-19 - Browse-path baseline and SQLite evaluation" and
"2026-08-19 - Manifest trees and derived-index sizing" below. The manifest
determinism they checked is asserted by `tests/test_manifest_tree.c` and
`tests/test_backup_incremental.c`, and `bench_phases` prints new and reused
tree counts for every backup.

### Results, default settings

Machine: i7-1265U laptop (2P+8E cores, 12 threads), 16 GB RAM, GCC 16.2.1,
Linux 7.2.15, built with the Makefile default `-O3 -flto -march=znver3`.
Working directories on `/tmp`, which is tmpfs here, so every figure below is
CPU and memory, not storage. Not comparable with the 2026-04-02 baseline: a
different machine, and different harnesses measuring a different engine.

#### `bench_micro`

| Benchmark | Result | Unit |
|-----------|-------:|------|
| lz4_decompress_16M | 19,677 | MiB/s |
| lz4_compress_16M | 15,498 | MiB/s |
| rs_parity_decode_clean_16M | 2,687 | MiB/s |
| rs_parity_encode_16M | 3,709 | MiB/s |
| rs_encode_single | 65.2 | Mops/s |
| sha256_evp_small_256B | 4.6 | Mops/s |
| sha256_evp_stream_16M | 2,191 | MiB/s |
| sha256_oneshot_16M | 2,203 | MiB/s |
| sha256_oneshot_1M | 2,211 | MiB/s |
| sha256_oneshot_64K | 2,202 | MiB/s |
| sha256_oneshot_4K | 2,071 | MiB/s |

The two `crc32c` rows (1.09e9 and 266,307 MiB/s) are omitted: the compiler
hoists the loop-invariant call out of the timing loop, as it did in the
2026-04-02 run, so they measure nothing.

#### `bench_phases`

10,000 small files (mean 4 KiB, half-compressible, all distinct), 3 x 64 MiB
random, 2 x 64 MiB identical text: 10,005 files, 359 MiB.

| Step | Time | Rate |
|------|-----:|-----:|
| first backup | 0.789 s | 12,676 files/s, 455 MiB/s |
| its pack | 0.014 s | 1 object (the text) |
| incremental, nothing changed | 0.198 s | 50,572 files/s |
| restore | 0.509 s | 19,647 files/s, 705 MiB/s |

The daemon's profile of the first backup: read+hash+write 0.44 s (62%, of
which `obj:io-loop` 0.22 s for the five large files), index-nodes 0.10 s,
index-put 0.06 s, cache-put 0.05 s, cache-lookup 0.03 s, scan 0.02 s, trees
0.01 s. The incremental is still half `index_nodes` (0.10 s of 0.20 s)
rewriting every node row for a snapshot in which nothing changed, as recorded
on 2026-08-25.

Options (`-n -s -l -t -m`, or `BENCH_*` variables) size the tree, `-d` puts
it on a real filesystem, and `-c` evicts the source from the page cache
before each backup and includes a trailing `sync` in every timing, which is
the shape the B086 measurements above used.

#### `bench_browse`

100,000 files in directories of 40, a 40-entry probe directory, a
10,000-entry directory, a 24-level chain; 4 snapshots (one backup and three
unchanged). 450,288 node rows, 112,572 per snapshot, `state.db` 226.8 MiB.
Building it took 15.8 s.

| Benchmark | Result | Unit |
|-----------|-------:|------|
| cold_open_first_listing | 226.9 | us/op |
| list_snapshot_root | 21.5 | us/op |
| expand_40child_dir | 51.1 | us/op |
| expand_wide_first_page (200 of 10k) | 166.0 | us/op |
| expand_wide_last_page (offset 9,800) | 376.2 | us/op |
| expand_wide_all_pages (50 pages) | 13,852.7 | us/op |
| resolve_path_deep (29 components) | 111.9 | us/op |
| resolve_path_bulk_file (6 components) | 23.4 | us/op |
| snapshot_switch (resolve+list) | 66.6 | us/op |
| search_one_snap_rare | 204.3 | us/op |
| search_one_snap_common | 230,868.0 | us/op |
| search_one_snap_common_oldest | 274.0 | us/op |
| search_all_snaps_rare | 204.9 | us/op |
| search_all_snaps_common | 1,783.4 | us/op |

Browsing is what the 2026-08-19 evaluation predicted: a 40-entry directory
in 51 us against 107 us under the old format at the same snapshot size, and a
snapshot switch in 67 us against 2,078 us. The old figures came from the
desktop in the header, so if anything the comparison flatters the old code.

**Search scoped to the newest snapshot is the outlier, at 231 ms.** The
snapshot filter sits on the joined `nodes` row, not in `node_fts`, so the
trigram index yields every snapshot's matches in insertion order and the
query rejects the older snapshots' rows before it reaches the current one.
The same search against the oldest snapshot takes 274 us. The cost grows
with matches times retained snapshots, and the newest snapshot is the one a
user searches. Not addressed here; recorded so it is not rediscovered.

`OFFSET` paging costs 2.3x more on the last page of a 10k directory than on
the first, which is acceptable at this size and grows linearly with the
offset.

## 2026-11-15 - Large-file first backup, pipelined (B086)

UAT reported a first backup of 16 GiB (6 x 2 GiB random, 2 x 2 GiB identical
text) on a 4x SATA SSD btrfs pool at 266 s, against 131 s for
`cp -r --reflink=never` on the same pool. Reproduced at 3 GiB scale: 4 x
512 MiB random, 2 x 512 MiB identical text. Different machine from the header:
i7-1265U laptop (2P+8E cores), 16 GB RAM, btrfs on LUKS on NVMe, GCC 16.2.1,
Linux 7.2.15. Source page cache evicted before every run; times include a
trailing `sync`; three interleaved rounds each.

| | before | after | `cp` |
|---|---:|---:|---:|
| repo on disk (btrfs), median | 7.80 s | **5.28 s** | 3.15 s |
| repo on disk, range | 7.41-7.89 s | 4.68-5.40 s | 3.07-3.17 s |
| repo on tmpfs, median | 6.17 s | **4.16 s** | 1.68 s |
| ratio to `cp`, disk | 2.5x | 1.7x | |

The tmpfs run is the diagnosis: with no write I/O at all the old backup was
only 20% faster, so it was CPU-bound on one thread.

### Where the time went

Per-phase (`phase_report`), repo on disk, one representative round:

| | before | after |
|---|---:|---:|
| read+hash+write | 5.93 s | 3.28 s |
| of which obj:io-loop | 5.33 s | 2.66 s |
| of which payload fadvise + writeback start | ~0.6 s (untimed) | 0.58 s |
| auto-pack of the text object (job minus profile) | ~1.4 s | ~1.4 s |
| scan, trees, index | < 0.05 s | < 0.05 s |

The loop read 128 MiB, then hashed it, then wrote it, then fed it to CRC and
RS parity, all on one thread. Each pass over a 128 MiB buffer comes from DRAM,
and SHA-256 (2.07 GB/s here with SHA-NI), RS encode (3.6 GB/s), the write copy
and the read copy add up serially to roughly 600 MB/s.

### What changed

`write_object_file_stream` is now a three-stage pipeline over 1 MiB chunks,
eight in flight: the caller reads (keeping the read-recovery ladder), a hasher
thread runs SHA-256, a writer thread encrypts in place, writes, and feeds CRC
and parity. Each stage sees chunks in file order and only the writer touches
the temp file, so the output is unchanged: backing the same data up with the
old and new binaries gives byte-identical loose objects (sha256 of every file
under `objects/`). Buffer memory per large file falls from 128 MiB to 8 MiB.
Files of 4 MiB or less run the stages inline.

The `POSIX_FADV_DONTNEED` on the finished temp file is now timed inside
`obj:sync`. Its pages are still dirty, so it starts writeback rather than
evicting, and on btrfs that costs 0.6-0.8 s per 3 GiB in the caller.

### Tried and dropped

- Chunk size and depth: 256 KiB x 32, 1 MiB x 8, 1 MiB x 16, 4 MiB x 8 all
  landed within noise (2.65-2.93 s io-loop on tmpfs). Kept 1 MiB x 8.
- Two stages (read+hash, then write): io-loop 3.3 s against 2.5 s for three.
- `sync_file_range(WRITE)` every 8 MiB from the writer: worse (5-7 s io-loop),
  the writer absorbed btrfs's writeback work and became the slow stage.
- Dropping the end-of-object fadvise and `sync_file_range`: no faster end to
  end; the same work came back as dirty-page throttling in the writer.

### Remaining gap

1.7x `cp` on disk, 2.5x on tmpfs. What is left:

- **SHA-256 is serial** and is the slowest stage: 1.55 s of pure hashing for
  3 GiB at this machine's single-core rate, against `cp`'s 1.6 s on tmpfs.
  Parallel hashing would need a tree hash, which is a format change.
- **The auto-pack re-copies compressible large files**: about 1.4 s here for
  one 512 MiB text object (re-read, single-threaded LZ4F at ~600 MB/s, write,
  fsync). Writing compressible large files straight into a pack during backup
  would remove a full write and read of them; it means streaming into the
  backup pack writer with the hash patched in afterwards and a truncate on a
  dedup hit, which is a larger change than this fix.
- **The writeback start** above, about 0.6 s.

On UAT's I/O-bound pool the overlap matters more than the CPU: reads of the
source now proceed while the previous chunk is hashed and written, where
before each 128 MiB read waited for the write of the one before it. Not
measured there yet.

## 2026-08-25 - Objects go straight into packs

The rename that was 41% of a full backup is gone, because the file it renamed
is never created. Same fixture as the attribution run: 60,000 files, 917 MB,
average 14 KB, 21,929 of them distinct, on NVMe-backed xfs.

| | before | after |
|--------------------|---------|---------|
| full backup | 31.07s | **6.33s** |
| read+hash+write | 18.25s | 1.34s |
| per object | 304 us | 22 us |
| inst:rename | 9.70s | - |
| trees | 1.64s | 1.57s |
| index-nodes | 1.15s | 1.25s |
| loose objects left | 21,929 | 0 |

4.9x, and the object write path is 13.6x faster per call. `repo_pack` after the
run reports nothing to do.

Write volume roughly halves as well. The old path wrote every object loose and
uncompressed, then read it back, compressed it and wrote it again: about 358 MB
followed by 311 MB, for 316 MB of logical content. Now it writes 311 MB once,
because compression happens on the only write there is.

Correctness held: a restore of all 60,000 files diffs clean against the source,
and a deep verify returns no findings.

### What it does not help

The incremental case is unchanged at 2.28s, and should be: nothing new is
written, so there was no rename to remove. It is still 67% `index_nodes`
rewriting every node row and FTS row for a snapshot in which nothing changed,
and that remains the next thing worth attacking. Direct-to-pack was the fix for
the full-backup profile, not for that one.

### Where the time goes now

Trees at 28% and index_nodes at 22% are what a full backup is made of once the
object writes stop dominating. Both are database work, and the gap between what
c-utils costs per call and what SQLite costs is a separate finding recorded
above: raw SQLite does an insert in 1.0-2.9 us where the measured phases sit at
5-9 us.

## 2026-08-25 - The whole backup path, attributed

`backup_run` reported one number for "scan+store", which was both the bucket
holding everything interesting and mislabelled - its clock started after the
scan had already finished. `src/common/phase_timer.h` now measures every
component and logs them sorted by cost, at about a tenth of a second of
overhead across 622,000 files, so it stays on in production. The profile that
matters only exists on the deployment.

Fixture: 60,000 files, 917 MB, average 14 KB, three levels deep. 21,929 of them
are distinct objects; the rest deduplicate.

### Two different problems, which one number was hiding

Full backup on NVMe-backed xfs, 31s:

| phase | time | share | per call |
|--------------------|--------|-------|---------|
| read+hash+write | 18.25s | 77.3% | 304 us |
| trees | 1.64s | 7.0% | |
| index-nodes | 1.15s | 4.9% | |
| index-put | 1.09s | 4.6% | 18.2 us |
| cache-put | 0.79s | 3.3% | 13.2 us |
| cache-lookup | 0.54s | 2.3% | 9.0 us |
| scan | 0.12s | 0.5% | |

Incremental, nothing changed, 2.26s:

| phase | time | share |
|--------------|--------|-------|
| index-nodes | 1.50s | 67.0% |
| cache-lookup | 0.55s | 24.4% |
| scan | 0.12s | 5.2% |
| trees | 0.06s | 2.7% |

**91% of an incremental is SQLite**, and two thirds of it is `index_nodes`
rewriting all 60,000 node rows and 60,000 FTS rows for a snapshot in which
nothing changed - while the manifest trees it derives from deduplicate at 3.3%.
The schema already carries `nodes_materialized` and a fault-in path for evicted
snapshots, so the machinery for not doing this eagerly exists.

### The full-backup cost is one syscall

Breaking the 77% bucket apart:

| | time | share | per call |
|-------------------|--------|-------|----------|
| obj:install | 9.86s | 41.7% | 449 us |
| **inst:rename** | 9.70s | 41.1% | **442 us** |
| obj:io-loop | 4.59s | 19.4% | 76.5 us |
| obj:setup | 1.45s | 6.1% | 24.2 us |
| obj:finish | 0.20s | 0.9% | 3.4 us |
| obj:sync | 0.04s | 0.2% | 2.0 us |
| inst:mkdir+open | 0.09s | 0.4% | 4.0 us |
| inst:close | 0.03s | 0.1% | 1.5 us |
| inst:loose-set | 0.00s | 0.0% | 0.1 us |

Renaming each finished temp file into `objects/xx/` is 41% of the run. Reading,
hashing, encrypting and writing the actual bytes is 19%.

`obj:sync` at 2.0 us also settles a standing suspicion: `async_writeback` is
genuinely asynchronous here, so the `sync_file_range` fallback to `fdatasync`
in `object.c` is not firing on this filesystem and is not a cost.

### What was ruled out, and what is still unexplained

A microbenchmark on the same filesystem does create+write+rename in 13 us, and
59 us with the same `sync_file_range` call. Ours costs 442 us for the rename
alone, so this is not simply what xfs charges.

Four explanations were measured and killed: a 256 MiB `malloc` per object in
`rs_parity_stream_init` (free - glibc caches it, 0 us), the `sync_file_range`
fallback (2 us, above), CPU cost (`perf` finds under 5% in `rs_encode_batch32`
and about 10% scattered in SQLite, so the time is blocked rather than spent),
and journal contention with `state.db` sharing the filesystem - moving the
database to tmpfs made rename *worse*, 825 us.

The last of those matters more as variance than as a result: the same operation
measured 442 us and 825 us on consecutive runs of an identical fixture. A
per-object filesystem metadata operation whose cost swings 2x with ambient
system state is not something to tune. It is something to stop doing.

### Where this points

Not at making rename faster. Objects are written loose and uncompressed, then
read back, compressed, written again into a pack, and unlinked - so each one
costs an inode create, a rename, a second full read, an unlink, and double the
write volume. On the Unraid runner tree that was 7.87 GB written loose before
4.48 GB written packed, for 8 GB of source, across 230,595 objects.

Writing into packs during the backup removes the rename entirely rather than
optimising it, compresses once instead of twice, and deletes the 41%. The
incremental path is a separate fix and a cheaper one: stop materialising node
rows eagerly, which the schema is already built for.

## 2026-08-25 - Where a small-file backup actually spends its time

`bench_phases` reports `backup_1k_4K` at 51,033 files/s, 0.0196 ms a file. The
first Unraid pass took 3,552 s for 622,000 files, 5.7 ms a file. A 290x gap
between the benchmark and the machine is worth explaining before optimising
anything, because one of the two is measuring the wrong thing.

It was the machine, and the cause was the test rig rather than the code.

### Same work, only the state database moves

60,875 files, 842 MB, `gitlab-runner/builds`. Source on ZFS cache both times,
repository on ZFS cache both times, same image, caches dropped before each.

| state.db on | wall | ms/file |
|--------------------------------|-----------|--------:|
| `/mnt/disk1` - xfs on `md1p1` | 204,165 ms | 3.35 |
| `/mnt/cache` - ZFS SSD | 64,056 ms | 1.05 |

3.2x, and the two runs produced byte-identical repositories: 35,855 objects,
phys 636,180,560, orig 1,198,057,486, 4 packs, 0 loose. The only difference was
which device the index lived on.

`md1p1` is an Unraid parity-protected array disk, so every write is a
read-modify-write across the data disk and the parity disk on spinning rust.
A backup does at least one `file_cache` SELECT and one INSERT per file, plus a
second SELECT on `objects` for every cache hit - roughly 1.2M queries for
622,000 files. Paying parity RMW on those costs about 2.3 ms a file, which was
68% of that run.

### The floor, for comparison

60,000 files, 917 MB, same shape, on tmpfs:

| Run | wall | ms/file |
|------------------------|--------|--------:|
| first backup | 6.2 s | 0.103 |
| incremental, warm cache | 1.7 s | 0.029 |

So the code path is about 0.1 ms a file and the rest is storage. The benchmark
was not wrong, it was just measuring the path with the index in RAM.

### What this does and does not say

The shipped deploy is already correct: `.gitlab-ci.yml` mounts
`/mnt/user/appdata/c-backupd`, and on this box `/mnt/user/appdata` resolves to
the ZFS cache dataset, not the array. Only the acceptance rig had the index on
`/mnt/disk1`, and the 59-minute figure recorded for the first Unraid pass is an
artifact of that. It would have been roughly three times faster as deployed.

What remains genuinely open is the 1.05 ms against a 0.103 ms floor - a factor
of ten between ZFS and RAM for the same work. That is the number to attack if
small-file throughput matters, and it has not been broken down yet.

## 2026-08-22 - Streaming parity repair

A/B against the commit before the fix (04b16ca), measured with a driver
calling the engine directly. Two questions: does a clean read pay anything
for the repair machinery, and is the repair path actually bounded.

### Clean-path throughput, 2 GiB loose object, page cache warm

| | run 1 | run 2 | run 3 |
|---|---:|---:|---:|
| before | 1736 MB/s | 1740 MB/s | 1783 MB/s |
| after | 1662 MB/s | 1768 MB/s | 1774 MB/s |

Within noise, as intended: detection rides on the SHA-256 that was already
being computed, so an intact object never enters the corrective path.

### Damaged 2 GiB loose object, one flipped byte

| | before | after |
|---|---|---|
| `object_load_stream` | **FAILED** (hash mismatch) | OK, 1 byte corrected, 595 MB/s |
| `object_repair` | fixed=0, peak RSS **2,115 MiB** | fixed=1, peak RSS **18 MiB** |
| streaming repair peak RSS | n/a | 34 MiB (was 158 MiB before parity was read per group) |

Two separate defects showed up here that the unit tests could not:

- `object_repair` read the whole payload with a single `pread`, and Linux
  caps one transfer at 2 GiB - 4 KiB. Objects of 2 GiB or more therefore
  failed the length comparison and were never repaired at all, silently,
  regardless of memory. That is why the before column reads fixed=0.
- The first version of the streaming repair loaded the entire parity block
  (about 1 KiB per 15 KiB of payload: 144 MiB for this object, gigabytes for
  a large one). Reading parity a group at a time brought peak RSS to a
  constant.

### Mixed repository, 10,798 objects, 11 injected single-byte faults

Corpus: a real 39,244-file tree plus three 60 MiB incompressible files
(which stay loose by design) and one large compressible file. Damage spread
across three storage classes: 3 large loose, 3 large packed, 5 small packed.

| Measure | Result |
|---|---|
| Deep verify with repair | 1.6s, 1.55 GB read, 957 MB/s, peak RSS 49 MiB |
| Found and repaired | 11 of 11, zero unrecoverable |
| False positives | 0 of 10,787 clean objects |
| Second verify | 10,798 ok, 0 repaired (corrections persisted) |
| Restore + `diff -r` vs source | 39,244 files, 0 skipped, **zero differences** |

### Restore of unrecoverable damage

Before, a failed object left a file of the right name and the right length
holding corrupt bytes, and the restore reported success with the failure
visible only in a counter: two 80 MiB videos looked restored and were not.
Restore now removes what it wrote when reconstruction fails.

## 2026-08-22 - xattr/ACL persistence cost

The metadata was always collected at scan time and discarded; this measures
what actually storing it costs. A/B against the pre-change HEAD (7586800)
with a one-shot driver calling backup_run/restore_run directly (no daemon,
no HTTP): one cold backup, then unchanged incrementals, median of 3+ runs.
Loose objects only; auto-pack not exercised (identical on both sides).

Trees:

- **real**: ~/git/home/easyraid, 39,240 files / 314 dirs / 1.9G, XFS,
  page cache primed. Zero xattrs, zero non-trivial ACLs: the common case.
- **plain**: synthetic 50,000 files / 507 dirs, tmpfs, no xattrs.
- **shared**: same shape, every file one identical xattr (the SELinux
  pattern). Dedups to a single blob object.
- **unique**: same shape, every file a distinct xattr value plus access +
  default ACLs on every dir. 50,000 distinct blobs: the adversarial case,
  memo never hits.

| Tree | Cold before | Cold after | Incr before | Incr after |
|------|------------:|-----------:|------------:|-----------:|
| real (no xattrs) | 8.36s | 8.37s (+0.2%) | 1.00s | 1.06s (+5%) |
| plain (no xattrs) | 4.82s | 4.77s (0%) | 1.36s | 1.39s (+2%) |
| shared (1 blob) | 4.83s | 4.95s (+2.6%) | 1.42s | 1.51s (+7%) |
| unique (50k blobs) | 4.93s | 7.05s (+43%) | 1.46s | 1.67s (+14%) |

Restore of the unique tree (applying 50k xattr sets + 507 ACL pairs):
3.38s -> 3.86s (+14%); that is the feature doing work, not overhead.

Reading of the numbers:

- The no-xattr overhead (~1.3 us/entry, all of it in the index phase: the
  nodes INSERT grew two NULLIF columns and c-utils re-parses the statement
  per row) is the price everyone pays. On the 223k-file reference tree that
  is ~0.3s on a 14s incremental.
- The shared case adds a SHA-256 of a ~70-byte blob per entry (~2 us); the
  in-run memo keeps it off the resolver, so it stays CPU-bound.
- The unique case is the bound: +43% cold is 50,000 extra loose-object
  writes, one per file. No real filesystem hands every file a distinct
  xattr set; anything shared collapses to the shared row.
- Trivial-ACL filtering is what keeps "real" out of the shared row: without
  acl_equiv_mode every file on the system would carry an ACL blob.

If the flat per-row index cost ever matters, the fix is a prepared-statement
cache in c-utils, not surgery here: it would pay across every insert in the
daemon, not just these two columns.

## 2026-08-19 - Manifest trees and derived-index sizing

Harness: `bench/manifest.c` (`make bench-manifest`). Answers the two questions
the daemonization plan had been settling by reasoning rather than measurement:
does a per-directory content-addressed manifest actually dedup, and how large
does the derived node table get.

### Manifest tree dedup

Two runs, deliberately different scales, to check the shape holds.

| Run | dirs | new trees/snapshot | reused | new bytes/snapshot |
|-----|-----:|-------------------:|-------:|-------------------:|
| 50k files, 6 snaps, 50 changed | 2,582 | 82 (3.2%) | 2,500 | 0.29 MiB |
| 200k files, 10 snaps, 200 changed | 10,316 | 344 (3.3%) | 9,972 | 1.29 MiB |

The first snapshot writes every tree (8.24 MiB / 32.96 MiB); every snapshot
after it writes only the directories on a path from a changed file to the root.
With 50 files changed the tree count changed by 82: roughly 50 leaves, their
~31 parents, and the root. That is O(changed dirs + depth), as designed.

**Determinism passed in both runs.** Rebuilding a snapshot's trees produced a
byte-identical root hash. This is the property the whole scheme rests on; if it
ever regresses, dedup degrades to a full rewrite per snapshot and nothing fails
loudly. `tests/test_manifest_tree.c` must assert it.

Total manifest volume:

| Run | tree objects (unique) | `.snap` today | ratio |
|-----|----------------------:|--------------:|------:|
| 50k x 6 | 9.34 MiB | 27.55 MiB | 2.95x |
| 200k x 10 | 43.10 MiB | 183.80 MiB | 4.26x |

The ratio improves with retention depth because the first snapshot dominates
the tree cost while `.snap` pays full price every time. At 30 snapshots of the
200k tree the projection is ~12x. Note the comparison is conservative: tree
bytes here are uncompressed, `.snap` bytes are already LZ4'd, and real tree
objects go through `store_put_object` and compress too.

Tree assembly cost is 26 ms for 10,316 directories over 210k dirents, which is
noise next to the backup itself.

### Derived node table sizing

| Run | rows | table only | + indexes | + FTS5 | bytes/row |
|-----|-----:|-----------:|----------:|-------:|----------:|
| 50k x 6 | 315,486 | 31.56 MiB | 52.25 MiB | 74.22 MiB | 246.7 |
| 200k x 10 | 2,103,150 | 211.46 MiB | 349.86 MiB | 486.02 MiB | 242.3 |

Bytes/row is stable across a 6.6x change in dataset size, so the extrapolation
is trustworthy:

| Repo shape | rows | fully-indexed DB |
|------------|-----:|-----------------:|
| 1M files x 30 snapshots | 30M | ~6.8 GiB |
| 1M files x 50 snapshots | 50M | ~11.3 GiB |
| 5M files x 30 snapshots | 150M | ~34 GiB |

Insert throughput is 764k-843k rows/s, so indexing a 1M-file snapshot costs
about 1.3 s. Index build over 2.1M rows: 2.21 s. FTS5 build: 4.44 s.

**The DB is roughly 11x the size of the manifest it is derived from** (486 MiB
vs 43 MiB at 200k x 10). That is the price of keeping the node table flat and
denormalised so browsing is an index seek. It is the right trade at small and
medium scale and it needs a mitigation at large scale.

### Consequence: the DB can be a cache, not a full materialisation

Manifest trees make per-snapshot reindex cheap, which creates an option that
did not exist under the old design: **node rows for cold snapshots can be
evicted and rematerialised on demand.** At 764k rows/s, faulting a 1M-file
snapshot back in costs about 1.3 s plus the tree walk, which is acceptable for
"user clicked a two-year-old snapshot."

That caps DB size at (hot snapshots x files) instead of (all snapshots x
files), and it turns the 34 GiB case into something bounded by retention
policy rather than by history. Worth designing the schema so eviction is
possible even if the daemon always materialises everything in v1.

## 2026-08-19 - Browse-path baseline and SQLite evaluation

Commit: `b87c546` (master). Purpose: establish the interactive-browse numbers
the daemon rewrite has to beat before the DB metadata schema is locked in.

Two new harnesses: `bench/browse.c` (`make bench-browse`) records the current
path, `bench/browse_sqlite.c` (`make bench-browse-sqlite`) runs the same
operations head to head against a SQLite node table.

Note: `BENCH_CFLAGS` was missing the per-module include paths added in the
April `src/` reorganisation, so no benchmark in this repo had been buildable
since then. Fixed in the same pass; the 2026-04-02 numbers below predate it.

### Baseline: `bench_browse`

| Benchmark | Result | Unit |
|-----------|-------:|------|
| cold_open_10k | 340.9 | us/op |
| cold_open_100k | 3,318.2 | us/op |
| build_tables_10k | 66.9 | us/op |
| build_tables_100k | 769.3 | us/op |
| expand_40child_dir_in_10k_snap | 10.8 | us/op |
| expand_40child_dir_in_100k_snap | 107.4 | us/op |
| expand_wide_10k_children | 83.6 | us/op |
| expand_deep_path_24_levels | 1,331.9 | us/op |
| snapshot_switch_50k (load+tables+expand) | 2,077.7 | us/op |
| pidx_lookup_100k | 3,380.1 | us/op |
| ls_collect_small_dir_in_100k | 6,142.7 | us/op |
| search_single_snap_100k | 7,464.4 | us/op |
| search_multi_8snaps_50k | 28,704.8 | us/op |

**Directory expansion is O(total dirents), not O(children).** Expanding the
same 40-entry directory costs 10.8 us in a 10k-file snapshot and 107.4 us in a
100k-file snapshot: 10x the snapshot, 10x the cost, same answer. `snap_dir_children`
(`json_api.c:598-606`) linearly scans every dirent record comparing `parent_node`;
the cached hash tables accelerate per-child metadata lookup but not the search
itself. Returning 10,000 children from a smaller snapshot (83.6 us) is cheaper
than returning 40 from a larger one (107.4 us), which confirms the scan
dominates. Extrapolated, a 1M-file snapshot costs roughly 1 ms per expansion
and a 10M-file snapshot roughly 10 ms, per click.

**`snap_pidx_lookup` is not O(log N) in practice** (3.4 ms/op). Per call it
re-opens the `.pidx`, mallocs and reads the entire index, and then calls
`snapshot_load_nodes_only`, which decompresses the whole node table
(`snapshot.c:1423-1454`). Only the bsearch is logarithmic. Independent of the
rewrite, this is worth fixing or caching.

### Head to head: `bench_browse_sqlite` (100k files, 4 snapshots)

442,284 node rows loaded in 1.61 s. Storage: `snapshots/` 38.8 MiB vs SQLite
73.1 MiB (1.9x), rising to 124.6 MiB (3.2x) with a trigram index.

| Operation | current (us) | sqlite (us) | speedup |
|-----------|-------------:|------------:|--------:|
| expand dir (40 children) | 118.0 | 10.4 | 11.4x |
| cold open + first listing | 5,427.2 | 56.8 | 95.6x |
| switch snapshot + list dir | 5,551.7 | 3.3 | 1695.4x |
| expand wide dir, paginated (name in index) | 177.7 | 93.4 | 1.9x |
| search one snapshot (FTS5 trigram) | 9,375.1 | 184.3 | 50.9x |
| expand wide dir, all 10k rows at once | 175.4 | 2,304.4 | 0.1x |
| search one snapshot (LIKE, no index) | 9,164.3 | 24,913.1 | 0.4x |

The last two rows are the naive implementations and both have fixes measured
above them: paginate instead of returning 10k rows, and index names instead of
scanning with LIKE.

### Schema consequences for the daemonization spec

1. **`nodes_by_snap_parent` needs `name` appended.** As specced,
   `(repo_id, snap_id, parent_node_id)` cannot serve `ORDER BY name`, so SQLite
   materialises and sorts every child before `LIMIT` applies: 1,650 us for the
   first 500 of 10k. With `(repo_id, snap_id, parent_node_id, name)` the scan
   stops after the first page: 93 us, a 17.7x difference. Every file browser
   lists directories in name order, so this is the normal path.
2. **Search needs a real index.** `LIKE '%x%'` cannot use a B-tree and
   full-scans 442k rows, losing to the current code by 2.7x. An FTS5 trigram
   table wins by 50.9x and costs about 51 MiB per 442k rows.
3. **Bulk row marshalling is the one genuine regression.** Returning 10k rows
   individually loses to a linear scan of a packed in-memory array by 13x. The
   API should paginate everywhere and never expose a full-snapshot dump.
4. **Budget 2-3x the `.snap` footprint** for the metadata DB.

## 2026-04-02 — Baseline (post HDD I/O optimizations)

Commit: `34c60fe` (master)
Optimizations in place: global pack index, pack sharding, dynamic DAT cache, async writeback, inode-sorted scan, pack-worker hash sort, restore pack-ordered reads, verify pack-ordered reads, parity read consolidation.

### Micro (`bench_micro`)

| Benchmark | Result | Unit |
|-----------|-------:|------|
| lz4_decompress_16M | 23,824 | MiB/s |
| lz4_compress_16M | 19,527 | MiB/s |
| rs_parity_decode_clean_16M | 2,622 | MiB/s |
| rs_parity_encode_16M | 3,598 | MiB/s |
| rs_encode_single | 30.0 | Mops/s |
| crc32c_16M | 481,722,091 | MiB/s |
| crc32c_4K | 117,422 | MiB/s |
| sha256_evp_small_256B | 2.2 | Mops/s |
| sha256_evp_stream_16M | 1,545 | MiB/s |
| sha256_oneshot_16M | 1,545 | MiB/s |
| sha256_oneshot_1M | 1,532 | MiB/s |
| sha256_oneshot_64K | 1,533 | MiB/s |
| sha256_oneshot_4K | 1,369 | MiB/s |

### Phases (`bench_phases`)

#### Pack Index (new)

| Benchmark | Result | Unit |
|-----------|-------:|------|
| pack_index_lookup | 78.8 | Mlookups/s |
| pack_index_rebuild (1k objs) | 10,602 | rebuilds/s |
| pack_resolve_location | 17.8 | Mresolves/s |

#### Backup

| Benchmark | Result | Unit |
|-----------|-------:|------|
| backup_1k_4K (full) | 199 | MiB/s |
| backup_1k_4K (full) | 51,033 | files/s |
| backup_1k_4K (incr, 10 changed) | 276,053 | entries/s |

#### Store (loose objects)

| Benchmark | Result | Unit |
|-----------|-------:|------|
| store_file_16M_loose | 572 | MiB/s |
| store_1k_4K_loose | 132 | MiB/s |
| store_1k_4K_loose | 33,752 | objects/s |

#### Object Load (parity, new)

| Benchmark | Result | Unit |
|-----------|-------:|------|
| object_load_loose_parity_500 | 685 | MiB/s |
| object_load_loose_parity_500 | 175,460 | objects/s |

#### Scan

| Benchmark | Result | Unit |
|-----------|-------:|------|
| scan_10k_64B | 357,603 | entries/s |
| scan_1k_64B | 324,985 | entries/s |

#### Pack

| Benchmark | Result | Unit |
|-----------|-------:|------|
| pack_1k_4K | 101,359 / 395 | objects/s / MiB/s |
| pack_200_64K | 36,495 / 2,247 | objects/s / MiB/s |
| pack_8_32M | 27 / 622 | objects/s / MiB/s |
| pack_mixed (500×4K + 4×32M) | 2,289 / 587 | objects/s / MiB/s |

#### Restore

| Benchmark | Result | Unit |
|-----------|-------:|------|
| restore_1k_4K_packed | 132 | MiB/s |
| restore_1k_4K_packed | 33,805 | files/s |
| restore_500_4K_packed | 121 | MiB/s |
| restore_500_4K_packed | 31,081 | files/s |

#### Verify

| Benchmark | Result | Unit |
|-----------|-------:|------|
| verify_5snap_1k_packed | 80,419 / 313 | objects/s / MiB/s |
| verify_500_4K_packed | 86,253 / 335 | objects/s / MiB/s |

#### GC

| Benchmark | Result | Unit |
|-----------|-------:|------|
| gc_500_4K | 533,762 | objects/s |

#### Snapshot

| Benchmark | Result | Unit |
|-----------|-------:|------|
| snap_load_1k_files | 15,112 | loads/s |
| snap_load_10k_files | 1,873 | loads/s |
| snap_load_nodes_only_10k | 1,896 | loads/s |
| snap_list_all (10 snaps) | 2,414 | calls/s |

#### Pathmap

| Benchmark | Result | Unit |
|-----------|-------:|------|
| pathmap_build_1k | 3,527 / 3.5M | builds/s / entries/s |
| pathmap_build_10k | 392 / 3.9M | builds/s / entries/s |

#### Diff

| Benchmark | Result | Unit |
|-----------|-------:|------|
| diff_collect_1k (50 changes) | 1,092 | calls/s |
| diff_collect_10k (200 changes) | 128 | calls/s |

#### Search

| Benchmark | Result | Unit |
|-----------|-------:|------|
| search_1k (substring) | 6,929 | calls/s |
| search_10k (substring) | 933 | calls/s |
| search_multi (5×1k snaps) | 1,384 | full-scans/s |

#### Ls

| Benchmark | Result | Unit |
|-----------|-------:|------|
| ls_collect_1k (recursive) | 3,393 | calls/s |
| ls_collect_10k (recursive) | 415 | calls/s |
| ls_collect_10k (non-recursive) | 1,117 | calls/s |
| ls_collect_10k (subdir non-rec) | 1,091 | calls/s |
