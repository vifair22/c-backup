# c-backup Performance Journal

Tracking benchmark results across commits to catch regressions and measure optimizations.

**System:** AMD Ryzen 9 5900XT (16C/32T), GCC 15.2.1, Linux 6.18.18, `-O3 -flto -march=znver3`

---

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
