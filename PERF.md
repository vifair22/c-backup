# c-backup Performance Journal

Tracking benchmark results across commits to catch regressions and measure optimizations.

**System:** AMD Ryzen 9 5900XT (16C/32T), GCC 15.2.1, Linux 6.18.18, `-O3 -flto -march=znver3`

---

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
