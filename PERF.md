# c-backup Performance Journal

Tracking benchmark results across commits to catch regressions and measure optimizations.

**System:** AMD Ryzen 9 5900XT (16C/32T), GCC 15.2.1, Linux 6.18.18, `-O3 -flto -march=znver3`

---

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
