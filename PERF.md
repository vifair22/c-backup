# c-backup Performance Journal

Tracking benchmark results across commits to catch regressions and measure optimizations.

**System:** AMD Ryzen 9 5900XT (16C/32T), GCC 15.2.1, Linux 6.18.18, `-O3 -flto -march=znver3`

---

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
