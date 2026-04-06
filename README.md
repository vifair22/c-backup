# c-backup

A deduplicating filesystem backup tool for Linux, written in C.

---

## What it does

`c-backup` captures incremental, deduplicated snapshots of one or more directory trees. Every file is content-addressed by its SHA-256 hash and stored exactly once across all snapshots. Restores are fully offline and metadata-exact: permissions, ownership, xattrs, ACLs, symlinks, hardlinks, sparse files, and device nodes are all preserved.

**Design constraints:**

- Individual files and total repository size may be arbitrarily large — no limit short of available disk space.
- Any single file may exceed available RAM. All large-object paths stream rather than buffer.
- Repositories with millions of inodes are expected. Change detection is O(n), not O(n2).
- Pre-compressed content (video, audio, database files) is detected at intake and stored verbatim — no CPU wasted re-compressing incompressible data.

For usage documentation, CLI reference, and technical internals see [manual.md](manual.md).

---

## Dependencies

### Build toolchain

| Package | Notes |
|---------|-------|
| GCC (C11) | Primary compiler; Clang may work but is not tested |
| GNU Make | Parallel builds via `-j$(nproc)` |
| `liblz4-dev` | LZ4 compression headers and library |
| `libssl-dev` | OpenSSL headers (SHA-256) |
| `libacl1-dev` | POSIX ACL headers |
| `libcmocka-dev` | Unit test framework (test builds only) |
| `libasan` | AddressSanitizer runtime (`make asan` only) |
| `gcovr` | Coverage report generation (`make coverage` only) |

### Runtime libraries

| Library | Purpose |
|---------|---------|
| `liblz4` | Object compression |
| `libssl` / `libcrypto` (OpenSSL) | SHA-256 hashing |
| `libacl` | POSIX ACL backup and restore |
| `libpthread` | Parallel worker pools |

### Viewer GUI (optional)

Python 3 with Tkinter. `pip install lz4` for compressed content preview.

---

## Building

Requires GCC with C11 support and GNU Make. Builds use all available CPU cores automatically.

### Build targets

| Target | Output | Optimisation | Description |
|--------|--------|-------------|-------------|
| `make` | `build/bins/backup` | `-O3` | Release build |
| `make static` | `build/bins/backup-static` | `-O3` | Statically linked release |
| `make debug` | `build/bins/backup-debug` | `-Og -g` | Debug build with full symbols |
| `make asan` | `build/bins/backup-asan` | `-O1 -g -fsanitize=address` | AddressSanitizer build |
| `make clean` | — | — | Remove `build/` |

Each build type compiles into its own subdirectory (`build/release/`, `build/static/`, `build/debug/`, `build/asan/`) so you can switch between them without stale object conflicts. All final binaries are placed in `build/bins/`.

Compiler warnings include `-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion -Wcast-qual -Wnull-dereference -Wmissing-prototypes` and more. Vendor code (`vendor/`) is compiled with relaxed flags.

### Versioning

The binary embeds a version string in the format `SEMVER_YYYYMMDD.HHMM.TYPE`:

```
$ build/bins/backup --version
c-backup 0.1.0_20260406.1944.release
```

- **SEMVER** is read from `release_version` (controlled, bumped for releases).
- **YYYYMMDD.HHMM** is the UTC build timestamp.
- **TYPE** is one of `release`, `static`, `debug`, or `asan`.

---

## Testing

Tests use the [cmocka](https://cmocka.org/) framework.

```sh
make test           # build release + all tests + static analysis, then run
make test-asan      # build asan + all tests under AddressSanitizer, then run
```

Individual test binaries can be run directly:

```sh
build/bins/test_parity
build/bins/test_restore
build/bins/test_pack_gc
```

Test binary names mirror source filenames under `tests/`. There are 33 test suites covering snapshots, objects, packing, GC, coalescing, parity, restore, export/import, CLI options, and fault injection.

The `make test` target also runs static analysis:

| Analyser | What it checks |
|----------|---------------|
| Stack usage | Flags any function exceeding 64 KiB stack frame |
| `gcc -fanalyzer` | GCC's built-in static analyser |
| `cppcheck` | Supplemental static analysis (if installed) |

### Coverage

```sh
make coverage
```

Builds with `--coverage`, runs all tests, and generates an HTML report at `build/coverage/coverage.html` via `gcovr`.

---

## Benchmarks

Two benchmark suites live under `bench/`:

| Suite | Binary | What it measures |
|-------|--------|-----------------|
| Micro | `build/bins/bench_micro` | SHA-256, CRC-32C, LZ4 compress/decompress, RS encode/decode, XOR parity, object store/load round-trips |
| Phases | `build/bins/bench_phases` | End-to-end per-phase timing: scan, backup, pack, verify, GC, restore with synthetic test data |

```sh
make bench            # build and run all benchmarks
make bench-micro      # micro only
make bench-phases     # phases only
```

Both binaries support name filtering and listing:

```sh
build/bins/bench_micro sha256       # run only benchmarks matching "sha256"
build/bins/bench_micro -l           # list available benchmarks
```

Benchmarks link against the full library (everything except `main.o`) and do not require cmocka.

---

## Source layout

```
c-backup/
├── src/                    # Core C source (21 .c, 23 .h)
│   ├── main.c              #   CLI entry point and command dispatch
│   ├── backup.c            #   Backup pipeline (scan, compare, parallel store)
│   ├── restore.c           #   Restore engine (pack-sorted, parallel)
│   ├── snapshot.c          #   Snapshot manifest read/write
│   ├── object.c            #   Object store (loose read/write, FUSE ring, streaming)
│   ├── pack.c              #   Pack system (creation, GC, coalesce, workers)
│   ├── pack_index.c        #   Global pack index (mmap'd, fanout + binary search)
│   ├── parity.c            #   CRC-32C, XOR parity, Reed-Solomon RS(255,239)
│   ├── parity_stream.c     #   Bounded-RAM streaming RS parity accumulator
│   ├── scan.c              #   Filesystem scanner (inode-sorted, exclusions, hardlinks)
│   ├── gc.c                #   Garbage collection (reference collection, loose + pack GC)
│   ├── gfs.c               #   GFS retention engine (tier assignment, prune logic)
│   ├── diff.c              #   Snapshot-to-snapshot diff
│   ├── ls.c                #   In-snapshot directory listing and search
│   ├── xfer.c              #   Bundle export/import, TAR export
│   ├── json_api.c          #   JSON RPC API (single-shot and session modes)
│   ├── repo.c              #   Repository open/close, locking, DAT cache, loose set
│   ├── policy.c            #   policy.toml parser and writer
│   ├── tag.c               #   Tag management
│   ├── stats.c             #   Repository statistics
│   ├── error.c             #   Thread-local error context
│   └── types.h             #   Core type definitions (node_t, object_header_t, etc.)
│
├── tests/                  # cmocka test suites (34 files)
│   ├── test_backup.c       #   End-to-end backup pipeline
│   ├── test_restore.c      #   Restore correctness
│   ├── test_pack_gc.c      #   Pack GC and rewrite
│   ├── test_parity.c       #   Parity encode/decode/repair
│   ├── test_*_fault.c      #   Fault injection tests (malloc/IO failure paths)
│   └── fault_inject.c      #   Shared fault injection harness (--wrap)
│
├── bench/                  # Benchmark suites
│   ├── micro.c             #   Primitive-level throughput benchmarks
│   ├── phases.c            #   End-to-end pipeline phase timing
│   └── fuse_xfer.c         #   FUSE transfer benchmarks
│
├── vendor/                 # Third-party (vendored, relaxed warnings)
│   ├── cJSON.{c,h}         #   JSON parser (MIT)
│   ├── toml.{c,h}          #   TOML parser (MIT)
│   └── log.h               #   Logging macros
│
├── tools/
│   └── viewer/             # Python/Tkinter GUI (read-only repo browser)
│       ├── app.py          #   Main application
│       ├── rpc.py          #   JSON RPC client
│       ├── tabs/           #   Per-tab UI modules
│       └── widgets.py      #   Shared display widgets
│
├── Makefile                # GNU Makefile (parallel, multi-target)
├── release_version         # SemVer string (e.g. "0.1.0")
├── manual.md               # Full technical manual
└── README.md               # This file
```

---

## Releases

Version numbers follow [Semantic Versioning](https://semver.org/). The release version is stored in `release_version` and baked into every binary at build time.

To cut a release:

1. Update `release_version` with the new version (e.g. `0.2.0`).
2. Build all targets (`make`, `make static`, `make debug`).
3. Tag the commit: `git tag v0.2.0`.
4. The resulting binaries embed the full version string (e.g. `0.2.0_20260407.1200.release`).

---

## Documentation

See [manual.md](manual.md) for:

- Complete CLI reference with all flags and examples
- `policy.toml` field reference
- Common configuration recipes
- Troubleshooting guide
- Environment variables
- Storage model, data flow, and algorithm internals
- On-disk file format specifications
- JSON RPC API reference
- Parity error correction details
- On-disk format version history
