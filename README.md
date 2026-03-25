# c-backup

A deduplicating filesystem backup tool for Linux, written in C.

---

## What it does

`c-backup` captures incremental, deduplicated snapshots of one or more directory trees. Every file is content-addressed by its SHA-256 hash and stored exactly once across all snapshots. Restores are fully offline and metadata-exact: permissions, ownership, xattrs, ACLs, symlinks, hardlinks, sparse files, and device nodes are all preserved.

**Design constraints:**

- Individual files and total repository size may be arbitrarily large — no limit short of available disk space.
- Any single file may exceed available RAM. All large-object paths stream rather than buffer.
- Repositories with millions of inodes are expected. Change detection is O(n), not O(n²).
- Pre-compressed content (video, audio, database files) is detected at intake and stored verbatim — no CPU wasted re-compressing incompressible data.

---

## Features

| Feature | Detail |
|---------|--------|
| Content deduplication | SHA-256 content addressing; identical data stored once regardless of filename or snapshot |
| Incremental backups | Only changed objects are stored; unchanged files cost one hash comparison |
| LZ4 compression | Per-object compression with 64 KiB probing; skip markers prevent re-probing incompressible content |
| Sparse file support | Holes are preserved using a region table; no sparse-to-dense bloat |
| Metadata fidelity | Stores mode, uid, gid, mtime, xattrs, POSIX ACLs, symlink targets, hardlink relationships, device numbers |
| Pack files | Loose objects are coalesced into pack files with binary-searchable indexes; multi-object packs are capped at 256 MiB, large compressible files get dedicated single-object packs |
| Parity error correction | Reed-Solomon RS(255,239) corrects up to 512-byte burst errors per object; XOR parity repairs single-byte header corruption; CRC-32C provides fast detection; `verify --repair` rewrites corrected files to disk |
| GFS retention | Grandfather-Father-Son calendar tiers: daily / weekly / monthly / yearly |
| Crash safety | All writes go to `tmp/` via `mkstemp`, fsynced, then atomically renamed |
| Export / Import | Native `.cbb` bundles for offline transfer; tar.gz export for interoperability |
| GUI viewer | Python/Tkinter read-only repository browser with analytics, diff, and content preview |

---

## Dependencies

| Library | Purpose |
|---------|---------|
| `liblz4` | Object compression |
| `libssl` / `libcrypto` (OpenSSL) | SHA-256 hashing |
| `libacl` | POSIX ACL backup and restore |
| `libpthread` | Parallel Phase 3 worker pool |

For the Python viewer: `pip install lz4` (optional — structural info is readable without it).

---

## Build

```sh
make          # builds build/backup
make static   # builds build/backup-static (statically linked)
make test     # builds and runs all tests (requires cmocka)
make clean    # removes build/
```

Requires GCC and C11.

---

## Quick start

```sh
# Create a repository
backup init --repo /mnt/backup/myrepo --path /home/alice

# Run a backup
backup run --repo /mnt/backup/myrepo

# List snapshots
backup list --repo /mnt/backup/myrepo

# Restore a snapshot
backup restore --repo /mnt/backup/myrepo --snapshot HEAD --dest /tmp/restore

# Restore a single file
backup restore --repo /mnt/backup/myrepo --snapshot HEAD \
    --path /home/alice/documents/report.pdf --dest /tmp/restore
```

---

## Common operations

```sh
# Manually pack loose objects
backup pack --repo /mnt/backup/myrepo

# Run garbage collection
backup gc --repo /mnt/backup/myrepo

# Prune old snapshots per retention policy
backup prune --repo /mnt/backup/myrepo

# Verify all object hashes
backup verify --repo /mnt/backup/myrepo

# Verify and repair bit-rot via parity
backup verify --repo /mnt/backup/myrepo --repair

# Tag a snapshot
backup tag --repo /mnt/backup/myrepo --snapshot 42 --name release-2025
backup tag --repo /mnt/backup/myrepo --snapshot 42 --name legal-hold --preserve

# Inspect a snapshot's file tree
backup ls --repo /mnt/backup/myrepo --snapshot HEAD

# Export to a portable bundle
backup export --repo /mnt/backup/myrepo --snapshot HEAD --out archive.cbb

# Import a bundle into a new repository
backup import --repo /mnt/backup/newrepo archive.cbb
```

---

## Repository layout

```
<repo>/
├── format                    # Version marker: "c-backup-1"
├── lock                      # Lock file for flock(2)
├── policy.toml               # Retention and backup policy
├── refs/HEAD                 # ID of the most recent snapshot
├── tags/<name>               # One file per named tag
├── snapshots/NNNNNNNN.snap   # Snapshot manifests
├── objects/XX/<62-hex>       # Loose content objects
├── packs/pack-NNNNNNNN.dat   # Pack data files
├── packs/pack-NNNNNNNN.idx   # Pack indexes (sorted, binary-searchable)
├── logs/                     # Structured log output, coalesce state
└── tmp/                      # Crash-safe staging area
```

---

## Retention policy (`policy.toml`)

```toml
path      = ["/home/alice", "/etc"]
exclude   = ["/home/alice/.cache"]

keep_snaps   = 7      # rolling window: keep last N snapshots
keep_daily   = 30     # GFS: daily anchors
keep_weekly  = 12     # GFS: weekly anchors (Sunday)
keep_monthly = 12     # GFS: monthly anchors (1st of month)
keep_yearly  = 5      # GFS: yearly anchors (Jan 1)

auto_pack    = true
auto_gc      = true
auto_prune   = true
verify_after = false
strict_meta  = false  # true = fail on unreadable xattrs/ACLs
```

All policy values can be overridden at runtime with `--` flags on any command that accepts them.

---

## Viewer GUI

A read-only repository browser built with Python and Tkinter.

```sh
cd tools/viewer
python -m viewer /mnt/backup/myrepo
```

**Tabs:** Overview · Snapshots · Objects · Packs · Analytics · Diff · Content · Search · Integrity · Export

The viewer reads repository files directly using pure-Python binary parsers — it does not invoke the `backup` binary and makes no writes to the repository.

---

## Full documentation

See [manual.md](manual.md) for complete technical documentation covering on-disk formats, algorithms, all CLI subcommands, policy reference, performance tuning, and troubleshooting.
