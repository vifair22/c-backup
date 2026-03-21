# c-backup Manual

`c-backup` is a deduplicating filesystem backup tool for Linux, written in C.

It stores history as **snapshot manifests** (`.snap` files) that reference immutable content objects. Restores are direct from manifests.

---

## 1) Glossary

- **Restore Point**: a snapshot ID (or tag) you can restore.
- **Manifest**: on-disk snapshot file (`snapshots/XXXXXXXX.snap`) describing paths, metadata, and object hashes.
- **HEAD**: the latest snapshot ID (`refs/HEAD`).
- **Object**: content-addressed blob keyed by SHA-256.
- **Loose Object**: object stored under `objects/aa/<hash-suffix>`.
- **Pack**: compacted object store (`packs/pack-*.dat` + `packs/pack-*.idx`).
- **GFS**: Grandfather-Father-Son calendar retention tiers (daily/weekly/monthly/yearly).
- **Anchor**: snapshot with one or more GFS tier flags.
- **Preserved tag**: a tag created with `--preserve`; pruning will keep that snapshot.

---

## 2) Repository layout

Typical repository tree:

```text
<repo>/
  format
  lock
  policy.conf
  refs/
    HEAD
  tags/
    <name>
  snapshots/
    00000001.snap
    00000002.snap
  objects/
    aa/
      0123...
  packs/
    pack-00000000.dat
    pack-00000000.idx
  logs/
  tmp/
```

Notes:

- `format` stores the repo format marker (`c-backup-1`).
- `lock` is used for writer/read coordination.
- `tmp/` is used for crash-safe temp writes.
- `snapshots/` contains restore-point manifests.

---

## 3) Storage model and restore behavior

### 3.1 Snapshot model

Each successful `backup run` writes one manifest with:

- snapshot metadata (`snap_id`, creation time, flags)
- node table (file/dir/symlink/special metadata)
- dirent tree for path reconstruction

### 3.2 Deduplication

Regular file payloads are hashed (SHA-256) and stored once.

- unchanged file content reuses existing hashes
- changed files add only new object blobs
- metadata-only changes update manifest records without duplicating payload blobs

### 3.3 Restore

Restores are manifest-driven:

- `backup restore` for full restore point
- `backup restore --file <path>` for one file or subtree
- optional `--verify` rehashes restored regular files against object-backed expectations

If a target manifest is missing (pruned or deleted), restoring that snapshot fails.

---

## 4) Retention (GFS + window)

Retention is controlled by:

- rolling window: `keep_snaps`
- calendar tiers: `keep_daily`, `keep_weekly`, `keep_monthly`, `keep_yearly`

GFS tier boundaries are UTC calendar-based:

- **daily**: day boundary crossed
- **weekly**: Sunday daily promoted to weekly
- **monthly**: last Sunday of month promoted to monthly
- **yearly**: December monthly promoted to yearly

`backup prune` (and post-run auto-prune) keeps snapshots if any of these hold:

- it is HEAD
- it is a live (not tier-expired) GFS anchor
- it is inside the `keep_snaps` window
- it has a preserved tag

Then GC runs to remove objects no longer referenced by surviving manifests.

---

## 5) Commands

## 5.1 Initialize repo

```bash
backup init --repo /mnt/backup/repo
```

With inline policy options:

```bash
backup init --repo /mnt/backup/repo \
  --path /home/alice --path /etc \
  --exclude .cache --exclude "*.tmp" \
  --keep-snaps 30 --keep-daily 14 --keep-weekly 8
```

## 5.2 Policy

```bash
backup policy --repo /mnt/backup/repo get
backup policy --repo /mnt/backup/repo set --keep-snaps 60 --auto-prune
backup policy --repo /mnt/backup/repo edit
```

Supported policy flags:

- `--path <p>` (repeatable)
- `--exclude <pattern>` (repeatable)
- `--keep-snaps N`
- `--keep-daily N --keep-weekly N --keep-monthly N --keep-yearly N`
- `--auto-pack/--no-auto-pack`
- `--auto-gc/--no-auto-gc`
- `--auto-prune/--no-auto-prune`
- `--verify-after/--no-verify-after`

## 5.3 Run backup

```bash
backup run --repo /mnt/backup/repo
```

Overrides at runtime:

```bash
backup run --repo /mnt/backup/repo \
  --path /home/alice --exclude "*.cache" \
  --verify-after --verbose
```

Behavior summary:

- loads policy unless `--no-policy`
- scans inputs and writes next snapshot manifest if changes exist
- if retention is configured and `auto_prune=true`, runs GFS prune (+ GC)
- otherwise may run post-run GC when enabled

## 5.4 List snapshots

```bash
backup list --repo /mnt/backup/repo
backup list --repo /mnt/backup/repo --simple
backup list --repo /mnt/backup/repo --json
```

Full table columns:

- `head` (`*` at HEAD)
- `id`
- `timestamp`
- `ent` (node count)
- `manifest` (`Y` if `.snap` present)
- `gfs`
- `tag`

## 5.5 Browse snapshot tree

```bash
backup ls --repo /mnt/backup/repo --snapshot 42
backup ls --repo /mnt/backup/repo --snapshot monthly-2026-03 --path /etc
```

## 5.6 Restore

```bash
backup restore --repo /mnt/backup/repo --dest /restore/out
backup restore --repo /mnt/backup/repo --snapshot 42 --dest /restore/out
backup restore --repo /mnt/backup/repo --snapshot release-q1 --dest /restore/out
backup restore --repo /mnt/backup/repo --snapshot 42 --file etc/hosts --dest /restore/out
backup restore --repo /mnt/backup/repo --snapshot 42 --file home/alice --dest /restore/out
backup restore --repo /mnt/backup/repo --snapshot 42 --dest /restore/out --verify
```

## 5.7 Diff snapshots

```bash
backup diff --repo /mnt/backup/repo --from 41 --to 42
```

Output markers:

- `A` added
- `D` deleted
- `M` content changed
- `m` metadata-only changed

## 5.8 Prune

```bash
backup prune --repo /mnt/backup/repo --keep-snaps 30 --keep-daily 14 --keep-weekly 8
backup prune --repo /mnt/backup/repo --dry-run
```

## 5.9 Maintenance

```bash
backup gc --repo /mnt/backup/repo
backup pack --repo /mnt/backup/repo
backup verify --repo /mnt/backup/repo
backup stats --repo /mnt/backup/repo
backup stats --repo /mnt/backup/repo --json
```

## 5.10 Tags

```bash
backup tag --repo /mnt/backup/repo set --snapshot 42 --name pre-upgrade
backup tag --repo /mnt/backup/repo set --snapshot 42 --name legal-hold --preserve
backup tag --repo /mnt/backup/repo list
backup tag --repo /mnt/backup/repo delete --name pre-upgrade
```

---

## 6) `policy.conf` reference

Stored at `<repo>/policy.conf` as `key = value` lines.

Fields:

- `paths = /src1 /src2 ...`
- `exclude = pattern1 pattern2 ...`
- `keep_snaps = N`
- `keep_daily = N`
- `keep_weekly = N`
- `keep_monthly = N`
- `keep_yearly = N`
- `auto_pack = true|false`
- `auto_gc = true|false`
- `auto_prune = true|false`
- `verify_after = true|false`

Runtime defaults (`policy_init_defaults`):

- `keep_snaps=1`
- `auto_pack=true`
- `auto_gc=true`
- `auto_prune=true`
- `verify_after=false`

---

## 7) Common configurations

### 7.1 Laptop / workstation

```conf
paths = /home/alice /etc
exclude = .cache node_modules *.tmp *.swp
keep_snaps = 30
keep_daily = 14
keep_weekly = 8
keep_monthly = 6
auto_pack = true
auto_gc = true
auto_prune = true
verify_after = false
```

### 7.2 Server with longer history

```conf
paths = /srv /etc /var/lib
exclude = *.tmp
keep_snaps = 14
keep_daily = 30
keep_weekly = 26
keep_monthly = 24
keep_yearly = 5
auto_pack = true
auto_gc = true
auto_prune = true
verify_after = true
```

### 7.3 Conservative prune (keep many local manifests)

```conf
paths = /data
keep_snaps = 180
keep_daily = 0
keep_weekly = 0
keep_monthly = 0
keep_yearly = 0
auto_prune = true
```

### 7.4 Disable automatic retention, run manually

```conf
paths = /data
keep_snaps = 30
keep_daily = 14
auto_prune = false
```

Run on demand:

```bash
backup prune --repo /mnt/backup/repo
```

---

## 8) On-disk format notes (implementation detail)

These are useful for low-level debugging and tooling.

### 8.1 Snapshot file (`.snap`)

Header (`SNAP_MAGIC="CBKP"`, version 2):

- `magic` (u32)
- `version` (u32)
- `snap_id` (u32)
- `created_sec` (u64)
- `node_count` (u32)
- `dirent_count` (u32)
- `dirent_data_len` (u64)
- `gfs_flags` (u32)
- `snap_flags` (u32)

Payload:

- `node_count * node_t`
- raw dirent blob (`dirent_data_len` bytes, sequence of `dirent_rec_t + name bytes`)

### 8.2 Loose object file

`object_header_t` + payload.

Header fields include:

- object `type`
- `compression` (`none`/`lz4`)
- uncompressed/compressed sizes
- SHA-256 hash

### 8.3 Pack files

- `.dat`: `PACK_DAT_MAGIC` (`BPKD`), version, object count, then object entries.
- `.idx`: `PACK_IDX_MAGIC` (`BPKI`), version, sorted hash→offset table.

### 8.4 Tags

Tag files in `tags/` store:

- `snapshot = <id>`
- `preserve = true|false`

---

## 9) Implementation/operational notes

- **Crash-safe writes**: key files are written via temp file + `fsync` + `rename`.
- **Single-writer model**: write commands take an exclusive lock; read commands take shared lock.
- **Prune + GC coupling**: retention removes manifests first, then GC prunes unreachable objects.
- **Path safety in restore**: absolute paths and traversal components are rejected.
- **Metadata replay**: restore applies ownership/mode/xattrs/ACL/timestamps best-effort; non-root restores may warn.
- **Sparse file support**: sparse payload type is preserved and reconstructed on restore.
- **Calendar anchoring in UTC**: GFS tier boundaries use UTC calendar semantics.

---

## 10) Troubleshooting quick checks

- `error: --repo required` → add `--repo <path>`.
- `no source paths specified` → set `policy paths` or pass `--path` in `backup run`.
- restore failed for old ID → manifest likely pruned/missing; check `backup list`.
- unexpected prune result → run `backup prune --dry-run` and verify `keep_*` settings.
- size not shrinking after prune → run `backup gc` (or ensure prune completed fully).
- need immutable keep point → create a preserved tag (`backup tag ... --preserve`).

---

## 11) Safety tips

- Keep at least one of: higher `keep_snaps`, GFS tiers, or preserved tags for legal/critical restore points.
- Test restores regularly to a separate destination.
- Prefer `--dry-run` before changing retention in production.
- Use `verify_after=true` where write-integrity assurance matters more than runtime cost.

---

## 12) Performance tuning knobs

- `CBACKUP_STORE_THREADS=<N>` controls parallel content-store workers in `backup run`.
  - Default: detected CPU count
  - Useful when source data sits on fast storage and hashing/compression is CPU-bound.
- Pack GC includes automatic small-pack coalescing heuristics.
  - Triggered when pack count grows large or small-pack ratio rises.
  - Rewrite budget is capped to avoid long post-backup stalls.
  - A snapshot-gap cooldown avoids coalescing every run.
- For automation or dashboards, prefer `backup list --json` and `backup stats --json`.
