# c-backup Manual

A reverse-incremental filesystem backup tool for Linux, written in C.

---

## Table of Contents

1. [Concepts](#concepts)
2. [Repository Layout](#repository-layout)
3. [Commands](#commands)
4. [Policy](#policy)
5. [Workflows](#workflows)
6. [Snapshot Numbering](#snapshot-numbering)
7. [Storage Management](#storage-management)
8. [What Gets Backed Up](#what-gets-backed-up)
9. [Caveats and Limitations](#caveats-and-limitations)

---

## Concepts

### Object store

File contents and metadata blobs (xattrs, ACLs) are stored as content-addressed objects, each identified by its SHA-256 hash and compressed with LZ4. Identical content is stored only once regardless of how many files reference it or how many snapshots contain it.

### How the reverse-incremental model works

Every `backup run` produces two things:

1. **A snapshot** — a complete manifest of the current filesystem state: every file path, its metadata, and a hash pointing to its content in the object store. The snapshot is always a full description; it references the object store directly and requires no reconstruction to read.

2. **A reverse record** — a description of what the *previous* snapshot's files looked like for every path that changed. This is the "undo" recipe: applying it to the current snapshot reconstructs the state one step back in time.

After three daily backups the repository looks like this:

```
Snapshot 1  (full)       ← day 1 state, .snap file present
  ↑ reverse record 2     ← "to get back to day 1 from day 2, restore these old hashes"
Snapshot 2  (full)       ← day 2 state, .snap file present
  ↑ reverse record 3     ← "to get back to day 2 from day 3, restore these old hashes"
Snapshot 3  (full, HEAD) ← day 3 state, .snap file present
```

**Restoring HEAD** (latest) is always direct: load the snapshot manifest, fetch the objects. No chain walking.

**Restoring an older snapshot whose .snap file still exists** is also direct: load that snapshot manifest, fetch its objects.

**Restoring a snapshot whose .snap file has been pruned** requires `--at`: the tool finds the nearest surviving .snap file ahead of the target, then walks the reverse records backward step by step until the target state is reconstructed in memory, then restores from that reconstructed state.

The cost of a `--at` restore is proportional to the number of reverse records that must be walked. If survivors are spaced 7 snapshots apart, worst case is 6 steps. If they are spaced 365 apart (two yearly survivors with nothing in between), worst case is 364 steps.

Reverse records are **never deleted by prune** — they are required to reconstruct any historical state. Only .snap manifest files are deleted. The GC retains every object referenced by any surviving snapshot or any reverse record.

### GFS retention

GFS (Grandfather-Father-Son) retention automatically selects one snapshot per calendar period to serve as a permanent anchor point. After each `backup run` the engine detects which tier boundaries were crossed and marks the appropriate snapshot with a GFS flag stored in its `.snap` file. GFS-flagged snapshots are never deleted.

**Calendar boundaries (all UTC)**

| Tier | Triggers when |
|------|---------------|
| Daily | A new calendar day has started since the previous backup |
| Weekly | The new day is a Sunday |
| Monthly | The new Sunday is the last Sunday of its calendar month |
| Yearly | The month is December |

Each tier is a promotion of the one below: a Sunday that is the last Sunday of December receives all four flags simultaneously.

When a boundary fires, the GFS engine selects the most recent snapshot from the *previous* day (the snapshot that represents the "complete" state of that day) and writes its GFS flags into that snapshot's `.snap` file.

**`keep_revs` — rolling retention window**

`keep_revs N` keeps the N most recent snapshots regardless of GFS status. Snapshots outside this window that carry no GFS flag are deleted. The effective window is silently extended when needed so that it always reaches back to the oldest GFS-anchored snapshot — this guarantees that `restore --at` always has a valid reverse chain to walk.

**Reverse record deletion**

Reverse records are deleted only when **both** conditions are true:
1. The record is older than the oldest GFS anchor; and
2. The record is outside the `keep_revs` window.

This ensures that every GFS-anchored snapshot remains restorable directly from its `.snap` file and that the reverse chain between anchors is intact for `restore --at`.

**`backup list` output with GFS flags**

GFS-anchored snapshots are shown with their tier in `backup list`:
```
snapshot 00000003  2024-03-17 00:00:31  [daily]         315 entries
snapshot 00000007  2024-03-24 00:00:18  [daily+weekly]  318 entries
```

**Activation**

The GFS engine runs automatically after `backup run` when `policy.auto_prune = true` and `keep_revs` or a GFS-related retention field (`keep_weekly`, `keep_monthly`, `keep_yearly`) is set. Pass `--no-gfs` to skip the GFS engine for a single run. Run the GFS engine manually with `backup prune --gfs`.

### Checkpoints

A checkpoint is a synthetic .snap file written for a snapshot that was previously pruned (or never had one). It short-circuits the reverse chain: once a checkpoint exists at snapshot N, a `--at` restore that would otherwise walk all the way back to HEAD only needs to walk back to N.

`checkpoint --every N` creates checkpoints at every Nth snapshot ID (e.g. `--every 30` creates snapshots 30, 60, 90, …). With this in place, the maximum chain walk for any `--at` restore is N−1 steps regardless of how many snapshots have been pruned.

### Policy

A file stored in the repository (`policy.conf`) that captures source paths, exclusion patterns, retention rules, and automation flags. `backup run` reads it automatically so you do not need to repeat flags on every invocation.

### Tags

Human-readable names that point to a snapshot ID. Tags can be used anywhere a snapshot number is accepted. A tag may be marked `preserve`, which prevents `prune` from deleting that snapshot.

---

## Repository Layout

```
repo/
  format.json          version marker
  lock                 advisory exclusive lock (held during write operations)
  policy.conf          retention policy and source paths (optional)
  refs/
    HEAD               snapshot ID of the current (latest) snapshot
  objects/
    xx/yyyyyy...       loose content-addressed objects (2-char prefix dirs)
  packs/
    pack-00000000.dat  packed object data
    pack-00000000.idx  sorted index of hashes and offsets into the .dat
    pack-00000001.dat
    pack-00000001.idx
  snapshots/
    00000001.snap      full snapshot manifests
    00000002.snap
    ...
  reverse/
    00000002.rev       reverse records (one per snapshot after the first)
    00000003.rev
    ...
  tags/
    release-v1.0       tag files (each contains the snapshot ID and preserve flag)
    before-migration
  tmp/                 crash-safe staging area (temp files, auto-cleaned)
```

Snapshot files for old snapshots are deleted by `prune`. Their reverse records are **not** deleted — they are required to reconstruct historical states. The GC retains any object referenced by a surviving snapshot or a surviving reverse record.

---

## Commands

All commands require `--repo <path>`. Commands that write to the repository acquire an exclusive lock; if another writer holds the lock the command exits immediately with an error.

---

### `backup init --repo <path> [policy options]`

Create a new, empty repository at `<path>`.

```
backup init --repo /mnt/backup/myrepo
backup init --repo /mnt/backup/myrepo --path /home/alice --keep-last 30 --keep-monthly 12
```

`<path>` must not already be an initialised repository. The directory is created if it does not exist. Any policy options provided are written to `policy.conf` immediately.

---

### `backup policy --repo <path> get`
### `backup policy --repo <path> set [policy options]`
### `backup policy --repo <path> edit`

Manage the stored policy.

```
backup policy --repo /mnt/backup/myrepo get
backup policy --repo /mnt/backup/myrepo set --path /home/alice --keep-last 14 --keep-monthly 12
backup policy --repo /mnt/backup/myrepo edit
```

`get` prints the current policy. `set` merges the supplied options into the existing policy (unspecified fields are unchanged). `edit` opens the policy file in `$EDITOR`; if `$EDITOR` is unset, prints the file path for manual editing.

See [Policy](#policy) for the full list of policy options.

---

### `backup run --repo <path> [options]`

Back up source paths into the repository, creating a new snapshot.

```
backup run --repo /mnt/backup/myrepo
backup run --repo /mnt/backup/myrepo --path /home/alice --path /etc
backup run --repo /mnt/backup/myrepo --path /home/alice --exclude "*.log" --exclude ".git"
backup run --repo /mnt/backup/myrepo --no-prune --no-gc --quiet
```

| Option | Meaning |
|--------|---------|
| `--path <p>` | Source path to back up. Repeatable. Overrides policy paths entirely when specified. |
| `--exclude <pat>` | Basename glob pattern to exclude. Repeatable. Overrides policy excludes when specified. |
| `--no-pack` | Skip auto-packing loose objects after the backup. |
| `--no-prune` | Skip post-backup prune even if policy has `auto_prune = true`. |
| `--no-gc` | Skip post-backup GC. |
| `--no-checkpoint` | Skip post-backup checkpoint even if policy has `auto_checkpoint = true`. |
| `--no-policy` | Ignore `policy.conf` entirely; all options must be supplied on the command line. |
| `--quiet` | Suppress progress output. |
| `--verify-after` | After committing the snapshot, verify every referenced object exists in the store. Overrides policy if policy has `verify_after = false`. |
| `--no-verify-after` | Suppress verification even if policy has `verify_after = true`. |
| `--no-gfs` | Skip the GFS retention engine even if policy would activate it. |

Source paths are stored under their **basename** in the snapshot. For example, backing up `/home/alice` stores it as `alice/` in the snapshot tree. Multiple paths are stored as siblings.

The backup is incremental: only files that have changed since the previous snapshot are re-read and re-hashed. If nothing has changed, no new snapshot is created.

After a successful backup, `run` automatically:
1. Packs loose objects (unless `--no-pack` or `policy.auto_pack = false`)
2. Synthesises checkpoints at the configured interval (if `policy.auto_checkpoint = true`)
3. Runs the GFS engine — detects calendar boundaries, flags GFS anchors, prunes non-anchor snapshots outside the `keep_revs` window, and runs GC (if `policy.auto_prune = true` and `keep_revs` or a GFS tier is set; skip with `--no-gfs`)
4. Legacy prune (if `policy.auto_prune = true` and `keep_last` is set but no GFS fields are active)
5. Runs GC (if `policy.auto_gc = true` and neither prune path ran it)

**Exclusion patterns** match against the **basename** of each filesystem entry using `fnmatch(3)` shell-glob syntax. When a directory matches, its entire subtree is skipped.

---

### `backup list --repo <path>`

List all snapshots in the repository.

```
backup list --repo /mnt/backup/myrepo
```

Output format:
```
snapshot 00000001  2024-03-15 09:42:11  312 entries
snapshot 00000002  2024-03-15 14:07:33  315 entries
snapshot 00000003  2024-03-17 00:00:18  [daily]         315 entries
snapshot 00000004  2024-03-24 00:00:05  [daily+weekly]  318 entries
```

GFS-anchored snapshots display their tier flags in brackets. Pruned snapshots (whose `.snap` file has been deleted) are shown as `[pruned]`.

---

### `backup ls --repo <path> --snapshot <id|tag> [--path <p>]`

Browse the contents of a directory within a snapshot, similar to `ls -l`.

```
backup ls --repo /mnt/backup/myrepo --snapshot 3
backup ls --repo /mnt/backup/myrepo --snapshot 3 --path alice/documents
backup ls --repo /mnt/backup/myrepo --snapshot release-v1.0 --path alice
```

`--snapshot` may be a numeric ID or a tag name. `--path` is relative to the snapshot root (omit leading slashes). Omitting `--path` lists the root.

Output format:
```
snapshot 3  /alice/documents
-rw-------  1000  1000     4096  2024-03-15 14:07  report.pdf
-rw-------  1000  1000     8192  2024-03-14 11:22  notes.txt
drwx------  1000  1000        0  2024-03-10 09:00  archive
```

Columns: permissions, uid, gid, size (bytes), mtime, name. Symlinks show `name -> target`. Character/block devices show major,minor instead of size.

---

### `backup restore --repo <path> --dest <path> [options]`

Restore files from a snapshot to `<dest>`.

```
# Restore the latest snapshot
backup restore --repo /mnt/backup/myrepo --dest /tmp/recovery

# Restore a specific snapshot
backup restore --repo /mnt/backup/myrepo --snapshot 5 --dest /tmp/recovery

# Restore a historical (possibly pruned) snapshot via the reverse chain
backup restore --repo /mnt/backup/myrepo --snapshot 1 --at --dest /tmp/recovery

# Restore a single file from a snapshot
backup restore --repo /mnt/backup/myrepo --snapshot 5 --file alice/documents/report.pdf --dest /tmp

# Restore a directory subtree from a snapshot
backup restore --repo /mnt/backup/myrepo --snapshot 5 --file alice/documents --dest /tmp/recovery

# Restore with post-restore verification
backup restore --repo /mnt/backup/myrepo --dest /tmp/recovery --verify
```

| Option | Meaning |
|--------|---------|
| `--snapshot <id\|tag>` | Snapshot to restore. Defaults to latest (HEAD). |
| `--at` | Reconstruct via the reverse chain. Required for pruned snapshots. Must be used with `--snapshot`. |
| `--file <path>` | Restore only a single file or directory subtree (repo-relative path as shown by `ls`). Requires `--snapshot`. |
| `--verify` | After restore, re-read every regular file and verify its content matches the stored hash. |
| `--quiet` | Suppress progress output. |

`--dest` is created if it does not exist. Source paths are restored as subdirectories of `--dest` (e.g. a backup of `/home/alice` restores to `<dest>/alice/`).

When `--at` is used, the tool finds the **nearest existing snapshot** at or after the target and walks backward only as far as necessary. Synthesised checkpoints (see `checkpoint`) reduce this walk significantly.

---

### `backup diff --repo <path> --from <id|tag> --to <id|tag>`

Show what changed between two snapshots.

```
backup diff --repo /mnt/backup/myrepo --from 3 --to 7
backup diff --repo /mnt/backup/myrepo --from before-migration --to after-migration
```

Either argument may be a numeric snapshot ID or a tag name. Works for pruned snapshots by reconstructing the working set via the reverse chain.

Output: one line per changed path.

```
A  alice/new-file.txt        added in --to snapshot
D  alice/deleted.txt         deleted in --to snapshot
M  alice/documents/notes.txt content changed
m  alice/config.ini          metadata only (mode/owner/mtime changed)
```

If there are no differences, prints `(no differences)`.

---

### `backup prune --repo <path> [options]`

Delete old snapshot files according to a retention policy, then run GC.

```
# GFS engine (recommended for regular scheduled use)
backup prune --repo /mnt/backup/myrepo --gfs

# Legacy sliding-window prune
backup prune --repo /mnt/backup/myrepo --keep-last 7 --keep-weekly 8 --keep-monthly 12 --keep-yearly 5
backup prune --repo /mnt/backup/myrepo --keep-last 5 --dry-run
backup prune --repo /mnt/backup/myrepo   # uses policy.conf
```

| Flag | Meaning |
|------|---------|
| `--gfs` | Run the GFS engine: detect calendar anchors, flag them, prune non-anchor snapshots outside `keep_revs`, delete eligible rev records, run GC. Requires `keep_revs` or a GFS tier to be set in policy. |
| `--keep-last N` | Legacy: always keep the N most recent snapshots by ID |
| `--keep-weekly N` | Legacy: keep one snapshot per week (Mon–Sun) for the last N weeks |
| `--keep-monthly N` | Legacy: keep one snapshot per calendar month for the last N months |
| `--keep-yearly N` | Legacy: keep one snapshot per calendar year for the last N years |
| `--no-policy` | Ignore `policy.conf`; all retention rules must be on the command line |
| `--dry-run` | Show which snapshots and reverse records would be removed without deleting anything |

**GFS mode (`--gfs`):** Uses the GFS engine (see [GFS retention](#gfs-retention)). Calendar anchor snapshots are never deleted. Non-anchor snapshots outside the `keep_revs` window are deleted. Reverse records are deleted only when outside the `keep_revs` window AND older than the oldest GFS anchor. GC runs automatically at the end.

**Legacy mode:** Any combination of `--keep-*` flags may be used; a snapshot is kept if it satisfies any rule. Snapshots not selected by any rule are deleted. If no rules are provided on the command line and no policy is loaded, the command exits with an error rather than deleting everything.

**Preserved tags** — A snapshot protected by a preserved tag is never deleted by either prune mode. See `tag set --preserve`.

Only `.snap` files are deleted by legacy prune. `--gfs` additionally manages reverse records as described above.

---

### `backup gc --repo <path>`

Run garbage collection: scan all surviving snapshots and reverse records, collect the set of referenced objects, and delete any objects not in that set.

```
backup gc --repo /mnt/backup/myrepo
```

GC also rewrites any pack files that contain dead entries, producing compacted packs. This is called automatically by `prune`; you rarely need to run it manually unless you have deleted snapshot or reverse files by hand.

---

### `backup pack --repo <path>`

Pack all loose objects into a new pack file.

```
backup pack --repo /mnt/backup/myrepo
```

Under normal operation, `backup run` packs automatically after each successful backup. Use this command to pack after operations that may produce loose objects without auto-packing (e.g. after a `checkpoint` run). `pack` runs GC first, so dead loose objects are dropped rather than packed.

---

### `backup checkpoint --repo <path> [--snapshot <id|tag>] [--every N]`

Materialise a full snapshot file for a historical snapshot that would otherwise require walking the reverse chain.

```
backup checkpoint --repo /mnt/backup/myrepo --snapshot 50
backup checkpoint --repo /mnt/backup/myrepo --every 10
```

`--every N` synthesises checkpoints at snapshot IDs that are multiples of N (e.g. `--every 10` creates snapshots 10, 20, 30, …). Snapshots that already have a `.snap` file are skipped.

Once a checkpoint exists, `restore --at` walks the reverse chain only from that checkpoint to the target, not all the way from HEAD.

---

### `backup verify --repo <path>`

Check that every object referenced by any surviving snapshot is present and readable.

```
backup verify --repo /mnt/backup/myrepo
```

Exits 0 if all objects are found. Exits 1 and prints error messages if any are missing. Pruned snapshots are skipped.

---

### `backup stats --repo <path>`

Print storage statistics for the repository.

```
backup stats --repo /mnt/backup/myrepo
```

Example output:
```
snapshots:       8 present / 12 total (HEAD)
loose objects:   0  (0 B)
pack files:      3  (14.2 MB)
reverse records: 11  (48.0 KB)
total repo size: 14.3 MB
```

---

### `backup tag --repo <path> set --snapshot <id|tag> --name <name> [--preserve]`
### `backup tag --repo <path> list`
### `backup tag --repo <path> delete --name <name>`

Manage human-readable tag names that point to snapshot IDs.

```
backup tag --repo /mnt/backup/myrepo set --snapshot 42 --name release-v2.0
backup tag --repo /mnt/backup/myrepo set --snapshot 42 --name before-upgrade --preserve
backup tag --repo /mnt/backup/myrepo list
backup tag --repo /mnt/backup/myrepo delete --name before-upgrade
```

Tag names may contain letters, digits, hyphens, underscores, and dots. Tags can be used in place of a numeric snapshot ID in any command that accepts `--snapshot`.

`--preserve` marks the tag so that `prune` will never delete the pointed-to snapshot. A warning is printed if prune encounters a preserved snapshot, and it is skipped (non-fatal). Use this to protect snapshots that must survive long-term regardless of retention policy.

`list` output:
```
  release-v2.0                   -> snapshot 00000042
  before-upgrade                 -> snapshot 00000042  [preserved]
```

---

## Policy

The policy file (`repo/policy.conf`) stores defaults for `backup run` and `backup prune`. It is read automatically unless `--no-policy` is passed.

Manage with:
```
backup policy --repo <path> set [options]
backup policy --repo <path> get
backup policy --repo <path> edit
```

`set` merges — only the fields you specify are changed.

### Policy options

| Option | Default | Meaning |
|--------|---------|---------|
| `--path <p>` | (none) | Source path. Repeatable. |
| `--exclude <pat>` | (none) | Exclusion glob. Repeatable. |
| `--keep-revs N` | 0 (off) | GFS: minimum rolling window of snapshots to keep. Silently extended to always reach the oldest GFS anchor. |
| `--checkpoint-every N` | 0 (off) | Auto-synthesise checkpoints every N snapshots after `run`. |
| `--keep-last N` | 0 (off) | Legacy retention: keep N most recent snapshots. |
| `--keep-weekly N` | 0 (off) | GFS activation: also used as a signal to activate the GFS engine when combined with `keep_revs`. |
| `--keep-monthly N` | 0 (off) | GFS activation signal (see above). |
| `--keep-yearly N` | 0 (off) | GFS activation signal (see above). |
| `--auto-pack` / `--no-auto-pack` | true | Pack objects after each `run`. |
| `--auto-gc` / `--no-auto-gc` | false | Run GC after each `run`. |
| `--auto-prune` / `--no-auto-prune` | false | Run prune/GFS after each `run`. |
| `--auto-checkpoint` / `--no-auto-checkpoint` | false | Run checkpoint after each `run`. |
| `--verify-after` / `--no-verify-after` | false | After committing each snapshot, confirm every object it references is present in the store. Recommended for mission-critical backups. |

### GFS retention example (recommended)

Keep a rolling window of 20 recent snapshots, plus permanent GFS anchors (daily/weekly/monthly/yearly):

```
backup policy --repo /mnt/backup/myrepo set \
    --keep-revs 20 \
    --auto-prune
```

With this policy and daily backups, after each `backup run`:

- The 20 most recent snapshots are always kept.
- Each calendar day's last snapshot is permanently anchored as a **daily** GFS snapshot.
- Each Sunday's daily snapshot is promoted to a **weekly** anchor.
- The last Sunday of each month is promoted to a **monthly** anchor.
- The last Sunday of December is promoted to a **yearly** anchor.
- Snapshots outside the 20-snapshot window that carry no GFS flag are deleted.
- Reverse records are deleted only when outside the 20-snapshot window *and* older than the oldest GFS anchor.

**What this looks like over time** (running daily):

| Age | What survives |
|-----|--------------|
| Last 20 days | All snapshots (rolling window) |
| 21+ days | Only GFS-anchored snaps (daily/weekly/monthly/yearly) |
| After a Sunday | That Sunday's snap is a weekly anchor — kept indefinitely |
| After December | That December's last-Sunday snap is a yearly anchor — kept indefinitely |

**Restoring any GFS-anchored snapshot is always direct** — its `.snap` file is preserved permanently. No reverse-chain walking is needed.

**Restoring a non-anchored day** (e.g. a Wednesday from 3 months ago) requires `restore --at`. The tool walks the reverse chain back from the nearest surviving GFS anchor. Since weekly anchors exist for every Sunday, the worst-case walk from a monthly anchor is ~4 steps (Sunday gaps). Between yearly anchors in years with no monthly anchors, the walk can be longer; adding `--checkpoint-every 30` caps it at 29 steps regardless of age.

### Legacy sliding-window retention

The older `keep_last / keep_weekly / keep_monthly / keep_yearly` flags still work for configurations that do not use `keep_revs`. They select survivors purely by recency within a window and do not write GFS flags into snapshot files.

---

## Workflows

### Initial setup with policy

```sh
backup init --repo /mnt/external/myrepo \
    --path /home/alice \
    --keep-revs 20 \
    --auto-prune
```

### Routine backup

```sh
backup run --repo /mnt/external/myrepo
```

Source paths, exclusions, and post-backup actions are all driven by `policy.conf`. If nothing changed since the last backup, no snapshot is created.

### Backup with ad-hoc overrides

```sh
# Override paths (ignores policy paths)
backup run --repo /mnt/external/myrepo --path /home/alice --path /etc

# Add exclusions without changing the policy
backup run --repo /mnt/external/myrepo --exclude "*.tmp" --exclude ".cache"

# Skip automatic post-backup prune this once
backup run --repo /mnt/external/myrepo --no-prune
```

### Scheduled maintenance (cron)

```sh
#!/bin/sh
# Scheduled backup — policy handles prune, checkpoint, and GC automatically
backup run --repo /mnt/external/myrepo --quiet
```

### Manual retention run

```sh
# GFS engine (recommended)
backup prune --repo /mnt/external/myrepo --gfs

# Legacy sliding-window
backup prune --repo /mnt/external/myrepo \
    --keep-last 7 --keep-weekly 8 --keep-monthly 12 --keep-yearly 5
backup verify --repo /mnt/external/myrepo
```

### Recover the latest backup

```sh
backup restore --repo /mnt/external/myrepo --dest /tmp/recovery
```

### Recover with integrity check

```sh
backup restore --repo /mnt/external/myrepo --dest /tmp/recovery --verify
```

### Browse and recover a specific old file

```sh
# Find which snapshot you want
backup list --repo /mnt/external/myrepo

# Browse the snapshot tree
backup ls --repo /mnt/external/myrepo --snapshot 12 --path alice/documents

# Restore just the file you need
backup restore --repo /mnt/external/myrepo --snapshot 12 \
    --file alice/documents/report.pdf --dest /tmp
```

### Restore a directory subtree

```sh
backup restore --repo /mnt/external/myrepo --snapshot 12 \
    --file alice/documents --dest /tmp/recovery
```

### Tag an important snapshot and protect it

```sh
backup tag --repo /mnt/external/myrepo set --snapshot 47 --name before-upgrade --preserve
# This snapshot will now be skipped by prune regardless of retention rules
```

### Restore a tagged (possibly pruned) historical snapshot

```sh
backup restore --repo /mnt/external/myrepo --snapshot before-upgrade --at --dest /tmp/pre-upgrade
```

### Compare two snapshots

```sh
backup diff --repo /mnt/external/myrepo --from 10 --to 20
backup diff --repo /mnt/external/myrepo --from before-upgrade --to after-upgrade
```

### Check repository storage usage

```sh
backup stats --repo /mnt/external/myrepo
```

---

## Snapshot Numbering

Snapshots are numbered sequentially starting at 1. The number never resets. After pruning, snapshot files 1 through N−keep are deleted, but the HEAD pointer and the numbering continue from where they left off. A repository with 1000 backups pruned to keep 50 will have snapshot files 951–1000 and a HEAD of 1000.

---

## Storage Management

**Deduplication** — Content is stored once per unique hash. If you back up two files with identical content, or the same file that has not changed across snapshots, only one object is stored.

**Incremental cost** — Each backup stores only new or changed object data, plus a small snapshot manifest and reverse record. Unchanged files add only a few bytes of metadata per snapshot.

**No-change detection** — If no files have changed since the last backup, no new snapshot or reverse record is created. The command exits cleanly with a message.

**Auto-packing** — Each successful `backup run` automatically packs loose objects. Use `--no-pack` to defer this and run `backup pack` manually at a convenient time.

**Reclaiming space** — `prune` deletes old snapshot manifests and runs GC. GC retains any object referenced by a surviving snapshot or a reverse record (needed for `restore --at`). Objects referenced only by fully-gone paths are deleted.

**Checkpoints for fast historical restores** — Without checkpoints, `restore --at` for an old snapshot must walk the entire reverse chain from HEAD. With checkpoints placed every N snapshots, the maximum walk is N−1 steps. Use `backup checkpoint --every N` to maintain a regular cadence.

**Pack files** — Multiple pack files accumulate over time (one per `pack` invocation); this is normal and all are searched transparently on lookup. GC compacts packs when it runs.

---

## What Gets Backed Up

The following filesystem properties are fully preserved:

| Property | Preserved |
|---|---|
| File contents | Yes |
| Directory structure | Yes |
| Regular files | Yes |
| Symbolic links (target) | Yes |
| Hard links | Yes (link topology reconstructed on restore) |
| FIFOs (named pipes) | Yes |
| Character devices | Yes |
| Block devices | Yes |
| Sparse files (holes) | Yes |
| File mode (permissions) | Yes |
| Owner uid/gid | Yes |
| Modification time | Yes |
| Extended attributes (xattrs) | Yes |
| POSIX ACLs | Yes |

**Not preserved:** access time (atime), creation time (ctime), sockets, mount points (not descended into), open file locks.

---

## Caveats and Limitations

- **Single writer.** An exclusive advisory lock (`flock`) is held during all write operations (`run`, `prune`, `gc`, `pack`, `checkpoint`). A second writer will fail immediately with an error rather than corrupting the repository. Read-only operations (`list`, `ls`, `restore`, `diff`, `stats`, `verify`) do not take the lock.
- **Local repositories only.** The repository must be on a locally-mounted filesystem. Network mounts work if the filesystem supports `fsync` and atomic `rename` correctly, but this is untested.
- **No rename detection.** A renamed file is treated as a deletion plus a creation. The content object is deduplicated, but the metadata history for the old path is not linked to the new path.
- **Running processes.** Files that are being written during a backup may be captured in a partially-written state. The tool makes no attempt to quiesce applications.
- **Root permissions for devices.** Backing up and restoring character/block devices requires root. Restoring uid/gid and ACLs to their original values also typically requires root.
- **Path length.** Internal path buffers are sized to `PATH_MAX` (typically 4096 bytes on Linux). Paths exceeding this limit are silently skipped during restore.
- **Checkpoint synthesis cost.** `checkpoint --every N` on a long repository with no existing checkpoints must reconstruct every Nth working set by walking the reverse chain. For large repositories this is a one-time cost; subsequent runs skip already-synthesised checkpoints.
