# c-backup Manual

A reverse-incremental filesystem backup tool for Linux, written in C.

---

## Table of Contents

1. [Concepts](#concepts)
2. [Repository Layout](#repository-layout)
3. [Commands](#commands)
4. [Workflows](#workflows)
5. [Snapshot Numbering](#snapshot-numbering)
6. [Storage Management](#storage-management)
7. [What Gets Backed Up](#what-gets-backed-up)
8. [Caveats and Limitations](#caveats-and-limitations)

---

## Concepts

**Object store** — File contents and metadata blobs (xattrs, ACLs) are stored as content-addressed objects identified by their SHA-256 hash, compressed with LZ4. Identical content is stored only once regardless of how many files reference it or how many snapshots contain it.

**Snapshot** — A complete description of the filesystem tree at one point in time. The latest snapshot is always a full state — restoring it requires no reconstruction. Each snapshot is a numbered binary file containing a node table (metadata for every file, directory, symlink, etc.) and a dirent table (the parent/child relationships that form the directory tree).

**Reverse record** — For every snapshot after the first, a reverse record is stored. It describes how to undo that snapshot to recover the previous state. This is how historical restores work: start from the current (full) snapshot, then apply reverse records backward until the target snapshot's state is reconstructed.

**Reverse-incremental model** — The most recent snapshot is the cheapest to restore because it is already fully materialised. Older snapshots cost proportionally more, since each step back requires applying one more reverse record. Storage cost for history is low: only changed data and reverse-record metadata are stored for old snapshots.

**Pack files** — Loose objects (individual files under `objects/`) can be compacted into pack files (`.dat` + `.idx` pairs) under `packs/`. Packing improves lookup performance and reduces inode count. Loose and packed objects are transparent to all commands.

---

## Repository Layout

```
repo/
  format.json          version marker
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
  tmp/                 crash-safe staging area (temp files, auto-cleaned)
```

Snapshot files for old snapshots are deleted by `prune`. Their reverse records are **not** deleted — they are required to reconstruct historical states. The GC retains any object referenced by a surviving snapshot or a surviving reverse record.

---

## Commands

### `backup init <repo>`

Create a new, empty repository at `<repo>`.

```
backup init /mnt/backup/myrepo
```

`<repo>` must not already exist as a repository. The directory is created if it does not exist.

---

### `backup run <repo> <path> [<path>...]`

Back up one or more source paths into the repository, creating a new snapshot.

```
backup run /mnt/backup/myrepo /home/alice
backup run /mnt/backup/myrepo /home/alice /etc /var/log
```

Each path is stored under its **basename** in the snapshot. For example, backing up `/home/alice` stores it as `alice/` in the snapshot tree. Backing up multiple paths stores them as siblings.

The backup is incremental: only files that have changed since the previous snapshot are re-read and re-hashed. Unchanged files are recorded in the new snapshot without re-storing their content.

A reverse record is written for every snapshot after the first, allowing any previous snapshot to be reconstructed.

---

### `backup list <repo>`

List all snapshots in the repository.

```
backup list /mnt/backup/myrepo
```

Output format:
```
snapshot 00000001  2024-03-15 09:42:11  312 entries
snapshot 00000002  2024-03-15 14:07:33  315 entries
snapshot 00000003  2024-03-16 08:00:01  315 entries
```

Pruned snapshots (whose `.snap` file has been deleted) are skipped silently.

---

### `backup ls <repo> <snapshot> [path]`

Browse the contents of a directory within a snapshot, similar to `ls -l`.

```
backup ls /mnt/backup/myrepo 3
backup ls /mnt/backup/myrepo 3 alice
backup ls /mnt/backup/myrepo 3 alice/documents
```

`<path>` is relative to the snapshot root (omit leading slashes). Omitting `<path>` or using `""` lists the root. Returns an error if the path does not name a directory in that snapshot.

Output format:
```
snapshot 3  /alice/documents
-rw-------  1000  1000     4096  2024-03-15 14:07  report.pdf
-rw-------  1000  1000     8192  2024-03-14 11:22  notes.txt
drwx------  1000  1000        0  2024-03-10 09:00  archive
```

Columns: permissions, uid, gid, size (bytes), mtime, name. Symlinks show `name -> target`. Character/block devices show major,minor instead of size.

---

### `backup restore <repo> <snapshot> <dest>`

Restore a specific snapshot to `<dest>`. The snapshot file must still exist (i.e., it has not been pruned). Use `restore-at` for historical restore of pruned snapshots.

```
backup restore /mnt/backup/myrepo 5 /tmp/restore-test
```

`<dest>` is created if it does not exist. Source paths are restored as subdirectories of `<dest>`. For example, a backup of `/home/alice` restores to `<dest>/alice/`.

---

### `backup restore-latest <repo> <dest>`

Restore the latest (current HEAD) snapshot to `<dest>`.

```
backup restore-latest /mnt/backup/myrepo /tmp/restore-test
```

This is the fastest restore operation. No reverse-chain walking is needed.

---

### `backup restore-at <repo> <snapshot> <dest>`

Restore the state of any snapshot — including those whose `.snap` file has been pruned — by walking the reverse chain.

```
backup restore-at /mnt/backup/myrepo 1 /tmp/old-restore
```

If the target snapshot file is still present, it is used directly (same as `restore`). If it has been pruned, the tool loads the current HEAD snapshot and applies reverse records backward until the target state is reconstructed. All objects needed for this reconstruction are guaranteed to be retained by the GC as long as the relevant reverse records exist.

Restoring very old snapshots over a long reverse chain is slower than restoring recent ones.

---

### `backup restore-file <repo> <snapshot> <file-path> <dest>`

Restore a single file from a snapshot.

```
backup restore-file /mnt/backup/myrepo 3 alice/documents/report.pdf /tmp
```

`<file-path>` is the repo-relative path shown by `ls`. The file is written into `<dest>`. Only the target snapshot's snapshot file needs to be present; this command does not walk the reverse chain.

---

### `backup prune <repo> <keep_count>`

Delete old snapshot files, keeping only the most recent `<keep_count>` snapshots.

```
backup prune /mnt/backup/myrepo 10
```

Only the `.snap` files are deleted. Reverse records for pruned snapshots are **retained** so that `restore-at` can still reconstruct any historical state. After deleting the snapshot files, `prune` runs garbage collection automatically to reclaim objects that are no longer referenced by any surviving snapshot or reverse record.

`keep_count` must be at least 1. If fewer snapshots exist than `keep_count`, nothing is pruned.

---

### `backup verify <repo>`

Check that every object referenced by any surviving snapshot is present and readable.

```
backup verify /mnt/backup/myrepo
```

Exits 0 if all objects are found. Exits 1 and prints error messages if any objects are missing or corrupt. Pruned snapshots are skipped.

`verify` does not check reverse records or objects referenced only by the reverse chain (those are not needed for restoring surviving snapshots directly).

---

### `backup gc <repo>`

Run garbage collection: scan all surviving snapshots and reverse records, collect the set of referenced objects, and delete any objects not in that set.

```
backup gc /mnt/backup/myrepo
```

GC also rewrites any pack files that contain dead entries, producing compacted packs. This is called automatically by `prune`; you rarely need to run it manually unless you have deleted snapshot or reverse files by hand.

---

### `backup pack <repo>`

Pack all loose objects into a new pack file.

```
backup pack /mnt/backup/myrepo
```

Loose objects under `objects/` are collected, sorted, written into a new `pack-NNNNNNNN.dat` with a corresponding `pack-NNNNNNNN.idx`, and then removed. Subsequent backups write new objects as loose files; run `pack` again when you want to consolidate them.

`pack` runs GC before packing, so dead loose objects are dropped rather than packed.

---

## Workflows

### Initial setup

```sh
backup init /mnt/external/myrepo
backup run  /mnt/external/myrepo /home/alice
```

### Routine backup

```sh
backup run /mnt/external/myrepo /home/alice
```

### Scheduled maintenance (e.g. weekly cron)

```sh
# Keep the last 30 snapshots, pack loose objects, verify integrity
backup prune  /mnt/external/myrepo 30
backup pack   /mnt/external/myrepo
backup verify /mnt/external/myrepo
```

### Recover the latest backup

```sh
backup restore-latest /mnt/external/myrepo /tmp/recovery
```

### Browse and recover a specific old file

```sh
# Find which snapshot you want
backup list /mnt/external/myrepo

# Browse the snapshot tree
backup ls /mnt/external/myrepo 12 alice/documents

# Restore just the file you need
backup restore-file /mnt/external/myrepo 12 alice/documents/report.pdf /tmp
```

### Recover the full state from a pruned snapshot

```sh
# Snapshot 1 was pruned, but restore-at can still reconstruct it
backup restore-at /mnt/external/myrepo 1 /tmp/recovery-old
```

---

## Snapshot Numbering

Snapshots are numbered sequentially starting at 1. The number never resets. After pruning, snapshot files 1 through N−keep are deleted, but the HEAD pointer and the numbering continue from where they left off. A repository with 1000 backups pruned to keep 50 will have snapshot files 951–1000 and a HEAD of 1000.

---

## Storage Management

**Deduplication** — Content is stored once per unique hash. If you back up two files with identical content, or the same file that has not changed across snapshots, only one object is stored.

**Incremental cost** — Each backup stores only new or changed object data, plus a small snapshot manifest and reverse record. Unchanged files add only a few bytes of metadata per snapshot.

**Reclaiming space** — `prune` deletes old snapshot manifests and runs GC. GC retains any object referenced by a surviving snapshot or a reverse record (needed for `restore-at`). Objects referenced only by fully-gone paths — with no reverse record pointing at them — are deleted.

**Pack files** — After many backups, `objects/` may contain thousands of small files. Run `pack` periodically to consolidate them. Multiple pack files accumulate over time (one per `pack` invocation); this is normal and all are searched transparently on lookup.

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

- **Single writer.** Running two `backup run` commands against the same repository simultaneously is not safe. No locking is implemented.
- **Local repositories only.** The repository must be on a locally-mounted filesystem. Network mounts work if the filesystem supports `fsync` and atomic `rename` correctly, but this is untested.
- **No rename detection.** A renamed file is treated as a deletion plus a creation. The content object is deduplicated, but the metadata history for the old path is not linked to the new path.
- **Running processes.** Files that are being written during a backup may be captured in a partially-written state. The tool makes no attempt to quiesce applications.
- **Root permissions for devices.** Backing up and restoring character/block devices requires root. Restoring uid/gid and ACLs to their original values also typically requires root.
- **Path length.** Internal path buffers are sized to `PATH_MAX` (typically 4096 bytes on Linux). Paths exceeding this limit are silently skipped during restore.
