# Head-to-head: c-backup vs restic, borg, kopia (protocol)

Approved on 2026-11-15, with the
amendments below folded in. **Not run yet**: it runs on the c-backup v3 image (content-defined chunking,
new repository format), whose chunking replaces the B086 write-once path
(orchestrator, 2026-11-16). Every c-backup repo on FS2 is recreated for v3
first; c-backup sends the tag.

## Tools (installed on FS2, 2026-11-15)

Binaries in `/mnt/cache/appdata/bench/bin`, nothing on FS1 or the Unraid OS image.

| Tool | Version | Verified by |
|---|---|---|
| restic | 0.19.1 (go1.26.4) | sha256 against the release `SHA256SUMS` |
| kopia | 0.23.1 (build 72ec08fd) | sha256 against the release `checksums.txt` |
| borg | 1.4.5 (`borg-linux-glibc231-x86_64`) | GPG good signature, key `6D5B EF9A DD20 7580 5747 B70F 9F88 FB52 FAF7 B393` |
| c-backup | the v3 image (tag to come) | registry image |

## Equal footing

- Every tool, c-backup included, runs in a `debian:trixie` container with the
  same `fs1-documents` read-only NFS volume and its repo on `/mnt/cache` (btrfs
  RAID10 on dm-crypt over 4 x BX500). No CPU or memory limits.
- Per run, from the container cgroup: wall time, CPU time (user + sys from
  `cpu.stat`), peak RSS (`memory.peak`). FS1 load logged from the desktop at
  each start (FS2 has no ssh key to FS1).
- c-backup timing ends only when the repo is quiescent: the job is complete,
  including the end-of-backup pack pass, node indexing and any follow-on job it
  triggers.

## Data

- **D1**: the 1,605-file / 15.24 GB FS1 Documents subset
  (`Vincent/{Projects,Lab,Vehicles,Firearms,Software,Records,Career}`), over NFS.
- **D1-local**: the same files copied onto FS2 (`/mnt/cache/bench/d1-pristine`),
  used for the changed-incremental scenario and for the NFS-vs-local split.
- **D2**: many small files, the Linux kernel source tree unpacked on FS2
  (`/mnt/cache/bench/d2-pristine`).
- **Change set** (`bench-change.sh`, fixed seed, applied to a reflink copy reset
  before every run): rewrite a 2% byte range in 20 files, append to 10, add 50
  new files (about 120 MB), delete 30, rename 10, touch the mtime of 100 without
  changing content.

## Scenarios (per data set)

1. First backup, fresh repo and fresh tool caches (D1 over NFS and D1-local).
2. No-change incremental.
3. Changed incremental (D1-local, D2).
4. Full restore to the same pool, plus `sync`; sha256 compared with the source.
5. Final repo size, with c-backup's Reed-Solomon parity share reported
   separately (about 6.7%; the others store none by default); each tool's
   check/verify timed separately, outside the comparison.

## Settings

- Encryption on for all: c-backup `--encrypt system`, restic repo password,
  borg `repokey-blake2`, kopia default AES256-GCM.
- Compression at each tool's default (restic auto/zstd, borg lz4, c-backup lz4
  at pack, kopia none), plus an extra kopia row with `--compression=zstd`.
- Threads at defaults. Each tool keeps its own cache between runs of one
  sequence; all caches wiped for each first backup.

## Runs

- FS2 page cache dropped before every timed run; tool order rotated each round.
- D1: one warm-up, then 3 runs per tool for first backup and restore (more if
  the tools' ranges overlap, before calling a winner), 5 for the incrementals.
- D2: one warm-up, then 5 runs per tool for everything.
- A `cp` baseline for D1 first backup and restore runs in the same rotation.
- Report medians, min/max, and every run.

## Prepared on FS2 (2026-11-15)

- `/mnt/cache/bench/d1-pristine`: D1 copied from the NFS source (1,605 files, 15,242,356,060 bytes, identical count and size).
- `/mnt/cache/bench/d2-pristine`: Linux 7.3.4 (`linux-7.3.4.tar.xz`, sha256 checked against kernel.org `sha256sums.asc`): 95,979 files, 6,286 dirs, 1,641,795,193 bytes.
- `/mnt/cache/bench/bench-change.sh PRISTINE WORK` (copy in `uat/`): resets WORK to a reflink copy and applies the change set. File choice by `awk srand(20261115)`, content from a fixed-key AES-128-CTR stream, so two runs give byte-identical trees (checked with sha256 over every file, D1 and D2). Effect per run: 90 paths new or changed (20 rewrites of a 2% range, 10 appends of 64 KiB, 50 new 2.4 MiB files, 10 renames), 70 removed or changed (incl. 30 deletes of files under 8 MiB), 100 mtime touches. D1 goes from 15.24 GB to 15.34 GB.
