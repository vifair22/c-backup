# c-backup

A content-addressed backup daemon. One `c-backupd` per machine owns every
repository on it and is the only writer; `c-backup` is a thin HTTP client, and
the Electron app talks to the same API.

Version 1.0. The operator's reference is [`Manual.md`](Manual.md): installing,
configuring, remote access, every operation, disaster recovery, capacity
planning and the known limits.

This version reads and writes repository format 3 only. A repository of any
other format is refused at registration and at open, naming its format, and
there is no migration: rebuild it by backing up again into a new repository.
The `pre-daemon-rewrite` tag builds a binary that reads the pre-rewrite format,
and the last format 2 build reads format 2.

## What it does

- **Content-addressed storage.** Objects named by SHA-256 of type and payload
  and written once, straight into packs; content-defined chunking for large
  files, so an edit stores only the chunks it touched; LZ4 with a 64 KiB probe
  that gives up quickly on incompressible data; Reed-Solomon parity for every
  pack entry; holes kept as holes; and pack coalescing.
- **Manifest trees.** A snapshot's metadata is one content-addressed object per
  directory, so an unchanged directory costs nothing on the next backup.
  Measured: 3.3% new tree objects per incremental snapshot.
- **A repository that describes itself.** The database is an index over the
  trees in the repository, not the only copy. Lose it and `reindex` rebuilds
  it; you lose query speed and job history, not backups.
- **Real scheduling.** Interval triggers anchored to the last successful run,
  and calendar triggers that handle DST, month ends and leap days.
- **GFS retention.** Daily, weekly, monthly and yearly tiers as a bitfield, so
  one snapshot can hold several at once. Tagged snapshots are never pruned.

## Building

Needs a sibling checkout of [c-utils](https://git.airies.net/vifair22/c-utils)
at `../c-utils`, or `CUTILS_DIR` pointing elsewhere. The pinned version is in
`cutils_version`. c-utils also supplies cJSON; this repository carries no
copy of its own.

```sh
make            # c-backupd and c-backup into build/bins/
make debug      # -Og -g
make asan       # -fsanitize=address,undefined (make tsan: -fsanitize=thread)
make analyze    # stack-usage (64 KiB limit), gcc -fanalyzer, cppcheck
make test       # unit suites, then the HTTP and CLI end-to-end suites
make coverage   # gcovr; fails under 80% whole-repo, or the src/store floor
make bench      # microbenchmarks and browse-path benchmarks
make test-asan  # every unit and fault suite plus the end-to-end suites, ASan + UBSan
make test-tsan  # the same under TSan
```

The sanitizer runs keep going past a failing suite and name every one at the
end. Reports are written to `build/<asan|tsan>/reports/<suite>/` as well,
and a suite that leaves one there fails even if it passed, since a race or a
leak at a daemon's exit does not change its status. `SAN_ONLY='test_db_txn
test_api_e2e.sh'` runs just those. The sanitizer runtime is linked
statically when the toolchain ships the archive, and dynamically otherwise.

### Test scratch

The suites write their scratch under one directory, `C_BACKUP_TEST_DIR`.
By default it is `$XDG_CACHE_HOME/c-backup-test-<checksum of the checkout
path>` (`~/.cache/...` when `XDG_CACHE_HOME` is unset): on disk, because
`/tmp` is often a tmpfs and dense scratch there is RAM, and per checkout, so
two worktrees can test at once. CI points it at the job directory. Keep the
path short; two suites put unix sockets in it, and a socket path holds at
most 108 bytes.

```sh
make test C_BACKUP_TEST_DIR=/mnt/scratch/cbt   # somewhere else
make test C_BACKUP_SCRATCH_BUDGET=4G           # a tighter budget
```

`make test`, `make coverage`, `make test-asan` and the fuzz targets run every
suite under `tests/scratch-guard.sh`. It prints each suite's name and start
time as it begins and its peak scratch as it ends, stops a suite that holds
more than `C_BACKUP_SCRATCH_BUDGET` (default `10G`) at once and fails it by
name, and removes and fails on anything a suite leaves behind. The C suites
have the directory compiled in, so changing it relinks them.

Build dependencies on Debian:

```sh
apt-get install build-essential pkg-config libssl-dev liblz4-dev \
                libacl1-dev libsqlite3-dev libmicrohttpd-dev \
                libcurl4-openssl-dev libcmocka-dev
```

**OpenSSL 3.2 or newer is required**, for Argon2id key derivation. Debian 13
(trixie) and Arch ship it; Ubuntu 24.04 and Debian 12 ship 3.0 and do not
build. Use the Docker image there. The build stops up front with a message
naming the version it found. Manual section 2.1 has the details.

The desktop app in `ui/` needs Node.js 22.12 or newer (`engines` in
`ui/package.json`): Vite 7 needs it to build, and `npm test` runs the
TypeScript tests directly with `--experimental-strip-types`. CI uses Node 24.

## Running

```sh
sudo mkdir -p /etc/c-backupd
sudo cp packaging/config.yaml /etc/c-backupd/config.yaml
sudo chmod 600 /etc/c-backupd/config.yaml    # it holds Pushover credentials
sudo cp packaging/systemd/c-backupd.service /etc/systemd/system/
sudo systemctl enable --now c-backupd
```

Then set an admin password on the host. It prompts, and it is the only way to
set the first one: until it is set the TCP listener serves nothing but the
health probes. The unix socket is always gated by filesystem permissions
instead.

```sh
c-backup auth setup
```

The TCP listener binds loopback by default. To connect the desktop app from
another machine, set `http.bind` in `/etc/c-backupd/config.yaml` to `0.0.0.0`
(or one interface's address) and restart. The transport is plain HTTP, so put
a reverse proxy or a tunnel in front on any network you do not trust.

## Using it

```sh
c-backup repo add /srv/backup/main main
c-backup backup 1 /home/me/documents
c-backup jobs                       # watch it
c-backup snapshots 1
c-backup ls 1 1                     # browse a snapshot
c-backup search 1 invoice
c-backup restore 1 1 /tmp/restored
```

Every command is an HTTP request over the daemon's socket. If the daemon is
not running the CLI says so and exits; there is no standalone mode, because
having exactly one writer is the point.

Output is the daemon's JSON, one document per command, so it pipes straight
into `jq`.

## Layout

| Path | Contents |
|---|---|
| `src/store/` | Storage engine: objects, packs, parity, manifest trees |
| `src/ops/` | Backup, restore, reindex, GC, prune, GFS, scan |
| `src/api/` | HTTP server, auth, routes |
| `src/scheduler/` | Triggers, schedule loop, job manager |
| `src/meta/` | Repository identity and per-repo locks |
| `src/daemon/`, `src/cli/` | The two entry points |
| `ui/` | Electron app |
| `docs/` | `api.md`, `repo-format.md`, `data-paths.md` |
| `bench/` | Benchmarks; results journalled in `PERF.md` |

## Documentation

- `Manual.md`: the operator's manual. Start here to run it.
- `docs/repo-format.md`: on-disk layout, manifest trees, and why determinism
  matters more than it looks.
- `docs/api.md`: every endpoint, with the reasoning behind pagination, auth
  and the trigger model.
- `docs/data-paths.md`: how bytes move end to end. The storage and restore
  pipelines, every per-file-type branch, packing and compression decisions,
  and the maintenance paths that rewrite data.
- `PERF.md`: benchmark journal. New numbers are added, not substituted.
- `docs/acceptance.md`: the acceptance checklist and the results of each full
  pass, including what was measured on a real Unraid server.
- `diagrams/`: Mermaid diagrams for individual mechanisms (parity layout,
  pack dispatch, GFS promotion, dedup cascade).

## License

GPL. See `LICENSE`.
