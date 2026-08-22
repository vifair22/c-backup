# c-backup

A content-addressed backup daemon. One `c-backupd` per machine owns every
repository on it and is the only writer; `c-backup` is a thin HTTP client, and
the Electron app talks to the same API.

**Status: mid-rewrite.** The daemon backs up, restores, prunes and schedules.
Several UI panels are still blocked on introspection endpoints that are
designed but not built. See `daemonization-spec.md` for the plan and the
per-phase record of what changed and why.

## What it does

- **Content-addressed storage.** SHA-256 objects, LZ4 with a 64 KiB probe that
  gives up quickly on incompressible data, Reed-Solomon parity in every object
  and pack trailer, sparse-file handling, and pack coalescing.
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
`cutils_version`.

```sh
make            # c-backupd and c-backup into build/bins/
make debug      # -Og -g
make asan       # -fsanitize=address
make analyze    # stack-usage (64 KiB limit), gcc -fanalyzer, cppcheck
make test       # unit suites, then the HTTP and CLI end-to-end suites
make coverage   # gcovr report with the src/store ratchet
make bench      # microbenchmarks and browse-path benchmarks
```

Build dependencies on Debian:

```sh
apt-get install build-essential pkg-config libssl-dev liblz4-dev \
                libacl1-dev libsqlite3-dev libmicrohttpd-dev \
                libcurl4-openssl-dev libcmocka-dev
```

## Running

```sh
sudo mkdir -p /etc/c-backupd
sudo cp packaging/config.yaml /etc/c-backupd/config.yaml
sudo chmod 600 /etc/c-backupd/config.yaml    # it holds Pushover credentials
sudo cp packaging/systemd/c-backupd.service /etc/systemd/system/
sudo systemctl enable --now c-backupd
```

Then set an admin password. Until you do, the TCP listener is open so the
daemon can be bootstrapped; the unix socket is always gated by filesystem
permissions instead.

```sh
curl -X POST -d '{"password":"..."}' http://localhost:8765/api/auth/setup
```

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

Output is the daemon's JSON, unmodified. Pipe through `jq` for fields.

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

- `docs/repo-format.md`: on-disk layout, manifest trees, and why determinism
  matters more than it looks.
- `docs/api.md`: every endpoint, with the reasoning behind pagination, auth
  and the trigger model.
- `docs/data-paths.md`: how bytes move end to end. The storage and restore
  pipelines, every per-file-type branch, packing and compression decisions,
  and the maintenance paths that rewrite data.
- `PERF.md`: benchmark journal. New numbers are added, not substituted.
- `diagrams/`: Mermaid diagrams for individual mechanisms (parity layout,
  pack dispatch, GFS promotion, dedup cascade, FUSE ring).

## License

GPL. See `LICENSE`.
