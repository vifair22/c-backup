# Source layout

Where things live under `src/`, one row per directory. CI checks that every
directory under `src/` has a row here (`make layout-check`, run by
`make analyze` and the `lint` job), so a new directory cannot land without
saying what it is for. Objects are flattened into one build directory by
basename, so no two files anywhere under `src/` may share a name; the
Makefile refuses to configure if they do.

The three programs are built from the same objects: `c-backupd` (the
daemon, `src/daemon/main.c`), `c-backup` (the thin HTTP client,
`src/cli/main.c`) and `c-backup-helper` (the privileged sidecar,
`src/helper/helper_main.c`). `src/virt` links into the helper alone.

| Path | Purpose | Key files |
|------|---------|-----------|
| `src/daemon/` | The daemon's entry point: configuration, privilege drop, database open and migrations, start-up repairs, then the API and the job workers | `main.c` |
| `src/cli/` | `c-backup`, the thin client: argument whitelists per command, HTTP over the unix socket or TCP, SSE job watching | `main.c` (command table), `args.c`, `http.c`, `sse.c` |
| `src/api/` | The HTTP server and its middleware, lifted from airies-ups: routing (longest match), session auth and login throttling, the NBD export server | `server.c`, `auth.c`, `login_throttle.c`, `nbd_server.c` |
| `src/api/routes/` | One file per resource family; each registers its routes and declares the body members it accepts | `repos.c`, `snapshots.c`, `backup_routes.c`, `units.c`, `metadata.c`, `schedules.c`, `introspect.c`, `exports.c`, `databases.c`, `db_actions.c`, `metrics.c`, `health.c`, `stream.c`, `json_row.c`, `paging.c` |
| `src/scheduler/` | When work runs: calendar triggers, the schedule loop, and the job manager (queue, per-repo dispatch lock, durable job ends) | `job_manager.c`, `scheduler.c`, `trigger.c` |
| `src/ops/` | The operations a job performs on a repository: scan, backup, restore, verify, reindex, gc, prune and GFS retention, replication, grep, and the index bookkeeping behind them | `backup.c`, `restore.c`, `scan.c`, `tree_build.c`, `verify.c`, `reindex.c`, `gc.c`, `prune.c`, `gfs.c`, `replicate.c`, `replica_local.c`, `store_index.c`, `pack_presence.c`, `index_startup.c` |
| `src/ops/` (databases) | Database detection and engine copies for plain file sources, and source snapshot mode | `db_detect.c`, `db_findings.c`, `db_capture.c`, `sqlite_copy.c`, `source_snapshot.c` |
| `src/ops/` (pipeline helpers) | Change detection, worker pools for small and large files, reading a file as it is rather than as scanned | `file_cache.c`, `small_files.c`, `store_pool.c`, `settle.c`, `open_scanned.c` |
| `src/store/` | The repository format below the index: objects, content-defined chunking, chunk lists, packs and their parity, manifest trees, encryption. `store.c` is the engine boundary | `store.c`, `pack.c`, `object.c`, `object_crypt.c`, `cdc.c`, `chunklist.c`, `manifest.c`, `parity.c`, `pack_ids.c`, `pack_writeback.c` |
| `src/meta/` | Per-repository metadata outside the object store: the `.cbackup` marker, per-repo locks, the repository key, snapshot-id high-water mark, tags, purge, and reconciling the index with `snapshots/` | `repo.c`, `lock.c`, `repo_key.c`, `snapshot_ids.c`, `repo_tags.c`, `repo_purge.c`, `index_reconcile.c` |
| `src/common/` | Shared infrastructure: status codes and the error buffer, the one database handle (transactions, guarded writes, readers, checkpointer, WAL durability and health, id reservation), the storage sync latch, privilege drop, snapshot records, node index generations | `types.h`, `error.c`, `db_txn.c`, `db_write.h`, `db_readers.c`, `checkpointer.c`, `wal_sync.c`, `wal_health.c`, `walidx.c`, `id_reserve.c`, `state_db.c`, `sync_latch.c`, `privs.c`, `snap_record.c`, `node_gen.c`, `read_budget.c` |
| `src/config/` | Configuration keys, their defaults, and the AppGuard builder that opens the database and runs migrations | `app_config.h`, `app_config.c` |
| `src/providers/` | The source-provider interface for units (VMs, containers, LXC): the registry, the remote provider that speaks to the helper, the fake provider for tests, disk streams and the NBD client | `provider.c`, `registry.c`, `remote_provider.c`, `fake_provider.c`, `disk_stream.c`, `nbd_client.c`, `unit_probe.c`, `artefact.c` |
| `src/units/` | Unit backup and restore in the daemon: assembling a unit snapshot, block sources, incremental tokens, the unit index, holds and their watchdog, restoring a unit's definition | `unit_backup.c`, `unit_restore.c`, `unit_define.c`, `unit_state.c`, `unit_tokens.c`, `unit_index.c`, `unit_detect.c`, `unit_json.c`, `block_source.c` |
| `src/helper/` | `c-backup-helper`, the privileged sidecar: its protocol, policy and server, the Docker and LXC backends, host tools, ZFS/btrfs capture snapshots, mount classification, process probes and database templates | `helper_main.c`, `helper_server.c`, `helper_proto.c`, `helper_policy.c`, `docker_backend.c`, `docker_api.c`, `lxc_backend.c`, `paths_backend.c`, `snap.c`, `mounts.c`, `host_tool.c`, `proc_probe.c`, `db_template.c`, `hooklog.c` |
| `src/virt/` | The libvirt provider, compiled with libvirt's and libxml2's flags into the helper alone, and left out when their development files are absent | `libvirt_backend.c` |

Outside `src/`: `tests/` holds every suite (C unit and fault suites, the
shell end-to-end suites, fuzzers under `tests/fuzz/`), `bench/` the
benchmark harnesses with results in `PERF.md`, `migrations/` the numbered
schema migrations embedded into the daemon, `ci/` the pipeline's scripts and
baselines, `ui/` the Electron app, and `docs/` the design references.
