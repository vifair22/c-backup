# c-backup-helper: the host helper

`c-backup-helper` is the privileged half of unit backup (option B of
`source-providers.md` section 11, decided 2026-11-16). It holds what is
root-equivalent on the host (sockets, devices, capabilities, the host's own
`zfs`, `lxc-*` and libvirt) and offers the daemon a fixed list of typed
operations over a unix socket. The daemon, which faces the network, holds
none of it.

What this build has: the protocol, the policy, the leases and watchdogs, the
peer check, fd passing for dump hooks, restore staging and swap, and two
backends: the fake provider (tests only) and `dir_domains` (a directory
"snapshot domain" for tests and for `capture.begin_paths`). libvirt, Docker
and LXC operations are validated and held to the policy and then answered
`unsupported` until those providers exist.

## Deployment (Docker, Unraid)

One image carries both binaries. Two containers run from it.

```sh
# The helper: privileged, no network, the host's pid namespace for nsenter.
docker run -d --name c-backup-helper --restart unless-stopped \
  --privileged --pid=host --network=none \
  -v /boot/config/c-backup-helper.ini:/etc/c-backup-helper.ini:ro \
  -v /mnt/cache/system/c-backup-helper:/mnt/cache/system/c-backup-helper \
  -v /run/c-backup-helper:/run/c-backup-helper \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --label net.cbackup.role=helper \
  --entrypoint /usr/local/bin/c-backup-helper \
  c-backup:latest --policy /etc/c-backup-helper.ini

# The daemon: starts as root with only the capabilities it needs, drops to
# uid 990 itself, and sees the helper's socket and nothing of the host's.
docker run -d --name c-backupd --restart unless-stopped \
  --cap-drop ALL \
  --cap-add DAC_READ_SEARCH --cap-add DAC_OVERRIDE --cap-add CHOWN \
  --cap-add FOWNER --cap-add FSETID --cap-add MKNOD \
  --cap-add SETUID --cap-add SETGID --cap-add SETPCAP \
  -e C_BACKUP_SECURITY_RUN_AS_UID=990 \
  -e C_BACKUP_UNITS_HELPER_SOCKET=/run/c-backup-helper/helper.sock \
  -v /run/c-backup-helper:/run/c-backup-helper \
  -v /mnt/user/appdata:/mnt/user/appdata:ro,rslave \
  -v /mnt/disk3/backups:/repos \
  -v /mnt/cache/appdata/c-backupd:/var/lib/c-backupd \
  -p 8765:8765 \
  c-backup:latest
```

Compose, equivalently:

```yaml
services:
  c-backup-helper:
    image: c-backup:latest
    entrypoint: ["/usr/local/bin/c-backup-helper", "--policy", "/etc/c-backup-helper.ini"]
    privileged: true
    pid: host
    network_mode: none
    restart: unless-stopped
    volumes:
      - /boot/config/c-backup-helper.ini:/etc/c-backup-helper.ini:ro
      - /mnt/cache/system/c-backup-helper:/mnt/cache/system/c-backup-helper
      - /run/c-backup-helper:/run/c-backup-helper
  c-backupd:
    image: c-backup:latest
    depends_on: [c-backup-helper]
    restart: unless-stopped
    cap_drop: [ALL]
    cap_add: [DAC_READ_SEARCH, DAC_OVERRIDE, CHOWN, FOWNER, FSETID, MKNOD, SETUID, SETGID, SETPCAP]
    environment:
      C_BACKUP_SECURITY_RUN_AS_UID: "990"
      C_BACKUP_UNITS_HELPER_SOCKET: /run/c-backup-helper/helper.sock
    volumes:
      - /run/c-backup-helper:/run/c-backup-helper
      - /mnt/user/appdata:/mnt/user/appdata:ro,rslave
      - /mnt/disk3/backups:/repos
      - /mnt/cache/appdata/c-backupd:/var/lib/c-backupd
    ports: ["8765:8765"]
```

Notes that matter:

- **The policy lives on the flash** (`/boot/config/`), root-owned, mode 0600
  or 0644. The helper refuses to start on a file anybody but its owner can
  write, an unknown section or key, or a value out of range. Unraid's root
  filesystem is RAM, so nothing installed on the host survives a reboot; the
  sidecar is the install, autostarted by Docker.
- **The helper's state directory is on a pool**, never on the RAM rootfs:
  `holds.json` records every hold it has taken, and its next start releases
  what a crash left held.
- **The socket directory** `/run/c-backup-helper` should be `0750
  root:990`; the helper creates the socket `0660`. On every connection the
  helper reads `SO_PEERCRED` and refuses any uid but the policy's
  `daemon_uid`, so the socket is safe to bind-mount into the daemon and
  nowhere else.
- **Why SETUID, SETGID and SETPCAP on the daemon.** Docker gives a non-root
  `--user` none of the capabilities added with `--cap-add`, so the daemon
  starts as root and drops itself (`security.run_as_uid`). The drop needs
  those three for a moment: SETPCAP to empty the bounding set of everything
  but the six, SETGID and SETUID to change ids. Once it returns, none of the
  three is in any set, `no_new_privs` is on, and only
  `CAP_DAC_READ_SEARCH` is effective; restores raise the other five on their
  own thread and drop them after (`src/common/privs.h`). This is a
  correction to section 11.6, which listed the six alone: without
  SETUID/SETGID the drop cannot happen at all.
- **Ownership.** The state volume and repositories must be owned by 990:
  AppGuard opens the config and `state.db` before the drop, so a database
  created as root would be unwritable by the daemon after it.
- **Start order** does not matter, and neither does a helper restart. The
  daemon's link to the helper retries with a bounded backoff (0.5 s
  doubling to 30 s) while the helper is away, says hello again when it
  answers (logging a policy whose digest changed), and enables its
  providers then, with no daemon restart. While connected it checks every
  15 s. A request that finds the helper gone marks the link down at once.
- **A unit job while the helper is away fails and alerts**, scheduled or
  not, with the socket path and the reason in the error, the journal and
  the push alert; it is never skipped quietly. A job in flight when the
  helper restarts fails cleanly (no snapshot, the helper's own start-up
  sweep releases what the old process held), and the next one reconnects.
  `GET /providers` shows the link (`helper.state`: `connected`,
  `disconnected` or `never-connected`, `since`, `last_error`,
  `reconnects`) and, per provider behind it, `connection.state`.

## The policy

```ini
[helper]
daemon_uid = 990              # the only peer uid accepted (non-root)
daemon_id = 7f3a09c1          # the daemon id artefacts are named with
max_pause_s = 30              # watchdog bound on freezes and pauses
max_captures_per_hour = 60    # rate limit, captures of either kind
capture_max_s = 86400         # lease: a capture nobody ends is destroyed
allow_restore = 1             # restore.stage, restore.swap, unit.stop/start
allow_define = 0
allow_privileged_define = 0
state_dir = /mnt/cache/system/c-backup-helper
socket = /run/c-backup-helper/helper.sock
# fake_spec = /path/units.json   (tests only: enables the fake provider)

[units]
allow = docker:compose:immich, docker:cbk-uat-*, libvirt:cbk-uat-*, lxc:cbk-uat-*

[snapshot]
zfs_domains = cache/appdata, cache/Photos
btrfs_domains =
dir_domains =                  # directory domains (tests, begin_paths)

[hook immich-pg_dumpall]
unit = docker:compose:immich
kind = dump                    # dump: stdout comes back as a pipe fd; run: discarded
phase = pre
container = immich_postgres
argv.0 = pg_dumpall
argv.1 = --clean
timeout_s = 900
on_fail = continue
```

Allow globs match `<provider>:<id>` and `<provider>:<name>`, the name from
the helper's own inspect. `c-backup-helper --policy PATH --check` validates a
policy and prints its SHA-256, the digest the helper's `hello` reports and
`GET /providers` shows.

## The protocol

`AF_UNIX`, `SOCK_SEQPACKET`, one request and one response per packet of at
most 64 KiB, each one UTF-8 JSON object (`source-providers.md` 11.2):

```json
{"v":1,"id":17,"op":"capture.begin","args":{"units":["fake:u1"],"pause":true}}
{"id":17,"ok":true,"result":{"capture":"<32 hex>","members":[...],"snapshot_ms":3,"expires":...}}
{"id":17,"ok":false,"error":{"code":"denied","msg":"unit fake:prod is not in the policy"}}
```

The first request on a connection must be `hello`. Error codes are the
closed set `denied`, `invalid`, `busy`, `timeout`, `failed`, `unsupported`.
An operation outside the list is `denied`; a request with an unknown or
missing key, a wrong type, an out-of-range value, a malformed unit key or an
unclean path is `invalid` before any operation sees it. The operations are
the 19 of section 11.3 (`hello`, `units.list`, `units.describe`,
`capture.begin`, `capture.begin_paths`, `capture.end`, `hook.run`,
`hook.wait`, `vm.backup_begin`, `vm.backup_end`, `vm.checkpoint_redefine`,
`vm.install_state`, `restore.stage`, `restore.swap`, `unit.stop`,
`unit.start`, `unit.define`, `lease.release`, `sweep`).

The daemon (`src/providers/remote_provider.c`) opens one connection per
request, says hello, sends it, and closes, so concurrent jobs never queue
behind each other and a helper restart costs nothing. Data never crosses the
socket: the daemon reads trees through the read roots the helper returns,
and disks through its own mounts (for the fake, its run map).

## What a compromised daemon gets

Exactly section 11.5's table. `tests/test_helper.c` asserts it: with every
operation spoken correctly, only allowlisted operations on units and domains
the policy allows succeed; tokens it did not get from the helper are
`invalid`; a sweep it asks for never removes an artefact the helper cannot
prove is its own.

## Configuration blobs

`units.describe` does not carry BLOB bytes in its JSON. The helper writes
every BLOB of the unit into one memfd, seals it (`F_SEAL_SHRINK`,
`F_SEAL_GROW`, `F_SEAL_WRITE`, `F_SEAL_SEAL`) and passes it with the
answer; each BLOB item names `data_off` and `data_len` in it, and the
answer carries `blob_bytes`. The daemon refuses a range outside the memfd.
One BLOB may be up to 16 MiB (`HELPER_BLOB_MAX`), which covers a
container's inspect JSON, an LXC config and UEFI NVRAM alike.

## Docker and LXC providers

Enabled by the policy, never by the daemon (`docs/container-backup.md`):

```ini
[snapshot]
zfs_domains = cache/appdata, cache/system, cache/Photos, cache/lxc
btrfs_domains = /mnt/cache                 ; FS2: the pool's top level
btrfs_snapshot_dir = /mnt/cache/system/.cbackup-snapshots

[host]
nsenter = 1                ; zfs, btrfs, lxc-* run as the host's binaries
host_root = /proc/1/root   ; how the helper reads host paths itself
; zfs = /usr/sbin/zfs, btrfs = /sbin/btrfs, realpath = /usr/bin/realpath,
; lxc_dir = /usr/bin, user_root = /mnt/user, mnt_root = /mnt,
; boot_config = /boot/config (all defaults)

[docker]
enabled = 1
; socket = /var/run/docker.sock
; include_roots = /mnt/cache/appdata, /mnt/user/appdata
; pause = auto             ; auto | always | never
; pause_always = compose:gitlab_server
; never_pause = adguard*
; pause_timeout_s = 5
; templates_dir = /boot/config/plugins/dockerMan/templates-user

[lxc]
enabled = 1
; path = /mnt/cache/lxc

[hook pg-flush]
unit = docker:compose:immich
container = immich_postgres
argv.0 = psql
argv.1 = -c
argv.2 = CHECKPOINT
phase = pre
on_fail = continue
```

What a capture of one unit does, in order:

1. Members are found by the `com.docker.compose.project` label (unit
   `compose:<project>`) or by name. Each member's inspect is stored
   verbatim, with its image record (`Id`, `RepoTags`, `RepoDigests`), its
   Unraid template and the project's compose files and `.env`, under
   `.cbackup-unit/config/`. `mounts.json` lists every mount with its
   outcome: `snapshot`, `fallback-live` or `excluded:<reason>`
   (`system`, `remote-volume`, `outside-include-roots`).
2. Each included mount is resolved to where its bytes are (`/mnt/user` to
   the one pool or disk holding the share, symlinks in the host's
   namespace) and to its snapshot domain: the dataset at the longest
   mountpoint if the policy lists it (never a parent dataset), or a btrfs
   pool root. A mount in no domain is read live and the unit's level is
   `inconsistent`.
3. Pre hooks run through `docker exec` (`lxc-attach` for LXC), live, one
   at a time; `on_fail = fail` fails the unit before anything is held.
4. Docker: a pause only when it adds something, which under `pause = auto`
   means more than one snapshot instant (several pools, several btrfs
   domains); `pause_always` units always; never the helper or daemon
   (label `net.cbackup.role`) or `never_pause` names. LXC: a running
   container is always frozen. Each pause or freeze is journalled first.
5. `syncfs` on each domain, then one `zfs snapshot -o
   com.cbackup:owner=<daemon id>` per pool, or `btrfs subvolume snapshot
   -r` into `btrfs_snapshot_dir`, each journalled before it runs. The
   btrfs directory is refused inside an exported share or at the top of a
   pool.
6. Unpause or thaw, post hooks, and the daemon reads from
   `<mountpoint>/.zfs/snapshot/<name>/...` or `<snapdir>/<name>/...`.
7. `capture.end` destroys each snapshot after proving it ours (the name
   grammar and the owner property; for btrfs read-only, the name and the
   parent uuid), and the sweep does the same for leftovers of dead jobs.

The daemon needs the pools mounted read-only with `rslave`
(`-v /mnt/cache:/mnt/cache:ro,rslave`) so the `.zfs` automount and the
btrfs snapshot directory are visible to it; the file scanner never enters
`.zfs` or `.cbackup-snapshots` itself, so a file backup of a pool does not
read capture snapshots.

## Testing

The unit suites run everything above against stand-ins: `tests/fake_host`
holds scripts standing in for the host's `zfs`, `btrfs` and `lxc-*` (a
snapshot is a copy, properties live in a state file), and
`tests/fake_docker.h` is an Engine API on a unix socket. `test_snap`,
`test_mounts`, `test_docker` and `test_lxc` need no privileges and run in
the ordinary CI job.

A real-Docker CI job is not added. The runners are Debian containers on a
docker executor; a Docker-in-Docker service needs a privileged runner, and
neither ZFS (a kernel module) nor btrfs snapshots of a real pool can be
exercised inside one in any case. The real paths are covered by UAT.

## UAT: real ZFS (FS1) and btrfs (FS2)

Disposable containers only, `units.allow = docker:cbk-uat-*` in the
daemon and the same globs in the helper's `[units] allow`. Run each step
on the host named, from a shell that can reach both containers.

**FS2 (btrfs, no subvolumes).** Policy: `btrfs_domains = /mnt/cache`,
`btrfs_snapshot_dir = /mnt/cache/system/.cbackup-snapshots`, `[docker]
enabled = 1`, include roots `/mnt/cache/appdata`.

1. Create the fixtures:
   `docker run -d --name cbk-uat-pg -e POSTGRES_PASSWORD=x -v
   /mnt/cache/appdata/cbk-uat-pg:/var/lib/postgresql/data postgres:16`;
   `docker run -d --name cbk-uat-sqlite -v
   /mnt/cache/appdata/cbk-uat-sqlite:/data alpine sh -c 'apk add sqlite;
   while :; do sqlite3 /data/t.db "create table if not exists t(x);
   insert into t values(random());"; done'`; a two-service compose
   project `cbk-uat-compose` with both services binding
   `/mnt/cache/appdata/cbk-uat-compose/<svc>`.
2. Start a pgbench load in `cbk-uat-pg`, then
   `c-backup unit backup <repo> --provider docker cbk-uat-pg`. Expect
   `completed`, level `crash`, `consist_flags` with FS_SNAPSHOT and
   without PAUSED (one btrfs domain is one instant), and the job's
   `snapshot_ms` in `unit.json`.
3. While it runs, `ls /mnt/cache/system/.cbackup-snapshots` shows one
   `cbk-<d8>-<seq>-0`; after it, none. `btrfs subvolume list /mnt/cache`
   shows no leftover.
4. `docker kill c-backup-helper` while a capture is held: the job fails
   cleanly if it still needed the helper, and is kept if its read had
   finished; `docker start c-backup-helper` then reports its sweep
   destroying the snapshot, and the next backup succeeds without a daemon
   restart.
5. Restore `cbk-uat-pg`'s mount to a scratch path and start a throwaway
   PostgreSQL on it: it recovers; `pg_amcheck --all` passes. SQLite:
   `PRAGMA integrity_check` says `ok`. The compose project:
   `docker compose up` on the restored files comes up.

**FS1 (ZFS), after the user approves disposable containers there.**
Policy: `zfs_domains = cache/appdata`. Repeat steps 1, 2, 4 and 5 under
`/mnt/cache/appdata/cbk-uat-*`; expect one `zfs snapshot` per capture
(`zpool history cache | tail`), the snapshot owned by the daemon id
(`zfs get com.cbackup:owner`), and none left after the job. Then:

6. Add a second dataset (`cache/Photos`, or a scratch dataset) to one
   fixture's mounts and the policy: the capture now pauses (two pools, or
   check that one pool stays unpaused) and `pause_ms` is reported.
7. U3: from a container binding `/mnt/cache` read-only, check whether
   `/mnt/cache/appdata/.zfs/snapshot/` lists the capture while a job runs,
   and whether `/mnt/user/appdata/.zfs` resolves through shfs.
8. That the daemon container sees `.zfs/snapshot/<name>` through its
   `rslave` mount. If not, record it: the helper would have to mount the
   snapshot explicitly (not built).

**LXC (FS1).** A disposable `cbk-uat-lxc` (busybox template) under
`/mnt/cache/lxc`, policy `[lxc] enabled = 1`, `zfs_domains` including
`cache/lxc`: back it up running (expect `lxc-freeze`/`lxc-unfreeze` in the
helper log, level `crash`) and stopped (`offline`), then restore the
rootfs to a scratch path and compare.

Exit: every restore verified; zero leftover snapshots, pauses, freezes or
journal rows after every fault (`GET /providers` `holds_open` 0, helper
`holds.json` empty).

## Known limits in this build

- `capture.begin_paths` and `restore.stage` over `dir_domains` take no real
  snapshot.
- `pause: false` is passed to the backend as the wanted level; the fake
  pauses whenever the unit can. The Docker provider decides from its
  policy (`pause`, `pause_always`) instead.
- Not built yet (container-backup.md): the fallback's second, paused pass
  (`pause-read`; a non-snapshot mount is read live and flagged
  `inconsistent`); `dump` hooks streamed into `hooks/<name>.dump` and hook
  logs stored as files (an `exec` hook's tail goes to the session's
  warnings); restore `define` (Engine API create, digest pull with tag
  fallback, templates, compose up) and the LXC define; enrolment
  (per-unit exclusions, pause scope `group`/`job`); btrfs nested
  subvolumes; members paused in parallel.
- `restore.stage` and `restore.swap` act on host paths directly, so for a
  container provider they need the helper to see the pools at their host
  paths (bind them in as well as using `host_root`).
