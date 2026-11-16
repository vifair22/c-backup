# Container backup (Docker and LXC)

Design document. Nothing here is built yet. It is the Docker and LXC
providers for the source-provider interface in `source-providers.md`,
which defines the vtable, consistency levels, session lifecycle, intents
and sweep, the repository representation, retention groups and the common
API. This document covers what is specific to containers.

Host facts are from the read-only survey of FS1 and FS2,
`unraid-infra/fs2/c-backup-uat/SURVEY-sources.md` (2026-11-16), cited as
"survey".

## FORMAT IMPACT

**None.** A container snapshot is an ordinary format 3 snapshot. The
container's data (bind mounts, named volumes, an LXC rootfs) is files and
directories recorded at their real host paths, exactly as a file backup
records them, so any format 3 build restores it by path. The definition
(`docker inspect`, image digests, the Unraid template, compose files, the
LXC config), hook logs, hook dumps and `unit.json` (the consistency level
and the per-mount capture method) are files under the reserved
`.cbackup-unit/` directory. Per-container history and retention are
derived into `state.db` from that directory by reindex. The one
prerequisite is the `.cbackup-unit` reservation in file backups, a
behaviour change, not a format change (`source-providers.md` section 7).

## 1. Scope and user decisions

| Decision | Choice |
|----------|--------|
| Docker and LXC | Both 1.0, behind the same provider interface |
| Consistency | Pre hooks (live), then pause, filesystem snapshot, unpause, post hooks; the backup reads from the snapshot while the workload runs |
| Filesystems | ZFS and btrfs first: FS1's cache is ZFS, FS2's is btrfs (survey section 2). Other filesystems get a fallback (section 7), flagged for the user |
| Images | Not backed up. The definition is, and restore pulls the image |
| Definitions | `docker inspect` output, plus the compose files or the Unraid template (`/boot/config/plugins/dockerMan/templates-user/my-*.xml`) where present |
| LXC | Unraid's LXC plugin (plain LXC), driven through the `lxc-*` tools, not liblxc |
| Testing | Disposable test containers only; `units.allow` enforces it (`source-providers.md` 8.3) |

## 2. What the hosts actually have

From the survey, the facts that shape this design:

- **Docker uses overlay2 on a directory** on both hosts
  (`/var/lib/docker` = `/mnt/cache/system/docker`), not the zfs or btrfs
  storage driver. There are no per-container or per-layer datasets or
  subvolumes. Images and layers are re-pullable. State worth keeping is in
  **bind mounts**, almost all under `/mnt/cache/appdata`, plus three named
  volumes (two on FS1 in `cache/system`, one NFS volume on FS2).
- **FS1** (production): cache pool is ZFS, 3 x 2-way mirror, 17 datasets.
  `cache/appdata` is **one dataset** of 506 GB holding nearly every
  container's state. Immich's library is in `cache/appdata` with photos in
  `cache/Photos`; named volumes are in `cache/system`. 33 containers (27
  running); compose projects `Immich`, `RXR` and `gitlab-server`.
- **FS2**: cache pool is btrfs RAID10 with **no subvolumes at all**;
  `appdata` is a plain directory in the top-level subvolume. Every container
  on FS2 is c-backup UAT; nothing there is a production workload.
- **LXC**: FS1 only, two stopped containers (DevBox, StoneBlock),
  `dir` backing inside the single dataset `cache/lxc`.
- **Today's backup**: the Appdata Backup plugin, Mondays 05:00 on FS1,
  stops each container and tars its appdata to `/mnt/user/UnRaid Backup/`.
  GitLab was down about 40 minutes this morning because of it.

## 3. Units

### 3.1 The compose project is the unit

A compose project is one application split across containers, and only a
capture of all its containers at one instant is consistent:
`gitlab_server` is GitLab plus OpenSearch plus two Zoekt containers sharing
an index; `immich` is the server, PostgreSQL, the ML worker and Valkey.
Backing up GitLab's data at one instant and OpenSearch's at another gives
an index that disagrees with the database it indexes.

So the Docker provider's units are:

| Unit | `unit_id` | When |
|------|-----------|------|
| compose project | `compose:<project>` (the `com.docker.compose.project` label) | every container carrying the label |
| standalone container | `<container name>` | every other container |

A compose unit's items are the union of its containers' mounts, and its
config blobs include every member's inspect. Restore granularity still goes
down to one container, one mount or one file (section 11).

`--all` covers both kinds. A container that joins or leaves a project
between sessions changes the unit's item set; that is an ordinary item
addition or removal.

### 3.2 Never paused

- the c-backupd container itself and the helper (section 8), found by their
  own container id (from `/proc/self/cgroup` and the label
  `net.cbackup.role`);
- any container in `containers.never_pause` (globs), for example a DNS
  server whose seconds of silence the user does not want. Such a unit is
  still backed up from a snapshot (crash-consistent for a single dataset,
  section 6.4), just not paused.

### 3.3 LXC units

One unit per container under the LXC path: `lxc-ls -P <lxcpath> --line`,
state from `lxc-info -P <lxcpath> -n <name> -s`. On FS1 the path is
`/mnt/cache/lxc` (dataset `cache/lxc`).

## 4. What is captured

### 4.1 The definition

Docker, through the Engine API over the unix socket (libcurl is already
linked for Pushover; `CURLOPT_UNIX_SOCKET_PATH` makes it a Docker client,
with no `docker` CLI in the image), API version negotiated via `/version`:

| File in `.cbackup-unit/config/` | Source |
|---|---|
| `containers/<name>/inspect.json` | `GET /containers/<id>/json`, verbatim |
| `containers/<name>/image.json` | `GET /images/<image id>/json`: `RepoTags`, `RepoDigests`, `Id` |
| `containers/<name>/unraid-template.xml` | `/boot/config/plugins/dockerMan/templates-user/my-<name>.xml`, when it exists |
| `compose/<file>` and `compose/.env` | the files named by `com.docker.compose.project.config_files`, and `.env` in `com.docker.compose.project.working_dir` (on FS1 under `/mnt/cache/appdata/<project>`) |

Secrets: the survey notes some FS1 templates carry secrets (fleet-derper,
tailsync), and inspect output contains every environment variable. They are
stored, because a restore needs them; into an unencrypted repository with a
warning on every snapshot that carries them (`definition contains
environment values; repository is not encrypted`).

LXC: `<lxcpath>/<name>/config`, plus any `lxc.include` file under the LXC
path (system includes under `/usr/share/lxc` are recorded by path only).
The plugin's own settings (`/boot/config/plugins/lxc/*.cfg`) are captured
once per unit as context.

### 4.2 The data: classifying every mount

For each mount of each container (`Mounts` in inspect) or each
`lxc.mount.entry` and the rootfs:

| Mount | Default | Capture |
|---|---|---|
| bind under an include root (`containers.include_roots`, default `/mnt/cache/appdata,/mnt/user/appdata`) | included | snapshot if its real path is on ZFS/btrfs, else fallback |
| named volume (`/var/lib/docker/volumes/<v>/_data`) | included | snapshot (FS1: in `cache/system`) |
| NFS or other remote volume driver | excluded: `remote-volume` | |
| bind outside the include roots (media libraries, `/mnt/user`, whole pools) | excluded: `outside-include-roots` | |
| `tmpfs`, `/dev/*`, `/proc`, `/sys`, `docker.sock`, single host files under `/etc` | excluded: `system` | |
| LXC rootfs (`lxc.rootfs.path = dir:...`) | included | snapshot (FS1: `cache/lxc`) |

Real path resolution, because a bind of `/mnt/user/...` goes through
Unraid's shfs (FUSE) and cannot be snapshotted as such:

```
resolve(/mnt/user/<share>/<rest>):
    candidates = [/mnt/<pool or diskN>/<share>/<rest> that exist]
    exactly one, on a pool        -> that path (snapshot-able)
    exactly one, on an array disk -> that path (xfs: fallback)
    several (share split across pool and array, mover in progress)
                                  -> fallback for this mount, warning 'share-split'
    none                          -> the mount source is missing: unit fails
```

Symlinks are resolved too (`/mnt/user/domains` is a symlink to
`../cache/domains` on FS1). The **recorded** path is always the source as
the container binds it, so a plain file restore puts data back where
Docker expects it; the read path is the snapshot's copy of the resolved
path.

Per-unit exclusions use the existing pattern syntax (Manual section 6.2),
applied to recorded paths. Suggested defaults for the FS1 workloads,
shipped as examples, not applied automatically:

| Unit | Exclude | Why |
|---|---|---|
| `compose:immich` | `/mnt/cache/appdata/Immich/ml_cache` | 11 GB of downloaded models, re-created on start |
| `compose:gitlab_server` | `/mnt/cache/appdata/gitlab-ee-opensearch/data` (optional) | 27 GB search index GitLab can rebuild; keeping it avoids a long reindex after restore |
| `Plex-Media-Server` | `**/Plex Media Server/Cache`, `**/Codecs` | re-creatable |

Every mount appears in `unit.json` and in the UI with its outcome:
`snapshot`, `fallback-paused`, `fallback-live`, or `excluded:<reason>`.

### 4.3 Where it lands

```
.cbackup-unit/unit.json
.cbackup-unit/config/...                 (4.1)
.cbackup-unit/hooks/<hook>.log           stdout+stderr, at most 1 MiB each
.cbackup-unit/hooks/<hook>.dump          dump hooks' stdout, any size (6.6)
mnt/cache/appdata/gitlab-ee/config/...   data at recorded paths
mnt/cache/appdata/gitlab-ee/data/...
```

## 5. One session, step by step

```mermaid
sequenceDiagram
  participant J as unit job
  participant H as provider (helper)
  participant D as dockerd
  participant F as ZFS / btrfs
  J->>H: begin(unit)
  H->>D: inspect members, classify mounts
  H->>D: pre hooks (docker exec, live)
  H->>F: syncfs on each snapshot domain (pre-flush, outside the pause)
  H->>H: intents 'pause' + 'fs_snapshot'
  par every member
    H->>D: POST /containers/<id>/pause
  end
  H->>F: syncfs (residual)
  H->>F: one atomic snapshot of every domain the unit touches
  par every member
    H->>D: POST /containers/<id>/unpause
  end
  H->>H: drop intent 'pause'
  H->>D: post hooks (docker exec)
  H-->>J: session: level, read roots per mount
  J->>J: scan + store from the snapshot (workload running)
  J->>H: end(COMMITTED)
  H->>F: destroy the snapshot; drop intent 'fs_snapshot'
```

The only time the workload is affected is between the first pause and the
last unpause.

## 6. Consistency

### 6.1 The pause window, quantified

| Step | Typical | Bound | Notes |
|---|---|---|---|
| pre-flush `syncfs` | outside the window | | writes back what is dirty now, so the in-window flush is small |
| `pause` (cgroup v2 freeze), members in parallel | 10-200 ms | `containers.pause_timeout_s` (5) | the slowest member dominates; a task in uninterruptible I/O delays freezing |
| residual `syncfs` | under 100 ms | | only what was written since the pre-flush |
| ZFS snapshot (one `zfs snapshot` command, one or more datasets) | 0.1-1 s | | forces a transaction group commit; the cache pool has an SLOG (survey) |
| btrfs snapshot (`BTRFS_IOC_SNAP_CREATE_V2`) | 0.05-2 s | | commits a transaction |
| `unpause`, in parallel | 10-50 ms | | |
| **total** | **about 0.3 to 1.5 s** | `containers.max_pause_s` (30) watchdog | |

These are estimates; UAT measures every one (`pause_ms`, `snapshot_ms` in
`unit.json`) on disposable containers on both hosts.

Against today: GitLab's 40-minute stop becomes a pause of about a second,
three orders of magnitude less, and GitLab keeps its connections and
processes (a pause freezes them; nothing restarts).

### 6.2 One snapshot per unit, or one for many units

With all of FS1's appdata in one dataset, every unit's snapshot is a
snapshot of `cache/appdata` (plus `cache/Photos` and `cache/system` where a
unit uses them). Two ways to take them:

| | Per unit (one snapshot per consistency group) | Shared (pause everything, one snapshot, unpause) |
|---|---|---|
| snapshots per run on FS1 | about 21 (27 running containers: 2 compose projects of 4, 19 standalone), created and destroyed one after another | 1 |
| each container paused | once, about 0.3-1.5 s, at a different moment from the others | once, for the slowest container's freeze plus one snapshot, about 0.5-2 s, all at the same moment |
| a container that will not freeze | delays and degrades only its own unit | delays every container's pause, or forces the whole capture to proceed without it |
| snapshot lifetime | only while that unit is read: minutes | the whole job: the first run of 506 GB at an array disk's 150 MB/s is about an hour |
| space held by snapshots | what that unit's dataset overwrites while it is read | everything appdata overwrites during the whole job (GitLab logs, Plex, Prometheus) |
| consistency across units | none (not needed between unrelated applications) | all units at one instant |
| cost of a snapshot | O(1) on ZFS and btrfs; each `zfs snapshot`/`destroy` call is tens to hundreds of ms of wall time, nobody waits on it except the job | |

**Recommendation: per unit**, where the unit is the compose project or the
standalone container (`containers.pause_scope: unit`, the default). Each
application is affected for about a second, independently; failures stay
local; snapshots are short-lived. `pause_scope: job` gives the shared mode
for users who want every container captured at one instant (for example a
reverse proxy and its certificate manager), and `pause_scope: group` with
an explicit `group` name in enrolment ties chosen standalone containers
together. The implementation is the same either way: a **capture** is a set
of units paused together plus one atomic snapshot per pool; units read from
their capture, and the capture is destroyed after its last unit is read.

### 6.3 What the level means

A paused-and-snapshotted unit is `crash`: every file of every member at one
instant, as after a power cut, with all writes completed before the pause
included (`syncfs` plus the snapshot's own commit). Journalled filesystems
are not involved (the snapshot is below them), and the applications that
matter recover from exactly this: PostgreSQL and SQLite with their WAL or
journal, Prometheus with its WAL, OpenSearch with its translog. A unit
whose pre hooks all exited 0 is `application`. A stopped unit is `offline`.
A unit any of whose included mounts fell back to a live read is
`inconsistent` for those mounts, and the unit's level is the lowest of its
mounts'.

### 6.4 What the pause buys

A single atomic snapshot of one dataset is already a crash-consistent
point in time for everything in it, pause or not. The pause adds:

1. **atomicity across snapshot domains that cannot be snapshotted together**:
   ZFS datasets in different pools, a btrfs snapshot plus anything else,
   and above all the fallback mounts of section 7;
2. a **quiet instant** between hooks and capture, so a pre hook's effect
   (a dump finished, a flush done) is not undone by writes in between;
3. **predictable `syncfs` time**: nothing new is dirtied during the flush.

On FS1, where nearly everything is in `cache/appdata`, (1) rarely applies.
`containers.pause: always` is the default per the user's call;
`pause: auto` (pause only when the unit spans more than one snapshot
domain, or has fallback mounts or hooks that ask for it) is offered as an
option that would make most FS1 units pause-free. Open question C1.

### 6.5 Failure handling

| Failure | Handling |
|---|---|
| a member fails to pause within `pause_timeout_s` | unpause every member already paused, skip the pause, take the snapshot anyway: level `crash` if the unit lives in one atomic snapshot, else the unit fails (or falls back, per `containers.fallback`) |
| snapshot fails | unpause, record the error, fail the unit; no data read |
| watchdog: the window exceeds `max_pause_s` | the watchdog unpauses (from its own thread, or from the helper); if the snapshot completed first, nothing is lost; if not, the unit fails |
| daemon or helper dies holding a pause | the `pause` intent survives; `sweep` at start unpauses (an unpause of a running container is a harmless error). Docker's restart policy brings the process back within seconds. Under the helper option the helper's watchdog does it without waiting for the daemon |
| read fails mid-unit | `end(ABORTED)`: snapshot destroyed, no snapshot record, the next unit proceeds |
| hook fails | per hook `on_fail`: `abort` (unit fails before any pause), `continue` (level drops to `crash`, warning) |

### 6.6 Hooks

Hooks run **outside** the pause: pre hooks while the workload is live,
before the pause; post hooks after the unpause and after the snapshot. A
paused container cannot run anything, and nobody should wait for a dump
inside the window.

Two kinds:

- `exec`: a command run in a member container (`POST
  /containers/<id>/exec` with a fixed argv, a user, a timeout); its output
  is kept as `hooks/<name>.log`, truncated at 1 MiB.
- `dump`: the same, but its **stdout is the backup**: streamed from the
  exec attach stream (Docker's multiplexed frames, stderr split off to the
  log) straight into `cdc_store_source` as `hooks/<name>.dump`. No temporary
  file in the volume, no size limit, memory bounded like any file. A dump
  is consistent by itself (it is the database's own export), at its own
  instant, seconds before the snapshot.

Configured per unit through enrolment, in JSON. Under the host helper
option, hooks are defined in the helper's allowlist and enrolment can only
refer to them by name (`source-providers.md` section 11); under option A,
`units.allow_host_hooks` stays 0 and only `exec`/`dump` in containers exist.

Examples for the FS1 workloads:

```json
{"unit":"docker:compose:immich",
 "exclude":["/mnt/cache/appdata/Immich/ml_cache"],
 "hooks":{"pre":[
   {"name":"pg_dumpall","kind":"dump","container":"immich_postgres",
    "argv":["pg_dumpall","--clean","--if-exists","-U","postgres"],
    "timeout_s":900,"on_fail":"continue"}]}}
```

Immich's PostgreSQL (466 MB of data) is safe from the snapshot alone,
because the whole data directory is captured atomically and PostgreSQL
recovers from a crash-consistent copy. The dump adds a logical backup that
restores into any later PostgreSQL, which matters because Immich pins a
specific PostgreSQL-with-vector-extension image that will change. It is a
few hundred MB, compresses well, and dedups poorly (a dump shifts); a
weekly dump schedule instead of nightly is open question C4.

```json
{"unit":"docker:Grafana",
 "hooks":{"pre":[
   {"name":"sqlite-check","kind":"exec","container":"Grafana",
    "argv":["sh","-c","test -f /var/lib/grafana/grafana.db"],
    "timeout_s":10,"on_fail":"abort"}]}}
```

Grafana's SQLite database is safe from the snapshot (SQLite's journal
recovers it); no dump is needed. The example only shows a guard hook.

```json
{"unit":"docker:prometheus",
 "hooks":{"pre":[
   {"name":"tsdb-snapshot","kind":"exec","container":"prometheus",
    "argv":["wget","-qO-","--post-data=","http://localhost:9090/api/v1/admin/tsdb/snapshot"],
    "timeout_s":120,"on_fail":"continue"}],
  "post":[
   {"name":"tsdb-snapshot-clean","kind":"exec","container":"prometheus",
    "argv":["sh","-c","rm -rf /prometheus/snapshots/*"],
    "timeout_s":60,"on_fail":"continue"}]}}
```

Prometheus's TSDB recovers from a crash-consistent copy through its WAL;
the TSDB snapshot (which needs `--web.enable-admin-api`) is optional
belt-and-braces.

```json
{"unit":"docker:compose:gitlab_server",
 "exclude":["/mnt/cache/appdata/gitlab-ee/log"]}
```

GitLab needs no hook: its PostgreSQL, Redis, Gitaly repositories and
OpenSearch are all in `cache/appdata` and captured together. `gitlab-backup
create` is what made today's downtime long and is not used. Logs (14 GB)
are excluded in the example because they are not needed to restore
service.

## 7. Filesystems that cannot be snapshotted (FLAGGED FOR THE USER)

Array disks (xfs on md), unassigned devices (`/mnt/disks/...`, for example
frigate's `/mnt/disks/NVR`), shares split between pool and array, and any
non-ZFS, non-btrfs filesystem cannot be snapshotted. Options per mount
(`containers.fallback`, overridable per unit):

| Mode | Behaviour | Level | Downtime |
|---|---|---|---|
| **`pause-read`** (proposed default) | two passes: (1) read the mount live, storing content and recording every file in the file cache; (2) pause the unit, rescan the mount and re-read only files whose metadata changed since pass 1, unpause | `crash` | the rescan plus the changed bytes: seconds for a typical appdata tree, since pass 1 stored nearly everything |
| `live` | read live, no pause | `inconsistent` | none |
| `stop` | stop the unit for the read, as the Appdata Backup plugin does | `offline` | the whole read |
| `fail` | refuse the unit | | |

`pause-read` details:

- pass 1's packs are sealed before pass 2, so the file cache's
  object-presence check sees what pass 1 stored;
- the pause in pass 2 is bounded by `containers.max_pause_s`. At the
  deadline the watchdog unpauses and pass 2 finishes live: those mounts are
  recorded `fallback-live`, the level drops to `inconsistent`, and the
  snapshot is kept with a warning, because a flagged backup is better than
  none (`containers.fallback_on_deadline: keep`, or `fail` to discard it);
- snapshot-able mounts of the same unit are still snapshotted in the same
  pause, so the unit's level is `crash` when pass 2 finishes in time.

On FS1 today only opt-in mounts would use it (frigate's NVR, if included);
appdata is all on the ZFS pool. Open question C2: is `pause-read` the right
default, and is keeping a flagged snapshot on deadline right?

## 8. Snapshot mechanics

### 8.1 ZFS (FS1)

- **Domain**: a dataset. The snapshot domain of a path is the dataset
  mounted at the longest mountpoint prefix of its resolved path (from
  `zfs list -H -o name,mountpoint`). FS1's 17 datasets (survey section 2):
  most container data is `cache/appdata`; Immich also touches `cache/Photos`
  and `cache/system`; LXC is `cache/lxc`.
- **Create**: one command per pool, so it is atomic across datasets:
  `zfs snapshot -o com.cbackup:owner=<daemon id> cache/appdata@cbk-<d8>-<job>-<n> cache/Photos@... cache/system@...`.
  Non-recursive: child datasets are added explicitly only when a unit's
  path lies in them.
- **Read**: `<mountpoint>/.zfs/snapshot/<name>/<rest>`. The `.zfs`
  directory is hidden from listings by default but reachable by path, and
  ZFS mounts the snapshot on first access. From inside a container, that
  automount must propagate into the container's view: the pool is bound
  with `rslave` (`-v /mnt/cache:/mnt/cache:ro,rslave`), or the helper
  mounts the snapshot explicitly (`mount -t zfs -o ro <ds>@<snap>
  /mnt/cache/.cbackup-mnt/<name>`). Which works on Unraid 7.4 / ZFS 2.4.4
  is UAT item U3.
- **Destroy**: `zfs destroy <ds>@<name>` only after checking the
  `com.cbackup:owner` property equals this daemon's id. A snapshot with a
  hold or a clone (neither of which c-backup creates) is left alone and
  reported.
- **Sweep**: `zfs list -H -t snapshot -o name,com.cbackup:owner`, filter by
  the name grammar and the owner property, skip those owned by a running
  job, destroy the rest.
- **Exclusion**: `.zfs` is never descended into by the scanner (it is a
  separate mount, and the scanner also excludes it by name at any dataset
  root), so a file backup of `/mnt/cache` never reads snapshots.

### 8.2 btrfs without subvolumes (FS2)

FS2's pool has no subvolumes (survey section 2), so the only snapshot
domain is the top-level subvolume (id 5): the whole pool.

- **Create**: `BTRFS_IOC_SNAP_CREATE_V2` with the source fd on the pool's
  mount point (subvolume 5), `BTRFS_SUBVOL_RDONLY`, into the snapshot
  directory. Snapshotting the top-level subvolume is supported and, like
  any btrfs snapshot, is a new root pointing at the existing tree plus a
  transaction commit: constant time whatever the pool's 1.78 TB holds, no
  data copied, no space used until the live tree diverges. Read-only, so
  nothing can modify it.
- **What it contains**: everything in the pool, except nested subvolumes,
  which appear as empty directories. There are none on FS2 today; if any
  appear, `describe` finds them (a directory whose inode is 256 on btrfs is
  a subvolume root) and snapshots each one a unit's path lies in, in the
  same pause (separate btrfs snapshots are not atomic together, which is
  what the pause is for).
- **Where it goes**: the snapshot must be inside the same filesystem. It
  must **not** be a new top-level directory: Unraid treats top-level
  directories on a pool as shares, so `/mnt/cache/.cbackup-snapshots` could
  surface as a user share, be walked through `/mnt/user`, or worse be
  handed to the mover. The proposed location is
  `/mnt/cache/system/.cbackup-snapshots/` (`containers.btrfs_snapshot_dir`),
  inside an existing pool-only share. Whether Unraid exports or moves dot
  directories at a pool's top level, and confirming `system` is pool-only
  on FS2, is UAT item U4.
- **Read**: `<snapshot dir>/<name>/<path relative to the pool root>`.
- **Destroy**: `BTRFS_IOC_SNAP_DESTROY_V2`, after checking the subvolume is
  read-only, its name matches the grammar with this daemon's id, and its
  `parent_uuid` (from `BTRFS_IOC_GET_SUBVOL_INFO`) is the top-level
  subvolume's uuid. Destroying a subvolume needs `CAP_SYS_ADMIN` unless the
  pool is mounted with `user_subvol_rm_allowed`.
- **Sweep after a crash**: list the snapshot directory, apply the same
  three checks, destroy what no running job owns. The intent row names
  each snapshot before it is created, so a crash between create and the
  read is covered even if the directory listing is the only evidence left.
- **Exclusion**: the scanner never descends into a directory named
  `.cbackup-snapshots` and never crosses into a btrfs subvolume whose name
  matches the grammar; snapshots are separate subvolumes with their own
  `st_dev`, so `--one-filesystem` stops at them as well. Other tools that
  walk `/mnt/cache/system` (indexers, `du`, another backup tool) would see
  a read-only copy of the pool for the minutes a job holds it; that window
  is the cost of no subvolumes.
- **The alternative** is converting `appdata` into a subvolume (stop
  containers, `btrfs subvolume create`, `cp -a --reflink=always`, swap
  names): snapshots then cover appdata only and can live inside it. It is
  an operator step with downtime and is **not** done without the user's
  approval. FS2 carries no production containers, so the top-level design
  is what UAT exercises there.

## 9. Privileges and deployment

### 9.1 Exactly what each piece needs

| Capability or mount | Docker provider | LXC provider | ZFS snapshots | btrfs snapshots | Root-equivalent |
|---|---|---|---|---|---|
| `/var/run/docker.sock` (root:docker 0660, survey) | inspect, pause, unpause, exec (hooks), create and pull (restore) | | | | **yes** |
| `lxc-*` tools from the host, in the host's mount namespace | | list, info, freeze, unfreeze, start (restore) | | | |
| host network namespace | | LXC's command socket is an abstract unix socket in the host netns | | | |
| `/sys/fs/cgroup` write | | freezer | | | |
| `/dev/zfs` (0666 on Unraid, survey) | | | snapshot, destroy, list | | **yes** (with the cap below, over every pool) |
| `CAP_SYS_ADMIN` | | (through the host tools) | ZFS's privilege check for snapshot/destroy; explicit snapshot mounts | subvolume destroy | **yes** |
| `zfs` userland matching kmod 2.4.4 | | | Debian trixie ships an older one (survey); use the host's binary | | |
| pool mounted read-only with `rslave` | | | read `.zfs/snapshot` | read snapshot dir | no |
| pool snapshot dir writable | | | | create snapshot (needs owner-or-`CAP_FOWNER` on the source) | no |
| `DAC_READ_SEARCH` (already in the image's cap set) | read data | read rootfs | | | no |
| restore caps (`DAC_OVERRIDE`, `CHOWN`, `FOWNER`, `FSETID`, `MKNOD`) and read-write mounts | restore data | restore rootfs (device nodes in `/dev`) | | | no |

So under option A (everything in the daemon) the c-backupd container needs
the Docker socket, `/dev/zfs`, `CAP_SYS_ADMIN`, the host's `zfs` binary
(by `nsenter` into the host's mount namespace, which needs `--pid=host`
and `CAP_SYS_PTRACE` as well) and, for LXC, the host network namespace:
in practice `--privileged --pid=host --network=host`. That is the whole
host, behind the daemon's HTTP API.

### 9.2 SECURITY DECISION: the host helper

`source-providers.md` section 11 sets out options A, B and C. For
containers the case for **B** is strongest:

- LXC and host-matching ZFS need host-side execution in any case;
- the Docker socket and `CAP_SYS_ADMIN` are each root on the host;
- the helper can own the pause watchdog, so a crashed daemon cannot leave
  GitLab paused.

**How the helper survives Unraid's RAM rootfs** (survey section 6: nothing
installed outside `/boot` or the pools survives a reboot): as a
**sidecar container**, `c-backup-helper`, from the same image with a
different entrypoint. Docker autostarts it from images on the pool, so it
needs nothing installed on the host:

```sh
docker run -d --name c-backup-helper --restart unless-stopped \
  --privileged --pid=host --network=none \
  --label net.cbackup.role=helper \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /var/run/libvirt/libvirt-sock:/var/run/libvirt/libvirt-sock \
  -v /run/c-backup-helper:/run/c-backup-helper \
  -v /boot/config/c-backup-helper.ini:/etc/c-backup-helper.ini:ro \
  <image> c-backupd --helper
```

It runs host tools as `nsenter -t 1 -m -- zfs ...` and
`nsenter -t 1 -m -n -- lxc-freeze ...`, so it always uses the host's own
`zfs` 2.4.4 and `lxc-*`, with no version skew. `--network=none` means it
has no network listener at all; its only interface is the socket in
`/run/c-backup-helper`, mode 0600. Its policy file on the flash names the
units, datasets, subvolumes and hook commands it will act on; anything else
is refused whatever the daemon asks. The alternative packaging, an Unraid
plugin (`.plg`) that installs a static binary at boot and starts it on the
array-started event, is more Unraid-native but more to build.

The main daemon then runs with the image's existing cap set
(`DAC_READ_SEARCH` and the restore caps), read-only pool mounts with
`rslave`, the helper socket and its repository mounts: no Docker socket, no
libvirt socket, no `/dev/zfs`, no `CAP_SYS_ADMIN`.

**Asked of the user:** A or B. Recommendation: B, as the sidecar.

### 9.3 Which host runs what

Snapshots are taken on the host that holds the data, and network transport
is post-1.0, so each host with workloads runs its own daemon:

```mermaid
flowchart LR
  subgraph FS1 [FS1: production workloads]
    D1[c-backupd-fs1] --- H1[c-backup-helper]
    H1 --- DK1[(dockerd)]
    H1 --- LV1[(libvirtd)]
    H1 --- Z1[(ZFS cache pool:<br/>appdata, system, Photos, lxc, domains)]
    D1 -- reads snapshots --> Z1
    D1 --> R1[(repo on an array disk<br/>/mnt/diskN/cbackup)]
  end
  subgraph FS2 [FS2: UAT]
    D2[c-backupd UAT] --- H2[helper UAT]
    H2 --- B2[(btrfs cache pool)]
    D2 --> R2[(UAT repos)]
    N2[(replica target share)]
  end
  R1 -- daily replicate,<br/>over an NFS mount of FS2 --> N2
```

- **FS1** gets a new `c-backupd-fs1` (plus helper) for its containers, LXC
  and VMs (`vm-backup.md` section 9.1). Its repository goes on an FS1 array
  disk by disk path (`/mnt/diskN/...`, not shfs): different devices from the
  cache pool it protects, local `fsync`/`rename`. A daily `replicate`
  schedule copies it to FS2 through an NFS mount, for one off-host copy.
- **FS2** keeps the UAT daemon, which exercises the btrfs path on
  disposable containers.
- **Two daemons never share a repository.** Repository locking is in-process
  (`src/meta/lock.c`); two daemons writing one repository would corrupt it.
  Cross-host dedup therefore means one repository per host; a replica is a
  copy, not a second writer.
- The desktop app connects to both daemons (Manual section 15.1).
- `c-backupd-fs1`'s own state (`/mnt/cache/appdata/c-backupd`) is excluded
  from its own unit backups by default; its encryption keys need a copy off
  the host, as the Manual already requires.

## 10. Replacing the Appdata Backup plugin

One schedule on FS1:

```sh
c-backup schedule add <repo> units --provider docker --all \
    --exclude 'c-backup*' --every day --at 05:00
c-backup schedule add <repo> units --provider lxc --all --every day --at 05:30
c-backup schedule add <repo> gfs --every day --at 07:00
```

| | Appdata Backup plugin (today) | c-backup |
|---|---|---|
| downtime per application | the container is stopped for the whole tar: GitLab about 40 minutes | paused about 0.3-1.5 s (6.1) |
| what is stored | a full tar of each appdata every run | only new chunks; unchanged files are not even read (file cache over snapshots) |
| consistency | offline per container, but a compose project's containers stopped at different times | `crash` per compose project at one instant; `application` with dump hooks |
| run time | as long as tarring 506 GB | the first run is bound by the repository's write speed (about an hour to an array disk); later runs by the scan of changed trees: minutes |
| restore | untar, re-add the container by hand | per file, per mount, per container, per project; definitions re-created, images pulled by digest |
| retention | by count | GFS per unit |

The plugin stays enabled until UAT on disposable containers passes and the
user decides; running both means keeping its downtime meanwhile.

## 11. Dedup and restore granularity

### 11.1 Dedup

- **Across snapshots of a unit**: the file cache, looked up by recorded
  path with device numbers ignored (`source-providers.md` 4.3), skips every
  unchanged file; ZFS and btrfs snapshots preserve inode numbers, ctime and
  mtime, so the remaining fields still prove a file unchanged. Changed large
  files store only changed chunks (SQLite and PostgreSQL pages, Plex
  databases).
- **Across units**: files with the same content anywhere in the repository
  are one set of objects: shared libraries in game servers' appdata,
  identical photos in Immich's library and `cache/Photos`.
- **Immich's library** (290 GB of photos, written once): stored once, then
  only new photos.
- **Dumps** dedup poorly, since a logical dump shifts with every change;
  which is why the dump hook is opt-in (C4).

### 11.2 Restore

| Granularity | How | Provider involvement |
|---|---|---|
| one file or directory | the ordinary restore with `--subpath` from a unit snapshot, to anywhere | none |
| one mount (volume) | `item` mode: to its original path (the unit stopped) or elsewhere | stop check |
| one container of a project | `unit` mode with `containers: [name]`: its mounts and its definition | define for that container |
| a whole unit | every included mount, then the definition | `restore_preflight`, `define` |

`define` for Docker:

1. **Preflight**: name collisions (refused unless `name` or `replace`),
   static IPs and published ports already in use, networks that do not
   exist (custom networks such as `br0` are recorded and must exist; they
   are not created), and privileged settings (`Privileged`, `CapAdd`,
   `Devices`, binds outside the include roots), which refuse the define
   without `accept_privileged` (`source-providers.md` section 11).
2. **Data first**, with the target containers stopped or absent.
3. **Image**: pulled by the recorded digest from `RepoDigests`
   (`POST /images/create?fromImage=<repo>@<digest>`), so the application
   comes back at the version its data was written by: tags such as
   `release`, `beta` and `latest` move, and GitLab in particular requires
   stepwise upgrades. If the registry no longer serves the digest, the
   request must say `allow_tag: true` to pull the tag instead, with a
   warning. Open question C3.
4. **Create**:
   - standalone container with an Unraid template: write the template back
     to `templates-user/my-<name>.xml` (so Unraid's UI knows the container)
     and create the container through the Engine API from the inspect
     output (`Config`, `HostConfig`, `NetworkingConfig`, minus runtime-only
     fields), with the pinned image;
   - standalone without a template: the Engine API create alone;
   - compose project: write the compose files and `.env` back to the
     project directory, then `docker compose -p <project> up -d --no-build`
     through the helper (`nsenter` into the host, where the compose plugin
     lives), with each service's image pinned by digest through a generated
     override file. Without the helper, the files and data are restored and
     the user runs compose.
5. **Start** only if asked (`start: true`).

`define` for LXC: write `config` and the rootfs to `<lxcpath>/<name or new
name>/`, rewriting `lxc.rootfs.path` and `lxc.uts.name`, and regenerating
`lxc.net.*.hwaddr` unless `keep_macs`; ownership is restored numerically, so
unprivileged containers keep their shifted ids; `lxc-start` only if asked.

## 12. LXC specifics

- **Running container**: `lxc-freeze -P <lxcpath> -n <name>`, `syncfs`,
  one snapshot covering the rootfs dataset and any bind-mount datasets
  (StoneBlock binds `/mnt/cache/appdata/StoneBlock4`, so its capture is
  `cache/lxc` and `cache/appdata` in one `zfs snapshot` command),
  `lxc-unfreeze`. The same watchdog, intents and sweep as Docker; the
  freezer is the cgroup freezer either way.
- **Stopped container** (both on FS1 today): no freeze; the snapshot is
  still taken (it is cheap, and it removes the race of the container
  starting mid-read): level `offline` if `lxc-info` reports `STOPPED`
  before and after.
- **Backing**: `dir` (FS1, survey section 4) is a TREE item. A rootfs on
  its own btrfs subvolume or ZFS dataset (`lxc-create -B btrfs|zfs`) is the
  same TREE item with that subvolume or dataset as its snapshot domain.
- **The plugin's own backup** (`LXC_BACKUP_SERVICE`, disabled on FS1) is
  not used.

## 13. Surface

### 13.1 Config

| Key | Default | Meaning |
|---|---|---|
| `containers.enabled` | `0` | Load the Docker provider |
| `containers.docker_socket` | `/var/run/docker.sock` | Engine API socket (option A only) |
| `containers.include_roots` | `/mnt/cache/appdata,/mnt/user/appdata` | Bind sources under these are data; others are excluded and listed |
| `containers.pause` | `always` | `always`, `auto`, `never` (6.4) |
| `containers.pause_scope` | `unit` | `unit`, `group`, `job` (6.2) |
| `containers.pause_timeout_s` | `5` | Time for every member to freeze |
| `containers.max_pause_s` | `30` | Watchdog bound on the window, fallback pass 2 included |
| `containers.never_pause` | empty | Container-name globs never paused |
| `containers.fallback` | `pause-read` | `pause-read`, `live`, `stop`, `fail` (section 7) |
| `containers.fallback_on_deadline` | `keep` | `keep` (flagged) or `fail` |
| `containers.btrfs_snapshot_dir` | `<pool>/system/.cbackup-snapshots` | Where btrfs snapshots go (8.2) |
| `containers.hook_timeout_s` | `600` | Default hook timeout |
| `lxc.enabled` | `0` | Load the LXC provider |
| `lxc.path` | `/mnt/cache/lxc` | The LXC path (`-P`) |

### 13.2 API and CLI

Common routes are in `source-providers.md` section 10, with `<p>` =
`docker` (units `compose:<project>` or a container name) or `lxc`.
Container-specific: restore bodies take `containers`, `mounts`, `name`,
`replace`, `allow_tag`, `start`, `accept_privileged`.

```sh
c-backup container list                       # units, members, mounts and their capture method
c-backup container show compose:immich
c-backup container enrol 1 compose:immich --exclude /mnt/cache/appdata/Immich/ml_cache
c-backup container hooks set 1 compose:immich hooks.json
c-backup container backup 1 --all
c-backup container history 1 compose:gitlab_server
c-backup container restore 1 5120 --mount /mnt/cache/appdata/Grafana --dest /tmp/grafana
c-backup container restore 1 5120 --define --name immich-test --start
c-backup lxc list; c-backup lxc backup 1 DevBox; c-backup lxc restore 1 5200 --define --name devbox2
```

### 13.3 UI

- **Containers** list grouped by unit (compose projects expanded to their
  members): last snapshot, level badge, pause time, per-mount capture
  badges (`snapshot`, `fallback`, `excluded`), schedule.
- **Unit** page: history with level and pause time per snapshot, hook
  results with their logs, the mount table with sizes and exclusions.
- **Restore wizard**: file, mount, container or unit; destination;
  preflight table (collisions, ports, IPs, networks, privileged settings,
  image digest availability) before anything changes.
- **Settings** per unit: exclusions, hooks, pause policy and scope, group,
  fallback mode.
- **LXC** tab: the same, without compose.

### 13.4 Metrics

The common ones (`source-providers.md` section 10) with
`provider="docker"` or `"lxc"`, plus `cbackup_container_fallback_mounts_total{mode}`
and `cbackup_container_hook_failures_total{kind}`.

### 13.5 Manual chapter outline

New chapter "Containers", after "Virtual machines":

1. What is captured: definitions, mounts, volumes; what is not: images, excluded mounts
2. Units: compose projects and standalone containers; never-paused containers
3. Consistency: hooks, pause, snapshot; the pause window and how it is measured; levels
4. ZFS and btrfs snapshots: domains, names, cleanup, the btrfs top-level case
5. Filesystems without snapshots: the fallback modes and their trade-offs
6. Hooks: kinds, examples for PostgreSQL, SQLite, Prometheus
7. Deployment: the helper sidecar, mounts, which host runs what
8. The all-containers schedule; moving off the Appdata Backup plugin
9. Restore: files, mounts, containers, projects; image digests; collisions
10. LXC
11. Troubleshooting: a container that will not pause, leftover snapshots, `.zfs` visibility

## 14. Test plan

### 14.1 Unit tests (fake provider and fixtures)

- Mount classification and `/mnt/user` resolution over a fake directory
  layout: single pool, array-only, split share, symlinked share, missing.
- Path remapping: a snapshot read root recorded at the original path; file
  cache hits across sessions whose snapshot paths and device numbers
  differ; a changed file under an unchanged one's directory.
- Capture planning: per-unit, group and job scopes produce the right pause
  sets and one snapshot per pool per capture.
- Lifecycle faults through the fake provider: a member that will not pause,
  snapshot failure, watchdog firing before and after the snapshot, hook
  timeouts and failures with each `on_fail`, read failure, end failure,
  crash at every step followed by `sweep`: every pause released, every
  snapshot of ours destroyed, a foreign snapshot with a matching-looking
  name left alone.
- Fallback two-pass: pass 2 re-reads exactly the files changed between
  passes; deadline behaviour for `keep` and `fail`.
- Dump hooks: a multi-GB generated stdout streamed with bounded memory; the
  Docker multiplexed stream parser against crafted frames.
- Definition round trip: inspect to create-body translation over a corpus
  of real inspect documents (including the FS1 ones, with secrets scrubbed).

### 14.2 Integration in CI

- ZFS: a file-backed pool (`zpool create` on a sparse file) in a privileged
  CI job where the runner has the zfs module; otherwise skipped with a
  reason. Snapshot, read through `.zfs`, destroy, sweep, owner property.
- btrfs: a loop-mounted btrfs image with no subvolumes: top-level snapshot,
  read, destroy, `parent_uuid` check, scanner exclusion of the snapshot
  directory.
- Docker: a rootful Docker-in-Docker service in a dedicated CI job, with
  throwaway containers (`alpine` writing a known pattern; a PostgreSQL with
  a generated workload): pause timing, hooks, restore with digest pinning.
- LXC: a privileged job with `lxc` installed and a busybox container, if a
  runner supports it; otherwise UAT only.

### 14.3 UAT (disposable containers only)

With `units.allow: cbk-uat-*`:

- **FS2 (btrfs, no subvolumes)**: `cbk-uat-pg` (PostgreSQL under a pgbench
  load), `cbk-uat-sqlite` (an SQLite writer), `cbk-uat-compose` (a
  two-service compose project). Nightly for a week; measure `pause_ms` and
  `snapshot_ms`; kill the daemon and the helper mid-pause and mid-read and
  check the sweep; restore each and verify (PostgreSQL starts and
  `pg_amcheck` passes, SQLite `integrity_check`, compose project comes up).
- **FS1 (ZFS)**: the same set, on `cache/appdata`, after the user approves
  running disposable containers there; U3 (`.zfs` visibility from a
  container) and the helper's `nsenter` paths.
- **LXC**: a disposable `cbk-uat-lxc` container on FS1, running and
  stopped.
- Exit: every restore verified as above; measured pause windows reported
  against 6.1; zero leftover snapshots, pauses or intents across all fault
  injections.

Production containers are enrolled only after the user's explicit OK.

## 15. Phasing

| Phase | Delivers | Depends on |
|---|---|---|
| C0 | Shared with VMs: interface, fake provider, unit assembler, reservation, retention groups, intents/sweep, `units` schedules (`vm-backup.md` V0) | - |
| C1 | Docker provider: units (compose and standalone), definition capture, mount classification and resolution, ZFS snapshots, pause/unpause, path remapping, file cache over snapshots; restore files and mounts | C0 |
| C2 | The helper sidecar and the control/data split; option A remains for development | C1 |
| C3 | Hooks (`exec`, `dump`), watchdog, the all-containers schedule | C1 |
| C4 | btrfs top-level snapshots (FS2), fallback `pause-read` | C1 |
| C5 | Restore define: Engine API create with digest pinning, Unraid templates, compose through the helper | C2 |
| C6 | LXC provider | C2 |

All of C1 to C6 are in the 1.0 scope the user set; C1, C3 and C5 are what
replacing the Appdata Backup plugin on FS1 needs.

## 16. Open questions for the user

1. **C1.** `containers.pause: always` (your call, the default) or `auto`,
   which skips the pause for units entirely inside one dataset where the
   snapshot alone is already crash-consistent?
2. **C2.** Fallback default `pause-read` with a 30-second cap, keeping a
   flagged `inconsistent` snapshot when the cap is hit (proposed)?
3. **C3.** Restore pulls by digest by default and needs `allow_tag` to fall
   back to the tag (proposed)?
4. **C4.** Immich `pg_dumpall` dump hook: nightly, weekly, or not at all
   (the snapshot is already crash-consistent)?
5. **C5.** SECURITY: option A or B (helper sidecar, recommended), section
   9.2.
6. **C6.** FS2 btrfs: snapshot the top-level subvolume into
   `system/.cbackup-snapshots` (proposed), or convert `appdata` to a
   subvolume (operator step, needs your approval)?
7. **C7.** Default exclusions for GitLab logs and the OpenSearch index:
   ship as suggestions only (proposed) or apply by default?

UAT items to settle before conclusions: U3 (`.zfs` snapshot visibility
inside a container with `rslave`), U4 (whether Unraid exports or moves dot
directories on a pool, and that `system` is pool-only on FS2).
