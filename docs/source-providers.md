# Source providers

Design document. Nothing here is built yet. It specifies the interface that
lets c-backup back up things that are not plain directory trees - virtual
machines, Docker containers, LXC containers - as first-class units with
their own history, consistency guarantees, retention and restore. The
provider-specific designs are in `vm-backup.md` (libvirt/QEMU) and
`container-backup.md` (Docker and LXC); both build on this one.

## FORMAT IMPACT

**None. Repository format 3 can freeze as it is.** No change to the marker,
object types, packs, chunk lists, tree payloads or snapshot records is
needed for VM, Docker or LXC backup. Everything a unit snapshot carries is
expressed with what format 3 already has:

- A VM disk is a regular-file entry whose content is a chunk list: offsets,
  content-defined splits and holes as gaps are exactly what a block device
  image needs. Disks up to 2^64 bytes fit (`size` is u64, a list tree of
  nine levels of 16,384 entries is far beyond any disk).
- Container data is files and directories, backed up as files and
  directories at their real host paths.
- Unit metadata (provider, unit identity, consistency level achieved, domain
  XML, `docker inspect` output, Unraid template, compose file, hook logs,
  checkpoint names for forensics) is ordinary files under one reserved
  top-level directory, `.cbackup-unit/`, in the snapshot's own tree.
- Everything the daemon needs to list, filter and retain units is derived
  from that directory into `state.db` by reindex, like every other index.

One **behavioural** reservation (not a format change) must ship with the
feature: file backups never emit a top-level `.cbackup-unit` entry, and a
file source at or under `/.cbackup-unit` is refused. That is what lets
reindex trust the directory as a daemon-written unit marker. It changes no
reader and no byte on disk. `db-detection.md` adds a second reserved name of
the same kind, `.cbackup-captures`, for engine copies in file snapshots;
both reservations should land together. Section 7 gives the alternatives that were
rejected, notably a field in `snapshots/<id>`, which a 1.0 replicator would
silently strip.

## 1. Terms

| Term | Meaning |
|------|---------|
| provider | A module that knows one kind of workload: `libvirt`, `docker`, `lxc`, and `fake` for tests |
| unit | One backed-up thing: a VM, a container. Identified by `(provider, unit_id)` |
| unit key | `<provider>:<unit_id>`, the grouping key for history and retention |
| item | One piece of a unit: a disk, a volume, a bind mount, a config blob |
| session | One backup of one unit, from `begin` to `end` |
| consistency level | What point-in-time guarantee a session achieved (section 5) |
| snapshot domain | The unit of atomic capture underneath: a libvirt backup job, a btrfs subvolume, a ZFS dataset |
| token | Provider-opaque incremental state carried from one session to the next (a libvirt checkpoint) |

Unit identity is provider-defined and must be stable across the things users
do routinely:

| Provider | `unit_id` | Why |
|----------|-----------|-----|
| libvirt | domain UUID | Names can be changed with `virsh domrename`; the UUID survives. The name is recorded per snapshot for display |
| docker | `compose:<project>` for a compose project, else the container name | A compose project is one application and the consistency unit (`container-backup.md` 3.1). Unraid and compose recreate a container (new id) on every update; the name is what persists. Container ids are recorded per snapshot |
| lxc | container name | The name is the directory under the LXC path |

## 2. Architecture

```mermaid
flowchart LR
  subgraph Daemon [c-backupd]
    SCH[scheduler / API] --> JM[job manager<br/>per-repo lock]
    JM --> UJ[unit backup job]
    UJ --> REG[provider registry]
    REG --> LV[libvirt provider]
    REG --> DK[docker provider]
    REG --> LX[lxc provider]
    REG --> FK[fake provider<br/>tests only]
    UJ --> ASM[unit tree assembler]
    ASM --> ENG[existing engine:<br/>cdc_store_source, scan + store,<br/>tree_build, pack writers]
    ENG --> SNAP[snapshot record<br/>+ index]
  end
  LV -- control: libvirt API --> HV[(libvirtd / QEMU)]
  LV -- data: native NBD client --> HV
  DK -- Engine API over unix socket --> DD[(dockerd)]
  LX -- lxc-* tools --> LXC[(LXC)]
  DK & LX -- snapshots --> FS[(btrfs / ZFS)]
```

The engine below the assembler is the existing one. A disk is handed to
`cdc_store_source` through a `cdc_source_t` (exactly what replication's
re-chunking already uses), a container path is handed to the scanner with a
read root that differs from the recorded root (section 4.3), and the unit's
metadata blobs are stored as small file objects. The tree is built by
`tree_build`, packs are sealed, the snapshot record is written, the index
is updated: the same sequence as Manual section 6.3.

The control plane (pausing, freezing, snapshotting, checkpoints, defining)
and the data plane (reading bytes) are separate in the interface on
purpose. Section 11 uses that split to move every root-equivalent operation
out of the network-facing daemon.

## 3. The interface

Sketch of `src/providers/provider.h`. Names are proposals; the semantics
are the specification.

```c
#define SP_ABI_VERSION 1

typedef enum {
    SP_CONSIST_INCONSISTENT = 0, /* read live; no single point in time */
    SP_CONSIST_CRASH        = 1, /* one point in time, as after a power cut */
    SP_CONSIST_QUIESCED     = 2, /* guest/container filesystems flushed and
                                    frozen at that point (guest agent) */
    SP_CONSIST_APPLICATION  = 3, /* quiesced, plus application participation:
                                    VSS writers, or a pre hook that succeeded */
    SP_CONSIST_OFFLINE      = 4, /* the unit was not running while read */
} sp_consistency_t;

typedef enum { SP_ITEM_BLOB = 1, SP_ITEM_BLOCK = 2, SP_ITEM_TREE = 3 } sp_item_kind_t;

/* Unit capabilities (sp_unit_desc_t.caps) and item capabilities. */
#define SP_CAP_QUIESCE      0x01u  /* can freeze (guest agent present) */
#define SP_CAP_PAUSE        0x02u  /* can pause the whole unit */
#define SP_CAP_SNAPSHOT     0x04u  /* item lives on an instantly snapshottable fs */
#define SP_CAP_INCREMENTAL  0x08u  /* item can report changed ranges vs a token */
#define SP_CAP_DEFINE       0x10u  /* provider can re-create the unit on restore */
#define SP_CAP_PARALLEL     0x20u  /* items of one session may be read concurrently */

typedef struct {
    char           id[64];          /* stable within the unit: "vda", "vol:pgdata" */
    sp_item_kind_t kind;
    uint64_t       size;            /* BLOCK: virtual size; BLOB: bytes; TREE: 0 */
    char           format[16];      /* BLOCK: "raw", "qcow2", ...; else "" */
    char          *source;          /* host path or URI, informational */
    char          *tree_path;       /* where it lands in the snapshot tree */
    uint32_t       caps;
} sp_item_desc_t;

typedef struct {
    char            provider[16];
    char            unit_id[128];
    char           *name;           /* display name at describe time */
    int             state;          /* SP_STATE_RUNNING/_PAUSED/_STOPPED/_SUSPENDED */
    uint32_t        caps;
    sp_item_desc_t *items;
    size_t          n_items;
} sp_unit_desc_t;

/* Per-item incremental request and result. */
typedef enum {
    SP_READ_FULL = 0,      /* every allocated byte is read */
    SP_READ_INCREMENTAL,   /* only ranges changed since base_token */
    SP_READ_UNCHANGED,     /* provably identical to the base: reuse its root */
} sp_read_mode_t;

typedef struct {
    const char *item_id;
    const char *base_token;         /* NULL: no usable base */
} sp_item_base_t;

typedef struct {
    int64_t                job_id;
    const sp_item_base_t  *bases;   /* from state.db (section 7.4) */
    size_t                 n_bases;
    sp_consistency_t       want;    /* best effort target */
    sp_consistency_t       require; /* below this, begin fails */
    unsigned               freeze_timeout_s;
    unsigned               pause_max_s;
    atomic_int            *cancel;
} sp_begin_req_t;

typedef struct {
    const char     *item_id;
    sp_read_mode_t  mode;
    const char     *reason;         /* bounded vocabulary, section 6.4 */
} sp_item_plan_t;

/* Filled by begin, read by the job, owned by the session. */
typedef struct {
    sp_consistency_t achieved;
    uint32_t         consist_flags; /* SP_CF_HOOK_PRE_OK, SP_CF_FROZEN_FS_n, ... */
    uint64_t         freeze_ms, pause_ms;
    sp_item_plan_t  *plans;
    size_t           n_plans;
    char            *new_token;     /* to bind if the snapshot commits */
    char           **warnings;
    size_t           n_warnings;
} sp_session_info_t;

/* A block item as a stream of extents. Forward-only. */
typedef enum { SP_EXT_DATA = 1, SP_EXT_ZERO = 2 } sp_ext_type_t;

typedef struct {
    /* Allocation from 'from' onward: the next extent, DATA or ZERO
     * (ZERO: guaranteed to read as zeroes). *done at the end of the disk. */
    status_t (*extent)(void *ctx, uint64_t from, sp_ext_type_t *type,
                       uint64_t *len, int *done);
    /* Changed ranges since the base token, ascending, from 'from' onward.
     * Only valid when the plan says SP_READ_INCREMENTAL. */
    status_t (*changed)(void *ctx, uint64_t from, uint64_t *s, uint64_t *e,
                        int *done);
    /* Exactly n bytes at off. May be called from several threads for
     * different items; for one item, calls are issued in increasing order
     * but may overlap in time (the reader pipelines). */
    status_t (*read)(void *ctx, void *buf, size_t n, uint64_t off);
    void     (*close)(void *ctx);
    void      *ctx;
} sp_block_stream_t;

typedef struct sp_session sp_session_t;
typedef struct sp_ctx     sp_ctx_t;   /* provider instance: config, connection */

typedef enum { SP_END_COMMITTED = 1, SP_END_ABORTED = 2 } sp_end_outcome_t;

typedef struct sp_provider_ops {
    const char *name;
    uint32_t    abi;

    status_t (*init)(const sp_config_t *cfg, sp_ctx_t **out);
    void     (*fini)(sp_ctx_t *ctx);

    /* Versions, permissions, reachable sockets, tools present. Never
     * touches a unit. Feeds GET /providers. */
    status_t (*probe)(sp_ctx_t *ctx, sp_probe_t *out);

    /* Live enumeration. cb is called once per unit, unit ids only plus a
     * name and state; describe gives detail. */
    status_t (*list_units)(sp_ctx_t *ctx, sp_unit_cb cb, void *cbctx);

    /* Full description, including items and capabilities. Read-only. */
    status_t (*describe)(sp_ctx_t *ctx, const char *unit_id,
                         sp_unit_desc_t **out);
    void     (*desc_free)(sp_unit_desc_t *d);

    /* Establish the point in time: freeze/pause/snapshot, start export.
     * On error nothing is left behind (the provider has already released
     * whatever it took). On success the caller MUST call end exactly once. */
    status_t (*begin)(sp_ctx_t *ctx, const sp_unit_desc_t *unit,
                      const sp_begin_req_t *req, sp_session_t **out,
                      const sp_session_info_t **info);

    /* Items, by kind. */
    status_t (*blob_read)(sp_session_t *s, const sp_item_desc_t *item,
                          void **data, size_t *len);      /* <= 16 MiB */
    status_t (*block_open)(sp_session_t *s, const sp_item_desc_t *item,
                           sp_block_stream_t *out);
    status_t (*tree_root)(sp_session_t *s, const sp_item_desc_t *item,
                          char *read_root, size_t sz);    /* path to read */

    /* Release everything: thaw, unpause, delete fs snapshots, abort the
     * export, and settle tokens: on COMMITTED keep new_token and retire the
     * base; on ABORTED discard new_token and keep the base. Idempotent,
     * never blocks indefinitely, and best-effort: a failure here is logged
     * and recorded as an intent for the sweep, it never fails a snapshot
     * that already committed. */
    status_t (*end)(sp_session_t *s, sp_end_outcome_t outcome,
                    int64_t snapshot_id);

    /* Crash recovery and hygiene, at daemon start and after every job:
     * thaw/unpause anything an intent says may be held, delete leftover
     * snapshots/checkpoints/exports this daemon created, retire tokens
     * state.db no longer binds. Never touches anything it cannot prove it
     * created (section 6.3). */
    status_t (*sweep)(sp_ctx_t *ctx, const sp_sweep_req_t *req,
                      sp_sweep_report_t *out);

    /* Restore support. preflight reports collisions and dangerous settings
     * without changing anything; define re-creates the unit from the
     * snapshot's config blobs, after the core has written the data. */
    status_t (*restore_preflight)(sp_ctx_t *ctx, const sp_restore_req_t *req,
                                  sp_restore_plan_t *out);
    status_t (*define)(sp_ctx_t *ctx, const sp_restore_req_t *req,
                       const sp_restore_plan_t *plan, sp_define_result_t *out);
} sp_provider_ops_t;
```

### 3.1 Contract details

- **Errors** follow the codebase: `status_t`, the deepest function sets the
  message with `set_error`, callers propagate. "Incremental not possible" is
  not an error: it is a plan with `SP_READ_FULL` and a reason. Errors are
  for "this unit cannot be backed up right now".
- **Memory.** `describe` and `begin` allocate bounded structures (items are
  few). Nothing in the interface hands over a whole disk or an extent map:
  extents and changed ranges are pulled forward one at a time, because a
  16 TiB disk with a fragmented allocation map must cost no memory
  (design constraints 1 to 3).
- **Threads.** One job thread drives a session. With `SP_CAP_PARALLEL` the
  job may read different items concurrently from up to
  `units.parallel_items` threads; `read` for one item is pipelined by the
  stream itself. Provider contexts are shared across jobs and must be
  thread-safe; the libvirt connection is (libvirt's public API is
  thread-safe per connection).
- **Cancellation.** `begin` and every read check `req->cancel`. A cancel
  mid-session still runs `end(ABORTED)`.
- **Time bounds.** Every provider call that talks to something external has
  a timeout. A call that cannot be bounded (a libvirt agent call stuck
  behind the guest) is bounded by a watchdog (section 6.2), never by hope.
- **Determinism.** Two reads of the same unit state must produce the same
  objects. For BLOCK items this rules out using anything but content to
  decide spans (see `vm-backup.md` section 5.2: holes are maximal zero runs,
  not whatever the allocation map said this time).

## 4. Item kinds

### 4.1 BLOB

Small configuration, at most 16 MiB (one object): domain XML, NVRAM
variables, `docker inspect` JSON, an Unraid template, a compose file, a hook
log. Stored as a regular file under `.cbackup-unit/`. Small blobs are one
`FILE` object; the rare large one (UEFI NVRAM is 128 KiB to 4 MiB) goes
through `cdc_store_source` like any file.

### 4.2 BLOCK

An extent stream (above). The core turns it into a `cdc_source_t`:
`next_span` walks `extent` and the zero-run rule, `read` pipelines into the
provider's `read`. For a full read that is all; for an incremental read the
core runs the delta merge of `vm-backup.md` section 5.3, which needs
`changed` plus the previous snapshot's chunk list for the item. The result
is a content hash naming a chunk list, written as a regular-file node at the
item's `tree_path` with `size` = the virtual size.

### 4.3 TREE

A directory to back up through the normal scanner, read from `read_root`
but recorded at the item's original path. For a container volume that is
`read_root = /mnt/cache/.cbackup-snapshots/cbk-.../appdata/gitlab`,
recorded as `/mnt/user/appdata/gitlab` (the path Docker binds). Two things
in the existing engine are generalised for it:

1. `backup_opts_t` gains a per-source `(read_root, record_root)` pair; the
   scanner reads under the first and emits entries under the second. Every
   other part of the pipeline sees record paths only.
2. The file cache is looked up by record path with `FILE_CACHE_IGNORE_DEV`
   forced on for snapshot reads. A btrfs snapshot is a new subvolume with a
   new anonymous device number every time, and a ZFS snapshot has its own
   `st_dev`; but both preserve inode numbers, ctime, mtime and size, so the
   remaining fields still prove a file unchanged. Without this every
   container incremental would re-read every file.

## 5. Consistency model

The level is ordinal for display and for `require`, but it is a statement of
fact about one session, not a promise about the data:

| Level | Produced by | What a restore is like |
|-------|-------------|------------------------|
| `offline` | VM shut off; container stopped | A clean stop, if the unit was stopped cleanly |
| `application` | VSS writers via the guest agent (Windows); a container or VM pre hook that exited 0 | Applications took part: databases flushed or dumped |
| `quiesced` | guest agent `fsfreeze` succeeded and was held across the capture | Filesystems clean, applications as after a sudden but orderly freeze |
| `crash` | one point in time with nothing flushed: a libvirt backup without freeze, a paused-and-snapshotted container | A power cut at that instant. Journalled filesystems and WAL databases recover |
| `inconsistent` | live read with no snapshot | Different files from different moments. Flagged everywhere it is shown |

Rules:

- The achieved level is recorded in the snapshot (`unit.json`,
  section 7.2) and indexed. The UI and API show it on every snapshot, and a
  unit's history shows when it changed.
- `require` lets a user say "fail rather than store less than `crash`". The
  default is `crash` for VMs and containers on snapshot filesystems, and
  `inconsistent` only where the user picked the live fallback explicitly.
- Downgrades are never silent: a freeze that timed out, a pre hook that
  failed, a pause that was cut short by the watchdog each produce a warning
  in the job result, a journal entry, and the lower level.

## 6. Lifecycle and failure handling

### 6.1 One session

```mermaid
stateDiagram-v2
  [*] --> Described: describe
  Described --> Holding: begin (freeze/pause/snapshot/export)
  Holding --> Reading: point in time established,<br/>freeze/pause released
  Holding --> Released: begin failed<br/>(provider released itself)
  Reading --> Assembling: every item stored
  Reading --> Aborting: read error / cancel
  Assembling --> Committed: packs sealed, record written, index rows
  Assembling --> Aborting: error
  Committed --> Released: end(COMMITTED): bind new token,<br/>retire base, drop fs snapshots/export
  Aborting --> Released: end(ABORTED): drop new token,<br/>keep base, drop fs snapshots/export
  Released --> [*]
```

The `Holding` state is the only one that affects the workload (a frozen
guest, a paused container) and it is kept as short as the mechanism
allows: seconds, not the duration of the read. Each provider design
quantifies it.

### 6.2 Intents and watchdogs

Every action that leaves something held outside the daemon is written down
before it is taken, in a daemon-state table:

```sql
-- migrations/002_units.sql (sketch)
CREATE TABLE unit_intents (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    provider   TEXT NOT NULL,
    unit_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,   -- 'freeze','pause','fs_snapshot','export','checkpoint_new'
    detail     TEXT NOT NULL,   -- what exactly: snapshot name, checkpoint name, socket
    job_id     INTEGER,
    created_at INTEGER NOT NULL
);
```

The row is inserted (and committed) before the action, and deleted after
the matching release succeeds. So after any crash, `sweep` at start knows
exactly what might still be held: it thaws, unpauses, deletes and aborts
each, idempotently (thawing a guest that is not frozen and unpausing a
running container are no-ops that succeed).

A watchdog thread bounds every hold: an intent of kind `freeze` or `pause`
older than its limit (`vm.freeze_timeout_s`, `containers.max_pause_s`) is
released by the watchdog itself, from another thread, whatever the job
thread is doing. The session then records the downgrade (section 5).
Freeze and pause are always released in `begin`, before any data is read,
so in the normal path the watchdog never fires.

The crash case that intents cannot cover is the daemon dying while holding
a pause or freeze and not coming back. Docker's restart policy (`unless-
stopped`) brings the daemon back within seconds and its sweep releases the
hold; the host helper of section 11, if adopted, holds the watchdog itself
and is independent of the daemon's lifetime, which closes the gap.

### 6.3 Never touch what we did not create

Every external artefact c-backup creates is named with a recognisable,
parseable grammar that includes a per-daemon id:

```
cbk-<daemon8>-<job>[-<n>]        e.g. cbk-7f3a09c1-4127-0
```

`daemon8` is the first 8 hex digits of a random id generated once per
`state.db` and kept in `auth_config`-style daemon state (and in a file in
the state directory, so a lost database does not make the daemon forget its
own artefacts). `sweep` deletes an artefact only when all hold:

1. its name matches the grammar exactly and carries this daemon's id;
2. it is not owned by a running job;
3. a provider-specific ownership proof passes: a btrfs subvolume must be
   read-only with a `parent_uuid` equal to the source subvolume's uuid; a
   ZFS snapshot must carry the user property `com.cbackup:owner=<daemon id>`
   set at creation; a libvirt checkpoint's XML must carry the
   `<description>` c-backup wrote.

Anything else is reported in the sweep result and left alone. A user's
snapshot named `cbk-something` is never at risk.

### 6.4 Read-mode reasons

A bounded vocabulary, so they can label a metric without exploding its
cardinality: `first`, `no-incremental-support` (raw disk, plain directory
tree), `no-base`, `base-pruned`, `token-missing`, `token-invalid`,
`item-new`, `item-resized`, `item-source-changed`, `unit-redefined`,
`audit`, `delta-too-costly`, `forced`, `unchanged-offline`, `incremental`.

### 6.5 One unit, one session at a time

Two repositories may back up the same unit. libvirt allows one backup job
per domain, and pausing a container twice is meaningless, so the daemon
keeps a process-wide mutex per unit key. A second job waits for it, up to
`units.unit_wait_s` (default 3600), then fails that unit with `unit busy`;
other units in the job proceed.

## 7. Representation in the repository

### 7.1 One snapshot per unit

A unit backup produces one snapshot per unit, even when one job backs up
twenty containers. Per-unit snapshots are what make per-unit history,
per-unit retention and per-unit restore simple: a snapshot id names exactly
one unit at exactly one point in time. A job backing up many units holds
the repository lock once and commits a snapshot after each unit, so a
failure in unit 17 keeps units 1 to 16.

### 7.2 The synthetic tree

```
/                                   root tree of a unit snapshot
  .cbackup-unit/                    reserved; only unit snapshots have it
    unit.json                       volatile: what this session did
    config/                         stable: dedups across snapshots
      domain.xml                    (libvirt)  inactive domain XML
      nvram.fd                      (libvirt)  UEFI variable store
      inspect.json                  (docker)   container inspect, verbatim
      image.json                    (docker)   image inspect: digests
      unraid-template.xml           (docker)   dockerMan template, if any
      compose/<file>                (docker)   compose files and .env
      lxc.config                    (lxc)      the container's config file
    disks/                          (libvirt)
      vda                           regular file, size = virtual size,
      vdb                           content = chunk list, holes = gaps
    hooks/
      pre.log, post.log             hook stdout/stderr, bounded to 1 MiB
  mnt/user/appdata/gitlab/...       (containers) data at its real paths
```

Why each choice:

- **Disks as regular files.** A disk image is a file with holes; format 3
  represents that exactly. A build that knows nothing about units restores
  a VM snapshot as `disks/vda`, a sparse raw image any hypervisor boots. The
  same is true of container data, restored by path. This is the property
  that made the "no format change" decision easy: unit support degrades to
  file support, never to unreadable.
- **Guest-visible content, not the container file.** `disks/vda` is the
  bytes the guest sees, whatever the image format (raw, qcow2, a qcow2
  overlay on a template, a block device). qcow2 cluster layout differs from
  VM to VM; guest content does not, so two VMs cloned from one template
  dedup against each other and against the template.
- **Data at real paths** for containers, so a plain file restore puts
  appdata back where the container binds it, and browsing and search work
  as for any file backup.
- **Volatile and stable separated.** `unit.json` changes every session (it
  records timing and the level achieved); everything under `config/` changes
  only when the configuration does, so its tree object is reused and costs
  nothing. Each session adds about three small objects of metadata.
- **Metadata `mtime`.** Synthetic files carry `mtime` 0 and mode 0600,
  uid/gid 0, so unchanged ones serialise identically (the determinism rule
  in `repo-format.md`). Disk nodes carry the image file's mtime where there
  is one; it is real metadata and costs only the one tree object that
  changes anyway.

`unit.json` (UTF-8 JSON, one object, at most 64 KiB):

```json
{"format":"cbackup-unit","version":1,
 "provider":"libvirt","unit_id":"5c1f...-...","name":"win11-test",
 "consistency":"quiesced","consist_flags":["agent-fsfreeze"],
 "freeze_ms":412,"pause_ms":0,
 "items":[
   {"id":"vda","kind":"block","path":".cbackup-unit/disks/vda","size":68719476736,
    "format":"qcow2","source":"/mnt/user/domains/win11-test/vdisk1.qcow2",
    "read_mode":"incremental","reason":"incremental","bytes_read":1288490188,
    "checkpoint":"cbk-7f3a09c1-4127"},
   {"id":"nvram","kind":"blob","path":".cbackup-unit/config/nvram.fd"}],
 "warnings":[]}
```

Readers ignore unknown keys and refuse `version` > 1 for interpretation
(the snapshot is then shown as an unclassified file snapshot, never
misread). The checkpoint name is informational: it is never trusted for an
incremental (section 7.4 says what is).

### 7.3 The reserved name

`.cbackup-unit` at the root is the classification signal. To make it
unforgeable by data, file backups must never produce it: `sources set` and
`POST /repos/<id>/backup` refuse a source at or under `/.cbackup-unit`,
and the scanner drops a root-level entry of that name with an error if one
appears through a source of `/`. Only the unit assembler writes it. Since
file backups record absolute paths, the root of a file snapshot contains
only first path components of sources; with this rule none of them can be
`.cbackup-unit`.

### 7.4 What lives where

| Fact | Repository (authoritative) | state.db (derived, rebuilt by reindex) | state.db (daemon-only, loss costs a full read) |
|------|---|---|---|
| unit, name, consistency, items, read modes | `unit.json` | `snapshot_units`, `snapshot_unit_items` | |
| domain XML, inspect, template, compose, NVRAM | `config/*` | | |
| disk and volume content | the tree | `nodes` as usual | |
| enrolment, per-unit settings, hooks | | | `unit_enrolment` |
| per-unit retention | | | `unit_policy` |
| incremental tokens (checkpoint name + XML, bound snapshot, item root hash) | informational copy in `unit.json` | | `unit_tokens` |
| intents | | | `unit_intents` |

```sql
-- derived
CREATE TABLE snapshot_units (
    repo_id     INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id     INTEGER NOT NULL,
    unit_key    TEXT NOT NULL,          -- 'libvirt:<uuid>', 'docker:<name>'
    provider    TEXT NOT NULL,
    unit_name   TEXT NOT NULL,
    consistency INTEGER NOT NULL,       -- sp_consistency_t
    PRIMARY KEY (repo_id, snap_id)
);
CREATE INDEX snapshot_units_by_unit ON snapshot_units(repo_id, unit_key, snap_id);

-- daemon-only
CREATE TABLE unit_tokens (
    repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    unit_key   TEXT NOT NULL,
    item_id    TEXT NOT NULL,
    token      TEXT NOT NULL,           -- provider-opaque (checkpoint name)
    token_blob TEXT,                    -- e.g. checkpoint XML for redefine
    snap_id    INTEGER NOT NULL,        -- the snapshot the token describes
    item_root  BLOB NOT NULL,           -- that snapshot's content hash for the item
    identity   TEXT NOT NULL,           -- source path, size, format, file identity
    created_at INTEGER NOT NULL,
    PRIMARY KEY (repo_id, unit_key, item_id)
);
```

An incremental base is usable only if the token row exists, its snapshot
still exists, the snapshot's tree still names `item_root` at the item's
path, and `identity` matches what `describe` reports now. Any mismatch is a
full read with a reason. That is why the token is daemon-only state: after
a lost database the daemon cannot prove a checkpoint matches a snapshot, so
it does not try, and pays one full read per item.

Reindex reads `.cbackup-unit/unit.json` from every snapshot root it walks
(one small object each; it already walks every root) and fills
`snapshot_units`. A snapshot whose `unit.json` is unreadable is indexed as
`unit_key = 'unknown:<snap id>'` and retention never prunes it (7.5).

### 7.5 Alternatives rejected

| Alternative | Why not |
|-------------|---------|
| A `unit` field in `snapshots/<id>` | The 1.0 reader ignores unknown keys, but both 1.0 writers (`backup.c` and `replica_local.c`) re-encode records through `snapshot_record_encode(id, ts, root, label)`, so a 1.0 replicator would strip the field silently. The tree survives every format 3 replicator, copying or re-chunking |
| A new object type or tree node type | A format change for something the tree already expresses; an older reader refuses it instead of degrading to files |
| A field inside `unit.json` deciding identity, with no reserved name | Data could plant it. The reservation makes the signal daemon-written by construction |
| Units as sources inside one multi-unit snapshot | GFS tiers by calendar day per repository; with twenty VMs per day it would keep one per day. Per-unit retention needs per-unit snapshots |
| One repository per unit | Loses dedup across units, which is most of the value for VMs from one template and containers sharing base layers in appdata |

## 8. Retention and schedules

### 8.1 Retention groups

GFS today elects tiers over every snapshot of a repository and never
prunes the head. With unit snapshots it runs once per **retention group**:

- group `files`: snapshots with no `snapshot_units` row (file backups);
- one group per `unit_key`.

Each group has its own head, which is never pruned, and its own policy:
`unit_policy` rows for that unit if present, else the repository's
`policy_values`, else the config defaults. `gfs_run` gains a group filter;
the election code is unchanged. A group named `unknown:*` is never pruned
and makes the pass emit a warning, because a snapshot that cannot be
classified must not be judged by the wrong group's calendar.

Pruning a unit snapshot is ordinary: it removes the record and its index
rows; gc reclaims unreferenced chunks later. Because every unit snapshot is
complete (incremental reads produce full chunk lists, there are no chains),
pruning any snapshot never breaks another. The only coupling is the
incremental base: if the token's snapshot is pruned, the next session reads
that item in full (`base-pruned`) and the provider retires the stale token.
GFS never prunes a group's head, so in steady state the base is the head
and survives.

### 8.2 Schedules

A new schedule action `units`:

```sh
c-backup schedule add 1 units --provider docker --all --exclude 'cbk-*' --every day --at 03:00
c-backup schedule add 1 units --provider libvirt --unit win11-test --every day --at 01:00
```

- `--all` enumerates at fire time, so a container added last week is
  included without editing the schedule; `--exclude` globs match unit
  names. `--unit` (repeatable) names units.
- A per-unit schedule is a `units` schedule with one `--unit`.
- Overlap and catchup policies are the existing ones, keyed by (repository,
  action, unit set).
- `gfs` schedules are unchanged; a pass covers every group.

Enrolment (`unit_enrolment`) holds per-unit settings: excluded items,
hooks, freeze policy, `require`. A unit selected by `--all` and not
enrolled uses defaults.

### 8.3 Guard rail for testing

`units.allow` is a glob list in the config file (comma-separated, the YAML
subset has no lists) of unit names the daemon may touch at all, for any
operation including describe-and-begin. Default empty means all. During
UAT it is set to the `cbk-uat-*` units, which enforces "no production VMs
or containers" in the daemon itself rather than in the operator's care. It
lives in the root-owned config file, not the API, so a session token cannot
widen it.

Each glob (`fnmatch(3)`) is tried against the unit's id, its name,
`<provider>:<id>` and `<provider>:<name>`. Ids are per provider: a compose
project is `compose:<project>` (key `docker:compose:<project>`), a
standalone container its name (`docker:<name>`), an LXC container its name,
a libvirt domain its UUID (its name matches through `<provider>:<name>`).
So a bare `cbk-uat-*` reaches containers, LXC containers and VMs named
`cbk-uat-...` but **not** the compose project `cbk-uat-compose`, whose id
and name are `compose:cbk-uat-compose`; that takes
`docker:compose:cbk-uat-*` (or `compose:cbk-uat-*`). The FS1 UAT value is
`docker:cbk-uat-*,docker:compose:cbk-uat-*,libvirt:cbk-uat-*,lxc:cbk-uat-*`.
Compose project names are lowercase (compose lowercases the directory name
it derives one from), so the project part of a `compose:` or
`docker:compose:` glob is matched lowercased: `compose:Immich` is the
project `immich`. Every other name keeps its case. The helper's
`[units] allow` uses the same keys (`helper.md`, The policy).

## 9. Jobs, locking, parallelism

- A unit job is a repo-scoped job like a backup: it takes the repository
  write lock and serialises with every other job on that repository
  (CLAUDE.md, "Job dispatch is serialised per repository").
- Inside the job, units are processed `units.parallel_units` at a time
  (default 1) and items of one unit `units.parallel_items` at a time
  (default 4). Each concurrent BLOCK item is a large file to the engine and
  uses the existing file-worker machinery: its own pack writer, the shared
  chunk workers and the shared claim set (`pack_claims_t`), so two disks
  holding the same template chunk store it once.
- Progress reports the unit (`unit 3/12 win11-test`), the item and bytes,
  through the existing `progress_cb`/`bytes_cb`.
- `jobs.max_parallel` still caps jobs daemon-wide.

## 10. Common surface

Provider-specific keys, routes and screens are in the provider documents.
The common part:

**Config** (`config.yaml`, read at start, refused if unknown, as every key):

| Key | Default | Meaning |
|-----|---------|---------|
| `units.allow` | empty (all) | Unit-name globs the daemon may touch (8.3) |
| `units.parallel_units` | `1` | Units of one job processed at once, 1 to 16 |
| `units.parallel_items` | `4` | Items of one unit read at once, 1 to 16 |
| `units.unit_wait_s` | `3600` | How long a job waits for a unit another job holds |
| `units.helper_socket` | empty | Path of the host helper's socket (11); empty = in-process control |

**API** (all under the existing auth rules; ids in paths are URL-escaped):

| Route | Purpose |
|-------|---------|
| `GET /providers` | Each compiled-in provider, enabled or not, and its `probe` result |
| `GET /providers/<p>/units` | Live enumeration, paginated by name |
| `GET /providers/<p>/units/<u>` | `describe`, read-only |
| `GET /repos/<id>/units` | Units with history in this repository: last snapshot, its level, enrolment |
| `PUT /repos/<id>/units/<p>/<u>` / `DELETE` | Enrol, change settings, unenrol (history stays) |
| `GET /repos/<id>/units/<p>/<u>/snapshots` | That unit's snapshots, paginated, with level and read modes |
| `POST /repos/<id>/units/backup` | Body `{"provider":"docker","all":true,"exclude":[...]}` or `{"units":[...]}`; returns a job |
| `POST /repos/<id>/snapshots/<sid>/unit/restore` | Restore a unit snapshot (provider docs give modes); returns a job |
| `POST /repos/<id>/units/<p>/<u>/reset-incremental` | Drop tokens; the next session reads everything |
| `GET/PUT /repos/<id>/units/<p>/<u>/policy` | Per-unit retention |

Existing routes gain an optional `unit` object on snapshot listings
(`{"key","provider","name","consistency"}`), additive.

**CLI**: a generic `c-backup unit ...` family (`list`, `show`, `enrol`,
`backup`, `history`, `restore`, `reset-incremental`, `policy`) with
`vm`, `container` and `lxc` as aliases that fill in `--provider`.
Output is the existing compact JSON.

**Metrics** (daemon-wide aggregates only, per the 1.0 cardinality
decision; labels are bounded enums, never unit names):

| Metric | Type | Labels |
|--------|------|--------|
| `cbackup_unit_sessions_total` | counter | `provider`, `result` (`ok`,`failed`,`aborted`) |
| `cbackup_unit_consistency_total` | counter | `provider`, `level` |
| `cbackup_unit_read_bytes_total` | counter | `provider`, `mode` (`full`,`incremental`) |
| `cbackup_unit_full_reads_total` | counter | `provider`, `reason` (6.4) |
| `cbackup_unit_hold_seconds_max` | gauge | `provider`, `kind` (`freeze`,`pause`): longest hold in the last job |
| `cbackup_unit_watchdog_releases_total` | counter | `provider`, `kind` |
| `cbackup_unit_sweep_removed_total` | counter | `provider`, `kind` |
| `cbackup_provider_up` | gauge | `provider` |

Per-unit numbers are API data, not metrics.

**UI**: a "Workloads" area with one tab per enabled provider (Machines,
Containers, LXC). Common pieces: a unit list (name, state, last snapshot,
consistency badge, schedule, last read mode), a unit page (history
timeline coloured by level, bytes read/new per snapshot, warnings), the
restore wizard, and per-unit settings. Provider documents list their
screens.

## 11. Security model, and the host helper (SECURITY DECISION)

Every provider needs at least one root-equivalent capability on the host:

| Provider | Root-equivalent requirement |
|----------|-----------------------------|
| libvirt | read-write libvirt socket: can define a domain with a host block device or a host directory as its disk |
| docker | the Docker socket: can start a privileged container with `/` mounted |
| btrfs snapshots | `CAP_SYS_ADMIN` to delete subvolumes (unless `user_subvol_rm_allowed`) |
| ZFS snapshots | `/dev/zfs` plus `CAP_SYS_ADMIN` and a `zfs` userland matching the host's kernel module (FS1 and FS2: kmod 2.4.4, newer than Debian trixie's userland, so the host's binary); root on `/dev/zfs` is root over every pool |
| lxc | the host's `lxc-*` tools, host network namespace (LXC's command socket is abstract, in the host netns), cgroup write access |

Granting those to `c-backupd` makes the daemon's HTTP API, which may face
the network with a session token, the gate to root on the host. Three
options:

| Option | What | For | Against |
|--------|------|-----|---------|
| A. All in the daemon | Mount the sockets and devices into the c-backupd container, add the caps | One component; simplest to ship | A bug in the HTTP-facing daemon, or a stolen admin token, is host root |
| **B. Host helper** (recommended) | `c-backup-helper`: a small privileged sidecar with no network, speaking a fixed, typed operation protocol over a unix socket shared into the daemon's container (11.1 to 11.5). It holds the sockets, tools and caps; the daemon holds none of them | The network-facing process is no longer root-equivalent; the helper uses the host's own `zfs`, `lxc-*` and libvirt, so no userland version skew; the helper owns the watchdogs, so a dead daemon cannot leave a guest frozen or a container paused | A second artefact to build, ship, start at boot and version; an RPC layer to test |
| C. Daemon on the host | Run c-backupd natively, not in Docker | No socket plumbing | Root anyway; on Unraid there is no supported native install path |

### 11.1 The helper

The helper is the control half of the vtable and nothing else. Data never
crosses it: the daemon reads NBD sockets, snapshot directories and image
files directly through its own read-only mounts. On Unraid the root
filesystem is RAM, so nothing installed on the host survives a reboot; the
helper is therefore a privileged sidecar container (`--privileged
--pid=host --network=none`) that runs the host's own tools through
`nsenter`, autostarted by Docker (`container-backup.md` section 9.2).
Estimated size: 1,500 to 2,500 lines of C, most of it the providers'
control code compiled twice (in-process for tests and development, behind
the helper for deployment), so the providers are written once against the
split. It is built with the same flags, analysers and fuzzing as the
daemon; its request parser is a fuzz target. Its own records (artefacts it
created, tokens, leases, and the XML of every checkpoint it created) live in
a state directory on a pool (for example `/mnt/cache/system/c-backup-helper/`),
never on Unraid's RAM rootfs, because they must survive a host reboot.

### 11.2 Wire protocol

- **Transport.** `AF_UNIX`, `SOCK_SEQPACKET`, at
  `/run/c-backup-helper/helper.sock`. The directory is `0750
  root:<daemon gid>`, the socket `0660`. On every connection the helper
  reads `SO_PEERCRED` and refuses any peer whose uid is not the configured
  `daemon_uid`; the socket is mounted into the daemon's container and
  nowhere else. Sequenced packets keep message boundaries, so there is no
  framing to get wrong.
- **Messages.** One request, one response, each a single packet of at most
  64 KiB holding one UTF-8 JSON object:

  ```json
  {"v":1,"id":17,"op":"capture.begin","args":{"units":["docker:compose:immich"],"pause":false}}
  {"id":17,"ok":true,"result":{"capture":"c-5f0e...","read_roots":{...},"snapshot_ms":212}}
  {"id":17,"ok":false,"error":{"code":"denied","msg":"unit docker:Plex-Mover is not in the policy"}}
  ```

  Error codes are a closed set: `denied`, `invalid`, `busy`, `timeout`,
  `failed`, `unsupported`. Requests on one connection are handled in
  order; the daemon may hold a few connections for concurrent jobs.
- **File descriptors** travel only as `SCM_RIGHTS` ancillary data, and in
  1.0 only on one response: `hook.run` for a dump hook (the read end of a
  pipe the helper fills with the hook's demultiplexed stdout). Bulk data
  never travels in messages.
- **Handshake.** The first request must be `hello`; the helper answers
  with its version, the daemon id it is configured for, and a digest of
  its policy, which the daemon logs and shows in `GET /providers`.

### 11.3 Operations

The complete list for 1.0. Anything else is `denied`. There is **no**
operation that runs a command, takes a shell string, a path to execute, a
snapshot name, raw XML for libvirt, or a raw create body for Docker.

| Operation | Arguments (typed) | What the helper does |
|---|---|---|
| `hello` | `client_version` int | handshake |
| `units.list` | `provider` enum | enumerate, filtered by the policy's unit allowlist |
| `units.describe` | `unit` unit key | inspect / domain XML / LXC config, templates and compose files, read by the helper and returned as data |
| `capture.begin` | `units` 1 to 32 unit keys; `pause` bool | resolves every included mount itself, computes snapshot domains itself and intersects them with the policy's domains; pauses or freezes members if asked, `syncfs`, one atomic snapshot per pool with names it generates, unpauses; returns an opaque capture token, the read root of each mount, timings and per-member outcomes. Holds a lease with a deadline |
| `capture.begin_paths` | `paths` 1 to 64 absolute paths | source snapshot mode for plain file sources (`db-detection.md` 9.4): resolves each path to its snapshot domain itself, checks the domains against the policy, takes one atomic snapshot per pool with names it generates, no pause; returns a capture token and a read root per path. Same lease, expiry and sweep as `capture.begin` |
| `capture.end` | `capture` token | destroys that capture's snapshots |
| `hook.run` | `unit` unit key; `hook` name | runs the named hook from the policy, inside the unit's container (`container-backup.md` 6.6); returns a run token and, for a dump, a pipe fd |
| `hook.wait` | `run` token | exit status, timed-out flag, log (capped) |
| `vm.backup_begin` | `unit`; `disks` list of {`target` string, `mode` enum full/incremental}; `base` checkpoint name or null; `freeze` enum | builds the backup and checkpoint XML itself from these fields and its own configured socket and scratch paths; accepts `base` only if it is a checkpoint the helper created for this daemon id; freeze, begin, thaw with its own watchdog; returns socket, export and bitmap names, level, `freeze_ms` |
| `vm.backup_end` | `session` token; `outcome` enum | aborts the job, settles checkpoints (keep or delete the new one, delete the retired base) |
| `vm.checkpoint_redefine` | `unit`; `checkpoint` name | redefines from the XML the helper itself stored when it created the checkpoint. The helper also runs this for every recorded checkpoint at its own start, because Unraid keeps checkpoint metadata on the RAM rootfs (`vm-backup.md` 5.8) |
| `vm.install_state` | `unit` (the domain just defined or replaced); `staging` token | installs the staged NVRAM file at the path the domain's XML names and the TPM state directory at `tpm-states/<that domain's uuid>/`, with recorded owners and modes; refuses any other UUID and an existing state directory for a new UUID; never lists or touches other UUIDs' state (`vm-backup.md` 7.2) |
| `restore.stage` | `unit`; `mount` (a source path as recorded); `job` int | creates `<dataset root>/.cbackup-restore/<job>-<random>/` for that mount, owned by the daemon uid, opened without following symlinks and pinned until the swap (`helper.md`, B171); returns a staging token and the path |
| `restore.swap` | `staging` token | with the unit stopped, renames the staged restore into place and moves the old data aside (11.6); never deletes |
| `unit.stop`, `unit.start` | `unit` | for restore; policy `allow_restore` |
| `unit.define` | `unit`; `definition` (the snapshot's inspect, template, compose files, or domain XML) | policy `allow_define` only. The helper parses the definition field by field, rejects privileged settings unless `allow_privileged_define`, rewrites names, UUIDs and MACs itself, and serialises its own document; the daemon's bytes are never passed through |
| `lease.release` | `capture` or `session` token | early release |
| `sweep` | none | releases everything its own records say is held; destroys its own leftover artefacts |

Offline VM reads need no operation: the daemon spawns `qemu-nbd
--read-only` itself over image files it can already read.

### 11.4 Validation, on the helper side

- **Schema.** Exactly the keys listed for the operation. An unknown or
  missing key, a wrong JSON type, an integer out of range, or an enum value
  outside its set is `invalid`. Strings are bounded (unit keys 160 bytes,
  names 64, paths 4,096), valid UTF-8, with no NUL or control characters.
- **Identifiers.** A unit key is `<provider>:<id>`, the provider from the
  enum and the id matched against that provider's grammar (Docker
  container names `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`, compose projects
  `[a-z0-9][a-z0-9_-]{0,63}`, libvirt UUIDs, LXC names), then against the
  policy's unit globs.
- **Paths** are never trusted from the daemon. Mount sources come from the
  helper's own inspect, snapshot names from its own generator, read roots
  from its own snapshots. The one path the daemon names, `restore.stage`'s
  `mount`, must equal a mount source of that unit in the helper's current
  inspect.
- **Execution.** Every external tool runs through `posix_spawn` with an argv
  the helper builds (`nsenter -t 1 -m -- zfs snapshot -o
  com.cbackup:owner=<id> <ds>@<name> ...`); there is no shell anywhere.
- **Tokens** (capture, session, run, staging) are 128-bit random values the
  helper issues and records; an unknown token is `invalid`.
- **Rates and bounds.** Pause and freeze leases are capped at
  `max_pause_s` and released by the helper's own watchdog; captures are
  rate-limited (`max_captures_per_hour`, default 60) and expire at
  `capture_max_s` (default 24 h), when the helper destroys their snapshots
  itself.

The policy is on the flash, root-owned, and the helper refuses to start if
it is group- or world-writable:

```ini
[helper]
daemon_uid = 990
daemon_id = 7f3a09c1
max_pause_s = 30
max_captures_per_hour = 60
capture_max_s = 86400
allow_restore = 1
allow_define = 0
allow_privileged_define = 0

[units]
allow = docker:compose:immich, docker:compose:gitlab_server, docker:Grafana, docker:cbk-uat-*, libvirt:cbk-uat-*, lxc:cbk-uat-*

[snapshot]
zfs_domains = cache/appdata, cache/Photos, cache/system, cache/lxc
btrfs_domains =

[hook immich-pg_dumpall]
unit = docker:compose:immich
kind = dump
phase = pre
container = immich_postgres
argv.0 = pg_dumpall
argv.1 = --clean
argv.2 = --if-exists
argv.3 = -U
argv.4 = postgres
timeout_s = 900
on_fail = continue
```

### 11.5 What a compromised daemon can still do

Assume an attacker runs code in the daemon (a parser bug, a stolen admin
session token).

| Can | Bound |
|---|---|
| read everything the daemon's mounts expose (appdata, images, repositories, keys) | unchanged by any option: reading is the daemon's job. Encrypted repositories' keys are in its state volume |
| pause or freeze allowed units | at most `max_pause_s` each and `max_captures_per_hour` per hour: a nuisance, bounded |
| create snapshots of allowed domains | names chosen by the helper; expired after `capture_max_s` |
| start VM backups of allowed domains | copy-before-write load and scratch space, bounded by the scratch reserve check |
| run hooks | only those named in the policy, with their fixed argv, inside the unit's own containers |
| stop units and swap restored data in (`allow_restore`) | only allowed units; the previous data is moved aside, never deleted, so the operator can reverse it |
| define units (`allow_define`, off by default) | never privileged ones unless `allow_privileged_define` |

| Cannot |
|---|
| run any command of its choosing, on the host or in a container |
| name a path, dataset, snapshot or checkpoint for the helper to act on |
| touch a unit, dataset or subvolume outside the policy |
| delete a snapshot or checkpoint the helper did not create |
| mount anything, reach the host network, or talk to dockerd or libvirtd directly |
| change the policy (a root-owned file on the flash, read at helper start) |

Under option A every row of the second table becomes possible.

### 11.6 The daemon's real capability set

"Unprivileged" means exactly this:

| | Value |
|---|---|
| uid / gid | a fixed non-root uid, `cbackup` (990 by default, configurable). Not uid 0: a uid-0 process with every capability dropped still owns every root-owned file, so it passes owner checks on them (it can write root's 0600 files in any read-write mount and read them without `CAP_DAC_READ_SEARCH`), and in Unraid's Docker (no user-namespace remapping) container uid 0 is host uid 0 |
| effective capabilities, normally | `CAP_DAC_READ_SEARCH` only: read any file and search any directory in its mounts. Enough for every backup |
| permitted, raised only by restore threads | `CAP_DAC_OVERRIDE` (write into directories others own), `CAP_CHOWN` (restore owners), `CAP_FOWNER` (modes, times, xattrs and ACLs on files it does not own), `CAP_FSETID` (keep setuid/setgid across `chown`), `CAP_MKNOD` (device nodes, an LXC rootfs's `/dev`) |
| at startup only | `CAP_SETUID`, `CAP_SETGID` and `CAP_SETPCAP`: the container must add them, because the daemon performs its own drop (SETPCAP to empty the bounding set of everything but the six, SETGID for `setgroups`/`setresgid`, SETUID for `setresuid`). All three are out of every set once the uid change is done |
| everything else | not in the bounding set: no `CAP_SYS_ADMIN`, `CAP_SYS_PTRACE`, `CAP_NET_ADMIN`, and no `CAP_SETUID`, `CAP_SETGID` or `CAP_SETPCAP` after start |
| namespaces | its own pid and network namespaces (a published TCP port for the API); no `--pid=host`, no `--privileged` |
| devices and sockets | none: no `/dev/zfs`, no Docker socket, no libvirt socket; only the helper socket and the NBD/export socket directory |
| mounts | sources read-only (`rslave`, for snapshot visibility); repository and state read-write, owned by 990; restore staging directories read-write (below) |

How it gets there: Docker gives a non-root `--user` none of the capabilities
added with `--cap-add` (it sets no ambient capabilities), so the container
starts as root with `--cap-drop ALL` plus the six capabilities above and
`CAP_SETUID`, `CAP_SETGID` and `CAP_SETPCAP` (without which the drop
cannot happen), and c-backupd drops privilege itself before it serves
anything: every capability but the six out of the bounding set,
`prctl(PR_SET_KEEPCAPS)`, `setgroups`, `setresgid`/`setresuid` to 990,
`capset` to effective = `CAP_DAC_READ_SEARCH` and permitted = the six
(SETUID, SETGID and SETPCAP gone), then `PR_SET_NO_NEW_PRIVS`. AppGuard has
already opened the config and `state.db` by then, so the state volume is
owned by 990. The uid is the new config key
`security.run_as_uid` (default 0, meaning no drop, so existing
deployments behave as today; the Docker image for workload backup sets
990). Restore worker threads raise the five restore
capabilities into their own effective set (capabilities are per thread on
Linux) and drop them when the job ends; backup threads never hold them.
That guards against bugs, not against an attacker with code execution, who
can raise anything in the permitted set. Against that attacker the
boundary is the mounts.

**Restore's needs.** Writing and `chown`ing restored files is root over
whatever the daemon can write, so the daemon never gets a read-write mount
of live appdata. A restore into place goes through `restore.stage`: the
helper creates `<dataset root>/.cbackup-restore/<job>/` on the same dataset
as the target, which the daemon sees read-write through a single mount of
`<dataset root>/.cbackup-restore`. The daemon restores into it with full
ownership, modes, xattrs and device nodes, and the helper then swaps it into
place with `restore.swap` while the unit is stopped: a rename within one
dataset, instant, with the previous data moved aside. A restore to a new
location (`--dest`) needs a read-write mount the operator adds for it.
Under option A, restore needs read-write mounts of the targets themselves.

**What the drop costs.** Without `CAP_SYS_ADMIN`, `trusted.*` extended
attributes cannot be read; they are skipped with one warning per
filesystem (appdata does not use them, and overlay2's `trusted.overlay.*`
lives in image layers, which are not backed up). `O_NOATIME` needs owner or
`CAP_FOWNER`, so the scanner opens without it when refused. Everything else
a backup captures (content, modes, owners, times, `user.*` and `security.*`
xattrs, POSIX ACLs, device numbers) is readable with
`CAP_DAC_READ_SEARCH`.

Under option A the same daemon would need `CAP_SYS_ADMIN`, `/dev/zfs`, the
Docker and libvirt sockets and `--pid=host`: in practice `--privileged`.

### 11.7 Hooks and define

Hooks run inside the unit's containers (`docker exec`, `lxc-attach`),
never on the host; there are no host hooks in 1.0 (`container-backup.md`
6.6). Under option B their definitions live in the helper's policy and the
daemon can only name them. Under option A they live in enrolment, so the
API can run commands as root inside those containers, which is one more
reason for B.

Restore-and-define is the other root-equivalent operation: re-creating a
domain or container from a snapshot can bring back `privileged: true`, host
device passthrough or a bind of `/`. `restore_preflight` lists every such
setting and the API refuses to define without
`"accept_privileged": true` when any is present; under option B the helper
enforces its own `allow_privileged_define` as well. An unencrypted
repository is not authenticated (content addressing detects damage, not a
forger who rewrites names), so on such a repository define is refused
unless the config sets `units.define_from_unencrypted: 1`.

### 11.8 Who else can see a capture snapshot

A capture snapshot is a frozen, read-only copy of a whole dataset (ZFS) or
a whole pool (FS2's btrfs), and it is visible beyond c-backup while it
exists (survey section 7, U3 and U4):

| Path to it | Who | Exposure |
|---|---|---|
| `<dataset root>/.zfs/snapshot/<name>` (`snapdir=hidden`: unlisted, reachable by name) | any container or process with a mount of that **dataset's root**. On FS1 today: Plex-Mover, which binds `/mnt/cache` read-write | every unit's data in that dataset as of the capture instant |
| `/mnt/user/<share>/.zfs/...`, if shfs resolves it (unverified) | containers binding `/mnt/user` (Plex-Mover again) | the same |
| `<pool>/system/.cbackup-snapshots/<name>` and its shfs alias `/mnt/user/system/.cbackup-snapshots/<name>` (btrfs) | anything that can read the pool root or `/mnt/user/system` | the whole pool as of the capture instant |
| NFS or SMB | nobody, by construction: the snapshot directory is refused inside an exported share, and `.zfs` is not under FS1's exported shares | none |

What this adds over what those processes can already read is narrow,
because a mount that reaches the snapshot also reaches the live data: data
deleted, or made unreadable, after the capture instant stays readable until
the snapshot is destroyed. So the defences are about time and about not
widening anything:

- snapshots live only while their unit is read, are destroyed immediately
  after, and are bounded by a lease the helper enforces itself
  (`container-backup.md` 8.3);
- c-backup never changes `snapdir`, never creates a snapshot directory in an
  exported share, and never puts one at a pool's top level;
- the operator should know which containers bind a dataset root or
  `/mnt/user` (on FS1, Plex-Mover): `GET /providers` lists them as able to
  read capture snapshots, and the Manual's security chapter says so.

**Decided (user, 2026-11-16): option B**, the privileged helper sidecar as
specified in 11.1 to 11.8, built in phase C2 of the container work.
Option A remains only as the in-process transport for tests and
development.

## 12. Restore framework

The core restores data; the provider re-creates definitions.

| Mode | What happens | Provider involvement |
|------|--------------|----------------------|
| `files` | The ordinary restore of a subtree to a directory: a disk becomes a sparse raw file, a volume becomes a directory | none |
| `item` | One item to a target: a disk to a raw or qcow2 image or a block device, a volume to its original path or elsewhere | format conversion for qcow2 (`vm-backup.md` 7.1) |
| `unit` | Every item to target locations chosen by a plan, then `define` | `restore_preflight`, `define` |
| `export` | A disk served read-only over NBD for file-level recovery | none (daemon NBD server, `vm-backup.md` 7.3) |

Restoring over a live unit is refused: the target unit must be stopped (or
absent). Replacing a unit in place requires `"replace": true` and runs
`preflight` first, which states what will be overwritten.

## 13. Testing through the interface

The fake provider (`tests/fake_provider.c`) is the reason the interface is
shaped as it is: every behaviour above is testable without a hypervisor or
a Docker daemon.

- Units with in-memory sparse disks (an extent map over a seeded PRNG
  generator, so terabyte disks cost no memory), BLOB items, and TREE items
  over temp directories.
- Scripted mutation between sessions: in-place writes, zero fills, hole
  punches, resizes, items added and removed, a unit renamed or redefined.
- Scripted `changed` sets, including **deliberately wrong** ones (a missed
  range) to prove the audit catches them.
- Fault injection at every call (`begin` fails after the freeze, `end`
  fails, a read fails at byte N, the watchdog fires mid-`begin`), driven by
  the existing `fault_inject.c` style.
- Assertions: an incremental session's item root equals a full read's root
  for the same state (the central invariant, `vm-backup.md` 5.3); intents
  are empty after every path; `sweep` after a simulated crash releases
  everything and deletes nothing foreign; per-group GFS keeps each head.

## 14. How each workload maps

| Interface | libvirt | Docker | LXC (Unraid plugin) |
|-----------|---------|--------|---------------------|
| unit | domain (UUID) | container (name) | container (name) |
| BLOB items | domain XML, NVRAM, TPM state | inspect, image inspect, Unraid template, compose files | config file, included files under the LXC path |
| BLOCK items | each disk | - | - |
| TREE items | - | named volumes, bind mounts under include roots | rootfs, bind mounts under include roots |
| point in time | libvirt backup job (pull), optional agent freeze | pause, syncfs, btrfs/ZFS snapshot, unpause | `lxc-freeze`, syncfs, snapshot, `lxc-unfreeze` |
| incremental | checkpoints / dirty bitmaps (qcow2) | file cache over snapshots | file cache over snapshots |
| define | `virDomainDefineXML` | Engine API create + image pull, or Unraid template | write config + rootfs, optional `lxc-start` |

Later providers fit the same shape: Proxmox (`qm`/QEMU backups are libvirt-
like with their own API), Incus/LXD (instances with storage-pool snapshots),
Podman (Docker-compatible API), Hyper-V (RCT change tracking as `changed`).

## 15. Decisions and open questions (common)

Decided by the user (2026-11-16):

| Question | Decision |
|---|---|
| Security model | Option B, the helper sidecar (section 11) |
| Secrets in captured configuration | Never stripped: backup does not mutate what it captures. Domain XML, `docker inspect` output and templates are stored verbatim; an unencrypted repository holds them in the clear, and the Manual says so plainly |
| Repository for FS1 workloads | An FS1 array disk by disk path, replicated daily to FS2 (`vm-backup.md` 9.1) |

The provider-specific decisions are listed in `vm-backup.md` section 13
and `container-backup.md` section 16. Database detection, which replaces
per-application hook decisions such as the Immich dump, is designed and
approved in `db-detection.md` (decisions in its section 13), to be built in
phases D0 to D4 after the provider work. Two of its decisions bear on the
helper: only templates that write nothing inside an application's data are
allowed by default (the rest are opt-in per host in the policy), and source
snapshot mode for plain file sources is 1.0, through `capture.begin_paths`
(11.3).

Not yet asked, with the proposals standing:

1. Per-unit retention defaults: inherit the repository policy (proposed), or
   ship a separate default for units?
2. Should a repository mix file snapshots and unit snapshots? Proposed:
   allowed (retention groups make it safe), but the UI suggests one
   repository per host for workloads, separate from file backups.
3. `units.allow` as the UAT guard rail (8.3).
