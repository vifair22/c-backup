#pragma once

/*
 * Source providers: the interface that lets c-backup back up things that are
 * not plain directory trees - virtual machines, Docker and LXC containers -
 * as first-class units with their own history, consistency, retention and
 * restore. docs/source-providers.md is the specification; this header is its
 * section 3, with the types that section names but does not spell out
 * filled in.
 *
 * A provider knows one kind of workload. A unit is one backed-up thing,
 * identified by (provider, unit_id); its items are the pieces it is made of:
 * BLOB (small configuration, at most 16 MiB), BLOCK (a disk, as a stream of
 * extents) and TREE (a directory, read through the ordinary scanner).
 *
 * The control plane (begin/end: freezing, pausing, snapshotting) and the
 * data plane (blob_read, block_open, tree_root) are separate on purpose:
 * the host helper of section 11 moves the first out of the network-facing
 * daemon and leaves the second where the bytes are.
 *
 * Errors follow the codebase: status_t, the deepest function sets the
 * message. "Incremental not possible" is a plan with SP_READ_FULL and a
 * reason, never an error; errors mean "this unit cannot be backed up now".
 */

#include "error.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define SP_ABI_VERSION 1

/* What point-in-time guarantee a session achieved (section 5). Ordinal for
 * display and for `require`; a statement of fact about one session. */
typedef enum {
    SP_CONSIST_INCONSISTENT = 0, /* read live; no single point in time */
    SP_CONSIST_CRASH        = 1, /* one point in time, as after a power cut */
    SP_CONSIST_QUIESCED     = 2, /* guest/container filesystems flushed and
                                    frozen at that point (guest agent) */
    SP_CONSIST_APPLICATION  = 3, /* quiesced, plus application participation:
                                    VSS writers, or a pre hook that succeeded */
    SP_CONSIST_OFFLINE      = 4, /* the unit was not running while read */
} sp_consistency_t;

/* STREAM: bytes of unknown length produced while the unit is read (a dump
 * hook's stdout), stored like a BLOB under the unit directory but never held
 * in memory, and left out of the snapshot when its producer fails. */
typedef enum { SP_ITEM_BLOB = 1, SP_ITEM_BLOCK = 2, SP_ITEM_TREE = 3, SP_ITEM_STREAM = 4 } sp_item_kind_t;

typedef enum {
    SP_STATE_UNKNOWN   = 0,
    SP_STATE_RUNNING   = 1,
    SP_STATE_PAUSED    = 2,
    SP_STATE_STOPPED   = 3,
    SP_STATE_SUSPENDED = 4,
} sp_state_t;

/* Unit capabilities (sp_unit_desc_t.caps) and item capabilities. */
#define SP_CAP_QUIESCE      0x01u  /* can freeze (guest agent present) */
#define SP_CAP_PAUSE        0x02u  /* can pause the whole unit */
#define SP_CAP_SNAPSHOT     0x04u  /* item lives on an instantly snapshottable fs */
#define SP_CAP_INCREMENTAL  0x08u  /* item can report changed ranges vs a token */
#define SP_CAP_DEFINE       0x10u  /* provider can re-create the unit on restore */
#define SP_CAP_PARALLEL     0x20u  /* items of one session may be read concurrently */

/* Bounds the core holds every provider to. */
#define SP_PROVIDER_MAX   16       /* provider name, NUL included */
#define SP_UNIT_ID_MAX    128      /* unit_id, NUL included */
#define SP_ITEM_ID_MAX    64       /* item id, NUL included */
#define SP_BLOB_MAX       ((size_t)16 * 1024 * 1024)

typedef struct {
    char           id[SP_ITEM_ID_MAX]; /* stable within the unit: "vda", "vol:pgdata" */
    sp_item_kind_t kind;
    uint64_t       size;            /* BLOCK: virtual size; BLOB: bytes; TREE: 0 */
    char           format[16];      /* BLOCK: "raw", "qcow2", ...; else "" */
    char          *source;          /* host path or URI, informational */
    /*
     * Where it lands in the snapshot tree. BLOB and BLOCK: a path relative
     * to the snapshot root inside the reserved directory
     * (".cbackup-unit/config/domain.xml", ".cbackup-unit/disks/vda").
     * TREE: the absolute path the data is recorded at, which is where a
     * plain file restore puts it back.
     */
    char          *tree_path;
    uint32_t       caps;
    /* BLOCK: the image file's mtime, 0 when there is none. Real metadata,
     * carried on the disk's node (section 7.2). Not in the section 3
     * sketch; the libvirt provider has it and the node needs it. */
    int64_t        mtime_sec;
} sp_item_desc_t;

typedef struct {
    char            provider[SP_PROVIDER_MAX];
    char            unit_id[SP_UNIT_ID_MAX];
    char           *name;           /* display name at describe time */
    int             state;          /* sp_state_t */
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

/*
 * The hold journal (section 6.2), handed to begin by the core. A provider
 * calls record before it takes anything that stays held outside the daemon
 * (a freeze, a pause, a filesystem snapshot, an export, a new checkpoint),
 * and the row is committed before record returns; it calls clear once the
 * matching release has succeeded. A crash between the two leaves the row,
 * and the next start's sweep releases what it names.
 */
typedef struct {
    status_t (*record)(void *ctx, const char *kind, const char *detail,
                       int64_t *out_id);
    status_t (*clear)(void *ctx, int64_t id);
    void      *ctx;
} sp_holds_t;

/* Hold kinds, the vocabulary of unit_intents.kind. */
#define SP_HOLD_FREEZE          "freeze"
#define SP_HOLD_PAUSE           "pause"
#define SP_HOLD_FS_SNAPSHOT     "fs_snapshot"
#define SP_HOLD_EXPORT          "export"
#define SP_HOLD_CHECKPOINT_NEW  "checkpoint_new"

typedef struct {
    int64_t                job_id;
    const sp_item_base_t  *bases;   /* from state.db (section 7.4) */
    size_t                 n_bases;
    sp_consistency_t       want;    /* best effort target */
    sp_consistency_t       require; /* below this, begin fails */
    unsigned               freeze_timeout_s;
    unsigned               pause_max_s;
    atomic_int            *cancel;
    /* Not in the section 3 sketch: how a provider journals its holds. */
    const sp_holds_t      *holds;
    /* The per-daemon id artefacts are named with (section 6.3), 8 hex. */
    const char            *daemon8;
    /* pause-read (container-backup.md section 7): when the unit has mounts
     * on no snapshot domain, pause it and keep the pause past begin
     * (SP_CF_PAUSE_HELD) until release, so the caller's second pass over
     * those mounts is consistent with the snapshot. */
    int                    hold_pause;
} sp_begin_req_t;

typedef struct {
    const char     *item_id;
    sp_read_mode_t  mode;
    const char     *reason;         /* bounded vocabulary, section 6.4 */
} sp_item_plan_t;

/* consist_flags bits. */
#define SP_CF_HOOK_PRE_OK       0x01u
#define SP_CF_FROZEN            0x02u
#define SP_CF_PAUSED            0x04u
#define SP_CF_FS_SNAPSHOT       0x08u
#define SP_CF_WATCHDOG_RELEASED 0x10u
#define SP_CF_PAUSE_HELD        0x20u  /* begin returned with the unit still paused */

/* Filled by begin, read by the job, owned by the session. */
typedef struct {
    sp_consistency_t achieved;
    uint32_t         consist_flags; /* SP_CF_* */
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

/* What a provider is started with. spec is provider-specific (the fake
 * provider's: a JSON file of units); daemon8 names its artefacts. */
typedef struct {
    const char *spec;
    const char *daemon8;
} sp_config_t;

/* probe: what GET /providers shows. Never touches a unit. */
typedef struct {
    int  available;          /* the provider can work at all */
    char version[64];
    char detail[256];
} sp_probe_t;

/* list_units callback: unit id, display name, sp_state_t. Anything but OK
 * stops the enumeration and is what list_units returns. */
typedef status_t (*sp_unit_cb)(void *cbctx, const char *unit_id,
                               const char *name, int state);

/* One unit_intents row, as sweep is given it. */
typedef struct {
    int64_t     id;
    const char *unit_id;
    const char *kind;           /* SP_HOLD_* */
    const char *detail;
    int64_t     job_id;
} sp_intent_t;

/*
 * sweep (section 6.3): release the holds listed, idempotently, and with
 * scan_artefacts also delete leftover artefacts of this daemon that no
 * live job owns and that pass the provider's ownership proof. released
 * (caller's, n_intents long, may be NULL) gets 1 for each intent whose
 * release succeeded, so the core deletes exactly those rows.
 *
 * absent (caller's, n_intents long, may be NULL) gets 1 for each intent
 * under which nothing was held. That release still succeeds, as a no-op,
 * and the start-up and end-of-job sweeps delete the row. The watchdog
 * keeps it: it sweeps holds of live jobs, and a row is recorded before its
 * hold is taken, so nothing held may mean not taken yet. Deleting the row
 * then would leave the hold, once taken, unjournalled and unbounded. The
 * helper's watchdog does the same with its own journal. Only freezes and
 * pauses are timed, so a provider must fill it for those kinds; for the
 * rest it may leave 0.
 */
typedef struct {
    const char        *daemon8;
    const sp_intent_t *intents;
    size_t             n_intents;
    int               *released;
    int               *absent;
    int                scan_artefacts;
    const int64_t     *live_jobs;
    size_t             n_live_jobs;
} sp_sweep_req_t;

typedef struct {
    unsigned released;       /* holds released */
    unsigned removed;        /* artefacts deleted */
    unsigned left_foreign;   /* artefacts seen and left alone */
    unsigned failed;         /* releases or deletions that failed */
} sp_sweep_report_t;

/*
 * Restore support (section 12). Only the shapes are fixed here; no provider
 * in this build defines units, and the core's `files` and `item` modes need
 * none of it.
 */
typedef struct {
    int64_t     repo_id;
    int64_t     snap_id;
    const char *unit_id;
    const char *dest;
    int         replace;
} sp_restore_req_t;

typedef struct {
    int  collisions;
    int  privileged;
    char detail[512];
} sp_restore_plan_t;

typedef struct {
    char unit_id[SP_UNIT_ID_MAX];
} sp_define_result_t;

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
     * that already committed. Frees the session. */
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
     * snapshot's config blobs, after the core has written the data. NULL
     * when the provider cannot define units. */
    status_t (*restore_preflight)(sp_ctx_t *ctx, const sp_restore_req_t *req,
                                  sp_restore_plan_t *out);
    status_t (*define)(sp_ctx_t *ctx, const sp_restore_req_t *req,
                       const sp_restore_plan_t *plan, sp_define_result_t *out);

    /* STREAM items. stream_open hands over a pipe the bytes arrive on, read
     * to EOF by the caller and closed by it; stream_close then says whether
     * the producer succeeded (*ok), and only then is the item kept. NULL
     * when the provider has no streams. */
    /* One capture over several units (pause scope group or job,
     * container-backup.md 6.2): every member of every unit paused together
     * when a pause is called for, one atomic snapshot per pool for all of
     * them, then one session per unit, read and ended separately; the
     * shared snapshots go with the last end. NULL: each unit begins on its
     * own. */
    status_t (*begin_group)(sp_ctx_t *ctx, const sp_unit_desc_t *const *units, size_t n,
                            const sp_begin_req_t *req, sp_session_t **out,
                            const sp_session_info_t **infos);

    /* Let go a pause begin kept (SP_CF_PAUSE_HELD). *cap_hit 1 when it had
     * already been released by the watchdog at the pause cap, so what was
     * read under it is not all from one instant. NULL: never held. */
    status_t (*release)(sp_session_t *s, int *cap_hit);

    status_t (*stream_open)(sp_session_t *s, const sp_item_desc_t *item, int *fd);
    status_t (*stream_close)(sp_session_t *s, const sp_item_desc_t *item, int *ok);
} sp_provider_ops_t;

/* Names, for unit.json, the API and the index. NULL for a value outside
 * the enum; the parsers return -1 for a name they do not know. */
const char *sp_consistency_name(sp_consistency_t c);
int         sp_consistency_parse(const char *name, sp_consistency_t *out);
const char *sp_item_kind_name(sp_item_kind_t k);
int         sp_item_kind_parse(const char *name, sp_item_kind_t *out);
const char *sp_state_name(int state);
const char *sp_read_mode_name(sp_read_mode_t m);

/* Whether a read-mode reason is in the bounded vocabulary of section 6.4. */
int         sp_reason_known(const char *reason);
/* The vocabulary's own static string for reason, or NULL: for a reason
 * that arrived as text (from the helper) and has to outlive it. */
const char *sp_reason_canonical(const char *reason);

/* Free a session info a provider built with the helpers below. */
void        sp_session_info_free(sp_session_info_t *info);
/* Append a warning (copied). ERR_NOMEM on failure. */
status_t    sp_session_info_warn(sp_session_info_t *info, const char *fmt, ...)
            __attribute__((format(printf, 2, 3)));

/* Deep copy and free of a unit description, for providers that keep one. */
status_t    sp_unit_desc_dup(const sp_unit_desc_t *in, sp_unit_desc_t **out);
void        sp_unit_desc_free(sp_unit_desc_t *d);
