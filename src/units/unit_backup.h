#pragma once

/*
 * Unit backups: one snapshot per unit per run (docs/source-providers.md
 * 7.1), assembled through the existing backup engine.
 *
 * For each unit selected: check units.allow, take the unit's process-wide
 * mutex, describe it, begin a session (the provider journals each hold
 * before taking it), and run backup_run with
 *
 *   - BLOB items as synthetic files under /.cbackup-unit/config/,
 *   - BLOCK items as synthetic files under /.cbackup-unit/disks/, their
 *     content the extent stream under the zero-run rule (block_source.h),
 *   - TREE items scanned from the provider's read root and recorded at
 *     their original paths,
 *   - /.cbackup-unit/unit.json last, describing what the session did,
 *
 * with the snapshot_units rows written in the snapshot row's transaction.
 * Then end the session, COMMITTED with the snapshot id or ABORTED. A unit
 * that fails leaves no snapshot and does not stop the others: a failure in
 * unit 17 keeps units 1 to 16.
 */

#include "error.h"
#include "provider.h"
#include "store.h"

#include <cutils/db.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    const char  *provider;
    /* Every unit the provider lists now (fire time for a schedule), less
     * those whose name matches an exclude glob. */
    int          all;
    const char **exclude;
    size_t       n_exclude;
    /* Or these, each a unit name or id. */
    const char **units;
    size_t       n_units;

    sp_consistency_t require;    /* below this a unit fails; default crash */
    int64_t      job_id;
    atomic_int  *cancel;
    void (*progress_cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *progress_ctx;
    void (*bytes_cb)(void *ctx, uint64_t done, uint64_t total);
    unsigned     store_workers;
    int          file_cache_match;
    /* Pause scope (container-backup.md 6.2): each unit its own capture
     * (unit), units sharing a capture.group in one (group), or every unit
     * of the job in one (job). Group and job need a provider with
     * begin_group; without one every unit is its own capture. */
    int          pause_scope;          /* UNIT_SCOPE_* */
} unit_backup_opts_t;

#define UNIT_SCOPE_UNIT  0
#define UNIT_SCOPE_GROUP 1
#define UNIT_SCOPE_JOB   2

typedef struct {
    char             unit_key[SP_PROVIDER_MAX + SP_UNIT_ID_MAX + 1];
    char             name[256];
    int64_t          snapshot_id;    /* 0 when it failed */
    status_t         st;
    char             error[512];
    sp_consistency_t achieved;
    uint64_t         bytes_read;     /* disk bytes read */
    uint64_t         files;
    uint64_t         freeze_ms;      /* how long the unit was frozen; 0 when it was not */
    unsigned         n_warnings;
    char             warning[512];   /* the first, or the one that outranks the rest */
    char           **warnings;       /* all n_warnings of them, owned (B202) */
} unit_backup_one_t;

typedef struct {
    unit_backup_one_t *units;
    size_t             n;
    size_t             ok, failed;
    size_t             filtered;     /* left out by units.allow under --all */
    /* The job's own warning, apart from any unit's: a hold the provider
     * cannot release (the end-of-job sweep). Empty when none. */
    char               warning[400];
    char               scope_note[256]; /* a pause scope the provider could not honour, or "" */
} unit_backup_result_t;

/*
 * OK when every selected unit was backed up; otherwise the first failure,
 * with every unit's outcome in *out either way (free with
 * unit_backup_result_free). A named unit outside units.allow, or one the
 * provider does not list, fails as that unit.
 */
status_t unit_backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                         const unit_backup_opts_t *opts, unit_backup_result_t *out);
void     unit_backup_result_free(unit_backup_result_t *r);

/*
 * The daemon's start-up step for checkpoints (vm-backup.md 5.8): every
 * checkpoint a libvirt token names is checked by the helper, which puts back
 * any libvirt lost from the XML it stored when it made it (Unraid loses all
 * of them at every reboot). Best effort: a checkpoint that cannot be put
 * back costs its disks one full read at their next session, which checks
 * again. *checked, *redefined and *refused (any may be NULL) count them.
 */
status_t unit_checkpoints_redefine(cutils_db_t *db, unsigned *checked, unsigned *redefined,
                                   unsigned *refused);
