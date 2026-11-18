#pragma once

/*
 * Restoring a unit as a unit (container-backup.md 11.2): its data, then its
 * definition re-created by the provider through the helper.
 *
 *   in place   the original unit, replaced: requires `replace`. The unit is
 *              stopped; each tree is restored into a staging directory in
 *              its own snapshot domain (restore.stage) and swapped in
 *              (restore.swap), the old data moved aside, never deleted;
 *              then unit.define with replace.
 *   new name   requires `target_root`: each tree is restored under
 *              target_root/<its original host path>, and the definition's
 *              bind sources (Docker), rootfs and mount sources (LXC) are
 *              rewritten to those paths before unit.define. Nothing is ever
 *              written to the original's paths.
 *
 * The definition goes to the helper as a sealed memfd, so its size is not
 * bounded by a packet.
 */

#include "error.h"
#include "restore.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    const char *name;          /* NULL or "": the original name */
    const char *target_root;   /* required for a new name */
    int         replace, pin_digest, start, keep_macs;
    restore_opts_t ropts;      /* progress and cancel for the data */
} unit_define_opts_t;

typedef struct {
    char *json;                /* the job result, malloc'd: what was done, the helper's answer */
    char *warnings[16];        /* the helper's warnings, malloc'd, for the journal */
    size_t n_warnings;
} unit_define_result_t;

/* Whether the request is acceptable for the unit (in place needs replace,
 * a new name needs a target_root). 0, or -1 with why. */
int unit_define_check(const char *unit_id, const unit_define_opts_t *o, char *why, size_t wsz);

/* name as unit_define_run takes it: NULL when it is the unit's own id or
 * the name the snapshot recorded for it, which is the unit in place, not a
 * new name. */
const char *unit_define_name(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                             const char *unit_id, const char *name);

status_t unit_define_run(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                         int64_t job_id, const unit_define_opts_t *o, unit_define_result_t *out);
void     unit_define_result_free(unit_define_result_t *r);

/* The definition unit_define_run would send for prov:unit, as JSON text
 * (malloc'd); for tests and for showing a restore before running it. */
status_t unit_define_build(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           const char *prov, const char *unit, const unit_define_opts_t *o, char **text);

/* A file of the snapshot (a path relative to its root) into memory, at
 * most max bytes. ERR_NOT_FOUND when it is not there. */
status_t unit_read_file(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                        const char *rel, size_t max, char **data, size_t *len);

/* The recorded type (NODE_TYPE_*) of the entry at rel in the snapshot.
 * ERR_NOT_FOUND when there is none. */
status_t unit_entry_type(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                         const char *rel, uint8_t *type);

/* The name recorded_types gives an entry (B210, NIT-44): "missing" when
 * not found, else "reg", "dir", "lnk", "link", "fifo", "chr", "blk",
 * "sock" for an unclassified entry (a socket) or "other". */
const char *unit_recorded_type_name(int found, uint8_t type);
