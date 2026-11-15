#pragma once

/*
 * Snapshot deletion.
 *
 * Removing a snapshot is two things that must not come apart: its record in
 * the repository and its rows in the index. The record goes last, because a
 * repository holding a snapshot the index has forgotten is recoverable by
 * reindexing, whereas an index describing a snapshot whose record is gone is
 * simply wrong.
 *
 * Nothing is unlinked from objects/ here. Deciding what is unreachable is
 * GC's job, and it can only be answered by looking at every remaining
 * snapshot at once.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

/* Remove one snapshot unconditionally, tags and all. For retention, which
 * has already decided (and never picks a tagged snapshot). A snapshot that
 * does not exist is not an error. */
status_t prune_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                        int64_t snap_id);

/* What a checked delete found: the snapshot's tags, removed with it or the
 * reason it was refused. */
typedef struct {
    unsigned ntags;
    char     tags[512];   /* names, comma separated, cut with "..." */
} prune_info_t;

/*
 * Remove one snapshot on an operator's request. ERR_NOT_FOUND when there is
 * no such snapshot. A tagged snapshot is refused with ERR_INVALID and
 * out->ntags set unless force is non-zero: a tag is how an operator marks a
 * snapshot as one to keep, and a delete used to remove it and its tags
 * without a word. The check runs in the deleting transaction. out may be
 * NULL.
 */
status_t prune_snapshot_checked(store_t *store, cutils_db_t *db, int64_t repo_id,
                                int64_t snap_id, int force, prune_info_t *out);
