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
    int64_t  export_id;   /* refused: this open NBD export pins it; else 0 */
} prune_info_t;

/*
 * Remove one snapshot on an operator's request. ERR_NOT_FOUND when there is
 * no such snapshot. A tagged snapshot is refused with ERR_INVALID and
 * out->ntags set unless force is non-zero: a tag is how an operator marks a
 * snapshot as one to keep, and a delete used to remove it and its tags
 * without a word. A snapshot an open NBD export pins is refused with
 * ERR_BUSY and out->export_id set, force or not. The check runs in the
 * deleting transaction. out may be NULL.
 */
status_t prune_snapshot_checked(store_t *store, cutils_db_t *db, int64_t repo_id,
                                int64_t snap_id, int force, prune_info_t *out);

/*
 * Finish every prune a crash or power cut interrupted (B148), for one
 * repository or, with 0, all. A prune commits the removal of the rows with
 * a prune-pending-<id> marker in recovery_state, unlinks the record, syncs
 * snapshots/ and clears the marker. Until the marker is cleared the record
 * may still be there, and it is not a snapshot the index lost: reconcile or
 * reindex taking it for one would bring back a snapshot whose objects gc
 * has swept. So this deletes the record, syncs, and clears the marker, and
 * runs at start before reconcile and at the start of every reindex.
 * Repositories that cannot be read keep their markers. *out_finished (may
 * be NULL) counts the records removed.
 */
status_t prune_finish_pending(cutils_db_t *db, int64_t repo_id, unsigned *out_finished);
