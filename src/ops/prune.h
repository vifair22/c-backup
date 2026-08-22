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

status_t prune_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                        int64_t snap_id);
