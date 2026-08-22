#pragma once

/*
 * Garbage collection.
 *
 * Mark and sweep over the manifest tree graph, not refcounting. Refcounts
 * drift: every path that adds or removes a reference has to adjust them
 * correctly forever, and a single missed decrement leaks an object while a
 * single missed increment deletes live data. Marking recomputes reachability
 * from the snapshot records each time, so it is self-correcting, and a
 * reachability walk is exactly what the tree structure already supports.
 *
 * The refcount column is used as the mark bit, which keeps it meaningful for
 * anything that wants to know how many snapshots reference an object.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    uint64_t reachable;
    uint64_t swept;
    uint64_t bytes_freed;
} gc_result_t;

status_t gc_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                int dry_run, volatile int *cancel, gc_result_t *out);
