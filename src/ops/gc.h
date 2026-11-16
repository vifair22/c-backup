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
#include "pack.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    uint64_t reachable;
    uint64_t swept;
    uint64_t bytes_freed;
    /* Pack compaction, the second half of the sweep: entries surviving a
     * pack rewrite and entries dropped by one. Both zero when no pack
     * contained anything dead. */
    uint64_t pack_entries_kept;
    uint64_t pack_entries_dropped;
    /* Packs rewritten without their dead entries, and packs deleted because
     * nothing in them was alive. */
    uint32_t packs_rewritten;
    uint32_t packs_deleted;
    /* Small packs the coalescing pass merged, and how many fewer pack
     * files that left: 20 merged into 3 is 20 and 17. */
    uint32_t packs_coalesced;
    uint32_t packs_removed_by_coalesce;
    /* Staging files an interrupted run left in tmp/, removed by this run.
     * A dry run removes none. */
    uint32_t tmp_files_removed;
    uint64_t tmp_bytes_removed;
    /* Entries a rewrite or coalesce copied with damage beyond parity, and
     * the first PACK_DAMAGED_LIST_MAX of their hashes (OWN-31). */
    uint64_t pack_entries_damaged;
    uint32_t damaged_listed;
    uint8_t  damaged[PACK_DAMAGED_LIST_MAX][OBJECT_HASH_SIZE];
} gc_result_t;

/*
 * Sweep unreferenced objects and compact packs. A pack entry found damaged
 * beyond parity while being copied is copied as it is and compaction goes
 * on, then gc returns ERR_CORRUPT naming how many: the new pack's parity
 * covers the damage, and reporting OK would hide it (OWN-31).
 */
status_t gc_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                int dry_run, atomic_int *cancel, gc_result_t *out);
