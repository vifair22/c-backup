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
 * The refcount column is used as the mark bit, which leaves it meaningful for
 * anything that wants to know how many references an object has - as of the
 * last gc (or reindex, which counts the same way). Nothing else maintains
 * it: a backup records new objects at 0 and a prune does not decrement, so
 * between runs it is stale in both directions (NIT-11, docs/api.md).
 */

#include "error.h"
#include "store.h"
#include "pack.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    uint64_t reachable;
    /* File content a tree names that the repository does not hold. Nothing
     * beneath it can be kept alive; counted so the loss is visible. */
    uint64_t content_missing;
    /* Objects the sweep removed from the index; their bytes go when
     * compaction rewrites the packs holding them. */
    uint64_t swept;
    /* Stored bytes compaction removed from packs (B197): only what it
     * finished, so a run whose compaction failed reports less than it swept.
     * A dry run reports what sweeping would make reclaimable. */
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
    /* Staging files an interrupted run left in tmp/ or packs/.staging,
     * removed by this run. A dry run removes none. */
    uint32_t tmp_files_removed;
    uint64_t tmp_bytes_removed;
    /* Entries a rewrite or coalesce copied with damage beyond parity, and
     * the first PACK_DAMAGED_LIST_MAX of their hashes (OWN-31). */
    uint64_t pack_entries_damaged;
    uint32_t damaged_listed;
    uint8_t  damaged[PACK_DAMAGED_LIST_MAX][OBJECT_HASH_SIZE];
    /* Packs compaction could not walk and left as they were, with why: a
     * failure like damage, until verify with repair restores them. */
    uint32_t packs_unwalkable;
    uint32_t unwalkable_listed;
    pack_unwalkable_t unwalkable[PACK_UNWALKABLE_LIST_MAX];
    /* Duplicate copies (OWN-22): dropped behind a confirmed indexed copy,
     * kept because the indexed copy could not be confirmed, and index rows
     * moved off such a copy onto one proven to be the object. */
    uint64_t dups_dropped;
    uint64_t dups_kept;
    uint64_t index_relocated;
} gc_result_t;

/*
 * Sweep unreferenced objects and compact packs. A pack entry found damaged
 * beyond parity while being copied is copied as it is and compaction goes
 * on, then gc returns ERR_CORRUPT naming how many: the new pack's parity
 * covers the damage, and reporting OK would hide it (OWN-31). A pack that
 * cannot be walked is left whole and fails gc the same way, naming it.
 */
status_t gc_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                int dry_run, atomic_int *cancel, gc_result_t *out);
