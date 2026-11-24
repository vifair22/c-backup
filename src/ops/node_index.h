#pragma once

/*
 * The node index of one snapshot (Phase 3 of a backup, after its record is
 * in the repository): a nodes row and an FTS row per scan entry, then a
 * node_hashes row per file with content, sorted by hash (migration 016).
 * Split out of backup.c, which owns the pipeline and calls this once.
 */

#include "error.h"
#include "scan.h"
#include "types.h"

#include <cutils/db.h>

#include <stdatomic.h>
#include <stdint.h>

/* What the backup learnt about each scan entry beyond the scan itself,
 * indexed like the scan's entries. */
typedef struct {
    uint64_t node_id;         /* scan-assigned, never written into a tree */
    uint64_t parent_id;
    uint32_t scan_index;
    uint8_t  tree_hash[OBJECT_HASH_SIZE];   /* directories only */
    int      hash_ready;
    /* A regular file the store could not read, or that never settled. It
     * is left out of the trees and the index entirely; see skip_file. */
    int      skipped;
    /* Stored although it never settled (OWN-45-LIVE): flagged in the tree,
     * and given no file cache row, so the next backup reads it again. */
    int      unsettled;
    /* opts->synth index + 1 for a synthetic entry, else 0. */
    size_t   synth;
} node_link_t;

/*
 * Index count entries of scan as snapshot snap_id, in batches under the
 * commit budget. The snapshots row is the caller's and comes after: rows
 * this leaves behind on failure belong to no snapshot, which a reindex
 * clears. ERR_BUSY when cancel reports the daemon stopping.
 */
status_t node_index_write(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                          const scan_result_t *scan, const node_link_t *links,
                          uint32_t count, atomic_int *cancel);
