#pragma once

/*
 * Rebuild a repository's index from the manifest trees in the repository
 * itself.
 *
 * This is the disaster-recovery path, and it is the reason the daemon
 * database can be described as derived rather than authoritative: lose it and
 * you lose query speed and job history, not backups. It is also the most
 * direct proof that tree serialisation round-trips, since a reindexed
 * snapshot must produce the same node set as the backup that wrote it.
 *
 * See daemonization-spec.md sections 5.4 and 11.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    uint64_t snapshots;
    uint64_t nodes;
    /* Regular files only. snapshots.file_count means the same thing whether a
     * snapshot was written by a backup or rebuilt here, so counting nodes
     * would make a rebuilt snapshot claim more files than it has. */
    uint64_t files;
    /* Logical bytes in those files, which is snapshots.total_size. */
    uint64_t bytes;
    uint64_t trees_read;
    /* Registry rebuilt alongside the nodes. Without these the recovered
     * database can browse and restore but cannot account for its own disk
     * usage, and GC has nothing to sweep. */
    uint64_t objects;
    uint64_t packs;
    uint64_t pack_entries;
    /* What could not be read. Either being non-zero fails the run, and what
     * could not be read keeps whatever index rows it had before. */
    uint64_t snapshots_failed;
    uint64_t packs_unreadable;
    /* Tags brought back from the repository's own tag list, and those whose
     * snapshot no longer exists there. */
    uint64_t tags_restored;
    uint64_t tags_skipped;
} reindex_result_t;

/*
 * Rebuild every snapshot found under the repo's snapshots/ directory.
 *
 * Refuses outright, changing nothing, when the repository is encrypted and
 * no key is loaded. Otherwise indexes everything it can read and returns
 * ERR_CORRUPT if any snapshot or pack could not be: the readable ones are
 * rebuilt, and nothing that failed loses the index rows it had.
 */
status_t reindex_repo(store_t *store, cutils_db_t *db, int64_t repo_id,
                      atomic_int *cancel, reindex_result_t *out);

/* Rebuild one snapshot, for faulting a cold snapshot back in on demand. */
status_t reindex_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                          int64_t snap_id, reindex_result_t *out);
