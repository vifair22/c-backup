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
    uint64_t trees_read;
} reindex_result_t;

/* Rebuild every snapshot found under the repo's snapshots/ directory. */
status_t reindex_repo(store_t *store, cutils_db_t *db, int64_t repo_id,
                      volatile int *cancel, reindex_result_t *out);

/* Rebuild one snapshot, for faulting a cold snapshot back in on demand. */
status_t reindex_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                          int64_t snap_id, reindex_result_t *out);
