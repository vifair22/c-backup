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
#include <stdatomic.h>
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
    /* The snapshot-id high-water mark the index now holds: the largest of
     * the repository's snapshot-ids file, its highest record, and what the
     * index held before. The next backup numbers above it. ids_repaired is
     * set when the file was missing, damaged or behind and was rewritten. */
    int64_t  snapshot_mark;
    int      snapshot_ids_repaired;
    /* The pack-number mark afterwards (<repo>/pack-ids), and whether the
     * file was missing, damaged or behind and rewritten (B156). */
    int64_t  pack_mark;
    int      pack_ids_repaired;
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

/* Rebuild one snapshot, for faulting a cold snapshot back in on demand. A
 * snapshot with no index row is walked in batches (B200), its row written
 * last; one that has a row is replaced in one transaction, so a walk that
 * fails keeps the rows it had. */
status_t reindex_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                          int64_t snap_id, reindex_result_t *out);

/*
 * Index every snapshot record the repository holds and the index lacks,
 * wherever its id falls (B200), before something that decides "unreferenced"
 * from the index deletes. gc marks from the snapshots table: a record with
 * no row there - a backup whose index did not commit after its record was
 * written (B195) - has its trees unmarked, and what only it holds would be
 * swept and compacted away. Interrupted prunes are finished first, so a
 * record on its way out is not brought back.
 *
 * Call under the repository's write lock, right before deciding what is
 * unreferenced, so no record can appear in between. what names the caller
 * for the error ("gc"). OK with *out_indexed records indexed (0 when there
 * were none); an error, naming the record, when one cannot be listed, read
 * or indexed, or the key is not loaded: the caller must refuse.
 */
status_t reindex_gaps(store_t *store, cutils_db_t *db, int64_t repo_id,
                      const char *what, uint64_t *out_indexed);

/*
 * reindex_gaps with a cancel checked between snapshots and progress
 * reported per snapshot ("index snapshot <id>", i of n). The start-up
 * reconcile's job (B264): a record the index lost is indexed on its own,
 * never by rebuilding the whole repository. A cancel returns ERR_BUSY with
 * *out_indexed those finished; the rest are found at the next start.
 */
status_t reindex_gaps_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                          const char *what, atomic_int *cancel,
                          void (*progress)(const char *, uint64_t, uint64_t, void *),
                          void *progress_ctx, uint64_t *out_indexed);

/* The statement a gap's chunk-list pass runs, for tests to read its plan. */
extern const char *const reindex_snap_lists_sql;
