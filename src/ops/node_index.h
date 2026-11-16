#pragma once

/*
 * The node index: entry rows keyed by tree, and each snapshot's directory
 * skeleton (docs/index-reuse.md).
 *
 * A tree's entries are written once per repository, however many snapshots
 * hold the tree, so a backup's index cost follows what changed. Each
 * snapshot adds one snap_dirs row per directory, which is what gives a
 * directory a single parent, a path and an id of its own in that snapshot.
 *
 * Every write here runs inside a transaction the caller holds (db_txn), and
 * none begins one of its own, except node_index_sweep, which batches its
 * own because it can delete a great deal.
 */

#include "error.h"
#include "manifest.h"

#include <cutils/db.h>
#include <stdint.h>

/* Ids for nodes and snap_dirs rows, from the database-wide sequence: n
 * consecutive ids starting at *first. Never handed out twice while the
 * database lives. */
status_t node_ids_reserve(cutils_db_t *db, uint64_t n, int64_t *first);

/* 1 if the tree's rows are complete and can be reused, 0 if not, -1 if the
 * database could not say (error set). */
int node_index_has_tree(cutils_db_t *db, int64_t repo_id,
                        const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Write one tree's entry rows and name rows, and the row that marks them
 * complete. Any rows already there for the hash without that mark (a run
 * that stopped half-way) are replaced. With replace set, complete rows are
 * replaced too, which is reindex rebuilding what it cannot trust.
 * *rows_out, if given, is the number of entries written.
 */
status_t node_index_put_tree(cutils_db_t *db, int64_t repo_id,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             const manifest_entry_t *entries, uint32_t count,
                             int replace, uint64_t *rows_out);

/* One directory of a snapshot's skeleton. parent_id is 0 for the root. */
status_t node_index_put_dir(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                            int64_t id, int64_t parent_id, const char *name,
                            const char *path, const uint8_t tree[OBJECT_HASH_SIZE]);

/*
 * Remove a snapshot's skeleton. With release set, every tree it reached
 * that no other snapshot's skeleton reaches loses its rows too, in the same
 * transaction; a tree another snapshot uses is never touched.
 */
status_t node_index_drop_snapshot(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                                  int release);

/*
 * Delete every tree no skeleton reaches, and rows of trees that were never
 * completed: what an interrupted backup or reindex leaves. Batched in
 * transactions of its own. Linear in the repository's distinct trees, so
 * gc and reindex run it, not a backup.
 */
status_t node_index_sweep(cutils_db_t *db, int64_t repo_id, uint64_t *trees_removed);

/* A path's place in a snapshot. */
typedef struct {
    int64_t dir_id;    /* the directory instance holding it, or itself if a directory */
    int64_t node_id;   /* a file's entry row; 0 for a directory (dir_id is its id) */
    int     is_dir;
    uint8_t tree[OBJECT_HASH_SIZE];  /* a directory's own tree */
} node_ref_t;

/*
 * Resolve an absolute path ("/a/b", "/" or "" for the root) in a snapshot,
 * one indexed lookup per component. 1 found, 0 not in the snapshot, -1 the
 * snapshot has no skeleton (not resident) or the database failed.
 */
int node_index_resolve(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                       const char *path, node_ref_t *out);

/* The snapshot's root directory instance: 1 found, 0 not resident, -1 error. */
int node_index_root(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                    int64_t *root_id, uint8_t tree[OBJECT_HASH_SIZE]);

/* Lowercase hex of a hash as SQL returns it through lower(hex(...)), back
 * to bytes. 0 on success, -1 if s is not 64 hex digits. */
int node_hex_to_hash(const char *s, uint8_t out[OBJECT_HASH_SIZE]);
