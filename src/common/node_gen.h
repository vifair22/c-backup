#pragma once

/*
 * Node index generations.
 *
 * A node id is an index handle, not a name: index_nodes and reindex number a
 * snapshot's rows from MAX(id) + 1 in the repository, so rebuilding one
 * snapshot's rows (a reindex, a gap filled, a cold snapshot faulted back in)
 * gives every file a new id, and an id a client still holds can name a
 * different file of the same snapshot, silently. The API hands node ids out
 * (tree entries, search results, object references, grep hits) and takes
 * them back (tree?parent=), so a client browsing across a reindex would read
 * the wrong directory without being told.
 *
 * Each snapshot's rows therefore carry a generation, a value that changes
 * whenever they are rebuilt. Every response with node ids reports it, a
 * request may pass it back, and a stale one is refused with 409 so the
 * client re-resolves by path (docs/api.md, "Node ids and index
 * generations"). A stable node id is a 1.1 candidate; this is the 1.0 fix.
 *
 * The rules for a site that writes node rows:
 *
 *   - Draw the value with node_gen_alloc before the transaction that writes
 *     the rows begins. It commits on its own, so a rebuild that rolls back
 *     burns its value instead of returning it to the sequence: a client that
 *     saw it (the shared connection shows uncommitted rows) can never meet
 *     it again on different rows. The sequence is AUTOINCREMENT, so the
 *     reserved marks cover it across a power cut as well (id_reserve.h).
 *   - Set it with node_gen_set in the transaction that writes the rows, after
 *     any delete of the old rows and before the first new one, so no reader
 *     sees new ids under the old value or old ids under the new one.
 *   - Removing a snapshot removes its generation (node_gen_drop).
 */

#include <cutils/db.h>
#include <stdint.h>

#include "error.h"

/* The error every route answers a stale generation with, 409. */
#define NODE_GEN_STALE_MSG "index rebuilt; re-resolve by path"

/* Draw a generation never handed out before. Must be called holding no
 * transaction (db_txn_held() == 0): joining one would let its rollback take
 * the value back. */
status_t node_gen_alloc(cutils_db_t *db, int64_t *out);

/* Make gen the snapshot's current generation. Runs in the caller's
 * transaction when it holds one. 0 or -1. */
int node_gen_set(cutils_db_t *db, int64_t repo_id, int64_t snap_id, int64_t gen);

/* Forget a snapshot's generation, with the snapshot. 0 or -1. */
int node_gen_drop(cutils_db_t *db, int64_t repo_id, int64_t snap_id);

/* The snapshot's current generation; 0 when it has none (never indexed) or
 * on a read error. 0 is never drawn, so it matches no live value. */
int64_t node_gen_get(cutils_db_t *db, int64_t repo_id, int64_t snap_id);
