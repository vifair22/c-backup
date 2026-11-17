#pragma once

/*
 * Bridge between the storage engine and the daemon database.
 *
 * The engine deliberately knows nothing about SQL: it asks where a hash lives
 * and announces what went into a pack, both through callbacks. This is the
 * only place those callbacks are backed by real tables.
 *
 * See daemonization-spec.md section 9.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct store_index store_index_t;

/* Register the locate and pack-commit callbacks on a store. The returned
 * handle owns the binding and must outlive the store. */
status_t store_index_bind(store_t *store, cutils_db_t *db, int64_t repo_id,
                          store_index_t **out);
void     store_index_free(store_index_t *idx);

/*
 * An object staged in a pack that is still open is recorded as 'staged'. Its
 * bytes are in a temporary file that only becomes the pack when it is
 * sealed, and the seal's commit callback rewrites the location to the pack.
 * Until then the row is a promise, not a fact.
 *
 * Recording it as present made a crash mid-backup unsafe in a way nothing
 * reported. The rows committed in batches while the pack was open; the pack
 * died with the process; and the next backup's file cache found the paths
 * unchanged, found their objects "present", and wrote a snapshot referencing
 * bytes that were never on disk - 23,674 files in one measured run, with the
 * backup reporting success.
 *
 * A staged row can outlive its pack only through a crash or an aborted
 * backup, so they are dropped at daemon start and at the start of every
 * backup, which holds the repository's write lock.
 */
status_t store_index_put_staged(cutils_db_t *db, int64_t repo_id,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                uint8_t type, uint64_t phys_size, uint64_t orig_size);

/* Drop staged rows left by an interrupted backup: one repository, or every
 * repository when repo_id is 0. Only safe when no backup is running on it. */
status_t store_index_drop_staged(cutils_db_t *db, int64_t repo_id);

/*
 * Drop the rows of pack commits that never reached their packs row (a crash
 * between db_pack_commit's batches), for one repository or, with 0, all.
 * Call before store_index_drop_staged; see store_index.c.
 */
status_t store_index_drop_unannounced(cutils_db_t *db, int64_t repo_id);

/*
 * Drop packs rows whose file is gone and through which nothing is located:
 * the row of a removed pack whose removal commit a power cut lost (B147).
 * One repository, or every one with 0. Called at start and by gc.
 */
status_t store_index_drop_vanished_packs(cutils_db_t *db, int64_t repo_id);

/*
 * Give objects rows marked 'absent' that have a pack_objects row the pack's
 * location and sizes: what an interrupted reindex leaves between walking
 * the trees and sizing the objects (B149). One repository, or every one
 * with 0. Called at start.
 */
status_t store_index_fix_absent(cutils_db_t *db, int64_t repo_id);

/* For tests: rows per batch of a pack commit's new-row phase (0 restores the
 * default), and a gate asked before each of its commits; a non-OK answer is
 * a power cut there, with no further commits and no cleanup. */
void store_index_set_pack_batch_rows(uint32_t rows);
void store_index_set_commit_gate(status_t (*gate)(void));
/* For tests: asked before a pack commit's final COMMIT; non-zero makes it a
 * commit that failed (not a power cut): rolled back, cleaned up, and
 * reported to the seal through store_pack_commit_failed. */
void store_index_set_commit_fault(int (*fault)(void));

/* Record an object the repository already holds in an installed pack, as a
 * backup meets one it deduplicated against. A row already there is left as
 * it is; the pack commit is what set its size. */
status_t store_index_put_packed(cutils_db_t *db, int64_t repo_id,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint8_t type, uint64_t phys_size, uint64_t orig_size);

/* Bump or drop an object's refcount. GC collects rows that reach zero. */
status_t store_index_addref(cutils_db_t *db, int64_t repo_id,
                            const uint8_t hash[OBJECT_HASH_SIZE]);
