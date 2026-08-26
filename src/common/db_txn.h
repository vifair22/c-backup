#pragma once

/*
 * One writer at a time, across every thread.
 *
 * A transaction is a process-wide resource here. One c-utils handle serves the
 * whole daemon and SQLite has no nested transactions, so a second BEGIN on that
 * handle fails outright while another is open. There are nine BEGIN sites
 * across backup.c, gc.c, prune.c, reindex.c, store_index.c and
 * routes/metadata.c, and until this existed only store_index.c took a lock
 * first - added when pack workers started colliding with each other. Everything
 * else raced.
 *
 * The symptom was a PUT /repos/<id>/sources during a running backup returning
 * 500 {"error":"cannot begin"}, intermittently, depending on whether a batch
 * happened to be open at that instant. Tags, notes and policy shared it.
 *
 * The lock is held from BEGIN to COMMIT or ROLLBACK, which makes writers queue
 * instead of fail. That only works while every transaction is short: the lock
 * turns a failure into a wait, and a wait behind a transaction spanning a whole
 * snapshot's node rows would be worse than the failure it replaced. Anything
 * writing per-entry has to commit in batches. See index_nodes and
 * store_content, which both do.
 *
 * These wrap the raw statements rather than sitting beside them so the lock
 * cannot drift out of step with the transaction across the many rollback
 * paths. No caller should issue BEGIN, COMMIT or ROLLBACK directly; a grep for
 * them is what proves this holds.
 *
 * Lock ordering: db_pack_commit takes the pack commit mutex and then begins a
 * transaction. Nothing acquires them the other way round - pack_gc runs after
 * gc's own COMMIT, and every store_emit_pack_removed site sits inside a pack
 * operation, which is only ever entered with no transaction open. Keep it that
 * way; taking this lock and then reaching for a pack lock would close the cycle.
 */

#include <cutils/db.h>

#include "error.h"

/*
 * Take the write lock and open a transaction. On failure the lock is released
 * before returning, so a caller that gets an error must not call commit or
 * rollback.
 */
status_t db_txn_begin(cutils_db_t *db);

/* Commit and release the lock. Releases the lock even when the commit fails:
 * there is nothing the caller can do about it and holding it would wedge every
 * other writer. */
status_t db_txn_commit(cutils_db_t *db);

/* Roll back and release the lock. Errors are logged rather than returned,
 * because every caller is already on an error path with a status worth more
 * than this one. */
void db_txn_rollback(cutils_db_t *db);
