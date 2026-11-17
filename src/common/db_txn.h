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
 * writing per-entry has to commit in batches. See index_nodes, build_trees
 * and store_content, which all do. Writers are served in arrival order, so
 * one that is waiting goes before the holder's next transaction.
 *
 * A long-running holder asks db_txn_waiters() rather than committing on a
 * timer. B107 bounded the backup's batch by time and let go around every
 * large file, which kept a sources write prompt and also turned a 1,605-file
 * backup into 1,600 commits that nobody was waiting for: a WAL sync each,
 * and 60 seconds on the UAT host's storage (OWN-16). A batch now ends early
 * only when somebody is queued behind it, so the commits a backup adds are
 * bounded by the number of foreign transactions, not by its own length.
 *
 * A write made outside a transaction needs the lock just as much: on the
 * shared connection a plain statement issued while another thread holds a
 * transaction becomes part of it (B157). db_write.h has the forms every
 * write outside one of these goes through, and the guard that reports one
 * that does not.
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

/*
 * How many threads are queued for the write lock right now, not counting the
 * one holding it. One relaxed atomic load: cheap enough to ask per entry and
 * per chunk read. A holder that sees non-zero should commit at its next safe
 * point; the waiter is served first and the holder's next begin queues behind
 * it. The answer is a snapshot and may be stale by the time it is used, which
 * only moves a commit by one check.
 */
unsigned db_txn_waiters(void);

/*
 * When a holder that commits in batches should let the lock go: at the
 * first check after a writer queues, and then, while writers keep coming
 * (contended, for DB_TXN_BATCH_CONTENDED_MS after the last one seen), once
 * a batch has been open DB_TXN_BATCH_HOLD_MS. The one copy of the rule:
 * the backup's batches (backup.c) and the pack commit (store_index.c) both
 * ask it. Contended mode is what keeps two writers
 * that never go away, such as a backup's store batch and its pack workers,
 * from trading the lock, and a commit, at every row.
 *
 * Call db_txn_batch_opened right after each db_txn_begin of the batch.
 */
#define DB_TXN_BATCH_CONTENDED_MS 1000
#define DB_TXN_BATCH_HOLD_MS      200

typedef struct {
    uint64_t opened_ns;
    uint64_t contended_until_ns;
} db_txn_batch_t;

void db_txn_batch_opened(db_txn_batch_t *b);
int  db_txn_batch_should_end(db_txn_batch_t *b);
/* Whether writers have kept coming lately, so B107's rules apply: a holder
 * also lets go before long waits of its own (backup.c). */
int  db_txn_batch_contended(const db_txn_batch_t *b);

/*
 * Make every commit so far durable before returning (B142).
 *
 * state.db runs synchronous=NORMAL: a commit is in the WAL but not synced,
 * and the flusher syncs it within about a second. That is fine for edits
 * whose loss the next start repairs, and fatal for a commit that something
 * irreversible then depends on: gc committed rows moving objects into the
 * packs it wrote, unlinked the packs they came from, synced the directory
 * so the unlinks stuck - and a power cut inside that second took the rows
 * back, leaving the index naming packs that were gone. Anything that is
 * about to remove the only other copy of what a commit describes calls
 * this first.
 *
 * A full WAL checkpoint, under the write lock so no transaction is open on
 * the shared connection: SQLite syncs the WAL before copying it into the
 * database and syncs the database after. ERR_IO when the checkpoint could
 * not complete, which callers treat as "do not remove anything yet". Must
 * not be called while this thread holds the write lock.
 */
status_t db_txn_make_durable(cutils_db_t *db);

/* Completed db_txn_make_durable calls since the process started; for tests
 * proving an irreversible step came after one. */
unsigned long db_txn_durable_count(void);

/*
 * For the power-cut tests: a gate asked before every make_durable, whose
 * non-OK answer fails it with nothing checkpointed (the cut came first),
 * and a callback after each one completes, still under the write lock, so
 * the database file it sees is exactly what was made durable. NULL clears.
 */
void db_txn_set_durable_hooks(status_t (*gate)(void), void (*done)(void));

/*
 * Time spent in this module is attributed to the calling thread's phase
 * timers (phase_timer.h), when it has any: lock wait and BEGIN as txn-wait,
 * COMMIT and ROLLBACK, WAL sync included, as txn-commit. Both are recorded as
 * nested spans so the phase enclosing them can report its own time without
 * them; see phase_add_excl.
 */
