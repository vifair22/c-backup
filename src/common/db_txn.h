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
 * and store_content, which all do. Writers are served in arrival order
 * within their class, so one that is waiting goes before the holder's next
 * transaction, and a non-bulk writer goes before every waiting bulk one
 * (db_txn_begin_bulk, B193).
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
#include <stddef.h>

#include "error.h"

/*
 * Take the write lock and open a transaction. On failure the lock is released
 * before returning, so a caller that gets an error must not call commit or
 * rollback.
 */
status_t db_txn_begin(cutils_db_t *db);

/*
 * db_txn_begin for a bulk writer: a backup's batches, index_nodes, a pack
 * commit's batches, reindex, gc, prune, purge, the pack presence sweep, and
 * every db_write_batch loop. Past the WAL's hard cap it first waits, holding
 * no lock, for the checkpointer to complete a copy (checkpointer_throttle,
 * B181), so a writer that never syncs cannot outrun the checkpoint on slow
 * storage. Everything else - API requests, job records, schedules, logs -
 * uses db_txn_begin and never waits for a checkpoint: a tag or a note that
 * did would bring back the waits B144 removed.
 */
status_t db_txn_begin_bulk(cutils_db_t *db);

/* Bulk transactions begun since the process started; for tests that count
 * how many a priority writer waited behind. A waiting priority writer (any
 * db_txn_begin, db_write, the checkpointer) is served before the next bulk
 * one (B193). */
unsigned long db_txn_bulk_count(void);

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
/* db_txn_batch_should_end also ends a batch that has reached its commit
 * budget (B144-REST, db_txn.c): about DB_TXN_COMMIT_BUDGET_MS of WAL writes
 * on this device, learnt from earlier commits, and never more than
 * DB_TXN_COMMIT_PAGES_MAX pages. */
int  db_txn_batch_should_end(db_txn_batch_t *b);
/* Whether writers have kept coming lately, so B107's rules apply: a holder
 * also lets go before long waits of its own (backup.c). */
int  db_txn_batch_contended(const db_txn_batch_t *b);

/*
 * Give c-utils this lock (c-utils 1.8.0, db_set_write_lock), so its log
 * writer, push queue and transaction scopes queue for it like every other
 * writer instead of landing inside another thread's transaction. Call once,
 * right after the handle is opened and before anything begins a
 * transaction.
 */
status_t db_txn_install_cutils_lock(cutils_db_t *db);

/*
 * The commit budget (B144-REST; the design is in db_txn.c). A batch that
 * asks db_txn_batch_should_end at each row ends once it has changed enough
 * rows to fill the budget. Active only once db_txn_track_wal has been given
 * the database (state_db_tune does) and the update hook is installed
 * (db_txn_guard_install, before the database is opened).
 */
#define DB_TXN_COMMIT_BUDGET_MS   150
#define DB_TXN_COMMIT_PAGES_MIN   16
#define DB_TXN_COMMIT_PAGES_MAX   256
#define DB_TXN_COMMIT_ROWS_MIN    4
/* Frames per row assumed at a call site's first transaction, before it has
 * learnt its own. Two, not one: a pack commit's rows land at random places
 * in the hash indexes, about 1.2 frames each as measured, and a first
 * commit that overshot its budget by that much was the kind FS2 saw run to
 * 438 pages (B193). */
#define DB_TXN_FRAMES_PER_ROW_FIRST 2.0

/* NULL stops tracking (tests that open another database). */
void db_txn_track_wal(cutils_db_t *db);

typedef struct {
    unsigned long commits;       /* measured commits */
    unsigned long capped;        /* batches ended by the budget */
    uint32_t      max_frames;    /* most WAL frames one COMMIT wrote */
    uint64_t      max_commit_ns; /* longest COMMIT */
    double        ns_per_frame;  /* the device estimate */
} db_txn_commit_stats_t;

void db_txn_commit_stats(db_txn_commit_stats_t *out);
void db_txn_commit_stats_reset(void);
/* Tests: pretend each WAL frame costs ns to write (0: measure). */
void db_txn_set_frame_cost_ns(double ns);
/* The calling thread's current row cap, UINT32_MAX when none applies. */
uint32_t db_txn_row_cap(void);

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
 * An fdatasync of the WAL, with no lock held during it (B144). It first
 * takes the write lock and lets it straight go, so a transaction open on
 * the shared connection has committed: a write made outside db_txn lands
 * in whatever transaction another thread has open, and is only committed
 * with it. In WAL mode a commit
 * is durable once its frames are: recovery replays every WAL frame up to
 * the last commit whose checksums hold, and the copy into the database
 * file is a cache, not part of durability. Every commit that returned
 * before this was called has written its frames into the WAL file (NORMAL
 * leaves out only the sync), so one sync of that file, from any
 * descriptor, covers them all, whatever other threads commit meanwhile.
 * The frames a later WAL restart overwrites were checkpointed first, and a
 * complete checkpoint syncs the database before the WAL is reused.
 *
 * It used to be a FULL checkpoint taken under the write lock: up to the
 * whole WAL (64 MiB by default) copied into the database, with two syncs,
 * while every writer, a running backup's batches and each API edit
 * included, queued behind it. Since every API write called this before
 * answering (api_server_set_after_write), that put a checkpoint on the
 * request path of every tag, note and sources edit, and a seconds-long
 * wait on every other writer during a backup on slow storage. Only the
 * durable class of API writes calls it now (api_write_class_t).
 *
 * ERR_IO when the WAL could not be synced, which callers treat as "do not
 * remove anything yet". Must not be called while this thread holds the
 * write lock: its own transaction is not committed, so there is nothing
 * of it to make durable.
 */
status_t db_txn_make_durable(cutils_db_t *db);

/*
 * Run fn holding the write lock, with no transaction open: no db_txn
 * writer can commit until it returns. For the checkpointer's escalation
 * (checkpointer.h), which needs a moment when nothing is appended to the
 * WAL. fn must be short and must not begin a transaction. ERR_BUSY when
 * this thread already holds the lock; otherwise fn's status.
 */
status_t db_txn_exclusive(status_t (*fn)(void *), void *arg);

/* The WAL file db_txn_make_durable syncs: "<main database file>-wal" when
 * the database is in WAL mode and on disk, "" when there is none (rollback
 * journal, in memory). 0, or -1 when it cannot be determined. */
int db_txn_wal_path(cutils_db_t *db, char *out, size_t outsz);

/* Completed db_txn_make_durable calls since the process started; for tests
 * proving an irreversible step came after one. */
unsigned long db_txn_durable_count(void);

/*
 * For the power-cut tests: a gate asked before every make_durable, whose
 * non-OK answer fails it with nothing synced (the cut came first), and a
 * callback after each one completes. Every commit that returned before the
 * gate was asked is durable once the callback runs; a commit made between
 * the two may or may not be, so a model has to take the WAL as of the gate.
 * NULL clears.
 */
void db_txn_set_durable_hooks(status_t (*gate)(void), void (*done)(void));

/*
 * Time spent in this module is attributed to the calling thread's phase
 * timers (phase_timer.h), when it has any: lock wait and BEGIN as txn-wait,
 * COMMIT and ROLLBACK, WAL sync included, as txn-commit. Both are recorded as
 * nested spans so the phase enclosing them can report its own time without
 * them; see phase_add_excl.
 */
