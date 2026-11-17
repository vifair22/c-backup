#pragma once

/*
 * Every write to the shared connection goes through the write lock (B157).
 *
 * db_txn.h holds the lock from BEGIN to COMMIT, which serialises writers that
 * open a transaction. A plain statement issued without one did not take the
 * lock at all, and on the one connection every thread shares that is worse
 * than unserialised: SQLite has one transaction per connection, so a plain
 * write made while another thread held a db_txn transaction silently became
 * part of it. It committed with that thread's work, or was rolled back with
 * it, and the caller had already been told it succeeded. store_index_drop_staged
 * did exactly this from inside a running backup.
 *
 * These are the drop-in forms of db_execute_non_query and db_execute for a
 * statement that writes. A thread that already holds the lock runs the
 * statement inside its own transaction, which is what it asked for; any
 * other thread gets a transaction of its own around just this statement,
 * queued behind whoever holds the lock. They return 0 or -1, like the c-utils
 * calls they replace, and declare nothing from error.h, so a file that uses
 * the c-utils set_error (auth.c) can include this.
 *
 * Several statements that must land together use db_write_enter and
 * db_write_leave around them instead, which join or open a transaction the
 * same way. Anything that writes many rows still commits in batches, as
 * db_txn.h asks.
 */

#include <cutils/db.h>

/* Whether the calling thread holds the write lock (is inside a db_txn
 * transaction). */
int db_txn_held(void);

/* db_execute_non_query under the write lock. */
int db_write(cutils_db_t *db, const char *sql, const char **params, int *affected);

/* db_execute under the write lock, for INSERT ... RETURNING. On failure,
 * including a commit that failed after the statement ran, *out is NULL. */
int db_write_returning(cutils_db_t *db, const char *sql, const char **params,
                       db_result_t **out);

/*
 * Open a write scope: 1 when this call began a transaction (and the matching
 * leave must end it), 0 when the thread already held one and joined it, -1
 * when a transaction could not be begun (nothing to leave). Must not be
 * called holding a lock that a transaction holder may wait for; see the lock
 * ordering note in db_txn.h.
 */
int db_write_enter(cutils_db_t *db);

/* Close a write scope opened by db_write_enter: commit when it began one and
 * ok is non-zero, roll back when it began one and ok is zero. A joined scope
 * is left to its owner. Returns 0 when ok and the commit (if any) succeeded,
 * -1 otherwise. */
int db_write_leave(cutils_db_t *db, int began, int ok);

/*
 * A write scope for a loop that writes many rows: committed every
 * DB_WRITE_BATCH_ROWS statements, and sooner when another writer is queued
 * for the lock, so the loop never holds it for long (db_txn.h). A thread that
 * already held the lock joins its transaction and the owner decides when to
 * commit. open and next return 0, or -1 when a transaction could not be
 * begun or committed, after which the batch is closed; close returns as
 * db_write_leave does.
 */
#define DB_WRITE_BATCH_ROWS 2000

typedef struct {
    cutils_db_t *db;
    int          began;
    unsigned     since;
} db_write_batch_t;

int db_write_batch_open(db_write_batch_t *b, cutils_db_t *db);
int db_write_batch_next(db_write_batch_t *b);
int db_write_batch_close(db_write_batch_t *b, int ok);

/*
 * The guard that proves the rule holds.
 *
 * db_txn_guard_install registers an SQLite auto-extension, so every
 * connection opened after it carries an update hook; it must run before the
 * database is opened. Once db_txn_guard_arm(1) has been called, every row a
 * statement inserts, updates or deletes in the main database must come from
 * a thread holding the write lock, and one that does not is a violation: an
 * error is logged naming the table, the count goes up, and when
 * C_BACKUP_TXN_GUARD=abort is set in the environment the process aborts, so
 * the end-to-end suites fail at the statement rather than at a symptom.
 *
 * c-utils' own writers are covered too: with db_txn_install_cutils_lock
 * they take this lock before writing, so a log line or a queued alert
 * that did not would be reported like any other write.
 *
 * The daemon arms it as the first background thread starts. Before that
 * nothing else can be inside a transaction, so start-up's plain writes are
 * correct and are not reported.
 */
void          db_txn_guard_install(void);
void          db_txn_guard_arm(int on);
unsigned long db_txn_guard_violations(void);
