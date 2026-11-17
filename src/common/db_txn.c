#define _POSIX_C_SOURCE 200809L

#include "db_txn.h"
#include "db_write.h"

#include "phase_timer.h"

#include <cutils/log.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

/*
 * Static rather than per-handle because the thing being guarded is not the
 * handle, it is SQLite's one-transaction-per-connection rule, and there is one
 * connection. If the daemon ever opens a second, this becomes a field on it.
 *
 * A ticket lock rather than a bare mutex, so writers are served in the order
 * they arrived. A backup commits a batch and begins the next one a few
 * microseconds later, and a pthread mutex makes no promise that a thread
 * already waiting gets in during that gap: the running thread usually wins
 * the re-lock, so a sources or tag write could keep losing to batch after
 * batch for as long as the backup ran (B107). With tickets the backup's next
 * BEGIN queues behind the write that was already waiting.
 */
static pthread_mutex_t g_ticket_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_ticket_cv = PTHREAD_COND_INITIALIZER;
static uint64_t        g_next_ticket;
static uint64_t        g_now_serving;

/* Threads inside write_lock that have not been served yet. Written under
 * g_ticket_mu, read without it by db_txn_waiters. */
static atomic_uint     g_waiting;

/* Completed make_durable calls, for tests. */
static atomic_ulong    g_durable;

static status_t (*volatile g_durable_gate)(void);
static void     (*volatile g_durable_done)(void);

void db_txn_set_durable_hooks(status_t (*gate)(void), void (*done)(void))
{
    g_durable_gate = gate;
    g_durable_done = done;
}

/*
 * Whether this thread holds the lock. A second begin on the holding thread
 * would queue behind its own ticket and never return; with this it fails
 * instead, loudly, which is the difference between a test failure and a
 * hung daemon. The backup holds its batch across more than it used to
 * (OWN-16), so the path that must not begin a nested transaction - sealing
 * a pack from inside a batch - is one refactor away from being taken.
 */
static _Thread_local int tls_held;

static void write_lock(void)
{
    pthread_mutex_lock(&g_ticket_mu);
    uint64_t mine = g_next_ticket++;
    if (mine != g_now_serving) {
        atomic_fetch_add_explicit(&g_waiting, 1, memory_order_relaxed);
        while (mine != g_now_serving)
            pthread_cond_wait(&g_ticket_cv, &g_ticket_mu);
        atomic_fetch_sub_explicit(&g_waiting, 1, memory_order_relaxed);
    }
    pthread_mutex_unlock(&g_ticket_mu);
    tls_held = 1;
}

static void write_unlock(void)
{
    tls_held = 0;
    pthread_mutex_lock(&g_ticket_mu);
    g_now_serving++;
    pthread_cond_broadcast(&g_ticket_cv);
    pthread_mutex_unlock(&g_ticket_mu);
}

status_t db_txn_begin(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_begin: null handle");
    if (tls_held)
        return set_error(ERR_BUSY, "db_txn_begin: this thread already holds "
                                   "the write lock (nested transaction)");

    uint64_t t0 = phase_now();
    write_lock();
    int rc = db_exec_raw(db, "BEGIN");
    phase_add_nested(phase_current, PHASE_TXN_WAIT, t0);
    if (rc != 0) {
        write_unlock();
        return set_error(ERR_IO, "cannot begin");
    }
    return OK;
}

status_t db_txn_commit(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_commit: null handle");

    uint64_t t0 = phase_now();
    int rc = db_exec_raw(db, "COMMIT");
    phase_add_commit(phase_current, t0);
    write_unlock();
    if (rc != 0) return set_error(ERR_IO, "cannot commit");
    return OK;
}

void db_txn_rollback(cutils_db_t *db)
{
    if (!db) return;

    uint64_t t0 = phase_now();
    if (db_exec_raw(db, "ROLLBACK") != 0)
        log_error("db_txn: rollback failed");
    phase_add_commit(phase_current, t0);
    write_unlock();
}

unsigned db_txn_waiters(void)
{
    return atomic_load_explicit(&g_waiting, memory_order_relaxed);
}

void db_txn_batch_opened(db_txn_batch_t *b)
{
    b->opened_ns = phase_now();
}

int db_txn_batch_contended(const db_txn_batch_t *b)
{
    return b->contended_until_ns > phase_now();
}

int db_txn_batch_should_end(db_txn_batch_t *b)
{
    uint64_t now = phase_now();
    if (db_txn_waiters() > 0) {
        int was = b->contended_until_ns > now;
        b->contended_until_ns = now + (uint64_t)DB_TXN_BATCH_CONTENDED_MS * 1000000ULL;
        if (!was) return 1;
    }
    return b->contended_until_ns > now &&
           now - b->opened_ns >= (uint64_t)DB_TXN_BATCH_HOLD_MS * 1000000ULL;
}

status_t db_txn_make_durable(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_make_durable: null handle");
    if (tls_held)
        return set_error(ERR_BUSY, "db_txn_make_durable: this thread holds the write "
                                   "lock (a checkpoint cannot run inside a transaction)");
    status_t (*gate)(void) = g_durable_gate;
    if (gate && gate() != OK) return ERR_IO;
    uint64_t t0 = phase_now();
    write_lock();
    db_result_t *r = NULL;
    int rc = db_execute(db, "PRAGMA wal_checkpoint(FULL)", NULL, &r);
    /* busy, log frames, checkpointed frames: busy 0 is a complete
     * checkpoint, log -1 a database that is not in WAL mode, where every
     * commit was synced as it was made. */
    int ok = rc == 0 && r && r->nrows == 1 && r->ncols >= 1 &&
             strcmp(r->rows[0][0], "0") == 0;
    db_result_free(r);
    phase_add_commit(phase_current, t0);
    void (*done)(void) = g_durable_done;
    if (ok && done) done();
    write_unlock();
    if (!ok) return set_error(ERR_IO, "state.db: cannot make the index durable "
                                      "(checkpoint did not complete)");
    atomic_fetch_add_explicit(&g_durable, 1, memory_order_relaxed);
    return OK;
}

unsigned long db_txn_durable_count(void)
{
    return atomic_load_explicit(&g_durable, memory_order_relaxed);
}

/* ---- Writes outside a transaction (B157) ------------------------------ */

int db_txn_held(void)
{
    return tls_held;
}

int db_write_enter(cutils_db_t *db)
{
    if (tls_held) return 0;
    if (db_txn_begin(db) != OK) {
        log_error("db_write: %s", err_msg());
        err_clear();
        return -1;
    }
    return 1;
}

int db_write_leave(cutils_db_t *db, int began, int ok)
{
    if (began != 1) return ok ? 0 : -1;
    if (!ok) {
        db_txn_rollback(db);
        return -1;
    }
    if (db_txn_commit(db) != OK) {
        log_error("db_write: %s", err_msg());
        err_clear();
        return -1;
    }
    return 0;
}

int db_write(cutils_db_t *db, const char *sql, const char **params, int *affected)
{
    int began = db_write_enter(db);
    if (began < 0) return -1;
    int rc = db_execute_non_query(db, sql, params, affected);
    return db_write_leave(db, began, rc == 0);
}

int db_write_returning(cutils_db_t *db, const char *sql, const char **params,
                       db_result_t **out)
{
    *out = NULL;
    int began = db_write_enter(db);
    if (began < 0) return -1;
    int rc = db_execute(db, sql, params, out);
    if (db_write_leave(db, began, rc == 0) != 0) {
        db_result_free(*out);
        *out = NULL;
        return -1;
    }
    return 0;
}

int db_write_batch_open(db_write_batch_t *b, cutils_db_t *db)
{
    b->db = db;
    b->since = 0;
    b->began = db_write_enter(db);
    return b->began < 0 ? -1 : 0;
}

int db_write_batch_next(db_write_batch_t *b)
{
    if (b->began != 1) return b->began < 0 ? -1 : 0;
    if (++b->since < DB_WRITE_BATCH_ROWS && db_txn_waiters() == 0) return 0;
    b->began = 0;
    if (db_write_leave(b->db, 1, 1) != 0) { b->began = -1; return -1; }
    return db_write_batch_open(b, b->db);
}

int db_write_batch_close(db_write_batch_t *b, int ok)
{
    if (b->began < 0) return -1;
    int rc = db_write_leave(b->db, b->began, ok);
    b->began = -1;
    return rc;
}

/*
 * The update hook fires on the thread running the statement, once per row
 * changed in a rowid table, which every table in the schema is. It does not
 * fire for SQLite's own tables (sqlite_sequence) or for the rows an FTS5
 * table writes on behalf of a statement that was itself checked, so it
 * reports exactly the statements this module is about. tls_held is the
 * calling thread's, so the test is one load.
 */
static atomic_int   g_guard_armed;
static atomic_ulong g_guard_violations;
static int          g_guard_abort = -1;   /* -1: environment not read yet */

#define GUARD_LOG_MAX 64

static void guard_hook(void *arg, int op, const char *dbname, const char *table,
                       sqlite3_int64 rowid)
{
    (void)arg;
    (void)rowid;
    if (!atomic_load_explicit(&g_guard_armed, memory_order_relaxed) || tls_held) return;
    if (!dbname || strcmp(dbname, "main") != 0) return;
    /* c-utils' own tables, written by c-utils' own threads on this same
     * connection: the log writer inserts every log line into logs, the push
     * worker queues and retires alerts in push, and config is seeded at
     * open. None of them can go through db_write from here, and all three
     * share the hazard; it is c-utils' to fix (B157 report), and reporting
     * every log line would bury the violations this guard is for. */
    if (table && (strcmp(table, "logs") == 0 || strcmp(table, "push") == 0 ||
                  strcmp(table, "config") == 0))
        return;
    unsigned long n = atomic_fetch_add_explicit(&g_guard_violations, 1,
                                                memory_order_relaxed) + 1;
    if (n <= GUARD_LOG_MAX)
        log_error("db_txn guard: %s on %s outside a write transaction (B157); "
                  "it would commit or roll back with another thread's work",
                  op == SQLITE_INSERT ? "INSERT" : op == SQLITE_DELETE ? "DELETE" : "UPDATE",
                  table ? table : "?");
    if (g_guard_abort == 1) abort();
}

static int guard_ext(sqlite3 *conn, char **err, const sqlite3_api_routines *api)
{
    (void)err;
    (void)api;
    sqlite3_update_hook(conn, guard_hook, NULL);
    return SQLITE_OK;
}

void db_txn_guard_install(void)
{
    if (g_guard_abort < 0) {
        const char *e = getenv("C_BACKUP_TXN_GUARD");
        g_guard_abort = e && strcmp(e, "abort") == 0;
    }
    /* Registering the same entry point twice is a no-op in SQLite. */
    if (sqlite3_auto_extension((void (*)(void))guard_ext) != SQLITE_OK)
        log_warn("db_txn guard: cannot register; writes outside a transaction "
                 "will not be reported");
}

void db_txn_guard_arm(int on)
{
    atomic_store_explicit(&g_guard_armed, on ? 1 : 0, memory_order_relaxed);
}

unsigned long db_txn_guard_violations(void)
{
    return atomic_load_explicit(&g_guard_violations, memory_order_relaxed);
}
