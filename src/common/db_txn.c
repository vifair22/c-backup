#define _POSIX_C_SOURCE 200809L

#include "db_txn.h"

#include "phase_timer.h"
#include "wal_sync.h"

#include <cutils/log.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>

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

int db_txn_wal_path(cutils_db_t *db, char *out, size_t outsz)
{
    if (!db || !out || outsz == 0) return -1;
    out[0] = '\0';
    db_result_t *r = NULL;
    int wal = db_execute(db, "PRAGMA journal_mode", NULL, &r) == 0 && r &&
              r->nrows == 1 && r->ncols >= 1 && strcasecmp(r->rows[0][0], "wal") == 0;
    db_result_free(r);
    if (!wal) return 0;
    r = NULL;
    /* seq, name, file: the main database's file, "" for an in-memory or
     * temporary one, which has no WAL on disk to sync. */
    if (db_execute(db, "PRAGMA database_list", NULL, &r) != 0 || !r) {
        db_result_free(r);
        return -1;
    }
    int rc = 0;
    for (int i = 0; i < r->nrows; i++) {
        if (r->ncols < 3 || strcmp(r->rows[i][1], "main") != 0) continue;
        if (r->rows[i][2][0] &&
            snprintf(out, outsz, "%s-wal", r->rows[i][2]) >= (int)outsz) {
            out[0] = '\0';
            rc = -1;
        }
        break;
    }
    db_result_free(r);
    return rc;
}

status_t db_txn_make_durable(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_make_durable: null handle");
    if (tls_held)
        return set_error(ERR_BUSY, "db_txn_make_durable: this thread holds the write "
                                   "lock (its own transaction is not committed yet)");
    status_t (*gate)(void) = g_durable_gate;
    if (gate && gate() != OK) return ERR_IO;
    uint64_t t0 = phase_now();

    /*
     * A barrier, not a hold: wait for whatever transaction is open to
     * commit, then let go before syncing. A write issued outside db_txn -
     * notes, policy, jobs, sessions all do - runs on the one shared
     * connection and so lands inside any transaction another thread has
     * open; it is committed only when that transaction is. Taking the lock
     * means that transaction is over. A batch holder sees the waiter and
     * commits at its next safe point (db_txn_batch_should_end), so this
     * waits for a commit, not for a backup.
     */
    write_lock();
    write_unlock();

    char wal[PATH_MAX];
    if (db_txn_wal_path(db, wal, sizeof(wal)) != 0)
        return set_error(ERR_IO, "state.db: cannot find its WAL to make the index durable");

    /* No lock held, so no writer waits for the sync. wal_sync also syncs
     * the directory the first time a given -wal is made durable, and checks
     * it syncs the live file (wal_sync.h). */
    if (wal[0]) {
        status_t st = wal_sync(wal);
        if (st != OK) return st;
    }
    phase_add_commit(phase_current, t0);
    void (*done)(void) = g_durable_done;
    if (done) done();
    atomic_fetch_add_explicit(&g_durable, 1, memory_order_relaxed);
    return OK;
}

unsigned long db_txn_durable_count(void)
{
    return atomic_load_explicit(&g_durable, memory_order_relaxed);
}
