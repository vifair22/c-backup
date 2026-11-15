#include "db_txn.h"

#include <cutils/log.h>
#include <pthread.h>
#include <stdint.h>

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

static void write_lock(void)
{
    pthread_mutex_lock(&g_ticket_mu);
    uint64_t mine = g_next_ticket++;
    while (mine != g_now_serving)
        pthread_cond_wait(&g_ticket_cv, &g_ticket_mu);
    pthread_mutex_unlock(&g_ticket_mu);
}

static void write_unlock(void)
{
    pthread_mutex_lock(&g_ticket_mu);
    g_now_serving++;
    pthread_cond_broadcast(&g_ticket_cv);
    pthread_mutex_unlock(&g_ticket_mu);
}

status_t db_txn_begin(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_begin: null handle");

    write_lock();
    if (db_exec_raw(db, "BEGIN") != 0) {
        write_unlock();
        return set_error(ERR_IO, "cannot begin");
    }
    return OK;
}

status_t db_txn_commit(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_commit: null handle");

    int rc = db_exec_raw(db, "COMMIT");
    write_unlock();
    if (rc != 0) return set_error(ERR_IO, "cannot commit");
    return OK;
}

void db_txn_rollback(cutils_db_t *db)
{
    if (!db) return;

    if (db_exec_raw(db, "ROLLBACK") != 0)
        log_error("db_txn: rollback failed");
    write_unlock();
}
