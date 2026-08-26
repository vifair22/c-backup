#include "db_txn.h"

#include <cutils/log.h>
#include <pthread.h>

/*
 * Static rather than per-handle because the thing being guarded is not the
 * handle, it is SQLite's one-transaction-per-connection rule, and there is one
 * connection. If the daemon ever opens a second, this becomes a field on it.
 */
static pthread_mutex_t g_write_lock = PTHREAD_MUTEX_INITIALIZER;

status_t db_txn_begin(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_begin: null handle");

    pthread_mutex_lock(&g_write_lock);
    if (db_exec_raw(db, "BEGIN") != 0) {
        pthread_mutex_unlock(&g_write_lock);
        return set_error(ERR_IO, "cannot begin");
    }
    return OK;
}

status_t db_txn_commit(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_commit: null handle");

    int rc = db_exec_raw(db, "COMMIT");
    pthread_mutex_unlock(&g_write_lock);
    if (rc != 0) return set_error(ERR_IO, "cannot commit");
    return OK;
}

void db_txn_rollback(cutils_db_t *db)
{
    if (!db) return;

    if (db_exec_raw(db, "ROLLBACK") != 0)
        log_error("db_txn: rollback failed");
    pthread_mutex_unlock(&g_write_lock);
}
