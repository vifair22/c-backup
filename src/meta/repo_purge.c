#define _POSIX_C_SOURCE 200809L

#include "repo_purge.h"
#include "db_txn.h"

#include <cutils/log.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Rows per transaction. Short enough that a backup waiting on the
 * transaction lock does not notice; the same figure index_nodes uses. */
#define PURGE_BATCH "2000"

/*
 * Tables whose primary key leads with repo_id, so `WHERE repo_id = ?` is an
 * index range and each batch costs what it deletes. Order does not matter
 * for correctness, since nothing reads a deregistered id; children first
 * keeps a purge interrupted half way looking like a smaller repository.
 */
static const char *const keyed_tables[] = {
    "nodes",
    "pack_objects", "packs", "objects", "file_cache",
    "tags", "notes", "snapshots", "policy_values", "recovery_state",
    "journal", "repo_backup_paths", "snapshot_id_seq",
    NULL
};

/* Delete one table's rows for a repository, a batch per transaction. */
static status_t purge_table(cutils_db_t *db, const char *table, const char *repo,
                            atomic_int *stop)
{
    char sql[256];
    snprintf(sql, sizeof(sql),
             "DELETE FROM %s WHERE rowid IN"
             " (SELECT rowid FROM %s WHERE repo_id = ? LIMIT " PURGE_BATCH ")",
             table, table);
    for (;;) {
        /* stopped: not done, not failed */
        if (stop && atomic_load_explicit(stop, memory_order_relaxed)) return ERR_BUSY;
        if (db_txn_begin(db) != OK) return set_error(ERR_IO, "purge %s: begin", table);
        int n = 0;
        if (db_execute_non_query(db, sql, (const char *[]){ repo, NULL }, &n) != 0) {
            db_txn_rollback(db);
            return set_error(ERR_IO, "purge %s: delete failed", table);
        }
        if (db_txn_commit(db) != OK) return set_error(ERR_IO, "purge %s: commit", table);
        if (n == 0) return OK;
    }
}

/* One statement, one transaction: for tables keyed some other way, and the
 * filename index, which is FTS5 and cannot range-scan on repo_id at all. */
static status_t purge_stmt(cutils_db_t *db, const char *sql, const char *repo)
{
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "purge: begin");
    if (db_execute_non_query(db, sql, (const char *[]){ repo, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "purge: %s", sql);
    }
    return db_txn_commit(db) == OK ? OK : set_error(ERR_IO, "purge: commit");
}

static status_t purge_one(cutils_db_t *db, const char *repo, atomic_int *stop)
{
    /* Runs and their findings and hits: the children are indexed by run. */
    static const char *const by_run[] = {
        "DELETE FROM grep_hits WHERE run_id IN (SELECT id FROM grep_runs WHERE repo_id = ?)",
        "DELETE FROM grep_runs WHERE repo_id = ?",
        "DELETE FROM verify_findings WHERE run_id IN (SELECT id FROM verify_runs WHERE repo_id = ?)",
        "DELETE FROM verify_runs WHERE repo_id = ?",
        "DELETE FROM schedules WHERE repo_id = ?",
        "DELETE FROM jobs WHERE repo_id = ?",
        /* A full scan of the trigram index, once per removed repository. */
        "DELETE FROM node_fts WHERE repo_id = ?",
        NULL
    };
    for (int i = 0; by_run[i]; i++) {
        /* stopped: not done, not failed */
        if (stop && atomic_load_explicit(stop, memory_order_relaxed)) return ERR_BUSY;
        status_t st = purge_stmt(db, by_run[i], repo);
        if (st != OK) return st;
    }
    for (int i = 0; keyed_tables[i]; i++) {
        status_t st = purge_table(db, keyed_tables[i], repo, stop);
        if (st != OK) return st;
    }
    return purge_stmt(db, "DELETE FROM repo_purge_pending WHERE repo_id = ?", repo);
}

status_t repo_purge_run(cutils_db_t *db, atomic_int *stop)
{
    for (;;) {
        db_result_t *r = NULL;
        if (db_execute(db, "SELECT repo_id FROM repo_purge_pending ORDER BY repo_id LIMIT 1",
                       NULL, &r) != 0) {
            db_result_free(r);
            return set_error(ERR_IO, "purge: cannot read the queue");
        }
        if (!r || r->nrows == 0) { db_result_free(r); return OK; }
        char repo[32];
        snprintf(repo, sizeof(repo), "%s", r->rows[0][0]);
        db_result_free(r);

        status_t st = purge_one(db, repo, stop);
        if (st == ERR_BUSY) return st;
        if (st != OK) {
            /* Leave it queued: the next start tries again. Stop here rather
             * than spin on a repository that fails every time. */
            log_warn("purge of removed repo %s failed: %s", repo, err_msg());
            return st;
        }
        log_info("removed repo %s: index rows deleted", repo);
    }
}

static struct {
    pthread_t       thread;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    cutils_db_t    *db;
    atomic_int      stop;
    int             kicked;
    int             running;
} g_purge = { .mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER };

static void *purge_main(void *arg)
{
    (void)arg;
    for (;;) {
        (void)repo_purge_run(g_purge.db, &g_purge.stop);
        pthread_mutex_lock(&g_purge.mu);
        while (!g_purge.kicked && !g_purge.stop)
            pthread_cond_wait(&g_purge.cv, &g_purge.mu);
        g_purge.kicked = 0;
        int stop = g_purge.stop;
        pthread_mutex_unlock(&g_purge.mu);
        if (stop) return NULL;
    }
}

status_t repo_purge_start(cutils_db_t *db)
{
    g_purge.db = db;
    g_purge.stop = 0;
    g_purge.kicked = 0;
    if (pthread_create(&g_purge.thread, NULL, purge_main, NULL) != 0)
        return set_error(ERR_IO, "cannot start the repo purge thread");
    g_purge.running = 1;
    return OK;
}

void repo_purge_kick(void)
{
    pthread_mutex_lock(&g_purge.mu);
    g_purge.kicked = 1;
    pthread_cond_signal(&g_purge.cv);
    pthread_mutex_unlock(&g_purge.mu);
}

void repo_purge_stop(void)
{
    if (!g_purge.running) return;
    pthread_mutex_lock(&g_purge.mu);
    g_purge.stop = 1;
    pthread_cond_signal(&g_purge.cv);
    pthread_mutex_unlock(&g_purge.mu);
    pthread_join(g_purge.thread, NULL);
    g_purge.running = 0;
}
