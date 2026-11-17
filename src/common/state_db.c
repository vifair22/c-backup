#define _POSIX_C_SOURCE 200809L

#include "state_db.h"
#include "wal_sync.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* One PRAGMA's value as SQLite reports it, or -1. */
static long long pragma_value(cutils_db_t *db, const char *name)
{
    char sql[64];
    snprintf(sql, sizeof(sql), "PRAGMA %s", name);
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, NULL, &r) == 0 && r && r->nrows == 1 && r->ncols >= 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

status_t state_db_tune(cutils_db_t *db, const state_db_tuning_t *t)
{
    if (!db || !t || !t->synchronous)
        return set_error(ERR_INVALID, "state_db_tune: null argument");

    long long want_sync;
    if (strcasecmp(t->synchronous, "normal") == 0)    want_sync = 1;
    else if (strcasecmp(t->synchronous, "full") == 0) want_sync = 2;
    else
        return set_error(ERR_INVALID, "db.synchronous '%s' is not normal or full",
                         t->synchronous);
    if (t->cache_mb < STATE_DB_CACHE_MB_MIN || t->cache_mb > STATE_DB_CACHE_MB_MAX)
        return set_error(ERR_INVALID, "db.cache_mb %d is outside %d to %d",
                         t->cache_mb, STATE_DB_CACHE_MB_MIN, STATE_DB_CACHE_MB_MAX);
    if (t->wal_autocheckpoint < STATE_DB_AUTOCKPT_MIN ||
        t->wal_autocheckpoint > STATE_DB_AUTOCKPT_MAX)
        return set_error(ERR_INVALID, "db.wal_autocheckpoint %d is outside %d to %d",
                         t->wal_autocheckpoint, STATE_DB_AUTOCKPT_MIN,
                         STATE_DB_AUTOCKPT_MAX);

    /* NORMAL is only safe in WAL mode: in rollback-journal mode it can
     * corrupt the database on a power cut. c-utils sets WAL at open, but a
     * database on a filesystem that cannot hold the shared-memory index
     * stays in its old mode, and that has to stop the daemon, not be
     * discovered after an outage. */
    db_result_t *jm = NULL;
    int wal = db_execute(db, "PRAGMA journal_mode", NULL, &jm) == 0 && jm &&
              jm->nrows == 1 && strcasecmp(jm->rows[0][0], "wal") == 0;
    db_result_free(jm);
    if (!wal)
        return set_error(ERR_INVALID, "state.db is not in WAL mode; refusing to "
                         "relax its sync settings");

    /* Negative cache_size is KiB, independent of the page size. */
    long long want_cache = -(long long)t->cache_mb * 1024;
    char sql[160];
    snprintf(sql, sizeof(sql),
             "PRAGMA synchronous=%lld; PRAGMA cache_size=%lld; "
             "PRAGMA wal_autocheckpoint=%d;",
             want_sync, want_cache, t->wal_autocheckpoint);
    if (db_exec_raw(db, sql) != 0)
        return set_error(ERR_IO, "cannot apply state.db settings");

    long long got_sync  = pragma_value(db, "synchronous");
    long long got_cache = pragma_value(db, "cache_size");
    long long got_ckpt  = pragma_value(db, "wal_autocheckpoint");
    if (got_sync != want_sync)
        return set_error(ERR_IO, "state.db synchronous is %lld after setting %lld",
                         got_sync, want_sync);
    if (got_cache != want_cache)
        return set_error(ERR_IO, "state.db cache_size is %lld after setting %lld",
                         got_cache, want_cache);
    if (got_ckpt != t->wal_autocheckpoint)
        return set_error(ERR_IO, "state.db wal_autocheckpoint is %lld after setting %d",
                         got_ckpt, t->wal_autocheckpoint);
    return OK;
}

/* ------------------------------------------------------------------ */
/* The WAL flusher                                                     */
/* ------------------------------------------------------------------ */

static pthread_mutex_t g_fl_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_fl_cv;
static pthread_t       g_fl_th;
static int             g_fl_running;
static int             g_fl_stop;
static unsigned        g_fl_interval_ms;
static char            g_fl_wal[PATH_MAX];
static atomic_ulong    g_fl_syncs;

/* The WAL as last synced: a change in either means there is something new
 * to make durable. Written by whichever thread syncs (the flusher, or stop
 * after joining it) and read by state_db_flusher_current, hence the lock. */
static pthread_mutex_t g_fl_seen_mu = PTHREAD_MUTEX_INITIALIZER;
static struct timespec g_fl_mtime;
static off_t           g_fl_size = -1;

static int seen_matches(const struct stat *st)
{
    pthread_mutex_lock(&g_fl_seen_mu);
    int same = st->st_size == g_fl_size &&
               st->st_mtim.tv_sec == g_fl_mtime.tv_sec &&
               st->st_mtim.tv_nsec == g_fl_mtime.tv_nsec;
    pthread_mutex_unlock(&g_fl_seen_mu);
    return same;
}

/*
 * Sync the WAL if it changed since the last sync. Through a descriptor of
 * its own, outside the database handle's mutex, so a backup's statements
 * never wait for it: fdatasync on any descriptor flushes the file's data.
 * SQLite resets the WAL in place rather than replacing it, but a missing
 * file (before the first write, or after a clean close) is normal.
 */
static void flush_once(void)
{
    struct stat st;
    if (stat(g_fl_wal, &st) != 0) return;
    if (seen_matches(&st)) return;
    /* wal_sync: the file's data, its directory entry the first time this
     * -wal is synced, and only ever the live file (wal_sync.h). */
    if (wal_sync(g_fl_wal) == OK) {
        pthread_mutex_lock(&g_fl_seen_mu);
        g_fl_size  = st.st_size;
        g_fl_mtime = st.st_mtim;
        pthread_mutex_unlock(&g_fl_seen_mu);
        atomic_fetch_add(&g_fl_syncs, 1);
    } else {
        log_warn("state.db: %s", err_msg());
        err_clear();
    }
}

static void *flusher_main(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&g_fl_mu);
    while (!g_fl_stop) {
        struct timespec until;
        clock_gettime(CLOCK_MONOTONIC, &until);
        until.tv_sec  += (time_t)(g_fl_interval_ms / 1000);
        until.tv_nsec += (long)(g_fl_interval_ms % 1000) * 1000000L;
        if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
        while (!g_fl_stop &&
               pthread_cond_timedwait(&g_fl_cv, &g_fl_mu, &until) != ETIMEDOUT) {}
        if (g_fl_stop) break;
        pthread_mutex_unlock(&g_fl_mu);
        flush_once();
        pthread_mutex_lock(&g_fl_mu);
    }
    pthread_mutex_unlock(&g_fl_mu);
    return NULL;
}

status_t state_db_flusher_start(const char *db_path, unsigned interval_ms)
{
    if (!db_path || !*db_path || interval_ms == 0)
        return set_error(ERR_INVALID, "state_db_flusher_start: bad argument");
    pthread_mutex_lock(&g_fl_mu);
    if (g_fl_running) {
        pthread_mutex_unlock(&g_fl_mu);
        return set_error(ERR_BUSY, "state.db flusher already running");
    }
    if (snprintf(g_fl_wal, sizeof(g_fl_wal), "%s-wal", db_path) >= (int)sizeof(g_fl_wal)) {
        pthread_mutex_unlock(&g_fl_mu);
        return set_error(ERR_INVALID, "state.db path too long");
    }
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&g_fl_cv, &ca);
    pthread_condattr_destroy(&ca);
    g_fl_interval_ms = interval_ms;
    g_fl_stop = 0;
    pthread_mutex_lock(&g_fl_seen_mu);
    g_fl_size = -1;
    memset(&g_fl_mtime, 0, sizeof(g_fl_mtime));
    pthread_mutex_unlock(&g_fl_seen_mu);
    atomic_store(&g_fl_syncs, 0);
    if (pthread_create(&g_fl_th, NULL, flusher_main, NULL) != 0) {
        pthread_cond_destroy(&g_fl_cv);
        pthread_mutex_unlock(&g_fl_mu);
        return set_error(ERR_IO, "cannot start the state.db flusher");
    }
    g_fl_running = 1;
    pthread_mutex_unlock(&g_fl_mu);
    return OK;
}

void state_db_flusher_stop(void)
{
    pthread_mutex_lock(&g_fl_mu);
    if (!g_fl_running) { pthread_mutex_unlock(&g_fl_mu); return; }
    g_fl_stop = 1;
    pthread_cond_signal(&g_fl_cv);
    pthread_mutex_unlock(&g_fl_mu);
    pthread_join(g_fl_th, NULL);
    flush_once();
    pthread_mutex_lock(&g_fl_mu);
    pthread_cond_destroy(&g_fl_cv);
    g_fl_running = 0;
    pthread_mutex_unlock(&g_fl_mu);
}

unsigned long state_db_flusher_syncs(void)
{
    return atomic_load(&g_fl_syncs);
}

int state_db_flusher_current(void)
{
    struct stat st;
    if (stat(g_fl_wal, &st) != 0) return 0;
    return seen_matches(&st);
}
