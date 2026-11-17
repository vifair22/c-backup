#define _POSIX_C_SOURCE 200809L

#include "checkpointer.h"

#include "db_txn.h"
#include "walidx.h"

#include <cutils/db.h>
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* The wal-index header                                                */
/* ------------------------------------------------------------------ */

/* Through walidx (B173): an open and close of -shm here would drop the
 * locks SQLite's connections hold on it. */
int checkpointer_wal_frames(const char *db_path, uint32_t *frames, uint32_t *backfilled)
{
    walidx_t w;
    if (walidx_read(db_path, &w) != 0) return -1;
    if (frames) *frames = w.frames;
    if (backfilled) *backfilled = w.backfilled;
    return 0;
}

/* ------------------------------------------------------------------ */
/* The thread                                                          */
/* ------------------------------------------------------------------ */

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv;
static pthread_t       g_th;
static int             g_running;
static int             g_stop;
static unsigned        g_interval_ms;
static uint32_t        g_threshold;
static char            g_path[PATH_MAX];
static cutils_db_t    *g_ck;
static checkpointer_stats_t g_stats;

static uint64_t now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

/* One PASSIVE checkpoint on the thread's connection. 0 when it ran (busy
 * or not), with the frames left uncopied in *left; -1 when it could not. */
static int passive(uint32_t *left)
{
    db_result_t *r = NULL;
    int rc = db_execute(g_ck, "PRAGMA wal_checkpoint(PASSIVE)", NULL, &r);
    int ok = rc == 0 && r && r->nrows == 1 && r->ncols >= 3;
    long long busy = ok ? strtoll(r->rows[0][0], NULL, 10) : 1;
    long long log  = ok ? strtoll(r->rows[0][1], NULL, 10) : 0;
    long long done = ok ? strtoll(r->rows[0][2], NULL, 10) : 0;
    db_result_free(r);
    if (!ok || busy) {
        pthread_mutex_lock(&g_mu);
        g_stats.busy++;
        pthread_mutex_unlock(&g_mu);
        return -1;
    }
    *left = log > done ? (uint32_t)(log - done) : 0;
    return 0;
}

typedef struct { uint32_t left; int rc; } locked_arg_t;

/*
 * Under the write lock no db_txn writer can commit, but a write issued
 * outside db_txn (job progress, notes, policy, sessions) still can, and
 * one whose snapshot began before the copy completed leaves a frame the
 * copy did not include; then no writer can restart the WAL until the next
 * complete copy. So copy until the wal-index says everything is copied,
 * a few times at most: each round copies only what such writes added.
 */
#define CKPT_LOCKED_ROUNDS 4
#define CKPT_UNLOCKED_ROUNDS 4
#define CKPT_SMALL_FRAMES  1024     /* 4 MiB: always small enough to copy under the lock */
#define CKPT_LOCK_BUDGET_NS (200ull * 1000000ull)

static status_t locked_passive(void *arg)
{
    locked_arg_t *a = arg;
    for (int i = 0; i < CKPT_LOCKED_ROUNDS; i++) {
        a->rc = passive(&a->left);
        if (a->rc != 0) break;
        uint32_t f = 0, b = 0;
        if (checkpointer_wal_frames(g_path, &f, &b) == 0 && f == b) break;
    }
    return OK;
}

static void tick(void)
{
    uint32_t frames = 0, backfilled = 0;
    if (checkpointer_wal_frames(g_path, &frames, &backfilled) != 0) return;
    pthread_mutex_lock(&g_mu);
    g_stats.ticks++;
    if (frames > g_stats.max_wal_frames) g_stats.max_wal_frames = frames;
    pthread_mutex_unlock(&g_mu);

    if (frames - backfilled < g_threshold) return;
    uint32_t left = 0;
    uint64_t r0 = now_ns();
    if (passive(&left) != 0) return;
    uint64_t round_ns = now_ns() - r0;
    pthread_mutex_lock(&g_mu);
    g_stats.passive++;
    pthread_mutex_unlock(&g_mu);

    /*
     * Copied, but a copy is only half of bounding the WAL: the next writer
     * restarts it only if that writer's snapshot began after the copy
     * completed, and a backup's batch began long before. One more PASSIVE
     * under the write lock settles it: the batch commits to let the lock
     * go, the remainder copied is small, and the next db_txn writer begins
     * on a complete copy and restarts the WAL. A large remainder waits for
     * the next tick, unless the WAL is past the hard cap.
     */
    uint64_t hard = (uint64_t)g_threshold * CKPT_HARD_FACTOR;
    uint32_t small = g_threshold / 4 < CKPT_SMALL_FRAMES ? g_threshold / 4 : CKPT_SMALL_FRAMES;
    for (int round = 0; ; round++) {
        if (checkpointer_wal_frames(g_path, &frames, &backfilled) != 0) return;
        left = frames - backfilled;
        if (frames < g_threshold) return;                   /* restarted meanwhile */
        if (left <= small || round >= CKPT_UNLOCKED_ROUNDS) break;
        /* Each round copies only what arrived during the last, so the
         * remainder shrinks quickly unless writers outpace the copy. */
        r0 = now_ns();
        if (passive(&left) != 0) return;
        round_ns = now_ns() - r0;
        pthread_mutex_lock(&g_mu);
        g_stats.passive++;
        pthread_mutex_unlock(&g_mu);
    }
    /* What is left is about what arrived during the last round, so copying
     * it under the lock takes about as long as that round did: escalate if
     * that is within budget, even when writers are too fast for the
     * remainder to shrink below small. */
    if (left > small && round_ns > CKPT_LOCK_BUDGET_NS && frames <= hard) return;

    locked_arg_t a = { 0, -1 };
    uint64_t t0 = now_ns();
    if (db_txn_exclusive(locked_passive, &a) != OK) return;
    uint64_t held = now_ns() - t0;
    pthread_mutex_lock(&g_mu);
    if (a.rc == 0) g_stats.escalations++;
    if (held > g_stats.max_lock_ns) g_stats.max_lock_ns = held;
    pthread_mutex_unlock(&g_mu);
    if (held > 1000000000ull || frames > hard)
        log_warn("state.db: WAL at %u frames; checkpointed under the write lock in %.0f ms "
                 "(%u frames were left)", frames, (double)held / 1e6, left);
}

static void *main_loop(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&g_mu);
    while (!g_stop) {
        struct timespec until;
        clock_gettime(CLOCK_MONOTONIC, &until);
        until.tv_sec  += (time_t)(g_interval_ms / 1000);
        until.tv_nsec += (long)(g_interval_ms % 1000) * 1000000L;
        if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
        while (!g_stop && pthread_cond_timedwait(&g_cv, &g_mu, &until) != ETIMEDOUT) {}
        if (g_stop) break;
        pthread_mutex_unlock(&g_mu);
        tick();
        pthread_mutex_lock(&g_mu);
    }
    pthread_mutex_unlock(&g_mu);
    return NULL;
}

status_t checkpointer_start(const char *db_path, int threshold_pages, unsigned interval_ms)
{
    if (!db_path || !*db_path || threshold_pages <= 0 || interval_ms == 0)
        return set_error(ERR_INVALID, "checkpointer_start: bad argument");
    pthread_mutex_lock(&g_mu);
    if (g_running) {
        pthread_mutex_unlock(&g_mu);
        return set_error(ERR_BUSY, "state.db checkpointer already running");
    }
    if (snprintf(g_path, sizeof(g_path), "%s", db_path) >= (int)sizeof(g_path)) {
        pthread_mutex_unlock(&g_mu);
        return set_error(ERR_INVALID, "state.db path too long");
    }
    /*
     * Its own connection: a checkpoint on the shared handle would hold
     * that handle's mutex, and every statement in the daemon, for the copy.
     * Short busy timeout, no automatic checkpoint of its own, and the
     * daemon's sync level, under which a checkpoint syncs the WAL before
     * copying and the database after.
     */
    char pragmas[96];
    snprintf(pragmas, sizeof(pragmas), "PRAGMA busy_timeout=%d; PRAGMA wal_autocheckpoint=0;",
             CKPT_BUSY_MS);
    if (db_open(&g_ck, db_path) != 0 || db_exec_raw(g_ck, pragmas) != 0) {
        if (g_ck) db_close(g_ck);
        g_ck = NULL;
        pthread_mutex_unlock(&g_mu);
        return set_error(ERR_IO, "cannot open a checkpoint connection to %s", db_path);
    }
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&g_cv, &ca);
    pthread_condattr_destroy(&ca);
    g_interval_ms = interval_ms;
    g_threshold = (uint32_t)threshold_pages;
    g_stop = 0;
    memset(&g_stats, 0, sizeof(g_stats));
    if (pthread_create(&g_th, NULL, main_loop, NULL) != 0) {
        pthread_cond_destroy(&g_cv);
        db_close(g_ck);
        g_ck = NULL;
        pthread_mutex_unlock(&g_mu);
        return set_error(ERR_IO, "cannot start the state.db checkpointer");
    }
    g_running = 1;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

void checkpointer_stop(void)
{
    pthread_mutex_lock(&g_mu);
    if (!g_running) { pthread_mutex_unlock(&g_mu); return; }
    g_stop = 1;
    pthread_cond_signal(&g_cv);
    pthread_mutex_unlock(&g_mu);
    pthread_join(g_th, NULL);
    pthread_mutex_lock(&g_mu);
    pthread_cond_destroy(&g_cv);
    db_close(g_ck);
    g_ck = NULL;
    g_running = 0;
    pthread_mutex_unlock(&g_mu);
}

void checkpointer_stats(checkpointer_stats_t *out)
{
    pthread_mutex_lock(&g_mu);
    *out = g_stats;
    pthread_mutex_unlock(&g_mu);
}
