#define _POSIX_C_SOURCE 200809L

#include "checkpointer.h"

#include "db_txn.h"
#include "db_readers.h"
#include "thread_name.h"
#include "walidx.h"

#include <cutils/db.h>
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
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
/* B181: hard-cap state shared with writers (checkpointer_throttle). */
static atomic_int    g_active;            /* the thread is running */
static atomic_ullong g_blocked_until_ns;  /* an escalation could not finish */
static atomic_ullong g_last_probe_ns;     /* writers' last look at the WAL */
static atomic_uint   g_test_pause_ms;
static atomic_int    g_test_idle;

void checkpointer_set_test_idle(int on)
{
    atomic_store(&g_test_idle, on ? 1 : 0);
}

void checkpointer_set_test_pause_ms(unsigned ms)
{
    atomic_store(&g_test_pause_ms, ms);
}

static int passive(uint32_t *left)
{
    unsigned pause = atomic_load(&g_test_pause_ms);
    if (pause) {
        struct timespec ts = { (time_t)(pause / 1000), (long)(pause % 1000) * 1000000L };
        nanosleep(&ts, NULL);
    }
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
#define CKPT_PROBE_NS        (5ull * 1000000ull)      /* writers look at most this often */
#define CKPT_THROTTLE_MAX_NS (2000ull * 1000000ull)   /* and never wait longer */
#define CKPT_BLOCKED_NS      (1000ull * 1000000ull)   /* after a copy that could not finish */
#define CKPT_LOCK_BUDGET_NS (200ull * 1000000ull)

/*
 * The WAL restart itself, done here rather than by whoever writes next
 * (B144-REST). The first frame written after a restart rewrites the WAL
 * header, and SQLite syncs it before going on (an out-of-order write after a
 * restart could otherwise corrupt the database), so the first writer pays a
 * sync while holding the write lock: FS2 measured 0.3 to 1.5 s, landing on a
 * job's progress UPDATE or on a request. With the copy complete and the
 * write lock held, one harmless row written on this connection restarts the
 * WAL and pays that sync on this thread instead; writers already queued for
 * the escalation wait for it once, and nobody pays it on their own write.
 * A reader still inside the old WAL keeps the restart from happening; the
 * row is then an ordinary frame, which the next checkpoint copies.
 */
static void pre_restart(void)
{
    char v[32];
    snprintf(v, sizeof(v), "%lu", g_stats.pre_restarts + 1);
    uint64_t t0 = now_ns();
    int rc = db_execute_non_query(g_ck,
            "INSERT INTO daemon_state (key, value) VALUES ('wal_restart', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (const char *[]){ v, NULL }, NULL);
    uint64_t took = now_ns() - t0;
    uint32_t f = 0, b = 0;
    int restarted = rc == 0 && checkpointer_wal_frames(g_path, &f, &b) == 0 && f <= 8;
    pthread_mutex_lock(&g_mu);
    if (restarted) {
        g_stats.pre_restarts++;
        if (took > g_stats.max_restart_ns) g_stats.max_restart_ns = took;
    } else {
        g_stats.pre_restart_misses++;
    }
    pthread_mutex_unlock(&g_mu);
}

static status_t locked_passive(void *arg)
{
    locked_arg_t *a = arg;
    /* With the readers held out as well, no read mark is in use: the copy
     * runs to the end and the restart lands (db_readers.h, B201). */
    db_readers_quiesce();
    for (int i = 0; i < CKPT_LOCKED_ROUNDS; i++) {
        a->rc = passive(&a->left);
        if (a->rc != 0) break;
        uint32_t f = 0, b = 0;
        if (checkpointer_wal_frames(g_path, &f, &b) == 0 && f == b) {
            if (f > 0) pre_restart();
            break;
        }
    }
    db_readers_resume();
    return OK;
}

status_t checkpointer_force(void)
{
    if (!g_ck) return set_error(ERR_INVALID, "checkpointer_force: not running");
    locked_arg_t a = { 0, -1 };
    status_t st = db_txn_exclusive(locked_passive, &a);
    if (st != OK) return st;
    return a.rc == 0 ? OK : set_error(ERR_BUSY, "checkpointer_force: the copy did not complete");
}

static void tick(void)
{
    if (atomic_load(&g_test_idle)) return;
    uint32_t frames = 0, backfilled = 0;
    if (checkpointer_wal_frames(g_path, &frames, &backfilled) != 0) return;
    pthread_mutex_lock(&g_mu);
    g_stats.ticks++;
    if (frames > g_stats.max_wal_frames) g_stats.max_wal_frames = frames;
    pthread_mutex_unlock(&g_mu);

    uint64_t hard = (uint64_t)g_threshold * CKPT_HARD_FACTOR;
    uint32_t left = frames - backfilled;
    uint64_t round_ns = 0;
    /* Past the hard cap: straight to the lock, no unlocked rounds first.
     * On a slow device each unlocked PASSIVE takes two syncs and the copy,
     * and a writer that does not sync appends far faster than that, so the
     * rounds were where the WAL ran to several times the cap (B181). */
    if (frames > hard) goto locked;
    if (left < g_threshold) return;
    uint64_t r0 = now_ns();
    if (passive(&left) != 0) return;
    round_ns = now_ns() - r0;
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
    uint32_t small = g_threshold / 4 < CKPT_SMALL_FRAMES ? g_threshold / 4 : CKPT_SMALL_FRAMES;
    for (int round = 0; ; round++) {
        if (checkpointer_wal_frames(g_path, &frames, &backfilled) != 0) return;
        left = frames - backfilled;
        if (frames < g_threshold) return;                   /* restarted meanwhile */
        if (frames > hard) goto locked;
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

locked:;
    locked_arg_t a = { 0, -1 };
    uint64_t t0 = now_ns();
    if (db_txn_exclusive(locked_passive, &a) != OK) return;
    uint64_t held = now_ns() - t0;
    pthread_mutex_lock(&g_mu);
    if (a.rc == 0) g_stats.escalations++;
    if (held > g_stats.max_lock_ns) g_stats.max_lock_ns = held;
    pthread_mutex_unlock(&g_mu);
    /* A copy that could not complete under the lock (a reader holding an
     * old snapshot) cannot be helped by holding writers back: let them
     * through for a while rather than make each wait for another failure. */
    uint32_t f2 = 0, b2 = 0;
    if (a.rc != 0 || (checkpointer_wal_frames(g_path, &f2, &b2) == 0 && f2 > hard && f2 != b2))
        atomic_store(&g_blocked_until_ns, now_ns() + CKPT_BLOCKED_NS);
    if (held > 1000000000ull || frames > hard) {
        uint32_t f3 = 0, b3 = 0;
        (void)checkpointer_wal_frames(g_path, &f3, &b3);
        log_warn("state.db: WAL at %u frames; checkpointed under the write lock in %.0f ms "
                 "(%u frames were left; now %u frames, %u copied, %u readers pinned)",
                 frames, (double)held / 1e6, left, f3, b3, db_readers_audit());
    }
}

/*
 * Writers' half of the hard cap (B181). The checkpointer alone cannot hold
 * the WAL to the cap on a slow device: between two of its looks a writer
 * that never syncs appends tens of thousands of frames. So every bulk
 * writer, before it queues for the write lock (db_txn_begin_bulk, and each
 * db_write_batch), looks at the WAL -
 * at most every CKPT_PROBE_NS across all writers, a stat and a pread - and
 * once it is past the hard cap and not fully copied, wakes the
 * checkpointer and waits for the copy to complete. Holding no lock while
 * it waits, it cannot hold up the checkpoint it waits for. It stops
 * waiting when the copy is complete (the first writer after it restarts
 * the WAL), when the checkpointer has found it cannot complete one
 * (g_blocked_until_ns), or after CKPT_THROTTLE_MAX_NS whatever happens.
 *
 * Only bulk writers: a request's write (db_txn_begin, db_write) never
 * waits here. Those writes are a few pages each and cannot carry the WAL
 * past the cap; the writers that can are the ones that write without
 * pause, and holding them is enough.
 */
void checkpointer_throttle(void)
{
    if (!atomic_load_explicit(&g_active, memory_order_relaxed)) return;
    uint64_t now = now_ns();
    uint64_t last = atomic_load_explicit(&g_last_probe_ns, memory_order_relaxed);
    if (now - last < CKPT_PROBE_NS) return;
    atomic_store_explicit(&g_last_probe_ns, now, memory_order_relaxed);
    if (now < atomic_load(&g_blocked_until_ns)) return;
    uint64_t hard = (uint64_t)g_threshold * CKPT_HARD_FACTOR;
    uint32_t f = 0, b = 0;
    if (checkpointer_wal_frames(g_path, &f, &b) != 0 || f <= hard || f == b) return;

    pthread_mutex_lock(&g_mu);
    pthread_cond_signal(&g_cv);
    pthread_mutex_unlock(&g_mu);
    uint64_t t0 = now;
    for (;;) {
        struct timespec ts = { 0, 2 * 1000000L };
        nanosleep(&ts, NULL);
        uint64_t t = now_ns();
        if (!atomic_load(&g_active) || t < atomic_load(&g_blocked_until_ns) ||
            t - t0 > CKPT_THROTTLE_MAX_NS) break;
        if (checkpointer_wal_frames(g_path, &f, &b) != 0 || f <= hard || f == b) break;
    }
    uint64_t waited = now_ns() - t0;
    pthread_mutex_lock(&g_mu);
    g_stats.throttled++;
    if (waited > g_stats.max_throttle_ns) g_stats.max_throttle_ns = waited;
    pthread_mutex_unlock(&g_mu);
}

static void *main_loop(void *arg)
{
    thread_name("cb-checkpoint");
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
    atomic_store(&g_blocked_until_ns, 0);
    atomic_store(&g_active, 1);
    pthread_mutex_unlock(&g_mu);
    return OK;
}

void checkpointer_stop(void)
{
    pthread_mutex_lock(&g_mu);
    if (!g_running) { pthread_mutex_unlock(&g_mu); return; }
    atomic_store(&g_active, 0);
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
