#define _POSIX_C_SOURCE 200809L

#include "checkpointer.h"

#include "db_txn.h"
#include "db_readers.h"
#include "sync_latch.h"
#include "thread_name.h"
#include "walidx.h"
#include "wal_health.h"
#include "wal_sync.h"

#include <cutils/log.h>
#include <sqlite3.h>

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
    int rc = walidx_read(db_path, &w);
    if (rc != 0) return rc;
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
static sqlite3        *g_ck;          /* opened by checkpointer_open, before the drop */
static int             g_db_fd = -1;  /* state.db itself, for presync; opened with g_ck */
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
/* Shutdown has begun (B218): no new checkpoint starts, and one interrupted
 * by checkpointer_halt is not a failure. */
static atomic_int    g_halting;
/* checkpointer_finish left the WAL: the close must not copy it. */
static int           g_leave_wal;

void checkpointer_set_test_idle(int on)
{
    atomic_store(&g_test_idle, on ? 1 : 0);
}

void checkpointer_set_test_pause_ms(unsigned ms)
{
    atomic_store(&g_test_pause_ms, ms);
}

static void report_sqlite_failure(const char *what, int rc)
{
    int ext = sqlite3_extended_errcode(g_ck);
    pthread_mutex_lock(&g_mu);
    g_stats.failed++;
    pthread_mutex_unlock(&g_mu);
    wal_health_fail(WAL_HEALTH_CHECKPOINT, "%s of %s refused: %s (SQLite code %d, extended %d)",
                    what, g_path, sqlite3_errmsg(g_ck), rc, ext);
}

static int passive(uint32_t *left)
{
    unsigned pause = atomic_load(&g_test_pause_ms);
    if (pause) {
        struct timespec ts = { (time_t)(pause / 1000), (long)(pause % 1000) * 1000000L };
        nanosleep(&ts, NULL);
    }
    int log = 0, done = 0;
    /* What was copied before, so the round's own copy can be counted
     * (B216). A restart in between leaves fewer backfilled frames than
     * before; then everything copied since counts. */
    uint32_t b0 = 0;
    if (checkpointer_wal_frames(g_path, NULL, &b0) != 0) b0 = 0;
    /* Checked before the call, and halt sets it before it interrupts: an
     * interrupt that lands between the two still aborts the call, since
     * SQLite clears the flag only when a call returns. */
    if (atomic_load(&g_halting)) return -1;
    int rc = sqlite3_wal_checkpoint_v2(g_ck, NULL, SQLITE_CHECKPOINT_PASSIVE, &log, &done);
    if (rc == SQLITE_INTERRUPT && atomic_load(&g_halting)) return -1;
    if (rc != SQLITE_OK && rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
        /* Not a checkpoint that could not start but one SQLite refused:
         * SQLITE_READONLY from a connection that could open the database
         * only read-only, an I/O error. It used to be counted as busy and
         * retried every tick, silently, for ever (B215). */
        report_sqlite_failure("PASSIVE checkpoint", rc);
        return -1;
    }
    if (rc != SQLITE_OK) {
        pthread_mutex_lock(&g_mu);
        g_stats.busy++;
        pthread_mutex_unlock(&g_mu);
        return -1;
    }
    if (log < 0) {
        /* SQLITE_OK with -1: the connection is not in WAL mode, so nothing
         * was copied and nothing ever will be. */
        report_sqlite_failure("PASSIVE checkpoint (the connection is not in WAL mode)", rc);
        return -1;
    }
    wal_health_ok(WAL_HEALTH_CHECKPOINT);
    uint32_t copied = (uint32_t)done >= b0 ? (uint32_t)done - b0 : (uint32_t)done;
    pthread_mutex_lock(&g_mu);
    g_stats.frames_copied += copied;
    pthread_mutex_unlock(&g_mu);
    *left = log > done ? (uint32_t)(log - done) : 0;
    return 0;
}

/* t_locked: when the write lock was granted, so the wait for it and the
 * hold are measured apart (B219). */
typedef struct { uint32_t left; int rc; uint64_t t_locked; } locked_arg_t;

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
 * write lock held, the checkpointer restarts the WAL and then writes one
 * harmless row on this connection, which pays that sync on this thread
 * instead; writers already queued for the escalation wait for it once, and
 * nobody pays it on their own write.
 *
 * The restart is a RESTART checkpoint, not the row (B201). A write restarts
 * the WAL only when no reader holds a read-mark slot, and the shared
 * connection's reads take one without the write lock: on FS2, requests and
 * the daemon's own lookups back to back after the backup meant one was
 * always running when the row was written, so it never restarted the WAL
 * and was an ordinary frame instead, one more each tick for as long as the
 * daemon ran. A RESTART checkpoint waits, through the busy timeout
 * (CKPT_BUSY_MS), for the readers already inside the WAL to finish, while
 * any reader that starts meanwhile reads the database file alone (slot 0,
 * the copy being complete), which a restart does not wait for. So the
 * readers drain and the restart lands with requests running; none of them
 * waits for any of it, and the round waits at most the busy timeout. Should
 * a reader outlast it, nothing is written: the next tick tries again.
 */
static int interrupted_by_halt(int rc)
{
    return rc == SQLITE_INTERRUPT && atomic_load(&g_halting);
}

static void pre_restart(void)
{
    if (atomic_load(&g_halting)) return;
    int rc = sqlite3_wal_checkpoint_v2(g_ck, NULL, SQLITE_CHECKPOINT_RESTART, NULL, NULL);
    if (interrupted_by_halt(rc)) return;
    if (rc != SQLITE_OK && rc != SQLITE_BUSY && rc != SQLITE_LOCKED)
        report_sqlite_failure("RESTART checkpoint", rc);
    if (rc != SQLITE_OK) {
        pthread_mutex_lock(&g_mu);
        g_stats.pre_restart_misses++;
        pthread_mutex_unlock(&g_mu);
        return;
    }
    /* A value the row has never held: SQLite writes no page for an update
     * that changes nothing, and with no frame the header sync would fall to
     * the next writer after all. */
    char v[32];
    snprintf(v, sizeof(v), "%lu", g_stats.pre_restarts + g_stats.pre_restart_misses + 1);
    uint64_t t0 = now_ns();
    sqlite3_stmt *st = NULL;
    rc = sqlite3_prepare_v2(g_ck,
            "INSERT INTO daemon_state (key, value) VALUES ('wal_restart', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value", -1, &st, NULL);
    if (rc == SQLITE_OK) rc = sqlite3_bind_text(st, 1, v, -1, SQLITE_TRANSIENT);
    if (rc == SQLITE_OK) rc = sqlite3_step(st) == SQLITE_DONE ? SQLITE_OK : sqlite3_errcode(g_ck);
    sqlite3_finalize(st);
    if (interrupted_by_halt(rc)) return;
    if (rc != SQLITE_OK && rc != SQLITE_BUSY && rc != SQLITE_LOCKED)
        report_sqlite_failure("the WAL restart write", rc);
    uint64_t took = now_ns() - t0;
    uint32_t f = 0, b = 0;
    int restarted = rc == SQLITE_OK && checkpointer_wal_frames(g_path, &f, &b) == 0 && f <= 8;
    pthread_mutex_lock(&g_mu);
    if (restarted) {
        g_stats.pre_restarts++;
        if (took > g_stats.max_restart_ns) g_stats.max_restart_ns = took;
    } else {
        g_stats.pre_restart_misses++;
    }
    pthread_mutex_unlock(&g_mu);
}

/*
 * Write out, without the lock, what a locked round would otherwise sync
 * holding it. A PASSIVE syncs the WAL before it copies and, when the copy
 * completes, the database after; under the lock both are what every writer
 * waits for.
 *
 *   - The WAL (B216): past the hard cap it is the whole backlog the
 *     writers left in the page cache.
 *   - The database (B233): an unlocked round that does not complete the
 *     copy writes pages into state.db and syncs nothing, since SQLite syncs
 *     the database only when the copy completes. Below the cap that is
 *     every round during a backup, so the pages of all of them waited for
 *     the locked round's sync: FS2 M9 R5 held the lock 23.3 s for a round
 *     with 2,043 frames left, almost all of it in that fdatasync.
 *
 * The locked round's own syncs then cover only what arrived since. The
 * syncs SQLite needs are unchanged - WAL before the copy, database before
 * the restart - only what is still dirty when they run. A failure latches
 * like any sync of state.db (sync_latch.h) and the round goes ahead: the
 * descriptor here is not SQLite's, so its own sync in the round sees the
 * same error (Linux reports a writeback error to every open file), the
 * copy does not complete, and the WAL is not restarted over frames the
 * database may not hold.
 */
static void presync(void)
{
    char wal[PATH_MAX + 8];
    snprintf(wal, sizeof(wal), "%s-wal", g_path);
    uint64_t s0 = now_ns();
    (void)wal_sync(wal);
    if (g_db_fd >= 0) (void)sync_latch_fdatasync(g_db_fd, g_path);
    uint64_t took = now_ns() - s0;
    pthread_mutex_lock(&g_mu);
    g_stats.presync_ns += took;
    g_stats.presyncs++;
    pthread_mutex_unlock(&g_mu);
}

static status_t locked_passive(void *arg)
{
    locked_arg_t *a = arg;
    a->t_locked = now_ns();
    /* With the pooled readers held out, none pins an old read mark: the
     * copy runs to the end and the restart lands (db_readers.h, B201). */
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
    presync();
    locked_arg_t a = { 0, -1, 0 };
    uint64_t t0 = now_ns();
    status_t st = db_txn_exclusive(locked_passive, &a);
    if (st != OK) return st;
    uint64_t held = now_ns() - a.t_locked;
    uint64_t waited = a.t_locked - t0;
    pthread_mutex_lock(&g_mu);
    g_stats.forced++;
    g_stats.lock_ns += held;
    g_stats.lock_wait_ns += waited;
    if (held > g_stats.max_lock_ns) g_stats.max_lock_ns = held;
    if (waited > g_stats.max_lock_wait_ns) g_stats.max_lock_wait_ns = waited;
    pthread_mutex_unlock(&g_mu);
    return a.rc == 0 ? OK : set_error(ERR_BUSY, "checkpointer_force: the copy did not complete");
}

static void tick(void)
{
    if (atomic_load(&g_test_idle)) return;
    uint32_t frames = 0, backfilled = 0;
    int wrc = checkpointer_wal_frames(g_path, &frames, &backfilled);
    if (wrc == -2) {
        /* -shm is there and this process cannot read it: without the
         * header the thread never sees the WAL grow, so it would never
         * checkpoint at all (B215). */
        int e = errno;
        pthread_mutex_lock(&g_mu);
        g_stats.failed++;
        pthread_mutex_unlock(&g_mu);
        wal_health_fail(WAL_HEALTH_WALIDX, "cannot read %s-shm: %s (errno %d)", g_path,
                        strerror(e), e);
        return;
    }
    if (wrc != 0) return;
    wal_health_ok(WAL_HEALTH_WALIDX);
    pthread_mutex_lock(&g_mu);
    g_stats.ticks++;
    if (frames > g_stats.max_wal_frames) g_stats.max_wal_frames = frames;
    pthread_mutex_unlock(&g_mu);

    uint64_t hard = (uint64_t)g_threshold * CKPT_HARD_FACTOR;
    uint32_t left = frames - backfilled;
    uint64_t round_ns = 0;
    /*
     * Past the hard cap: one unlocked PASSIVE, then the lock (B216). The
     * lock used to come first (B181: on a slow device each unlocked round
     * takes two syncs and the copy, and a writer that does not sync
     * appends far faster, so several rounds let the WAL run to several
     * times the cap). But a PASSIVE begins by syncing the WAL, and at the
     * cap that is the whole backlog the writers left in the page cache:
     * done under the lock, every writer waited for all of it, 5 to 25 s a
     * round in the B216 model and most of M8's 963 s of lock wait. One
     * round without the lock writes the backlog out and copies it while
     * the bulk writers are held at the cap (checkpointer_throttle), so the
     * round under the lock syncs and copies only what arrived since. The
     * syncs SQLite needs are unchanged - WAL before the copy, database
     * before the restart - only which of them runs while writers wait.
     */
    if (frames > hard) {
        uint32_t ignored = 0;
        if (passive(&ignored) == 0) {
            pthread_mutex_lock(&g_mu);
            g_stats.passive++;
            pthread_mutex_unlock(&g_mu);
        }
        goto locked;
    }
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
    /* What the unlocked rounds left dirty, the WAL's tail and the pages
     * they copied, goes to disk now, before the lock (presync). */
    presync();
    locked_arg_t a = { 0, -1, 0 };
    uint64_t t0 = now_ns();
    if (db_txn_exclusive(locked_passive, &a) != OK) return;
    /* The hold is from the grant; the time before it was spent queued for
     * the lock behind writers, which the hold used to include (B219). */
    uint64_t held = now_ns() - a.t_locked;
    uint64_t waited = a.t_locked - t0;
    pthread_mutex_lock(&g_mu);
    if (a.rc == 0) g_stats.escalations++;
    if (held > g_stats.max_lock_ns) g_stats.max_lock_ns = held;
    if (waited > g_stats.max_lock_wait_ns) g_stats.max_lock_wait_ns = waited;
    g_stats.forced++;
    g_stats.lock_ns += held;
    g_stats.lock_wait_ns += waited;
    pthread_mutex_unlock(&g_mu);
    /* A copy that could not complete under the lock (a reader holding an
     * old snapshot) cannot be helped by holding writers back: let them
     * through for a while rather than make each wait for another failure. */
    uint32_t f2 = 0, b2 = 0;
    if (a.rc != 0 || (checkpointer_wal_frames(g_path, &f2, &b2) == 0 && f2 > hard && f2 != b2))
        atomic_store(&g_blocked_until_ns, now_ns() + CKPT_BLOCKED_NS);
    /* Once a minute at most while past the cap (B201: this line every
     * tick was half of an idle daemon's writes), at once for a long hold. */
    static uint64_t last_warn_ns;
    uint64_t now = now_ns();
    if (held > 1000000000ull || (frames > hard && now - last_warn_ns >= 60000000000ull)) {
        last_warn_ns = now;
        uint32_t f3 = 0, b3 = 0;
        (void)checkpointer_wal_frames(g_path, &f3, &b3);
        log_warn("state.db: WAL at %u frames; checkpointed holding the write lock for %.0f ms, "
                 "after waiting %.0f ms for it (%u frames were left; now %u frames, %u copied, "
                 "%u readers pinned)",
                 frames, (double)held / 1e6, (double)waited / 1e6, left, f3, b3,
                 db_readers_audit());
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
    int timed_out = 0;
    for (;;) {
        struct timespec ts = { 0, 2 * 1000000L };
        nanosleep(&ts, NULL);
        uint64_t t = now_ns();
        if (!atomic_load(&g_active) || t < atomic_load(&g_blocked_until_ns) ||
            t - t0 > CKPT_THROTTLE_MAX_NS) {
            timed_out = t - t0 > CKPT_THROTTLE_MAX_NS;
            break;
        }
        if (checkpointer_wal_frames(g_path, &f, &b) != 0 || f <= hard || f == b) break;
    }
    uint64_t waited = now_ns() - t0;
    pthread_mutex_lock(&g_mu);
    g_stats.throttled++;
    if (waited > g_stats.max_throttle_ns) g_stats.max_throttle_ns = waited;
    g_stats.throttle_ns += waited;
    if (timed_out) g_stats.throttle_timeouts++;
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

/*
 * Its own connection: a checkpoint on the shared handle would hold that
 * handle's mutex, and every statement in the daemon, for the copy. Short
 * busy timeout, no automatic checkpoint of its own, and the daemon's sync
 * level, under which a checkpoint syncs the WAL before copying and the
 * database after.
 *
 * A raw SQLite handle rather than a c-utils one, for the result codes: a
 * refused checkpoint has to be told apart from a busy one and reported
 * with its code (B215).
 *
 * Read-write or not at all. SQLite falls back to a read-only open, without
 * an error, when the file cannot be opened for writing, and every
 * checkpoint on such a connection fails with SQLITE_READONLY. That was
 * B215: the daemon opened this connection after dropping to its run_as uid,
 * state.db was root's (AppGuard creates it before the drop), and the WAL
 * grew without bound while nothing said a word. Under g_mu.
 */
static status_t open_locked(const char *db_path)
{
    if (g_ck) {
        if (strcmp(g_path, db_path) == 0) return OK;
        return set_error(ERR_BUSY, "state.db checkpointer already open on %s", g_path);
    }
    if (snprintf(g_path, sizeof(g_path), "%s", db_path) >= (int)sizeof(g_path))
        return set_error(ERR_INVALID, "state.db path too long");
    sqlite3 *h = NULL;
    int rc = sqlite3_open_v2(db_path, &h, SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, NULL);
    if (rc != SQLITE_OK) {
        status_t st = set_error(ERR_IO, "state.db: cannot open the checkpoint connection to %s: "
                                "%s (SQLite code %d)", db_path,
                                h ? sqlite3_errmsg(h) : sqlite3_errstr(rc), rc);
        sqlite3_close(h);
        wal_health_fail(WAL_HEALTH_CHECKPOINT, "%s", err_msg());
        return st;
    }
    if (sqlite3_db_readonly(h, "main") == 1) {
        sqlite3_close(h);
        /* Not "give the file to the daemon's uid": after the privilege
         * drop the state files are root's, 0600, by design (Manual
         * 22.2.2), and this connection is opened before the drop. Read-only
         * here means this process could not write them when it opened them:
         * it was started as the uid it would drop to, or the mode denies
         * even root. */
        status_t st = set_error(ERR_IO, "state.db: the checkpoint connection could open %s "
                                "only read-only (uid %u cannot write it), so the WAL would "
                                "never be copied back. The daemon opens it before it drops to "
                                "security.run_as_uid, and the file stays root's, 0600, by "
                                "design: start the daemon as root, or make the file writable "
                                "by the uid it starts as",
                                db_path, (unsigned)geteuid());
        wal_health_fail(WAL_HEALTH_CHECKPOINT, "%s", err_msg());
        return st;
    }
    /* journal_mode=WAL also reads the database, which is what attaches the
     * connection to the WAL: until then a checkpoint on it succeeds with
     * -1 frames and copies nothing. */
    char pragmas[128];
    snprintf(pragmas, sizeof(pragmas), "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=%d; "
             "PRAGMA wal_autocheckpoint=0;", CKPT_BUSY_MS);
    rc = sqlite3_exec(h, pragmas, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        status_t st = set_error(ERR_IO, "state.db: cannot set up the checkpoint connection: "
                                "%s (SQLite code %d)", sqlite3_errmsg(h), rc);
        sqlite3_close(h);
        return st;
    }
    g_ck = h;
    /* A descriptor of its own for presync, opened here because this runs
     * before the privilege drop. Without one the database's sync stays in
     * the locked round, as it was before B233. */
    g_db_fd = open(db_path, O_RDONLY | O_CLOEXEC);
    if (g_db_fd < 0)
        log_warn("state.db: cannot open %s to sync it before a checkpoint takes the write "
                 "lock: %s", db_path, strerror(errno));
    return OK;
}

status_t checkpointer_open(const char *db_path)
{
    if (!db_path || !*db_path) return set_error(ERR_INVALID, "checkpointer_open: no path");
    pthread_mutex_lock(&g_mu);
    status_t st = open_locked(db_path);
    pthread_mutex_unlock(&g_mu);
    return st;
}

/*
 * The WAL file's size on disk follows its contents only if the connection
 * that restarts it carries journal_size_limit: SQLite trims the file at the
 * first commit after a restart, to that connection's limit. Since B144-REST
 * and B201 the restart is this connection's (pre_restart), and it carried
 * none, so the file stayed at its high-water mark however short the WAL
 * was. The trim is an ftruncate of the open file, never a new file, so the
 * root-owned -wal stays root's and every connection keeps its descriptor
 * (B215 follow-up). Under g_mu.
 */
static status_t limit_wal_locked(int threshold_pages)
{
    sqlite3_stmt *st = NULL;
    long long page = 0;
    if (sqlite3_prepare_v2(g_ck, "PRAGMA page_size", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW)
        page = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    if (page <= 0) page = 4096;
    char sql[80];
    snprintf(sql, sizeof(sql), "PRAGMA journal_size_limit=%lld",
             (long long)threshold_pages * CKPT_CAP_FACTOR * page);
    int rc = sqlite3_exec(g_ck, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK)
        return set_error(ERR_IO, "state.db: cannot set the checkpoint connection's "
                         "journal_size_limit: %s (SQLite code %d)", sqlite3_errmsg(g_ck), rc);
    return OK;
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
    if (open_locked(db_path) != OK || limit_wal_locked(threshold_pages) != OK) {
        pthread_mutex_unlock(&g_mu);
        return ERR_IO;
    }
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&g_cv, &ca);
    pthread_condattr_destroy(&ca);
    g_interval_ms = interval_ms;
    g_threshold = (uint32_t)threshold_pages;
    g_stop = 0;
    atomic_store(&g_halting, 0);
    memset(&g_stats, 0, sizeof(g_stats));
    if (pthread_create(&g_th, NULL, main_loop, NULL) != 0) {
        pthread_cond_destroy(&g_cv);
        sqlite3_close(g_ck);
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

void checkpointer_halt(void)
{
    pthread_mutex_lock(&g_mu);
    if (!g_running) { pthread_mutex_unlock(&g_mu); return; }
    atomic_store(&g_active, 0);
    atomic_store(&g_halting, 1);
    g_stop = 1;
    pthread_cond_signal(&g_cv);
    /* A copy of a large backlog runs for as long as the disk takes, and the
     * join used to wait for all of it (B218). SQLite checks the flag
     * between pages and keeps it until the call in progress, or the next,
     * returns, so whichever call the thread is in or about to make stops;
     * what it copied is simply copied again later. g_ck stays open until
     * the join below, so the handle is valid here. */
    sqlite3_interrupt(g_ck);
    pthread_mutex_unlock(&g_mu);
    pthread_join(g_th, NULL);
    pthread_mutex_lock(&g_mu);
    pthread_cond_destroy(&g_cv);
    g_running = 0;
    pthread_mutex_unlock(&g_mu);
}

/* Interrupts checkpointer_finish's copy at its deadline. */
typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             done;
    int             fired;
    unsigned        budget_ms;
} final_watch_t;

static void *final_watch(void *arg)
{
    final_watch_t *w = arg;
    struct timespec until;
    clock_gettime(CLOCK_MONOTONIC, &until);
    until.tv_sec  += (time_t)(w->budget_ms / 1000);
    until.tv_nsec += (long)(w->budget_ms % 1000) * 1000000L;
    if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
    pthread_mutex_lock(&w->mu);
    while (!w->done && pthread_cond_timedwait(&w->cv, &w->mu, &until) != ETIMEDOUT) {}
    if (!w->done) {
        w->fired = 1;
        sqlite3_interrupt(g_ck);
    }
    pthread_mutex_unlock(&w->mu);
    return NULL;
}

int checkpointer_finish(unsigned budget_ms)
{
    checkpointer_halt();
    pthread_mutex_lock(&g_mu);
    if (!g_ck) { pthread_mutex_unlock(&g_mu); return 1; }
    g_leave_wal = 0;
    /* Any statement clears an interrupt flag the halt left set with no
     * call to consume it. */
    (void)sqlite3_exec(g_ck, "SELECT 1", NULL, NULL, NULL);
    uint32_t f = 0, b = 0;
    if (checkpointer_wal_frames(g_path, &f, &b) != 0) {
        /* No readable wal-index: nothing to judge the copy by. The close
         * would copy whatever there is, unbounded; leave it instead. */
        g_leave_wal = 1;
        pthread_mutex_unlock(&g_mu);
        log_info("state.db: leaving the WAL for the next start (its index could not be read)");
        return 0;
    }
    uint32_t left = f - b;
    if (left == 0) { pthread_mutex_unlock(&g_mu); return 1; }
    if (left > CKPT_FINAL_FRAMES) {
        g_leave_wal = 1;
        pthread_mutex_unlock(&g_mu);
        log_info("state.db: leaving %u uncopied WAL frames (%.0f MiB) for the next start "
                 "rather than copy them while the stop waits", left,
                 (double)left * 4096.0 / 1048576.0);
        return 0;
    }

    final_watch_t w;
    memset(&w, 0, sizeof(w));
    pthread_mutex_init(&w.mu, NULL);
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&w.cv, &ca);
    pthread_condattr_destroy(&ca);
    w.budget_ms = budget_ms;
    pthread_t th;
    int watching = pthread_create(&th, NULL, final_watch, &w) == 0;
    uint64_t t0 = now_ns();
    int log = 0, done = 0;
    int rc = sqlite3_wal_checkpoint_v2(g_ck, NULL, SQLITE_CHECKPOINT_PASSIVE, &log, &done);
    uint64_t took = now_ns() - t0;
    if (watching) {
        pthread_mutex_lock(&w.mu);
        w.done = 1;
        pthread_cond_signal(&w.cv);
        pthread_mutex_unlock(&w.mu);
        pthread_join(th, NULL);
    }
    pthread_cond_destroy(&w.cv);
    pthread_mutex_destroy(&w.mu);
    /* The watch may have fired as the copy returned: clear the flag, so
     * the close is not interrupted by it. */
    (void)sqlite3_exec(g_ck, "SELECT 1", NULL, NULL, NULL);
    int complete = rc == SQLITE_OK && log >= 0 && done == log;
    if (rc == SQLITE_OK && done > 0)
        g_stats.frames_copied += (uint32_t)done >= b ? (uint32_t)done - b : (uint32_t)done;
    if (!complete) g_leave_wal = 1;
    pthread_mutex_unlock(&g_mu);
    if (complete) {
        log_debug("state.db: final checkpoint copied %d frames in %.0f ms", done,
                  (double)took / 1e6);
    } else if (rc == SQLITE_INTERRUPT && w.fired) {
        log_info("state.db: the final checkpoint did not finish within %u ms; leaving %u WAL "
                 "frames for the next start", budget_ms, left);
    } else if (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
        log_info("state.db: the final checkpoint could not copy everything (%u frames left, "
                 "a reader holds an older snapshot); leaving the WAL for the next start", left);
    } else {
        log_warn("state.db: the final checkpoint failed: %s (SQLite code %d); leaving the WAL "
                 "for the next start", sqlite3_errmsg(g_ck), rc);
    }
    return complete;
}

/* The close leaves the WAL in place: make what the last writers added to it
 * since the flusher's final sync (the log writer's rows) durable, which the
 * close's own checkpoint would otherwise have done. */
static void sync_left_wal(void)
{
    char wal[PATH_MAX + 8];
    snprintf(wal, sizeof(wal), "%s-wal", g_path);
    int fd = open(wal, O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno != ENOENT) log_warn("state.db: cannot open %s to sync it: %s", wal, strerror(errno));
        return;
    }
    if (fsync(fd) != 0) log_warn("state.db: cannot sync %s: %s", wal, strerror(errno));
    close(fd);
}

void checkpointer_stop(void)
{
    checkpointer_halt();
    pthread_mutex_lock(&g_mu);
    if (g_ck) {
        if (g_leave_wal) {
            /* SQLite would copy the whole WAL in the close when this is the
             * last connection, which is what checkpointer_finish declined
             * to wait for. */
            (void)sqlite3_db_config(g_ck, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, (int *)NULL);
            sync_left_wal();
        } else {
            /* An interrupt from the halt that no call consumed would stop
             * the close's own checkpoint. */
            (void)sqlite3_exec(g_ck, "SELECT 1", NULL, NULL, NULL);
        }
        sqlite3_close(g_ck);
    }
    g_ck = NULL;
    if (g_db_fd >= 0) close(g_db_fd);
    g_db_fd = -1;
    g_leave_wal = 0;
    atomic_store(&g_halting, 0);
    pthread_mutex_unlock(&g_mu);
}

void checkpointer_stats(checkpointer_stats_t *out)
{
    pthread_mutex_lock(&g_mu);
    *out = g_stats;
    pthread_mutex_unlock(&g_mu);
}
