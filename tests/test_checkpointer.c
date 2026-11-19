#define _GNU_SOURCE

/*
 * SPEED-3: state.db's checkpoints run on their own connection, off the
 * request path, and the WAL stays bounded anyway.
 *
 *   - No COMMIT on the shared handle checkpoints, however far past the
 *     threshold the WAL goes: the database file does not change.
 *   - Under a writer that never pauses - a long backup's batches - the WAL
 *     stays near the cap, which takes the escalation (checkpointer.h):
 *     PASSIVE alone never finds the copy complete at a BEGIN.
 *   - A reader pinning an old snapshot stops checkpoints from completing,
 *     and writers are not held up for it; once it lets go, the WAL comes
 *     back down.
 *
 * The threshold is the smallest the config accepts (1000 pages), so the
 * cases write tens of MiB rather than hundreds.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <linux/capability.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "checkpointer.h"
#include "db_readers.h"
#include "db_txn.h"
#include "db_write.h"
#include "state_db.h"
#include "wal_health.h"
#include "full_vfs.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_checkpointer"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_SHM  TEST_DB "-shm"
#define TEST_WAL  TEST_DB "-wal"
#define THRESHOLD 1000
#define ROW_BYTES 16000          /* about four pages a row */

static cutils_db_t *db;
static char g_blob[ROW_BYTES + 1];

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static uint64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

static void sleep_ms(long ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    /* Passes everything through until a case arms it (B221) or slows it
     * down (B218). */
    if (full_vfs_install() != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    state_db_tuning_t t = { "normal", 8, THRESHOLD };
    if (state_db_tune(db, &t) != OK) return -1;
    memset(g_blob, 'x', ROW_BYTES);
    g_blob[ROW_BYTES] = '\0';
    /* Each row its own pages, so a commit always adds frames. */
    /* daemon_state: where the checkpointer writes the row that restarts
     * the WAL (B144-REST), as migration 002 creates it. */
    return db_exec_raw(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"
                           " CREATE TABLE daemon_state (key TEXT PRIMARY KEY,"
                           " value TEXT NOT NULL);") == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    checkpointer_set_test_idle(0);
    checkpointer_set_test_pause_ms(0);
    full_vfs_arm(0);
    atomic_store(&full_vfs_delay_us, 0);
    checkpointer_stop();
    db_readers_close();
    if (db) db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* One commit through db_txn, as every writer in the daemon makes them.
 * Rows are rewritten over a fixed set of ids, so the WAL sees every page
 * while the database file stays a few MiB. */
static atomic_uint g_row;

static int commit_rows_with(status_t (*begin)(cutils_db_t *), int n)
{
    if (begin(db) != OK) return -1;
    for (int i = 0; i < n; i++) {
        char id[16];
        snprintf(id, sizeof(id), "%u", atomic_fetch_add(&g_row, 1) % 512u);
        if (db_execute_non_query(db, "INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)",
                                 (const char *[]){ id, g_blob, NULL }, NULL) != 0) {
            db_txn_rollback(db);
            return -1;
        }
    }
    return db_txn_commit(db) == OK ? 0 : -1;
}

/* The writers here stand for a backup's batches, which are bulk writers
 * (db_txn_begin_bulk, B181). */
static int commit_rows(int n)
{
    return commit_rows_with(db_txn_begin_bulk, n);
}

static uint32_t wal_frames(void)
{
    uint32_t f = 0, b = 0;
    for (int i = 0; i < 100; i++) {
        if (checkpointer_wal_frames(TEST_DB, &f, &b) == 0) return f;
        sleep_ms(1);
    }
    fail_msg("wal-index header never read consistently");
    return 0;
}

/* The shared handle never checkpoints: five thresholds' worth of commits
 * and the database file is exactly as it was. */
static void test_no_commit_checkpoints(void **state)
{
    (void)state;
    assert_int_equal(commit_rows(1), 0);
    struct stat before, after;
    assert_int_equal(stat(TEST_DB, &before), 0);
    for (int i = 0; i < THRESHOLD * 5 / 40; i++) assert_int_equal(commit_rows(10), 0);
    assert_int_equal(stat(TEST_DB, &after), 0);
    assert_true(before.st_size == after.st_size &&
                before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
                before.st_mtim.tv_nsec == after.st_mtim.tv_nsec);
    uint32_t f = wal_frames(), b = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);
    assert_true(f > THRESHOLD * 4);
    assert_int_equal(b, 0);                       /* nothing copied back */
}

typedef struct {
    atomic_int stop;
    long       pause_us;     /* between commits; 0 for none */
    atomic_uint commits;     /* read by run_writer_until while it runs */
    uint64_t   max_ms;
    atomic_int failed;
} writer_t;

/* Commits a case needs before its WAL is about `frames` frames: four rows a
 * commit, each about four pages, as the cases' own assertions count them. */
#define COMMITS_FOR(frames) ((unsigned)((frames) / (4 * 4) + 1))

/* A backup's batches: commit after commit, no pause between. */
static void *writer_main(void *arg)
{
    writer_t *w = arg;
    while (!atomic_load(&w->stop)) {
        uint64_t t0 = now_ms();
        if (commit_rows(4) != 0) { atomic_store(&w->failed, 1); break; }
        uint64_t took = now_ms() - t0;
        if (took > w->max_ms) w->max_ms = took;
        atomic_fetch_add(&w->commits, 1);
        if (w->pause_us) {
            struct timespec ts = { 0, w->pause_us * 1000L };
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

/*
 * Run the writer for at least ms, until it has made min_commits and until
 * enough() says so, when there is one. The
 * cases assert both what the WAL did and that enough was written for it to
 * mean something, and a writer bounded by time alone wrote too little on a
 * contended host (FLAKE-33: 589 commits in 6 s against the 751 asked for).
 * Ten times ms is the backstop; a writer slower than that fails the case's
 * own "enough written" assertion rather than passing on less.
 */
static uint32_t run_writer_until(writer_t *w, uint64_t ms, unsigned min_commits,
                                 int (*enough)(void))
{
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, writer_main, w), 0);
    uint32_t peak = 0;
    uint64_t start = now_ms(), end = start + ms, backstop = start + ms * 10;
    for (;;) {
        uint64_t now = now_ms();
        if (now >= backstop || atomic_load(&w->failed)) break;
        if (now >= end && atomic_load(&w->commits) >= min_commits &&
            (!enough || enough())) break;
        uint32_t f = 0, b = 0;
        if (checkpointer_wal_frames(TEST_DB, &f, &b) == 0 && f > peak) peak = f;
        sleep_ms(5);
    }
    atomic_store(&w->stop, 1);
    pthread_join(th, NULL);
    assert_int_equal(w->failed, 0);
    return peak;
}

static void report(const char *what, const writer_t *w, uint32_t peak)
{
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    print_message("  %s: %u commits, %.1f MiB, slowest %llu ms; WAL peak %u frames "
                  "(cap %u, hard %u); %lu passive, %lu escalated, longest hold %.1f ms\n",
                  what, atomic_load(&w->commits),
                  (double)atomic_load(&w->commits) * 4 * ROW_BYTES / 1048576.0,
                  (unsigned long long)w->max_ms, peak, THRESHOLD * CKPT_CAP_FACTOR,
                  THRESHOLD * CKPT_HARD_FACTOR, s.passive, s.escalations,
                  (double)s.max_lock_ns / 1e6);
}

/*
 * The WAL stays bounded under a writer that keeps committing, as a long
 * backup does. Frames arrive while every PASSIVE runs, so PASSIVE alone
 * rarely finds the copy complete at a BEGIN; the escalation (one more
 * PASSIVE with the write lock held, over a small remainder) is what lets
 * the WAL restart, and it stays near the cap.
 */
static void test_wal_stays_near_the_cap_under_a_long_writer(void **state)
{
    (void)state;
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), ERR_BUSY);
    err_clear();
    writer_t w;
    memset(&w, 0, sizeof(w));
    w.pause_us = 5000;
    uint32_t peak = run_writer_until(&w, 6000, COMMITS_FOR(THRESHOLD * CKPT_CAP_FACTOR * 3), NULL);
    report("writer", &w, peak);
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    /* Enough written that an unbounded WAL would be several caps over. */
    assert_true((uint64_t)w.commits * 4 * 4 > (uint64_t)THRESHOLD * CKPT_CAP_FACTOR * 3);
    assert_true(peak < THRESHOLD * 3);
    assert_true(s.passive > 0);
    /* The restarts were the checkpointer's own: the header sync landed on
     * its thread, not on the writer's next commit. */
    print_message("  restarts made by the checkpointer: %lu (%lu missed), longest %.1f ms\n",
                  s.pre_restarts, s.pre_restart_misses, (double)s.max_restart_ns / 1e6);
    assert_true(s.pre_restarts > 0);
    assert_true(w.max_ms < 2000);
}

/*
 * A writer faster than a checkpoint can copy (no pause at all, a few
 * hundred MiB a second into the page cache), on a fast device and on one
 * whose syncs take 50 ms: the WAL holds to the hard cap (B181). What it
 * may pass the cap by is what writers append between two looks at the WAL
 * (5 ms) and the batch a writer already holds, so the bound asserted is a
 * quarter over the cap. Before B181 the checkpointer's unlocked rounds let
 * the WAL run to 2.7 times the cap at 20 ms a sync and 5.6 times at 50,
 * and this test, asserting twice the cap, failed on the slower CI runner.
 * Once the writer stops the WAL comes back under four thresholds.
 */
static void flat_out(unsigned pause_ms)
{
    checkpointer_set_test_pause_ms(pause_ms);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    writer_t w;
    memset(&w, 0, sizeof(w));
    uint32_t peak = run_writer_until(&w, 4000, COMMITS_FOR(THRESHOLD * CKPT_HARD_FACTOR * 3), NULL);
    report(pause_ms ? "flat out, slow syncs" : "flat out", &w, peak);
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    print_message("  writers held at the hard cap: %lu, longest %.1f ms\n",
                  s.throttled, (double)s.max_throttle_ns / 1e6);
    assert_true((uint64_t)w.commits * 4 * 4 > (uint64_t)THRESHOLD * CKPT_HARD_FACTOR * 3);
    assert_true(peak < THRESHOLD * CKPT_HARD_FACTOR + THRESHOLD * CKPT_HARD_FACTOR / 4);
    assert_true(s.escalations > 0);
    uint32_t f = UINT32_MAX;
    for (int i = 0; i < 300 && f > THRESHOLD * CKPT_CAP_FACTOR; i++) {
        assert_int_equal(commit_rows(1), 0);
        f = wal_frames();
        sleep_ms(10);
    }
    assert_true(f <= THRESHOLD * CKPT_CAP_FACTOR);
    checkpointer_set_test_pause_ms(0);
}

static void test_wal_is_bounded_under_a_writer_faster_than_checkpoints(void **state)
{
    (void)state;
    flat_out(0);
}

static void test_the_hard_cap_holds_on_a_slow_device(void **state)
{
    (void)state;
    flat_out(50);
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    assert_true(s.throttled > 0);        /* writers did their half */
    /* And /metrics' totals (B216) moved with them. */
    assert_true(s.forced > 0);
    assert_true(s.lock_ns >= s.max_lock_ns && s.max_lock_ns > 0);
    assert_true(s.frames_copied > 0);
    assert_true(s.throttle_ns >= s.max_throttle_ns && s.max_throttle_ns > 0);
    assert_true(s.throttle_timeouts <= s.throttled);
}

/*
 * A reader holding an old snapshot (another connection, in a read
 * transaction) stops any checkpoint from completing. Writers carry on
 * without waiting for the checkpointer, and once the reader lets go the
 * WAL comes back under the cap.
 */
static void test_pinned_reader_holds_nobody_up(void **state)
{
    (void)state;
    assert_int_equal(commit_rows(1), 0);
    cutils_db_t *reader = NULL;
    assert_int_equal(db_open(&reader, TEST_DB), 0);
    assert_int_equal(db_exec_raw(reader, "BEGIN"), 0);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(reader, "SELECT COUNT(*) FROM t", NULL, &r), 0);
    db_result_free(r);

    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    writer_t w;
    memset(&w, 0, sizeof(w));
    w.pause_us = 2000;
    uint32_t peak = run_writer_until(&w, 2000, COMMITS_FOR(THRESHOLD * CKPT_CAP_FACTOR * 2), NULL);
    report("pinned", &w, peak);
    assert_true(peak > THRESHOLD * CKPT_CAP_FACTOR);   /* it could not checkpoint */
    assert_true(w.max_ms < 2000);                      /* and nobody waited for it */

    assert_int_equal(db_exec_raw(reader, "COMMIT"), 0);
    db_close(reader);
    /* Released: the next checkpoint completes and a BEGIN restarts the WAL. */
    uint32_t f = UINT32_MAX;
    for (int i = 0; i < 500 && f > THRESHOLD * CKPT_CAP_FACTOR; i++) {
        assert_int_equal(commit_rows(1), 0);
        f = wal_frames();
        sleep_ms(10);
    }
    assert_true(f <= THRESHOLD * CKPT_CAP_FACTOR);
}

/*
 * B181: at the hard cap only bulk writers wait. With the WAL past the cap
 * and its copy held incomplete (the thread idle), a bulk writer's BEGIN is
 * held, while a request-style write - a plain db_write - goes straight
 * through: a tag or a note never waits for a checkpoint. Once the thread
 * works again the copy completes and the bulk writer is let go.
 */
typedef struct {
    atomic_int done;
    uint64_t   waited_ms;
    int        rc;
} bulk_t;

static void *bulk_main(void *arg)
{
    bulk_t *b = arg;
    uint64_t t0 = now_ms();
    b->rc = commit_rows_with(db_txn_begin_bulk, 1);
    b->waited_ms = now_ms() - t0;
    atomic_store(&b->done, 1);
    return NULL;
}

static void test_only_bulk_writers_wait_at_the_hard_cap(void **state)
{
    (void)state;
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    checkpointer_set_test_idle(1);
    /* Past the cap with plain writes, which nothing holds back. */
    while (wal_frames() <= (uint32_t)THRESHOLD * CKPT_HARD_FACTOR)
        assert_int_equal(commit_rows_with(db_txn_begin, 8), 0);
    sleep_ms(10);                     /* past the writers' probe interval */

    bulk_t b;
    memset(&b, 0, sizeof(b));
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, bulk_main, &b), 0);
    sleep_ms(100);
    assert_int_equal(atomic_load(&b.done), 0);       /* held */

    uint64_t t0 = now_ms();
    assert_int_equal(db_write(db, "INSERT INTO t (v) VALUES ('a tag')", NULL, NULL), 0);
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_txn_commit(db), OK);
    uint64_t api_ms = now_ms() - t0;
    assert_int_equal(atomic_load(&b.done), 0);       /* still held */

    checkpointer_set_test_idle(0);
    pthread_join(th, NULL);
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    print_message("  request writes took %llu ms while a bulk writer waited %llu ms; "
                  "%lu held\n", (unsigned long long)api_ms,
                  (unsigned long long)b.waited_ms, s.throttled);
    assert_true(api_ms < 100);
    assert_int_equal(b.rc, 0);
    assert_true(b.waited_ms >= 100);
    assert_true(s.throttled >= 1);
}

/* ------------------------------------------------------------------ */
/* B201: the store's pooled readers must never pin the WAL              */
/* ------------------------------------------------------------------ */

typedef struct {
    atomic_int    stop;
    unsigned long lookups;
    int           errors;
} reader_t;

/* A file worker asking "already stored?" for chunk after chunk, flat out. */
/*
 * Every reader's lookups so far. The case asserts the readers made more than
 * 10,000 while the writer ran, and a writer bounded by 3 s alone left them
 * short on a contended host; the writer runs until they have.
 */
#define MIN_LOOKUPS 10000
static atomic_ulong g_lookups;

static int lookups_enough(void)
{
    return atomic_load(&g_lookups) > MIN_LOOKUPS;
}

static void *reader_main(void *arg)
{
    reader_t *r = arg;
    unsigned k = 0;
    while (!atomic_load(&r->stop)) {
        char hex[65];
        snprintf(hex, sizeof(hex), "%064x", k++ % 4096u);
        uint64_t c[6];
        if (db_readers_locate(1, hex, c) < 0) r->errors++;
        r->lookups++;
        atomic_fetch_add(&g_lookups, 1);
    }
    return NULL;
}

#define N_READERS 16

/*
 * FS2 (B201): with lookups running on every pooled connection at once, the
 * WAL reached 1.4 million frames against a 262,144-frame cap, and kept
 * growing after the backup. Readers in WAL mode hold a read-mark slot for
 * each read; there are only a few slots, and readers that overlap without
 * a gap keep one held at an old mark for as long as they keep coming, which
 * stops both the copy beyond that mark and the restart. Here sixteen
 * threads look objects up without pause while a writer commits, and the
 * WAL has to stay near its cap, be fully copied and restart - all while
 * the lookups are still running.
 */
static void test_pooled_readers_never_pin_the_wal(void **state)
{
    (void)state;
    assert_int_equal(db_exec_raw(db,
        "CREATE TABLE packs (repo_id INTEGER, id INTEGER, version INTEGER,"
        " PRIMARY KEY (repo_id, id));"
        " CREATE TABLE pack_objects (repo_id INTEGER, hash BLOB, pack_id INTEGER,"
        " offset INTEGER, comp_size INTEGER, orig_size INTEGER, entry_index INTEGER,"
        " PRIMARY KEY (repo_id, hash));"
        " INSERT INTO packs VALUES (1, 1, 2);"), 0);
    for (int i = 0; i < 2048; i += 2) {
        char hex[65], sql[256];
        snprintf(hex, sizeof(hex), "%064x", i);
        snprintf(sql, sizeof(sql), "INSERT INTO pack_objects VALUES (1, '%s', 1, %d, 10, 10, %d)",
                 hex, i, i);
        assert_int_equal(db_exec_raw(db, sql), 0);
    }
    assert_int_equal(db_readers_open(TEST_DB, N_READERS), OK);
    /* Each read held for a millisecond, as on FS2's loaded disk: the reads
     * overlap without a gap, which is what pinned a read-mark slot there. */
    db_readers_set_test_hold_us(1000);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);

    atomic_store(&g_lookups, 0);
    reader_t rd[N_READERS];
    pthread_t th[N_READERS];
    memset(rd, 0, sizeof(rd));
    for (int i = 0; i < N_READERS; i++)
        assert_int_equal(pthread_create(&th[i], NULL, reader_main, &rd[i]), 0);

    writer_t w;
    memset(&w, 0, sizeof(w));
    uint32_t peak = run_writer_until(&w, 3000, 0, lookups_enough);

    /* The writer has stopped, the readers have not. A PASSIVE from outside
     * gets only as far as their read marks; the checkpointer's locked round,
     * which holds them out, copies everything and restarts the WAL. */
    uint32_t f0 = 0, b0 = 0, f = 0, b = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f0, &b0), 0);
    /* OK only when every frame was copied (log == checkpointed); then the
     * restart row it writes starts the WAL again from the top. */
    assert_int_equal(checkpointer_force(), OK);
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);
    uint32_t after = f;
    unsigned bad = db_readers_audit();

    unsigned long lookups = 0;
    for (int i = 0; i < N_READERS; i++) {
        atomic_store(&rd[i].stop, 1);
        pthread_join(th[i], NULL);
        lookups += rd[i].lookups;
        assert_int_equal(rd[i].errors, 0);
    }
    db_readers_set_test_hold_us(0);
    fprintf(stderr, "B201: %lu lookups, %lu commits, WAL peak %u frames (hard cap %u), "
            "then %u of %u frames before the forced round, %u after it\n",
            lookups, (unsigned long)atomic_load(&w.commits), peak, (unsigned)THRESHOLD * CKPT_HARD_FACTOR, f0 - b0, f0, after);
    assert_true(lookups > MIN_LOOKUPS);
    assert_true(peak <= (uint32_t)THRESHOLD * CKPT_HARD_FACTOR * 2);
    assert_true(after <= 8 && after < f0);
    assert_int_equal(bad, 0);
}

/* A request reading through the shared connection, over and over: the API
 * polled by a client, the scheduler's look at what is due. */
typedef struct { atomic_int stop; unsigned long reads; atomic_ullong max_ms; } shared_reader_t;

static void *shared_reader_main(void *arg)
{
    shared_reader_t *r = arg;
    while (!atomic_load(&r->stop)) {
        uint64_t t0 = now_ms();
        db_result_t *res = NULL;
        (void)db_execute(db, "SELECT COUNT(*), SUM(LENGTH(v)) FROM t", NULL, &res);
        db_result_free(res);
        uint64_t took = now_ms() - t0;
        if (took > atomic_load(&r->max_ms)) atomic_store(&r->max_ms, took);
        r->reads++;
    }
    return NULL;
}

/*
 * B201's idle half. After the backup on FS2 the WAL never restarted, and the
 * only writes left were the checkpointer's own: each tick past the hard cap
 * wrote its restart row and its warning line, about four frames a second.
 * What kept the restart from landing was reads on the shared connection,
 * which take a read-mark slot like any reader and do not take the write
 * lock: requests and the daemon's own looks at the index, back to back.
 * The restart is now a RESTART checkpoint, which waits for the readers
 * already inside the WAL while new ones read the database file, so with
 * reads running without pause and nothing being written, the locked round
 * copies the WAL and restarts it.
 */
static void test_reads_on_the_shared_connection_do_not_pin_the_wal(void **state)
{
    (void)state;
    for (int i = 0; i < 40; i++) assert_int_equal(commit_rows(50), 0);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    checkpointer_set_test_idle(1);          /* the tick out of the way */

    shared_reader_t sr[4];
    pthread_t th[4];
    memset(sr, 0, sizeof(sr));
    for (int i = 0; i < 4; i++)
        assert_int_equal(pthread_create(&th[i], NULL, shared_reader_main, &sr[i]), 0);
    sleep_ms(100);

    uint32_t f0 = 0, b0 = 0, f = 0, b = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f0, &b0), 0);
    assert_int_equal(checkpointer_force(), OK);
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);

    unsigned long reads = 0;
    for (int i = 0; i < 4; i++) {
        atomic_store(&sr[i].stop, 1);
        pthread_join(th[i], NULL);
        reads += sr[i].reads;
    }
    fprintf(stderr, "B201 idle: %lu shared reads; WAL %u frames before the forced round, "
            "%u after\n", reads, f0, f);
    assert_true(reads > 10);
    assert_true(f0 > 1000);
    assert_true(f <= 8);
}

/*
 * B201: and a request is not held for the round. The copy at the hard cap
 * is the whole WAL, seconds on FS2; holding the shared connection for it
 * would stall every request that long. The round holds only the write lock
 * and the pooled readers, so here, with each copy slowed to 800 ms, a read
 * on the shared connection waits for none of it, and the WAL still
 * restarts with the reads running. One reading thread: two would wait on
 * each other for the connection, which says nothing about the round.
 */
#define SLOW_COPY_MS 800

static void test_a_locked_round_does_not_hold_shared_reads(void **state)
{
    (void)state;
    for (int i = 0; i < 40; i++) assert_int_equal(commit_rows(50), 0);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    checkpointer_set_test_idle(1);

    shared_reader_t sr[1];
    pthread_t th[1];
    memset(sr, 0, sizeof(sr));
    for (int i = 0; i < 1; i++)
        assert_int_equal(pthread_create(&th[i], NULL, shared_reader_main, &sr[i]), 0);
    sleep_ms(100);
    for (int i = 0; i < 1; i++) atomic_store(&sr[i].max_ms, 0);

    checkpointer_set_test_pause_ms(SLOW_COPY_MS);
    uint64_t t0 = now_ms();
    assert_int_equal(checkpointer_force(), OK);
    uint64_t round_ms = now_ms() - t0;
    checkpointer_set_test_pause_ms(0);
    uint32_t f = 0, b = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);

    uint64_t max_ms = 0;
    unsigned long reads = 0;
    for (int i = 0; i < 1; i++) {
        atomic_store(&sr[i].stop, 1);
        pthread_join(th[i], NULL);
        if (atomic_load(&sr[i].max_ms) > max_ms) max_ms = atomic_load(&sr[i].max_ms);
        reads += sr[i].reads;
    }
    fprintf(stderr, "B201 round: held %llu ms; %lu shared reads, the longest %llu ms; "
            "WAL %u frames after\n", (unsigned long long)round_ms, reads,
            (unsigned long long)max_ms, f);
    assert_true(round_ms >= SLOW_COPY_MS);
    assert_true(max_ms < SLOW_COPY_MS / 2);
    assert_true(f <= 8);
}

/*
 * B201, the idle growth itself: with the WAL past the hard cap and a reader
 * that keeps it from restarting, nothing else writing, the checkpointer
 * used to write its restart row every tick, each one an ordinary frame
 * when the restart missed - with its warning line, about four frames a
 * second on FS2 for as long as the daemon ran. It now writes the row only
 * after a RESTART checkpoint has restarted the WAL, so an idle daemon's
 * WAL stays still; and once the reader lets go, it restarts.
 */
static void test_a_pinned_idle_wal_is_not_written(void **state)
{
    (void)state;
    while (wal_frames() <= (uint32_t)THRESHOLD * CKPT_HARD_FACTOR)
        assert_int_equal(commit_rows(50), 0);
    cutils_db_t *reader = NULL;
    assert_int_equal(db_open(&reader, TEST_DB), 0);
    assert_int_equal(db_exec_raw(reader, "BEGIN"), 0);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(reader, "SELECT COUNT(*) FROM t", NULL, &r), 0);
    db_result_free(r);

    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    sleep_ms(300);                          /* copied, and the first miss */
    uint32_t f0 = wal_frames();
    sleep_ms(2000);                         /* a hundred ticks, idle */
    uint32_t f1 = wal_frames();
    checkpointer_stats_t st;
    checkpointer_stats(&st);
    fprintf(stderr, "B201 idle pinned: %u -> %u frames over 2 s, %lu ticks, %lu restart misses\n",
            f0, f1, st.ticks, st.pre_restart_misses);
    assert_true(st.ticks > 20);
    assert_int_equal(f1, f0);

    assert_int_equal(db_exec_raw(reader, "COMMIT"), 0);
    db_close(reader);
    /* Let go, and the next locked round restarts the WAL with nothing else
     * written: the retry needs no write to set it off. */
    uint32_t f = UINT32_MAX;
    for (int i = 0; i < 300 && f > 8; i++) {
        sleep_ms(10);
        f = wal_frames();
    }
    checkpointer_stats(&st);
    uint32_t fb = 0, bb = 0;
    (void)checkpointer_wal_frames(TEST_DB, &fb, &bb);
    fprintf(stderr, "B201 released: %u frames (%u copied), %lu escalations, %lu restarts, "
            "%lu misses, %lu busy\n", fb, bb, st.escalations, st.pre_restarts,
            st.pre_restart_misses, st.busy);
    assert_true(f <= 8);
}

/*
 * B215. The daemon dropped to its run_as uid and then opened this
 * connection; state.db was root's, 0600, and SQLite quietly opened it
 * read-only, so every checkpoint was refused and the WAL grew five times
 * past its cap. A connection that can only read is refused at open, naming
 * the uid, and wal_health has it.
 *
 * Root opens anything read-write, so as root (CI) the open is made in a
 * child without CAP_DAC_OVERRIDE, which is what the dropped daemon lacks;
 * the child's verdict is its exit code.
 */
static int drop_dac_override(void)
{
    struct __user_cap_header_struct h = { _LINUX_CAPABILITY_VERSION_3, 0 };
    struct __user_cap_data_struct d[2];
    memset(d, 0, sizeof(d));
    if (syscall(SYS_capget, &h, d) != 0) return -1;
    d[0].effective &= ~(1u << CAP_DAC_OVERRIDE);
    return (int)syscall(SYS_capset, &h, d);
}

static int open_read_only_verdict(void)
{
    wal_health_reset();
    if (checkpointer_open(TEST_DB) == OK) { checkpointer_stop(); return 1; }
    if (!strstr(err_msg(), "only read-only")) return 2;
    char msg[512];
    if (!wal_health_failing(msg, sizeof(msg), NULL)) return 3;
    if (!strstr(msg, "checkpointer") || !strstr(msg, TEST_DB)) return 4;
    if (wal_health_failures() < 1) return 5;
    return 0;
}

static void test_a_read_only_connection_is_refused(void **state)
{
    (void)state;
    assert_int_equal(chmod(TEST_DB, 0444), 0);
    int verdict;
    if (geteuid() == 0) {
        pid_t pid = fork();
        assert_true(pid >= 0);
        if (pid == 0) {
            if (drop_dac_override() != 0) _exit(100);
            _exit(open_read_only_verdict());
        }
        int ws = 0;
        assert_int_equal(waitpid(pid, &ws, 0), pid);
        assert_true(WIFEXITED(ws));
        verdict = WEXITSTATUS(ws);
    } else {
        verdict = open_read_only_verdict();
    }
    assert_int_equal(chmod(TEST_DB, 0644), 0);
    assert_int_equal(verdict, 0);
    /* And a writable one opens, and is the one the thread then uses. */
    wal_health_reset();
    assert_int_equal(checkpointer_open(TEST_DB), OK);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_false(wal_health_failing(NULL, 0, NULL));
}

/*
 * A -shm the thread cannot read meant it never saw the WAL grow and so
 * never checkpointed, without a word. Now it is a failure the moment it
 * happens, and a recovery when it ends. A symlink loop stands in for a
 * file the daemon's uid may not open, because it fails for root too. The
 * real -shm is kept by a second link and put back, so SQLite's own
 * descriptors and locks are never disturbed.
 */
static void test_an_unreadable_shm_is_reported_and_recovers(void **state)
{
    (void)state;
    wal_health_reset();
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_int_equal(commit_rows(4), 0);
    (void)wal_frames();

    assert_int_equal(link(TEST_SHM, TEST_SHM ".keep"), 0);
    assert_int_equal(symlink("state.db-shm", TEST_SHM ".loop"), 0);
    assert_int_equal(rename(TEST_SHM ".loop", TEST_SHM), 0);
    char msg[512] = "";
    int failing = 0;
    for (int i = 0; i < 200 && !failing; i++) {
        sleep_ms(10);
        failing = wal_health_failing(msg, sizeof(msg), NULL);
    }
    assert_true(failing);
    assert_non_null(strstr(msg, "wal-index reader"));
    assert_non_null(strstr(msg, TEST_SHM));
    assert_non_null(strstr(msg, "errno"));
    unsigned long n = wal_health_failures();
    assert_true(n >= 1);
    checkpointer_stats_t st;
    checkpointer_stats(&st);
    assert_true(st.failed >= 1);

    assert_int_equal(rename(TEST_SHM ".keep", TEST_SHM), 0);
    for (int i = 0; i < 200 && failing; i++) {
        sleep_ms(10);
        failing = wal_health_failing(NULL, 0, NULL);
    }
    assert_false(failing);
    assert_true(wal_health_failures() >= n);
}

/*
 * B221: a checkpoint SQLite refuses for any reason but contention reaches
 * wal_health and the failure count, and is not counted as busy. Before
 * B215 every refusal was "busy" and retried each tick without a word; B215
 * fixed it for SQLITE_READONLY, and this holds it for the rest: here a
 * full disk under the database (SQLITE_FULL) as the copy writes its pages.
 */
static void test_a_refused_checkpoint_is_a_failure_not_busy(void **state)
{
    (void)state;
    wal_health_reset();
    checkpointer_set_test_idle(1);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_int_equal(commit_rows(400), 0);
    assert_true(wal_frames() >= THRESHOLD);
    checkpointer_stats_t st0;
    checkpointer_stats(&st0);

    full_vfs_arm(1);
    checkpointer_set_test_idle(0);
    char msg[512] = "";
    int failing = 0;
    for (int i = 0; i < 300 && !failing; i++) {
        sleep_ms(10);
        failing = wal_health_failing(msg, sizeof(msg), NULL);
    }
    checkpointer_set_test_idle(1);
    full_vfs_arm(0);
    assert_true(failing);
    assert_non_null(strstr(msg, "PASSIVE checkpoint"));
    assert_non_null(strstr(msg, "SQLite code 13"));
    assert_non_null(strstr(msg, TEST_DB));
    checkpointer_stats_t st;
    checkpointer_stats(&st);
    assert_true(st.failed > st0.failed);
    assert_int_equal(st.busy, st0.busy);
    assert_true(wal_health_failures() >= 1);

    /* The disk has room again: the next checkpoint works and says so. */
    checkpointer_set_test_idle(0);
    for (int i = 0; i < 300 && failing; i++) {
        sleep_ms(10);
        failing = wal_health_failing(NULL, 0, NULL);
    }
    assert_false(failing);
}

/*
 * B219: a locked round's wait for the write lock is not its hold. The
 * round queues behind a writer holding the lock for 300 ms and then holds
 * it only for a short copy; the hold used to be timed from before the
 * queue, so it read 300 ms.
 */
static void *force_round(void *arg)
{
    *(status_t *)arg = checkpointer_force();
    return NULL;
}

static void test_the_lock_wait_is_not_counted_as_the_hold(void **state)
{
    (void)state;
    checkpointer_set_test_idle(1);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_int_equal(commit_rows(8), 0);
    assert_int_equal(db_txn_begin(db), OK);
    status_t fst = ERR_IO;
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, force_round, &fst), 0);
    /* The 300 ms are counted from the round queueing, not from starting
     * its thread: on a contended host the thread had not reached the lock
     * by the end of a plain 300 ms sleep, and the wait read short. */
    for (int i = 0; i < 6000 && db_txn_waiters() < 1; i++) sleep_ms(1);
    assert_true(db_txn_waiters() >= 1);
    sleep_ms(300);
    assert_int_equal(db_txn_commit(db), OK);
    pthread_join(th, NULL);
    assert_int_equal(fst, OK);
    checkpointer_stats_t st;
    checkpointer_stats(&st);
    fprintf(stderr, "B219: waited %.0f ms, held %.0f ms\n", (double)st.lock_wait_ns / 1e6,
            (double)st.lock_ns / 1e6);
    assert_true(st.lock_wait_ns >= 250ull * 1000000ull);
    assert_true(st.max_lock_wait_ns >= 250ull * 1000000ull);
    assert_true(st.lock_ns < st.lock_wait_ns);
    assert_true(st.max_lock_ns < 250ull * 1000000ull);
}

static int file_exists(const char *p)
{
    struct stat sb;
    return stat(p, &sb) == 0;
}

static long rows_in_t(void)
{
    db_result_t *r = NULL;
    long n = -1;
    if (db_execute(db, "SELECT COUNT(*) FROM t", NULL, &r) == 0 && r && r->nrows == 1)
        n = strtol(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

/*
 * B218: a stop does not wait for a copy. The disk is slowed so the copy of
 * about 2,000 pages takes ten seconds; the halt interrupts it within a
 * fraction of that, counts nothing as failed, and the bounded final copy
 * gives up at its budget. With the shared handle closed first, the
 * checkpointer's close is the last and leaves the WAL, synced; reopened,
 * every row is there.
 */
static void test_shutdown_does_not_wait_for_a_slow_copy(void **state)
{
    (void)state;
    wal_health_reset();
    checkpointer_set_test_idle(1);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_int_equal(commit_rows(500), 0);
    long rows = rows_in_t();
    assert_true(rows > 0);
    uint32_t f0 = wal_frames();
    assert_true(f0 >= THRESHOLD && f0 <= CKPT_FINAL_FRAMES);

    atomic_store(&full_vfs_slow_writes, 0);
    atomic_store(&full_vfs_delay_us, 5000);
    checkpointer_set_test_idle(0);
    for (int i = 0; i < 500 && atomic_load(&full_vfs_slow_writes) < 5; i++) sleep_ms(10);
    assert_true(atomic_load(&full_vfs_slow_writes) >= 5);   /* a copy is under way */

    uint64_t t0 = now_ms();
    checkpointer_halt();
    uint64_t halt_ms = now_ms() - t0;
    fprintf(stderr, "B218: halt in %llu ms\n", (unsigned long long)halt_ms);
    assert_true(halt_ms < 2000);
    assert_false(wal_health_failing(NULL, 0, NULL));
    checkpointer_stats_t st;
    checkpointer_stats(&st);
    assert_int_equal(st.failed, 0);

    t0 = now_ms();
    assert_int_equal(checkpointer_finish(200), 0);
    uint64_t fin_ms = now_ms() - t0;
    fprintf(stderr, "B218: final copy gave up in %llu ms\n", (unsigned long long)fin_ms);
    assert_true(fin_ms < 2000);
    atomic_store(&full_vfs_delay_us, 0);

    db_readers_close();
    db_close(db);
    db = NULL;
    checkpointer_stop();
    assert_true(file_exists(TEST_WAL));

    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(rows_in_t(), rows);
}

/* And a WAL small enough is copied within the budget, so the last close
 * removes it, as a stop always did. */
static void test_shutdown_copies_a_small_wal_and_removes_it(void **state)
{
    (void)state;
    checkpointer_set_test_idle(1);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    assert_int_equal(commit_rows(100), 0);
    long rows = rows_in_t();
    assert_true(wal_frames() > 0);
    assert_int_equal(checkpointer_finish(5000), 1);
    uint32_t f = 0, b = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);
    assert_int_equal(f, b);
    db_readers_close();
    db_close(db);
    db = NULL;
    checkpointer_stop();
    assert_false(file_exists(TEST_WAL));
    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(rows_in_t(), rows);
}

/* A WAL past CKPT_FINAL_FRAMES is not copied at all at shutdown, whatever
 * the budget: the stop would wait for it on a slow disk. */
static void test_shutdown_leaves_a_large_wal(void **state)
{
    (void)state;
    checkpointer_set_test_idle(1);
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    for (int i = 0; i < 12 && wal_frames() <= CKPT_FINAL_FRAMES; i++)
        assert_int_equal(commit_rows(100), 0);
    assert_true(wal_frames() > CKPT_FINAL_FRAMES);
    long rows = rows_in_t();
    uint32_t f0 = 0, b0 = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f0, &b0), 0);
    assert_int_equal(checkpointer_finish(60000), 0);
    uint32_t f = 0, b = 0;
    assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);
    assert_int_equal(b, b0);                  /* nothing copied */
    db_readers_close();
    db_close(db);
    db = NULL;
    checkpointer_stop();
    assert_true(file_exists(TEST_WAL));
    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(rows_in_t(), rows);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_no_commit_checkpoints, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wal_stays_near_the_cap_under_a_long_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wal_is_bounded_under_a_writer_faster_than_checkpoints, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_hard_cap_holds_on_a_slow_device, setup, teardown),
        cmocka_unit_test_setup_teardown(test_only_bulk_writers_wait_at_the_hard_cap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pinned_reader_holds_nobody_up, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pooled_readers_never_pin_the_wal, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reads_on_the_shared_connection_do_not_pin_the_wal,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_locked_round_does_not_hold_shared_reads,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_pinned_idle_wal_is_not_written, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_read_only_connection_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_an_unreadable_shm_is_reported_and_recovers,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_refused_checkpoint_is_a_failure_not_busy,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_lock_wait_is_not_counted_as_the_hold,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_shutdown_does_not_wait_for_a_slow_copy,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_shutdown_copies_a_small_wal_and_removes_it,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_shutdown_leaves_a_large_wal, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
