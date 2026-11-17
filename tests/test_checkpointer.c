#define _POSIX_C_SOURCE 200809L

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

#include <cmocka.h>
#include <cutils/db.h>

#include "checkpointer.h"
#include "db_txn.h"
#include "state_db.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_checkpointer"
#define TEST_DB   TEST_DIR "/state.db"
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
    if (db_open(&db, TEST_DB) != 0) return -1;
    state_db_tuning_t t = { "normal", 8, THRESHOLD };
    if (state_db_tune(db, &t) != OK) return -1;
    memset(g_blob, 'x', ROW_BYTES);
    g_blob[ROW_BYTES] = '\0';
    /* Each row its own pages, so a commit always adds frames. */
    return db_exec_raw(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    checkpointer_stop();
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* One commit through db_txn, as every writer in the daemon makes them.
 * Rows are rewritten over a fixed set of ids, so the WAL sees every page
 * while the database file stays a few MiB. */
static atomic_uint g_row;

static int commit_rows(int n)
{
    if (db_txn_begin(db) != OK) return -1;
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
    unsigned   commits;
    uint64_t   max_ms;
    int        failed;
} writer_t;

/* A backup's batches: commit after commit, no pause between. */
static void *writer_main(void *arg)
{
    writer_t *w = arg;
    while (!atomic_load(&w->stop)) {
        uint64_t t0 = now_ms();
        if (commit_rows(4) != 0) { w->failed = 1; break; }
        uint64_t took = now_ms() - t0;
        if (took > w->max_ms) w->max_ms = took;
        w->commits++;
        if (w->pause_us) {
            struct timespec ts = { 0, w->pause_us * 1000L };
            nanosleep(&ts, NULL);
        }
    }
    return NULL;
}

static uint32_t run_writer_for(writer_t *w, uint64_t ms)
{
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, writer_main, w), 0);
    uint32_t peak = 0;
    uint64_t end = now_ms() + ms;
    while (now_ms() < end) {
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
                  what, w->commits, (double)w->commits * 4 * ROW_BYTES / 1048576.0,
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
    uint32_t peak = run_writer_for(&w, 6000);
    report("writer", &w, peak);
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    /* Enough written that an unbounded WAL would be several caps over. */
    assert_true((uint64_t)w.commits * 4 * 4 > (uint64_t)THRESHOLD * CKPT_CAP_FACTOR * 3);
    assert_true(peak < THRESHOLD * 3);
    assert_true(s.passive > 0);
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
    uint32_t peak = run_writer_for(&w, 4000);
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
    uint32_t peak = run_writer_for(&w, 2000);
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_no_commit_checkpoints, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wal_stays_near_the_cap_under_a_long_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wal_is_bounded_under_a_writer_faster_than_checkpoints, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_hard_cap_holds_on_a_slow_device, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pinned_reader_holds_nobody_up, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
