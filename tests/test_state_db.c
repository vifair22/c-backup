#define _POSIX_C_SOURCE 200809L

/*
 * state.db's connection settings are applied, read back and refused when
 * they cannot be honoured, and the WAL flusher syncs what changed.
 *
 * A setting that is accepted and not honoured is this codebase's commonest
 * bug, so every case asks SQLite what it is actually running on rather than
 * trusting that the PRAGMA was sent.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "state_db.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_state_db"
#define TEST_DB  TEST_DIR "/state.db"

static cutils_db_t *db;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static long long pragma(const char *name)
{
    char sql[64];
    snprintf(sql, sizeof(sql), "PRAGMA %s", name);
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, NULL, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    return db_open(&db, TEST_DB) == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void test_defaults_are_applied(void **state)
{
    (void)state;
    state_db_tuning_t t = { STATE_DB_SYNC_DEFAULT, STATE_DB_CACHE_MB_DEFAULT,
                            STATE_DB_AUTOCKPT_DEFAULT };
    assert_int_equal(state_db_tune(db, &t), OK);
    assert_int_equal(pragma("synchronous"), 1);                 /* NORMAL */
    assert_int_equal(pragma("cache_size"), -64 * 1024);
    assert_int_equal(pragma("wal_autocheckpoint"), 16384);
}

static void test_full_and_other_values(void **state)
{
    (void)state;
    state_db_tuning_t t = { "FULL", 8, 2000 };
    assert_int_equal(state_db_tune(db, &t), OK);
    assert_int_equal(pragma("synchronous"), 2);
    assert_int_equal(pragma("cache_size"), -8 * 1024);
    assert_int_equal(pragma("wal_autocheckpoint"), 2000);
}

static void test_bad_values_are_refused(void **state)
{
    (void)state;
    state_db_tuning_t t = { "off", 64, 16384 };
    assert_int_equal(state_db_tune(db, &t), ERR_INVALID);
    t.synchronous = "normal";
    t.cache_mb = 1;
    assert_int_equal(state_db_tune(db, &t), ERR_INVALID);
    t.cache_mb = 64;
    t.wal_autocheckpoint = 10;
    assert_int_equal(state_db_tune(db, &t), ERR_INVALID);
    /* Nothing was applied by a refused call. */
    assert_int_equal(pragma("synchronous"), 2);
}

/* NORMAL outside WAL mode can corrupt a database on a power cut, so a
 * handle that is not in WAL mode is refused rather than relaxed. */
static void test_not_wal_is_refused(void **state)
{
    (void)state;
    assert_int_equal(db_exec_raw(db, "PRAGMA journal_mode=DELETE"), 0);
    state_db_tuning_t t = { "normal", 64, 16384 };
    assert_int_equal(state_db_tune(db, &t), ERR_INVALID);
    assert_int_equal(pragma("synchronous"), 2);
}

static void sleep_ms(long ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

/* A write reaches the WAL and is synced within an interval; an idle WAL is
 * not synced again; stop syncs once more and is idempotent. */
static void test_flusher_syncs_changes_only(void **state)
{
    (void)state;
    state_db_tuning_t t = { "normal", 8, 16384 };
    assert_int_equal(state_db_tune(db, &t), OK);
    assert_int_equal(db_exec_raw(db, "CREATE TABLE t (v INTEGER)"), 0);

    assert_int_equal(state_db_flusher_start(TEST_DB, 20), OK);
    assert_int_equal(state_db_flusher_start(TEST_DB, 20), ERR_BUSY);
    assert_int_equal(db_exec_raw(db, "INSERT INTO t VALUES (1)"), 0);
    unsigned long n = 0;
    for (int i = 0; i < 200 && (n = state_db_flusher_syncs()) == 0; i++) sleep_ms(10);
    assert_true(n >= 1);
    /* That first sync may have caught the WAL before the insert landed (the
     * flusher's first tick can beat a slow insert), leaving one more sync
     * due, which then fell inside the idle window below (FLAKE-17). Wait,
     * bounded, until the WAL on disk is the one last synced. */
    for (int i = 0; i < 500 && !state_db_flusher_current(); i++) sleep_ms(10);
    assert_true(state_db_flusher_current());

    /* Idle: the WAL has not changed, so no further sync. */
    sleep_ms(120);
    unsigned long idle = state_db_flusher_syncs();
    sleep_ms(120);
    assert_int_equal(state_db_flusher_syncs(), idle);

    /* A new write is picked up. */
    assert_int_equal(db_exec_raw(db, "INSERT INTO t VALUES (2)"), 0);
    for (int i = 0; i < 200 && state_db_flusher_syncs() == idle; i++) sleep_ms(10);
    assert_true(state_db_flusher_syncs() > idle);

    state_db_flusher_stop();
    state_db_flusher_stop();
    /* And can be started again after a stop. */
    assert_int_equal(state_db_flusher_start(TEST_DB, 20), OK);
    state_db_flusher_stop();
}

static void test_flusher_bad_arguments(void **state)
{
    (void)state;
    assert_int_equal(state_db_flusher_start(NULL, 1000), ERR_INVALID);
    assert_int_equal(state_db_flusher_start(TEST_DB, 0), ERR_INVALID);
    state_db_flusher_stop();   /* not running: a no-op */
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_defaults_are_applied, setup, teardown),
        cmocka_unit_test_setup_teardown(test_full_and_other_values, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bad_values_are_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_not_wal_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_flusher_syncs_changes_only, setup, teardown),
        cmocka_unit_test_setup_teardown(test_flusher_bad_arguments, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
