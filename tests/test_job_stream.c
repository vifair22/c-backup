#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

/*
 * /jobs/stream/<id> follows a job for as long as the job runs (B228).
 *
 * The stream used to end after an hour with an "event: timeout" frame, so
 * `jobs watch` on a long backup stopped following a job that was still
 * running. These drive the stream's reader directly, with its clock moved
 * forward, which is the only way to test an hour in a unit test.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "db_write.h"
#include "routes.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_job_stream"
#define TEST_DB  TEST_DIR "/state.db"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static time_t g_fake;

static time_t fake_now(void) { return g_fake; }

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (db_write(db, "INSERT INTO jobs (id, action, state, started, progress_phase) "
                     "VALUES (7, 'backup', 'running', strftime('%s','now'), 'store')",
                 NULL, NULL) != 0) return -1;
    g_fake = time(NULL);
    job_stream_set_test_clock(fake_now);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    job_stream_set_test_clock(NULL);
    db_close(db);
    rmrf(TEST_DIR);
    return 0;
}

static void test_follows_a_running_job_past_an_hour(void **state)
{
    (void)state;
    void *st = job_stream_open_for_test(db, 7);
    assert_non_null(st);
    char buf[4096];

    ssize_t n = job_stream_read_for_test(st, buf, sizeof(buf) - 1);
    assert_true(n > 0);
    buf[n] = '\0';
    assert_non_null(strstr(buf, "data: "));
    assert_non_null(strstr(buf, "\"state\":\"running\""));

    /* Two hours on, the job still running: a heartbeat frame, not the end
     * of the stream. */
    g_fake += 2 * 3600;
    n = job_stream_read_for_test(st, buf, sizeof(buf) - 1);
    assert_true(n > 0);
    buf[n] = '\0';
    assert_null(strstr(buf, "event: timeout"));
    assert_non_null(strstr(buf, "\"state\":\"running\""));

    /* And it still ends with the job. */
    assert_int_equal(db_write(db, "UPDATE jobs SET state = 'completed', "
                                  "ended = strftime('%s','now') WHERE id = 7",
                              NULL, NULL), 0);
    n = job_stream_read_for_test(st, buf, sizeof(buf) - 1);
    assert_true(n > 0);
    buf[n] = '\0';
    assert_non_null(strstr(buf, "\"state\":\"completed\""));
    assert_int_equal(job_stream_read_for_test(st, buf, sizeof(buf) - 1), -1);
    job_stream_close_for_test(st);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_follows_a_running_job_past_an_hour,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
