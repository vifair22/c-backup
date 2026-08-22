#define _POSIX_C_SOURCE 200809L

/*
 * Scheduler loop and overlap policy.
 *
 * These drive scheduler_tick() with an explicit `now` rather than waiting for
 * wall-clock time, which is the only way to test a scheduler in a test suite.
 * Dispatch is injected, so what is under test is the decision to fire, not
 * what firing does.
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

#include <cmocka.h>
#include <cutils/db.h>

#include "scheduler.h"

#define TEST_DB "/tmp/c_backup_test_sched.db"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static scheduler_t *sched;

/* What the injected dispatcher saw. */
static int   g_fires;
static char  g_last_action[64];

static status_t record_dispatch(void *user, int64_t schedule_id, int64_t repo_id,
                                const char *action, const char *params)
{
    (void)user; (void)schedule_id; (void)repo_id; (void)params;
    g_fires++;
    snprintf(g_last_action, sizeof(g_last_action), "%s", action ? action : "");
    return OK;
}

static int setup(void **state)
{
    (void)state;
    setenv("TZ", "UTC", 1);
    tzset();
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (db_execute_non_query(db,
            "INSERT INTO repos (id, uuid, path, added_at) VALUES (1,'u','/tmp/r',0)",
            NULL, NULL) != 0) return -1;
    g_fires = 0;
    g_last_action[0] = '\0';
    /*
     * Opened without its loop thread. Dispatch is injected and ticks are
     * driven explicitly, so what is under test is the decision to fire; a
     * concurrent background loop would make the counts depend on timing.
     */
    if (scheduler_open(db, record_dispatch, NULL, &sched) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    scheduler_stop(sched); sched = NULL;
    db_close(db);          db = NULL;
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    return 0;
}

static int64_t add_schedule(const char *action, const char *spec,
                            time_t due, const char *overlap, const char *catchup)
{
    char d[32];
    snprintf(d, sizeof(d), "%lld", (long long)due);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO schedules (repo_id, action, trigger_type, trigger_spec,"
        " on_overlap, catchup, enabled, next_due_at)"
        " VALUES (1, ?, 'interval', ?, ?, ?, 1, ?)",
        (const char *[]){ action, spec, overlap ? overlap : "skip",
                          catchup ? catchup : "skip", d, NULL }, NULL), 0);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT last_insert_rowid()", NULL, &r), 0);
    int64_t id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return id;
}

static void add_job(const char *action, const char *state)
{
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO jobs (repo_id, action, state) VALUES (1, ?, ?)",
        (const char *[]){ action, state, NULL }, NULL), 0);
}

static time_t due_of(int64_t sid)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)sid);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT next_due_at FROM schedules WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0;
    }
    time_t v = (time_t)strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int enabled_of(int64_t sid)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)sid);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT enabled FROM schedules WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

#define SPEC_HOURLY "{\"type\":\"interval\",\"seconds\":3600}"

/* ------------------------------------------------------------------ */

static void test_due_schedule_fires(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, NULL, NULL);

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
    assert_string_equal(g_last_action, "backup");
}

static void test_future_schedule_does_not_fire(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now + 3600, NULL, NULL);

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 0);
}

static void test_disabled_schedule_does_not_fire(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t sid = add_schedule("backup", SPEC_HOURLY, now - 10, NULL, NULL);
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)sid);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE schedules SET enabled = 0 WHERE id = ?",
        (const char *[]){ id, NULL }, NULL), 0);

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 0);
}

/*
 * After firing, next_due_at must move into the future. A schedule that kept
 * its old due time would fire on every subsequent tick, which is a runaway
 * rather than a schedule.
 */
static void test_firing_advances_due_time(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t sid = add_schedule("backup", SPEC_HOURLY, now - 10, NULL, NULL);

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_true(due_of(sid) > now);

    /* A second tick at the same instant must not fire again. */
    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
}

/* ---------- overlap ---------- */

/* skip: a run already covers this window, so another would duplicate work. */
static void test_overlap_skip(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t sid = add_schedule("backup", SPEC_HOURLY, now - 10, "skip", NULL);
    add_job("backup", "running");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 0);
    /* Skipped, but still rescheduled: otherwise it retries every tick. */
    assert_true(due_of(sid) > now);
}

/* queue: the second run is meaningful once the first finishes. */
static void test_overlap_queue(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("gc", SPEC_HOURLY, now - 10, "queue", NULL);
    add_job("gc", "running");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
}

static void test_overlap_cancel_running(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, "cancel_running", NULL);
    add_job("backup", "running");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'", NULL, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 1);
    db_result_free(r);
}

/* A finished job is not an overlap. */
static void test_completed_job_is_not_an_overlap(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, "skip", NULL);
    add_job("backup", "completed");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
}

/* A different action on the same repo is not an overlap either. */
static void test_overlap_is_per_action(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, "skip", NULL);
    add_job("gc", "running");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
}

/* ---------- catchup ---------- */

/*
 * skip: after an outage, move to the next future fire. Nobody wants a
 * stampede of catch-up backups when a laptop wakes from a three-day sleep.
 */
static void test_catchup_skip_moves_forward(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t sid = add_schedule("backup", SPEC_HOURLY, now - 100000, "skip", "skip");

    char id[32], last[32];
    snprintf(id,   sizeof(id),   "%lld", (long long)sid);
    snprintf(last, sizeof(last), "%lld", (long long)(now - 100000));
    assert_int_equal(db_execute_non_query(db,
        "UPDATE schedules SET last_fired_at = ? WHERE id = ?",
        (const char *[]){ last, id, NULL }, NULL), 0);

    assert_int_equal(scheduler_recompute(db, sid, now), OK);
    time_t due = due_of(sid);
    assert_true(due > now);
    assert_true(due <= now + 3600);
}

/* run_once: fire once for a missed window, then resume. Right for hygiene
 * work like gc that should happen if it was skipped. */
static void test_catchup_run_once_fires_now(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t sid = add_schedule("gc", SPEC_HOURLY, now - 100000, "skip", "run_once");

    char id[32], last[32];
    snprintf(id,   sizeof(id),   "%lld", (long long)sid);
    snprintf(last, sizeof(last), "%lld", (long long)(now - 100000));
    assert_int_equal(db_execute_non_query(db,
        "UPDATE schedules SET last_fired_at = ? WHERE id = ?",
        (const char *[]){ last, id, NULL }, NULL), 0);

    assert_int_equal(scheduler_recompute(db, sid, now), OK);
    assert_int_equal(due_of(sid), now);
}

/* An unparseable trigger disables its schedule rather than failing the tick
 * forever: one bad row must not stop every other schedule on the machine. */
static void test_bad_trigger_disables_schedule(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t sid = add_schedule("backup", "{\"type\":\"nonsense\"}", now - 10, NULL, NULL);

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(enabled_of(sid), 0);
}

static void test_one_bad_schedule_does_not_block_others(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", "{\"type\":\"nonsense\"}", now - 10, NULL, NULL);
    add_schedule("gc", SPEC_HOURLY, now - 10, NULL, NULL);

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
    assert_string_equal(g_last_action, "gc");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_due_schedule_fires, setup, teardown),
        cmocka_unit_test_setup_teardown(test_future_schedule_does_not_fire, setup, teardown),
        cmocka_unit_test_setup_teardown(test_disabled_schedule_does_not_fire, setup, teardown),
        cmocka_unit_test_setup_teardown(test_firing_advances_due_time, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_skip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_queue, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_cancel_running, setup, teardown),
        cmocka_unit_test_setup_teardown(test_completed_job_is_not_an_overlap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_is_per_action, setup, teardown),
        cmocka_unit_test_setup_teardown(test_catchup_skip_moves_forward, setup, teardown),
        cmocka_unit_test_setup_teardown(test_catchup_run_once_fires_now, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bad_trigger_disables_schedule, setup, teardown),
        cmocka_unit_test_setup_teardown(test_one_bad_schedule_does_not_block_others, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
