#define _DEFAULT_SOURCE
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
#include <stdatomic.h>
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

#include "routes.h"
#include "job_manager.h"
#include "scheduler.h"

#define TEST_DB "/tmp/c_backup_test_sched.db"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static scheduler_t *sched;

/* What the injected dispatcher saw. */
static int   g_fires;
static char  g_last_action[64];

/*
 * When set, a fire also leaves the queued job row a real dispatch would, so
 * the overlap policies see their own earlier fires waiting.
 */
static int   g_dispatch_queues;

static status_t record_dispatch(void *user, int64_t schedule_id, int64_t repo_id,
                                const char *action, const char *params)
{
    (void)user; (void)params;
    g_fires++;
    snprintf(g_last_action, sizeof(g_last_action), "%s", action ? action : "");
    if (g_dispatch_queues) {
        char sid[32], rid[32];
        snprintf(sid, sizeof(sid), "%lld", (long long)schedule_id);
        snprintf(rid, sizeof(rid), "%lld", (long long)repo_id);
        if (db_execute_non_query(db,
                "INSERT INTO jobs (repo_id, action, state, origin, schedule_id)"
                " VALUES (?, ?, 'queued', 'schedule', ?)",
                (const char *[]){ rid, action, sid, NULL }, NULL) != 0)
            return set_error(ERR_IO, "test dispatch: cannot insert job");
    }
    return OK;
}

/* What the injected canceller was asked to stop. */
static int     g_cancels;
static int64_t g_cancelled[8];

static status_t record_cancel(void *user, int64_t job_id)
{
    (void)user;
    if (g_cancels < 8) g_cancelled[g_cancels] = job_id;
    g_cancels++;
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
    g_dispatch_queues = 0;
    g_cancels = 0;
    memset(g_cancelled, 0, sizeof(g_cancelled));
    /*
     * Opened without its loop thread. Dispatch is injected and ticks are
     * driven explicitly, so what is under test is the decision to fire; a
     * concurrent background loop would make the counts depend on timing.
     */
    if (scheduler_open(db, record_dispatch, NULL, &sched) != OK) return -1;
    scheduler_set_canceller(sched, record_cancel);
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

static int64_t add_job(const char *action, const char *state)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "INSERT INTO jobs (repo_id, action, state) VALUES (1, ?, ?) RETURNING id",
        (const char *[]){ action, state, NULL }, &r), 0);
    assert_non_null(r);
    int64_t id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return id;
}

static int count_of(const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(db, sql, NULL, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    int v = atoi(r->rows[0][0]);
    db_result_free(r);
    return v;
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

static void reason_of(int64_t sid, char *out, size_t outsz)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)sid);
    db_result_t *r = NULL;
    out[0] = '\0';
    if (db_execute(db, "SELECT disabled_reason FROM schedules WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1)
        snprintf(out, outsz, "%s", r->rows[0][0] ? r->rows[0][0] : "");
    db_result_free(r);
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

/*
 * B109: queue added one run per missed fire, so a gc running across three
 * intervals left three more lined up behind it. Fires while a run of the
 * schedule is already waiting coalesce into that run.
 */
static void test_overlap_queue_coalesces_missed_fires(void **state)
{
    (void)state;
    g_dispatch_queues = 1;
    time_t now = 1720000000;
    add_schedule("gc", SPEC_HOURLY, now - 10, "queue", NULL);
    add_job("gc", "running");

    for (int k = 0; k < 5; k++)
        assert_int_equal(scheduler_tick(sched, now + k * 3600), OK);
    assert_int_equal(g_fires, 1);
    assert_int_equal(count_of("SELECT COUNT(*) FROM jobs WHERE state = 'queued'"), 1);
}

/* A queued run from somewhere else, a manual gc, is not this schedule's
 * pending run: queue still adds one of its own, exactly once. */
static void test_overlap_queue_counts_only_its_own_runs(void **state)
{
    (void)state;
    g_dispatch_queues = 1;
    time_t now = 1720000000;
    add_schedule("gc", SPEC_HOURLY, now - 10, "queue", NULL);
    add_job("gc", "running");
    add_job("gc", "queued");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(scheduler_tick(sched, now + 3600), OK);
    assert_int_equal(g_fires, 1);
}

/*
 * cancel_running asks the job manager to stop the running job, the request
 * `jobs cancel` makes, and submits one replacement. It does not write the
 * job's state itself: that was all it did (B108), and the job never saw it.
 */
static void test_overlap_cancel_running(void **state)
{
    (void)state;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, "cancel_running", NULL);
    int64_t running = add_job("backup", "running");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
    assert_int_equal(g_cancels, 1);
    assert_int_equal(g_cancelled[0], running);
    /* Recorded cancelled by the worker once the job stops, not here. */
    assert_int_equal(count_of("SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'"), 0);
}

/*
 * The second half of B108: the old UPDATE matched queued jobs too, so every
 * fire after the first cancelled the previous fire's replacement before it
 * started and the schedule never completed a run. A fire that finds its
 * replacement still waiting leaves it alone and adds nothing.
 */
static void test_overlap_cancel_running_spares_the_replacement(void **state)
{
    (void)state;
    g_dispatch_queues = 1;
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, "cancel_running", NULL);
    int64_t running = add_job("backup", "running");

    for (int k = 0; k < 4; k++)
        assert_int_equal(scheduler_tick(sched, now + k * 3600), OK);

    assert_int_equal(g_fires, 1);
    /* Every fire repeats the request; the job manager treats a repeat as
     * the same flag. Only the running job is ever named. */
    assert_true(g_cancels >= 1);
    for (int k = 0; k < g_cancels && k < 8; k++)
        assert_int_equal(g_cancelled[k], running);
    assert_int_equal(count_of("SELECT COUNT(*) FROM jobs WHERE state = 'queued'"), 1);
    assert_int_equal(count_of("SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'"), 0);
}

/* With nothing to stop the job, cancel_running queues rather than
 * recording a cancellation that never happened. */
static void test_overlap_cancel_running_without_canceller(void **state)
{
    (void)state;
    scheduler_set_canceller(sched, NULL);
    time_t now = 1720000000;
    add_schedule("backup", SPEC_HOURLY, now - 10, "cancel_running", NULL);
    add_job("backup", "running");

    assert_int_equal(scheduler_tick(sched, now), OK);
    assert_int_equal(g_fires, 1);
    assert_int_equal(g_cancels, 0);
    assert_int_equal(count_of("SELECT COUNT(*) FROM jobs WHERE state = 'running'"), 1);
}

/* ---------- overlap against a real job manager ---------- */

/*
 * A job that runs until it is released or cancelled, polling the way a
 * pipeline does. While `g_deaf` is set it does not look at its cancel flag,
 * standing in for a job busy between two checks.
 */
static atomic_int g_started;
static atomic_int g_release;
static atomic_int g_deaf;

static status_t slow_job(void *arg, volatile int *cancel,
                         void (*progress)(const char *, uint64_t, uint64_t, void *),
                         void *progress_ctx)
{
    (void)arg; (void)progress; (void)progress_ctx;
    atomic_fetch_add(&g_started, 1);
    /* Bounded so a broken test fails instead of hanging: 20 s. */
    for (int i = 0; i < 4000; i++) {
        if (!atomic_load(&g_deaf) && *cancel)
            return set_error(ERR_IO, "cancelled");
        if (atomic_load(&g_release)) return OK;
        usleep(5000);
    }
    return set_error(ERR_IO, "slow_job: never released");
}

static status_t jm_dispatch(void *user, int64_t schedule_id, int64_t repo_id,
                            const char *action, const char *params)
{
    return job_submit_scheduled(user, repo_id, action, params, schedule_id,
                                slow_job, NULL, NULL, NULL);
}

static status_t jm_cancel(void *user, int64_t job_id)
{
    return job_cancel(user, job_id);
}

static void job_state(int64_t job_id, char *out, size_t outsz)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)job_id);
    db_result_t *r = NULL;
    out[0] = '\0';
    if (db_execute(db, "SELECT state FROM jobs WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1)
        snprintf(out, outsz, "%s", r->rows[0][0]);
    db_result_free(r);
}

/* Wait up to 10 s for a job to reach a state; returns 1 if it did. */
static int wait_state(int64_t job_id, const char *want)
{
    char st[32];
    for (int i = 0; i < 2000; i++) {
        job_state(job_id, st, sizeof(st));
        if (strcmp(st, want) == 0) return 1;
        usleep(5000);
    }
    return 0;
}

/* The schedule's jobs, oldest first. Returns how many. */
static int schedule_jobs(int64_t sid, int64_t *ids, int max)
{
    char s[32];
    snprintf(s, sizeof(s), "%lld", (long long)sid);
    db_result_t *r = NULL;
    int n = 0;
    if (db_execute(db, "SELECT id FROM jobs WHERE schedule_id = ? ORDER BY id",
                   (const char *[]){ s, NULL }, &r) == 0 && r)
        for (int i = 0; i < r->nrows && n < max; i++)
            ids[n++] = strtoll(r->rows[i][0], NULL, 10);
    db_result_free(r);
    return n;
}

/*
 * B108 end to end, on one worker so starvation shows. The first run is
 * cancelled for real and stops; later fires while it is winding down leave
 * the replacement alone; another repository's job, queued meanwhile, gets
 * the worker; and the replacement then runs to completion.
 */
static void test_cancel_running_stops_the_job_and_the_replacement_completes(void **state)
{
    (void)state;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id, uuid, path, added_at) VALUES (2,'u2','/tmp/r2',0)",
        NULL, NULL), 0);
    atomic_store(&g_started, 0);
    atomic_store(&g_release, 0);
    atomic_store(&g_deaf, 0);

    job_manager_t *jm = NULL;
    assert_int_equal(job_manager_start(db, 1, &jm), OK);
    scheduler_t *s = NULL;
    assert_int_equal(scheduler_open(db, jm_dispatch, jm, &s), OK);
    scheduler_set_canceller(s, jm_cancel);

    time_t now = 1720000000;
    int64_t sid = add_schedule("backup", SPEC_HOURLY, now - 10, "cancel_running", NULL);

    assert_int_equal(scheduler_tick(s, now), OK);
    int64_t ids[8];
    assert_int_equal(schedule_jobs(sid, ids, 8), 1);
    int64_t first = ids[0];
    assert_true(wait_state(first, "running"));

    /* Busy between checks, so the cancel takes a while to land. */
    atomic_store(&g_deaf, 1);
    int64_t other = 0;
    assert_int_equal(job_submit(jm, 2, "backup", NULL, slow_job, NULL, NULL, &other), OK);

    assert_int_equal(scheduler_tick(s, now + 3600), OK);
    assert_int_equal(schedule_jobs(sid, ids, 8), 2);
    int64_t replacement = ids[1];

    /* Further fires while the first is still stopping: no new run, and the
     * waiting replacement is not cancelled. */
    assert_int_equal(scheduler_tick(s, now + 7200), OK);
    assert_int_equal(scheduler_tick(s, now + 10800), OK);
    assert_int_equal(schedule_jobs(sid, ids, 8), 2);
    char st[32];
    job_state(replacement, st, sizeof(st));
    assert_string_equal(st, "queued");

    /* The job looks at its flag again and stops. */
    atomic_store(&g_deaf, 0);
    assert_true(wait_state(first, "cancelled"));
    /* The freed worker takes the other repository's job, queued first. */
    assert_true(wait_state(other, "running"));

    atomic_store(&g_release, 1);
    assert_true(wait_state(other, "completed"));
    assert_true(wait_state(replacement, "completed"));
    assert_int_equal(atomic_load(&g_started), 3);
    assert_int_equal(schedule_jobs(sid, ids, 8), 2);

    scheduler_stop(s);
    job_manager_stop(jm);
}

/* B109 against the real queue: four missed fires behind a long run leave
 * exactly one run waiting, and both complete. */
static void test_queue_coalesces_against_the_job_manager(void **state)
{
    (void)state;
    atomic_store(&g_started, 0);
    atomic_store(&g_release, 0);
    atomic_store(&g_deaf, 0);

    job_manager_t *jm = NULL;
    assert_int_equal(job_manager_start(db, 2, &jm), OK);
    scheduler_t *s = NULL;
    assert_int_equal(scheduler_open(db, jm_dispatch, jm, &s), OK);
    scheduler_set_canceller(s, jm_cancel);

    time_t now = 1720000000;
    int64_t sid = add_schedule("gc", SPEC_HOURLY, now - 10, "queue", NULL);
    assert_int_equal(scheduler_tick(s, now), OK);
    int64_t ids[8];
    assert_int_equal(schedule_jobs(sid, ids, 8), 1);
    assert_true(wait_state(ids[0], "running"));

    for (int k = 1; k <= 4; k++)
        assert_int_equal(scheduler_tick(s, now + k * 3600), OK);
    assert_int_equal(schedule_jobs(sid, ids, 8), 2);

    atomic_store(&g_release, 1);
    assert_true(wait_state(ids[0], "completed"));
    assert_true(wait_state(ids[1], "completed"));
    assert_int_equal(atomic_load(&g_started), 2);

    scheduler_stop(s);
    job_manager_stop(jm);
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

/*
 * B064: catchup was stored and only honoured for a schedule that had fired
 * before. One that never fired and missed its first window while the
 * daemon was down simply lost it. A restart is where that is decided, so
 * reopen the scheduler over a past due time with no last fire.
 */
static void test_catchup_run_once_without_history_fires_at_startup(void **state)
{
    (void)state;
    time_t past = time(NULL) - 100000;
    int64_t once = add_schedule("gc", SPEC_HOURLY, past, "skip", "run_once");
    int64_t skip = add_schedule("verify", SPEC_HOURLY, past, "skip", "skip");

    scheduler_stop(sched);
    sched = NULL;
    time_t before = time(NULL);
    assert_int_equal(scheduler_open(db, record_dispatch, NULL, &sched), OK);
    time_t after = time(NULL);

    assert_true(due_of(once) >= before && due_of(once) <= after);
    assert_true(due_of(skip) > after);

    /* A tick that cannot tell a failed dispatch from a missed window must
     * not apply it: recomputing outside startup moves to the future. */
    assert_int_equal(scheduler_recompute(db, once, after), OK);
    assert_true(due_of(once) > after);
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
    /* And says so where the list shows it, not in the log alone. */
    char why[256];
    reason_of(sid, why, sizeof(why));
    assert_non_null(strstr(why, "invalid trigger"));
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

/*
 * With nothing scheduled the loop must actually sleep.
 *
 * MIN() over no rows is SQL NULL, and this db layer returns an empty string
 * for NULL rather than a NULL pointer, so the guard meaning "nothing is
 * scheduled" never fired: strtoll("") gave 0, the wait clamped to zero and
 * the thread spun. Because every thread in the daemon shares one database
 * handle behind one mutex, that did not merely waste a core, it starved
 * everything else - a backup measured 89 of its 104 seconds waiting on that
 * mutex rather than working.
 *
 * Asserting that something sleeps is awkward, so this asserts on the
 * consequence: a spinning loop burns wall-clock worth of CPU, a sleeping one
 * burns almost none.
 */
static double process_cpu_seconds(void)
{
    struct timespec t;
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static void test_idle_scheduler_does_not_spin(void **state)
{
    (void)state;
    assert_int_equal(db_execute_non_query(db, "DELETE FROM schedules", NULL, NULL), 0);

    scheduler_t *s = NULL;
    assert_int_equal(scheduler_open(db, record_dispatch, NULL, &s), OK);
    assert_int_equal(scheduler_run(s), OK);

    double cpu0 = process_cpu_seconds();
    usleep(500000);
    double cpu1 = process_cpu_seconds();
    scheduler_stop(s);

    /* A spin consumed essentially the whole half second. A tenth of it is
     * generous for a thread that should be asleep throughout. */
    assert_true(cpu1 - cpu0 < 0.05);
}

/* ---------- NIT-3 ---------- */

/*
 * Rows written before the route refused them: an impossible yearly day, an
 * overlap and a catchup policy outside their sets, an action nothing
 * dispatches. Each survived the upgrade and ran under whatever the
 * scheduler made of it. The startup check disables each, naming why, and
 * leaves a valid schedule and one a person turned off alone.
 */
#define SPEC_FEB30 "{\"type\":\"calendar\",\"every\":\"year\",\"on_month\":2," \
                   "\"on_day\":30,\"at\":\"03:00\"}"
static void test_audit_disables_rows_the_route_refuses(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t good    = add_schedule("gc", SPEC_HOURLY, now + 60, NULL, NULL);
    int64_t feb30   = add_schedule("verify", SPEC_FEB30, now + 60, NULL, NULL);
    int64_t overlap = add_schedule("gc", SPEC_HOURLY, now + 60, "sometimes", NULL);
    int64_t catchup = add_schedule("gc", SPEC_HOURLY, now + 60, NULL, "always");
    int64_t action  = add_schedule("defrag", SPEC_HOURLY, now + 60, NULL, NULL);
    int64_t off     = add_schedule("gc", SPEC_HOURLY, now + 60, "sometimes", NULL);
    char offid[32];
    snprintf(offid, sizeof(offid), "%lld", (long long)off);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE schedules SET enabled = 0 WHERE id = ?",
        (const char *[]){ offid, NULL }, NULL), 0);

    assert_int_equal(routes_schedules_audit(db), 4);

    char why[256];
    assert_int_equal(enabled_of(good), 1);
    reason_of(good, why, sizeof(why));
    assert_string_equal(why, "");

    assert_int_equal(enabled_of(feb30), 0);
    reason_of(feb30, why, sizeof(why));
    assert_non_null(strstr(why, "month 2 has no day 30"));

    assert_int_equal(enabled_of(overlap), 0);
    reason_of(overlap, why, sizeof(why));
    assert_string_equal(why, "on_overlap 'sometimes' is not skip, queue or cancel_running");

    assert_int_equal(enabled_of(catchup), 0);
    reason_of(catchup, why, sizeof(why));
    assert_string_equal(why, "catchup 'always' is not skip or run_once");

    assert_int_equal(enabled_of(action), 0);
    reason_of(action, why, sizeof(why));
    assert_non_null(strstr(why, "action 'defrag' cannot be scheduled"));

    /* Turned off by a person: not the daemon's to annotate. */
    reason_of(off, why, sizeof(why));
    assert_string_equal(why, "");

    /* Nothing left to find the second time. */
    assert_int_equal(routes_schedules_audit(db), 0);

    /* And a disabled row does not fire, however overdue. */
    assert_int_equal(scheduler_tick(sched, now + 7200), OK);
    assert_int_equal(g_fires, 1);
    assert_string_equal(g_last_action, "gc");
}

static api_request_t body_req(const char *body)
{
    api_request_t req;
    memset(&req, 0, sizeof(req));
    req.method   = API_PUT;
    req.url      = "/repos/1/schedules/x";
    req.body     = body;
    req.body_len = body ? strlen(body) : 0;
    req.is_local = 1;
    req.client_addr = "";
    return req;
}

/* The list shows the reason, enabling one still broken is refused, and a
 * change by hand clears what the daemon recorded. */
static void test_disabled_reason_is_listed_and_enable_refused(void **state)
{
    (void)state;
    time_t now = 1720000000;
    int64_t bad = add_schedule("gc", SPEC_HOURLY, now + 60, NULL, "always");
    int64_t ok  = add_schedule("gc", SPEC_HOURLY, now + 60, NULL, NULL);
    assert_int_equal(routes_schedules_audit(db), 1);
    routes_schedules_init(db, sched);

    api_request_t get = body_req(NULL);
    get.method = API_GET;
    api_response_t resp = schedules_get(&get, 1);
    assert_int_equal(resp.status, 200);
    assert_non_null(strstr(resp.body,
        "\"disabled_reason\":\"catchup 'always' is not skip or run_once\""));
    /* Only on the row the daemon turned off. */
    assert_null(strstr(strstr(resp.body, "disabled_reason") + 1, "disabled_reason"));
    free(resp.body);

    api_request_t on = body_req("{\"enabled\":true}");
    resp = schedules_patch(&on, 1, bad);
    assert_int_equal(resp.status, 409);
    assert_non_null(strstr(resp.body, "catchup 'always' is not skip or run_once"));
    free(resp.body);
    assert_int_equal(enabled_of(bad), 0);

    api_request_t offr = body_req("{\"enabled\":false}");
    resp = schedules_patch(&offr, 1, bad);
    assert_int_equal(resp.status, 200);
    free(resp.body);
    char why[256];
    reason_of(bad, why, sizeof(why));
    assert_string_equal(why, "");

    /* A valid schedule still toggles both ways. */
    resp = schedules_patch(&offr, 1, ok);
    assert_int_equal(resp.status, 200);
    free(resp.body);
    resp = schedules_patch(&on, 1, ok);
    assert_int_equal(resp.status, 200);
    free(resp.body);
    assert_int_equal(enabled_of(ok), 1);
    routes_schedules_init(NULL, NULL);
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
        cmocka_unit_test_setup_teardown(test_overlap_queue_coalesces_missed_fires, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_queue_counts_only_its_own_runs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_cancel_running, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_cancel_running_spares_the_replacement, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_cancel_running_without_canceller, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_running_stops_the_job_and_the_replacement_completes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_queue_coalesces_against_the_job_manager, setup, teardown),
        cmocka_unit_test_setup_teardown(test_completed_job_is_not_an_overlap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_overlap_is_per_action, setup, teardown),
        cmocka_unit_test_setup_teardown(test_catchup_skip_moves_forward, setup, teardown),
        cmocka_unit_test_setup_teardown(test_catchup_run_once_fires_now, setup, teardown),
        cmocka_unit_test_setup_teardown(test_catchup_run_once_without_history_fires_at_startup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bad_trigger_disables_schedule, setup, teardown),
        cmocka_unit_test_setup_teardown(test_one_bad_schedule_does_not_block_others, setup, teardown),
        cmocka_unit_test_setup_teardown(test_idle_scheduler_does_not_spin, setup, teardown),
        cmocka_unit_test_setup_teardown(test_audit_disables_rows_the_route_refuses, setup, teardown),
        cmocka_unit_test_setup_teardown(test_disabled_reason_is_listed_and_enable_refused, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
