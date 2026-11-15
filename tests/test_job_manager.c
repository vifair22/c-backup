#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

/*
 * Job dispatch.
 *
 * Every repo-scoped job takes the repository write lock, and that lock
 * rejects rather than queues. So the question that matters is not whether a
 * job runs, but whether the dispatcher ever puts two jobs for one repository
 * on workers at the same time: if it does, the second one fails instantly
 * with "repo is busy" and the work is simply dropped after being accepted.
 *
 * That is what these tests pin down, from both sides. Jobs for one repository
 * must never overlap, and jobs for different repositories still must, because
 * serialising everything would be an easy way to pass the first test and a
 * bad way to run a daemon.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "job_manager.h"

#define TEST_DIR "/tmp/c_backup_test_jobm"
#define TEST_DB  TEST_DIR "/state.db"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static job_manager_t *jm;

/* Observed concurrency, guarded because the workers touch it. */
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_running[8];      /* per repo id */
static int g_peak[8];         /* highest simultaneous count seen per repo */
static int g_total_running;
static int g_peak_total;
static int g_completed;

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

    memset(g_running, 0, sizeof(g_running));
    memset(g_peak, 0, sizeof(g_peak));
    g_total_running = 0;
    g_peak_total = 0;
    g_completed = 0;

    if (job_manager_start(db, 2, &jm) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    job_manager_stop(jm); jm = NULL;
    db_close(db);         db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* Sleeps long enough that an overlap, if the dispatcher allows one, is
 * observed rather than missed by luck of scheduling. */
static status_t probe_job(void *arg, volatile int *cancel,
                          void (*progress)(const char *, uint64_t, uint64_t, void *),
                          void *progress_ctx)
{
    (void)cancel; (void)progress; (void)progress_ctx;
    int repo = (int)(intptr_t)arg;

    pthread_mutex_lock(&g_mu);
    g_running[repo]++;
    g_total_running++;
    if (g_running[repo] > g_peak[repo]) g_peak[repo] = g_running[repo];
    if (g_total_running > g_peak_total) g_peak_total = g_total_running;
    pthread_mutex_unlock(&g_mu);

    usleep(120000);

    pthread_mutex_lock(&g_mu);
    g_running[repo]--;
    g_total_running--;
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void submit(int64_t repo_id, const char *action)
{
    int64_t id = 0;
    assert_int_equal(job_submit(jm, repo_id, action, NULL, probe_job,
                                (void *)(intptr_t)repo_id, NULL, &id), OK);
    assert_true(id > 0);
}

static void wait_for(int n)
{
    for (int i = 0; i < 400; i++) {
        pthread_mutex_lock(&g_mu);
        int done = g_completed;
        pthread_mutex_unlock(&g_mu);
        if (done >= n) return;
        usleep(25000);
    }
}

static int db_count(const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(db, sql, NULL, &r) != 0) { db_result_free(r); return -1; }
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/* ------------------------------------------------------------------ */

/*
 * Four jobs, one repository, two workers. Before the dispatcher consulted
 * repo_id this ran two at a time and half of them died on the write lock.
 */
static void test_one_repo_never_runs_two_jobs_at_once(void **state)
{
    (void)state;
    for (int i = 0; i < 4; i++) submit(1, "gc");
    wait_for(4);

    pthread_mutex_lock(&g_mu);
    int peak = g_peak[1];
    int done = g_completed;
    pthread_mutex_unlock(&g_mu);

    assert_int_equal(peak, 1);
    assert_int_equal(done, 4);
}

/* And every one of them must actually have run, not been dropped. */
static void test_queued_jobs_all_complete(void **state)
{
    (void)state;
    for (int i = 0; i < 4; i++) submit(1, "gc");
    wait_for(4);
    /* Give the manager a moment to record the final states. */
    usleep(200000);

    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'completed'"), 4);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"), 0);
}

/*
 * The counterweight: serialising every job would pass the test above and
 * halve the daemon's throughput. Two repositories must still overlap.
 */
static void test_different_repos_still_run_in_parallel(void **state)
{
    (void)state;
    submit(1, "gc");
    submit(2, "gc");
    wait_for(2);

    pthread_mutex_lock(&g_mu);
    int peak_total = g_peak_total;
    pthread_mutex_unlock(&g_mu);

    assert_int_equal(peak_total, 2);
}

/*
 * A repository with a long queue must not stall an unrelated one behind it.
 * The dispatcher scans past a blocked job rather than waiting on the head.
 */
static void test_a_busy_repo_does_not_block_another(void **state)
{
    (void)state;
    submit(1, "gc");
    submit(1, "gc");
    submit(1, "gc");
    submit(2, "gc");   /* queued last, behind three for repo 1 */
    wait_for(4);

    pthread_mutex_lock(&g_mu);
    int peak_total = g_peak_total;
    pthread_mutex_unlock(&g_mu);

    /* Repo 2's job ran alongside one of repo 1's rather than after all of
     * them, which only happens if the scan skipped the blocked entries. */
    assert_int_equal(peak_total, 2);
}

/*
 * Finished jobs older than a month are dropped, at start and as jobs finish.
 * A recent one stays, and so does anything not finished: age alone is never
 * a reason to lose a job that is still running.
 */
static void insert_job(const char *state, long long ended_ago)
{
    char ended[32];
    snprintf(ended, sizeof(ended), "%lld", (long long)time(NULL) - ended_ago);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO jobs (repo_id, action, state, started, ended)"
        " VALUES (1, 'gc', ?, ?, ?)",
        (const char *[]){ state, ended, ended, NULL }, NULL), 0);
}

static void test_old_finished_jobs_are_pruned_at_start(void **state)
{
    (void)state;
    job_manager_stop(jm); jm = NULL;
    insert_job("completed", 40LL * 86400);
    insert_job("failed",    40LL * 86400);
    insert_job("cancelled", 40LL * 86400);
    insert_job("completed", 2LL * 86400);
    assert_int_equal(job_manager_start(db, 2, &jm), OK);

    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs"), 1);
}

static void test_old_finished_jobs_are_pruned_as_jobs_finish(void **state)
{
    (void)state;
    insert_job("completed", 40LL * 86400);
    submit(1, "gc");
    wait_for(1);
    usleep(200000);
    /* The one just run is all that is left. */
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs"), 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE ended < strftime('%s','now') - 86400"), 0);
}

/*
 * Progress is throttled to a few writes a second, and that must not cost the
 * final tick: a job record that stops at 4,999 of 5,000 reads as a stall.
 */
static status_t chatty_job(void *arg, volatile int *cancel,
                           void (*progress)(const char *, uint64_t, uint64_t, void *),
                           void *progress_ctx)
{
    (void)arg; (void)cancel;
    for (uint64_t i = 1; i <= 5000; i++) progress("work", i, 5000, progress_ctx);
    pthread_mutex_lock(&g_mu);
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void test_throttled_progress_keeps_the_last_tick(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(job_submit(jm, 1, "gc", NULL, chatty_job, NULL, NULL, &id), OK);
    wait_for(1);
    usleep(200000);
    assert_int_equal(db_count("SELECT progress_current FROM jobs WHERE id = 1"), 5000);
}

static int db_text(const char *sql, char *out, size_t cap)
{
    db_result_t *r = NULL;
    out[0] = '\0';
    if (db_execute(db, sql, NULL, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    snprintf(out, cap, "%s", r->rows[0][0]);
    db_result_free(r);
    return 0;
}

/*
 * B059: a job the daemon never finished - SIGKILL, docker kill, a power cut -
 * is marked failed at the next start, and must also reach the journal. The
 * journal is written by the worker when a job ends, and a killed process has
 * no worker left to write it, so the permanent record simply lacked every
 * interrupted backup.
 */
static void test_interrupted_jobs_are_journalled_at_start(void **state)
{
    (void)state;
    job_manager_stop(jm); jm = NULL;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO jobs (repo_id, action, state, started, origin, schedule_id)"
        " VALUES (1, 'backup', 'running', strftime('%s','now'), 'schedule', 7)",
        NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO jobs (repo_id, action, state) VALUES (2, 'verify', 'queued')",
        NULL, NULL), 0);
    assert_int_equal(job_manager_start(db, 2, &jm), OK);

    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"
                              " AND error = 'interrupted by daemon restart'"), 2);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal"), 2);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE repo_id = 1"
                              " AND operation = 'backup' AND result = 'failed'"
                              " AND source = 'schedule'"
                              " AND message = 'schedule 7: interrupted by daemon restart'"), 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE repo_id = 2"
                              " AND operation = 'verify' AND result = 'failed'"
                              " AND source = 'api'"
                              " AND message = 'interrupted by daemon restart'"), 1);

    /* Once only: the next start finds nothing left to journal. */
    job_manager_stop(jm); jm = NULL;
    assert_int_equal(job_manager_start(db, 2, &jm), OK);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal"), 2);
}

/*
 * B072: every journal entry said source "daemon", so a scheduled backup and
 * one run by hand could not be told apart afterwards. The job carries its
 * origin, and the journal says which schedule.
 */
static void test_journal_source_says_who_asked(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(job_submit_scheduled(jm, 1, "gc", NULL, 42, probe_job,
                                          (void *)(intptr_t)1, NULL, &id), OK);
    wait_for(1);
    submit(1, "gc");
    wait_for(2);
    usleep(200000);

    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE source = 'daemon'"), 0);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE source = 'schedule'"
                              " AND message LIKE 'schedule 42%'"), 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE source = 'api'"), 1);

    char origin[32];
    assert_int_equal(db_text("SELECT origin || ':' || COALESCE(schedule_id, 0)"
                             "  FROM jobs ORDER BY id LIMIT 1", origin, sizeof(origin)), 0);
    assert_string_equal(origin, "schedule:42");
    assert_int_equal(db_text("SELECT origin || ':' || COALESCE(schedule_id, 0)"
                             "  FROM jobs ORDER BY id DESC LIMIT 1", origin, sizeof(origin)), 0);
    assert_string_equal(origin, "api:0");
}

/*
 * B083: a failed job's alert said "repo 3: <error>" under the title
 * "c-backup: backup failed", and the title is all c-utils logs when a send
 * is rejected, so neither the line in the journal nor the notification named
 * the repository. Manual 16.3 promises the action, the repository and the
 * error.
 */
static void test_alert_names_repository_and_error(void **state)
{
    (void)state;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id, uuid, path, label, added_at)"
        " VALUES (7, 'u-7', '/srv/repos/photos', 'photos', 0)", NULL, NULL), 0);

    char title[250], body[1024];
    job_alert_compose(db, 7, 41, "backup", "cannot read /data: Permission denied",
                      title, sizeof(title), body, sizeof(body));
    assert_string_equal(title, "c-backup: backup of photos failed");
    assert_non_null(strstr(body, "Repository photos (id 7) at /srv/repos/photos"));
    assert_non_null(strstr(body, "Job 41 (backup) failed: cannot read /data: Permission denied"));

    /* A repository removed while its job ran is still named, by id. */
    job_alert_compose(db, 99, 42, "verify", NULL,
                      title, sizeof(title), body, sizeof(body));
    assert_string_equal(title, "c-backup: verify of repo 99 failed");
    assert_non_null(strstr(body, "Repository 99 (no longer registered)"));
    assert_non_null(strstr(body, "failed: no detail recorded"));

    /* An unlabelled one is named by id in the title and by path below. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id, uuid, path, added_at)"
        " VALUES (8, 'u-8', '/srv/repos/misc', 0)", NULL, NULL), 0);
    job_alert_compose(db, 8, 43, "gc", "disk full",
                      title, sizeof(title), body, sizeof(body));
    assert_string_equal(title, "c-backup: gc of repo 8 failed");
    assert_non_null(strstr(body, "(no label) (id 8) at /srv/repos/misc"));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_one_repo_never_runs_two_jobs_at_once, setup, teardown),
        cmocka_unit_test_setup_teardown(test_queued_jobs_all_complete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_different_repos_still_run_in_parallel, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_busy_repo_does_not_block_another, setup, teardown),
        cmocka_unit_test_setup_teardown(test_old_finished_jobs_are_pruned_at_start, setup, teardown),
        cmocka_unit_test_setup_teardown(test_old_finished_jobs_are_pruned_as_jobs_finish, setup, teardown),
        cmocka_unit_test_setup_teardown(test_throttled_progress_keeps_the_last_tick, setup, teardown),
        cmocka_unit_test_setup_teardown(test_interrupted_jobs_are_journalled_at_start, setup, teardown),
        cmocka_unit_test_setup_teardown(test_journal_source_says_who_asked, setup, teardown),
        cmocka_unit_test_setup_teardown(test_alert_names_repository_and_error, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
