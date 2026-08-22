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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_one_repo_never_runs_two_jobs_at_once, setup, teardown),
        cmocka_unit_test_setup_teardown(test_queued_jobs_all_complete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_different_repos_still_run_in_parallel, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_busy_repo_does_not_block_another, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
