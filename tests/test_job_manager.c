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
#include <sched.h>
#include <stdatomic.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/log.h>

#include "job_manager.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_jobm"
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
static status_t probe_job(void *arg, atomic_int *cancel,
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
static status_t chatty_job(void *arg, atomic_int *cancel,
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

/*
 * B093: a phase that ends short of its total inside one throttle window kept
 * its first count. /jobs said trees 0/171 for a job whose closing log line
 * said 170/171. The last report is what the job record must end on, whether
 * or not it reached the total.
 */
static status_t short_phase_job(void *arg, atomic_int *cancel,
                                void (*progress)(const char *, uint64_t, uint64_t, void *),
                                void *progress_ctx)
{
    (void)arg; (void)cancel;
    for (uint64_t i = 0; i < 171; i++) progress("trees", i, 171, progress_ctx);
    pthread_mutex_lock(&g_mu);
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void test_final_progress_is_the_last_report(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, short_phase_job, NULL, NULL, &id), OK);
    wait_for(1);
    usleep(200000);
    assert_int_equal(db_count("SELECT progress_current FROM jobs WHERE id = 1"), 170);
    assert_int_equal(db_count("SELECT progress_total FROM jobs WHERE id = 1"), 171);
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
 * Job ids come from the job's own INSERT. They came from a following
 * SELECT last_insert_rowid(), which is per connection, and every thread
 * shares the one connection: a grep or verify inserting its run row in
 * between gave the submitter that row's id. The CLI then waited on, and
 * reported, some other job. Here a thread inserts rows into another table
 * while each job is submitted, and every id handed back has to be the job
 * that was submitted.
 *
 * The noise runs on a budget per submission rather than flat out (FLAKE-5).
 * A thread re-taking the database mutex in a tight loop can hold it for as
 * long as the scheduler lets it, and this test sometimes took a minute
 * waiting for its turn. Each submission now races a burst of up to
 * NOISE_PER_SUBMIT inserts, released just before it starts, which is the
 * same window the old code lost ids in, and the database is quiet while the
 * returned id is checked.
 */
#define NOISE_PER_SUBMIT 32

static atomic_int  g_noise_stop;
static atomic_int  g_noise_budget;
static atomic_long g_noise_rows;

static void *noise_inserts(void *arg)
{
    (void)arg;
    while (!atomic_load(&g_noise_stop)) {
        int left = atomic_load(&g_noise_budget);
        if (left <= 0) { sched_yield(); continue; }
        if (!atomic_compare_exchange_weak(&g_noise_budget, &left, left - 1))
            continue;
        if (db_execute_non_query(db,
                "INSERT INTO grep_runs (repo_id, snap_id, pattern, started_at)"
                " VALUES (1, 1, 'noise', 0)", NULL, NULL) == 0)
            atomic_fetch_add(&g_noise_rows, 1);
    }
    return NULL;
}

static status_t noop_job(void *arg, atomic_int *cancel,
                         void (*progress)(const char *, uint64_t, uint64_t, void *),
                         void *progress_ctx)
{
    (void)arg; (void)cancel; (void)progress; (void)progress_ctx;
    return OK;
}

static void test_job_ids_survive_concurrent_inserts(void **state)
{
    (void)state;
    /* Start the other table far ahead, so a stolen id cannot also happen
     * to be a real job's. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO grep_runs (id, repo_id, snap_id, pattern, started_at)"
        " VALUES (1000000, 1, 1, 'seed', 0)", NULL, NULL), 0);

    atomic_store(&g_noise_stop, 0);
    atomic_store(&g_noise_budget, 0);
    atomic_store(&g_noise_rows, 0);
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, noise_inserts, NULL), 0);

    int wrong = 0;
    for (int i = 0; i < 200; i++) {
        char action[32];
        snprintf(action, sizeof(action), "probe-%d", i);
        int64_t id = 0;
        atomic_store(&g_noise_budget, NOISE_PER_SUBMIT);
        assert_int_equal(job_submit(jm, 1 + (i % 4), action, NULL, noop_job,
                                    NULL, NULL, &id), OK);
        atomic_store(&g_noise_budget, 0);
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
        db_result_t *r = NULL;
        assert_int_equal(db_execute(db, "SELECT action FROM jobs WHERE id = ?",
                                    (const char *[]){ idbuf, NULL }, &r), 0);
        if (!r || r->nrows != 1 || strcmp(r->rows[0][0], action) != 0) wrong++;
        db_result_free(r);
        if (i % 16 == 15) usleep(20000);   /* let the workers drain the queue */
    }
    atomic_store(&g_noise_stop, 1);
    pthread_join(th, NULL);
    assert_int_equal(wrong, 0);
    /* The noise has to have happened for the ids to prove anything. */
    assert_true(atomic_load(&g_noise_rows) > 0);
}

/*
 * B090: cancelling a queued job flagged it and left it in the queue, so it
 * read "queued" until a worker reached it, and only then "cancelled". It
 * must be cancelled at once, journalled, and never run.
 */
static atomic_int g_gate_open;
static int g_gated_runs;

static status_t gated_job(void *arg, atomic_int *cancel,
                          void (*progress)(const char *, uint64_t, uint64_t, void *),
                          void *progress_ctx)
{
    (void)arg; (void)cancel; (void)progress; (void)progress_ctx;
    pthread_mutex_lock(&g_mu);
    g_gated_runs++;
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < 400 && !g_gate_open; i++) usleep(10000);
    pthread_mutex_lock(&g_mu);
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static atomic_int g_arg_freed;
static void count_free(void *arg) { (void)arg; g_arg_freed++; }

static void test_cancelling_a_queued_job_is_immediate(void **state)
{
    (void)state;
    g_gate_open = 0;
    g_gated_runs = 0;
    g_arg_freed = 0;
    int64_t first = 0, second = 0, third = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, gated_job, NULL, NULL, &first), OK);
    for (int i = 0; i < 200 && db_count("SELECT COUNT(*) FROM jobs WHERE state = 'running'") != 1; i++)
        usleep(10000);
    /* Same repository, so both wait behind the first. */
    assert_int_equal(job_submit(jm, 1, "verify", NULL, gated_job, NULL, count_free, &second), OK);
    assert_int_equal(job_submit(jm, 1, "gc", NULL, gated_job, NULL, NULL, &third), OK);

    assert_int_equal(job_cancel(jm, second), OK);

    char q[160], st[32];
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)second);
    assert_int_equal(db_text(q, st, sizeof(st)), 0);
    assert_string_equal(st, "cancelled");
    assert_int_equal(g_arg_freed, 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE operation = 'verify'"
                              " AND result = 'cancelled'"), 1);

    /* A second cancel finds nothing left to cancel. */
    assert_int_not_equal(job_cancel(jm, second), OK);

    g_gate_open = 1;
    wait_for(2);
    usleep(200000);
    /* The first and third ran; the cancelled one never did. */
    assert_int_equal(g_gated_runs, 2);
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)third);
    assert_int_equal(db_text(q, st, sizeof(st)), 0);
    assert_string_equal(st, "completed");
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE operation = 'verify'"), 1);
}

/*
 * B110: stopping flagged every slot and let the workers drain the queue, so
 * each queued job was claimed, marked running and started already cancelled.
 * Manual 2.2 promises a queued job never starts: it is recorded cancelled
 * and journalled "cancelled before it started", and only the running job is
 * asked to stop. Queued jobs on another repository are no different.
 */
static atomic_int g_polling_runs;

static status_t polling_job(void *arg, atomic_int *cancel,
                            void (*progress)(const char *, uint64_t, uint64_t, void *),
                            void *progress_ctx)
{
    (void)arg; (void)progress; (void)progress_ctx;
    pthread_mutex_lock(&g_mu);
    g_polling_runs++;
    pthread_mutex_unlock(&g_mu);
    for (int i = 0; i < 2000; i++) {
        if (atomic_load(cancel)) return set_error(ERR_IO, "cancelled");
        usleep(5000);
    }
    return set_error(ERR_IO, "polling_job: never cancelled");
}

static void test_shutdown_never_starts_a_queued_job(void **state)
{
    (void)state;
    g_polling_runs = 0;
    g_arg_freed = 0;
    int64_t a = 0, b = 0, q1 = 0, q2 = 0, q3 = 0;
    /* Two workers, both busy, so everything after waits in the queue. */
    assert_int_equal(job_submit(jm, 1, "backup", NULL, polling_job, NULL, NULL, &a), OK);
    assert_int_equal(job_submit(jm, 2, "backup", NULL, polling_job, NULL, NULL, &b), OK);
    for (int i = 0; i < 400 && db_count("SELECT COUNT(*) FROM jobs WHERE state = 'running'") != 2; i++)
        usleep(5000);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'running'"), 2);
    assert_int_equal(job_submit(jm, 1, "verify", NULL, polling_job, NULL, count_free, &q1), OK);
    assert_int_equal(job_submit(jm, 2, "gc", NULL, polling_job, NULL, count_free, &q2), OK);
    assert_int_equal(job_submit(jm, 3, "gc", NULL, polling_job, NULL, count_free, &q3), OK);

    job_manager_stop(jm);
    jm = NULL;

    /* Only the two that were running ever started. */
    assert_int_equal(g_polling_runs, 2);
    assert_int_equal(g_arg_freed, 3);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'"), 5);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'"
                              " AND started IS NULL"), 3);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE result = 'cancelled'"
                              " AND message = 'cancelled before it started'"), 3);
    char q[96], st[32];
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)a);
    assert_int_equal(db_text(q, st, sizeof(st)), 0);
    assert_string_equal(st, "cancelled");
}

/*
 * OWN-15: a claimed job stayed in its ring slot behind the head while the
 * full check counted only queued jobs, so with every worker busy the tail
 * wrapped onto a running job's slot after JOB_QUEUE_MAX - workers queued
 * jobs. The next submits overwrote it, cancel flag included: `jobs cancel`
 * on the running job then answered "not running" and it ran on.
 *
 * Here both workers are busy, the queue takes exactly JOB_QUEUE_MAX more,
 * the one after is refused cleanly, and the running jobs are untouched:
 * still running, never flagged, and still stopped by a cancel.
 */
static int g_never_runs_calls;

static status_t never_runs_job(void *arg, atomic_int *cancel,
                               void (*progress)(const char *, uint64_t, uint64_t, void *),
                               void *progress_ctx)
{
    (void)arg; (void)cancel; (void)progress; (void)progress_ctx;
    pthread_mutex_lock(&g_mu);
    g_never_runs_calls++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void test_full_queue_refuses_and_spares_running_jobs(void **state)
{
    (void)state;
    g_polling_runs = 0;
    g_arg_freed = 0;
    g_never_runs_calls = 0;
    int64_t a = 0, b = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, polling_job, NULL, NULL, &a), OK);
    assert_int_equal(job_submit(jm, 2, "backup", NULL, polling_job, NULL, NULL, &b), OK);
    for (int i = 0; i < 400 && db_count("SELECT COUNT(*) FROM jobs WHERE state = 'running'") != 2; i++)
        usleep(5000);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'running'"), 2);

    /* Every slot the ring has, all on the busy repositories so none can
     * start: a running job's slot would be overwritten from the
     * (JOB_QUEUE_MAX - 1)th of these on. */
    for (int i = 0; i < JOB_QUEUE_MAX; i++) {
        int64_t id = 0;
        assert_int_equal(job_submit(jm, 1 + (i % 2), "verify", NULL, never_runs_job,
                                    NULL, count_free, &id), OK);
        assert_true(id > b);
    }

    /* One more is refused, cleanly: busy (503 at the route), a message
     * that says why, its argument released, its row marked failed. */
    int64_t over = -1;
    assert_int_equal(job_submit(jm, 3, "gc", NULL, never_runs_job, NULL, count_free, &over),
                     ERR_BUSY);
    assert_int_equal(over, -1);
    assert_non_null(strstr(err_msg(), "job queue full"));
    err_clear();
    assert_int_equal(g_arg_freed, 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"
                              " AND error = 'job queue full'"), 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'queued'"),
                     JOB_QUEUE_MAX);

    /* The running jobs: same ids, still running, never flagged (a polling
     * job returns as soon as it sees the flag), and nothing queued ran. */
    char q[96], st[32];
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)a);
    assert_int_equal(db_text(q, st, sizeof(st)), 0);
    assert_string_equal(st, "running");
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)b);
    assert_int_equal(db_text(q, st, sizeof(st)), 0);
    assert_string_equal(st, "running");
    assert_int_equal(g_never_runs_calls, 0);

    /* Cancelling each still reaches that job and stops it. */
    assert_int_equal(job_cancel(jm, a), OK);
    assert_int_equal(job_cancel(jm, b), OK);
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs WHERE id IN (%lld, %lld)"
             " AND state = 'cancelled'", (long long)a, (long long)b);
    for (int i = 0; i < 400 && db_count(q) != 2; i++) usleep(5000);
    assert_int_equal(db_count(q), 2);
    assert_int_equal(g_polling_runs, 2);

    /* And the queue behind them drains in full, every job intact. */
    for (int i = 0; i < 1000; i++) {
        if (db_count("SELECT COUNT(*) FROM jobs WHERE state = 'completed'") == JOB_QUEUE_MAX)
            break;
        usleep(5000);
    }
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'completed'"
                              " AND action = 'verify'"), JOB_QUEUE_MAX);
    pthread_mutex_lock(&g_mu);
    int calls = g_never_runs_calls;
    pthread_mutex_unlock(&g_mu);
    assert_int_equal(calls, JOB_QUEUE_MAX);
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

/*
 * A job that succeeds with a warning alerts through the failure path, at
 * WARN and titled as completed, and stays completed. One without a warning
 * sends nothing. B118: a backup that left files out reported completed and
 * told nobody unless they opened the job's result.
 */
static char g_alert_log[2048];
static pthread_mutex_t g_alert_mu = PTHREAD_MUTEX_INITIALIZER;

static void capture_alert(const char *ts, const char *level, const char *func,
                          const char *message, void *ud)
{
    (void)ts; (void)func; (void)ud;
    if (!strstr(message, "notification for job")) return;
    pthread_mutex_lock(&g_alert_mu);
    size_t have = strlen(g_alert_log);
    snprintf(g_alert_log + have, sizeof(g_alert_log) - have, "%s %s\n", level, message);
    pthread_mutex_unlock(&g_alert_mu);
}

static status_t warning_job(void *arg, atomic_int *cancel,
                            void (*progress)(const char *, uint64_t, uint64_t, void *),
                            void *progress_ctx)
{
    (void)cancel; (void)progress;
    if (arg) job_set_warning(progress_ctx, (const char *)arg);
    pthread_mutex_lock(&g_mu);
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

/* Wait for the job to end and, since the alert is sent after the state is
 * written, for the log to settle a little longer. */
static void wait_ended(int64_t id)
{
    char sql[128];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM jobs WHERE id = %lld AND state = 'completed'",
             (long long)id);
    for (int i = 0; i < 400 && db_count(sql) != 1; i++) usleep(25000);
    assert_int_equal(db_count(sql), 1);
}

static void test_a_warning_is_alerted_and_the_job_completes(void **state)
{
    (void)state;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id, uuid, path, label, added_at)"
        " VALUES (3, 'u-3', '/srv/repos/docs', 'docs', 0)", NULL, NULL), 0);
    g_alert_log[0] = '\0';
    int h = log_stream_register(capture_alert, NULL);
    assert_true(h >= 0);

    int64_t clean = 0, warned = 0;
    assert_int_equal(job_submit(jm, 3, "backup", NULL, warning_job, NULL, NULL,
                                &clean), OK);
    wait_ended(clean);
    assert_int_equal(job_submit(jm, 3, "backup", NULL, warning_job,
                                "1 file(s) could not be read; first: /d/x",
                                NULL, &warned), OK);
    wait_ended(warned);

    char want[96];
    snprintf(want, sizeof(want), "sending warning notification for job %lld:",
             (long long)warned);
    int seen = 0;
    for (int i = 0; i < 200 && !seen; i++) {
        pthread_mutex_lock(&g_alert_mu);
        seen = strstr(g_alert_log, want) != NULL;
        pthread_mutex_unlock(&g_alert_mu);
        if (!seen) usleep(10000);
    }
    log_stream_unregister(h);

    pthread_mutex_lock(&g_alert_mu);
    char got[sizeof(g_alert_log)];
    snprintf(got, sizeof(got), "%s", g_alert_log);
    pthread_mutex_unlock(&g_alert_mu);
    if (!seen) fail_msg("no warning alert for job %lld:\n%s", (long long)warned, got);
    assert_non_null(strstr(got, "c-backup: backup of docs completed with a warning"));
    assert_non_null(strstr(got, "1 file(s) could not be read; first: /d/x"));
    /* At warning level, not error, and only the one job. */
    assert_non_null(strstr(got, "warning sending warning notification"));
    snprintf(want, sizeof(want), "notification for job %lld:", (long long)clean);
    assert_null(strstr(got, want));
    assert_null(strstr(got, "failure notification"));
}

static void test_warning_alert_text(void **state)
{
    (void)state;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id, uuid, path, label, added_at)"
        " VALUES (7, 'u-7', '/srv/repos/photos', 'photos', 0)", NULL, NULL), 0);
    char title[250], body[1024];
    job_warning_compose(db, 7, 44, "backup", "2 file(s) could not be read",
                        title, sizeof(title), body, sizeof(body));
    assert_string_equal(title, "c-backup: backup of photos completed with a warning");
    assert_non_null(strstr(body, "Repository photos (id 7) at /srv/repos/photos"));
    assert_non_null(strstr(body, "Job 44 (backup) completed with a warning: "
                                 "2 file(s) could not be read"));
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
        cmocka_unit_test_setup_teardown(test_final_progress_is_the_last_report, setup, teardown),
        cmocka_unit_test_setup_teardown(test_interrupted_jobs_are_journalled_at_start, setup, teardown),
        cmocka_unit_test_setup_teardown(test_journal_source_says_who_asked, setup, teardown),
        cmocka_unit_test_setup_teardown(test_job_ids_survive_concurrent_inserts, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancelling_a_queued_job_is_immediate, setup, teardown),
        cmocka_unit_test_setup_teardown(test_shutdown_never_starts_a_queued_job, setup, teardown),
        cmocka_unit_test_setup_teardown(test_full_queue_refuses_and_spares_running_jobs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_alert_names_repository_and_error, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_warning_is_alerted_and_the_job_completes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_warning_alert_text, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
