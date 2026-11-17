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
#include "lock.h"
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
/* The rendezvous below (FLAKE-21), under the same mutex. */
#define RV_NEED      2
#define RV_BOUND_S   60
static pthread_cond_t g_rv_cv = PTHREAD_COND_INITIALIZER;
static int g_rv_arrived;      /* rendezvous jobs that have started */
static int g_rv_met;          /* of those, how many saw all RV_NEED running */
static int g_rv_timed_out;

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
    g_rv_arrived = 0;
    g_rv_met = 0;
    g_rv_timed_out = 0;

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

/*
 * A rendezvous, for proving two jobs run at once without timing them
 * (FLAKE-21). Each rendezvous job arrives, then waits until RV_NEED have
 * arrived: they can all return only if they were all running together, so
 * a dispatcher that serialised them leaves the first waiting and the test
 * fails at the bound, while one that overlaps them passes however slowly
 * the host schedules the second. Measuring overlap by wall time (two
 * 120 ms sleeps) failed on a loaded host, where the second job could start
 * after the first had finished its sleep.
 */

static status_t rendezvous_job(void *arg, atomic_int *cancel,
                               void (*progress)(const char *, uint64_t, uint64_t, void *),
                               void *progress_ctx)
{
    (void)cancel; (void)progress; (void)progress_ctx;
    int repo = (int)(intptr_t)arg;
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += RV_BOUND_S;

    pthread_mutex_lock(&g_mu);
    g_running[repo]++;
    if (g_running[repo] > g_peak[repo]) g_peak[repo] = g_running[repo];
    g_rv_arrived++;
    pthread_cond_broadcast(&g_rv_cv);
    int rc = 0;
    while (g_rv_arrived < RV_NEED && rc == 0)
        rc = pthread_cond_timedwait(&g_rv_cv, &g_mu, &deadline);
    if (g_rv_arrived >= RV_NEED) g_rv_met++;
    else                         g_rv_timed_out++;
    g_running[repo]--;
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void submit_rendezvous(int64_t repo_id)
{
    int64_t id = 0;
    assert_int_equal(job_submit(jm, repo_id, "gc", NULL, rendezvous_job,
                                (void *)(intptr_t)repo_id, NULL, &id), OK);
    assert_true(id > 0);
}

/* Wait until n jobs have returned, bounded past the rendezvous bound so a
 * rendezvous that times out is what the test reports. */
static int wait_completed(int n)
{
    for (int i = 0; i < (RV_BOUND_S + 30) * 100; i++) {
        pthread_mutex_lock(&g_mu);
        int done = g_completed;
        pthread_mutex_unlock(&g_mu);
        if (done >= n) return done;
        usleep(10000);
    }
    pthread_mutex_lock(&g_mu);
    int done = g_completed;
    pthread_mutex_unlock(&g_mu);
    return done;
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

/*
 * Poll until `sql` counts `want`, for at most about ten seconds, and return
 * the last count. For a step the worker takes after the job function
 * returns, which wait_for cannot see: a fixed sleep there is a guess at how
 * long the worker takes, and under a loaded `make test` the guess loses
 * (FLAKE-16). The caller still asserts the count, so a step that never
 * happens fails at the bound instead of passing on a timer.
 */
static int wait_db_count(const char *sql, int want)
{
    int v = db_count(sql);
    for (int i = 0; i < 2000 && v != want; i++) {
        usleep(5000);
        v = db_count(sql);
    }
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
    /* The worker records each final state after the job function returns. */
    wait_db_count("SELECT COUNT(*) FROM jobs WHERE state = 'completed'", 4);

    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'completed'"), 4);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"), 0);
}

/*
 * The counterweight: serialising every job would pass the test above and
 * halve the daemon's throughput. Two repositories must still overlap: each
 * job waits for the other to be running, so both return only if they ran
 * at once (FLAKE-21).
 */
static void test_different_repos_still_run_in_parallel(void **state)
{
    (void)state;
    submit_rendezvous(1);
    submit_rendezvous(2);
    assert_int_equal(wait_completed(2), 2);

    pthread_mutex_lock(&g_mu);
    int met = g_rv_met, timed_out = g_rv_timed_out;
    pthread_mutex_unlock(&g_mu);
    assert_int_equal(timed_out, 0);
    assert_int_equal(met, 2);
}

/*
 * A repository with a long queue must not stall an unrelated one behind it.
 * The dispatcher scans past a blocked job rather than waiting on the head.
 *
 * Repo 1's first job will not finish until repo 2's is running, and repo
 * 2's is queued behind two more for repo 1. A dispatcher that waited on the
 * head of the queue would never start it while repo 1's first job held the
 * repository, so the rendezvous would time out (FLAKE-21: this measured
 * overlap by wall time and failed on a loaded host).
 */
static void test_a_busy_repo_does_not_block_another(void **state)
{
    (void)state;
    submit_rendezvous(1);
    submit(1, "gc");
    submit(1, "gc");
    submit_rendezvous(2);   /* queued last, behind three for repo 1 */
    assert_int_equal(wait_completed(4), 4);

    pthread_mutex_lock(&g_mu);
    int met = g_rv_met, timed_out = g_rv_timed_out, peak1 = g_peak[1];
    pthread_mutex_unlock(&g_mu);
    assert_int_equal(timed_out, 0);
    assert_int_equal(met, 2);
    /* And repo 1 still ran one job at a time. */
    assert_int_equal(peak1, 1);
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
    /* The prune runs on the worker after the job function returns and the
     * job's state and journal entry are written, so wait_for alone returns
     * before it. Wait for the old row to go, then check what is left. */
    assert_int_equal(wait_db_count("SELECT COUNT(*) FROM jobs WHERE ended < "
                                   "strftime('%s','now') - 86400", 0), 0);
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
    /* The final count is written before the final state, so a job seen
     * finished must already show it. */
    assert_int_equal(wait_db_count("SELECT COUNT(*) FROM jobs WHERE id = 1"
                                   " AND state = 'completed'", 1), 1);
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
    /* As above: finished means the final report is already recorded. */
    assert_int_equal(wait_db_count("SELECT COUNT(*) FROM jobs WHERE id = 1"
                                   " AND state = 'completed'", 1), 1);
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
    /* Each journal entry is written after its job function returns. */
    assert_int_equal(wait_db_count("SELECT COUNT(*) FROM journal", 2), 2);

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
 * A job the daemon queues itself at start (the reconcile's reindex) says so:
 * origin "startup" on the job, source "startup" in the journal, and the same
 * again if it is interrupted - not "api", which would claim an operator ran
 * it. A negative schedule id from a caller is still an API job.
 */
static void test_startup_jobs_say_startup(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(job_submit_startup(jm, 1, JOB_LOCK_WRITE, NULL, 0, "reindex", NULL, probe_job,
                                        (void *)(intptr_t)1, NULL, &id), OK);
    assert_true(id > 0);
    wait_for(1);
    assert_int_equal(job_submit_scheduled(jm, 1, "gc", NULL, -1, probe_job,
                                          (void *)(intptr_t)1, NULL, &id), OK);
    wait_for(2);
    assert_int_equal(wait_db_count("SELECT COUNT(*) FROM journal", 2), 2);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE source = 'startup'"
                              " AND operation = 'reindex'"), 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE source = 'api'"
                              " AND operation = 'gc'"), 1);
    char origin[32];
    assert_int_equal(db_text("SELECT origin || ':' || COALESCE(schedule_id, 0)"
                             "  FROM jobs ORDER BY id LIMIT 1", origin, sizeof(origin)), 0);
    assert_string_equal(origin, "startup:0");
    assert_int_equal(db_text("SELECT origin || ':' || COALESCE(schedule_id, 0)"
                             "  FROM jobs ORDER BY id DESC LIMIT 1", origin, sizeof(origin)), 0);
    assert_string_equal(origin, "api:0");

    /* Interrupted by a crash, it is journalled as startup at the next start. */
    job_manager_stop(jm); jm = NULL;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO jobs (repo_id, action, state, started, origin)"
        " VALUES (1, 'reindex', 'running', strftime('%s','now'), 'startup')",
        NULL, NULL), 0);
    assert_int_equal(job_manager_start(db, 2, &jm), OK);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE source = 'startup'"
                              " AND message = 'interrupted by daemon restart'"), 1);
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
    /* The third is queued behind the cancelled one, so once it is recorded
     * finished the cancelled one has had every chance to run wrongly. */
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs WHERE id = %lld"
             " AND state = 'completed'", (long long)third);
    assert_int_equal(wait_db_count(q, 1), 1);
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
    /*
     * Runs until cancelled. The bound only stops a broken test hanging for
     * ever, so it is wall-clock and far past any test's own work: it was
     * 2000 sleeps of 5 ms, about 10 s, and under load the 256 submits of
     * test_full_queue_refuses_and_spares_running_jobs took 14 to 37 s, so
     * both jobs gave up, their workers took queued jobs and the submit
     * meant to find the queue full found room (FLAKE-18).
     */
    struct timespec t0, now;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    do {
        if (atomic_load(cancel)) return set_error(ERR_IO, "cancelled");
        usleep(5000);
        clock_gettime(CLOCK_MONOTONIC, &now);
    } while (now.tv_sec - t0.tv_sec < 120);
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
 * B123: a job dispatched while a synchronous request held its repository's
 * lock failed "repo N is busy", failure alert and all. Snapshot delete and
 * repository removal take the repo write lock inline, so an operator's
 * delete could cost a scheduled backup. The test thread stands in for the
 * request, taking the lock the way snapshots_route_delete does.
 */
static int job_waiting(int64_t id)
{
    char q[192];
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs WHERE id = %lld AND state = 'queued'"
             " AND progress_phase = '" JOB_PHASE_WAITING "'", (long long)id);
    for (int i = 0; i < 400; i++) {
        if (db_count(q) == 1) return 1;
        usleep(5000);
    }
    return 0;
}

static int job_state_is(int64_t id, const char *want)
{
    char q[96], st[32];
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)id);
    for (int i = 0; i < 400; i++) {
        if (db_text(q, st, sizeof(st)) == 0 && strcmp(st, want) == 0) return 1;
        usleep(5000);
    }
    return 0;
}

static void test_a_job_waits_for_a_request_holding_the_repo(void **state)
{
    (void)state;
    assert_int_equal(repo_trylock_write(1), OK);
    int64_t id = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, probe_job,
                                (void *)(intptr_t)1, NULL, &id), OK);

    /* Deferred, not failed: still queued, saying why, never started. */
    assert_true(job_waiting(id));
    usleep(3 * JOB_DEFER_MAX_MS * 1000);   /* several retries, all refused */
    pthread_mutex_lock(&g_mu);
    assert_int_equal(g_completed, 0);
    assert_int_equal(g_peak[1], 0);
    pthread_mutex_unlock(&g_mu);
    char q[96], buf[32];
    snprintf(q, sizeof(q), "SELECT COALESCE(started, 'null') FROM jobs WHERE id = %lld",
             (long long)id);
    assert_int_equal(db_text(q, buf, sizeof(buf)), 0);
    assert_string_equal(buf, "null");
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"), 0);

    repo_unlock(1);
    wait_for(1);
    assert_true(job_state_is(id, "completed"));
    snprintf(q, sizeof(q), "SELECT COALESCE(progress_phase, '') FROM jobs WHERE id = %lld",
             (long long)id);
    assert_int_equal(db_text(q, buf, sizeof(buf)), 0);
    assert_string_equal(buf, "");   /* the waiting phase does not outlive the wait */
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE result = 'failed'"), 0);
}

/* And a request still gets its 503 when a job holds the repository: the
 * manager holds the lock for the job's whole run, in the job's mode. */
static void test_a_running_job_still_refuses_requests(void **state)
{
    (void)state;
    g_gate_open = 0;
    int64_t w = 0, r = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, gated_job, NULL, NULL, &w), OK);
    assert_true(job_state_is(w, "running"));
    assert_int_equal(repo_trylock_write(1), ERR_BUSY);
    err_clear();
    assert_int_equal(repo_trylock_read(1), ERR_BUSY);
    err_clear();
    g_gate_open = 1;
    assert_true(job_state_is(w, "completed"));

    /* A read-locked job shares with readers and refuses a writer. */
    g_gate_open = 0;
    assert_int_equal(job_submit_locked(jm, 2, JOB_LOCK_READ, "verify", NULL, 0,
                                       gated_job, NULL, NULL, &r), OK);
    assert_true(job_state_is(r, "running"));
    assert_int_equal(repo_trylock_write(2), ERR_BUSY);
    err_clear();
    assert_int_equal(repo_trylock_read(2), OK);
    repo_unlock(2);
    g_gate_open = 1;
    assert_true(job_state_is(r, "completed"));

    /* A read-locked job runs beside a reader and waits for a writer. */
    g_gate_open = 1;
    assert_int_equal(repo_trylock_read(2), OK);
    assert_int_equal(job_submit_locked(jm, 2, JOB_LOCK_READ, "verify", NULL, 0,
                                       gated_job, NULL, NULL, &r), OK);
    assert_true(job_state_is(r, "completed"));
    repo_unlock(2);
    assert_int_equal(repo_trylock_write(2), OK);
    assert_int_equal(job_submit_locked(jm, 2, JOB_LOCK_READ, "verify", NULL, 0,
                                       gated_job, NULL, NULL, &r), OK);
    assert_true(job_waiting(r));
    repo_unlock(2);
    assert_true(job_state_is(r, "completed"));
}

static void test_a_deferred_job_can_be_cancelled(void **state)
{
    (void)state;
    g_arg_freed = 0;
    assert_int_equal(repo_trylock_write(1), OK);
    int64_t id = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, probe_job,
                                (void *)(intptr_t)1, count_free, &id), OK);
    assert_true(job_waiting(id));

    assert_int_equal(job_cancel(jm, id), OK);
    char q[96], st[32];
    snprintf(q, sizeof(q), "SELECT state FROM jobs WHERE id = %lld", (long long)id);
    assert_int_equal(db_text(q, st, sizeof(st)), 0);
    assert_string_equal(st, "cancelled");
    assert_int_equal(g_arg_freed, 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM journal WHERE result = 'cancelled'"
                              " AND message = 'cancelled before it started'"), 1);

    repo_unlock(1);
    usleep(2 * JOB_DEFER_MAX_MS * 1000);
    pthread_mutex_lock(&g_mu);
    assert_int_equal(g_completed, 0);
    pthread_mutex_unlock(&g_mu);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'"
                              " AND started IS NULL"), 1);
}

/* Shutdown treats a deferred job as the queued job it is. */
static void test_shutdown_cancels_a_deferred_job(void **state)
{
    (void)state;
    assert_int_equal(repo_trylock_write(1), OK);
    int64_t id = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, probe_job,
                                (void *)(intptr_t)1, NULL, &id), OK);
    assert_true(job_waiting(id));
    job_manager_stop(jm);
    jm = NULL;
    repo_unlock(1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'cancelled'"
                              " AND started IS NULL"), 1);
    assert_int_equal(g_completed, 0);
}

static int g_order[16];
static int g_norder;

static status_t ordered_job(void *arg, atomic_int *cancel,
                            void (*progress)(const char *, uint64_t, uint64_t, void *),
                            void *progress_ctx)
{
    (void)cancel; (void)progress; (void)progress_ctx;
    pthread_mutex_lock(&g_mu);
    if (g_norder < 16) g_order[g_norder++] = (int)(intptr_t)arg;
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

/* Jobs queued behind a deferred one stay behind it: the second must not
 * take the lock in the gap between two of the first one's retries. */
static void test_deferred_jobs_keep_their_order(void **state)
{
    (void)state;
    g_norder = 0;
    assert_int_equal(repo_trylock_write(1), OK);
    int64_t first = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, ordered_job,
                                (void *)(intptr_t)0, NULL, &first), OK);
    assert_true(job_waiting(first));
    for (int i = 1; i < 6; i++) {
        int64_t id = 0;
        assert_int_equal(job_submit(jm, 1, "backup", NULL, ordered_job,
                                    (void *)(intptr_t)i, NULL, &id), OK);
    }
    /* Release in the middle of the first job's backoff so a later job,
     * if the scan let it, would be the first to see the lock free. */
    usleep((JOB_DEFER_MIN_MS + JOB_DEFER_MIN_MS / 2) * 1000);
    repo_unlock(1);
    wait_for(6);
    pthread_mutex_lock(&g_mu);
    assert_int_equal(g_norder, 6);
    for (int i = 0; i < 6; i++) assert_int_equal(g_order[i], i);
    pthread_mutex_unlock(&g_mu);
}

/* A repository held by a request does not hold up another repository. */
static void test_a_held_repo_does_not_block_another(void **state)
{
    (void)state;
    assert_int_equal(repo_trylock_write(1), OK);
    int64_t a = 0, b = 0;
    assert_int_equal(job_submit(jm, 1, "backup", NULL, probe_job,
                                (void *)(intptr_t)1, NULL, &a), OK);
    assert_int_equal(job_submit(jm, 2, "backup", NULL, probe_job,
                                (void *)(intptr_t)2, NULL, &b), OK);
    assert_true(job_state_is(b, "completed"));
    assert_true(job_waiting(a));
    repo_unlock(1);
    assert_true(job_state_is(a, "completed"));
}

/*
 * The UAT trigger, in-process: backups for two repositories submitted and
 * snapshot-delete-shaped requests (take the write lock, hold it briefly,
 * release; a refusal is the route's 503) fired in a fixed shuffle from
 * twelve threads. Every job must complete and none may fail, within a
 * bounded time.
 */
#define STRESS_JOBS     40
#define STRESS_REQUESTS 40
#define STRESS_THREADS  12

static int         g_stress_ops[STRESS_JOBS + STRESS_REQUESTS];
static atomic_int  g_stress_next;
static atomic_int  g_stress_refused;

static status_t stress_job(void *arg, atomic_int *cancel,
                           void (*progress)(const char *, uint64_t, uint64_t, void *),
                           void *progress_ctx)
{
    (void)cancel; (void)progress; (void)progress_ctx;
    int repo = (int)(intptr_t)arg;
    pthread_mutex_lock(&g_mu);
    g_running[repo]++;
    if (g_running[repo] > g_peak[repo]) g_peak[repo] = g_running[repo];
    pthread_mutex_unlock(&g_mu);
    usleep(2000);
    pthread_mutex_lock(&g_mu);
    g_running[repo]--;
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void *stress_thread(void *arg)
{
    (void)arg;
    for (;;) {
        int n = atomic_fetch_add(&g_stress_next, 1);
        if (n >= STRESS_JOBS + STRESS_REQUESTS) return NULL;
        int op = g_stress_ops[n];
        int64_t repo = 5 + (n % 2);   /* g_running, g_peak: ids 0-7 */
        if (op) {
            int64_t id = 0;
            if (job_submit(jm, repo, "backup", NULL, stress_job,
                           (void *)(intptr_t)repo, NULL, &id) != OK)
                return (void *)1;
        } else if (repo_trylock_write(repo) == OK) {
            usleep(3000);
            repo_unlock(repo);
        } else {
            err_clear();
            g_stress_refused++;
        }
    }
}

static void test_requests_racing_jobs_lose_no_job(void **state)
{
    (void)state;
    for (int i = 0; i < STRESS_JOBS + STRESS_REQUESTS; i++)
        g_stress_ops[i] = i < STRESS_JOBS;
    /* Fixed seed: the same interleaving of submits and requests each run. */
    unsigned seed = 123;
    for (int i = STRESS_JOBS + STRESS_REQUESTS - 1; i > 0; i--) {
        int k = rand_r(&seed) % (i + 1);
        int t = g_stress_ops[i]; g_stress_ops[i] = g_stress_ops[k]; g_stress_ops[k] = t;
    }
    g_stress_next = 0;
    g_stress_refused = 0;

    pthread_t th[STRESS_THREADS];
    for (int i = 0; i < STRESS_THREADS; i++)
        assert_int_equal(pthread_create(&th[i], NULL, stress_thread, NULL), 0);
    for (int i = 0; i < STRESS_THREADS; i++) {
        void *rv = NULL;
        pthread_join(th[i], &rv);
        assert_null(rv);
    }

    /* Bounded: 40 jobs of 2 ms on two workers, plus at most one backoff
     * step per deferral. 20 s is generous on a loaded CI runner. */
    for (int i = 0; i < 800; i++) {
        if (db_count("SELECT COUNT(*) FROM jobs WHERE state IN ('queued','running')") == 0)
            break;
        usleep(25000);
    }
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'completed'"),
                     STRESS_JOBS);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"), 0);
    pthread_mutex_lock(&g_mu);
    assert_int_equal(g_completed, STRESS_JOBS);
    assert_int_equal(g_peak[5], 1);
    assert_int_equal(g_peak[6], 1);
    pthread_mutex_unlock(&g_mu);
}

/*
 * Lock sets. A replicate into a registered repository holds the destination
 * as well as its source. It used to try-lock the destination itself while
 * already holding the source, and failed outright when it was busy. The
 * dispatcher now takes the whole set or none of it, and a job queued on a
 * set is the head of every member's queue.
 */
static int locked_int(const int *p)
{
    pthread_mutex_lock(&g_mu);
    int v = *p;
    pthread_mutex_unlock(&g_mu);
    return v;
}

static void submit_set(int64_t src, int64_t dst, job_fn fn, void *arg,
                       void (*arg_free)(void *), int64_t *id)
{
    assert_int_equal(job_submit_lockset(jm, src, JOB_LOCK_WRITE, &dst, 1, "replicate",
                                        NULL, 0, fn, arg, arg_free, id), OK);
}

/* Runs as both repositories of a set: arg is src * 8 + dst. */
static status_t set_job(void *arg, atomic_int *cancel,
                        void (*progress)(const char *, uint64_t, uint64_t, void *),
                        void *progress_ctx)
{
    (void)cancel; (void)progress; (void)progress_ctx;
    int src = (int)(intptr_t)arg / 8, dst = (int)(intptr_t)arg % 8;
    pthread_mutex_lock(&g_mu);
    g_running[src]++;
    g_running[dst]++;
    if (g_running[src] > g_peak[src]) g_peak[src] = g_running[src];
    if (g_running[dst] > g_peak[dst]) g_peak[dst] = g_running[dst];
    pthread_mutex_unlock(&g_mu);
    usleep(50000);
    pthread_mutex_lock(&g_mu);
    g_running[src]--;
    g_running[dst]--;
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void test_lock_set_is_bounded_and_deduplicated(void **state)
{
    (void)state;
    g_arg_freed = 0;
    int64_t id = 0, also[2] = { 2, 3 }, self = 1;
    assert_int_equal(job_submit_lockset(jm, 1, JOB_LOCK_WRITE, also, 2, "replicate",
                                        NULL, 0, set_job, (void *)(intptr_t)(1 * 8 + 2),
                                        count_free, &id), ERR_INVALID);
    err_clear();
    assert_int_equal(g_arg_freed, 1);
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs"), 0);
    /* Itself, or nothing, is a set of one. */
    assert_int_equal(job_submit_lockset(jm, 1, JOB_LOCK_WRITE, &self, 1, "replicate",
                                        NULL, 0, probe_job, (void *)(intptr_t)1,
                                        NULL, &id), OK);
    wait_for(1);
    assert_true(job_state_is(id, "completed"));
}

/* A sync request (the 503 path) holding a member defers the job, and the
 * job holds nothing meanwhile: the free member stays takeable. */
static void test_a_held_set_member_defers_and_holds_nothing(void **state)
{
    (void)state;
    assert_int_equal(repo_trylock_write(2), OK);
    int64_t id = 0;
    submit_set(1, 2, set_job, (void *)(intptr_t)(1 * 8 + 2), NULL, &id);
    assert_true(job_waiting(id));
    for (int i = 0; i < 5; i++) {   /* across several retries */
        assert_int_equal(repo_trylock_write(1), OK);
        repo_unlock(1);
        usleep(JOB_DEFER_MAX_MS * 1000 / 2);
    }
    assert_int_equal(db_count("SELECT COUNT(*) FROM jobs WHERE state = 'failed'"), 0);
    repo_unlock(2);
    assert_true(job_state_is(id, "completed"));

    /* Same with the source held instead. */
    assert_int_equal(repo_trylock_write(1), OK);
    submit_set(1, 2, set_job, (void *)(intptr_t)(1 * 8 + 2), NULL, &id);
    assert_true(job_waiting(id));
    assert_int_equal(repo_trylock_write(2), OK);
    repo_unlock(2);
    repo_unlock(1);
    assert_true(job_state_is(id, "completed"));
}

static void test_a_job_waiting_on_a_set_can_be_cancelled(void **state)
{
    (void)state;
    g_arg_freed = 0;
    assert_int_equal(repo_trylock_write(2), OK);
    int64_t id = 0;
    submit_set(1, 2, set_job, (void *)(intptr_t)(1 * 8 + 2), count_free, &id);
    assert_true(job_waiting(id));
    assert_int_equal(job_cancel(jm, id), OK);
    assert_true(job_state_is(id, "cancelled"));
    assert_int_equal(g_arg_freed, 1);
    repo_unlock(2);
    usleep(2 * JOB_DEFER_MAX_MS * 1000);
    assert_int_equal(locked_int(&g_completed), 0);
    /* Neither member was left locked. */
    assert_int_equal(repo_trylock_write(1), OK);
    repo_unlock(1);
    assert_int_equal(repo_trylock_write(2), OK);
    repo_unlock(2);
}

/* A->B beside B->A: no hold-and-wait, so no deadlock, and never both at
 * once. A request on A makes the first wait, so the second is queued
 * against a set that is half free. */
static void test_crossed_sets_both_complete(void **state)
{
    (void)state;
    assert_int_equal(repo_trylock_write(1), OK);
    int64_t ab = 0, ba = 0;
    submit_set(1, 2, set_job, (void *)(intptr_t)(1 * 8 + 2), NULL, &ab);
    submit_set(2, 1, set_job, (void *)(intptr_t)(2 * 8 + 1), NULL, &ba);
    assert_true(job_waiting(ab));
    usleep(JOB_DEFER_MAX_MS * 1000);
    /* FIFO on both repos: B->A must not run while A->B, ahead of it on B,
     * waits for A. */
    assert_int_equal(locked_int(&g_completed), 0);
    repo_unlock(1);
    wait_for(2);
    assert_true(job_state_is(ab, "completed"));
    assert_true(job_state_is(ba, "completed"));
    pthread_mutex_lock(&g_mu);
    assert_int_equal(g_peak[1], 1);
    assert_int_equal(g_peak[2], 1);
    pthread_mutex_unlock(&g_mu);

    /* And submitted together with nothing held. */
    int64_t ids[6];
    for (int i = 0; i < 6; i++)
        submit_set(i % 2 ? 2 : 1, i % 2 ? 1 : 2, set_job,
                   (void *)(intptr_t)(i % 2 ? 2 * 8 + 1 : 1 * 8 + 2), NULL, &ids[i]);
    wait_for(8);
    for (int i = 0; i < 6; i++) assert_true(job_state_is(ids[i], "completed"));
    assert_int_equal(locked_int(&g_peak[1]), 1);
    assert_int_equal(locked_int(&g_peak[2]), 1);
}

/*
 * Starvation: backups on dst submitted continuously, so dst always has one
 * running and more queued, while a replicate into dst waits. A request on
 * the source makes it wait through several backoff steps first, which is
 * exactly when later dst jobs would slip past a job that counted only on
 * its own repository. It must run within a bound, and no dst job submitted
 * after it may start before it.
 */
static atomic_int g_feed_stop;
static atomic_int g_feed_seq;
static int g_starts[512];
static int g_nstarts;
#define SET_MARK 100000

static status_t feed_job(void *arg, atomic_int *cancel,
                         void (*progress)(const char *, uint64_t, uint64_t, void *),
                         void *progress_ctx)
{
    (void)cancel; (void)progress; (void)progress_ctx;
    pthread_mutex_lock(&g_mu);
    if (g_nstarts < 512) g_starts[g_nstarts++] = (int)(intptr_t)arg;
    pthread_mutex_unlock(&g_mu);
    usleep(10000);
    pthread_mutex_lock(&g_mu);
    g_completed++;
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static void *feeder(void *arg)
{
    (void)arg;
    while (!g_feed_stop) {
        /* Keep a few dst jobs outstanding: always one to run next. */
        if (db_count("SELECT COUNT(*) FROM jobs WHERE repo_id = 2"
                     " AND state IN ('queued','running')") < 4) {
            int seq = ++g_feed_seq;
            int64_t id = 0;
            if (job_submit(jm, 2, "backup", NULL, feed_job, (void *)(intptr_t)seq,
                           NULL, &id) != OK)
                return (void *)1;
        }
        usleep(3000);
    }
    return NULL;
}

static void test_a_stream_on_dst_cannot_starve_a_set(void **state)
{
    (void)state;
    g_feed_stop = 0;
    g_feed_seq = 0;
    g_nstarts = 0;
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, feeder, NULL), 0);
    while (g_feed_seq < 10) usleep(5000);

    assert_int_equal(repo_trylock_write(1), OK);
    int64_t id = 0;
    int seq_at_submit = g_feed_seq;
    submit_set(1, 2, feed_job, (void *)(intptr_t)SET_MARK, NULL, &id);
    assert_true(job_waiting(id));
    usleep(3 * JOB_DEFER_MAX_MS * 1000);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    repo_unlock(1);

    /* Bound: the dst jobs queued ahead of it (at most 4 of 10 ms) and one
     * backoff step. 3 s is generous on a loaded runner. */
    assert_true(job_state_is(id, "completed"));
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double waited = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    g_feed_stop = 1;
    void *rv = NULL;
    pthread_join(th, &rv);
    assert_null(rv);
    assert_true(waited < 3.0);

    pthread_mutex_lock(&g_mu);
    int pos = -1;
    for (int i = 0; i < g_nstarts; i++) if (g_starts[i] == SET_MARK) { pos = i; break; }
    assert_true(pos >= 0);
    for (int i = 0; i < pos; i++) assert_true(g_starts[i] <= seq_at_submit + 1);
    pthread_mutex_unlock(&g_mu);
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

/* Run one clean and one warned job with alerts on or off, and return what
 * the log said about notifications. */
static void run_warned_pair(int alerts_on, int64_t *clean, int64_t *warned,
                            char *got, size_t got_sz)
{
    job_manager_set_alerts(jm, alerts_on);
    g_alert_log[0] = '\0';
    int h = log_stream_register(capture_alert, NULL);
    assert_true(h >= 0);

    assert_int_equal(job_submit(jm, 3, "backup", NULL, warning_job, NULL, NULL,
                                clean), OK);
    wait_ended(*clean);
    assert_int_equal(job_submit(jm, 3, "backup", NULL, warning_job,
                                "1 file(s) could not be read; first: /d/x",
                                NULL, warned), OK);
    wait_ended(*warned);

    /* The line follows the completed state, so it is waited for. */
    char want[96];
    snprintf(want, sizeof(want), "warning notification for job %lld",
             (long long)*warned);
    int seen = 0;
    for (int i = 0; i < 200 && !seen; i++) {
        pthread_mutex_lock(&g_alert_mu);
        seen = strstr(g_alert_log, want) != NULL;
        pthread_mutex_unlock(&g_alert_mu);
        if (!seen) usleep(10000);
    }
    log_stream_unregister(h);
    pthread_mutex_lock(&g_alert_mu);
    snprintf(got, got_sz, "%s", g_alert_log);
    pthread_mutex_unlock(&g_alert_mu);
    job_manager_set_alerts(jm, 0);
    if (!seen) fail_msg("no warning alert line for job %lld:\n%s", (long long)*warned, got);

    assert_non_null(strstr(got, "c-backup: backup of docs completed with a warning"));
    assert_non_null(strstr(got, "1 file(s) could not be read; first: /d/x"));
    assert_null(strstr(got, "failure notification"));
    /* The clean job alerts nobody either way. */
    snprintf(want, sizeof(want), "notification for job %lld", (long long)*clean);
    assert_null(strstr(got, want));
}

static void test_a_warning_is_alerted_and_the_job_completes(void **state)
{
    (void)state;
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id, uuid, path, label, added_at)"
        " VALUES (3, 'u-3', '/srv/repos/docs', 'docs', 0)", NULL, NULL), 0);
    char got[sizeof(g_alert_log)], want[128];
    int64_t clean = 0, warned = 0;

    /* With Pushover configured: sent, at warning level. */
    run_warned_pair(1, &clean, &warned, got, sizeof(got));
    snprintf(want, sizeof(want), "warning sending warning notification for job %lld:",
             (long long)warned);
    assert_non_null(strstr(got, want));

    /* Without: still logged at warning, and never called "sending"
     * (NIT-11's rule for the failure alert). */
    run_warned_pair(0, &clean, &warned, got, sizeof(got));
    snprintf(want, sizeof(want), "warning warning notification for job %lld not sent, "
             "Pushover is not configured: ", (long long)warned);
    assert_non_null(strstr(got, want));
    assert_null(strstr(got, "sending warning notification"));
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
        cmocka_unit_test_setup_teardown(test_startup_jobs_say_startup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_job_ids_survive_concurrent_inserts, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancelling_a_queued_job_is_immediate, setup, teardown),
        cmocka_unit_test_setup_teardown(test_shutdown_never_starts_a_queued_job, setup, teardown),
        cmocka_unit_test_setup_teardown(test_full_queue_refuses_and_spares_running_jobs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_alert_names_repository_and_error, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_warning_is_alerted_and_the_job_completes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_warning_alert_text, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_job_waits_for_a_request_holding_the_repo, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_running_job_still_refuses_requests, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_deferred_job_can_be_cancelled, setup, teardown),
        cmocka_unit_test_setup_teardown(test_shutdown_cancels_a_deferred_job, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deferred_jobs_keep_their_order, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_held_repo_does_not_block_another, setup, teardown),
        cmocka_unit_test_setup_teardown(test_requests_racing_jobs_lose_no_job, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lock_set_is_bounded_and_deduplicated, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_held_set_member_defers_and_holds_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_job_waiting_on_a_set_can_be_cancelled, setup, teardown),
        cmocka_unit_test_setup_teardown(test_crossed_sets_both_complete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_stream_on_dst_cannot_starve_a_set, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
