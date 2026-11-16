#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * The daemon-wide read budget (src/common/read_budget.h).
 *
 * One-shot AEAD holds a whole object until its tag verifies, so object
 * buffers in flight are capped across every job. Two things matter and both
 * are held to here: the cap holds under any concurrency, and nothing
 * deadlocks when it is as small as it may be - one largest object - with
 * every kind of reader running at once. The latter is the reason no reader
 * acquires while holding: a nested acquire would be counted, and the count
 * is asserted to stay at zero.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "grep.h"
#include "read_budget.h"
#include "replicate.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_read_budget"
#define TEST_DB    TEST_DIR "/state.db"
#define REPO_A     TEST_DIR "/repo_a"
#define REPO_B     TEST_DIR "/repo_b"
#define SRC_A      TEST_DIR "/src_a"
#define SRC_B      TEST_DIR "/src_b"
#define REPL       TEST_DIR "/replica"

#define MiB ((uint64_t)1024 * 1024)
/* The smallest budget the daemon accepts; every semaphore test runs at it. */
#define BUDGET READ_BUDGET_MIN

extern const db_migration_t app_migrations[];

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

/* ------------------------------------------------------------------ */
/* The semaphore                                                       */
/* ------------------------------------------------------------------ */

static void test_floor_is_refused(void **state)
{
    (void)state;
    assert_int_equal(read_budget_init(READ_BUDGET_MIN - 1), ERR_INVALID);
    assert_int_equal(read_budget_init(READ_BUDGET_MIN), OK);
    assert_int_equal(read_budget_init(0), OK);
}

static void test_oversized_request_is_clamped(void **state)
{
    (void)state;
    assert_int_equal(read_budget_init(BUDGET), OK);
    uint64_t t = read_budget_acquire(BUDGET + 24 * MiB);
    assert_true(t == BUDGET);
    read_budget_release(t);
    assert_true(read_budget_in_use() == 0);
    read_budget_init(0);
}

/* A holder asking for more is the deadlock this exists to prevent: it is
 * not made to wait, and it is counted. */
static void test_nested_acquire_never_waits(void **state)
{
    (void)state;
    assert_int_equal(read_budget_init(BUDGET), OK);
    read_budget_reset_stats();
    uint64_t a = read_budget_acquire(BUDGET);
    uint64_t b = read_budget_acquire(8 * MiB);
    assert_true(read_budget_nested() == 1);
    read_budget_release(b);
    read_budget_release(a);
    assert_true(read_budget_in_use() == 0);
    read_budget_init(0);
}

typedef struct { unsigned seed; int rounds; } churn_t;

static void *churn(void *arg)
{
    churn_t *c = arg;
    for (int i = 0; i < c->rounds; i++) {
        c->seed = c->seed * 1103515245u + 12345u;
        uint64_t n = 1 + (c->seed >> 8) % (BUDGET / 2);
        uint64_t t = read_budget_acquire(n);
        struct timespec ts = { 0, 20000 };
        nanosleep(&ts, NULL);
        read_budget_release(t);
    }
    return NULL;
}

static void test_cap_holds_under_contention(void **state)
{
    (void)state;
    assert_int_equal(read_budget_init(BUDGET), OK);
    read_budget_reset_stats();
    enum { N = 16 };
    pthread_t th[N];
    churn_t c[N];
    for (int i = 0; i < N; i++) {
        c[i].seed = (unsigned)i + 1;
        c[i].rounds = 300;
        assert_int_equal(pthread_create(&th[i], NULL, churn, &c[i]), 0);
    }
    for (int i = 0; i < N; i++) pthread_join(th[i], NULL);
    printf("peak %llu of %llu\n", (unsigned long long)read_budget_peak(),
           (unsigned long long)BUDGET);
    assert_true(read_budget_peak() <= BUDGET);
    assert_true(read_budget_peak() > BUDGET / 2);
    assert_true(read_budget_nested() == 0);
    assert_true(read_budget_in_use() == 0);
    read_budget_init(0);
}

/* A slot handed to another thread is given back there. */
static void *finish_foreign(void *arg)
{
    read_budget_release_foreign(*(uint64_t *)arg);
    return NULL;
}

static void test_handoff(void **state)
{
    (void)state;
    assert_int_equal(read_budget_init(BUDGET), OK);
    read_budget_reset_stats();
    uint64_t t = read_budget_acquire(BUDGET * 5 / 8);
    read_budget_handoff(t);
    /* This thread holds nothing now, so it waits like anyone else. */
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, finish_foreign, &t), 0);
    uint64_t u = read_budget_acquire(BUDGET * 5 / 8);
    pthread_join(th, NULL);
    read_budget_release(u);
    assert_true(read_budget_nested() == 0);
    assert_true(read_budget_in_use() == 0);
    read_budget_init(0);
}

/* ------------------------------------------------------------------ */
/* Every reader at once, on one object's worth of budget                */
/* ------------------------------------------------------------------ */

static cutils_db_t *db;
static int64_t id_a, id_b;
static int64_t snap_a;

static void write_random(const char *path, uint64_t n, uint64_t seed)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    uint64_t x = seed;
    uint8_t buf[65536];
    for (uint64_t done = 0; done < n; ) {
        for (size_t i = 0; i < sizeof(buf); i += 8) {
            x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
            uint64_t v = x * 0x2545F4914F6CDD1DULL;
            memcpy(buf + i, &v, 8);
        }
        size_t k = n - done < sizeof(buf) ? (size_t)(n - done) : sizeof(buf);
        assert_int_equal(fwrite(buf, 1, k, f), k);
        done += k;
    }
    fclose(f);
}

static void write_text(const char *path, const char *s)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(s, f);
    fclose(f);
}

static store_t *open_store(const char *path, int64_t id, store_index_t **idx)
{
    store_t *st = NULL;
    if (repo_store_open(path, NULL, &st) != OK) return NULL;
    if (store_index_bind(st, db, id, idx) != OK) { store_close(st); return NULL; }
    return st;
}

static backup_result_t backup_into(const char *repo, int64_t id, const char *src,
                                   unsigned workers)
{
    store_index_t *idx = NULL;
    store_t *st = open_store(repo, id, &idx);
    assert_non_null(st);
    const char *paths[] = { src };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(st, db, id, &o, &r), OK);
    store_index_free(idx);
    store_close(st);
    return r;
}

static int setup_jobs(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    { int rc = system("mkdir -p '" TEST_DIR "'"); (void)rc; }
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO_A, "a", &id_a) != OK) return -1;
    if (repo_register(db, REPO_B, "b", &id_b) != OK) return -1;
    if (mkdir(SRC_A, 0700) != 0 || mkdir(SRC_B, 0700) != 0) return -1;
    return 0;
}

static int teardown_jobs(void **state)
{
    (void)state;
    read_budget_init(0);
    db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

typedef struct {
    const char *what;
    status_t    st;
    char        err[ERR_MSG_MAX];
} job_t;

static void job_done(job_t *j, status_t st)
{
    j->st = st;
    if (st != OK) snprintf(j->err, sizeof(j->err), "%s", err_msg());
}

static void *job_restore(void *arg)
{
    job_t *j = arg;
    store_index_t *idx = NULL;
    store_t *st = open_store(REPO_A, id_a, &idx);
    char out[PATH_MAX];
    snprintf(out, sizeof(out), "%s/out_%s", TEST_DIR, j->what);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    job_done(j, restore_run(st, db, id_a, snap_a, out, NULL, &rr));
    store_index_free(idx);
    store_close(st);
    return NULL;
}

static void *job_verify(void *arg)
{
    job_t *j = arg;
    store_index_t *idx = NULL;
    store_t *st = open_store(REPO_A, id_a, &idx);
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    job_done(j, verify_run(st, db, id_a, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr));
    store_index_free(idx);
    store_close(st);
    return NULL;
}

static void *job_grep(void *arg)
{
    job_t *j = arg;
    store_index_t *idx = NULL;
    store_t *st = open_store(REPO_A, id_a, &idx);
    grep_opts_t o;
    memset(&o, 0, sizeof(o));
    o.pattern = "needle";
    o.max_hits_per_file = 10;
    grep_result_t gr;
    memset(&gr, 0, sizeof(gr));
    job_done(j, grep_run(st, db, id_a, snap_a, &o, NULL, NULL, NULL, &gr));
    store_index_free(idx);
    store_close(st);
    return NULL;
}

static void *job_replicate(void *arg)
{
    job_t *j = arg;
    store_index_t *idx = NULL;
    store_t *st = open_store(REPO_A, id_a, &idx);
    repo_marker_t m;
    status_t s = repo_marker_read(REPO_A, &m);
    replica_sink_t sink;
    if (s == OK) s = replica_sink_local_open(REPL, &m, &sink);
    replicate_result_t rr;
    if (s == OK) {
        s = replicate_run(st, db, id_a, &sink, NULL, NULL, NULL, &rr);
        sink.v->close(sink.ctx);
    }
    job_done(j, s);
    store_index_free(idx);
    store_close(st);
    return NULL;
}

static void *job_backup(void *arg)
{
    job_t *j = arg;
    store_index_t *idx = NULL;
    store_t *st = open_store(REPO_B, id_b, &idx);
    const char *paths[] = { SRC_B };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 4;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    job_done(j, backup_run(st, db, id_b, &o, &r));
    store_index_free(idx);
    store_close(st);
    return NULL;
}

/*
 * The smallest budget - one largest object and one pack writer - and
 * restore twice, verify, grep, replicate and a pooled backup all running at
 * once over chunked files.
 * Every one finishes - the alarm fails the suite if anything deadlocks -
 * the peak never passes the budget, and nobody acquired while holding.
 */
static void test_every_reader_finishes_on_one_object(void **state)
{
    (void)state;
    write_random(SRC_A "/big.bin", 30 * MiB, 7);
    write_random(SRC_A "/mid.bin", 5 * MiB, 8);
    write_text(SRC_A "/needle.txt", "a needle in here\n");
    write_random(SRC_B "/big.bin", 40 * MiB, 9);
    write_text(SRC_B "/small.txt", "small\n");
    snap_a = backup_into(REPO_A, id_a, SRC_A, 4).snapshot_id;

    assert_int_equal(read_budget_init(READ_BUDGET_MIN), OK);
    read_budget_reset_stats();
    alarm(600);

    job_t jobs[] = {
        { .what = "restore1" }, { .what = "restore2" }, { .what = "verify" },
        { .what = "grep" }, { .what = "replicate" }, { .what = "backup" },
    };
    void *(*fn[])(void *) = { job_restore, job_restore, job_verify, job_grep,
                              job_replicate, job_backup };
    enum { NJ = sizeof(jobs) / sizeof(jobs[0]) };
    pthread_t th[NJ];
    for (int i = 0; i < NJ; i++)
        assert_int_equal(pthread_create(&th[i], NULL, fn[i], &jobs[i]), 0);
    for (int i = 0; i < NJ; i++) pthread_join(th[i], NULL);
    alarm(0);

    for (int i = 0; i < NJ; i++) {
        if (jobs[i].st != OK)
            fail_msg("%s failed: %s", jobs[i].what, jobs[i].err);
    }
    printf("read budget peak %llu of %llu, nested %llu\n",
           (unsigned long long)read_budget_peak(),
           (unsigned long long)READ_BUDGET_MIN,
           (unsigned long long)read_budget_nested());
    assert_true(read_budget_peak() <= READ_BUDGET_MIN);
    assert_true(read_budget_peak() > 0);
    assert_true(read_budget_nested() == 0);
    assert_true(read_budget_in_use() == 0);

    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s/out_restore1%s' >/dev/null 2>&1",
             SRC_A, TEST_DIR, SRC_A);
    assert_int_equal(system(cmd), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_floor_is_refused),
        cmocka_unit_test(test_oversized_request_is_clamped),
        cmocka_unit_test(test_nested_acquire_never_waits),
        cmocka_unit_test(test_cap_holds_under_contention),
        cmocka_unit_test(test_handoff),
        cmocka_unit_test_setup_teardown(test_every_reader_finishes_on_one_object,
                                        setup_jobs, teardown_jobs),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
