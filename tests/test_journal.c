#define _POSIX_C_SOURCE 200809L

/*
 * Journal entries written concurrently for one repository all land.
 *
 * OWN-18: journal_record read MAX(id) + 1 and inserted in a second
 * statement. Two threads journalling for the same repository between those
 * statements took the same id, the second insert failed on the primary key,
 * and that entry was dropped with nothing but a warning in the log. Jobs end
 * on worker threads while routes journal on request threads, so this is the
 * normal shape of a busy daemon, not a contrived one.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "db_txn.h"
#include "routes.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_journal"
#define TEST_DB  TEST_DIR "/state.db"

#define WRITERS    8
#define PER_WRITER 250

extern const db_migration_t app_migrations[];

static cutils_db_t *db;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static long long scalar(const char *sql, const char *p1)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, sql, (const char *[]){ p1, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    long long v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

typedef struct {
    pthread_barrier_t *start;
    int                writer;
    int64_t            repo;
    int                in_txn;   /* journal from inside a db_txn, as some callers do */
} writer_arg_t;

static void *writer_main(void *p)
{
    writer_arg_t *a = p;
    pthread_barrier_wait(a->start);
    for (int i = 0; i < PER_WRITER; i++) {
        char msg[64];
        snprintf(msg, sizeof(msg), "w%d-%d", a->writer, i);
        int txn = a->in_txn && db_txn_begin(db) == OK;
        journal_record(db, a->repo, "api", "test", "success", 0, msg);
        if (txn) db_txn_commit(db);
    }
    return NULL;
}

static void run_writers(const int64_t *repos, int in_txn_every)
{
    pthread_t th[WRITERS];
    writer_arg_t args[WRITERS];
    pthread_barrier_t start;
    pthread_barrier_init(&start, NULL, WRITERS);
    for (int i = 0; i < WRITERS; i++) {
        args[i] = (writer_arg_t){ &start, i, repos[i % 2],
                                  in_txn_every && i % in_txn_every == 0 };
        assert_int_equal(pthread_create(&th[i], NULL, writer_main, &args[i]), 0);
    }
    for (int i = 0; i < WRITERS; i++) pthread_join(th[i], NULL);
    pthread_barrier_destroy(&start);
}

/* Every entry for one repository, with ids 1..N and no gaps or repeats. */
static void assert_complete(const char *repo, long long n)
{
    assert_int_equal(scalar("SELECT COUNT(*) FROM journal WHERE repo_id = ?", repo), n);
    assert_int_equal(scalar("SELECT COUNT(DISTINCT message) FROM journal WHERE repo_id = ?",
                            repo), n);
    assert_int_equal(scalar("SELECT MIN(id) FROM journal WHERE repo_id = ?", repo), 1);
    assert_int_equal(scalar("SELECT MAX(id) FROM journal WHERE repo_id = ?", repo), n);
}

static void test_concurrent_writers_on_one_repo_lose_nothing(void **state)
{
    (void)state;
    static const int64_t one[2] = { 1, 1 };
    run_writers(one, 0);
    assert_complete("1", (long long)WRITERS * PER_WRITER);
}

/* Two repositories interleaved: each keeps its own unbroken sequence, and
 * writers that journal from inside a transaction neither deadlock nor lose
 * entries beside the ones that do not. */
static void test_sequences_stay_per_repo_with_transactions_open(void **state)
{
    (void)state;
    static const int64_t two[2] = { 1, 2 };
    run_writers(two, 3);
    assert_complete("1", (long long)WRITERS / 2 * PER_WRITER);
    assert_complete("2", (long long)WRITERS / 2 * PER_WRITER);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_concurrent_writers_on_one_repo_lose_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sequences_stay_per_repo_with_transactions_open, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
