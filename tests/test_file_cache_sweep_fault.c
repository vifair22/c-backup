#define _POSIX_C_SOURCE 200809L

/*
 * B222 R3: the file cache sweep reads the cache a page at a time.
 *
 * It read every row of the repository in one statement on the shared
 * connection: the whole result materialised under the connection's mutex,
 * 100 ms for 50,000 rows and about 1.2 s at 622,000, with every request on
 * that connection waiting, and every path copied into memory at once. The
 * fault wrap records the largest single allocation, which for c-utils'
 * result is the row array: it grows with the rows read in one statement,
 * so it is what says whether the sweep read them all at once.
 *
 * A fault suite only for that wrap; nothing here fails.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "db_txn.h"
#include "fault_inject.h"
#include "file_cache.h"
#include "object.h"
#include "repo.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_fcache_sweep_fault"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define ROWS      20000

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static int64_t      repo_id;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static int setup(void **state)
{
    (void)state;
    fault_reset_all();
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_reset_all();
    db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static int count_rows(void)
{
    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (db_execute(db, "SELECT count(*) FROM file_cache WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0) return -1;
    int n = r && r->nrows == 1 ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return n;
}

static int cmp(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static void test_the_sweep_reads_a_page_at_a_time(void **state)
{
    (void)state;
    static char names[ROWS][48];
    const char **seen = calloc(ROWS, sizeof(*seen));
    assert_non_null(seen);
    size_t n_seen = 0;
    struct stat st;
    memset(&st, 0, sizeof(st));
    uint8_t hash[OBJECT_HASH_SIZE] = { 1 };
    assert_int_equal(db_txn_begin(db), OK);
    for (int i = 0; i < ROWS; i++) {
        /* Odd rows are the ones the run saw; even ones are stale. */
        snprintf(names[i], sizeof(names[i]), "/src/d%03d/f%06d", i % 97, i);
        file_cache_put(db, repo_id, names[i], &st, hash, 1);
        if (i % 2) seen[n_seen++] = names[i];
    }
    assert_int_equal(db_txn_commit(db), OK);
    qsort(seen, n_seen, sizeof(*seen), cmp);
    assert_int_equal(count_rows(), ROWS);

    fault_malloc_peak = 0;
    file_cache_sweep(db, repo_id, seen, n_seen);
    size_t peak = fault_malloc_peak;
    print_message("  largest allocation during a sweep of %d rows: %zu bytes\n", ROWS, peak);

    /* Exactly the stale half went, across every page boundary. */
    assert_int_equal(count_rows(), ROWS / 2);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    for (int i = 0; i < ROWS; i += 997) {
        db_result_t *r = NULL;
        assert_int_equal(db_execute(db, "SELECT 1 FROM file_cache WHERE repo_id = ? AND path = ?",
                                    (const char *[]){ repo, names[i], NULL }, &r), 0);
        assert_int_equal(r ? r->nrows : 0, i % 2);
        db_result_free(r);
    }
    /* One statement's rows at a time: 2,000 row pointers, not 20,000. A
     * sweep that read everything grew one array to 160 KiB. */
    assert_true(peak < 64 * 1024);
    free(seen);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_the_sweep_reads_a_page_at_a_time, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
