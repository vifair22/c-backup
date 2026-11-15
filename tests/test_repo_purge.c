#define _POSIX_C_SOURCE 200809L

/*
 * Removing a repository deletes its index rows.
 *
 * It used to delete the repos row alone: every child table declares
 * ON DELETE CASCADE, but foreign keys are never enforced, so a removed
 * repository's snapshots, nodes, objects, schedules and journal stayed in
 * the database for good. These tests back up for real, remove, purge, and
 * then look in every table that carries a repo_id.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "repo.h"
#include "repo_purge.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_purge"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_SRC  TEST_DIR "/src"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0 || mkdir(TEST_SRC, 0700) != 0) return -1;
    write_file(TEST_SRC "/a.txt", "alpha");
    write_file(TEST_SRC "/b.txt", "beta");
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

/* Register a repository and back TEST_SRC up into it twice. */
static int64_t repo_with_history(const char *name)
{
    char path[256];
    snprintf(path, sizeof(path), TEST_DIR "/%s", name);
    int64_t id = 0;
    assert_int_equal(repo_register(db, path, name, &id), OK);
    store_t *store = NULL;
    store_index_t *idx = NULL;
    assert_int_equal(store_open(path, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, id, &idx), OK);
    static const char *paths[] = { TEST_SRC };
    for (int i = 0; i < 2; i++) {
        backup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.paths = paths;
        o.n_paths = 1;
        backup_result_t r;
        assert_int_equal(backup_run(store, db, id, &o, &r), OK);
    }
    store_index_free(idx);
    store_close(store);

    /* The tables a backup does not fill. */
    char rid[32];
    snprintf(rid, sizeof(rid), "%lld", (long long)id);
    static const char *const extra[] = {
        "INSERT INTO tags (repo_id, name, snap_id, created_at) VALUES (?, 'keep', 1, 0)",
        "INSERT INTO notes (repo_id, snap_id, body, updated_at) VALUES (?, 1, 'n', 0)",
        "INSERT INTO policy_values (repo_id, key, value) VALUES (?, 'retention.keep_daily', '3')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result, duration_ms, message)"
        " VALUES (?, 1, 0, 'api', 'backup', 'ok', 0, '')",
        "INSERT INTO repo_backup_paths (repo_id, kind, value) VALUES (?, 'source', '/x')",
        "INSERT INTO grep_runs (repo_id, snap_id, pattern, started_at) VALUES (?, 1, 'a', 0)",
        "INSERT INTO verify_runs (repo_id, started_at, level) VALUES (?, 0, 'index')",
        NULL
    };
    for (int i = 0; extra[i]; i++) {
        int rc = db_execute_non_query(db, extra[i], (const char *[]){ rid, NULL }, NULL);
        if (rc != 0) printf("insert failed: %s\n", extra[i]);
        assert_int_equal(rc, 0);
    }
    return id;
}

/* Rows for this repository in every table that has a repo_id column. */
static long rows_for(int64_t id)
{
    db_result_t *t = NULL;
    assert_int_equal(db_execute(db,
        "SELECT m.name FROM sqlite_master m, pragma_table_info(m.name) p"
        " WHERE m.type = 'table' AND p.name = 'repo_id'"
        "   AND m.name NOT IN ('repos', 'repo_purge_pending')"
        "   AND m.name NOT LIKE 'node_fts_%'",
        NULL, &t), 0);
    char rid[32];
    snprintf(rid, sizeof(rid), "%lld", (long long)id);
    long total = 0;
    for (int i = 0; i < t->nrows; i++) {
        char sql[256];
        snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s WHERE repo_id = ?", t->rows[i][0]);
        db_result_t *c = NULL;
        assert_int_equal(db_execute(db, sql, (const char *[]){ rid, NULL }, &c), 0);
        long n = atol(c->rows[0][0]);
        if (n) printf("  %s: %ld rows for repo %s\n", t->rows[i][0], n, rid);
        total += n;
        db_result_free(c);
    }
    db_result_free(t);
    return total;
}

static long pending(void)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM repo_purge_pending", NULL, &r), 0);
    long n = atol(r->rows[0][0]);
    db_result_free(r);
    return n;
}

static void test_remove_deletes_every_row_and_spares_the_rest(void **state)
{
    (void)state;
    int64_t gone = repo_with_history("gone");
    int64_t kept = repo_with_history("kept");
    long kept_before = rows_for(kept);
    assert_true(rows_for(gone) > 0);

    assert_int_equal(repo_deregister(db, gone), OK);
    assert_int_equal(pending(), 1);
    assert_int_equal(repo_purge_run(db, NULL), OK);

    assert_int_equal(rows_for(gone), 0);
    assert_int_equal(pending(), 0);
    assert_int_equal(rows_for(kept), kept_before);
}

/* A stop between batches leaves the job queued, and the next run finishes. */
static void test_a_stopped_purge_resumes(void **state)
{
    (void)state;
    int64_t gone = repo_with_history("gone");
    assert_int_equal(repo_deregister(db, gone), OK);

    volatile int stop = 1;
    assert_int_equal(repo_purge_run(db, &stop), ERR_BUSY);
    assert_int_equal(pending(), 1);
    assert_true(rows_for(gone) > 0);

    assert_int_equal(repo_purge_run(db, NULL), OK);
    assert_int_equal(rows_for(gone), 0);
}

/* The thread drains what is queued when it starts, and what is kicked. */
static void test_the_thread_purges_in_the_background(void **state)
{
    (void)state;
    int64_t first = repo_with_history("first");
    int64_t second = repo_with_history("second");
    assert_int_equal(repo_deregister(db, first), OK);

    assert_int_equal(repo_purge_start(db), OK);
    assert_int_equal(repo_deregister(db, second), OK);
    repo_purge_kick();
    for (int i = 0; i < 200 && pending() > 0; i++) {
        struct timespec ts = { 0, 10 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    repo_purge_stop();
    assert_int_equal(pending(), 0);
    assert_int_equal(rows_for(first), 0);
    assert_int_equal(rows_for(second), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_remove_deletes_every_row_and_spares_the_rest, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_stopped_purge_resumes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_thread_purges_in_the_background, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
