#define _POSIX_C_SOURCE 200809L

/*
 * Backup pipeline, end to end.
 *
 * The assertions that matter are the consistency ones: every node's content
 * hash must exist in objects, and the snapshot's root tree must be walkable
 * back to the files that went in. A backup that "succeeds" while leaving the
 * index disagreeing with the repo is the failure mode worth guarding, because
 * nothing surfaces it until a restore.
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

#include "backup.h"
#include "manifest.h"
#include "object.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_e2e"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

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
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/sub", 0700) != 0) return -1;
    write_file(TEST_SRC "/alpha.txt", "alpha content");
    write_file(TEST_SRC "/beta.txt",  "beta content");
    write_file(TEST_SRC "/sub/gamma.txt", "gamma content");
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static backup_opts_t basic_opts(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    return o;
}

static int scalar(const char *sql, const char *p1)
{
    db_result_t *r = NULL;
    const char *params[] = { p1, NULL };
    if (db_execute(db, sql, p1 ? params : NULL, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/* ------------------------------------------------------------------ */

static void test_backup_runs(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    assert_int_equal(res.snapshot_id, 1);
    assert_int_equal(res.files, 3);
    assert_true(res.dirs >= 2);          /* src and src/sub */
    assert_int_equal(res.objects_new, 3);
    assert_true(res.trees_new > 0);
}

/* The snapshot record on disk is what makes the repo self-describing; if it
 * is missing, the database is the only copy and the design premise is void. */
static void test_snapshot_record_written(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/snapshots/1", &sb), 0);
    assert_true(sb.st_size > 0);

    char line[512];
    FILE *f = fopen(TEST_REPO "/snapshots/1", "r");
    assert_non_null(f);
    assert_non_null(fgets(line, sizeof(line), f));
    fclose(f);
    assert_non_null(strstr(line, "\"root\":"));
}

/* Every content hash referenced by a node must exist in objects. A node
 * pointing at an object that was never recorded is a restore failure that
 * only shows up when someone needs the file back. */
static void test_index_is_internally_consistent(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    int orphans = scalar(
        "SELECT COUNT(*) FROM nodes n"
        " WHERE n.repo_id = ? AND n.content_hash IS NOT NULL"
        "   AND NOT EXISTS (SELECT 1 FROM objects o"
        "                    WHERE o.repo_id = n.repo_id AND o.hash = n.content_hash)",
        repo);
    assert_int_equal(orphans, 0);

    int snaps = scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo);
    assert_int_equal(snaps, 1);

    int rooted = scalar(
        "SELECT COUNT(*) FROM snapshots"
        " WHERE repo_id = ? AND root_tree_hash IS NOT NULL", repo);
    assert_int_equal(rooted, 1);
}

/* The root tree must be readable and lead to the source directory. */
static void test_root_tree_is_walkable(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db,
        "SELECT root_tree_hash FROM snapshots WHERE repo_id = ? AND id = 1",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);

    uint8_t root[OBJECT_HASH_SIZE];
    const char *hex = r->rows[0][0];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        sscanf(hex + i * 2, "%2x", &v);
        root[i] = (uint8_t)v;
    }
    db_result_free(r);

    manifest_entry_t *entries = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, root, &entries, &n), OK);
    assert_true(n >= 1);
    manifest_free_entries(entries, n);
}

/* Filename search has to find what was just backed up, or the trigram index
 * is being populated wrong and nobody notices until someone searches. */
static void test_search_finds_backed_up_files(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT node_id FROM node_fts WHERE name MATCH 'lpha'", NULL, &r), 0);
    assert_true(r->nrows >= 1);
    db_result_free(r);
}

/* Content is deduplicated across files with identical bytes. */
static void test_identical_files_share_one_object(void **state)
{
    (void)state;
    write_file(TEST_SRC "/dup1.txt", "the very same bytes");
    write_file(TEST_SRC "/dup2.txt", "the very same bytes");

    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    assert_int_equal(res.files, 5);
    /* Five files, four distinct contents. */
    assert_int_equal(res.objects_new, 4);
}

static void test_cancellation_stops_the_run(void **state)
{
    (void)state;
    volatile int cancel = 1;
    backup_opts_t o = basic_opts();
    o.cancel = &cancel;
    assert_int_not_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    /* A cancelled run must not leave a snapshot behind. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);
}

static void test_rejects_empty_path_list(void **state)
{
    (void)state;
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    assert_int_not_equal(backup_run(store, db, repo_id, &o, NULL), OK);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_backup_runs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_record_written, setup, teardown),
        cmocka_unit_test_setup_teardown(test_index_is_internally_consistent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_root_tree_is_walkable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_search_finds_backed_up_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_identical_files_share_one_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancellation_stops_the_run, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rejects_empty_path_list, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
