#define _POSIX_C_SOURCE 200809L

/*
 * Backup then restore, compared byte for byte.
 *
 * The reindex cases are the interesting half. They wipe the index and rebuild
 * it from the manifest trees alone, then restore from the rebuilt index. If
 * that produces the same files, the claim that the database is derived rather
 * than authoritative is demonstrated rather than asserted.
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
#include "reindex.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_restore"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_DST  TEST_DIR "/dst"

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
    if (mkdir(TEST_SRC "/nested", 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/nested/deeper", 0700) != 0) return -1;
    write_file(TEST_SRC "/top.txt", "top level content\n");
    write_file(TEST_SRC "/nested/mid.txt", "middle content\n");
    write_file(TEST_SRC "/nested/deeper/leaf.txt", "leaf content\n");
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

/* diff -r between the source and the restored copy. Returns 0 when identical. */
static int trees_differ(const char *a, const char *b)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s' >/dev/null 2>&1", a, b);
    return system(cmd);
}

/* The restore lands under dest + the source's absolute path, since the
 * snapshot records ancestors so a restore can recreate the original layout. */
static const char *restored_root(void)
{
    static char buf[512];
    snprintf(buf, sizeof(buf), "%s%s", TEST_DST, TEST_SRC);
    return buf;
}

/* ------------------------------------------------------------------ */

static void test_roundtrip_is_identical(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.files, 3);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);
}

static void test_restore_preserves_modes(void **state)
{
    (void)state;
    assert_int_equal(chmod(TEST_SRC "/top.txt", 0640), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.overwrite = 1;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, NULL), OK);

    char path[512];
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0640);
}

static void test_restore_recreates_directory_structure(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, NULL), OK);

    char path[512];
    struct stat sb;
    snprintf(path, sizeof(path), "%s/nested/deeper", restored_root());
    assert_int_equal(stat(path, &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));
}

/* An older snapshot must restore its own content, not the newest. */
static void test_restores_the_requested_snapshot(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;
    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);

    write_file(TEST_SRC "/top.txt", "REPLACED\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    assert_int_equal(restore_run(store, db, repo_id, first.snapshot_id,
                                 TEST_DST, NULL, NULL), OK);

    char path[512];
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    FILE *f = fopen(path, "r");
    assert_non_null(f);
    char line[64] = { 0 };
    assert_non_null(fgets(line, sizeof(line), f));
    fclose(f);
    assert_string_equal(line, "top level content\n");
}

static void test_restore_of_unknown_snapshot_fails(void **state)
{
    (void)state;
    assert_int_not_equal(restore_run(store, db, repo_id, 999, TEST_DST, NULL, NULL), OK);
}

/* ---------- reindex ---------- */

/*
 * Wipe every derived row and rebuild from the trees in the repository. This
 * is the disaster-recovery path: if it works, the database really is an index
 * rather than the only copy.
 */
static void test_reindex_rebuilds_the_index(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    int before = atoi(r->rows[0][0]);
    db_result_free(r);
    assert_true(before > 0);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);
    assert_true(rx.trees_read > 0);

    r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), before);
    db_result_free(r);
}

/*
 * A rebuilt snapshot must describe itself the same way a written one does.
 * file_count was taking the node total, so a rebuilt snapshot claimed every
 * directory as a file and the count silently changed under the user the first
 * time a repository was recovered.
 */
static void test_reindex_preserves_the_file_count(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT file_count FROM snapshots WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    int written = atoi(r->rows[0][0]);
    db_result_free(r);
    assert_int_equal(written, (int)br.files);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT file_count FROM snapshots WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), written);
    db_result_free(r);

    /* Directories were counted too, so the bug showed as rebuilt > written. */
    assert_true(rx.nodes > rx.files);
}

/* And the rebuilt index must be good enough to restore from. */
static void test_restore_after_reindex_is_identical(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.files, 3);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);
}

/* Reindexing twice must not duplicate rows. */
static void test_reindex_is_idempotent(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    reindex_result_t a, b;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &a), OK);
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &b), OK);
    assert_int_equal(a.nodes, b.nodes);

    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM snapshots WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 1);
    db_result_free(r);
}

static void test_reindex_handles_multiple_snapshots(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    write_file(TEST_SRC "/top.txt", "v2\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 2);

    /* Each snapshot keeps its own nodes, and node ids stay unique per repo. */
    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db,
        "SELECT COUNT(DISTINCT snap_id), COUNT(*), COUNT(DISTINCT id)"
        " FROM nodes WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 2);
    assert_int_equal(atoi(r->rows[0][1]), atoi(r->rows[0][2]));
    db_result_free(r);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_roundtrip_is_identical, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_preserves_modes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_recreates_directory_structure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restores_the_requested_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_of_unknown_snapshot_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_rebuilds_the_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_preserves_the_file_count, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_after_reindex_is_identical, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_is_idempotent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_handles_multiple_snapshots, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
