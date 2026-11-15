#define _POSIX_C_SOURCE 200809L

/*
 * Reindex as disaster recovery: what it must refuse, what it must report,
 * and what it must never destroy.
 *
 * test_restore_roundtrip proves a rebuilt index restores. These cases are
 * the other half, found in UAT: a reindex that could read nothing reported
 * success, and one that could read only part of a repository was free to
 * throw away the index rows of the part it could not.
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
#include <sys/stat.h>
#include <limits.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "reindex.h"
#include "repo.h"
#include "repo_key.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_reindex"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_KEYS TEST_DIR "/keys"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo[32];

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

static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static void open_store(void)
{
    assert_int_equal(repo_store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
}

static void close_store(void)
{
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
}

static int setup_common(repo_enc_mode_t mode)
{
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = mode;
    enc.key_dir = TEST_KEYS;
    if (repo_register_ex(db, TEST_REPO, "test", &enc, &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/nested", 0700) != 0) return -1;
    write_file(TEST_SRC "/top.txt", "top level content\n");
    write_file(TEST_SRC "/nested/mid.txt", "middle content\n");
    return 0;
}

static int setup(void **state)     { (void)state; return setup_common(REPO_ENC_NONE); }
static int setup_enc(void **state) { (void)state; return setup_common(REPO_ENC_SYSTEM); }

static int teardown(void **state)
{
    (void)state;
    close_store();
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void backup_once(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
}

/* ---------- B043 ---------- */

/*
 * Without the key every snapshot failed with "pack entry is encrypted and no
 * key is loaded", and the reindex reported success with 0 snapshots. It is
 * refused now, before anything is touched.
 */
static void test_reindex_without_the_key_refuses_and_changes_nothing(void **state)
{
    (void)state;
    backup_once();
    long long snaps = scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?");
    long long nodes = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?");
    long long packed = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    assert_int_equal(snaps, 1);
    assert_true(nodes > 0);
    assert_true(packed > 0);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    close_store();
    assert_int_equal(repo_key_remove(TEST_KEYS, m.uuid), OK);
    open_store();
    assert_true(store_needs_key(store));

    reindex_result_t rx;
    assert_int_not_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_non_null(strstr(err_msg(), "no key is loaded"));
    err_clear();

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), snaps);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?"), nodes);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), packed);
}

/*
 * A snapshot whose tree cannot be read fails the run, and keeps its index
 * rows: it is replaced only in a transaction that commits on success.
 */
static void test_unreadable_snapshot_fails_the_run_and_keeps_its_rows(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_SRC "/top.txt", "second version\n");
    backup_once();
    long long nodes2 = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 2");
    assert_true(nodes2 > 0);

    /* Snapshot 2's record now names a tree that does not exist. */
    write_file(TEST_REPO "/snapshots/2",
               "{\"id\":2,\"ts\":1,\"root\":\""
               "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
               "\",\"label\":\"\"}\n");

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "1 of 2 snapshot(s) could not be read"));
    err_clear();
    assert_int_equal(rx.snapshots, 1);
    assert_int_equal(rx.snapshots_failed, 1);

    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 2"),
                     nodes2);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 2);
}

/*
 * The pack index rebuild began by deleting every pack row, so one pack that
 * could not be read lost its rows for good. They are kept now, and the pack
 * fails the run.
 */
static void test_unreadable_pack_keeps_its_rows(void **state)
{
    (void)state;
    backup_once();
    long long packed = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    assert_true(packed > 0);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT id FROM packs WHERE repo_id = ? LIMIT 1",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    uint32_t pack_num = (uint32_t)strtoul(r->rows[0][0], NULL, 10);
    db_result_free(r);

    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "f=$(find '%s/packs' -name 'pack-%08u.dat' | head -1);"
             " printf 'garbagegarbagegarbage' | dd of=\"$f\" conv=notrunc status=none",
             TEST_REPO, pack_num);
    assert_int_equal(system(cmd), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), ERR_CORRUPT);
    err_clear();
    assert_int_equal(rx.packs_unreadable, 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), packed);
}

/* A clean repository still reindexes to OK, with nothing counted as failed. */
static void test_clean_reindex_reports_no_failures(void **state)
{
    (void)state;
    backup_once();
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.packs_unreadable, 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_reindex_without_the_key_refuses_and_changes_nothing,
                                        setup_enc, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_snapshot_fails_the_run_and_keeps_its_rows,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_pack_keeps_its_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_clean_reindex_reports_no_failures, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
