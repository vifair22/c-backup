#define _POSIX_C_SOURCE 200809L

/*
 * Incremental backup behaviour.
 *
 * The prototype in bench/manifest.c measured 3.3% new tree objects per
 * incremental snapshot against a 200k-file tree. These tests assert the same
 * property through the real pipeline at a size that runs in a test suite.
 *
 * The failure this guards against is silent by construction: if tree
 * serialisation stops being deterministic, backups still succeed and restores
 * still work, the repository just grows as though every snapshot were a full
 * manifest. Nothing logs an error. The only symptom is disk usage, months
 * later.
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

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "file_cache.h"
#include "object.h"
#include "pack.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_incr"
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

/*
 * A tree deep and wide enough that "only the changed path is rewritten" is a
 * meaningful claim rather than an accident of a three-file fixture.
 *
 * Note the ancestor chain. scan_tree records the absolute path of a source
 * root, so backing up /tmp/c_backup_test_incr/src also produces directory
 * entries for /tmp and /tmp/c_backup_test_incr. A restore needs them to
 * recreate the path. They are why a one-file change rewrites more than the
 * one directory containing it: the changed directory, every ancestor up to
 * /, and the synthetic root. That is the "+ depth" term in
 * O(changed dirs + depth), and it is a fixed cost per snapshot rather than
 * one that grows with the size of the tree.
 */
#define ANCESTOR_TREES 3   /* /tmp, /tmp/c_backup_test_incr, and src itself */
#define ROOT_TREES     1   /* the synthetic root joining all source paths */
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
    char path[512];
    for (int d = 0; d < 8; d++) {
        snprintf(path, sizeof(path), "%s/dir%02d", TEST_SRC, d);
        if (mkdir(path, 0700) != 0) return -1;
        for (int f = 0; f < 5; f++) {
            char content[64];
            snprintf(path, sizeof(path), "%s/dir%02d/file%02d.txt", TEST_SRC, d, f);
            snprintf(content, sizeof(content), "content of dir %d file %d", d, f);
            write_file(path, content);
        }
    }
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

/* ------------------------------------------------------------------ */

/*
 * Backing up an unchanged tree twice must store no new objects and no new
 * trees. This is the canary: if it fails, dedup is broken, and every other
 * test in the suite still passes.
 */
static void test_unchanged_rebackup_writes_nothing(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;

    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);
    assert_true(first.objects_new > 0);
    assert_true(first.trees_new > 0);

    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);
    assert_int_equal(second.snapshot_id, 2);
    assert_int_equal(second.files, first.files);

    assert_int_equal(second.objects_new, 0);
    assert_int_equal(second.trees_new, 0);
    assert_int_equal(second.trees_reused, first.trees_new + first.trees_reused);
}

/*
 * Changing one file must rewrite only that file's object and the directories
 * on its path to the root: O(changed dirs + depth), not O(total files).
 */
static void test_single_change_rewrites_only_its_path(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;

    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);
    write_file(TEST_SRC "/dir03/file02.txt", "changed content");
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    assert_int_equal(second.objects_new, 1);

    /* dir03, its ancestor chain, and the synthetic root. The other seven
     * sibling directories are untouched and must be reused verbatim. */
    assert_int_equal(second.trees_new, 1 + ANCESTOR_TREES + ROOT_TREES);
    assert_true(second.trees_reused >= 7);
}

/* Adding a file changes its directory and the root, nothing else. */
static void test_added_file_is_localised(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;

    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);
    write_file(TEST_SRC "/dir05/newcomer.txt", "brand new");
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    assert_int_equal(second.files, first.files + 1);
    assert_int_equal(second.objects_new, 1);
    assert_int_equal(second.trees_new, 1 + ANCESTOR_TREES + ROOT_TREES);
}

/* Deleting a file is a change to its directory, not to any other. */
static void test_deleted_file_is_localised(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;

    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);
    assert_int_equal(unlink(TEST_SRC "/dir01/file03.txt"), 0);
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    assert_int_equal(second.files, first.files - 1);
    /* Nothing new is stored: the remaining files are byte-identical. */
    assert_int_equal(second.objects_new, 0);
    assert_int_equal(second.trees_new, 1 + ANCESTOR_TREES + ROOT_TREES);
}

/*
 * The proportion that matters. With 9 directories and one changed file, new
 * trees should be a small fraction of the total, mirroring the 3.3% measured
 * on a 200k-file tree.
 */
static void test_new_tree_fraction_stays_low(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;

    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);
    write_file(TEST_SRC "/dir07/file01.txt", "touched");
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    uint64_t total = second.trees_new + second.trees_reused;
    assert_true(total > 0);

    /*
     * With 8 sibling directories the reused set must dominate. The absolute
     * fraction here is worse than the 3.3% measured on a 200k-file tree
     * purely because the fixed ancestor cost is amortised over 8 directories
     * instead of 10,000. What matters is that untouched siblings are reused
     * at all: a determinism regression takes this to 1.0.
     */
    double fraction = (double)second.trees_new / (double)total;
    assert_true(fraction < 0.6);
    assert_true(second.trees_reused >= 7);
}

/* Each run is its own snapshot, and each snapshot keeps its own node rows. */
static void test_snapshots_are_independent(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    write_file(TEST_SRC "/dir00/file00.txt", "v2");
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT snap_id, COUNT(*) FROM nodes WHERE repo_id = ?"
        " GROUP BY snap_id ORDER BY snap_id",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(r->nrows, 2);
    assert_true(atoi(r->rows[0][1]) > 0);
    assert_true(atoi(r->rows[1][1]) > 0);
    db_result_free(r);

    /* Both snapshots must have distinct root trees, since content changed. */
    r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(DISTINCT root_tree_hash) FROM snapshots WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 2);
    db_result_free(r);
}

/*
 * A backup that dies mid-run must not poison the next one.
 *
 * Small files go straight into a pack the backup holds open, and the index
 * rows and file-cache entries for them are committed in batches while that
 * pack is still a temporary file. A crash loses the pack and keeps the rows.
 * When those rows said "loose", the next backup's file cache saw the path
 * unchanged, found the object "present", and wrote a snapshot referencing
 * bytes that were never on disk: 23,674 such files in one measured run, with
 * the backup reporting success.
 *
 * This builds that state exactly - an object staged into a pack that is then
 * abandoned, with its index row and its cache entry written - and requires
 * the next backup to store the file again.
 */
static void test_backup_after_a_crash_restores_what_the_crash_lost(void **state)
{
    (void)state;
    const char *victim = TEST_SRC "/dir03/file02.txt";

    pack_backup_writer_t *pw = NULL;
    assert_int_equal(pack_backup_open(store, &pw), OK);
    int fd = open(victim, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    uint8_t hash[OBJECT_HASH_SIZE];
    object_write_t w;
    assert_int_equal(object_store_file_packed(store, fd, (uint64_t)st.st_size,
                                              pw, hash, &w), OK);
    close(fd);
    assert_true(pack_backup_holds(pw, hash));
    assert_int_equal(store_index_put_staged(db, repo_id, hash, OBJECT_TYPE_FILE,
                                            w.phys_bytes, w.payload_size), OK);
    struct stat lst;
    assert_int_equal(lstat(victim, &lst), 0);
    file_cache_put(db, repo_id, victim, &lst, hash, 1);
    pack_backup_abort(pw);   /* the process died: the pack never landed */

    /* A staged row is not an object, so the cache does not trust it. */
    uint8_t cached[OBJECT_HASH_SIZE];
    assert_int_equal(file_cache_lookup(db, repo_id, victim, &lst, 0, cached), 0);
    assert_false(object_exists(store, hash));

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    /* Stored for real this time, and indexed where it actually is. */
    assert_true(object_exists(store, hash));
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT location FROM objects WHERE repo_id = ? AND hash = ?",
        (const char *[]){ repo, hex, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_not_equal(r->rows[0][0], "staged");
    assert_string_not_equal(r->rows[0][0], "loose");   /* it went into a pack */
    db_result_free(r);
}

/* What the daemon does at start: staged rows belong to a dead backup. */
static void test_staged_rows_are_dropped(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE];
    memset(h, 0xab, sizeof(h));
    assert_int_equal(store_index_put_staged(db, repo_id, h, OBJECT_TYPE_FILE, 1, 1), OK);
    assert_int_equal(store_index_drop_staged(db, 0), OK);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM objects WHERE location = 'staged'",
                                NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_unchanged_rebackup_writes_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_single_change_rewrites_only_its_path, setup, teardown),
        cmocka_unit_test_setup_teardown(test_added_file_is_localised, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleted_file_is_localised, setup, teardown),
        cmocka_unit_test_setup_teardown(test_new_tree_fraction_stays_low, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshots_are_independent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_after_a_crash_restores_what_the_crash_lost, setup, teardown),
        cmocka_unit_test_setup_teardown(test_staged_rows_are_dropped, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
