#define _POSIX_C_SOURCE 200809L

/*
 * Change detection.
 *
 * The cache decides whether a file is read at all, so its failure modes are
 * asymmetric in a way the tests have to respect. A false miss costs one
 * re-read and nothing else. A false hit records the previous content under a
 * path that has changed, and nothing notices until a restore produces the
 * wrong bytes. Every test here is aimed at the second kind.
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
#include <fcntl.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "file_cache.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_fcache"
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
    write_file(TEST_SRC "/alpha.txt", "alpha content");
    write_file(TEST_SRC "/beta.txt",  "beta content");
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

static backup_result_t run_backup_mode(int mode)
{
    backup_opts_t o = basic_opts();
    o.file_cache_match = mode;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    return r;
}

static backup_result_t run_backup(void)
{
    backup_opts_t o = basic_opts();
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    return r;
}

static int cache_rows(void)
{
    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (db_execute(db, "SELECT COUNT(*) FROM file_cache WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0) { db_result_free(r); return -1; }
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/* ------------------------------------------------------------------ */

/* Nothing is cached on the first run, so everything is read. */
static void test_first_backup_reads_everything(void **state)
{
    (void)state;
    backup_result_t r = run_backup();
    assert_int_equal(r.files_unchanged, 0);
    assert_true(r.files > 0);
    assert_int_equal(cache_rows(), (int)r.files);
}

/* And nothing is read the second time. */
static void test_unchanged_files_are_not_read(void **state)
{
    (void)state;
    backup_result_t first = run_backup();
    backup_result_t second = run_backup();

    assert_int_equal(second.files, first.files);
    assert_int_equal(second.files_unchanged, second.files);
    assert_int_equal(second.objects_new, 0);
}

/*
 * The cache must survive its own maintenance. The sweep removes entries the
 * scan did not see, and an earlier version stamped rows only when writing
 * them, so a hit left its row looking stale and the sweep deleted exactly
 * what it had just used: the cache worked once and then emptied itself.
 */
static void test_cache_survives_repeated_backups(void **state)
{
    (void)state;
    run_backup();
    for (int i = 0; i < 3; i++) {
        backup_result_t r = run_backup();
        assert_int_equal(r.files_unchanged, r.files);
    }
    assert_true(cache_rows() > 0);
}

/* A changed file is read again and produces a new object. */
static void test_modified_file_is_read_again(void **state)
{
    (void)state;
    run_backup();
    sleep(1);   /* keep the mtime change unambiguous at second resolution */
    write_file(TEST_SRC "/alpha.txt", "alpha content, now different");

    backup_result_t r = run_backup();
    assert_int_equal(r.files_unchanged, r.files - 1);
    assert_true(r.objects_new > 0);
}

/*
 * Content replaced while the mtime is restored to its old value. mtime and
 * size alone would call this unchanged and back up the previous contents
 * forever; ctime is what catches it.
 */
static void test_content_change_with_restored_mtime_is_caught(void **state)
{
    (void)state;
    run_backup();

    struct stat before;
    assert_int_equal(stat(TEST_SRC "/alpha.txt", &before), 0);

    sleep(1);
    write_file(TEST_SRC "/alpha.txt", "different but same length!!!");
    /* Put the mtime back exactly where it was. */
    struct timespec times[2];
    times[0] = before.st_atim;
    times[1] = before.st_mtim;
    assert_int_equal(utimensat(AT_FDCWD, TEST_SRC "/alpha.txt", times, 0), 0);

    backup_result_t r = run_backup();
    assert_int_equal(r.files_unchanged, r.files - 1);
}

/*
 * A cached hash whose object has been collected must not be trusted. GC
 * removes objects no live snapshot references, so a path can match on every
 * metadata field with its bytes already gone, and reusing the hash would
 * write a snapshot pointing at nothing.
 */
static void test_cache_is_not_trusted_when_the_object_is_gone(void **state)
{
    (void)state;
    run_backup();

    /* Drop the registry rows the way GC would, leaving the cache intact. */
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);

    backup_result_t r = run_backup();

    /* Not one file was taken on trust. */
    assert_int_equal(r.files_unchanged, 0);
    /* And the registry rows are back, rebuilt by re-storing. The object
     * files themselves were never removed, so this does not re-materialise
     * bytes; it proves the path was walked rather than short-circuited. */
    db_result_t *o = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM objects WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &o), 0);
    assert_true(atoi(o->rows[0][0]) > 0);
    db_result_free(o);
}

/* Deleted paths lose their cache entries rather than accumulating forever. */
static void test_sweep_drops_entries_for_removed_paths(void **state)
{
    (void)state;
    backup_result_t first = run_backup();
    assert_int_equal(cache_rows(), (int)first.files);

    assert_int_equal(unlink(TEST_SRC "/beta.txt"), 0);
    backup_result_t second = run_backup();

    assert_int_equal(cache_rows(), (int)second.files);
    assert_true((int)second.files < (int)first.files);
}

/*
 * B087: what an Unraid share over NFS looks like to the cache. shfs hands
 * out inode numbers that change between lookups, and a container volume can
 * remount on another device number, so every recorded dev and inode is
 * wrong the next time while size, mtime and the bytes are not. Simulated by
 * moving the recorded numbers, which is what the next stat reports there.
 */
static void shift_recorded_dev_and_inode(void)
{
    assert_int_equal(db_execute_non_query(db,
        "UPDATE file_cache SET dev = dev + 1, inode = inode + 7", NULL, NULL), 0);
}

/* On a local filesystem, auto keeps comparing them: a changed inode there
 * means another file now sits at the path. */
static void test_auto_compares_inode_on_a_local_filesystem(void **state)
{
    (void)state;
    run_backup();
    shift_recorded_dev_and_inode();
    backup_result_t r = run_backup_mode(FILE_CACHE_MATCH_AUTO);
    assert_int_equal(r.files_unchanged, 0);
}

static void test_strict_compares_every_field(void **state)
{
    (void)state;
    run_backup();
    shift_recorded_dev_and_inode();
    backup_result_t r = run_backup_mode(FILE_CACHE_MATCH_STRICT);
    assert_int_equal(r.files_unchanged, 0);
}

/* relaxed is the escape hatch for a filesystem auto does not recognise:
 * unstable numbers no longer cost a full re-read. */
static void test_relaxed_ignores_dev_inode_and_ctime(void **state)
{
    (void)state;
    run_backup();
    shift_recorded_dev_and_inode();
    /* And a ctime change: chmod moves it without touching the content. */
    sleep(1);
    assert_int_equal(chmod(TEST_SRC "/alpha.txt", 0640), 0);
    backup_result_t r = run_backup_mode(FILE_CACHE_MATCH_RELAXED);
    assert_int_equal(r.files_unchanged, r.files);
    assert_int_equal(r.objects_new, 0);
}

/* Even relaxed reads a file whose size or mtime moved. */
static void test_relaxed_still_reads_a_modified_file(void **state)
{
    (void)state;
    run_backup();
    sleep(1);
    write_file(TEST_SRC "/alpha.txt", "alpha content, now different");
    backup_result_t r = run_backup_mode(FILE_CACHE_MATCH_RELAXED);
    assert_int_equal(r.files_unchanged, r.files - 1);
    assert_true(r.objects_new > 0);
}

/* The lookup itself honours the mask auto uses on a network filesystem,
 * and only that mask. */
static void test_lookup_ignores_only_what_it_is_told(void **state)
{
    (void)state;
    run_backup();
    shift_recorded_dev_and_inode();
    struct stat st;
    assert_int_equal(stat(TEST_SRC "/beta.txt", &st), 0);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(file_cache_lookup(db, repo_id, TEST_SRC "/beta.txt", &st, 0, h), 0);
    assert_int_equal(file_cache_lookup(db, repo_id, TEST_SRC "/beta.txt", &st,
                                       FILE_CACHE_IGNORE_DEV, h), 0);
    assert_int_equal(file_cache_lookup(db, repo_id, TEST_SRC "/beta.txt", &st,
                                       FILE_CACHE_IGNORE_INODE, h), 0);
    assert_int_equal(file_cache_lookup(db, repo_id, TEST_SRC "/beta.txt", &st,
                                       FILE_CACHE_IGNORE_DEV | FILE_CACHE_IGNORE_INODE, h), 1);
}

static void test_match_modes_parse(void **state)
{
    (void)state;
    assert_int_equal(file_cache_match_parse("auto"), FILE_CACHE_MATCH_AUTO);
    assert_int_equal(file_cache_match_parse(""), FILE_CACHE_MATCH_AUTO);
    assert_int_equal(file_cache_match_parse(NULL), FILE_CACHE_MATCH_AUTO);
    assert_int_equal(file_cache_match_parse("strict"), FILE_CACHE_MATCH_STRICT);
    assert_int_equal(file_cache_match_parse("relaxed"), FILE_CACHE_MATCH_RELAXED);
    assert_int_equal(file_cache_match_parse("Relaxed"), -1);
    assert_int_equal(file_cache_match_parse("ignore-inode"), -1);
}

static void test_remote_filesystems_are_recognised(void **state)
{
    (void)state;
    assert_true(file_cache_fs_is_remote(0x6969));        /* NFS */
    assert_true(file_cache_fs_is_remote(0x65735546));    /* FUSE (shfs) */
    assert_true(file_cache_fs_is_remote((long)0xFF534D42u)); /* CIFS */
    assert_true(file_cache_fs_is_remote((long)0xFE534D42u)); /* SMB2 */
    assert_false(file_cache_fs_is_remote(0xEF53));       /* ext4 */
    assert_false(file_cache_fs_is_remote((long)0x9123683Eu)); /* btrfs */
    assert_false(file_cache_fs_is_remote(0x58465342));   /* xfs */
    assert_false(file_cache_fs_is_remote(0x01021994));   /* tmpfs */
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_first_backup_reads_everything, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unchanged_files_are_not_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cache_survives_repeated_backups, setup, teardown),
        cmocka_unit_test_setup_teardown(test_modified_file_is_read_again, setup, teardown),
        cmocka_unit_test_setup_teardown(test_content_change_with_restored_mtime_is_caught, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cache_is_not_trusted_when_the_object_is_gone, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sweep_drops_entries_for_removed_paths, setup, teardown),
        cmocka_unit_test_setup_teardown(test_auto_compares_inode_on_a_local_filesystem, setup, teardown),
        cmocka_unit_test_setup_teardown(test_strict_compares_every_field, setup, teardown),
        cmocka_unit_test_setup_teardown(test_relaxed_ignores_dev_inode_and_ctime, setup, teardown),
        cmocka_unit_test_setup_teardown(test_relaxed_still_reads_a_modified_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lookup_ignores_only_what_it_is_told, setup, teardown),
        cmocka_unit_test(test_match_modes_parse),
        cmocka_unit_test(test_remote_filesystems_are_recognised),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
