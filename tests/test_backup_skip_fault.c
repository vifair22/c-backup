#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

/*
 * B118: a file whose read fails mid-store is left out of the snapshot.
 *
 * test_backup_e2e.c covers the other ways a file is skipped - gone before
 * it is opened, unreadable by mode, replaced by a directory - with the
 * progress callback standing between scan and store. A read that fails
 * partway through cannot be made that way, so the fault injector does it:
 * every read() on one path fails with EIO, as a dying disk or a dropped
 * mount would.
 *
 * Both store paths are covered, because each has its own skip site: the
 * backup thread for small files, and the large-file pool, whose failures
 * come back through pool_record. Before the fix either one left the file
 * in its tree with no content hash, and every full restore failed.
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
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "fault_inject.h"
#include "backup.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_skip_fault"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define MIB (1u << 20)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

/* `mib` MiB of a pattern, or a short string when mib is 0. */
static void write_file(const char *path, unsigned mib, char fill)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    if (mib == 0) {
        assert_int_equal(write(fd, "small file\n", 11), 11);
    } else {
        char *buf = malloc(MIB);
        assert_non_null(buf);
        for (unsigned i = 0; i < mib; i++) {
            memset(buf, fill, MIB);
            snprintf(buf, 32, "block %u", i);
            assert_int_equal(write(fd, buf, MIB), (ssize_t)MIB);
        }
        free(buf);
    }
    close(fd);
}

static int setup(void **state)
{
    (void)state;
    fault_reset_all();
    fault_read_eio_path = NULL;
    fault_read_refused = 0;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    write_file(TEST_SRC "/a.txt", 0, 0);
    write_file(TEST_SRC "/b.txt", 0, 0);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_read_eio_path = NULL;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
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

/* Back up TEST_SRC with reads of `bad` failing; it must be the one file
 * skipped, and the snapshot must restore whole and verify clean. */
static void expect_read_failure_left_out(const char *bad, const char *name,
                                         unsigned workers, uint64_t want_files)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;

    fault_read_eio_path = bad;
    backup_result_t res;
    status_t st = backup_run(store, db, repo_id, &o, &res);
    fault_read_eio_path = NULL;
    assert_int_equal(st, OK);
    assert_true(fault_read_refused > 0);
    /* The path under test is the one that ran: the pool, or not. */
    if (workers > 1) assert_true(res.store_workers > 1);
    else             assert_int_equal(res.store_workers, 1);

    assert_int_equal(res.files, want_files);
    assert_int_equal(res.files_skipped, 1);
    assert_int_equal(res.skipped_listed, 1);
    assert_string_equal(res.skipped[0].path, bad);
    if (!strstr(res.skipped[0].reason, "cannot read"))
        fail_msg("reason '%s' does not say the read failed", res.skipped[0].reason);

    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", name), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE type = 1"
                            " AND content_hash IS NULL", NULL), 0);
    /* And no cache row vouches for content this snapshot does not have. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM file_cache WHERE path = ?", bad), 0);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, res.snapshot_id,
                                 TEST_DIR "/dst", &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, want_files);

    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing + vr.unrecoverable, 0);
    backup_result_free(&res);

    /* With the disk well again the next backup picks it up. */
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files, want_files + 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", name), 1);
    backup_result_free(&res);
}

static void test_read_error_on_the_backup_thread(void **state)
{
    (void)state;
    write_file(TEST_SRC "/failing.txt", 0, 0);
    expect_read_failure_left_out(TEST_SRC "/failing.txt", "failing.txt", 1, 2);
}

static void test_read_error_on_a_pool_worker(void **state)
{
    (void)state;
    /* Two files over the 16 MiB threshold so the pool starts at all. */
    write_file(TEST_SRC "/big-ok.bin", 17, 'k');
    write_file(TEST_SRC "/big-bad.bin", 17, 'b');
    expect_read_failure_left_out(TEST_SRC "/big-bad.bin", "big-bad.bin", 2, 3);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_read_error_on_the_backup_thread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_read_error_on_a_pool_worker, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
