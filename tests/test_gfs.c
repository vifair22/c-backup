#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/gfs.h"
#include "../src/policy.h"
#include "../src/repo.h"
#include "../src/snapshot.h"

#define TEST_REPO "/tmp/c_backup_gfs_repo"
#define TEST_SRC  "/tmp/c_backup_gfs_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

static status_t set_snap_ts(uint32_t snap_id, uint64_t ts) {
    snapshot_t *s = NULL;
    status_t st = snapshot_load(repo, snap_id, &s);
    if (st != OK) return st;
    s->created_sec = ts;
    st = snapshot_write(repo, s);
    snapshot_free(s);
    return st;
}

static int setup(void **state) {
    (void)state;
    (void)system("rm -rf " TEST_REPO " " TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file(TEST_SRC "/f.txt", "v1\n");
    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    (void)system("rm -rf " TEST_REPO " " TEST_SRC);
    return 0;
}

static void test_gfs_flags_str(void **state) {
    (void)state;
    char buf[64];

    gfs_flags_str(0, buf, sizeof(buf));
    assert_string_equal(buf, "none");

    gfs_flags_str(GFS_DAILY | GFS_WEEKLY | GFS_MONTHLY | GFS_YEARLY, buf, sizeof(buf));
    assert_string_equal(buf, "daily+weekly+monthly+yearly");
}

static void test_gfs_run_prunes_and_flags_anchor(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };

    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    write_file(TEST_SRC "/f.txt", "v2\n");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    write_file(TEST_SRC "/f.txt", "v3\n");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    write_file(TEST_SRC "/f.txt", "v4\n");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Sat Dec 30 2023 12:00:00 UTC and Sun Dec 31 2023 12:00:00 UTC */
    assert_int_equal(set_snap_ts(1, 1703764800ULL), OK); /* Thu */
    assert_int_equal(set_snap_ts(2, 1703851200ULL), OK); /* Fri */
    assert_int_equal(set_snap_ts(3, 1703937600ULL), OK); /* Sat */
    assert_int_equal(set_snap_ts(4, 1704024000ULL), OK); /* Sun, last in month/year */

    policy_t pol;
    memset(&pol, 0, sizeof(pol));
    pol.keep_snaps = 1;
    pol.keep_daily = 1;
    pol.keep_weekly = 1;
    pol.keep_monthly = 1;
    pol.keep_yearly = 1;

    assert_int_equal(gfs_run(repo, &pol, 4, 0, 1, 0), OK);

    uint32_t flags = 0;
    assert_int_equal(snapshot_read_gfs_flags(repo, 3, &flags), OK);
    assert_true((flags & GFS_DAILY) != 0);
    assert_true((flags & GFS_WEEKLY) != 0);
    assert_true((flags & GFS_MONTHLY) != 0);
    assert_true((flags & GFS_YEARLY) != 0);

    snapshot_t *s = NULL;
    assert_true(snapshot_load(repo, 1, &s) != OK);
    assert_true(snapshot_load(repo, 2, &s) != OK);
    assert_int_equal(snapshot_load(repo, 3, &s), OK);
    snapshot_free(s);
    assert_int_equal(snapshot_load(repo, 4, &s), OK);
    snapshot_free(s);
}

static void test_gfs_dry_run_does_not_prune(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };

    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    write_file(TEST_SRC "/f.txt", "v2\n");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    policy_t pol;
    memset(&pol, 0, sizeof(pol));
    pol.keep_snaps = 1;

    assert_int_equal(gfs_run(repo, &pol, 2, 1, 1, 0), OK);

    snapshot_t *s = NULL;
    assert_int_equal(snapshot_load(repo, 1, &s), OK);
    snapshot_free(s);
    assert_int_equal(snapshot_load(repo, 2, &s), OK);
    snapshot_free(s);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gfs_flags_str, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_run_prunes_and_flags_anchor, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_dry_run_does_not_prune, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
