#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/snapshot.h"

#define TEST_REPO "/tmp/c_backup_bkup_test_repo"
#define TEST_SRC  "/tmp/c_backup_bkup_test_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC);
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "hello");
    write_file(TEST_SRC "/b.txt", "world");
    mkdir(TEST_SRC "/sub", 0755);
    write_file(TEST_SRC "/sub/c.txt", "nested");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO " " TEST_SRC);
    return 0;
}

static void test_first_backup_creates_manifest(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);
    assert_true(snap->node_count >= 4);
    snapshot_free(snap);
}

static void test_incremental_backup_advances_head(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    sleep(1);
    write_file(TEST_SRC "/a.txt", "hello modified");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 2u);
}

static void test_unchanged_backup_is_skipped(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);
}

static void test_deleted_file_not_in_next_manifest(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);
    unlink(TEST_SRC "/b.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    pathmap_t *pm = NULL;
    assert_int_equal(pathmap_build(snap, &pm), OK);
    snapshot_free(snap);

    assert_null(pathmap_lookup(pm, "c_backup_bkup_test_src/b.txt"));
    pathmap_free(pm);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_first_backup_creates_manifest, setup, teardown),
        cmocka_unit_test_setup_teardown(test_incremental_backup_advances_head, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unchanged_backup_is_skipped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleted_file_not_in_next_manifest, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
