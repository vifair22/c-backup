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
#include "../src/reverse.h"

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

/* First backup: all entries must be CREATED (reverse = REV_OP_REMOVE) */
static void test_first_backup(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);
    /* 4 entries: src root dir, a.txt, b.txt, sub dir, sub/c.txt */
    assert_true(snap->node_count >= 4);
    snapshot_free(snap);

    /* reverse record for snap 1: every entry is CREATED → REV_OP_REMOVE */
    rev_record_t *rev = NULL;
    assert_int_equal(reverse_load(repo, 1, &rev), OK);
    assert_non_null(rev);
    assert_true(rev->entry_count >= 4);
    for (uint32_t i = 0; i < rev->entry_count; i++) {
        assert_int_equal(rev->entries[i].op_type, REV_OP_REMOVE);
    }
    reverse_free(rev);
}

/* Second backup after modifying a file: changed entry has REV_OP_RESTORE */
static void test_incremental_backup(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    /* First backup */
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Modify one file */
    sleep(1);  /* ensure mtime differs */
    write_file(TEST_SRC "/a.txt", "hello modified");

    /* Second backup */
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 2u);

    /* Reverse record for snap 2: a.txt should be RESTORE, rest unchanged */
    rev_record_t *rev = NULL;
    assert_int_equal(reverse_load(repo, 2, &rev), OK);
    assert_non_null(rev);

    int found_restore = 0;
    for (uint32_t i = 0; i < rev->entry_count; i++) {
        if (rev->entries[i].op_type == REV_OP_RESTORE)
            found_restore = 1;
    }
    assert_true(found_restore);
    reverse_free(rev);
}

/* Third backup with no changes: no reverse records */
static void test_unchanged_backup(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Sync mtimes — nothing should change */
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 2u);

    /* Snapshot 2 reverse file should not exist (no changes) */
    char rev_path[256];
    snprintf(rev_path, sizeof(rev_path), "%s/reverse/%08u.rev", TEST_REPO, 2u);
    assert_int_not_equal(access(rev_path, F_OK), 0);
}

/* Deleted file: reverse record should contain REV_OP_RESTORE for that path */
static void test_deleted_file(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);

    unlink(TEST_SRC "/b.txt");

    assert_int_equal(backup_run(repo, paths, 1), OK);

    rev_record_t *rev = NULL;
    assert_int_equal(reverse_load(repo, 2, &rev), OK);
    assert_non_null(rev);

    int found_deleted = 0;
    for (uint32_t i = 0; i < rev->entry_count; i++) {
        if (rev->entries[i].op_type == REV_OP_RESTORE &&
            strstr(rev->entries[i].path, "b.txt")) {
            found_deleted = 1;
        }
    }
    assert_true(found_deleted);
    reverse_free(rev);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_first_backup,       setup, teardown),
        cmocka_unit_test_setup_teardown(test_incremental_backup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unchanged_backup,   setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleted_file,       setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
