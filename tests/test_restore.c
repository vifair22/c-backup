#define _POSIX_C_SOURCE 200809L
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
#include "../src/restore.h"

#define TEST_REPO  "/tmp/c_backup_rst_repo"
#define TEST_SRC   "/tmp/c_backup_rst_src"
#define TEST_DEST  "/tmp/c_backup_rst_dest"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *read_file_str(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f); rewind(f);
    if (sz <= 0) { fclose(f); return strdup(""); }
    char *buf = malloc(sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t nr = fread(buf, 1, (size_t)sz, f);
    if (nr != (size_t)sz) { free(buf); fclose(f); return NULL; }
    buf[sz] = '\0';
    fclose(f);
    return buf;
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/hello.txt", "hello world");
    write_file(TEST_SRC "/data.bin", "binary data here");
    mkdir(TEST_SRC "/subdir", 0755);
    write_file(TEST_SRC "/subdir/nested.txt", "nested content");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
    return 0;
}

/* Backup and restore: verify file contents match */
static void test_restore_latest_roundtrip(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    /* Restored tree uses tar-like relative-absolute layout under DEST/tmp/... */
    char path[256];
    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/hello.txt", TEST_DEST);
    char *content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);

    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/subdir/nested.txt", TEST_DEST);
    content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "nested content");
    free(content);

    /* Subdirectory must exist */
    struct stat st;
    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/subdir", TEST_DEST);
    assert_int_equal(stat(path, &st), 0);
    assert_true(S_ISDIR(st.st_mode));
}

/* Symlink is restored correctly */
static void test_restore_symlink(void **state) {
    (void)state;
    /* create a symlink in the source */
    assert_int_equal(symlink("hello.txt", TEST_SRC "/link.txt"), 0);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char lpath[256];
    snprintf(lpath, sizeof(lpath), "%s/tmp/c_backup_rst_src/link.txt", TEST_DEST);

    struct stat lst;
    assert_int_equal(lstat(lpath, &lst), 0);
    assert_true(S_ISLNK(lst.st_mode));

    char target[256] = {0};
    ssize_t tlen = readlink(lpath, target, sizeof(target) - 1);
    assert_true(tlen > 0);
    target[tlen] = '\0';
    assert_string_equal(target, "hello.txt");
}

/* Restore specific snapshot ID */
static void test_restore_by_id(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* modify and run a second backup */
    sleep(1);
    write_file(TEST_SRC "/hello.txt", "modified");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* restore snapshot 1 (original) */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char path[256];
    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/hello.txt", TEST_DEST);
    char *content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);
}

static void test_restore_latest_no_snapshots(void **state) {
    (void)state;
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), ERR_IO);
}

static void test_restore_verify_dest_detects_mismatch(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);

    write_file(TEST_DEST "/tmp/c_backup_rst_src/hello.txt", "tampered");
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), ERR_CORRUPT);
}

static void test_restore_file_and_subtree(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);

    assert_int_equal(restore_file(repo, 1, "tmp/c_backup_rst_src/hello.txt", TEST_DEST), OK);
    char *content = read_file_str(TEST_DEST "/tmp/c_backup_rst_src/hello.txt");
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);

    assert_int_equal(restore_file(repo, 1, "tmp/c_backup_rst_src/subdir", TEST_DEST), ERR_NOT_FOUND);
    assert_int_equal(restore_file(repo, 1, "tmp/c_backup_rst_src/missing.txt", TEST_DEST), ERR_INVALID);

    int rc = system("rm -rf " TEST_DEST);
    (void)rc;
    mkdir(TEST_DEST, 0755);

    assert_int_equal(restore_subtree(repo, 1, "tmp/c_backup_rst_src/subdir", TEST_DEST), OK);
    content = read_file_str(TEST_DEST "/tmp/c_backup_rst_src/subdir/nested.txt");
    assert_non_null(content);
    assert_string_equal(content, "nested content");
    free(content);
    assert_null(read_file_str(TEST_DEST "/tmp/c_backup_rst_src/hello.txt"));

    assert_int_equal(restore_subtree(repo, 1, "tmp/c_backup_rst_src/does-not-exist", TEST_DEST), ERR_NOT_FOUND);
}

static void test_restore_snapshot_missing_manifest_fails(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    sleep(1);
    write_file(TEST_SRC "/hello.txt", "modified in snap2");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    assert_int_equal(unlink(TEST_REPO "/snapshots/00000001.snap"), 0);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), ERR_NOT_FOUND);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_restore_latest_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_symlink,          setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_by_id,            setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_latest_no_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_verify_dest_detects_mismatch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_file_and_subtree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_snapshot_missing_manifest_fails, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
