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
    fread(buf, 1, sz, f);
    buf[sz] = '\0';
    fclose(f);
    return buf;
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
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
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    return 0;
}

/* Backup and restore: verify file contents match */
static void test_restore_latest_roundtrip(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    /* The restored tree is at DEST/<basename(src)>/... = DEST/c_backup_rst_src/... */
    char path[256];
    snprintf(path, sizeof(path), "%s/c_backup_rst_src/hello.txt", TEST_DEST);
    char *content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);

    snprintf(path, sizeof(path), "%s/c_backup_rst_src/subdir/nested.txt", TEST_DEST);
    content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "nested content");
    free(content);

    /* Subdirectory must exist */
    struct stat st;
    snprintf(path, sizeof(path), "%s/c_backup_rst_src/subdir", TEST_DEST);
    assert_int_equal(stat(path, &st), 0);
    assert_true(S_ISDIR(st.st_mode));
}

/* Symlink is restored correctly */
static void test_restore_symlink(void **state) {
    (void)state;
    /* create a symlink in the source */
    symlink("hello.txt", TEST_SRC "/link.txt");

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char lpath[256];
    snprintf(lpath, sizeof(lpath), "%s/c_backup_rst_src/link.txt", TEST_DEST);

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
    snprintf(path, sizeof(path), "%s/c_backup_rst_src/hello.txt", TEST_DEST);
    char *content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_restore_latest_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_symlink,          setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_by_id,            setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
