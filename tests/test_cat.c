#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/restore.h"

#define TEST_REPO "/tmp/c_backup_cat_repo"
#define TEST_SRC  "/tmp/c_backup_cat_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *capture_cat_output(const char *path, uint32_t snap_id, status_t *out_st) {
    char tmp[] = "/tmp/c_backup_cat_out.XXXXXX";
    int fd = mkstemp(tmp);
    if (fd < 0) return NULL;

    fflush(stdout);
    int old = dup(STDOUT_FILENO);
    if (old < 0) { close(fd); unlink(tmp); return NULL; }
    if (dup2(fd, STDOUT_FILENO) < 0) {
        close(old); close(fd); unlink(tmp); return NULL;
    }

    status_t st = restore_cat_file(repo, snap_id, path);
    fflush(stdout);

    dup2(old, STDOUT_FILENO);
    close(old);
    if (out_st) *out_st = st;

    off_t end = lseek(fd, 0, SEEK_END);
    if (end < 0) { close(fd); unlink(tmp); return NULL; }
    if (lseek(fd, 0, SEEK_SET) < 0) { close(fd); unlink(tmp); return NULL; }

    char *buf = malloc((size_t)end + 1);
    if (!buf) { close(fd); unlink(tmp); return NULL; }
    ssize_t nr = read(fd, buf, (size_t)end);
    close(fd);
    unlink(tmp);
    if (nr < 0) { free(buf); return NULL; }
    buf[nr] = '\0';
    return buf;
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;

    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/hello.txt", "hello from cat\n");

    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;

    const char *paths[] = { TEST_SRC };
    if (backup_run(repo, paths, 1) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

static void test_cat_regular_file_absolute_path(void **state) {
    (void)state;
    status_t st = ERR_IO;
    char *out = capture_cat_output("/tmp/c_backup_cat_src/hello.txt", 1, &st);
    assert_non_null(out);
    assert_int_equal(st, OK);
    assert_string_equal(out, "hello from cat\n");
    free(out);
}

static void test_cat_regular_file_relative_path(void **state) {
    (void)state;
    status_t st = ERR_IO;
    char *out = capture_cat_output("tmp/c_backup_cat_src/hello.txt", 1, &st);
    assert_non_null(out);
    assert_int_equal(st, OK);
    assert_string_equal(out, "hello from cat\n");
    free(out);
}

static void test_cat_missing_file_returns_not_found(void **state) {
    (void)state;
    status_t st = restore_cat_file(repo, 1, "/tmp/c_backup_cat_src/missing.txt");
    assert_int_equal(st, ERR_NOT_FOUND);
}

static void test_cat_directory_returns_invalid(void **state) {
    (void)state;
    status_t st = restore_cat_file(repo, 1, "/tmp/c_backup_cat_src");
    assert_int_equal(st, ERR_INVALID);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_cat_regular_file_absolute_path, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cat_regular_file_relative_path, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cat_missing_file_returns_not_found, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cat_directory_returns_invalid, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
