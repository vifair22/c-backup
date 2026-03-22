#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/ls.h"
#include "../src/repo.h"

#define TEST_REPO "/tmp/c_backup_ls_repo"
#define TEST_SRC  "/tmp/c_backup_ls_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static char *capture_ls_output(uint32_t snap_id, const char *path,
                               int recursive, char type_filter,
                               const char *name_glob, status_t *out_st) {
    char tmp[] = "/tmp/c_backup_ls_out.XXXXXX";
    int fd = mkstemp(tmp);
    assert_true(fd >= 0);

    fflush(stdout);
    int old = dup(STDOUT_FILENO);
    assert_true(old >= 0);
    assert_int_equal(dup2(fd, STDOUT_FILENO), STDOUT_FILENO);

    status_t st = snapshot_ls(repo, snap_id, path, recursive, type_filter, name_glob);
    fflush(stdout);

    assert_int_equal(dup2(old, STDOUT_FILENO), STDOUT_FILENO);
    close(old);
    if (out_st) *out_st = st;

    off_t end = lseek(fd, 0, SEEK_END);
    assert_true(end >= 0);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);

    char *buf = malloc((size_t)end + 1);
    assert_non_null(buf);
    ssize_t nr = read(fd, buf, (size_t)end);
    assert_true(nr >= 0);
    buf[nr] = '\0';

    close(fd);
    unlink(tmp);
    return buf;
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;

    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file(TEST_SRC "/a.txt", "alpha\n");
    write_file(TEST_SRC "/exec.sh", "#!/bin/sh\necho hi\n");
    assert_int_equal(chmod(TEST_SRC "/exec.sh", 0755), 0);

    assert_int_equal(mkdir(TEST_SRC "/dir1", 0755), 0);
    write_file(TEST_SRC "/dir1/nested.txt", "nested\n");

    assert_int_equal(mkdir(TEST_SRC "/emptydir", 0755), 0);

    assert_int_equal(symlink("a.txt", TEST_SRC "/lnk"), 0);
    assert_int_equal(mkfifo(TEST_SRC "/pipe1", 0644), 0);

    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

static void test_ls_non_recursive_header_and_rows(void **state) {
    (void)state;
    status_t st = ERR_IO;
    char *out = capture_ls_output(1, "tmp/c_backup_ls_src", 0, 0, NULL, &st);
    assert_int_equal(st, OK);

    assert_non_null(strstr(out, "snapshot 1  /tmp/c_backup_ls_src"));
    assert_non_null(strstr(out, "MODE        UID   GID          SIZE  MTIME             NAME"));
    assert_non_null(strstr(out, "a.txt"));
    assert_non_null(strstr(out, "exec.sh"));
    assert_non_null(strstr(out, "dir1"));
    assert_non_null(strstr(out, "lnk -> a.txt"));
    assert_non_null(strstr(out, "pipe1"));
    free(out);
}

static void test_ls_recursive_type_and_name_filter(void **state) {
    (void)state;
    status_t st = ERR_IO;
    char *out = capture_ls_output(1, "tmp/c_backup_ls_src", 1, 'f', "*.txt", &st);
    assert_int_equal(st, OK);

    assert_non_null(strstr(out, "(recursive)"));
    assert_non_null(strstr(out, "a.txt"));
    assert_non_null(strstr(out, "dir1/nested.txt"));
    assert_null(strstr(out, "exec.sh"));
    assert_null(strstr(out, "lnk"));
    free(out);
}

static void test_ls_empty_dir_and_invalid_dir(void **state) {
    (void)state;
    status_t st = ERR_IO;

    char *out = capture_ls_output(1, "tmp/c_backup_ls_src/emptydir", 0, 0, NULL, &st);
    assert_int_equal(st, OK);
    assert_non_null(strstr(out, "(empty)"));
    free(out);

    out = capture_ls_output(1, "tmp/c_backup_ls_src/not-a-dir", 0, 0, NULL, &st);
    assert_int_equal(st, ERR_INVALID);
    free(out);
}

static void test_ls_type_filters_symlink_and_fifo(void **state) {
    (void)state;
    status_t st = ERR_IO;

    char *out = capture_ls_output(1, "tmp/c_backup_ls_src", 0, 'l', NULL, &st);
    assert_int_equal(st, OK);
    assert_non_null(strstr(out, "lnk -> a.txt"));
    free(out);

    out = capture_ls_output(1, "tmp/c_backup_ls_src", 0, 'p', NULL, &st);
    assert_int_equal(st, OK);
    assert_non_null(strstr(out, "pipe1"));
    free(out);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_ls_non_recursive_header_and_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ls_recursive_type_and_name_filter, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ls_empty_dir_and_invalid_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ls_type_filters_symlink_and_fifo, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
