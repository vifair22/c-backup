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
#include "../src/diff.h"

#define TEST_REPO "/tmp/c_backup_diff_repo"
#define TEST_SRC  "/tmp/c_backup_diff_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *capture_diff_output(uint32_t from_id, uint32_t to_id) {
    char tmp[] = "/tmp/c_backup_diff_out.XXXXXX";
    int fd = mkstemp(tmp);
    if (fd < 0) return NULL;

    fflush(stdout);
    int old = dup(STDOUT_FILENO);
    if (old < 0) { close(fd); unlink(tmp); return NULL; }
    if (dup2(fd, STDOUT_FILENO) < 0) {
        close(old); close(fd); unlink(tmp); return NULL;
    }

    assert_int_equal(snapshot_diff(repo, from_id, to_id), OK);
    fflush(stdout);

    dup2(old, STDOUT_FILENO);
    close(old);

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
    write_file(TEST_SRC "/a.txt", "one");
    write_file(TEST_SRC "/b.txt", "meta");
    write_file(TEST_SRC "/gone.txt", "bye");

    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

static void test_snapshot_diff_output(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    write_file(TEST_SRC "/a.txt", "one-modified");
    assert_int_equal(chmod(TEST_SRC "/b.txt", 0600), 0);
    assert_int_equal(unlink(TEST_SRC "/gone.txt"), 0);
    write_file(TEST_SRC "/new.txt", "new file");

    assert_int_equal(backup_run(repo, paths, 1), OK);

    char *out = capture_diff_output(1, 2);
    assert_non_null(out);
    assert_non_null(strstr(out, "M  tmp/c_backup_diff_src/a.txt\n"));
    assert_non_null(strstr(out, "m  tmp/c_backup_diff_src/b.txt\n"));
    assert_non_null(strstr(out, "D  tmp/c_backup_diff_src/gone.txt\n"));
    assert_non_null(strstr(out, "A  tmp/c_backup_diff_src/new.txt\n"));
    free(out);
}

static void test_snapshot_diff_no_changes(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    char *out = capture_diff_output(1, 1);
    assert_non_null(out);
    assert_non_null(strstr(out, "(no differences)\n"));
    free(out);
}

/* Diffing against a non-existent snapshot must return an error, not crash. */
static void test_snapshot_diff_nonexistent_snap_returns_error(void **state) {
    (void)state;
    /* No backups run — no snapshots exist at all */
    assert_int_equal(snapshot_diff(repo, 1, 2), ERR_NOT_FOUND);
    assert_int_equal(snapshot_diff(repo, 999, 1), ERR_NOT_FOUND);
}

/* Diffing a valid first snap against a nonexistent second must fail cleanly. */
static void test_snapshot_diff_second_snap_missing_returns_error(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    assert_int_equal(snapshot_diff(repo, 1, 999), ERR_NOT_FOUND);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_snapshot_diff_output, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_diff_no_changes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_diff_nonexistent_snap_returns_error, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_diff_second_snap_missing_returns_error, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
