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

#include "repo.h"
#include "backup.h"
#include "pack.h"
#include "stats.h"

#define TEST_REPO "/tmp/c_backup_stats_repo"
#define TEST_SRC  "/tmp/c_backup_stats_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *capture_stats_output(const repo_stat_t *s) {
    char tmp[] = "/tmp/c_backup_stats_out.XXXXXX";
    int fd = mkstemp(tmp);
    if (fd < 0) return NULL;

    fflush(stdout);
    int old = dup(STDOUT_FILENO);
    if (old < 0) { close(fd); unlink(tmp); return NULL; }
    if (dup2(fd, STDOUT_FILENO) < 0) {
        close(old); close(fd); unlink(tmp); return NULL;
    }

    repo_stats_print(s);
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
    system("rm -rf " TEST_REPO " " TEST_SRC);
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "hello");
    write_file(TEST_SRC "/b.txt", "world");

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

static void test_repo_stats_counts_and_print(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run_opts(repo, paths, 1, NULL), OK);
    write_file(TEST_SRC "/a.txt", "hello-changed");
    assert_int_equal(backup_run_opts(repo, paths, 1, NULL), OK);

    repo_stat_t s = {0};
    assert_int_equal(repo_stats(repo, &s), OK);
    assert_true(s.snap_total >= 2);
    assert_true(s.snap_count >= 2);
    assert_true(s.snap_bytes > 0);
    assert_true(s.total_bytes > 0);

    assert_int_equal(repo_pack(repo, NULL), OK);
    repo_stat_t sp = {0};
    assert_int_equal(repo_stats(repo, &sp), OK);
    assert_true(sp.pack_files >= 1);

    char *out = capture_stats_output(&sp);
    assert_non_null(out);
    assert_non_null(strstr(out, "snapshots:"));
    assert_non_null(strstr(out, "head logical size:"));
    assert_non_null(strstr(out, "loose objects physical:"));
    assert_non_null(strstr(out, "pack files physical:"));
    assert_non_null(strstr(out, "repo physical total:"));
    free(out);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_repo_stats_counts_and_print, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
