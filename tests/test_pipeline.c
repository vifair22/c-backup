/*
 * Full pipeline tests: backup → pack → restore → verify.
 *
 * Moved from test_data_paths.c.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "repo.h"
#include "object.h"
#include "pack.h"
#include "snapshot.h"
#include "backup.h"
#include "restore.h"
#include "gc.h"
#include "parity.h"
#include "types.h"

#define TEST_REPO  "/tmp/c_backup_pipeline_repo"
#define TEST_SRC   "/tmp/c_backup_pipeline_src"
#define TEST_DEST  "/tmp/c_backup_pipeline_dest"

static repo_t *repo;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
}

static int setup_pipeline(void **state) {
    (void)state;
    cleanup();
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_pipeline(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

static void write_file(const char *path, const void *data, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    if (len > 0) {
        ssize_t w = write(fd, data, len);
        assert_int_equal((size_t)w, len);
    }
    close(fd);
}

static void write_random_file(const char *path, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    unsigned int seed = 0xDEADBEEF;
    char buf[4096];
    size_t remaining = len;
    while (remaining > 0) {
        size_t chunk = remaining < sizeof(buf) ? remaining : sizeof(buf);
        for (size_t i = 0; i < chunk; i++) {
            seed = seed * 1103515245 + 12345;
            buf[i] = (char)(seed >> 16);
        }
        ssize_t w = write(fd, buf, chunk);
        assert_true(w > 0);
        remaining -= (size_t)w;
    }
    close(fd);
}

static void write_compressible_file(const char *path, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    const char *pattern = "The quick brown fox jumps over the lazy dog. ";
    size_t plen = strlen(pattern);
    size_t remaining = len;
    while (remaining > 0) {
        size_t chunk = remaining < plen ? remaining : plen;
        ssize_t w = write(fd, pattern, chunk);
        assert_true(w > 0);
        remaining -= (size_t)w;
    }
    close(fd);
}

static void create_sparse_file(const char *path, off_t total_size,
                                off_t data_offset, const char *data, size_t dlen) {
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, total_size), 0);
    if (dlen > 0) {
        assert_int_equal(lseek(fd, data_offset, SEEK_SET), data_offset);
        ssize_t w = write(fd, data, dlen);
        assert_int_equal((size_t)w, dlen);
    }
    close(fd);
}

static void verify_file_matches(const char *path, const void *expected, size_t len) {
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    assert_int_equal((size_t)st.st_size, len);
    if (len > 0) {
        void *buf = malloc(len);
        assert_non_null(buf);
        ssize_t r = read(fd, buf, len);
        assert_int_equal((size_t)r, len);
        assert_memory_equal(buf, expected, len);
        free(buf);
    }
    close(fd);
}

/* ================================================================== */
/* Tests                                                               */
/* ================================================================== */

static void test_pipeline_mixed_files(void **state) {
    (void)state;

    write_file(TEST_SRC "/empty.bin", "", 0);
    write_file(TEST_SRC "/tiny.txt", "hi\n", 3);
    write_compressible_file(TEST_SRC "/compressible.txt", 51200);
    write_random_file(TEST_SRC "/random.bin", 51200);
    create_sparse_file(TEST_SRC "/sparse.bin", 65536, 32768, "SPARSE", 6);

    int rc = system("mkdir -p " TEST_SRC "/subdir");
    (void)rc;
    write_file(TEST_SRC "/subdir/nested.txt", "nested content\n", 15);
    symlink("tiny.txt", TEST_SRC "/link.txt");

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);

    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);

    verify_file_matches(TEST_DEST TEST_SRC "/tiny.txt", "hi\n", 3);
    verify_file_matches(TEST_DEST TEST_SRC "/subdir/nested.txt", "nested content\n", 15);

    struct stat st;
    assert_int_equal(stat(TEST_DEST TEST_SRC "/empty.bin", &st), 0);
    assert_int_equal(st.st_size, 0);

    char lnk[256];
    ssize_t ll = readlink(TEST_DEST TEST_SRC "/link.txt", lnk, sizeof(lnk) - 1);
    assert_true(ll > 0);
    lnk[ll] = '\0';
    assert_string_equal(lnk, "tiny.txt");
}

static void test_pipeline_incremental_snapshots(void **state) {
    (void)state;

    write_file(TEST_SRC "/file1.txt", "version 1\n", 10);
    write_file(TEST_SRC "/file2.txt", "unchanged\n", 10);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    write_file(TEST_SRC "/file1.txt", "version 2 with more data\n", 25);
    write_file(TEST_SRC "/file3.txt", "new file\n", 9);

    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);

    int rc = system("mkdir -p " TEST_DEST "/snap1");
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST "/snap1"), OK);
    verify_file_matches(TEST_DEST "/snap1" TEST_SRC "/file1.txt", "version 1\n", 10);

    rc = system("mkdir -p " TEST_DEST "/snap2");
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST "/snap2"), OK);
    verify_file_matches(TEST_DEST "/snap2" TEST_SRC "/file1.txt", "version 2 with more data\n", 25);
    verify_file_matches(TEST_DEST "/snap2" TEST_SRC "/file3.txt", "new file\n", 9);
}

static void test_pipeline_gc_cycle(void **state) {
    (void)state;

    write_file(TEST_SRC "/keep.txt", "keep this\n", 10);
    write_file(TEST_SRC "/temp.txt", "temporary\n", 10);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    unlink(TEST_SRC "/temp.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    assert_int_equal(snapshot_delete(repo, 1), OK);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);
    assert_true(kept > 0);

    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);

    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);
    verify_file_matches(TEST_DEST TEST_SRC "/keep.txt", "keep this\n", 10);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_pipeline_mixed_files, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pipeline_incremental_snapshots, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pipeline_gc_cycle, setup_pipeline, teardown_pipeline),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
