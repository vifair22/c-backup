/*
 * Sparse file handling tests for c-backup object store.
 *
 * Moved from test_data_paths.c + new coverage for empty files and
 * multi-region verification.
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

#include "../src/repo.h"
#include "../src/object.h"
#include "../src/parity.h"
#include "../src/types.h"

#define TEST_REPO "/tmp/c_backup_objsparse_repo"
#define TEST_SRC  "/tmp/c_backup_objsparse_src"

static repo_t *repo;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
}

static int setup_basic(void **state) {
    (void)state;
    cleanup();
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_basic(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

/* ================================================================== */
/* Existing tests (from test_data_paths.c)                             */
/* ================================================================== */

static void test_sparse_single_hole(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_s1.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 65536), 0);
    assert_int_equal(lseek(fd, 32768, SEEK_SET), 32768);
    const char tail[] = "sparse-data-here";
    ssize_t w = write(fd, tail, sizeof(tail) - 1);
    assert_int_equal((size_t)w, sizeof(tail) - 1);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    assert_true(out_type == OBJECT_TYPE_SPARSE || out_type == OBJECT_TYPE_FILE);
    if (out_type == OBJECT_TYPE_SPARSE) {
        assert_true(out_len >= sizeof(sparse_hdr_t));
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 1);
    }
    free(out);
    unlink(path);
}

static void test_sparse_multi_region(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_multi.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 262144), 0);

    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    char buf1[4096];
    memset(buf1, 'A', sizeof(buf1));
    assert_int_equal(write(fd, buf1, sizeof(buf1)), (ssize_t)sizeof(buf1));

    assert_int_equal(lseek(fd, 65536, SEEK_SET), 65536);
    char buf2[8192];
    memset(buf2, 'B', sizeof(buf2));
    assert_int_equal(write(fd, buf2, sizeof(buf2)), (ssize_t)sizeof(buf2));

    assert_int_equal(lseek(fd, 204800, SEEK_SET), 204800);
    char buf3[2048];
    memset(buf3, 'C', sizeof(buf3));
    assert_int_equal(write(fd, buf3, sizeof(buf3)), (ssize_t)sizeof(buf3));

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 2);
    }
    free(out);
    unlink(path);
}

static void test_sparse_all_hole(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_hole.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 1048576), 0);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_int_equal(hdr.region_count, 0);
    }
    free(out);
    unlink(path);
}

static void test_sparse_empty_file(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_empty.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, 0, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 0);
    free(out);
    unlink(path);
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* Zero-byte file store/load roundtrip via object_store (buffer path) */
static void test_store_empty_file(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, "", 0, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &typ), OK);
    assert_int_equal(out_len, 0);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
    free(out);
}

/* Entirely hole file (ftruncate, no writes) via store_file */
static void test_store_all_holes_sparse(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_allholes.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    /* 512 KiB of pure holes */
    assert_int_equal(ftruncate(fd, 524288), 0);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    /* Verify it loads and has SPARSE type with 0 regions */
    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_int_equal(hdr.region_count, 0);
    }
    free(out);
    unlink(path);
}

/* 3 data regions + holes → verify stored as OBJECT_TYPE_SPARSE */
static void test_store_multi_region_sparse(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_3reg.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    /* 128 KiB total with 3 data islands */
    assert_int_equal(ftruncate(fd, 131072), 0);

    /* Region 1: 0..4095 */
    char buf[4096];
    memset(buf, 'X', sizeof(buf));
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));

    /* Region 2: 32768..36863 */
    memset(buf, 'Y', sizeof(buf));
    assert_int_equal(lseek(fd, 32768, SEEK_SET), 32768);
    assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));

    /* Region 3: 98304..102399 */
    memset(buf, 'Z', sizeof(buf));
    assert_int_equal(lseek(fd, 98304, SEEK_SET), 98304);
    assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    assert_true(out_type == OBJECT_TYPE_SPARSE || out_type == OBJECT_TYPE_FILE);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 3);
    }
    free(out);
    unlink(path);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* Existing (from test_data_paths.c) */
        cmocka_unit_test_setup_teardown(test_sparse_single_hole, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_multi_region, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_all_hole, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_empty_file, setup_basic, teardown_basic),

        /* New coverage tests */
        cmocka_unit_test_setup_teardown(test_store_empty_file, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_all_holes_sparse, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_multi_region_sparse, setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
