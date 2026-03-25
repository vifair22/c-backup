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
#include "../src/types.h"

#define TEST_REPO "/tmp/c_backup_test_repo"

static repo_t *repo;

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    return 0;
}

static void object_hash_path(const uint8_t hash[OBJECT_HASH_SIZE], char out[512]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    snprintf(out, 512, "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
}

static void flip_byte_at(const char *path, off_t off) {
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(lseek(fd, off, SEEK_SET), off);
    unsigned char b = 0;
    assert_int_equal(read(fd, &b, 1), 1);
    b ^= 0x01;
    assert_int_equal(lseek(fd, off, SEEK_SET), off);
    assert_int_equal(write(fd, &b, 1), 1);
    close(fd);
}

static void test_store_and_load(void **state) {
    (void)state;
    const char *data = "Hello, snapshot backup!";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE,
                                  data, len, hash), OK);

    void *out = NULL; size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

static void test_dedup(void **state) {
    (void)state;
    const char *data = "duplicate content";
    size_t len = strlen(data);
    uint8_t hash1[OBJECT_HASH_SIZE] = {0};
    uint8_t hash2[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash1), OK);
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash2), OK);
    assert_memory_equal(hash1, hash2, OBJECT_HASH_SIZE);
}

static void test_object_exists(void **state) {
    (void)state;
    const char *data = "existence check";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE,
                                  data, strlen(data), hash), OK);
    assert_true(object_exists(repo, hash));
}

static void test_repo_init_requires_empty_dir(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_nonempty_repo";
    int rc = system("rm -rf /tmp/c_backup_nonempty_repo");
    (void)rc;
    assert_int_equal(mkdir(path, 0755), 0);

    char marker[256];
    snprintf(marker, sizeof(marker), "%s/keep.txt", path);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fputs("do not clobber\n", f);
    fclose(f);

    assert_int_not_equal(repo_init(path), OK);

    rc = system("rm -rf /tmp/c_backup_nonempty_repo");
    (void)rc;
}

static void strip_parity_trailer(const char *path) {
    /* Truncate object file to header + payload, removing the parity trailer. */
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    object_header_t hdr;
    assert_int_equal(read(fd, &hdr, sizeof(hdr)), (ssize_t)sizeof(hdr));
    off_t data_end = (off_t)sizeof(hdr) + (off_t)hdr.compressed_size;
    assert_int_equal(ftruncate(fd, data_end), 0);
    /* Also set version back to 1 so load doesn't look for parity */
    hdr.version = 1;
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(write(fd, &hdr, sizeof(hdr)), (ssize_t)sizeof(hdr));
    close(fd);
}

static void test_object_load_detects_hash_mismatch(void **state) {
    (void)state;
    const char *data = "integrity check payload";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Strip parity trailer so corruption can't be repaired, then flip payload byte. */
    strip_parity_trailer(path);
    flip_byte_at(path, (off_t)sizeof(object_header_t));

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), ERR_CORRUPT);
}

static void test_object_load_detects_unknown_compression(void **state) {
    (void)state;
    const char *data = "compression marker";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Strip parity trailer so header corruption can't be repaired. */
    strip_parity_trailer(path);

    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    object_header_t hdr;
    assert_int_equal(read(fd, &hdr, sizeof(hdr)), (ssize_t)sizeof(hdr));
    hdr.compression = 99;
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(write(fd, &hdr, sizeof(hdr)), (ssize_t)sizeof(hdr));
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), ERR_CORRUPT);
}

static void test_object_load_detects_truncated_payload(void **state) {
    (void)state;
    const char *data = "truncate me";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Strip parity trailer first, then truncate within the payload region. */
    strip_parity_trailer(path);

    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    off_t end = lseek(fd, 0, SEEK_END);
    assert_true(end > (off_t)sizeof(object_header_t));
    /* Truncate within the payload (not just the trailer). */
    assert_int_equal(ftruncate(fd, (off_t)sizeof(object_header_t) + 1), 0);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), ERR_CORRUPT);
}

static void test_object_load_rejects_none_size_mismatch(void **state) {
    (void)state;
    const char *data = "size mismatch";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Strip parity trailer so header corruption can't be repaired. */
    strip_parity_trailer(path);

    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    object_header_t hdr;
    assert_int_equal(read(fd, &hdr, sizeof(hdr)), (ssize_t)sizeof(hdr));
    hdr.compression = COMPRESS_NONE;
    hdr.uncompressed_size = (uint64_t)hdr.compressed_size + 1u;
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(write(fd, &hdr, sizeof(hdr)), (ssize_t)sizeof(hdr));
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), ERR_CORRUPT);
}

static void test_object_store_file_sparse_roundtrip(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_sparse_file.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);

    const char tail[] = "tail-data";
    assert_int_equal(write(fd, "A", 1), 1);
    assert_int_equal(lseek(fd, 4096, SEEK_SET), 4096);
    assert_int_equal(write(fd, tail, sizeof(tail) - 1), (ssize_t)(sizeof(tail) - 1));

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

static void test_object_physical_size_edge_cases(void **state) {
    (void)state;
    const char *data = "phys-size-check";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    assert_int_equal(object_physical_size(repo, hash, NULL), ERR_INVALID);

    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xAB, sizeof(missing));
    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(repo, missing, &bytes), ERR_NOT_FOUND);
}

static void test_object_physical_size_returns_file_size(void **state) {
    (void)state;
    const char *data = "phys-size-with-trailer";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);

    struct stat st;
    assert_int_equal(stat(path, &st), 0);

    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(repo, hash, &bytes), OK);
    /* physical_size should return the full file size including parity trailer */
    assert_int_equal(bytes, (uint64_t)st.st_size);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_store_and_load, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dedup,          setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_exists,  setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_load_detects_hash_mismatch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_load_detects_unknown_compression, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_load_detects_truncated_payload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_load_rejects_none_size_mismatch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_store_file_sparse_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_physical_size_edge_cases, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_physical_size_returns_file_size, setup, teardown),
        cmocka_unit_test(test_repo_init_requires_empty_dir),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
