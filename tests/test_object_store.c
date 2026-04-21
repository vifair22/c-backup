/*
 * Object store tests: store, load, dedup, exists, get_info, physical_size.
 *
 * Moved from test_data_paths.c + new coverage tests for object_store_ex,
 * object_store_fd, and streaming load paths.
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
#include "parity.h"
#include "types.h"

#define TEST_REPO "/tmp/c_backup_objstore_repo"
#define TEST_SRC  "/tmp/c_backup_objstore_src"

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

static void test_object_empty(void **state) {
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

static void test_object_tiny(void **state) {
    (void)state;
    const char d = 'X';
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, &d, 1, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 1);
    assert_int_equal(*(char *)out, 'X');
    free(out);
}

static void test_object_compressible_10k(void **state) {
    (void)state;
    size_t len = 10240;
    char *data = malloc(len);
    assert_non_null(data);
    const char *pat = "AAABBBCCCDDD";
    for (size_t i = 0; i < len; i++) data[i] = pat[i % 12];

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
    free(data);
}

static void test_object_incompressible_64k(void **state) {
    (void)state;
    size_t len = 65536;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    unsigned int seed = 42;
    for (size_t i = 0; i < len; i++) {
        seed = seed * 1103515245 + 12345;
        data[i] = (uint8_t)(seed >> 16);
    }

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
    free(data);
}

static void test_object_dedup(void **state) {
    (void)state;
    const char *data = "deduplicate me across snapshots";
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), h1), OK);
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), h2), OK);
    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
}

static void test_object_exists_check(void **state) {
    (void)state;
    const char *data = "existence verification";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    assert_true(object_exists(repo, hash));

    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xAB, OBJECT_HASH_SIZE);
    assert_false(object_exists(repo, missing));
}

static void test_object_get_info(void **state) {
    (void)state;
    const char *data = "info query test payload";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_get_info(repo, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

static void test_object_physical_size(void **state) {
    (void)state;
    const char *data = "physical size check";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(repo, hash, &bytes), OK);
    assert_true(bytes > 0);
    assert_true(bytes >= sizeof(object_header_t) + strlen(data));
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* object_exists: loose hit, then pack fallback after loose deletion */
static void test_exists_loose_and_pack_fallback(void **state) {
    (void)state;
    const char *data = "exists-loose-then-pack";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    /* Object is loose — should exist. */
    assert_true(object_exists(repo, hash));

    /* Pack the loose object. */
    assert_int_equal(repo_pack(repo, NULL, NULL, NULL), OK);

    /* Now delete the loose file. */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
    unlink(path);

    /* Should still exist via pack fallback. */
    assert_true(object_exists(repo, hash));
}

/* object_get_info: via loose and packed paths */
static void test_get_info_loose_and_packed(void **state) {
    (void)state;
    const char *data = "get-info-both-paths";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    /* Loose path */
    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_get_info(repo, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);

    /* Pack, delete loose */
    assert_int_equal(repo_pack(repo, NULL, NULL, NULL), OK);
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
    unlink(path);

    /* Packed path */
    sz = 0;
    typ = 0;
    assert_int_equal(object_get_info(repo, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

/* object_load_stream for COMPRESS_NONE (small object, should still work) */
static void test_load_stream_compress_none(void **state) {
    (void)state;
    /* 64 KiB incompressible → COMPRESS_NONE */
    size_t len = 65536;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    unsigned int seed = 99;
    for (size_t i = 0; i < len; i++) {
        seed = seed * 1103515245 + 12345;
        data[i] = (uint8_t)(seed >> 16);
    }

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    /* Stream load to /dev/null */
    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    uint64_t stream_sz = 0;
    uint8_t obj_type = 0;
    assert_int_equal(object_load_stream(repo, hash, null_fd, &stream_sz, &obj_type), OK);
    close(null_fd);
    assert_true(stream_sz == len);
    assert_int_equal(obj_type, OBJECT_TYPE_FILE);

    free(data);
}

/* object_store_ex: is_new=1 first, is_new=0 on dedup; phys_bytes populated */
static void test_store_ex_is_new_and_phys(void **state) {
    (void)state;
    const char *data = "store_ex dedup and phys test";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    int is_new = 0;
    uint64_t phys = 0;

    assert_int_equal(object_store_ex(repo, OBJECT_TYPE_FILE, data, strlen(data),
                                      hash, &is_new, &phys), OK);
    assert_true(is_new == 1);
    assert_true(phys > 0);

    /* Store again — should be dedup'd */
    uint8_t hash2[OBJECT_HASH_SIZE] = {0};
    int is_new2 = 0;
    uint64_t phys2 = 0;
    assert_int_equal(object_store_ex(repo, OBJECT_TYPE_FILE, data, strlen(data),
                                      hash2, &is_new2, &phys2), OK);
    assert_memory_equal(hash, hash2, OBJECT_HASH_SIZE);
    assert_true(is_new2 == 0);
}

/* object_store_fd: store via fd, load back, verify content */
static void test_store_fd_roundtrip(void **state) {
    (void)state;
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    const char *path = TEST_SRC "/fd_test.bin";
    const char *data = "object_store_fd roundtrip test content";
    size_t len = strlen(data);

    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fwrite(data, 1, len, f);
    fclose(f);

    /* Compute expected hash by storing via object_store first */
    uint8_t expected_hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, expected_hash), OK);

    /* Delete the loose object so store_fd can re-create */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(expected_hash, hex);
    char obj_path[512];
    snprintf(obj_path, sizeof(obj_path), "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
    unlink(obj_path);

    /* Store via fd */
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(object_store_fd(repo, OBJECT_TYPE_FILE, fd, (uint64_t)len,
                                      expected_hash), OK);
    close(fd);

    /* Load back and verify */
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, expected_hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* object_store_fd: wrong hash → ERR_CORRUPT */
static void test_store_fd_hash_mismatch(void **state) {
    (void)state;
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    const char *path = TEST_SRC "/fd_mismatch.bin";
    const char *data = "this will have wrong hash";
    size_t len = strlen(data);

    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fwrite(data, 1, len, f);
    fclose(f);

    /* Use a bogus hash */
    uint8_t bad_hash[OBJECT_HASH_SIZE];
    memset(bad_hash, 0xAA, OBJECT_HASH_SIZE);

    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    status_t st = object_store_fd(repo, OBJECT_TYPE_FILE, fd, (uint64_t)len, bad_hash);
    close(fd);
    assert_int_equal(st, ERR_CORRUPT);
}

/* ================================================================== */
/* Coverage: object_load_stream pack fallback paths                    */
/* ================================================================== */

/* object_load_stream: loose object missing, falls back to pack path.
 * Exercises object_load_stream lines 896–903 (pack fallback). */
static void test_load_stream_pack_fallback(void **state) {
    (void)state;
    const char *data = "load-stream-pack-fallback-test";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    /* Pack the object (also deletes loose) */
    assert_int_equal(repo_pack(repo, NULL, NULL, NULL), OK);

    /* Verify loose file is gone */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
    struct stat st;
    assert_int_not_equal(stat(path, &st), 0);  /* should not exist */

    /* Stream load should fall through to pack_object_load_stream */
    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    uint64_t ssz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load_stream(repo, hash, null_fd, &ssz, &typ), OK);
    close(null_fd);
    assert_true(ssz == len);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

/* object_load_stream: pack fallback with NULL out_type and out_size */
static void test_load_stream_pack_fallback_null_params(void **state) {
    (void)state;
    const char *data = "null-params-pack-fallback";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(repo_pack(repo, NULL, NULL, NULL), OK);

    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    /* Pass NULL for both out_size and out_type */
    assert_int_equal(object_load_stream(repo, hash, null_fd, NULL, NULL), OK);
    close(null_fd);
}

/* object_load_stream: non-existent hash → ERR_NOT_FOUND */
static void test_load_stream_missing_hash(void **state) {
    (void)state;
    uint8_t bad_hash[OBJECT_HASH_SIZE];
    memset(bad_hash, 0xBB, OBJECT_HASH_SIZE);

    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    status_t st = object_load_stream(repo, bad_hash, null_fd, NULL, NULL);
    close(null_fd);
    assert_int_not_equal(st, OK);
}

/* object_load: pack fallback for multiple objects with different sizes */
static void test_load_from_pack_various_sizes(void **state) {
    (void)state;
    /* Store objects of various sizes to test different pack behaviors */
    const char *small = "x";
    char medium[4096];
    memset(medium, 'M', sizeof(medium));
    char large[65536];
    unsigned int seed = 77;
    for (size_t i = 0; i < sizeof(large); i++) {
        seed = seed * 1103515245 + 12345;
        large[i] = (char)(seed >> 16);
    }

    uint8_t h_small[OBJECT_HASH_SIZE], h_medium[OBJECT_HASH_SIZE], h_large[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, small, 1, h_small), OK);
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, medium, sizeof(medium), h_medium), OK);
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, large, sizeof(large), h_large), OK);

    assert_int_equal(repo_pack(repo, NULL, NULL, NULL), OK);

    /* Load all three from pack */
    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(repo, h_small, &data, &sz, NULL), OK);
    assert_int_equal(sz, 1);
    free(data);

    assert_int_equal(object_load(repo, h_medium, &data, &sz, NULL), OK);
    assert_int_equal(sz, sizeof(medium));
    free(data);

    assert_int_equal(object_load(repo, h_large, &data, &sz, NULL), OK);
    assert_int_equal(sz, sizeof(large));
    free(data);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* Existing (from test_data_paths.c) */
        cmocka_unit_test_setup_teardown(test_object_empty, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_tiny, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_compressible_10k, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_incompressible_64k, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_dedup, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_exists_check, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_get_info, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_physical_size, setup_basic, teardown_basic),

        /* New coverage tests */
        cmocka_unit_test_setup_teardown(test_exists_loose_and_pack_fallback, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_get_info_loose_and_packed, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_stream_compress_none, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_ex_is_new_and_phys, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_fd_roundtrip, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_fd_hash_mismatch, setup_basic, teardown_basic),

        /* Pack fallback coverage */
        cmocka_unit_test_setup_teardown(test_load_stream_pack_fallback, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_stream_pack_fallback_null_params, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_stream_missing_hash, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_from_pack_various_sizes, setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
