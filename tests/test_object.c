#define _POSIX_C_SOURCE 200809L

/*
 * Objects in repository format 3: naming, the object bound, and the store's
 * own pack writer.
 *
 * The properties pinned here are the format's, so a regression in any of
 * them is a format break rather than a bug in one path:
 *
 *   - a name is SHA-256(type || payload), so equal bytes stored as two
 *     types are two objects (OWN-38: a file whose bytes are a valid tree
 *     serialisation must never be read back as that tree);
 *   - every object is at most OBJECT_MAX_BYTES, refused on write;
 *   - only the known types are written (SPARSE, 4, is retired);
 *   - everything lands in a pack: object_store stages, and a read, a flush
 *     or a close seals, after which a fresh handle finds it.
 */

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

#include <openssl/sha.h>

#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_object"

static store_t *store;

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    store_close(store);
    store = NULL;
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    return 0;
}

static void reopen(void)
{
    store_close(store);
    store = NULL;
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
}

static int count_packs(void)
{
    FILE *p = popen("find '" TEST_REPO "/packs' -name 'pack-*.dat' | wc -l", "r");
    assert_non_null(p);
    int n = -1;
    assert_int_equal(fscanf(p, "%d", &n), 1);
    pclose(p);
    return n;
}

static void test_store_and_load(void **state) {
    (void)state;
    const char *data = "Hello, snapshot backup!";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL; size_t out_len = 0; uint8_t type = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &type), OK);
    assert_int_equal(out_len, len);
    assert_int_equal(type, OBJECT_TYPE_FILE);
    assert_memory_equal(out, data, len);
    free(out);
}

static void test_empty_object(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "", 0, hash), OK);
    void *out = NULL; size_t out_len = 1;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 0);
    free(out);
}

static void test_dedup(void **state) {
    (void)state;
    const char *data = "duplicate content";
    size_t len = strlen(data);
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    int is_new = -1;

    assert_int_equal(object_store_ex(store, OBJECT_TYPE_FILE, data, len, h1, &is_new, NULL), OK);
    assert_int_equal(is_new, 1);
    /* Staged in the open pack: still a duplicate. */
    assert_int_equal(object_store_ex(store, OBJECT_TYPE_FILE, data, len, h2, &is_new, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);

    /* And once installed. */
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(object_store_ex(store, OBJECT_TYPE_FILE, data, len, h2, &is_new, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_int_equal(count_packs(), 1);
}

/* The name is SHA-256 over the type byte and then the payload, nothing
 * else: computed here independently of object_name. */
static void test_name_is_sha256_of_type_and_payload(void **state) {
    (void)state;
    const char *data = "named by what it is and what it holds";
    size_t len = strlen(data);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_XATTR, data, len, hash), OK);

    uint8_t buf[256];
    buf[0] = OBJECT_TYPE_XATTR;
    memcpy(buf + 1, data, len);
    uint8_t want[OBJECT_HASH_SIZE];
    SHA256(buf, len + 1, want);
    assert_memory_equal(hash, want, OBJECT_HASH_SIZE);

    uint8_t direct[OBJECT_HASH_SIZE];
    object_name(OBJECT_TYPE_XATTR, data, len, direct);
    assert_memory_equal(direct, want, OBJECT_HASH_SIZE);

    /* The bare payload digest, format 2's name, is not it. */
    uint8_t bare[OBJECT_HASH_SIZE];
    SHA256((const uint8_t *)data, len, bare);
    assert_memory_not_equal(hash, bare, OBJECT_HASH_SIZE);
}

/* Equal bytes, two types: two names, two objects, each loading as its own
 * type. */
static void test_same_bytes_two_types_are_two_objects(void **state) {
    (void)state;
    const char *data = "same bytes, different meaning";
    size_t len = strlen(data);
    uint8_t hf[OBJECT_HASH_SIZE], ha[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hf), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_ACL, data, len, ha), OK);
    assert_memory_not_equal(hf, ha, OBJECT_HASH_SIZE);

    uint8_t t = 0;
    void *out = NULL; size_t n = 0;
    assert_int_equal(object_load(store, hf, &out, &n, &t), OK);
    assert_int_equal(t, OBJECT_TYPE_FILE);
    free(out);
    assert_int_equal(object_load(store, ha, &out, &n, &t), OK);
    assert_int_equal(t, OBJECT_TYPE_ACL);
    free(out);
}

/*
 * OWN-38: the attack is a file whose content is the serialisation of a
 * directory tree the attacker chose. Named by payload alone it was the same
 * object as that tree, so backing up the file planted the tree - and a
 * restore or gc walking a snapshot met the attacker's directory wherever the
 * real tree had that name. With the type in the name the file is its own
 * object and never reads as a tree.
 */
static void test_file_with_a_trees_bytes_is_not_the_tree(void **state) {
    (void)state;
    manifest_entry_t e[1];
    memset(e, 0, sizeof(e));
    e[0].name = "planted";
    e[0].name_len = 7;
    e[0].type = NODE_TYPE_REG;
    e[0].mode = 0100644;
    e[0].size = 5;
    memset(e[0].target_hash, 0xAB, OBJECT_HASH_SIZE);

    uint8_t tree[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, e, 1, tree), OK);
    void *bytes = NULL; size_t n = 0; uint8_t t = 0;
    assert_int_equal(object_load(store, tree, &bytes, &n, &t), OK);
    assert_int_equal(t, OBJECT_TYPE_TREE);

    /* The same bytes, backed up as somebody's file. */
    uint8_t file[OBJECT_HASH_SIZE];
    int is_new = 0;
    assert_int_equal(object_store_ex(store, OBJECT_TYPE_FILE, bytes, n, file, &is_new, NULL), OK);
    assert_int_equal(is_new, 1);
    assert_memory_not_equal(file, tree, OBJECT_HASH_SIZE);
    free(bytes);

    /* The file never reads as a tree, and the tree is still the tree. */
    manifest_entry_t *got = NULL;
    uint32_t cnt = 0;
    assert_int_not_equal(manifest_read_tree(store, file, &got, &cnt), OK);
    err_clear();
    assert_int_equal(manifest_read_tree(store, tree, &got, &cnt), OK);
    assert_int_equal(cnt, 1);
    manifest_free_entries(got, cnt);
}

static void test_only_known_types_are_written(void **state) {
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE];
    /* 4 was SPARSE, retired with format 3; 7 has never existed. */
    assert_int_equal(object_store(store, 4, "x", 1, h), ERR_INVALID);
    err_clear();
    assert_int_equal(object_store(store, 7, "x", 1, h), ERR_INVALID);
    err_clear();
    assert_int_equal(object_store(store, 0, "x", 1, h), ERR_INVALID);
    err_clear();
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, "list", 4, h), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_TREE, "tree", 4, h), OK);
}

/* The bound is exact: OBJECT_MAX_BYTES is stored and read back, one byte
 * more is refused, and nothing is written for the refusal. */
static void test_object_bound_on_write(void **state) {
    (void)state;
    size_t max = (size_t)OBJECT_MAX_BYTES;
    uint8_t *buf = malloc(max + 1);
    assert_non_null(buf);
    for (size_t i = 0; i < max + 1; i++) buf[i] = (uint8_t)(i * 2654435761u >> 13);

    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, buf, max + 1, h), ERR_TOO_LARGE);
    err_clear();
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(count_packs(), 0);

    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, buf, max, h), OK);
    void *out = NULL; size_t n = 0;
    assert_int_equal(object_load(store, h, &out, &n, NULL), OK);
    assert_int_equal(n, max);
    assert_memory_equal(out, buf, max);
    free(out);
    free(buf);
}

/* A file larger than one object is the chunker's; until it is wired in the
 * whole-file store refuses it rather than writing past the bound. */
static void test_file_past_the_bound_is_refused(void **state) {
    (void)state;
    char path[] = TEST_TMP "/c_backup_test_object_big.XXXXXX";
    int fd = mkstemp(path);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)OBJECT_MAX_BYTES + 1), 0);
    uint8_t h[OBJECT_HASH_SIZE];
    object_write_t w = { 0 };
    assert_int_equal(object_store_file_packed(store, fd, OBJECT_MAX_BYTES + 1, NULL, h, &w),
                     ERR_TOO_LARGE);
    err_clear();
    assert_int_equal(ftruncate(fd, 4096), 0);
    assert_int_equal(object_store_file_packed(store, fd, 4096, NULL, h, &w), OK);
    assert_int_equal(w.is_new, 1);
    assert_int_equal(w.payload_size, 4096);
    close(fd);
    unlink(path);
}

/* Staged objects are visible at once; closing seals them, and a new handle
 * with nothing but the path finds them through its own index. */
static void test_close_seals_and_reopen_finds(void **state) {
    (void)state;
    uint8_t h[8][OBJECT_HASH_SIZE];
    char buf[64];
    for (int i = 0; i < 8; i++) {
        int n = snprintf(buf, sizeof(buf), "object number %d", i);
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, buf, (size_t)n, h[i]), OK);
        assert_true(object_exists(store, h[i]));
    }
    assert_int_equal(count_packs(), 0);
    reopen();
    assert_int_equal(count_packs(), 1);
    for (int i = 0; i < 8; i++) {
        assert_true(object_exists(store, h[i]));
        void *out = NULL; size_t n = 0;
        assert_int_equal(object_load(store, h[i], &out, &n, NULL), OK);
        int want = snprintf(buf, sizeof(buf), "object number %d", i);
        assert_int_equal(n, (size_t)want);
        assert_memory_equal(out, buf, n);
        free(out);
    }
}

static void test_physical_size_counts_the_entry_header(void **state) {
    (void)state;
    const char *data = "physical size of a packed entry";
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), h), OK);
    uint64_t phys = 0;
    uint32_t hdr = 0;
    assert_int_equal(object_physical_size(store, h, &phys, &hdr), OK);
    assert_int_equal(hdr, sizeof(pack_dat_entry_hdr_t));
    /* Unencrypted and too small to compress: header plus payload. */
    assert_int_equal(phys, sizeof(pack_dat_entry_hdr_t) + strlen(data));

    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0x5A, sizeof(missing));
    assert_int_not_equal(object_physical_size(store, missing, &phys, NULL), OK);
    err_clear();
    assert_false(object_exists(store, missing));
}

/* Compressible content is compressed per object and still reads back. */
static void test_compressible_object_round_trips(void **state) {
    (void)state;
    size_t n = 1 << 20;
    char *buf = malloc(n);
    assert_non_null(buf);
    for (size_t i = 0; i < n; i++) buf[i] = "abcdefgh"[i % 8];
    uint8_t h[OBJECT_HASH_SIZE];
    object_write_t w = { 0 };
    assert_int_equal(object_store_packed(store, OBJECT_TYPE_FILE, buf, n, NULL, h, &w), OK);
    assert_true(w.phys_bytes < n / 10);
    void *out = NULL; size_t got = 0;
    assert_int_equal(object_load(store, h, &out, &got, NULL), OK);
    assert_int_equal(got, n);
    assert_memory_equal(out, buf, n);
    free(out);
    free(buf);
}

static void test_repo_init_requires_empty_dir(void **state) {
    (void)state;
    const char *path = TEST_TMP "/c_backup_test_object_nonempty";
    int rc = system("rm -rf " TEST_TMP "/c_backup_test_object_nonempty");
    (void)rc;
    assert_int_equal(mkdir(path, 0755), 0);
    FILE *f = fopen(TEST_TMP "/c_backup_test_object_nonempty/x", "w");
    assert_non_null(f);
    fclose(f);
    assert_int_not_equal(store_init_dirs(path), OK);
    err_clear();
    rc = system("rm -rf " TEST_TMP "/c_backup_test_object_nonempty");
    (void)rc;
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_store_and_load, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dedup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_name_is_sha256_of_type_and_payload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_same_bytes_two_types_are_two_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_with_a_trees_bytes_is_not_the_tree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_only_known_types_are_written, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_bound_on_write, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_past_the_bound_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_close_seals_and_reopen_finds, setup, teardown),
        cmocka_unit_test_setup_teardown(test_physical_size_counts_the_entry_header, setup, teardown),
        cmocka_unit_test_setup_teardown(test_compressible_object_round_trips, setup, teardown),
        cmocka_unit_test(test_repo_init_requires_empty_dir),
    };
    return cmocka_run_group_tests_name("object", tests, NULL, NULL);
}
