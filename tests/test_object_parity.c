#define _POSIX_C_SOURCE 200809L

/*
 * Loose object parity: detection and repair of single-byte corruption.
 *
 * Every object carries a XOR record over its header and Reed-Solomon parity
 * over its payload. That costs disk on every object ever written, so it has
 * to actually work: the tests here flip bytes on disk and require the read to
 * come back correct, and require two-byte header damage to be reported rather
 * than silently mis-decoded. A parity scheme that fails open is worse than no
 * parity, because it also fails quietly.
 *
 * Ported from the pre-daemon suite. Its snapshot-file parity tests are gone
 * with the .snap format; manifest trees are ordinary objects, so the tree case
 * is covered here by storing one and corrupting it like any other object.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "fake_index.h"
#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"

#define TEST_REPO "/tmp/c_backup_test_object_parity"

static store_t *store;

static void cleanup(void)
{
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
}

static int setup(void **state)
{
    (void)state;
    cleanup();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    parity_stats_reset();
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (store) { store_close(store); store = NULL; }
    cleanup();
    return 0;
}

static void object_path(const uint8_t hash[OBJECT_HASH_SIZE], char out[512])
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    snprintf(out, 512, "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
}

static void flip_byte(const char *path, off_t off)
{
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    unsigned char b = 0;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0x01;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
}

static void store_and_path(const char *data, uint8_t hash[OBJECT_HASH_SIZE],
                           char path[512])
{
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    object_path(hash, path);
}

static void assert_loads_as(const uint8_t hash[OBJECT_HASH_SIZE], const char *data)
{
    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), OK);
    assert_int_equal(len, strlen(data));
    assert_memory_equal(out, data, len);
    free(out);
}

/* ------------------------------------------------------------------ */

/* A clean object must take the CRC fast path: no RS decode, no repair. */
static void test_clean_object_needs_no_repair(void **state)
{
    (void)state;
    const char *data = "clean object, CRC should match on the first try";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    parity_stats_reset();
    assert_loads_as(hash, data);

    parity_stats_t ps = parity_stats_get();
    assert_int_equal((int)ps.repaired, 0);
    assert_int_equal((int)ps.uncorrectable, 0);
}

/* A flipped payload byte is repaired in memory by the RS decode. */
static void test_payload_corruption_is_repaired_on_read(void **state)
{
    (void)state;
    const char *data = "payload corruption should be transparently repaired";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, (off_t)sizeof(object_header_t) + 5);

    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_true(parity_stats_get().repaired > 0);
}

/* A flipped header byte is repaired by the XOR record. Header damage is the
 * more dangerous case: a bad compressed_size would misread the payload. */
static void test_header_corruption_is_repaired_on_read(void **state)
{
    (void)state;
    const char *data = "header corruption repaired via the XOR record";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, 6);

    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_true(parity_stats_get().repaired > 0);
}

/*
 * Two damaged header bytes exceed what a single XOR record can correct. The
 * load must fail rather than return a plausible-looking wrong answer.
 */
static void test_double_header_corruption_is_reported(void **state)
{
    (void)state;
    const char *data = "two flipped header bytes are beyond XOR";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, 10);
    flip_byte(path, 20);

    void *out = NULL;
    size_t len = 0;
    assert_int_not_equal(object_load(store, hash, &out, &len, NULL), OK);
    err_clear();
    free(out);
}

/* object_repair writes the correction back, so the damage does not have to be
 * re-corrected on every subsequent read. */
static void test_repair_persists_the_correction(void **state)
{
    (void)state;
    const char *data = "repair should persist, not just fix the read";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, 10);
    flip_byte(path, (off_t)sizeof(object_header_t) + 3);

    assert_true(object_repair(store, hash) > 0);

    /* Now the read is clean: nothing left for parity to correct. */
    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

static void test_repair_of_clean_object_is_a_noop(void **state)
{
    (void)state;
    const char *data = "nothing wrong with this one";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    assert_int_equal(object_repair(store, hash), 0);
}

/*
 * Objects written before the parity trailer existed still have to load. The
 * repo predates the rewrite in the field, and refusing to read v1 objects
 * would strand data that is perfectly intact.
 */
static void test_v1_object_without_parity_still_loads(void **state)
{
    (void)state;
    const char *data = "a v1 object carries no parity trailer at all";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    uint8_t *buf = malloc((size_t)sb.st_size);
    assert_non_null(buf);
    assert_int_equal(read(fd, buf, (size_t)sb.st_size), (ssize_t)sb.st_size);
    close(fd);

    object_header_t *hdr = (object_header_t *)buf;
    size_t trunc = sizeof(object_header_t) + (size_t)hdr->compressed_size;
    hdr->version = 1;

    fd = open(path, O_WRONLY | O_TRUNC);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, buf, trunc), (ssize_t)trunc);
    close(fd);
    free(buf);

    assert_loads_as(hash, data);
}

/*
 * Manifest trees are objects like any other, which is the whole point of
 * content-addressing them: a corrupted tree is repaired by the same machinery
 * that repairs file content. Losing a tree would orphan every file under it,
 * so this is worth asserting directly rather than inferring.
 */
static void test_tree_object_corruption_is_repaired(void **state)
{
    (void)state;
    uint8_t content[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "leaf", 4, content), OK);

    manifest_entry_t e;
    memset(&e, 0, sizeof(e));
    e.name     = "leaf.txt";
    e.name_len = 8;
    e.type     = NODE_TYPE_REG;
    e.size     = 4;
    e.mode     = 0644;
    memcpy(e.target_hash, content, OBJECT_HASH_SIZE);

    uint8_t tree[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &e, 1, tree), OK);

    char path[512];
    object_path(tree, path);
    flip_byte(path, (off_t)sizeof(object_header_t) + 2);

    parity_stats_reset();
    manifest_entry_t *out = NULL;
    uint32_t count = 0;
    assert_int_equal(manifest_read_tree(store, tree, &out, &count), OK);
    assert_int_equal(count, 1);
    assert_string_equal(out[0].name, "leaf.txt");
    manifest_free_entries(out, count);
    assert_true(parity_stats_get().repaired > 0);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_clean_object_needs_no_repair, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_is_repaired_on_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_header_corruption_is_repaired_on_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_double_header_corruption_is_reported, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_persists_the_correction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_of_clean_object_is_a_noop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_v1_object_without_parity_still_loads, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tree_object_corruption_is_repaired, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
