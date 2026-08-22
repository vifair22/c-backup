#define _POSIX_C_SOURCE 200809L

/*
 * Manifest tree determinism.
 *
 * This is the canary for the whole dedup scheme. If tree serialisation stops
 * being byte-stable, every directory gets a fresh hash on every backup, the
 * repo grows as if manifests were monolithic again, and absolutely nothing
 * fails: backups still succeed, restores still work, write volume just
 * quietly goes up. Measured baseline is 3.3% new tree objects per incremental
 * snapshot. These tests are what stands between that number and 100%.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>

#include "manifest.h"
#include "object.h"
#include "store.h"

#define TEST_ROOT "/tmp/c_backup_test_manifest"

static store_t *store;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_ROOT);
    if (store_init_dirs(TEST_ROOT) != OK) return -1;
    if (store_open(TEST_ROOT, NULL, &store) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_close(store);
    store = NULL;
    rmrf(TEST_ROOT);
    return 0;
}

static manifest_entry_t mk(const char *name, uint8_t type, uint64_t size)
{
    manifest_entry_t e;
    memset(&e, 0, sizeof(e));
    e.name           = name;
    e.name_len       = (uint16_t)strlen(name);
    e.type           = type;
    e.mode           = 0644;
    e.uid            = 1000;
    e.gid            = 1000;
    e.size           = size;
    e.mtime_sec      = 1700000000;
    e.mtime_nsec     = 123456789;
    e.inode_identity = 42;
    memset(e.target_hash, 0xAB, OBJECT_HASH_SIZE);
    return e;
}

/* ------------------------------------------------------------------ */

static void test_roundtrip(void **state)
{
    (void)state;
    manifest_entry_t in[2] = { mk("alpha.txt", NODE_TYPE_REG, 100),
                               mk("beta.txt",  NODE_TYPE_REG, 200) };
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, in, 2, hash), OK);

    manifest_entry_t *out = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, hash, &out, &n), OK);
    assert_int_equal(n, 2);
    assert_string_equal(out[0].name, "alpha.txt");
    assert_string_equal(out[1].name, "beta.txt");
    assert_int_equal(out[0].size, 100);
    assert_int_equal(out[1].size, 200);
    assert_int_equal(out[0].mtime_nsec, 123456789);
    assert_int_equal(out[0].inode_identity, 42);
    manifest_free_entries(out, n);
}

/* The same directory serialised twice must be byte-identical. */
static void test_serialisation_is_stable(void **state)
{
    (void)state;
    manifest_entry_t a[2] = { mk("one", NODE_TYPE_REG, 1), mk("two", NODE_TYPE_REG, 2) };
    manifest_entry_t b[2] = { mk("one", NODE_TYPE_REG, 1), mk("two", NODE_TYPE_REG, 2) };

    uint8_t *ba = NULL, *bb = NULL;
    size_t la = 0, lb = 0;
    assert_int_equal(manifest_serialise(a, 2, &ba, &la), OK);
    assert_int_equal(manifest_serialise(b, 2, &bb, &lb), OK);
    assert_int_equal(la, lb);
    assert_memory_equal(ba, bb, la);
    free(ba); free(bb);
}

/* Input order must not matter: the writer sorts, so a scan that yields
 * entries in readdir order produces the same tree as a sorted one. */
static void test_input_order_does_not_matter(void **state)
{
    (void)state;
    manifest_entry_t fwd[3] = { mk("a", NODE_TYPE_REG, 1),
                                mk("b", NODE_TYPE_REG, 2),
                                mk("c", NODE_TYPE_REG, 3) };
    manifest_entry_t rev[3] = { mk("c", NODE_TYPE_REG, 3),
                                mk("b", NODE_TYPE_REG, 2),
                                mk("a", NODE_TYPE_REG, 1) };
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, fwd, 3, h1), OK);
    assert_int_equal(manifest_write_tree(store, rev, 3, h2), OK);
    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
}

/*
 * Sorting is memcmp byte order, not locale collation. Under a locale-aware
 * comparison "a" and "B" can order either way depending on the environment
 * of whichever process happened to write the tree.
 */
static void test_sort_is_byte_order(void **state)
{
    (void)state;
    manifest_entry_t in[3] = { mk("b", NODE_TYPE_REG, 1),
                               mk("A", NODE_TYPE_REG, 2),
                               mk("a", NODE_TYPE_REG, 3) };
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, in, 3, hash), OK);

    manifest_entry_t *out = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, hash, &out, &n), OK);
    assert_int_equal(n, 3);
    /* 'A' is 0x41, 'a' is 0x61, 'b' is 0x62. */
    assert_string_equal(out[0].name, "A");
    assert_string_equal(out[1].name, "a");
    assert_string_equal(out[2].name, "b");
    manifest_free_entries(out, n);
}

/*
 * THE canary: writing an identical tree twice must store zero new objects.
 * If this fails, dedup is broken and every other test still passes.
 */
static void test_identical_tree_stores_nothing_new(void **state)
{
    (void)state;
    manifest_entry_t in[2] = { mk("x", NODE_TYPE_REG, 10), mk("y", NODE_TYPE_REG, 20) };

    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, in, 2, h1), OK);

    manifest_entry_t again[2] = { mk("x", NODE_TYPE_REG, 10), mk("y", NODE_TYPE_REG, 20) };
    uint8_t *buf = NULL; size_t len = 0;
    assert_int_equal(manifest_serialise(again, 2, &buf, &len), OK);

    int is_new = 1;
    assert_int_equal(object_store_ex(store, OBJECT_TYPE_TREE, buf, len, h2,
                                     &is_new, NULL), OK);
    free(buf);

    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
    assert_int_equal(is_new, 0);
}

/* Any change to any field must change the hash, or a modified directory
 * would be mistaken for an unchanged one and silently not backed up. */
static void test_every_field_affects_the_hash(void **state)
{
    (void)state;
    manifest_entry_t base = mk("f", NODE_TYPE_REG, 100);
    uint8_t h_base[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &base, 1, h_base), OK);

    struct { const char *what; manifest_entry_t e; } vary[] = {
        { "size",      mk("f", NODE_TYPE_REG, 101) },
        { "type",      mk("f", NODE_TYPE_DIR, 100) },
        { "name",      mk("g", NODE_TYPE_REG, 100) },
    };
    for (size_t i = 0; i < sizeof(vary) / sizeof(vary[0]); i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        assert_int_equal(manifest_write_tree(store, &vary[i].e, 1, h), OK);
        assert_memory_not_equal(h, h_base, OBJECT_HASH_SIZE);
    }

    manifest_entry_t m = mk("f", NODE_TYPE_REG, 100);
    m.mode = 0755;
    uint8_t hm[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &m, 1, hm), OK);
    assert_memory_not_equal(hm, h_base, OBJECT_HASH_SIZE);

    manifest_entry_t t = mk("f", NODE_TYPE_REG, 100);
    t.mtime_sec = 1700000001;
    uint8_t ht[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &t, 1, ht), OK);
    assert_memory_not_equal(ht, h_base, OBJECT_HASH_SIZE);

    manifest_entry_t c = mk("f", NODE_TYPE_REG, 100);
    memset(c.target_hash, 0xCD, OBJECT_HASH_SIZE);
    uint8_t hc[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &c, 1, hc), OK);
    assert_memory_not_equal(hc, h_base, OBJECT_HASH_SIZE);
}

/* A directory whose child subtree changed must get a new hash: that is how
 * a change propagates to the root and makes the snapshot distinguishable. */
static void test_subtree_change_propagates(void **state)
{
    (void)state;
    manifest_entry_t child_a = mk("inner", NODE_TYPE_REG, 1);
    uint8_t hca[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &child_a, 1, hca), OK);

    manifest_entry_t child_b = mk("inner", NODE_TYPE_REG, 2);
    uint8_t hcb[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &child_b, 1, hcb), OK);
    assert_memory_not_equal(hca, hcb, OBJECT_HASH_SIZE);

    manifest_entry_t pa = mk("sub", NODE_TYPE_DIR, 0);
    memcpy(pa.target_hash, hca, OBJECT_HASH_SIZE);
    uint8_t hpa[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &pa, 1, hpa), OK);

    manifest_entry_t pb = mk("sub", NODE_TYPE_DIR, 0);
    memcpy(pb.target_hash, hcb, OBJECT_HASH_SIZE);
    uint8_t hpb[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &pb, 1, hpb), OK);

    assert_memory_not_equal(hpa, hpb, OBJECT_HASH_SIZE);
}

static void test_empty_tree(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, NULL, 0, hash), OK);

    manifest_entry_t *out = NULL;
    uint32_t n = 1;
    assert_int_equal(manifest_read_tree(store, hash, &out, &n), OK);
    assert_int_equal(n, 0);
    manifest_free_entries(out, n);
}

static void test_symlink_target_survives(void **state)
{
    (void)state;
    manifest_entry_t e = mk("link", NODE_TYPE_SYMLINK, 0);
    e.link_target = "../elsewhere/target";
    e.link_target_len = (uint16_t)strlen(e.link_target);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &e, 1, hash), OK);

    manifest_entry_t *out = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, hash, &out, &n), OK);
    assert_int_equal(n, 1);
    assert_string_equal(out[0].link_target, "../elsewhere/target");
    manifest_free_entries(out, n);
}

/* A truncated or foreign object must be rejected, not misparsed into
 * plausible-looking garbage. */
static void test_rejects_non_tree_object(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "not a tree", 10, hash), OK);

    manifest_entry_t *out = NULL;
    uint32_t n = 0;
    assert_int_not_equal(manifest_read_tree(store, hash, &out, &n), OK);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_serialisation_is_stable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_input_order_does_not_matter, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sort_is_byte_order, setup, teardown),
        cmocka_unit_test_setup_teardown(test_identical_tree_stores_nothing_new, setup, teardown),
        cmocka_unit_test_setup_teardown(test_every_field_affects_the_hash, setup, teardown),
        cmocka_unit_test_setup_teardown(test_subtree_change_propagates, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_tree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlink_target_survives, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rejects_non_tree_object, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
