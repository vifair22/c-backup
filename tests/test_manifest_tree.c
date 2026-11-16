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
#include "test_tmp.h"

#define TEST_ROOT TEST_TMP "/c_backup_test_manifest"

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

/*
 * B118's backstop: a regular file with no content is refused, never written.
 * A tree is permanent and every snapshot naming it would carry an entry no
 * restore can produce. Other types legitimately have a zero target: a
 * symlink's payload is its target string, a device node's its numbers.
 */
/*
 * OWN-45-LIVE: the "never settled, may be torn" flag is a byte at the end
 * of an entry's fixed part. It round-trips, sits where the format says,
 * changes the tree's bytes (it is a property of what was stored), and only
 * a regular file may carry it, on write and on read.
 */
static void test_unsettled_flag_round_trips_and_is_canonical(void **state)
{
    (void)state;
    manifest_entry_t e[2] = { mk("calm", NODE_TYPE_REG, 1), mk("torn", NODE_TYPE_REG, 2) };
    e[1].flags = MANIFEST_FLAG_UNSETTLED;
    uint8_t *buf = NULL;
    size_t len = 0;
    assert_int_equal(manifest_serialise(e, 2, &buf, &len), OK);

    /* header 10 bytes; the flags byte closes the fixed part, before the
     * name. Entries sort by name, so "calm" is first. */
    const size_t fixed = 2 + 2 + 1 + 4 + 4 + 4 + 8 + 8 + 8 + 8 + 4 + 4 + 3 * OBJECT_HASH_SIZE + 1;
    assert_int_equal(buf[10 + fixed - 1], 0);
    assert_int_equal(buf[10 + fixed + 4 + fixed - 1], MANIFEST_FLAG_UNSETTLED);

    manifest_entry_t *out = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_parse(buf, len, &out, &n), OK);
    assert_int_equal(n, 2);
    assert_int_equal(out[0].flags, 0);
    assert_int_equal(out[1].flags, MANIFEST_FLAG_UNSETTLED);
    manifest_free_entries(out, n);

    /* The same entries without the flag are a different tree. */
    uint8_t *plain = NULL;
    size_t plen = 0;
    e[1].flags = 0;
    assert_int_equal(manifest_serialise(e, 2, &plain, &plen), OK);
    assert_int_equal(plen, len);
    assert_memory_not_equal(plain, buf, len);
    free(plain);

    /* A bit nobody knows, or a flag on a directory, is not canonical: read
     * as corrupt, and never written. */
    buf[10 + fixed + 4 + fixed - 1] = 0x80;
    assert_int_equal(manifest_parse(buf, len, &out, &n), ERR_CORRUPT);
    free(buf);
    manifest_entry_t d = mk("dir", NODE_TYPE_DIR, 0);
    d.flags = MANIFEST_FLAG_UNSETTLED;
    err_clear();
    assert_int_equal(manifest_serialise(&d, 1, &buf, &len), ERR_INVALID);
    e[1].flags = 0x02;
    assert_int_equal(manifest_serialise(e, 2, &buf, &len), ERR_INVALID);
    err_clear();
}

static void test_file_entry_without_content_is_refused(void **state)
{
    (void)state;
    manifest_entry_t e[2] = { mk("ok", NODE_TYPE_REG, 1), mk("hole", NODE_TYPE_REG, 2) };
    memset(e[1].target_hash, 0, OBJECT_HASH_SIZE);
    uint8_t *buf = NULL;
    size_t len = 0;
    err_clear();
    assert_int_equal(manifest_serialise(e, 2, &buf, &len), ERR_INVALID);
    assert_null(buf);
    assert_non_null(strstr(err_msg(), "'hole' has no content hash"));
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, e, 2, h), ERR_INVALID);
    err_clear();

    manifest_entry_t s = mk("link", NODE_TYPE_SYMLINK, 4);
    memset(s.target_hash, 0, OBJECT_HASH_SIZE);
    s.link_target = "dest";
    s.link_target_len = 4;
    assert_int_equal(manifest_serialise(&s, 1, &buf, &len), OK);
    free(buf);
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

/*
 * mtime_sec is signed in memory and the same u64 slot on disk.
 *
 * Before 1970 is a real timestamp, and an unsigned field turned it into
 * 1.8e19 on the way to the index. Making the field signed must not move a
 * single byte of any tree already written, or every directory gets a fresh
 * hash and dedup quietly resets: so the slot holds the two's complement
 * pattern, a positive time encodes exactly as it did, and a negative one
 * reads back as itself.
 */
static void test_mtime_is_signed_without_changing_the_bytes(void **state)
{
    (void)state;
    /* header (4 + 2 + 4), then name_len, link_len, type, mode, uid, gid,
     * size before the mtime. */
    const size_t at = 10 + 2 + 2 + 1 + 4 + 4 + 4 + 8;
    const int64_t times[] = { 1700000000, -315619200, -1 };

    for (size_t t = 0; t < sizeof(times) / sizeof(times[0]); t++) {
        manifest_entry_t e = mk("f", NODE_TYPE_REG, 1);
        e.mtime_sec = times[t];
        uint8_t *buf = NULL;
        size_t len = 0;
        assert_int_equal(manifest_serialise(&e, 1, &buf, &len), OK);
        assert_true(len >= at + 8);

        uint64_t want = (uint64_t)times[t];
        for (int i = 0; i < 8; i++)
            assert_int_equal(buf[at + (size_t)i], (uint8_t)(want >> (8 * i)));

        manifest_entry_t *out = NULL;
        uint32_t n = 0;
        assert_int_equal(manifest_parse(buf, len, &out, &n), OK);
        assert_int_equal(n, 1);
        assert_true(out[0].mtime_sec == times[t]);
        assert_int_equal(out[0].mtime_nsec, 123456789);
        manifest_free_entries(out, n);
        free(buf);
    }
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
 * Entries with the same name still order deterministically.
 *
 * The comparator sorted on the name alone and returned 0 when two names
 * matched, and qsort is not stable - so which of two same-named entries
 * came first depended on the order the caller passed them in, and the tree
 * hash with it. No directory contains two entries with the same name, so
 * nothing a scan produces reaches this. It matters because the promise in
 * manifest.h is unconditional and because the failure is the silent kind:
 * a tree that hashes differently for the same contents rewrites itself on
 * every backup and the only symptom is a repository that will not stop
 * growing.
 *
 * Found by fuzz_manifest_write, which reverses its entry list and requires
 * the bytes not to move.
 */
static void test_duplicate_names_still_order_deterministically(void **state)
{
    (void)state;
    manifest_entry_t fwd[3] = { mk("dup", NODE_TYPE_REG, 1),
                                mk("dup", NODE_TYPE_REG, 2),
                                mk("dup", NODE_TYPE_REG, 3) };
    manifest_entry_t rev[3] = { mk("dup", NODE_TYPE_REG, 3),
                                mk("dup", NODE_TYPE_REG, 2),
                                mk("dup", NODE_TYPE_REG, 1) };

    uint8_t *bf = NULL, *br = NULL;
    size_t lf = 0, lr = 0;
    assert_int_equal(manifest_serialise(fwd, 3, &bf, &lf), OK);
    assert_int_equal(manifest_serialise(rev, 3, &br, &lr), OK);
    assert_int_equal(lf, lr);
    assert_memory_equal(bf, br, lf);
    free(bf); free(br);
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
/* The device pair is part of the hash, or two different devices with the
 * same name would dedup to one tree. */
static void test_device_numbers_affect_the_hash(void **state)
{
    (void)state;
    manifest_entry_t e = { 0 };
    e.name = "null";
    e.name_len = 4;
    e.type = NODE_TYPE_CHR;
    e.mode = 020666;
    e.dev_major = 1;
    e.dev_minor = 3;

    uint8_t a[OBJECT_HASH_SIZE], b[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &e, 1, a), OK);
    e.dev_minor = 5;   /* /dev/zero rather than /dev/null */
    assert_int_equal(manifest_write_tree(store, &e, 1, b), OK);
    assert_memory_not_equal(a, b, OBJECT_HASH_SIZE);
}

static void test_rejects_non_tree_object(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "not a tree", 10, hash), OK);

    manifest_entry_t *out = NULL;
    uint32_t n = 0;
    assert_int_not_equal(manifest_read_tree(store, hash, &out, &n), OK);
}

/* ------------------------------------------------------------------ */
/* Directories of any size (OWN-34)                                    */
/* ------------------------------------------------------------------ */

#define BIG_N 120000u

/* BIG_N entries named f0000000..., every third with a symlink target so the
 * entries are not all one size. Names live in one block. */
static manifest_entry_t *big_dir(uint32_t n, char **names_out)
{
    char *names = malloc((size_t)n * 16);
    manifest_entry_t *e = calloc(n, sizeof(*e));
    assert_non_null(names);
    assert_non_null(e);
    for (uint32_t i = 0; i < n; i++) {
        char *nm = names + (size_t)i * 16;
        snprintf(nm, 16, "f%07u", i * 2);
        e[i] = mk(nm, (i % 3) ? NODE_TYPE_REG : NODE_TYPE_SYMLINK, i);
        if (!(i % 3)) {
            e[i].link_target = "../some/where/else";
            e[i].link_target_len = 18;
        }
        e[i].target_hash[0] = (uint8_t)i;
        e[i].target_hash[1] = (uint8_t)(i >> 8);
    }
    *names_out = names;
    return e;
}

typedef struct {
    unsigned trees, lists;
    uint64_t largest;
} objs_t;

static void count_object(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type)
{
    objs_t *o = ctx;
    if (type == OBJECT_TYPE_TREE) o->trees++; else o->lists++;
    uint64_t size = 0;
    uint8_t t = 0;
    assert_int_equal(object_get_info(store, hash, &size, &t), OK);
    assert_int_equal(t, type);
    if (size > o->largest) o->largest = size;
}

static void test_large_directory_fragments(void **state)
{
    (void)state;
    char *names = NULL;
    manifest_entry_t *e = big_dir(BIG_N, &names);
    cdc_params_t cp;
    cdc_params_default(&cp);
    obj_sink_t sink = obj_sink_store(store);
    uint8_t h[OBJECT_HASH_SIZE];
    manifest_store_stats_t ms;
    assert_int_equal(manifest_store_tree(e, BIG_N, &cp, &sink, h, &ms), OK);
    printf("%u entries: %llu fragments, %llu objects\n", BIG_N,
           (unsigned long long)ms.fragments, (unsigned long long)ms.objects);
    assert_true(ms.fragments > 1);

    uint64_t size = 0;
    uint8_t type = 0;
    assert_int_equal(object_get_info(store, h, &size, &type), OK);
    assert_int_equal(type, OBJECT_TYPE_CHUNKLIST);

    /* Read back one fragment at a time, every object bounded */
    obj_loader_t ld = obj_loader_store(store);
    manifest_iter_t *it = NULL;
    objs_t o;
    memset(&o, 0, sizeof(o));
    assert_int_equal(manifest_iter_open(&ld, h, count_object, &o, &it), OK);
    uint32_t n = 0;
    for (;;) {
        const manifest_entry_t *x = NULL;
        int done = 0;
        assert_int_equal(manifest_iter_next(it, &x, &done), OK);
        if (done) break;
        char want[16];
        snprintf(want, sizeof(want), "f%07u", n * 2);
        assert_string_equal(x->name, want);
        assert_true(x->size == n);
        n++;
    }
    manifest_iter_close(it);
    assert_int_equal(n, BIG_N);
    assert_true(o.trees == ms.fragments);
    assert_true(o.largest <= MANIFEST_FRAG_MAX);

    /* And whole, for the callers that want an array */
    manifest_entry_t *all = NULL;
    uint32_t cnt = 0;
    assert_int_equal(manifest_read_tree(store, h, &all, &cnt), OK);
    assert_int_equal(cnt, BIG_N);
    assert_string_equal(all[BIG_N - 1].name, e[BIG_N - 1].name);
    manifest_free_entries(all, cnt);

    free(e);
    free(names);
}

static void test_large_directory_change_rewrites_one_fragment(void **state)
{
    (void)state;
    char *names = NULL;
    manifest_entry_t *e = big_dir(BIG_N + 1, &names);
    cdc_params_t cp;
    cdc_params_default(&cp);
    obj_sink_t sink = obj_sink_store(store);
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE], h3[OBJECT_HASH_SIZE];
    manifest_store_stats_t ms;

    /* Without the middle entry, then with it, then identical again */
    manifest_entry_t mid = e[BIG_N / 2];
    e[BIG_N / 2] = e[BIG_N];
    assert_int_equal(manifest_store_tree(e, BIG_N, &cp, &sink, h1, &ms), OK);
    uint64_t frags = ms.fragments;

    e[BIG_N] = mid;
    assert_int_equal(manifest_store_tree(e, BIG_N + 1, &cp, &sink, h2, &ms), OK);
    printf("one entry added to %u: %llu of %llu objects new\n", BIG_N,
           (unsigned long long)ms.objects_new, (unsigned long long)ms.objects);
    assert_memory_not_equal(h1, h2, OBJECT_HASH_SIZE);
    assert_true(ms.objects_new <= 4);
    assert_true(frags > 4);

    assert_int_equal(manifest_store_tree(e, BIG_N + 1, &cp, &sink, h3, &ms), OK);
    assert_memory_equal(h2, h3, OBJECT_HASH_SIZE);
    assert_int_equal(ms.objects_new, 0);

    free(e);
    free(names);
}

static void test_small_directory_is_one_tree(void **state)
{
    (void)state;
    manifest_entry_t e[3] = { mk("c", NODE_TYPE_REG, 1), mk("a", NODE_TYPE_REG, 2),
                              mk("b", NODE_TYPE_DIR, 0) };
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, e, 3, h), OK);
    uint8_t *buf = NULL;
    size_t len = 0;
    assert_int_equal(manifest_serialise(e, 3, &buf, &len), OK);
    void *got = NULL;
    size_t got_len = 0;
    uint8_t type = 0;
    assert_int_equal(object_load(store, h, &got, &got_len, &type), OK);
    assert_int_equal(type, OBJECT_TYPE_TREE);
    assert_int_equal(got_len, len);
    assert_memory_equal(got, buf, len);
    free(got);
    free(buf);
}

/* Fragments out of order, or a list of files where a directory is
 * expected, are refused: either would hand a reader a directory that is
 * not what was written. */
static void test_fragment_list_validated(void **state)
{
    (void)state;
    manifest_entry_t a[1] = { mk("aaa", NODE_TYPE_REG, 1) };
    manifest_entry_t b[1] = { mk("bbb", NODE_TYPE_REG, 2) };
    uint8_t *ba = NULL, *bb = NULL;
    size_t la = 0, lb = 0;
    assert_int_equal(manifest_serialise(a, 1, &ba, &la), OK);
    assert_int_equal(manifest_serialise(b, 1, &bb, &lb), OK);
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_TREE, ba, la, ha), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_TREE, bb, lb, hb), OK);

    chunklist_entry_t ent[2];
    memcpy(ent[0].hash, hb, OBJECT_HASH_SIZE);
    ent[0].offset = 0; ent[0].length = lb;
    memcpy(ent[1].hash, ha, OBJECT_HASH_SIZE);
    ent[1].offset = lb; ent[1].length = la;
    chunklist_hdr_t hdr = { .level = 0, .leaf_type = OBJECT_TYPE_TREE,
                            .count = 2, .total_len = la + lb };
    uint8_t lbuf[CHUNKLIST_HDR_BYTES + 2 * CHUNKLIST_ENTRY_BYTES];
    size_t n = chunklist_serialise(&hdr, ent, lbuf);
    uint8_t root[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, lbuf, n, root), OK);
    manifest_entry_t *out = NULL;
    uint32_t cnt = 0;
    assert_int_equal(manifest_read_tree(store, root, &out, &cnt), ERR_CORRUPT);

    /* In order it reads */
    memcpy(ent[0].hash, ha, OBJECT_HASH_SIZE);
    ent[0].length = la;
    memcpy(ent[1].hash, hb, OBJECT_HASH_SIZE);
    ent[1].offset = la; ent[1].length = lb;
    n = chunklist_serialise(&hdr, ent, lbuf);
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, lbuf, n, root), OK);
    assert_int_equal(manifest_read_tree(store, root, &out, &cnt), OK);
    assert_int_equal(cnt, 2);
    manifest_free_entries(out, cnt);

    /* A gap between fragments */
    ent[1].offset = la + 1;
    hdr.total_len = la + lb + 1;
    n = chunklist_serialise(&hdr, ent, lbuf);
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, lbuf, n, root), OK);
    assert_int_equal(manifest_read_tree(store, root, &out, &cnt), ERR_CORRUPT);

    /* A file list is not a directory */
    hdr.leaf_type = OBJECT_TYPE_FILE;
    hdr.total_len = la + lb;
    ent[1].offset = la;
    n = chunklist_serialise(&hdr, ent, lbuf);
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, lbuf, n, root), OK);
    assert_int_equal(manifest_read_tree(store, root, &out, &cnt), ERR_CORRUPT);
    free(ba);
    free(bb);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_serialisation_is_stable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_entry_without_content_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unsettled_flag_round_trips_and_is_canonical, setup, teardown),
        cmocka_unit_test_setup_teardown(test_mtime_is_signed_without_changing_the_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_input_order_does_not_matter, setup, teardown),
        cmocka_unit_test_setup_teardown(test_duplicate_names_still_order_deterministically, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sort_is_byte_order, setup, teardown),
        cmocka_unit_test_setup_teardown(test_identical_tree_stores_nothing_new, setup, teardown),
        cmocka_unit_test_setup_teardown(test_every_field_affects_the_hash, setup, teardown),
        cmocka_unit_test_setup_teardown(test_subtree_change_propagates, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_tree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlink_target_survives, setup, teardown),
        cmocka_unit_test_setup_teardown(test_device_numbers_affect_the_hash, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rejects_non_tree_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_directory_fragments, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_directory_change_rewrites_one_fragment, setup, teardown),
        cmocka_unit_test_setup_teardown(test_small_directory_is_one_tree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fragment_list_validated, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
