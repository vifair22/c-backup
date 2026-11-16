#define _POSIX_C_SOURCE 200809L

/*
 * Parity overhead against data, by the shape of what is stored (OWN-44).
 *
 * Reed-Solomon RS(255,239) interleaved 64 deep costs 1,024 parity bytes per
 * 15,296 data bytes: 6.7%. Paid per entry, with every entry rounding up to a
 * whole group and carrying a 260-byte header record of its own, a pack of
 * small objects paid more than its data in parity: 17 times the rate. The pack stripes its
 * parity over the whole body instead, so the rate holds whatever the entry
 * sizes; these cases pin that with the three shapes that matter: many small
 * files, a mixed tree, and content-defined chunks.
 *
 * The figures are what the store reports writing (STORE_WR_PACK and
 * STORE_WR_PACK_PARITY), so they include every fixed trailer byte.
 * Content is random, so compression does not move the sizes.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "object.h"
#include "pack.h"
#include "store.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_pack_parity_ratio"

/* The RS rate, and how close to it each shape must land. The per-entry CRC
 * table (8 bytes an entry) and its own parity are what separates a pack
 * from the bare rate; for 12,000 small entries that is about 0.8%. */
#define RS_RATE ((double)PARITY_GROUP_PAR / (double)PARITY_GROUP_DATA)

static uint64_t rng = 0x9E3779B97F4A7C15ull;
static uint64_t next_rand(void)
{
    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
    return rng;
}

static void fill(uint8_t *b, size_t n)
{
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = next_rand();
        size_t k = n - i < 8 ? n - i : 8;
        memcpy(b + i, &v, k);
    }
}

typedef struct { const char *name; double ratio; uint64_t data, parity; size_t n; } shape_t;

/* Store n objects whose sizes come from size_of(i), flush, and report the
 * parity bytes written against the pack body. */
static shape_t run(const char *name, size_t n, size_t (*size_of)(size_t))
{
    int rc = system("rm -rf " TEST_REPO); (void)rc;
    assert_int_equal(store_init_dirs(TEST_REPO), OK);
    store_t *st = NULL;
    assert_int_equal(store_open(TEST_REPO, NULL, &st), OK);
    uint8_t *buf = malloc(8u * 1024 * 1024);
    assert_non_null(buf);
    store_write_stats_t before, after;
    store_write_stats(st, &before);
    for (size_t i = 0; i < n; i++) {
        size_t len = size_of(i);
        fill(buf, len);
        uint8_t h[OBJECT_HASH_SIZE];
        assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, len, h), OK);
    }
    assert_int_equal(object_store_flush(st), OK);
    store_write_stats(st, &after);
    store_write_stats_t d;
    store_write_stats_diff(&after, &before, &d);
    shape_t s = { name, (double)d.bytes[STORE_WR_PACK_PARITY] / (double)d.bytes[STORE_WR_PACK],
                  d.bytes[STORE_WR_PACK], d.bytes[STORE_WR_PACK_PARITY], n };
    print_message("parity: %-14s %6zu objects  data %11llu  parity %10llu  ratio %.4f"
                  "  (%.2fx the RS rate)\n", name, n,
                  (unsigned long long)s.data, (unsigned long long)s.parity, s.ratio,
                  s.ratio / RS_RATE);
    free(buf);
    store_close(st);
    rc = system("rm -rf " TEST_REPO); (void)rc;
    return s;
}

/* Small files: 100 B to 2 KiB, mean about 1 KiB. */
static size_t small_size(size_t i) { (void)i; return 100 + (size_t)(next_rand() % 1949); }

/* A mixed tree: sizes log-uniform from 64 B to 1 MiB, the long tail most
 * real trees have, plus a few chunk-sized objects. */
static size_t mixed_size(size_t i)
{
    if (i % 100 == 99) return (size_t)4 * 1024 * 1024 + (size_t)(next_rand() % 4096);
    double e = 6.0 + (double)(next_rand() % 1000) / 1000.0 * 14.0;   /* 2^6 .. 2^20 */
    size_t s = (size_t)1 << (size_t)e;
    return s + (size_t)(next_rand() % s);
}

/* Content-defined chunks: 512 KiB to 8 MiB, mean near the 1 MiB average. */
static size_t chunk_size(size_t i)
{
    (void)i;
    size_t s = (size_t)512 * 1024 + (size_t)(next_rand() % (1536u * 1024));
    if (next_rand() % 16 == 0) s = (size_t)6 * 1024 * 1024 + (size_t)(next_rand() % (2u * 1024 * 1024));
    return s;
}

static void test_parity_stays_near_the_rs_rate(void **state)
{
    (void)state;
    shape_t a = run("small files", 12000, small_size);
    shape_t b = run("mixed tree", 1000, mixed_size);
    shape_t c = run("chunks", 64, chunk_size);
#ifndef PARITY_RATIO_MEASURE_ONLY
    /* Close to the rate whatever the shape: at most the rate plus 1% of
     * the data for 12,000 small entries, and within half a percent for
     * chunks. */
    assert_true(a.ratio < RS_RATE + 0.01);
    assert_true(b.ratio < RS_RATE + 0.01);
    assert_true(c.ratio < RS_RATE + 0.005);
    assert_true(a.ratio > RS_RATE * 0.95);
#else
    (void)a; (void)b; (void)c;
#endif
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_parity_stays_near_the_rs_rate),
    };
    return cmocka_run_group_tests_name("pack_parity_ratio", tests, NULL, NULL);
}
