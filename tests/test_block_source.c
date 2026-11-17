#define _POSIX_C_SOURCE 200809L

/*
 * The zero-run rule for disks (vm-backup.md 5.2, src/units/block_source.c).
 *
 * The property everything later rests on: the same guest content gives the
 * same chunk list however it was read - through the extent map with ZERO
 * extents skipped, through it with them read, or in full - and whatever the
 * map says about the same bytes. And the list is the right one: each case is
 * also chunked from a plain in-memory copy of the disk with spans found by
 * brute force (maximal zero runs of 64 KiB or more are holes), and the
 * content hashes must agree.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "block_source.h"
#include "cdc.h"
#include "fake_provider.h"
#include "object.h"

/* ---- a sink that names objects and keeps nothing ---- */

static status_t name_put(void *ctx, uint8_t type, const void *data, size_t len,
                         obj_put_t *out)
{
    (void)ctx;
    object_name(type, data, len, out->hash);
    out->is_new = 0;
    out->phys_bytes = 0;
    return OK;
}

static const obj_sink_t g_sink = { name_put, NULL };

/* ---- the reference: brute-force spans over a buffer ---- */

typedef struct {
    const uint8_t *buf;
    uint64_t       size;
    uint64_t       pos;
} ref_t;

static status_t ref_next(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    ref_t *r = ctx;
    *got = 0;
    uint64_t p = r->pos;
    while (p < r->size) {
        /* Skip a hole: a zero run of CDC_HOLE_MIN or more. */
        uint64_t z = p;
        while (z < r->size && r->buf[z] == 0) z++;
        if (z - p >= CDC_HOLE_MIN) { p = z; continue; }
        /* A span: up to the next zero run that is long enough. */
        uint64_t q = p;
        for (;;) {
            while (q < r->size && r->buf[q] != 0) q++;
            if (q >= r->size) break;
            uint64_t zz = q;
            while (zz < r->size && r->buf[zz] == 0) zz++;
            if (zz - q >= CDC_HOLE_MIN) break;
            q = zz;
            if (q >= r->size) break;
        }
        *s = p;
        *e = q;
        *got = 1;
        r->pos = q;
        return OK;
    }
    r->pos = p;
    return OK;
}

static status_t ref_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    ref_t *r = ctx;
    memcpy(buf, r->buf + off, n);
    return OK;
}

/* ---- the fake disk behind a block stream ---- */

static sp_ctx_t          *g_fake;
static sp_session_t      *g_sess;
static const sp_unit_desc_t *g_desc;

static int setup(void **state)
{
    (void)state;
    sp_config_t cfg = { NULL, "0badc0de" };
    return fake_provider_ops.init(&cfg, &g_fake) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    fake_provider_ops.fini(g_fake);
    return 0;
}

/* Begin a session on unit "u" with one disk "d"; stopped, so nothing held. */
static sp_unit_desc_t *open_disk(uint64_t size, const fake_run_t *runs, size_t n,
                                 fake_alloc_t alloc, sp_block_stream_t *bs)
{
    assert_int_equal(fake_unit_add(g_fake, "u", "u", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_disk_add(g_fake, "u", "d", size, runs, n, alloc), OK);
    sp_unit_desc_t *d = NULL;
    assert_int_equal(fake_provider_ops.describe(g_fake, "u", &d), OK);
    const sp_session_info_t *info = NULL;
    sp_begin_req_t req;
    memset(&req, 0, sizeof(req));
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &req, &g_sess, &info), OK);
    assert_int_equal(fake_provider_ops.block_open(g_sess, &d->items[0], bs), OK);
    g_desc = d;
    return d;
}

static void close_disk(sp_unit_desc_t *d, sp_block_stream_t *bs)
{
    bs->close(bs->ctx);
    assert_int_equal(fake_provider_ops.end(g_sess, SP_END_ABORTED, 0), OK);
    fake_provider_ops.desc_free(d);
    g_sess = NULL;
}

static status_t chunk_block(const cdc_params_t *p, const sp_block_stream_t *bs,
                            uint64_t size, int use_extents, int trust_zero,
                            size_t window, uint8_t out[OBJECT_HASH_SIZE],
                            block_source_stats_t *stats)
{
    block_source_opts_t o = { use_extents, trust_zero, window, NULL, NULL };
    block_source_t *b = NULL;
    status_t st = block_source_open(bs, size, &o, &b);
    if (st != OK) return st;
    cdc_source_t src = block_source_iface(b);
    cdc_result_t cr;
    st = cdc_store_source(p, &src, size, &g_sink, NULL, &cr);
    if (st == OK) memcpy(out, cr.hash, OBJECT_HASH_SIZE);
    if (stats) block_source_stats(b, stats);
    block_source_close(b);
    return st;
}

static void ref_hash(const cdc_params_t *p, const uint8_t *buf, uint64_t size,
                     uint8_t out[OBJECT_HASH_SIZE])
{
    ref_t r = { buf, size, 0 };
    cdc_source_t src = { ref_next, ref_read, &r, NULL };
    cdc_result_t cr;
    assert_int_equal(cdc_store_source(p, &src, size, &g_sink, NULL, &cr), OK);
    memcpy(out, cr.hash, OBJECT_HASH_SIZE);
}

/* Every way of reading one layout agrees, and with the reference. */
static void check_layout(const cdc_params_t *p, uint64_t size,
                         const fake_run_t *runs, size_t n)
{
    static const fake_alloc_t allocs[] = {
        FAKE_ALLOC_EXACT, FAKE_ALLOC_ALL_DATA, FAKE_ALLOC_ZERO_AS_ZERO,
    };
    static const size_t windows[] = { 0, 65536, 100003 };

    uint8_t want[OBJECT_HASH_SIZE];
    uint8_t *buf = malloc(size ? size : 1);
    assert_non_null(buf);
    {
        sp_block_stream_t bs;
        sp_unit_desc_t *d = open_disk(size, runs, n, FAKE_ALLOC_EXACT, &bs);
        if (size) assert_int_equal(bs.read(bs.ctx, buf, (size_t)size, 0), OK);
        close_disk(d, &bs);
    }
    ref_hash(p, buf, size, want);
    free(buf);

    for (size_t a = 0; a < sizeof(allocs) / sizeof(allocs[0]); a++) {
        for (size_t w = 0; w < sizeof(windows) / sizeof(windows[0]); w++) {
            for (int mode = 0; mode < 3; mode++) {
                sp_block_stream_t bs;
                sp_unit_desc_t *d = open_disk(size, runs, n, allocs[a], &bs);
                uint8_t got[OBJECT_HASH_SIZE];
                status_t st = chunk_block(p, &bs, size, mode != 2, mode == 0,
                                          windows[w], got, NULL);
                if (st != OK) fail_msg("alloc %zu window %zu mode %d: %s", a, w, mode, err_msg());
                if (memcmp(got, want, OBJECT_HASH_SIZE) != 0)
                    fail_msg("alloc %zu window %zu mode %d: list differs from the reference",
                             a, w, mode);
                close_disk(d, &bs);
            }
        }
    }
}

static void small_params(cdc_params_t *p)
{
    assert_int_equal(cdc_params_init(p, 4096, 16384, 65536, NULL), OK);
}

#define K ((uint64_t)1024)
#define M ((uint64_t)1024 * 1024)

static void test_edges(void **state)
{
    (void)state;
    cdc_params_t ps, pd;
    small_params(&ps);
    cdc_params_default(&pd);
    const cdc_params_t *params[] = { &ps, &pd };

    /* Hand-picked layouts, each about one edge of the rule. */
    const fake_run_t l1[] = {   /* a zero run one byte short of a hole */
        { 0, 300 * K, FAKE_RUN_DATA, 1 },
        { 300 * K, CDC_HOLE_MIN - 1, FAKE_RUN_ZERO_DATA, 0 },
        { 300 * K + CDC_HOLE_MIN - 1, 300 * K, FAKE_RUN_DATA, 2 },
    };
    const fake_run_t l2[] = {   /* exactly a hole, reported as data */
        { 0, 300 * K, FAKE_RUN_DATA, 3 },
        { 300 * K, CDC_HOLE_MIN, FAKE_RUN_ZERO_DATA, 0 },
        { 300 * K + CDC_HOLE_MIN, 300 * K, FAKE_RUN_DATA, 4 },
    };
    const fake_run_t l3[] = {   /* a 4 KiB ZERO extent inside data: data */
        { 0, 1 * M, FAKE_RUN_DATA, 5 },
        { 1 * M + 4 * K, 1 * M, FAKE_RUN_DATA, 6 },
    };
    const fake_run_t l4[] = {   /* zeroes as data + a ZERO extent = one hole */
        { 0, 1 * M, FAKE_RUN_DATA, 7 },
        { 1 * M, 40 * K, FAKE_RUN_ZERO_DATA, 0 },
        { 1 * M + 64 * K, 1 * M, FAKE_RUN_DATA, 8 },
    };
    const fake_run_t l5[] = {   /* short zeroes at both ends */
        { 0, 10 * K, FAKE_RUN_ZERO_DATA, 0 },
        { 10 * K, 2 * M, FAKE_RUN_DATA, 9 },
    };
    const fake_run_t l6[] = {   /* constant bytes, and a run across windows */
        { 0, 3 * M, FAKE_RUN_FILL, 0x41 },
        { 3 * M, 2 * M, FAKE_RUN_ZERO_DATA, 0 },
        { 5 * M, 70 * K, FAKE_RUN_FILL, 0x01 },
    };
    const fake_run_t l7[] = {   /* a hole at the start and data in the last byte */
        { 6 * M - 1, 1, FAKE_RUN_FILL, 0xff },
    };
    struct { const fake_run_t *r; size_t n; uint64_t size; } cases[] = {
        { l1, 3, 300 * K * 2 + CDC_HOLE_MIN - 1 },
        { l2, 3, 300 * K * 2 + CDC_HOLE_MIN },
        { l3, 2, 2 * M + 4 * K },
        { l4, 3, 2 * M + 64 * K },
        { l5, 2, 2 * M + 10 * K + 30 * K },
        { l6, 3, 5 * M + 70 * K + 1 },
        { l7, 1, 6 * M },
        { NULL, 0, 10 * M },        /* nothing but zeroes: an empty list */
        { NULL, 0, 1000 },          /* short and zero: one span of data */
        { NULL, 0, 0 },
    };
    for (size_t pi = 0; pi < 2; pi++)
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++)
            check_layout(params[pi], cases[i].size, cases[i].r, cases[i].n);
}

static uint64_t rnd(uint64_t *s)
{
    *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17;
    return *s;
}

/* Seeded random layouts, weighted towards lengths near the hole bound. */
static void test_random(void **state)
{
    (void)state;
    cdc_params_t ps;
    small_params(&ps);
    uint64_t seed = 0x9e3779b97f4a7c15ULL;
    static const uint64_t lens[] = {
        1, 7, 4095, 4096, 65535, 65536, 65537, 131071, 200000, 1048576, 3000000,
    };
    for (int c = 0; c < 30; c++) {
        fake_run_t runs[24];
        size_t n = 0;
        uint64_t off = 0;
        while (n < 24) {
            uint64_t gap = (rnd(&seed) % 3 == 0) ? lens[rnd(&seed) % 11] : 0;
            uint64_t len = lens[rnd(&seed) % 11];
            if (off + gap + len > 8 * M) break;
            off += gap;
            fake_run_kind_t k = (fake_run_kind_t)(1 + rnd(&seed) % 4);
            runs[n].off = off;
            runs[n].len = len;
            runs[n].kind = k;
            runs[n].seed = k == FAKE_RUN_FILL ? (rnd(&seed) % 3) : rnd(&seed);
            n++;
            off += len;
        }
        uint64_t size = off + ((rnd(&seed) & 1) ? lens[rnd(&seed) % 11] : 0);
        check_layout(&ps, size, runs, n);
    }
}

typedef struct {
    const sp_block_stream_t *bs;
    const fake_run_t        *runs;
    size_t                   n, i;
} tb_ref_t;

static status_t tb_next(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    tb_ref_t *r = ctx;
    *got = r->i < r->n;
    if (*got) { *s = r->runs[r->i].off; *e = *s + r->runs[r->i].len; r->i++; }
    return OK;
}

static status_t tb_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    tb_ref_t *r = ctx;
    return r->bs->read(r->bs->ctx, buf, n, off);
}

/* A terabyte disk with a few megabytes on it costs reading those, through
 * the map, and nothing for the rest. */
static void test_sparse_terabyte(void **state)
{
    (void)state;
    cdc_params_t pd;
    cdc_params_default(&pd);
    const uint64_t T = (uint64_t)1 << 40;
    const fake_run_t runs[] = {
        { 0, 2 * M, FAKE_RUN_DATA, 11 },
        { 512 * (uint64_t)1 << 30, 3 * M, FAKE_RUN_DATA, 12 },
        { T - 1 * M, 1 * M, FAKE_RUN_DATA, 13 },
    };
    sp_block_stream_t bs;
    sp_unit_desc_t *d = open_disk(T, runs, 3, FAKE_ALLOC_EXACT, &bs);
    uint8_t got[OBJECT_HASH_SIZE];
    block_source_stats_t st;
    assert_int_equal(chunk_block(&pd, &bs, T, 1, 1, 0, got, &st), OK);
    assert_true(st.bytes_read <= 6 * M);
    assert_true(st.zero_skipped >= T - 6 * M);

    /* The reference: the same three runs as spans, read from the fake.
     * Generated data has no zero run anywhere near 64 KiB long. */
    tb_ref_t r = { &bs, runs, 3, 0 };
    cdc_source_t src = { tb_next, tb_read, &r, NULL };
    cdc_result_t cr;
    assert_int_equal(cdc_store_source(&pd, &src, T, &g_sink, NULL, &cr), OK);
    assert_memory_equal(cr.hash, got, OBJECT_HASH_SIZE);
    close_disk(d, &bs);
}

/* Failures underneath come out as the chunker's error, not as a list. */
static void test_failures(void **state)
{
    (void)state;
    cdc_params_t ps;
    small_params(&ps);
    const fake_run_t runs[] = { { 0, 4 * M, FAKE_RUN_DATA, 1 } };
    uint8_t got[OBJECT_HASH_SIZE];

    fake_fail_set(g_fake, "u", FAKE_FAIL_READ, 3 * M + 5);
    sp_block_stream_t bs;
    sp_unit_desc_t *d = open_disk(4 * M, runs, 1, FAKE_ALLOC_EXACT, &bs);
    assert_int_equal(chunk_block(&ps, &bs, 4 * M, 1, 1, 0, got, NULL), ERR_IO);
    assert_non_null(strstr(err_msg(), "byte"));
    close_disk(d, &bs);

    fake_fail_set(g_fake, "u", FAKE_FAIL_EXTENT, 0);
    d = open_disk(4 * M, runs, 1, FAKE_ALLOC_EXACT, &bs);
    assert_int_equal(chunk_block(&ps, &bs, 4 * M, 1, 1, 0, got, NULL), ERR_IO);
    /* Read in full never asks the map. */
    assert_int_equal(chunk_block(&ps, &bs, 4 * M, 0, 0, 0, got, NULL), OK);
    close_disk(d, &bs);
    fake_fail_clear(g_fake);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_edges),
        cmocka_unit_test(test_random),
        cmocka_unit_test(test_sparse_terabyte),
        cmocka_unit_test(test_failures),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
