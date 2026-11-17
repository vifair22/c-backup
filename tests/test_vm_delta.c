#define _POSIX_C_SOURCE 200809L

/*
 * Incremental stores (docs/vm-backup.md 5.3, 5.4; src/store/cdc.c).
 *
 * The invariant everything rests on: for any disk and any set of in-place
 * changes, cdc_store_delta over the new content, given the base's list and
 * a changed set covering every change, names exactly what a full
 * cdc_store_source of the new content names. Checked over randomised disks
 * and randomised edits (random writes, zero fills that make holes, hole
 * fills, edits within CDC_HOLE_MIN of a hole on either side, edits merging
 * and splitting spans, edits in the first and last chunk and on exact
 * chunk boundaries), with the allocation map reported differently between
 * the base's read and the new one. Seeded and reproducible: a failure
 * prints the seed.
 *
 * Also: an over-reported changed set costs reads, never correctness; an
 * under-reported one gives a different name, and the audit finds it at the
 * right place; the read estimate is what the delta reads, give or take a
 * divergent chunk; isolated small writes resync within about a chunk.
 *
 * Disks are run maps over the fake provider's offset-addressed generator,
 * so edits split runs and a disk costs no memory; the CDC parameters are
 * scaled down so each case has hundreds of chunks over a few MiB.
 */

#include <pthread.h>
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

#define K ((uint64_t)1024)
#define M (K * K)

/* ---- an in-memory object store: sink and loader ---- */

typedef struct obj {
    struct obj *next;
    uint8_t     hash[OBJECT_HASH_SIZE];
    uint8_t     type;
    size_t      len;
    uint8_t    *data;    /* NULL for FILE objects other than roots: never loaded */
} obj_t;

#define BUCKETS 4096
static obj_t *g_tab[BUCKETS];
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_puts_new;

static unsigned bucket(const uint8_t *h) { return ((unsigned)h[0] << 8 | h[1]) % BUCKETS; }

static status_t mem_put(void *ctx, uint8_t type, const void *data, size_t len, obj_put_t *out)
{
    (void)ctx;
    object_name(type, data, len, out->hash);
    out->phys_bytes = 0;
    pthread_mutex_lock(&g_mu);
    unsigned b = bucket(out->hash);
    obj_t *o = g_tab[b];
    while (o && memcmp(o->hash, out->hash, OBJECT_HASH_SIZE) != 0) o = o->next;
    out->is_new = o == NULL;
    if (!o) {
        o = calloc(1, sizeof(*o));
        assert_non_null(o);
        memcpy(o->hash, out->hash, OBJECT_HASH_SIZE);
        o->type = type;
        o->len = len;
        /* Lists and small FILE objects are kept (a one-chunk base is a FILE
         * root); big chunks are only named. */
        if (type != OBJECT_TYPE_FILE || len <= 256 * K) {
            o->data = malloc(len ? len : 1);
            assert_non_null(o->data);
            if (len) memcpy(o->data, data, len);
        }
        o->next = g_tab[b];
        g_tab[b] = o;
        g_puts_new++;
    }
    pthread_mutex_unlock(&g_mu);
    return OK;
}

static status_t mem_load(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE], void **data,
                         size_t *len, uint8_t *type)
{
    (void)ctx;
    pthread_mutex_lock(&g_mu);
    obj_t *o = g_tab[bucket(hash)];
    while (o && memcmp(o->hash, hash, OBJECT_HASH_SIZE) != 0) o = o->next;
    status_t st = OK;
    if (!o || !o->data) st = set_error(ERR_NOT_FOUND, "test store: no such object");
    else {
        *data = malloc(o->len ? o->len : 1);
        assert_non_null(*data);
        memcpy(*data, o->data, o->len);
        *len = o->len;
        *type = o->type;
    }
    pthread_mutex_unlock(&g_mu);
    return st;
}

static const obj_sink_t g_sink = { mem_put, NULL };
static const obj_loader_t g_ld = { mem_load, NULL };

static void store_reset(void)
{
    for (unsigned i = 0; i < BUCKETS; i++) {
        obj_t *o = g_tab[i];
        while (o) { obj_t *n = o->next; free(o->data); free(o); o = n; }
        g_tab[i] = NULL;
    }
}

/* ---- a deterministic PRNG ---- */

static uint64_t g_rng;
static uint64_t rnd(void)
{
    g_rng ^= g_rng << 13; g_rng ^= g_rng >> 7; g_rng ^= g_rng << 17;
    return g_rng;
}
static uint64_t rnd_below(uint64_t n) { return n ? rnd() % n : 0; }

/* ---- disks as run maps ---- */

#define RUNS_MAX 4096

typedef struct {
    uint64_t   size;
    fake_run_t r[RUNS_MAX];
    size_t     n;
} disk_t;

/* Overwrite [off, off + len) with one run of kind/seed, splitting what it
 * overlaps. Bytes outside every run read as zero (reported ZERO). */
static void disk_put(disk_t *d, uint64_t off, uint64_t len, fake_run_kind_t kind, uint64_t seed)
{
    if (off >= d->size) return;
    if (len > d->size - off) len = d->size - off;
    if (!len) return;
    uint64_t end = off + len;
    fake_run_t out[RUNS_MAX];
    size_t n = 0;
    int placed = 0;
    for (size_t i = 0; i < d->n; i++) {
        fake_run_t x = d->r[i];
        uint64_t xe = x.off + x.len;
        if (xe <= off || x.off >= end) {
            if (!placed && x.off >= end) { out[n++] = (fake_run_t){ off, len, kind, seed }; placed = 1; }
            out[n++] = x;
            continue;
        }
        if (x.off < off) out[n++] = (fake_run_t){ x.off, off - x.off, x.kind, x.seed };
        if (!placed) { out[n++] = (fake_run_t){ off, len, kind, seed }; placed = 1; }
        if (xe > end) out[n++] = (fake_run_t){ end, xe - end, x.kind, x.seed };
        assert_true(n < RUNS_MAX - 4);
    }
    if (!placed) out[n++] = (fake_run_t){ off, len, kind, seed };
    memcpy(d->r, out, n * sizeof(out[0]));
    d->n = n;
}

/* Bytes made zero and reported ZERO: a hole as a sparse image has it. */
static void disk_punch(disk_t *d, uint64_t off, uint64_t len)
{
    disk_put(d, off, len, FAKE_RUN_ZERO, 0);
}

/* ---- the changed set ---- */

#define RANGES_MAX 4096

typedef struct {
    uint64_t s[RANGES_MAX], e[RANGES_MAX];
    size_t   n;
} ranges_t;

static void ranges_add(ranges_t *g, uint64_t s, uint64_t e, uint64_t gran, uint64_t size)
{
    if (gran) { s = s / gran * gran; e = (e + gran - 1) / gran * gran; }
    if (e > size) e = size;
    if (s >= e) return;
    assert_true(g->n < RANGES_MAX);
    g->s[g->n] = s;
    g->e[g->n] = e;
    g->n++;
}

static int cmp_u64pair(const void *a, const void *b)
{
    const uint64_t *x = a, *y = b;
    return x[0] < y[0] ? -1 : x[0] > y[0];
}

/* Sorted; left unmerged on purpose, so the engine's merging is exercised. */
static void ranges_sort(ranges_t *g)
{
    uint64_t (*p)[2] = malloc(g->n * sizeof(*p) + 1);
    assert_non_null(p);
    for (size_t i = 0; i < g->n; i++) { p[i][0] = g->s[i]; p[i][1] = g->e[i]; }
    qsort(p, g->n, sizeof(*p), cmp_u64pair);
    for (size_t i = 0; i < g->n; i++) { g->s[i] = p[i][0]; g->e[i] = p[i][1]; }
    free(p);
}

typedef struct { const ranges_t *g; } chg_ctx_t;

static status_t changed_fn(void *ctx, uint64_t from, uint64_t *s, uint64_t *e, int *done)
{
    const ranges_t *g = ((chg_ctx_t *)ctx)->g;
    /* The first range ending after from: as a bitmap query would answer. */
    for (size_t i = 0; i < g->n; i++) {
        if (g->e[i] <= from) continue;
        *s = g->s[i] < from ? from : g->s[i];
        *e = g->e[i];
        *done = 0;
        return OK;
    }
    *done = 1;
    return OK;
}

/* ---- full and incremental stores of a disk ---- */

static cdc_params_t g_p;

static void open_src(const disk_t *d, fake_alloc_t alloc, int trust_zero, sp_block_stream_t *bs,
                     block_source_t **b, cdc_source_t *src)
{
    assert_int_equal(fake_stream_from_runs(d->size, d->r, d->n, alloc, bs), OK);
    block_source_opts_t o = { 1, trust_zero, 64 * K, NULL, NULL };
    assert_int_equal(block_source_open(bs, d->size, &o, b), OK);
    *src = block_source_iface(*b);
}

static void close_src(sp_block_stream_t *bs, block_source_t *b)
{
    block_source_close(b);
    bs->close(bs->ctx);
}

static void full_hash(const disk_t *d, fake_alloc_t alloc, uint8_t out[OBJECT_HASH_SIZE])
{
    sp_block_stream_t bs;
    block_source_t *b = NULL;
    cdc_source_t src;
    open_src(d, alloc, 1, &bs, &b, &src);
    cdc_result_t r;
    status_t st = cdc_store_source(&g_p, &src, d->size, &g_sink, NULL, &r);
    if (st != OK) fail_msg("full read: %s", err_msg());
    memcpy(out, r.hash, OBJECT_HASH_SIZE);
    close_src(&bs, b);
}

static status_t delta_hash(const disk_t *d, fake_alloc_t alloc, const uint8_t base[OBJECT_HASH_SIZE],
                           const ranges_t *g, cdc_workers_t *w, cdc_delta_result_t *out)
{
    sp_block_stream_t bs;
    block_source_t *b = NULL;
    cdc_source_t src;
    open_src(d, alloc, (int)(rnd() & 1), &bs, &b, &src);
    chg_ctx_t cc = { g };
    cdc_opts_t o;
    memset(&o, 0, sizeof(o));
    o.workers = w;
    status_t st = cdc_store_delta(&g_p, &g_ld, base, &src, d->size, changed_fn, &cc, &g_sink, &o, out);
    close_src(&bs, b);
    return st;
}

/* The audit over a full read of d against base and g. */
typedef struct { cdc_audit_t *a; } tapctx_t;

static status_t tap(void *ctx, uint64_t off, const uint8_t *buf, size_t len)
{
    return cdc_audit_feed(((tapctx_t *)ctx)->a, off, buf, len);
}

static void audit_run(const disk_t *d, const uint8_t base[OBJECT_HASH_SIZE], const ranges_t *g,
                      cdc_audit_result_t *res)
{
    chg_ctx_t cc = { g };
    tapctx_t t = { NULL };
    assert_int_equal(cdc_audit_open(&g_ld, base, d->size, changed_fn, &cc, &t.a), OK);
    sp_block_stream_t bs;
    assert_int_equal(fake_stream_from_runs(d->size, d->r, d->n, FAKE_ALLOC_EXACT, &bs), OK);
    block_source_opts_t o = { 1, 1, 64 * K, tap, &t };
    block_source_t *b = NULL;
    assert_int_equal(block_source_open(&bs, d->size, &o, &b), OK);
    cdc_source_t src = block_source_iface(b);
    cdc_result_t r;
    if (cdc_store_source(&g_p, &src, d->size, &g_sink, NULL, &r) != OK) fail_msg("audit read: %s", err_msg());
    if (cdc_audit_finish(t.a, res) != OK) fail_msg("audit: %s", err_msg());
    cdc_audit_close(t.a);
    close_src(&bs, b);
}

/* Where two lists of one disk first differ, for a failure message. */
static void diff_lists(const uint8_t a[OBJECT_HASH_SIZE], const uint8_t b[OBJECT_HASH_SIZE], const ranges_t *g,
                       const disk_t *d)
{
    chunklist_iter_t *ia = NULL, *ib = NULL;
    if (chunklist_iter_open(&g_ld, a, OBJECT_TYPE_FILE, 0, NULL, &ia) != OK ||
        chunklist_iter_open(&g_ld, b, OBJECT_TYPE_FILE, 0, NULL, &ib) != OK) {
        print_message("cannot walk the lists: %s\n", err_msg());
        chunklist_iter_close(ia);
        return;
    }
    for (unsigned i = 0;; i++) {
        chunklist_item_t x, y;
        int dx = 0, dy = 0;
        if (chunklist_iter_next(ia, &x, &dx) != OK || chunklist_iter_next(ib, &y, &dy) != OK) break;
        if (dx || dy) { print_message("one list ends first at leaf %u (%d %d)\n", i, dx, dy); break; }
        if (x.offset != y.offset || x.length != y.length || memcmp(x.hash, y.hash, OBJECT_HASH_SIZE)) {
            print_message("leaf %u: full [%llu,+%llu) delta [%llu,+%llu)%s\n", i,
                          (unsigned long long)x.offset, (unsigned long long)x.length,
                          (unsigned long long)y.offset, (unsigned long long)y.length,
                          x.offset == y.offset && x.length == y.length ? " hash differs" : "");
            break;
        }
    }
    chunklist_iter_close(ia);
    chunklist_iter_close(ib);
    for (size_t i = 0; i < g->n; i++)
        print_message("range [%llu, %llu)\n", (unsigned long long)g->s[i], (unsigned long long)g->e[i]);
    for (size_t i = 0; i < d->n; i++)
        print_message("run [%llu, +%llu) kind %d\n", (unsigned long long)d->r[i].off,
                      (unsigned long long)d->r[i].len, (int)d->r[i].kind);
}

/* ---- random disks and edits ---- */

static void random_disk(disk_t *d, uint64_t size)
{
    memset(d, 0, sizeof(*d));
    d->size = size;
    /* Data runs with gaps, some gaps shorter than a hole, some preallocated
     * zeroes, and the odd fill byte. */
    uint64_t pos = rnd_below(3) == 0 ? 0 : rnd_below(256 * K);
    while (pos < size && d->n < 64) {
        uint64_t len = 1 + rnd_below(rnd_below(4) == 0 ? 2 * M : 300 * K);
        int k = (int)rnd_below(10);
        fake_run_kind_t kind = k < 7 ? FAKE_RUN_DATA : k < 9 ? FAKE_RUN_ZERO_DATA : FAKE_RUN_FILL;
        disk_put(d, pos, len, kind, rnd() | 1);
        uint64_t gap = rnd_below(3) == 0 ? rnd_below(CDC_HOLE_MIN + 16 * K) : rnd_below(1 * M);
        pos += len + gap;
    }
}

/* One edit, recorded in g (exactly, or rounded to gran). */
static void random_edit(disk_t *d, ranges_t *g, uint64_t gran, int data_only)
{
    uint64_t size = d->size;
    uint64_t off, len;
    int where = (int)rnd_below(8);
    if (where == 0) off = rnd_below(64 * K);                                   /* the first chunk */
    else if (where == 1) off = size > 64 * K ? size - 1 - rnd_below(64 * K) : 0; /* the last */
    else if (where == 2 && d->n) {
        /* Within CDC_HOLE_MIN of a run edge, either side. */
        const fake_run_t *x = &d->r[rnd_below(d->n)];
        uint64_t edge = rnd_below(2) ? x->off : x->off + x->len;
        uint64_t delta = rnd_below(CDC_HOLE_MIN + 8 * K);
        off = rnd_below(2) ? edge + delta : (edge > delta ? edge - delta : 0);
    } else off = rnd_below(size);
    int big = rnd_below(6) == 0;
    len = 1 + rnd_below(big ? 4 * M : (rnd_below(2) ? 64 * K : 512));
    if (off >= size) off = size - 1;
    if (len > size - off) len = size - off;
    int kind = data_only ? 0 : (int)rnd_below(6);
    if (kind <= 2) disk_put(d, off, len, FAKE_RUN_DATA, rnd() | 1);
    else if (kind == 3) disk_put(d, off, len, FAKE_RUN_ZERO_DATA, 0);      /* zeroes written */
    else if (kind == 4) disk_punch(d, off, len);                            /* discarded */
    else disk_put(d, off, len, FAKE_RUN_FILL, rnd() & 0xff);
    ranges_add(g, off, off + len, gran, size);
}

static const fake_alloc_t k_allocs[] = { FAKE_ALLOC_EXACT, FAKE_ALLOC_ALL_DATA, FAKE_ALLOC_ZERO_AS_ZERO };

/* ---- tests ---- */

static int setup(void **state)
{
    (void)state;
    /* min 8 KiB, avg 32 KiB, max 128 KiB: hundreds of chunks a case. */
    return cdc_params_init(&g_p, 8 * 1024, 32 * 1024, 128 * 1024, NULL) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    store_reset();
    return 0;
}

/* The central invariant, over many seeds. */
static void test_invariant(void **state)
{
    (void)state;
    disk_t *d = malloc(sizeof(*d));
    ranges_t *g = malloc(sizeof(*g));
    assert_non_null(d);
    assert_non_null(g);
    unsigned cases = 0;
    uint64_t sum_resync = 0;
    cdc_workers_t *w = NULL;
    assert_int_equal(cdc_workers_start(3, &w), OK);
    for (uint64_t seed = 1; seed <= 700; seed++) {
        g_rng = seed * 0x9e3779b97f4a7c15ull;
        uint64_t size = 64 * K + rnd_below(6 * M);
        random_disk(d, size);
        uint8_t base[OBJECT_HASH_SIZE];
        full_hash(d, k_allocs[rnd_below(3)], base);
        /* Several sessions in a row, each the base of the next. */
        for (int step = 0; step < 3; step++) {
            memset(g, 0, sizeof(*g));
            uint64_t gran = rnd_below(3) == 0 ? 0 : 64 * K;
            int edits = 1 + (int)rnd_below(rnd_below(3) == 0 ? 40 : 6);
            for (int i = 0; i < edits; i++) random_edit(d, g, gran, 0);
            ranges_sort(g);
            uint8_t want[OBJECT_HASH_SIZE];
            fake_alloc_t alloc = k_allocs[rnd_below(3)];
            full_hash(d, alloc, want);
            cdc_delta_result_t dr;
            status_t st = delta_hash(d, alloc, base, g, (seed & 3) == 0 ? w : NULL, &dr);
            if (st != OK) fail_msg("seed %llu step %d: delta: %s", (unsigned long long)seed, step, err_msg());
            if (memcmp(want, dr.r.hash, OBJECT_HASH_SIZE) != 0) {
                diff_lists(want, dr.r.hash, g, d);
                fail_msg("seed %llu step %d: the incremental's name differs from a full read's "
                         "(size %llu, %zu ranges)", (unsigned long long)seed, step,
                         (unsigned long long)size, g->n);
            }
            /* The audit of a full read with a true changed set is clean. */
            if ((seed % 5) == 0) {
                cdc_audit_result_t ar;
                audit_run(d, base, g, &ar);
                if (ar.bad) fail_msg("seed %llu step %d: audit flagged %llu bytes at %llu with a true changed set",
                                     (unsigned long long)seed, step, (unsigned long long)ar.bad_bytes,
                                     (unsigned long long)ar.first_bad);
            }
            sum_resync += dr.resync_bytes;
            memcpy(base, want, OBJECT_HASH_SIZE);
            cases++;
        }
        store_reset();
    }
    cdc_workers_stop(w);
    print_message("%u incremental sessions, each equal to a full read; mean resync %llu bytes\n",
                  cases, (unsigned long long)(sum_resync / (cases ? cases : 1)));
    free(d);
    free(g);
}

/* Over-reporting costs reads, never the name. */
static void test_over_reporting(void **state)
{
    (void)state;
    disk_t *d = malloc(sizeof(*d));
    ranges_t *g = malloc(sizeof(*g));
    assert_non_null(d);
    assert_non_null(g);
    for (uint64_t seed = 1; seed <= 150; seed++) {
        g_rng = seed * 0xd1b54a32d192ed03ull;
        random_disk(d, 1 * M + rnd_below(8 * M));
        uint8_t base[OBJECT_HASH_SIZE], want[OBJECT_HASH_SIZE];
        full_hash(d, FAKE_ALLOC_EXACT, base);
        memset(g, 0, sizeof(*g));
        for (int i = 0; i < 3; i++) random_edit(d, g, 64 * K, 0);
        /* Ranges where nothing changed. */
        for (int i = 0; i < 8; i++) {
            uint64_t s = rnd_below(d->size);
            ranges_add(g, s, s + 1 + rnd_below(256 * K), 64 * K, d->size);
        }
        if (seed % 10 == 0) { g->n = 0; ranges_add(g, 0, d->size, 0, d->size); }  /* everything */
        ranges_sort(g);
        full_hash(d, FAKE_ALLOC_EXACT, want);
        cdc_delta_result_t dr;
        assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
        if (memcmp(want, dr.r.hash, OBJECT_HASH_SIZE) != 0) fail_msg("seed %llu: over-reported set changed the name", (unsigned long long)seed);
        store_reset();
    }
    free(d);
    free(g);
}

/* Under-reporting gives another name, and the audit says where. */
static void test_under_reporting(void **state)
{
    (void)state;
    disk_t *d = malloc(sizeof(*d));
    ranges_t *g = malloc(sizeof(*g));
    assert_non_null(d);
    assert_non_null(g);
    unsigned found = 0, differs = 0;
    for (uint64_t seed = 1; seed <= 120; seed++) {
        g_rng = seed * 0x94d049bb133111ebull;
        random_disk(d, 2 * M + rnd_below(8 * M));
        uint8_t base[OBJECT_HASH_SIZE], want[OBJECT_HASH_SIZE];
        full_hash(d, FAKE_ALLOC_EXACT, base);
        memset(g, 0, sizeof(*g));
        for (int i = 0; i < 2; i++) random_edit(d, g, 64 * K, 0);
        /* The missed write: random data far from the reported ranges, so it
         * really changes bytes nobody reported. */
        uint64_t ms = 0;
        int ok = 0;
        for (int tries = 0; tries < 50 && !ok; tries++) {
            ms = rnd_below(d->size - 4 * K);
            ok = 1;
            for (size_t i = 0; i < g->n; i++)
                if (ms + 4 * K + 512 * K > g->s[i] && ms < g->e[i] + 512 * K) ok = 0;
        }
        if (!ok) continue;
        disk_put(d, ms, 4 * K, FAKE_RUN_DATA, rnd() | 1);
        ranges_sort(g);
        full_hash(d, FAKE_ALLOC_EXACT, want);
        cdc_delta_result_t dr;
        assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
        /* Usually another name; the same one only when a re-chunk that had
         * not resynced yet happened to run over the missed write. */
        if (memcmp(want, dr.r.hash, OBJECT_HASH_SIZE) != 0) differs++;
        cdc_audit_result_t ar;
        audit_run(d, base, g, &ar);
        if (!ar.bad) fail_msg("seed %llu: the audit did not see the missed write at %llu", (unsigned long long)seed, (unsigned long long)ms);
        /* The first bad offset is the missed write, or the start of the
         * base leaf it fell in (at most one chunk before it). */
        if (ar.first_bad > ms || ms - ar.first_bad > 128 * K)
            fail_msg("seed %llu: audit says %llu, the missed write was at %llu", (unsigned long long)seed,
                     (unsigned long long)ar.first_bad, (unsigned long long)ms);
        found++;
        store_reset();
    }
    print_message("%u missed writes: the audit saw every one, %u gave another name\n", found, differs);
    assert_true(found > 80);
    assert_true(differs * 4 >= found * 3);
    free(d);
    free(g);
}

/* The read estimate, against what the delta read, for data-only writes:
 * never more than the re-chunk read; and empty changes cost nothing. */
static void test_estimate(void **state)
{
    (void)state;
    disk_t *d = malloc(sizeof(*d));
    ranges_t *g = malloc(sizeof(*g));
    assert_non_null(d);
    assert_non_null(g);
    for (uint64_t seed = 1; seed <= 150; seed++) {
        g_rng = seed * 0xbf58476d1ce4e5b9ull;
        memset(d, 0, sizeof(*d));
        d->size = 4 * M + rnd_below(8 * M);
        disk_put(d, 0, d->size, FAKE_RUN_DATA, seed);           /* fully allocated */
        if (seed & 1) disk_punch(d, d->size / 3, 512 * K);      /* and one hole */
        uint8_t base[OBJECT_HASH_SIZE];
        full_hash(d, FAKE_ALLOC_EXACT, base);
        memset(g, 0, sizeof(*g));
        int clustered = seed % 3 == 0;
        int edits = 1 + (int)rnd_below(20);
        for (int i = 0; i < edits; i++) {
            if (clustered) {
                uint64_t s = d->size / 2 + rnd_below(256 * K);
                disk_put(d, s, 4 * K, FAKE_RUN_DATA, rnd() | 1);
                ranges_add(g, s, s + 4 * K, 64 * K, d->size);
            } else {
                random_edit(d, g, 64 * K, 1);
            }
        }
        ranges_sort(g);
        chg_ctx_t cc = { g };
        cdc_delta_estimate_t e;
        assert_int_equal(cdc_delta_estimate(&g_ld, base, d->size, changed_fn, &cc, &e), OK);
        cdc_delta_result_t dr;
        assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
        assert_true(e.data_bytes > 0);
        if (e.estimate > dr.rechunked)
            fail_msg("seed %llu: estimate %llu over the %llu actually re-chunked", (unsigned long long)seed,
                     (unsigned long long)e.estimate, (unsigned long long)dr.rechunked);
        /* And it is close: at most a few divergent chunks short. */
        if (dr.rechunked - e.estimate > (uint64_t)(4 * e.clusters + 4) * 128 * K)
            fail_msg("seed %llu: estimate %llu, re-chunked %llu over %llu clusters", (unsigned long long)seed,
                     (unsigned long long)e.estimate, (unsigned long long)dr.rechunked, (unsigned long long)e.clusters);
        assert_true(e.ranges >= 1);
        store_reset();
    }
    /* No change: no range, no estimate. */
    memset(d, 0, sizeof(*d));
    d->size = 2 * M;
    disk_put(d, 0, d->size, FAKE_RUN_DATA, 5);
    uint8_t base[OBJECT_HASH_SIZE];
    full_hash(d, FAKE_ALLOC_EXACT, base);
    memset(g, 0, sizeof(*g));
    chg_ctx_t cc = { g };
    cdc_delta_estimate_t e;
    assert_int_equal(cdc_delta_estimate(&g_ld, base, d->size, changed_fn, &cc, &e), OK);
    assert_int_equal(e.ranges, 0);
    assert_int_equal(e.estimate, 0);
    assert_int_equal(e.data_bytes, 2 * M);
    /* A base of another length is refused. */
    assert_int_equal(cdc_delta_estimate(&g_ld, base, d->size + 1, changed_fn, &cc, &e), ERR_INVALID);
    err_clear();
    store_reset();
    free(d);
    free(g);
}

/* Isolated small writes resync within about a chunk, at the format's own
 * chunk sizes. */
static void test_resync_bound(void **state)
{
    (void)state;
    cdc_params_t keep = g_p;
    cdc_params_default(&g_p);
    disk_t *d = malloc(sizeof(*d));
    ranges_t *g = malloc(sizeof(*g));
    assert_non_null(d);
    assert_non_null(g);
    memset(d, 0, sizeof(*d));
    d->size = 64 * M;
    disk_put(d, 0, d->size, FAKE_RUN_DATA, 77);
    uint8_t base[OBJECT_HASH_SIZE];
    full_hash(d, FAKE_ALLOC_EXACT, base);
    g_rng = 4242;
    uint64_t total = 0;
    const int trials = 24;
    for (int t = 0; t < trials; t++) {
        disk_t *n = malloc(sizeof(*n));
        assert_non_null(n);
        memcpy(n, d, sizeof(*n));
        memset(g, 0, sizeof(*g));
        uint64_t s = rnd_below(n->size - 4 * K);
        disk_put(n, s, 4 * K, FAKE_RUN_DATA, rnd() | 1);
        ranges_add(g, s, s + 4 * K, 64 * K, n->size);
        cdc_delta_result_t dr;
        assert_int_equal(delta_hash(n, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
        if (t % 6 == 0) {
            uint8_t want[OBJECT_HASH_SIZE];
            full_hash(n, FAKE_ALLOC_EXACT, want);
            assert_memory_equal(want, dr.r.hash, OBJECT_HASH_SIZE);
        }
        total += dr.resync_bytes;
        free(n);
    }
    uint64_t mean = total / (uint64_t)trials;
    print_message("isolated 4 KiB writes: mean resync %llu bytes\n", (unsigned long long)mean);
    assert_true(mean < 2 * M);
    store_reset();
    g_p = keep;
    free(d);
    free(g);
}

/* Edge shapes: a one-chunk base (a FILE object), an all-hole disk, a write
 * that creates the first data, and the whole disk changed. */
static void test_edges(void **state)
{
    (void)state;
    disk_t *d = malloc(sizeof(*d));
    ranges_t *g = malloc(sizeof(*g));
    assert_non_null(d);
    assert_non_null(g);
    uint8_t base[OBJECT_HASH_SIZE], want[OBJECT_HASH_SIZE];
    cdc_delta_result_t dr;

    /* One chunk, no holes: the base is a FILE object. */
    memset(d, 0, sizeof(*d));
    d->size = 20 * K;
    disk_put(d, 0, d->size, FAKE_RUN_DATA, 3);
    full_hash(d, FAKE_ALLOC_EXACT, base);
    memset(g, 0, sizeof(*g));
    disk_put(d, 100, 10, FAKE_RUN_DATA, 4);
    ranges_add(g, 100, 110, 0, d->size);
    full_hash(d, FAKE_ALLOC_EXACT, want);
    assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
    assert_memory_equal(want, dr.r.hash, OBJECT_HASH_SIZE);

    /* All hole, then data appears in it, then it all goes again. */
    memset(d, 0, sizeof(*d));
    d->size = 4 * M;
    full_hash(d, FAKE_ALLOC_EXACT, base);
    memset(g, 0, sizeof(*g));
    disk_put(d, 1 * M, 300 * K, FAKE_RUN_DATA, 9);
    ranges_add(g, 1 * M, 1 * M + 300 * K, 64 * K, d->size);
    full_hash(d, FAKE_ALLOC_EXACT, want);
    assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
    assert_memory_equal(want, dr.r.hash, OBJECT_HASH_SIZE);
    memcpy(base, want, OBJECT_HASH_SIZE);
    memset(g, 0, sizeof(*g));
    disk_punch(d, 0, d->size);
    ranges_add(g, 1 * M, 1 * M + 300 * K, 64 * K, d->size);
    full_hash(d, FAKE_ALLOC_EXACT, want);
    assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
    assert_memory_equal(want, dr.r.hash, OBJECT_HASH_SIZE);

    /* Every byte changed. */
    memset(d, 0, sizeof(*d));
    d->size = 3 * M + 17;
    disk_put(d, 0, d->size, FAKE_RUN_DATA, 1);
    full_hash(d, FAKE_ALLOC_EXACT, base);
    memset(g, 0, sizeof(*g));
    disk_put(d, 0, d->size, FAKE_RUN_DATA, 2);
    ranges_add(g, 0, d->size, 0, d->size);
    full_hash(d, FAKE_ALLOC_EXACT, want);
    assert_int_equal(delta_hash(d, FAKE_ALLOC_EXACT, base, g, NULL, &dr), OK);
    assert_memory_equal(want, dr.r.hash, OBJECT_HASH_SIZE);

    /* A source that cannot seek is refused. */
    sp_block_stream_t bs;
    block_source_t *b = NULL;
    cdc_source_t src;
    open_src(d, FAKE_ALLOC_EXACT, 1, &bs, &b, &src);
    src.seek = NULL;
    chg_ctx_t cc = { g };
    assert_int_equal(cdc_store_delta(&g_p, &g_ld, base, &src, d->size, changed_fn, &cc, &g_sink, NULL, &dr), ERR_INVALID);
    close_src(&bs, b);
    err_clear();
    store_reset();
    free(d);
    free(g);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_edges),
        cmocka_unit_test(test_invariant),
        cmocka_unit_test(test_over_reporting),
        cmocka_unit_test(test_under_reporting),
        cmocka_unit_test(test_estimate),
        cmocka_unit_test(test_resync_bound),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
