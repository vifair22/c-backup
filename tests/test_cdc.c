#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * Content-defined chunking and chunk lists (repository format 3).
 *
 * What these hold the code to, in order of how quietly each would fail:
 *
 *   - determinism: the same bytes give the same chunks, the same list and
 *     the same hash whatever the read sizes and however many workers ran.
 *     If this breaks, dedup silently stops working and nothing errors;
 *   - shift resistance: one byte inserted at the front of a large file
 *     costs a chunk or two, not the file;
 *   - the format's bounds and canonical form, on read as well as write;
 *   - byte-exact restore, holes included.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#include "cdc.h"
#include "pack.h"
#include "chunklist.h"
#include "object.h"
#include "store.h"
#include "test_tmp.h"

#define TEST_ROOT TEST_TMP "/c_backup_test_cdc"
#define REPO      TEST_ROOT "/repo"
#define SRC       TEST_ROOT "/src"
#define DST       TEST_ROOT "/dst"

#define MiB ((size_t)1024 * 1024)

static store_t *store;
static cdc_params_t P;

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
    { int rc = system("mkdir -p '" TEST_ROOT "'"); (void)rc; }
    if (store_init_dirs(REPO) != OK) return -1;
    if (store_open(REPO, NULL, &store) != OK) return -1;
    cdc_params_default(&P);
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

/* xorshift64*, so every test's data is reproducible */
static uint64_t rng_state;
static uint64_t rnd(void)
{
    uint64_t x = rng_state;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    rng_state = x;
    return x * 0x2545F4914F6CDD1DULL;
}

static uint8_t *rand_buf(size_t n, uint64_t seed)
{
    rng_state = seed ? seed : 1;
    uint8_t *b = malloc(n ? n : 1);
    assert_non_null(b);
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = rnd();
        size_t k = n - i < 8 ? n - i : 8;
        memcpy(b + i, &v, k);
    }
    return b;
}

static void write_file(const char *path, const void *data, size_t n)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    const uint8_t *p = data;
    while (n) {
        ssize_t w = write(fd, p, n);
        assert_true(w > 0);
        p += w; n -= (size_t)w;
    }
    close(fd);
}

/* A sink over object_store_ex that workers can share. */
typedef struct {
    pthread_mutex_t mu;
    store_t *st;
    unsigned puts;
} locked_sink_t;

static status_t locked_put(void *ctx, uint8_t type, const void *data,
                           size_t len, obj_put_t *out)
{
    locked_sink_t *s = ctx;
    pthread_mutex_lock(&s->mu);
    status_t st = object_store_ex(s->st, type, data, len, out->hash,
                                  &out->is_new, &out->phys_bytes);
    s->puts++;
    pthread_mutex_unlock(&s->mu);
    return st;
}

static status_t store_path(const char *path, cdc_workers_t *w, cdc_result_t *r)
{
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    locked_sink_t ls = { .st = store };
    pthread_mutex_init(&ls.mu, NULL);
    obj_sink_t sink = { .put = locked_put, .ctx = &ls };
    cdc_opts_t o = { .workers = w };
    status_t rc = cdc_store_fd(&P, fd, (uint64_t)st.st_size, &sink, &o, r);
    pthread_mutex_destroy(&ls.mu);
    close(fd);
    return rc;
}

static uint8_t *slurp(const char *path, size_t *n)
{
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    fstat(fd, &st);
    uint8_t *b = malloc((size_t)st.st_size + 1);
    assert_non_null(b);
    size_t got = 0;
    while (got < (size_t)st.st_size) {
        ssize_t r = read(fd, b + got, (size_t)st.st_size - got);
        assert_true(r > 0);
        got += (size_t)r;
    }
    close(fd);
    *n = got;
    return b;
}

static void restore_and_compare(const uint8_t hash[OBJECT_HASH_SIZE],
                                const uint8_t *want, size_t n)
{
    unlink(DST);
    int fd = open(DST, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    uint64_t size = 0;
    assert_int_equal(content_restore_fd(store, hash, fd, &size), OK);
    close(fd);
    assert_int_equal(size, n);
    size_t got_n = 0;
    uint8_t *got = slurp(DST, &got_n);
    assert_int_equal(got_n, n);
    assert_memory_equal(got, want, n);
    free(got);
}

/* ------------------------------------------------------------------ */
/* Parameters and scanner                                              */
/* ------------------------------------------------------------------ */

static void test_params_bounds(void **state)
{
    (void)state;
    cdc_params_t p;
    assert_int_equal(cdc_params_init(&p, 1024, 4096, 8192, NULL), ERR_INVALID);
    assert_int_equal(cdc_params_init(&p, 8192, 6000, 65536, NULL), ERR_INVALID);
    assert_int_equal(cdc_params_init(&p, 8192, 16384, 32 * MiB, NULL), ERR_INVALID);
    assert_int_equal(cdc_params_init(&p, 65536, 32768, 1 * MiB, NULL), ERR_INVALID);
    assert_int_equal(cdc_params_init(&p, CDC_MIN_DEFAULT, CDC_AVG_DEFAULT,
                                     CDC_MAX_DEFAULT, NULL), OK);
    /* log2(1 MiB) = 20: masks of 22 and 18 bits, directly below bit 63 */
    assert_true(p.mask_s == 0x7FFFFE0000000000ULL);
    assert_true(p.mask_l == 0x7FFFE00000000000ULL);
}

/*
 * The unkeyed table is a format constant: these values are in
 * docs/repo-format.md, and a change to them changes every large file's hash
 * in every unencrypted repository.
 */
static void test_gear_table(void **state)
{
    (void)state;
    cdc_params_t a, b, k1, k2, k3;
    cdc_params_default(&a);
    cdc_params_default(&b);
    assert_memory_equal(a.gear, b.gear, sizeof(a.gear));
    assert_false(a.keyed);
    printf("gear[0]=%016llx gear[1]=%016llx gear[255]=%016llx\n",
           (unsigned long long)a.gear[0], (unsigned long long)a.gear[1],
           (unsigned long long)a.gear[255]);
    assert_true(a.gear[0]   == 0x35613eebd8f12fc8ULL);
    assert_true(a.gear[1]   == 0x7347c3514d8276c7ULL);
    assert_true(a.gear[255] == 0x8e60ead3fe2d9631ULL);

    uint8_t key1[32], key2[32];
    memset(key1, 0x11, sizeof(key1));
    memset(key2, 0x22, sizeof(key2));
    assert_int_equal(cdc_params_init(&k1, CDC_MIN_DEFAULT, CDC_AVG_DEFAULT,
                                     CDC_MAX_DEFAULT, key1), OK);
    assert_int_equal(cdc_params_init(&k2, CDC_MIN_DEFAULT, CDC_AVG_DEFAULT,
                                     CDC_MAX_DEFAULT, key2), OK);
    assert_int_equal(cdc_params_init(&k3, CDC_MIN_DEFAULT, CDC_AVG_DEFAULT,
                                     CDC_MAX_DEFAULT, key1), OK);
    assert_true(k1.keyed);
    assert_memory_not_equal(k1.gear, a.gear, sizeof(a.gear));
    assert_memory_not_equal(k1.gear, k2.gear, sizeof(a.gear));
    assert_memory_equal(k1.gear, k3.gear, sizeof(a.gear));
}

/* Cut points for a buffer, scanned in pieces of `step` bytes. */
static size_t cuts_of(const cdc_params_t *p, const uint8_t *buf, size_t n,
                      size_t step, uint64_t *cuts, size_t max_cuts)
{
    cdc_state_t s;
    cdc_reset(&s);
    size_t nc = 0, pos = 0, start = 0;
    while (pos < n) {
        size_t lim = n - pos < step ? n - pos : step;
        int cut = 0;
        size_t k = cdc_scan(p, &s, buf + pos, lim, &cut);
        pos += k;
        if (cut) {
            assert_true(nc < max_cuts);
            cuts[nc++] = pos;
            assert_true(pos - start >= p->min_size);
            assert_true(pos - start <= p->max_size);
            start = pos;
        }
    }
    return nc;
}

/*
 * The definition in docs/repo-format.md, one byte at a time, written
 * independently of cdc_scan's two-byte steps. The two must agree on every
 * cut or the documented format is not the one being written.
 */
static size_t reference_cuts(const cdc_params_t *p, const uint8_t *buf, size_t n,
                             uint64_t *cuts, size_t max_cuts)
{
    size_t nc = 0, start = 0;
    while (start < n) {
        size_t i = start + p->min_size;
        uint64_t fp = 0;
        size_t end = n;
        if (i < n) {
            for (; i < n; i++) {
                size_t len = i - start + 1;
                fp = (fp << 1) + p->gear[buf[i]];
                uint64_t m = len <= p->avg_size ? p->mask_s : p->mask_l;
                if (!(fp & m) || len == p->max_size) { end = i + 1; break; }
            }
        }
        if (end == n) break;    /* the end of input is not a scanner cut */
        assert_true(nc < max_cuts);
        cuts[nc++] = end;
        start = end;
    }
    return nc;
}

static void test_scan_matches_reference(void **state)
{
    (void)state;
    /* Small sizes so a few MiB holds hundreds of cuts, both mask regions,
     * odd and even offsets, and max-size cuts (the zero run). */
    cdc_params_t p;
    assert_int_equal(cdc_params_init(&p, 4096, 16384, 65536, NULL), OK);
    size_t n = 6 * MiB;
    uint8_t *buf = rand_buf(n, 1234);
    memset(buf + 2 * MiB, 0, 512 * 1024);
    static uint64_t a[4096], b[4096];
    size_t na = reference_cuts(&p, buf, n, a, 4096);
    size_t nb = cuts_of(&p, buf, n, 7777, b, 4096);
    printf("reference %zu cuts, scanner %zu\n", na, nb);
    assert_true(na > 100);
    assert_int_equal(na, nb);
    assert_memory_equal(a, b, na * sizeof(a[0]));

    /* And for the defaults */
    na = reference_cuts(&P, buf, n, a, 4096);
    nb = cuts_of(&P, buf, n, n, b, 4096);
    assert_int_equal(na, nb);
    assert_memory_equal(a, b, na * sizeof(a[0]));

    /*
     * Region edges, which random data almost never cuts on. With a mask
     * that always matches (0) and one that practically never does (every
     * bit below 63), the first cut lands exactly where a region starts:
     * byte min + 1 uses mask_s, byte avg + 1 is the first to use mask_l,
     * byte max is cut regardless. Odd and even offsets both, since the
     * scanner takes two bytes a step.
     */
    const uint64_t never = 0x7FFFFFFFFFFFFFFFULL;
    for (int odd = 0; odd < 2; odd++) {
        cdc_params_t q = p;
        size_t step = odd ? 4097 : 4096;
        uint64_t c[64];
        q.mask_s = 0; q.mask_l = never;
        assert_true(cuts_of(&q, buf, 70000, step, c, 64) >= 1);
        assert_true(c[0] == q.min_size + 1);
        assert_int_equal(reference_cuts(&q, buf, 70000, a, 4096) >= 1, 1);
        assert_true(a[0] == c[0]);

        q.mask_s = never; q.mask_l = 0;
        assert_true(cuts_of(&q, buf, 70000, step, c, 64) >= 1);
        assert_true(c[0] == q.avg_size + 1);
        assert_int_equal(reference_cuts(&q, buf, 70000, a, 4096) >= 1, 1);
        assert_true(a[0] == c[0]);

        q.mask_s = never; q.mask_l = never;
        assert_true(cuts_of(&q, buf, 70000, step, c, 64) >= 1);
        assert_true(c[0] == q.max_size);
        assert_int_equal(reference_cuts(&q, buf, 70000, a, 4096) >= 1, 1);
        assert_true(a[0] == c[0]);
    }
    free(buf);
}

static void test_scan_streaming_invariant(void **state)
{
    (void)state;
    size_t n = 48 * MiB;
    uint8_t *buf = rand_buf(n, 7);
    static uint64_t a[256], b[256], c[256];
    size_t na = cuts_of(&P, buf, n, n, a, 256);
    size_t nb = cuts_of(&P, buf, n, 4093, b, 256);
    size_t nc = cuts_of(&P, buf, n, 1 * MiB + 17, c, 256);
    assert_int_equal(na, nb);
    assert_int_equal(na, nc);
    assert_memory_equal(a, b, na * sizeof(a[0]));
    assert_memory_equal(a, c, na * sizeof(a[0]));

    /* Mean chunk on random data: the normalised masks put it a little
     * above avg. Recorded in docs/repo-format.md. */
    double mean = (double)a[na - 1] / (double)na;
    printf("random 48 MiB: %zu cuts, mean chunk %.0f KiB\n", na, mean / 1024.0);
    assert_true(mean > 0.8 * (double)MiB && mean < 1.8 * (double)MiB);
    free(buf);
}

static void test_scan_max_cut(void **state)
{
    (void)state;
    /* A run that never satisfies a mask is cut at exactly max_size. Zeroes
     * give fp a fixed point quickly; whatever it settles to, every chunk is
     * within bounds and the scan terminates. */
    size_t n = 20 * MiB;
    uint8_t *buf = calloc(1, n);
    static uint64_t cuts[64];
    size_t nc = cuts_of(&P, buf, n, 1 * MiB, cuts, 64);
    assert_true(nc >= 2);
    free(buf);
}

static void test_name_hash(void **state)
{
    (void)state;
    uint64_t a = cdc_name_hash(&P, "alpha", 5);
    uint64_t b = cdc_name_hash(&P, "alphb", 5);
    assert_true(a != b);
    assert_true(a == cdc_name_hash(&P, "alpha", 5));
    assert_true(cdc_name_hash(&P, "", 0) != cdc_name_hash(&P, "a", 1));
}

/* ------------------------------------------------------------------ */
/* Chunk list format                                                   */
/* ------------------------------------------------------------------ */

static void fill_hash(uint8_t h[OBJECT_HASH_SIZE], uint64_t v)
{
    rng_state = v * 2654435761u + 1;
    for (int i = 0; i < OBJECT_HASH_SIZE; i += 8) {
        uint64_t x = rnd();
        memcpy(h + i, &x, 8);
    }
}

static void test_parse_rejects(void **state)
{
    (void)state;
    chunklist_entry_t e[3];
    memset(e, 0, sizeof(e));
    e[0].offset = 0;   e[0].length = 10; fill_hash(e[0].hash, 1);
    e[1].offset = 10;  e[1].length = 10; fill_hash(e[1].hash, 2);
    e[2].offset = 100; e[2].length = 5;  fill_hash(e[2].hash, 3);
    chunklist_hdr_t h = { .level = 0, .leaf_type = OBJECT_TYPE_FILE,
                          .count = 3, .total_len = 200 };
    uint8_t buf[CHUNKLIST_HDR_BYTES + 3 * CHUNKLIST_ENTRY_BYTES];
    size_t n = chunklist_serialise(&h, e, buf);
    assert_int_equal(n, sizeof(buf));
    chunklist_hdr_t out;
    assert_int_equal(chunklist_parse(buf, n, &out), OK);
    assert_int_equal(out.count, 3);
    assert_true(out.total_len == 200);

    /* Truncated, trailing garbage */
    assert_int_equal(chunklist_parse(buf, n - 1, &out), ERR_CORRUPT);
    uint8_t big[sizeof(buf) + 1];
    memcpy(big, buf, n); big[n] = 0;
    assert_int_equal(chunklist_parse(big, n + 1, &out), ERR_CORRUPT);
    assert_int_equal(chunklist_parse(buf, 10, &out), ERR_CORRUPT);

    uint8_t bad[sizeof(buf)];
    /* magic */
    memcpy(bad, buf, n); bad[0] ^= 1;
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* version */
    memcpy(bad, buf, n); bad[4] = 2;
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* level */
    memcpy(bad, buf, n); bad[6] = CHUNKLIST_MAX_LEVEL + 1;
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* leaf type */
    memcpy(bad, buf, n); bad[7] = OBJECT_TYPE_ACL;
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);

    /* overlap */
    chunklist_entry_t o[3];
    memcpy(o, e, sizeof(o)); o[1].offset = 5;
    chunklist_serialise(&h, o, bad);
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* empty entry */
    memcpy(o, e, sizeof(o)); o[2].length = 0;
    chunklist_serialise(&h, o, bad);
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* past total */
    memcpy(o, e, sizeof(o)); o[2].length = 101;
    chunklist_serialise(&h, o, bad);
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* wrapping */
    memcpy(o, e, sizeof(o)); o[2].offset = UINT64_MAX - 2; o[2].length = 5;
    chunklist_serialise(&h, o, bad);
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
    /* count over the bound, length to match would be huge: refused on count */
    memcpy(bad, buf, n);
    bad[8] = 0xFF; bad[9] = 0xFF; bad[10] = 0xFF; bad[11] = 0x00;
    assert_int_equal(chunklist_parse(bad, n, &out), ERR_CORRUPT);
}

/* Build a list over synthetic leaves into the real store (lists only; the
 * leaves are never loaded by the walker). */
static void build_synthetic(uint64_t nleaves, uint64_t first_len, uint64_t seed,
                            uint8_t root[OBJECT_HASH_SIZE], chunklist_stats_t *cs,
                            uint64_t *total)
{
    obj_sink_t sink = obj_sink_store(store);
    chunklist_builder_t *b = NULL;
    assert_int_equal(chunklist_builder_new(OBJECT_TYPE_FILE, &sink, &b), OK);
    uint64_t off = 0;
    for (uint64_t i = 0; i < nleaves; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        uint64_t len = (i == 0) ? first_len : 1000 + (i % 7);
        fill_hash(h, seed + i);
        assert_int_equal(chunklist_builder_add(b, off, len, h), OK);
        off += len;
    }
    assert_int_equal(chunklist_builder_finish(b, off, root), OK);
    chunklist_builder_stats(b, cs);
    chunklist_builder_free(b);
    *total = off;
}

static void test_builder_hierarchy(void **state)
{
    (void)state;
    const uint64_t N = 100000;
    uint8_t root[OBJECT_HASH_SIZE], root2[OBJECT_HASH_SIZE];
    chunklist_stats_t cs, cs2;
    uint64_t total = 0, total2 = 0;
    build_synthetic(N, 1000, 1000, root, &cs, &total);
    printf("100k leaves: %llu lists\n", (unsigned long long)cs.lists);
    /* More than one level: every list is bounded, so 100k leaves need
     * several. */
    assert_true(cs.lists > 1);
    assert_true(cs.lists < 2 * (N / CHUNKLIST_MIN_ENTRIES) + 4);

    /* Same leaves, same root, nothing new stored */
    build_synthetic(N, 1000, 1000, root2, &cs2, &total2);
    assert_memory_equal(root, root2, OBJECT_HASH_SIZE);
    assert_int_equal(cs2.lists_new, 0);

    /* Walk: every leaf back, in order, offsets as given */
    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t h;
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 0, &h, &it), OK);
    assert_true(h.total_len == total);
    assert_true(h.level >= 1);
    uint64_t i = 0, off = 0;
    for (;;) {
        chunklist_item_t x;
        int done = 0;
        assert_int_equal(chunklist_iter_next(it, &x, &done), OK);
        if (done) break;
        assert_false(x.is_list);
        uint8_t want[OBJECT_HASH_SIZE];
        fill_hash(want, 1000 + i);
        assert_memory_equal(x.hash, want, OBJECT_HASH_SIZE);
        assert_true(x.offset == off);
        off += x.length;
        i++;
    }
    assert_true(i == N);
    chunklist_iter_close(it);

    /* Wrong leaf type is refused at the root */
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_TREE, 0, NULL, &it),
                     ERR_CORRUPT);
}

/*
 * Shift resistance at the list level: one leaf inserted at the front moves
 * every offset, and relative offsets plus content-defined grouping mean only
 * the sub-lists on the path to it change.
 */
static void test_builder_shift(void **state)
{
    (void)state;
    const uint64_t N = 60000;
    uint8_t root[OBJECT_HASH_SIZE], root2[OBJECT_HASH_SIZE];
    chunklist_stats_t cs, cs2;
    uint64_t total = 0;
    build_synthetic(N, 1000, 5000, root, &cs, &total);

    obj_sink_t sink = obj_sink_store(store);
    chunklist_builder_t *b = NULL;
    assert_int_equal(chunklist_builder_new(OBJECT_TYPE_FILE, &sink, &b), OK);
    uint8_t h[OBJECT_HASH_SIZE];
    fill_hash(h, 999999999);
    assert_int_equal(chunklist_builder_add(b, 0, 777, h), OK);
    uint64_t off = 777;
    for (uint64_t i = 0; i < N; i++) {
        uint64_t len = (i == 0) ? 1000 : 1000 + (i % 7);
        fill_hash(h, 5000 + i);
        assert_int_equal(chunklist_builder_add(b, off, len, h), OK);
        off += len;
    }
    assert_int_equal(chunklist_builder_finish(b, off, root2), OK);
    chunklist_builder_stats(b, &cs2);
    chunklist_builder_free(b);
    printf("shift: %llu lists, %llu new after insert\n",
           (unsigned long long)cs.lists, (unsigned long long)cs2.lists_new);
    assert_memory_not_equal(root, root2, OBJECT_HASH_SIZE);
    assert_true(cs2.lists_new <= 4);
    assert_true(cs.lists >= 8);
}

static void test_builder_holes_and_empty(void **state)
{
    (void)state;
    obj_sink_t sink = obj_sink_store(store);
    chunklist_builder_t *b = NULL;
    uint8_t root[OBJECT_HASH_SIZE];

    /* All hole: empty root, the length alone */
    assert_int_equal(chunklist_builder_new(OBJECT_TYPE_FILE, &sink, &b), OK);
    assert_int_equal(chunklist_builder_finish(b, 1ULL << 40, root), OK);
    chunklist_builder_free(b);
    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t h;
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 0, &h, &it), OK);
    assert_int_equal(h.count, 0);
    assert_true(h.total_len == (1ULL << 40));
    chunklist_item_t x;
    int done = 0;
    assert_int_equal(chunklist_iter_next(it, &x, &done), OK);
    assert_true(done);
    chunklist_iter_close(it);

    /* Misuse is refused */
    assert_int_equal(chunklist_builder_new(OBJECT_TYPE_FILE, &sink, &b), OK);
    uint8_t hh[OBJECT_HASH_SIZE];
    fill_hash(hh, 1);
    assert_int_equal(chunklist_builder_add(b, 100, 10, hh), OK);
    assert_int_equal(chunklist_builder_add(b, 105, 10, hh), ERR_INVALID);
    assert_int_equal(chunklist_builder_add(b, 200, 0, hh), ERR_INVALID);
    assert_int_equal(chunklist_builder_finish(b, 50, root), ERR_INVALID);
    chunklist_builder_free(b);
}

/* A sub-list that does not start at 0 is not canonical; the walker refuses
 * it even though the bytes parse. */
static void test_iter_rejects_noncanonical_child(void **state)
{
    (void)state;
    chunklist_entry_t child[1];
    fill_hash(child[0].hash, 42);
    child[0].offset = 5;
    child[0].length = 10;
    chunklist_hdr_t ch = { .level = 0, .leaf_type = OBJECT_TYPE_FILE,
                           .count = 1, .total_len = 15 };
    uint8_t buf[CHUNKLIST_HDR_BYTES + CHUNKLIST_ENTRY_BYTES];
    size_t n = chunklist_serialise(&ch, child, buf);
    uint8_t ch_hash[OBJECT_HASH_SIZE] = { 0 };
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, buf, n, ch_hash), OK);

    chunklist_entry_t top[1];
    memcpy(top[0].hash, ch_hash, OBJECT_HASH_SIZE);
    top[0].offset = 0;
    top[0].length = 15;
    chunklist_hdr_t th = { .level = 1, .leaf_type = OBJECT_TYPE_FILE,
                           .count = 1, .total_len = 15 };
    n = chunklist_serialise(&th, top, buf);
    uint8_t root[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_CHUNKLIST, buf, n, root), OK);

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 0, NULL, &it), OK);
    chunklist_item_t x;
    int done = 0;
    assert_int_equal(chunklist_iter_next(it, &x, &done), ERR_CORRUPT);
    chunklist_iter_close(it);

    /* A FILE object where a list is expected */
    uint8_t fh[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "abc", 3, fh), OK);
    assert_int_equal(chunklist_iter_open(&ld, fh, OBJECT_TYPE_FILE, 0, NULL, &it),
                     ERR_CORRUPT);
}

/* ------------------------------------------------------------------ */
/* Files                                                               */
/* ------------------------------------------------------------------ */

static void test_small_file_is_one_object(void **state)
{
    (void)state;
    size_t n = 100 * 1024;
    uint8_t *data = rand_buf(n, 3);
    write_file(SRC, data, n);
    cdc_result_t r;
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    assert_int_equal(r.type, OBJECT_TYPE_FILE);
    assert_true(r.chunks == 1);
    /* The same object a plain store of the bytes makes */
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, n, h), OK);
    assert_memory_equal(h, r.hash, OBJECT_HASH_SIZE);
    restore_and_compare(r.hash, data, n);

    /* Empty file */
    write_file(SRC, data, 0);
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    assert_int_equal(r.type, OBJECT_TYPE_FILE);
    restore_and_compare(r.hash, data, 0);
    free(data);
}

static void test_large_file_roundtrip_and_determinism(void **state)
{
    (void)state;
    size_t n = 40 * MiB + 12345;
    uint8_t *data = rand_buf(n, 11);
    write_file(SRC, data, n);

    cdc_result_t r1, r2, r4;
    assert_int_equal(store_path(SRC, NULL, &r1), OK);
    assert_int_equal(r1.type, OBJECT_TYPE_CHUNKLIST);
    assert_true(r1.chunks > 10);
    assert_true(r1.chunks_new == r1.chunks);
    assert_true(r1.data_bytes == n);

    /* Again, serial: identical, nothing new */
    assert_int_equal(store_path(SRC, NULL, &r2), OK);
    assert_memory_equal(r1.hash, r2.hash, OBJECT_HASH_SIZE);
    assert_true(r2.chunks == r1.chunks);
    assert_true(r2.chunks_new == 0);
    assert_true(r2.lists.lists_new == 0);

    /* On four workers: identical */
    cdc_workers_t *w = NULL;
    assert_int_equal(cdc_workers_start(4, &w), OK);
    assert_int_equal(store_path(SRC, w, &r4), OK);
    cdc_workers_stop(w);
    assert_memory_equal(r1.hash, r4.hash, OBJECT_HASH_SIZE);
    assert_true(r4.chunks == r1.chunks);
    assert_true(r4.chunks_new == 0);

    restore_and_compare(r1.hash, data, n);

    /* Streamed and prefix forms agree */
    void *pre = NULL;
    size_t pre_n = 0;
    uint64_t full = 0;
    assert_int_equal(content_read_prefix(store, r1.hash, 3 * MiB + 7, &pre,
                                         &pre_n, &full), OK);
    assert_int_equal(pre_n, 3 * MiB + 7);
    assert_true(full == n);
    assert_memory_equal(pre, data, pre_n);
    free(pre);
    free(data);
}

/* Fresh store, four workers, then serial: the same objects either way. */
static void test_workers_first(void **state)
{
    (void)state;
    size_t n = 24 * MiB;
    uint8_t *data = rand_buf(n, 77);
    write_file(SRC, data, n);
    cdc_workers_t *w = NULL;
    assert_int_equal(cdc_workers_start(3, &w), OK);
    cdc_result_t a, b;
    assert_int_equal(store_path(SRC, w, &a), OK);
    cdc_workers_stop(w);
    assert_true(a.chunks_new == a.chunks);
    assert_int_equal(store_path(SRC, NULL, &b), OK);
    assert_memory_equal(a.hash, b.hash, OBJECT_HASH_SIZE);
    assert_true(b.chunks_new == 0);
    restore_and_compare(a.hash, data, n);
    free(data);
}

typedef struct {
    uint64_t chunks, chunks_new, bytes_new, written;
    uint8_t  hash[OBJECT_HASH_SIZE];
} delta_t;

static delta_t store_delta(const char *path)
{
    store_write_stats_t before, after, d;
    store_write_stats(store, &before);
    cdc_result_t r;
    assert_int_equal(store_path(path, NULL, &r), OK);
    /* Objects stage into the store's own pack; sealing it is the write. */
    assert_int_equal(object_store_flush(store), OK);
    store_write_stats(store, &after);
    delta_t out;
    out.written = store_write_stats_diff(&after, &before, &d);
    out.chunks = r.chunks;
    out.chunks_new = r.chunks_new;
    out.bytes_new = r.chunk_bytes_new;
    memcpy(out.hash, r.hash, OBJECT_HASH_SIZE);
    return out;
}

static void test_shift_append_edit(void **state)
{
    (void)state;
    size_t n = 64 * MiB;
    uint8_t *data = rand_buf(n, 21);
    write_file(SRC, data, n);
    delta_t base = store_delta(SRC);
    printf("base: %llu chunks, written %llu\n",
           (unsigned long long)base.chunks, (unsigned long long)base.written);
    assert_true(base.written >= n);

    /* One byte inserted at the front */
    uint8_t *ins = malloc(n + 1);
    ins[0] = 0x5A;
    memcpy(ins + 1, data, n);
    write_file(SRC, ins, n + 1);
    delta_t sh = store_delta(SRC);
    printf("insert 1 byte: %llu of %llu chunks new, %llu bytes new, %llu written\n",
           (unsigned long long)sh.chunks_new, (unsigned long long)sh.chunks,
           (unsigned long long)sh.bytes_new, (unsigned long long)sh.written);
    assert_true(sh.chunks_new <= 2);
    assert_true(sh.written < n / 8);
    free(ins);

    /* 1 MiB appended */
    uint8_t *app = malloc(n + MiB);
    memcpy(app, data, n);
    uint8_t *tail = rand_buf(MiB, 99);
    memcpy(app + n, tail, MiB);
    free(tail);
    write_file(SRC, app, n + MiB);
    delta_t ap = store_delta(SRC);
    printf("append 1 MiB: %llu of %llu chunks new, %llu bytes new, %llu written\n",
           (unsigned long long)ap.chunks_new, (unsigned long long)ap.chunks,
           (unsigned long long)ap.bytes_new, (unsigned long long)ap.written);
    assert_true(ap.chunks_new <= 3);
    assert_true(ap.written < (uint64_t)MiB + 2 * CDC_MAX_DEFAULT);
    free(app);

    /* 2% of the file rewritten in the middle */
    uint8_t *ed = malloc(n);
    memcpy(ed, data, n);
    size_t span = n / 50;
    uint8_t *junk = rand_buf(span, 1234);
    memcpy(ed + n / 2, junk, span);
    free(junk);
    write_file(SRC, ed, n);
    delta_t e2 = store_delta(SRC);
    printf("2%% edit: %llu of %llu chunks new, %llu bytes new, %llu written\n",
           (unsigned long long)e2.chunks_new, (unsigned long long)e2.chunks,
           (unsigned long long)e2.bytes_new, (unsigned long long)e2.written);
    /* the edited span plus a chunk either side, on average */
    assert_true(e2.written < span + 4 * (uint64_t)CDC_AVG_DEFAULT * 2);
    restore_and_compare(e2.hash, ed, n);
    free(ed);
    free(data);
}

static void test_sparse_file(void **state)
{
    (void)state;
    size_t total = 64 * MiB;
    int fd = open(SRC, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)total), 0);
    uint8_t *a = rand_buf(MiB, 5);
    uint8_t *b = rand_buf(3 * MiB, 6);
    assert_int_equal(pwrite(fd, a, MiB, 10 * (off_t)MiB), (ssize_t)MiB);
    assert_int_equal(pwrite(fd, b, 3 * MiB, 40 * (off_t)MiB), (ssize_t)(3 * MiB));
    close(fd);

    uint8_t *want = calloc(1, total);
    memcpy(want + 10 * MiB, a, MiB);
    memcpy(want + 40 * MiB, b, 3 * MiB);

    cdc_result_t r;
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    assert_int_equal(r.type, OBJECT_TYPE_CHUNKLIST);
    struct stat sst;
    stat(SRC, &sst);
    if ((uint64_t)sst.st_blocks * 512 < total / 2) {
        /* The filesystem kept the holes: only data was read */
        printf("sparse: read %llu of %zu bytes\n",
               (unsigned long long)r.data_bytes, total);
        assert_true(r.data_bytes <= 4 * MiB + 2 * CDC_HOLE_MIN);
    }
    restore_and_compare(r.hash, want, total);
    struct stat dst;
    stat(DST, &dst);
    if ((uint64_t)sst.st_blocks * 512 < total / 2)
        assert_true((uint64_t)dst.st_blocks * 512 < total / 2);

    /* Holes read back as zeroes through the stream form too */
    void *pre = NULL;
    size_t pre_n = 0;
    uint64_t full = 0;
    assert_int_equal(content_read_prefix(store, r.hash, 11 * MiB, &pre, &pre_n,
                                         &full), OK);
    assert_memory_equal(pre, want, pre_n);
    free(pre);

    /* Entirely hole */
    fd = open(SRC, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_int_equal(ftruncate(fd, (off_t)total), 0);
    close(fd);
    stat(SRC, &sst);
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    if (sst.st_blocks == 0) {
        assert_int_equal(r.type, OBJECT_TYPE_CHUNKLIST);
        assert_true(r.chunks == 0);
        assert_true(r.data_bytes == 0);
    }
    memset(want, 0, total);
    restore_and_compare(r.hash, want, total);

    free(want);
    free(a);
    free(b);
}

/* A hole under CDC_HOLE_MIN is data: a small file with one is a single
 * FILE object, the same as the file with that range written as zeroes. */
static void test_short_hole_is_data(void **state)
{
    (void)state;
    size_t n = 200 * 1024;
    uint8_t *data = rand_buf(n, 8);
    memset(data + 64 * 1024, 0, 16 * 1024);
    int fd = open(SRC, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    assert_int_equal(pwrite(fd, data, 64 * 1024, 0), 64 * 1024);
    assert_int_equal(pwrite(fd, data + 80 * 1024, n - 80 * 1024, 80 * 1024),
                     (ssize_t)(n - 80 * 1024));
    close(fd);
    cdc_result_t r;
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    assert_int_equal(r.type, OBJECT_TYPE_FILE);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, n, h), OK);
    assert_memory_equal(h, r.hash, OBJECT_HASH_SIZE);
    free(data);
}

/* A file that shrinks between scan and read is refused, not stored short. */
static void test_file_shorter_than_scanned(void **state)
{
    (void)state;
    size_t n = 2 * MiB;
    uint8_t *data = rand_buf(n, 9);
    write_file(SRC, data, n);
    int fd = open(SRC, O_RDONLY);
    obj_sink_t sink = obj_sink_store(store);
    cdc_result_t r;
    assert_int_equal(cdc_store_fd(&P, fd, n + 4096, &sink, NULL, &r), ERR_IO);
    close(fd);
    free(data);
}

/* A sink that fails partway: the error comes back, serial and pooled, and
 * nothing is left running. */
typedef struct { locked_sink_t ls; unsigned fail_at; } failing_sink_t;

static status_t failing_put(void *ctx, uint8_t type, const void *data,
                            size_t len, obj_put_t *out)
{
    failing_sink_t *f = ctx;
    pthread_mutex_lock(&f->ls.mu);
    unsigned k = f->ls.puts++;
    pthread_mutex_unlock(&f->ls.mu);
    if (k == f->fail_at) return set_error(ERR_IO, "injected put failure");
    pthread_mutex_lock(&f->ls.mu);
    status_t st = object_store_ex(f->ls.st, type, data, len, out->hash,
                                  &out->is_new, &out->phys_bytes);
    pthread_mutex_unlock(&f->ls.mu);
    return st;
}

static void test_sink_failure(void **state)
{
    (void)state;
    size_t n = 20 * MiB;
    uint8_t *data = rand_buf(n, 31);
    write_file(SRC, data, n);
    for (int pooled = 0; pooled < 2; pooled++) {
        failing_sink_t f;
        memset(&f, 0, sizeof(f));
        pthread_mutex_init(&f.ls.mu, NULL);
        f.ls.st = store;
        f.fail_at = 5;
        obj_sink_t sink = { .put = failing_put, .ctx = &f };
        cdc_workers_t *w = NULL;
        if (pooled) assert_int_equal(cdc_workers_start(4, &w), OK);
        cdc_opts_t o = { .workers = w };
        int fd = open(SRC, O_RDONLY);
        cdc_result_t r;
        assert_int_equal(cdc_store_fd(&P, fd, n, &sink, &o, &r), ERR_IO);
        assert_non_null(strstr(err_msg(), "injected"));
        close(fd);
        cdc_workers_stop(w);
        pthread_mutex_destroy(&f.ls.mu);
    }
    free(data);
}

/* Every object a large file depends on is visited, lists once. */
typedef struct { unsigned files, lists; uint8_t seen[64][OBJECT_HASH_SIZE]; unsigned nseen; } visit_t;

static int visit_cb(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type)
{
    visit_t *v = ctx;
    for (unsigned i = 0; i < v->nseen; i++)
        if (!memcmp(v->seen[i], hash, OBJECT_HASH_SIZE)) return 1;
    if (v->nseen < 64) memcpy(v->seen[v->nseen++], hash, OBJECT_HASH_SIZE);
    if (type == OBJECT_TYPE_CHUNKLIST) v->lists++; else v->files++;
    return 0;
}

static void test_collect(void **state)
{
    (void)state;
    size_t n = 30 * MiB;
    uint8_t *data = rand_buf(n, 41);
    write_file(SRC, data, n);
    cdc_result_t r;
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    visit_t v;
    memset(&v, 0, sizeof(v));
    assert_int_equal(content_collect(store, r.hash, OBJECT_TYPE_FILE, visit_cb, &v), OK);
    assert_int_equal(v.lists, 1);
    assert_true(v.files == r.chunks);
    /* Second time: the root is seen, nothing below it is visited */
    unsigned before = v.nseen;
    assert_int_equal(content_collect(store, r.hash, OBJECT_TYPE_FILE, visit_cb, &v), OK);
    assert_int_equal(v.nseen, before);
    free(data);
}

/* A damaged chunk fails the restore loudly; it is never written out. */
static void test_corrupt_chunk_detected(void **state)
{
    (void)state;
    size_t n = 12 * MiB;
    uint8_t *data = rand_buf(n, 51);
    write_file(SRC, data, n);
    cdc_result_t r;
    assert_int_equal(store_path(SRC, NULL, &r), OK);
    assert_int_equal(r.type, OBJECT_TYPE_CHUNKLIST);

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    assert_int_equal(chunklist_iter_open(&ld, r.hash, OBJECT_TYPE_FILE, 0, NULL, &it), OK);
    chunklist_item_t x;
    int done = 0;
    assert_int_equal(chunklist_iter_next(it, &x, &done), OK);
    assert_int_equal(chunklist_iter_next(it, &x, &done), OK);
    chunklist_iter_close(it);

    /* Flip far more bytes in the second chunk's pack entry than its
     * parity can correct. */
    assert_int_equal(object_store_flush(store), OK);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, x.hash, &pnum, &off), OK);
    char path[512];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), REPO, pnum), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t junk[65536];
    memset(junk, 0xEE, sizeof(junk));
    assert_int_equal(pwrite(fd, junk, sizeof(junk),
                            (off_t)(off + sizeof(pack_dat_entry_hdr_t) + 4096)),
                     (ssize_t)sizeof(junk));
    close(fd);
    pack_cache_invalidate(store);

    unlink(DST);
    fd = open(DST, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_int_not_equal(content_restore_fd(store, r.hash, fd, NULL), OK);
    close(fd);
    free(data);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_params_bounds, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gear_table, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_matches_reference, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_streaming_invariant, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_max_cut, setup, teardown),
        cmocka_unit_test_setup_teardown(test_name_hash, setup, teardown),
        cmocka_unit_test_setup_teardown(test_parse_rejects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_builder_hierarchy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_builder_shift, setup, teardown),
        cmocka_unit_test_setup_teardown(test_builder_holes_and_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_iter_rejects_noncanonical_child, setup, teardown),
        cmocka_unit_test_setup_teardown(test_small_file_is_one_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_file_roundtrip_and_determinism, setup, teardown),
        cmocka_unit_test_setup_teardown(test_workers_first, setup, teardown),
        cmocka_unit_test_setup_teardown(test_shift_append_edit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sparse_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_short_hole_is_data, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_shorter_than_scanned, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sink_failure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_collect, setup, teardown),
        cmocka_unit_test_setup_teardown(test_corrupt_chunk_detected, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
