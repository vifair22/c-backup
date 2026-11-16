#define _POSIX_C_SOURCE 200809L

/*
 * Packed-entry parity in bounded memory, measured.
 *
 * Every read of a packed entry used to load the entry's whole RS parity into
 * one allocation - 6.7% of the stored size, 3.3 GiB for a 50 GiB entry - and
 * repair stopped at 16 MiB (OWN-24, OWN-23). Parity is now fetched a window
 * of groups at a time. To prove the bound without an entry larger than the
 * machine, the window is lowered to four groups (61 KiB of data, 4 KiB of
 * parity) so an entry of a few MiB crosses dozens of windows, and the
 * largest single allocation made while reading or repairing it is checked
 * against the window rather than the entry. fault_malloc_peak sees every
 * allocation this program makes; liblz4's and libcrypto's own are invisible
 * to it, and fixed in size.
 *
 * Linked with the fault injector for that counter, not to inject faults.
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
#include <pthread.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <openssl/sha.h>

#include "fake_index.h"
#include "fault_inject.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_pack_parity_bound"

#define GROUP_DATA  ((size_t)RS_K * RS_INTERLEAVE)    /* 15,296 */
#define GROUP_PAR   ((size_t)RS_2T * RS_INTERLEAVE)   /* 1,024 */
#define WIN_GROUPS  4u
/* Everything a windowed read or repair allocates, with slack for the small
 * fixed allocations around it. A whole-parity read of any fixture below is
 * several times this. */
#define PEAK_BOUND  ((size_t)64 * 1024)

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
    fault_reset_all();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    parity_stats_reset();
    pack_test_set_parity_window(WIN_GROUPS);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    pack_test_set_parity_window(0);
    pack_test_set_parity_fit_limit(0);
    if (store) { store_close(store); store = NULL; }
    cleanup();
    return 0;
}

static uint32_t xs(uint32_t *s)
{
    *s ^= *s << 13; *s ^= *s >> 17; *s ^= *s << 5;
    return *s;
}

static void fill_random(uint8_t *buf, size_t n, uint32_t seed)
{
    for (size_t i = 0; i < n; i += 4) {
        uint32_t v = xs(&seed);
        size_t take = n - i < 4 ? n - i : 4;
        memcpy(buf + i, &v, take);
    }
}

/* 96 random bytes and 32 repeated per 128: LZ4 compresses it to about 78%,
 * so a compressed entry is still many groups long. */
static void fill_semicompressible(uint8_t *buf, size_t n, uint32_t seed)
{
    for (size_t s = 0; s < n; s += 128) {
        size_t seg = n - s < 128 ? n - s : 128;
        size_t rnd = seg < 96 ? seg : 96;
        for (size_t i = 0; i < rnd; i += 4) {
            uint32_t v = xs(&seed);
            memcpy(buf + s + i, &v, rnd - i < 4 ? rnd - i : 4);
        }
        if (seg > 96) memcpy(buf + s + 96, buf + s, seg - 96);
    }
}

static size_t whole_parity(uint64_t csize)
{
    return (size_t)((csize + GROUP_DATA - 1) / GROUP_DATA) * GROUP_PAR;
}

/* Where an entry's payload starts in its pack, and its stored length. */
static void entry_info(const uint8_t hash[OBJECT_HASH_SIZE], char *dat, size_t datsz,
                       off_t *payload, uint64_t *csize, uint8_t *codec)
{
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    assert_int_equal(pack_dat_path_resolve(dat, datsz, TEST_REPO, pnum), 0);
    int fd = open(dat, O_RDONLY);
    assert_true(fd >= 0);
    pack_dat_entry_hdr_t eh;
    assert_int_equal(pread(fd, &eh, sizeof(eh), (off_t)off), (ssize_t)sizeof(eh));
    close(fd);
    *payload = (off_t)(off + sizeof(eh));
    *csize = eh.compressed_size;
    if (codec) *codec = eh.compression;
}

static void flip(const char *dat, off_t off)
{
    int fd = open(dat, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0x10;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
    pack_cache_invalidate(store);
}

/* Stream into a regular file and compare. Returns the peak allocation. */
static size_t stream_and_compare(const uint8_t hash[OBJECT_HASH_SIZE],
                                 const uint8_t *expect, size_t len)
{
    char out[] = TEST_TMP "/c_backup_pack_parity_bound.XXXXXX";
    int fd = mkstemp(out);
    assert_true(fd >= 0);
    fault_malloc_peak = 0;
    uint64_t sz = 0;
    assert_int_equal(object_load_stream(store, hash, fd, &sz, NULL), OK);
    size_t peak = fault_malloc_peak;
    assert_int_equal(sz, len);
    uint8_t *got = malloc(len);
    assert_non_null(got);
    assert_int_equal(pread(fd, got, len, 0), (ssize_t)len);
    assert_memory_equal(got, expect, len);
    free(got);
    close(fd);
    unlink(out);
    return peak;
}

/* ------------------------------------------------------------------ */

/*
 * A stored entry: the streaming read corrects it, and the repair writes it
 * back, both inside the window, for an entry whose parity alone is more
 * than four times the bound.
 */
static void test_stored_entry_read_and_repair_stay_in_window(void **state)
{
    (void)state;
    size_t len = (size_t)4 * 1024 * 1024;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_random(data, len, 0xBADC0DEu);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    char dat[512]; off_t pay = 0; uint64_t csize = 0; uint8_t codec = 0;
    entry_info(hash, dat, sizeof(dat), &pay, &csize, &codec);
    assert_int_equal(codec, COMPRESS_NONE);
    assert_true(whole_parity(csize) > 4 * PEAK_BOUND);

    /* Clean first: one pass, and nothing is repaired. */
    parity_stats_reset();
    assert_true(stream_and_compare(hash, data, len) <= PEAK_BOUND);
    assert_int_equal((int)parity_stats_get().repaired, 0);

    /* Damage in a late window. */
    flip(dat, pay + (off_t)(3 * 1024 * 1024 + 4321));
    parity_stats_reset();
    assert_true(stream_and_compare(hash, data, len) <= PEAK_BOUND);
    assert_true(parity_stats_get().repaired > 0);

    fault_malloc_peak = 0;
    assert_true(pack_object_repair(store, hash) > 0);
    print_message("repair: largest allocation %zu bytes; whole parity %zu bytes\n",
                  (size_t)fault_malloc_peak, whole_parity(csize));
    assert_true(fault_malloc_peak <= PEAK_BOUND);
    assert_int_equal(pack_object_repair(store, hash), 0);

    parity_stats_reset();
    assert_true(stream_and_compare(hash, data, len) <= PEAK_BOUND);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(data);
}

/* The frame codec, which used to be detection-only: corrected while
 * streaming and repaired on disk, inside the same window. */
static void test_frame_entry_read_and_repair_stay_in_window(void **state)
{
    (void)state;
    size_t len = (size_t)PACK_STREAM_THRESHOLD + 1024 * 1024;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_semicompressible(data, len, 0xF00DFACEu);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    char dat[512]; off_t pay = 0; uint64_t csize = 0; uint8_t codec = 0;
    entry_info(hash, dat, sizeof(dat), &pay, &csize, &codec);
    assert_int_equal(codec, COMPRESS_LZ4_FRAME);
    assert_true(whole_parity(csize) > 4 * PEAK_BOUND);

    flip(dat, pay + (off_t)(csize - 20000));
    flip(dat, pay + (off_t)(csize / 3));
    parity_stats_reset();
    assert_true(stream_and_compare(hash, data, len) <= PEAK_BOUND);
    assert_true(parity_stats_get().repaired > 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);

    fault_malloc_peak = 0;
    assert_int_equal(pack_object_repair(store, hash), 2);
    assert_true(fault_malloc_peak <= PEAK_BOUND);

    parity_stats_reset();
    assert_true(stream_and_compare(hash, data, len) <= PEAK_BOUND);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(data);
}

/* A pipe cannot be rewound, so the stored bytes are CRC-scanned first and
 * the emitting pass repairs; this is the path grep and sparse restore use. */
typedef struct { int fd; uint8_t *buf; size_t cap; size_t got; } drain_t;

static void *drain(void *arg)
{
    drain_t *d = arg;
    for (;;) {
        uint8_t tmp[65536];
        ssize_t r = read(d->fd, tmp, sizeof(tmp));
        if (r <= 0) break;
        size_t take = (size_t)r;
        if (d->got + take > d->cap) take = d->cap - d->got;
        memcpy(d->buf + d->got, tmp, take);
        d->got += take;
    }
    return NULL;
}

static void test_damaged_entry_streams_into_a_pipe(void **state)
{
    (void)state;
    size_t len = (size_t)2 * 1024 * 1024;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_random(data, len, 0x1BADB002u);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    char dat[512]; off_t pay = 0; uint64_t csize = 0;
    entry_info(hash, dat, sizeof(dat), &pay, &csize, NULL);
    flip(dat, pay + (off_t)(len / 2));

    int p[2];
    assert_int_equal(pipe(p), 0);
    drain_t d = { p[0], malloc(len + 16), len + 16, 0 };
    assert_non_null(d.buf);
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, drain, &d), 0);

    parity_stats_reset();
    uint64_t sz = 0;
    status_t st = object_load_stream(store, hash, p[1], &sz, NULL);
    close(p[1]);
    pthread_join(th, NULL);
    close(p[0]);

    assert_int_equal(st, OK);
    assert_int_equal(d.got, len);
    assert_memory_equal(d.buf, data, len);
    assert_true(parity_stats_get().repaired > 0);
    free(d.buf);
    free(data);
}

/* The in-memory reader takes the payload whole (it is capped at 16 MiB),
 * but its parity a window at a time: an LZ4 block entry spanning many
 * windows is corrected in its last one. */
static void test_block_entry_corrected_across_windows(void **state)
{
    (void)state;
    size_t len = (size_t)1024 * 1024;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_semicompressible(data, len, 0xACEDu);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    char dat[512]; off_t pay = 0; uint64_t csize = 0; uint8_t codec = 0;
    entry_info(hash, dat, sizeof(dat), &pay, &csize, &codec);
    assert_int_equal(codec, COMPRESS_LZ4);
    assert_true(csize > 8 * WIN_GROUPS * GROUP_DATA);
    flip(dat, pay + (off_t)(csize - 100));

    parity_stats_reset();
    void *out = NULL;
    size_t got = 0;
    assert_int_equal(object_load(store, hash, &out, &got, NULL), OK);
    assert_int_equal(got, len);
    assert_memory_equal(out, data, len);
    free(out);
    assert_true(parity_stats_get().repaired > 0);
    free(data);
}

/*
 * gc copying a damaged entry into a new pack corrects it on the way. The new
 * pack's parity is computed over whatever it holds, so a damaged byte copied
 * as it stood would have been protected from then on as if it were right,
 * with the old pack - the only thing able to fix it - deleted.
 */
static void test_gc_copy_does_not_launder_damage(void **state)
{
    (void)state;
    uint8_t a[8192], b[8192];
    fill_random(a, sizeof(a), 0xAAAA5555u);
    fill_random(b, sizeof(b), 0x5555AAAAu);
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, a, sizeof(a), ha), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, b, sizeof(b), hb), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    char dat[512]; off_t pay = 0; uint64_t csize = 0;
    entry_info(ha, dat, sizeof(dat), &pay, &csize, NULL);
    flip(dat, pay + 777);

    /* Only a is referenced, so gc rewrites the pack without b. */
    parity_stats_reset();
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc(store, ha, 1, &kept, &deleted), OK);
    assert_int_equal(kept, 1);
    assert_int_equal(deleted, 1);
    assert_true(parity_stats_get().repaired > 0);

    /* The copy holds the right bytes, not damage under fresh parity. */
    char dat2[512];
    entry_info(ha, dat2, sizeof(dat2), &pay, &csize, NULL);
    assert_string_not_equal(dat, dat2);
    parity_stats_reset();
    void *out = NULL;
    size_t got = 0;
    assert_int_equal(object_load(store, ha, &out, &got, NULL), OK);
    assert_int_equal(got, sizeof(a));
    assert_memory_equal(out, a, sizeof(a));
    free(out);
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

/* The whole-buffer stored read (tests, inspection) hands over stored bytes;
 * damage parity covers is corrected first, for the same reason as gc's copy. */
static void test_stored_read_corrects_damage(void **state)
{
    (void)state;
    uint8_t a[20000];
    fill_random(a, sizeof(a), 0x13572468u);
    uint8_t ha[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, a, sizeof(a), ha), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    object_stored_info_t info;
    void *clean = NULL;
    size_t clen = 0;
    assert_int_equal(object_read_stored(store, ha, &info, &clean, &clen), OK);

    char dat[512]; off_t pay = 0; uint64_t csize = 0;
    entry_info(ha, dat, sizeof(dat), &pay, &csize, NULL);
    flip(dat, pay + (off_t)(clen - 3));

    parity_stats_reset();
    void *got = NULL;
    size_t glen = 0;
    assert_int_equal(object_read_stored(store, ha, &info, &got, &glen), OK);
    assert_int_equal(glen, clen);
    assert_memory_equal(got, clean, clen);
    assert_true(parity_stats_get().repaired > 0);
    free(got);
    free(clean);
}

/*
 * The streaming stored read replication uses: pieces of an awkward size,
 * which straddle the reader's corrected windows, come back as the entry was
 * written, and the reader holds one window however large the entry is.
 */
static void test_streaming_stored_read_corrects_damage(void **state)
{
    (void)state;
    size_t len = (size_t)3 * 1024 * 1024;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_random(data, len, 0x5743AEDu);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, h), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    char dat[512]; off_t pay = 0; uint64_t csize = 0; uint8_t codec = 0;
    entry_info(h, dat, sizeof(dat), &pay, &csize, &codec);
    assert_int_equal(codec, COMPRESS_NONE);
    flip(dat, pay + 12345);
    flip(dat, pay + (off_t)(len - 777));

    parity_stats_reset();
    fault_malloc_peak = 0;
    object_stored_info_t info;
    uint64_t slen = 0;
    object_stored_reader_t *r = NULL;
    assert_int_equal(object_stored_open(store, h, &info, &slen, &r), OK);
    size_t peak_open = fault_malloc_peak;
    assert_int_equal(slen, len);
    uint8_t *got = malloc(len);   /* the test's own copy, after the peak is read */
    assert_non_null(got);
    fault_malloc_peak = 0;
    for (size_t o = 0; o < len; ) {
        size_t n = len - o < 10007 ? len - o : 10007;
        assert_int_equal(object_stored_read(r, got + o, n), OK);
        o += n;
    }
    size_t peak_read = fault_malloc_peak;
    object_stored_close(r);

    assert_memory_equal(got, data, len);
    assert_true(peak_open <= PEAK_BOUND);
    assert_true(peak_read <= PEAK_BOUND);
    assert_true(parity_stats_get().repaired > 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);
    free(got);
    free(data);
}

/*
 * Damage past 256 MiB into a stored entry. Group and parity offsets there
 * are past anything a 32-bit intermediate could hold once multiplied out;
 * the window is left lowered, so this also crosses some 4,400 windows.
 */
static void test_damage_past_256_mib_is_repaired(void **state)
{
    (void)state;
    size_t len = (size_t)PACK_MAX_MULTI_BYTES + 2 * 1024 * 1024;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_random(data, len, 0x2560FFu);
    uint8_t hash[OBJECT_HASH_SIZE];
    SHA256(data, len, hash);

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                     ENCRYPT_NONE, len, data, len), OK);
    free(data);
    uint32_t packed = 0;
    assert_int_equal(pack_stored_finish(w, &packed), OK);
    assert_int_equal(packed, 1);

    char dat[512]; off_t pay = 0; uint64_t csize = 0;
    entry_info(hash, dat, sizeof(dat), &pay, &csize, NULL);
    off_t deep = (off_t)PACK_MAX_MULTI_BYTES + 1024 * 1024 + 99;
    flip(dat, pay + deep);

    /* /dev/null: verify's sink. The read returns OK only if the hash of what
     * it emitted matches, which is the byte-exact check without holding
     * another copy. */
    int nul = open("/dev/null", O_WRONLY);
    assert_true(nul >= 0);
    parity_stats_reset();
    fault_malloc_peak = 0;
    uint64_t sz = 0;
    assert_int_equal(object_load_stream(store, hash, nul, &sz, NULL), OK);
    print_message("past-256 MiB read: largest allocation %zu bytes; whole parity %zu bytes\n",
                  (size_t)fault_malloc_peak, whole_parity(csize));
    assert_true(fault_malloc_peak <= PEAK_BOUND);
    assert_true(parity_stats_get().repaired > 0);

    assert_int_equal(pack_object_repair(store, hash), 1);
    parity_stats_reset();
    assert_int_equal(object_load_stream(store, hash, nul, &sz, NULL), OK);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    close(nul);
}

/* Every byte of a file, for comparing before and after. */
static uint8_t *slurp(const char *path, size_t *len)
{
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    *len = (size_t)sb.st_size;
    uint8_t *b = malloc(*len);
    assert_non_null(b);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, b, *len, 0), (ssize_t)*len);
    close(fd);
    return b;
}

/*
 * OWN-30: a pack past the size where its 32-bit trailer sizes can wrap
 * (about 59.75 GiB, faked low here) is not corrected from at all. Repair
 * refuses and reports failure, and not one byte of the pack changes; the
 * damaged entry reads as damaged rather than as a confident wrong
 * correction.
 */
static void test_repair_refuses_past_the_parity_fit_limit(void **state)
{
    (void)state;
    uint8_t a[50000];
    fill_random(a, sizeof(a), 0x0B5E55EDu);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, a, sizeof(a), h), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    char dat[512]; off_t pay = 0; uint64_t csize = 0;
    entry_info(h, dat, sizeof(dat), &pay, &csize, NULL);
    flip(dat, pay + 1000);
    flip(dat, 4);   /* the entry header too: its XOR record must not be used */

    size_t blen = 0;
    uint8_t *before = slurp(dat, &blen);
    pack_test_set_parity_fit_limit((uint64_t)blen - 1);

    parity_stats_reset();
    assert_int_equal(pack_object_repair(store, h), -1);
    err_clear();
    void *out = NULL;
    size_t got = 0;
    assert_int_not_equal(object_load(store, h, &out, &got, NULL), OK);
    err_clear();
    assert_int_equal((int)parity_stats_get().repaired, 0);

    size_t alen = 0;
    uint8_t *after = slurp(dat, &alen);
    assert_int_equal(alen, blen);
    assert_memory_equal(after, before, blen);

    /* Below the limit the same pack repairs, so it was the guard that held. */
    pack_test_set_parity_fit_limit(0);
    assert_true(pack_object_repair(store, h) > 0);
    free(before);
    free(after);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_stored_entry_read_and_repair_stay_in_window, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_entry_read_and_repair_stay_in_window, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_entry_streams_into_a_pipe, setup, teardown),
        cmocka_unit_test_setup_teardown(test_block_entry_corrected_across_windows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_copy_does_not_launder_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stored_read_corrects_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_streaming_stored_read_corrects_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damage_past_256_mib_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_refuses_past_the_parity_fit_limit, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
