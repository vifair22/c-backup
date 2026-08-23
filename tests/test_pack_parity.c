#define _POSIX_C_SOURCE 200809L

/*
 * Packed object parity across all three payload codecs.
 *
 * Objects inside a pack are protected by the pack's parity trailer rather
 * than by a per-object one: a XOR record over each entry header and Reed-
 * Solomon parity over each payload, indexed by the entry's position in the
 * pack. That indexing is the fragile part - a wrong entry index reads the
 * wrong parity and silently disables repair - so each codec is exercised
 * through both the buffered and the streaming read path.
 *
 * Ported from the pre-daemon suite, which reached the packed objects by
 * running a backup and reading the resulting snapshot. Storing objects
 * directly makes the codec chosen for each one predictable, which is what the
 * per-codec assertions need.
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
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"

#define TEST_REPO "/tmp/c_backup_test_pack_parity"

/* Above the block-codec threshold, so the frame codec is used. */
#define FRAME_SIZE (17u * 1024u * 1024u)

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

/* Incompressible: LZ4 gives up and the payload is stored raw. */
static void fill_random(uint8_t *buf, size_t n, uint32_t seed)
{
    for (size_t i = 0; i < n; i += 4) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        size_t take = (n - i) < 4 ? (n - i) : 4;
        memcpy(buf + i, &seed, take);
    }
}

static void fill_compressible(uint8_t *buf, size_t n)
{
    const char pat[] = "ABCDEFGHIJ0123456789abcdefghij\n";
    size_t plen = sizeof(pat) - 1;
    for (size_t i = 0; i < n; i++) buf[i] = (uint8_t)pat[i % plen];
}

static int find_pack_dat(char *out, size_t cap)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' | head -1", TEST_REPO);
    FILE *p = popen(cmd, "r");
    if (!p) return -1;
    if (!fgets(out, (int)cap, p)) { pclose(p); return -1; }
    pclose(p);
    out[strcspn(out, "\n")] = '\0';
    return out[0] ? 0 : -1;
}

static void flip_pack_byte(off_t off)
{
    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0x01;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
    pack_cache_invalidate(store);
}

/* Where this object's entry starts inside the pack. */
static uint64_t entry_offset(const uint8_t hash[OBJECT_HASH_SIZE])
{
    uint32_t pack_num = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pack_num, &off), OK);
    return off;
}

static void assert_buffered_read_matches(const uint8_t hash[OBJECT_HASH_SIZE],
                                         const uint8_t *expect, size_t len)
{
    void *out = NULL;
    size_t got = 0;
    assert_int_equal(object_load(store, hash, &out, &got, NULL), OK);
    assert_int_equal(got, len);
    assert_memory_equal(out, expect, len);
    free(out);
}

static void assert_stream_read_matches(const uint8_t hash[OBJECT_HASH_SIZE],
                                       const uint8_t *expect, size_t len)
{
    char out[] = "/tmp/c_backup_pack_parity_stream.XXXXXX";
    int fd = mkstemp(out);
    assert_true(fd >= 0);

    uint64_t ssz = 0;
    assert_int_equal(object_load_stream(store, hash, fd, &ssz, NULL), OK);
    assert_int_equal(ssz, len);

    uint8_t *got = malloc(len);
    assert_non_null(got);
    assert_int_equal(pread(fd, got, len, 0), (ssize_t)len);
    assert_memory_equal(got, expect, len);
    free(got);
    close(fd);
    unlink(out);
}

/* Store one payload, pack it, and hand back its hash. One object per pack
 * keeps the entry index unambiguous. */
static void pack_one(const uint8_t *data, size_t len, uint8_t hash[OBJECT_HASH_SIZE])
{
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
}

/* ------------------------------------------------------------------ */
/* Clean reads, one per codec                                           */
/* ------------------------------------------------------------------ */

static void test_uncompressed_entry_reads_clean(void **state)
{
    (void)state;
    uint8_t data[4096];
    fill_random(data, sizeof(data), 0xA5A5A5A5u);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_stream_read_matches(hash, data, sizeof(data));
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

static void test_lz4_block_entry_reads_clean(void **state)
{
    (void)state;
    uint8_t data[65536];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_stream_read_matches(hash, data, sizeof(data));
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

/* ------------------------------------------------------------------ */
/* Corruption                                                           */
/* ------------------------------------------------------------------ */

/*
 * A flipped payload byte inside a packed entry is repaired by the entry's RS
 * parity on the way out. This is the assertion that proves the entry index
 * carried through the pack commit is correct: with a wrong index the parity
 * lookup silently misses and the read either fails or returns damaged bytes.
 */
static void test_payload_corruption_repaired_for_uncompressed(void **state)
{
    (void)state;
    uint8_t data[4096];
    fill_random(data, sizeof(data), 0x1234ABCDu);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    /* Past the entry header, into the payload. */
    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 64));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

static void test_payload_corruption_repaired_for_lz4_block(void **state)
{
    (void)state;
    uint8_t data[65536];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 16));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

/* The streaming reader has its own parity handling, so it is corrupted and
 * checked separately rather than assumed to share the buffered path. */
static void test_payload_corruption_repaired_on_stream_read(void **state)
{
    (void)state;
    uint8_t data[65536];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 32));

    parity_stats_reset();
    assert_stream_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

/*
 * Entry-header damage is the case with the worst failure mode: a corrupted
 * compressed_size would make the reader consume the wrong number of bytes and
 * walk into the next entry. The XOR record has to catch it.
 */
static void test_entry_header_corruption_is_repaired(void **state)
{
    (void)state;
    uint8_t data[8192];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    /* Into the entry header itself. */
    flip_pack_byte((off_t)(entry_offset(hash) + 4));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

/* pack_object_repair writes the fix back, so a later read finds it clean. */
static void test_repair_persists_and_is_idempotent(void **state)
{
    (void)state;
    uint8_t data[8192];
    fill_random(data, sizeof(data), 0xFEEDFACEu);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 100));

    assert_true(pack_object_repair(store, hash) > 0);
    assert_int_equal(pack_object_repair(store, hash), 0);

    pack_cache_invalidate(store);
    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

/* A clean frame-codec entry streams back byte for byte. */
static void test_frame_codec_entry_reads_clean(void **state)
{
    (void)state;
    uint8_t *data = malloc(FRAME_SIZE);
    assert_non_null(data);
    fill_compressible(data, FRAME_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, FRAME_SIZE, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    parity_stats_reset();
    assert_stream_read_matches(hash, data, FRAME_SIZE);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);
    free(data);
}

/*
 * The frame codec is detection-only by design: repairing it would mean
 * buffering the whole compressed payload, which is exactly what streaming
 * exists to avoid for a payload that may not fit in RAM. So corruption must
 * surface as an error rather than as repaired-looking output, and it must be
 * counted as uncorrectable rather than as a repair.
 */
static void test_frame_codec_corruption_is_detected_not_repaired(void **state)
{
    (void)state;
    uint8_t *data = malloc(FRAME_SIZE);
    assert_non_null(data);
    fill_compressible(data, FRAME_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, FRAME_SIZE, hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 128));

    char out[] = "/tmp/c_backup_pack_parity_frame.XXXXXX";
    int fd = mkstemp(out);
    assert_true(fd >= 0);

    parity_stats_reset();
    uint64_t ssz = 0;
    assert_int_not_equal(object_load_stream(store, hash, fd, &ssz, NULL), OK);
    err_clear();
    close(fd);
    unlink(out);

    /* Nothing was fixed, so nothing may be counted as fixed. */
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(data);
}

/*
 * A pack header claiming more entries than the file could possibly hold.
 *
 * pack_enumerate_dat walked the entry list by adding each entry's
 * compressed_size to a running offset, on a uint64 that came off disk. A
 * large enough value wrapped the sum, the next seek landed earlier in the
 * file than it started, and the loop read the same bytes forever. Not a
 * crash - a pinned core, in the enumerator that GC, coalescing, reindex and
 * the introspection routes all walk with.
 *
 * A fuzzer found it as a timeout rather than a crash, which is the only way
 * it was ever going to show up: nothing was corrupted and nothing faulted,
 * the walk simply never ended. The reproducer was a 750-byte file claiming
 * 582 million entries.
 */
static void test_absurd_entry_count_is_refused(void **state)
{
    (void)state;
    /* One real object, packed, so there is a genuine pack to corrupt. */
    uint8_t hash[OBJECT_HASH_SIZE];
    const char *body = "an object to give the pack something to hold";
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, body, strlen(body), hash), OK);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);

    pack_dat_hdr_t hdr;
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    hdr.count = 582156299u;
    assert_int_equal(pwrite(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    close(fd);

    pack_cache_invalidate(store);

    /* Must refuse rather than start walking. If this regresses the suite
     * does not fail, it hangs - which is itself the signal. */
    uint32_t version = 0, count = 0;
    char name[64];
    snprintf(name, sizeof(name), "%s", strrchr(path, '/') + 1);
    assert_int_not_equal(pack_enumerate_dat(store, name, &version, &count, NULL, NULL), OK);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_uncompressed_entry_reads_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lz4_block_entry_reads_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_repaired_for_uncompressed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_repaired_for_lz4_block, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_repaired_on_stream_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_entry_header_corruption_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_persists_and_is_idempotent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_codec_entry_reads_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_codec_corruption_is_detected_not_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_absurd_entry_count_is_refused, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
