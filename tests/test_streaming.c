#define _POSIX_C_SOURCE 200809L

/*
 * Streaming paths for objects larger than memory.
 *
 * Design constraint 2 says a file may exceed RAM, so every large-object path
 * has to be chunked end to end: store from a file descriptor, load back out to
 * a file descriptor, and never materialise the payload. object_load refusing a
 * large object with ERR_TOO_LARGE is a feature, not a limitation, and is
 * asserted here so nobody "fixes" it into a malloc.
 *
 * Constraint 4 is the other half: incompressible payloads must bail out of
 * compression fast and stay bailed out. The prober writes a skip marker into
 * the loose object header so the next pack run does not pay the probe again.
 *
 * These were ported from the pre-daemon suite, which discovered content hashes
 * by loading a snapshot. Storing through the object API directly means the
 * hash is known up front, so the tests no longer drag the whole pipeline in to
 * exercise the storage engine.
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
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"

#define TEST_REPO "/tmp/c_backup_test_streaming"
#define TEST_SRC  "/tmp/c_backup_test_streaming_src"

/* Above PACK_STREAM_THRESHOLD, so the large-object path is taken. */
#define LARGE_SIZE (17u * 1024u * 1024u)
/* Exactly at the threshold: not greater than, so it goes to the small-object
 * worker path while still being large enough to be probed. */
#define BOUNDARY_SIZE (16u * 1024u * 1024u)

static store_t *store;

static void cleanup(void)
{
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
}

static int setup(void **state)
{
    (void)state;
    cleanup();
    if (mkdir(TEST_SRC, 0755) != 0) return -1;
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (store) { store_close(store); store = NULL; }
    cleanup();
    return 0;
}

/* Incompressible: xorshift output has no exploitable redundancy, which is
 * what the LZ4 probe is meant to detect. */
static void create_prng_file(const char *path, size_t size)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    uint32_t seed = 0xDEADBEEFu;
    size_t remaining = size;
    while (remaining > 0) {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        size_t n = remaining < sizeof(seed) ? remaining : sizeof(seed);
        fwrite(&seed, 1, n, f);
        remaining -= n;
    }
    fclose(f);
}

static void create_compressible_file(const char *path, size_t size)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    const char pattern[] = "ABCDEFGHIJ0123456789abcdefghij\n";
    size_t plen = sizeof(pattern) - 1;
    size_t remaining = size;
    while (remaining > 0) {
        size_t n = remaining < plen ? remaining : plen;
        fwrite(pattern, 1, n, f);
        remaining -= n;
    }
    fclose(f);
}

static void store_path(const char *path, uint8_t hash[OBJECT_HASH_SIZE])
{
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(object_store_file(store, fd, (uint64_t)sb.st_size, hash), OK);
    close(fd);
}

static void assert_streams_match(const uint8_t hash[OBJECT_HASH_SIZE],
                                 const char *original, size_t expect_size)
{
    char outpath[] = "/tmp/c_backup_stream_out_XXXXXX";
    int out_fd = mkstemp(outpath);
    assert_true(out_fd >= 0);

    uint64_t ssz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load_stream(store, hash, out_fd, &ssz, &typ), OK);
    close(out_fd);
    assert_int_equal(ssz, expect_size);
    assert_int_equal(typ, OBJECT_TYPE_FILE);

    FILE *a = fopen(original, "rb");
    FILE *b = fopen(outpath, "rb");
    assert_non_null(a);
    assert_non_null(b);
    static char ba[65536], bb[65536];
    size_t remaining = expect_size;
    while (remaining > 0) {
        size_t n = remaining < sizeof(ba) ? remaining : sizeof(ba);
        assert_int_equal(fread(ba, 1, n, a), n);
        assert_int_equal(fread(bb, 1, n, b), n);
        assert_memory_equal(ba, bb, n);
        remaining -= n;
    }
    fclose(a);
    fclose(b);
    unlink(outpath);
}

static void read_header(const uint8_t hash[OBJECT_HASH_SIZE], object_header_t *out)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%.2s/%s", store_root(store), hex, hex + 2);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(read(fd, out, sizeof(*out)), (ssize_t)sizeof(*out));
    close(fd);
}

static int is_loose(const uint8_t hash[OBJECT_HASH_SIZE])
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%.2s/%s", store_root(store), hex, hex + 2);
    struct stat sb;
    return stat(path, &sb) == 0;
}

/* ------------------------------------------------------------------ */
/* Loose large objects                                                  */
/* ------------------------------------------------------------------ */

static void test_large_object_refuses_in_memory_load(void **state)
{
    (void)state;
    create_prng_file(TEST_SRC "/large.bin", LARGE_SIZE);

    int fd = open(TEST_SRC "/large.bin", O_RDONLY);
    assert_true(fd >= 0);
    uint8_t hash[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0;
    assert_int_equal(object_store_file_ex(store, fd, LARGE_SIZE, hash, &is_new, &phys), OK);
    close(fd);
    assert_true(is_new);
    assert_true(phys > 0);

    /* The refusal is deliberate: callers must stream. */
    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(store, hash, &data, &sz, NULL), ERR_TOO_LARGE);
    err_clear();

    assert_streams_match(hash, TEST_SRC "/large.bin", LARGE_SIZE);
}

static void test_large_compressible_object_streams(void **state)
{
    (void)state;
    create_compressible_file(TEST_SRC "/large.txt", LARGE_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/large.txt", hash);
    assert_true(object_exists(store, hash));
    assert_streams_match(hash, TEST_SRC "/large.txt", LARGE_SIZE);
}

static uint64_t g_progress_bytes;
static int      g_progress_calls;

static void progress_cb(uint64_t chunk_bytes, void *ctx)
{
    (void)ctx;
    g_progress_bytes += chunk_bytes;
    g_progress_calls++;
}

/* The callback is what a UI renders a progress bar from, so the byte total it
 * reports has to add up to the file. It fires once per STREAM_CHUNK, which is
 * 128 MiB, so a test file sized to force several calls would cost more disk
 * than the assertion is worth; the total is the part that matters. */
static void test_streaming_store_reports_progress(void **state)
{
    (void)state;
    create_prng_file(TEST_SRC "/progress.bin", LARGE_SIZE);

    int fd = open(TEST_SRC "/progress.bin", O_RDONLY);
    assert_true(fd >= 0);

    g_progress_bytes = 0;
    g_progress_calls = 0;

    uint8_t hash[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0;
    assert_int_equal(object_store_file_cb(store, fd, NULL, LARGE_SIZE, hash,
                                          &is_new, &phys, progress_cb, NULL), OK);
    close(fd);

    assert_true(g_progress_calls >= 1);
    assert_int_equal(g_progress_bytes, LARGE_SIZE);
}

/* Content addressing has to hold at large sizes too: the second store of
 * identical bytes must not write a second copy. */
static void test_large_object_dedups(void **state)
{
    (void)state;
    create_compressible_file(TEST_SRC "/a.txt", LARGE_SIZE);
    create_compressible_file(TEST_SRC "/b.txt", LARGE_SIZE);

    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0;

    int fd = open(TEST_SRC "/a.txt", O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(object_store_file_ex(store, fd, LARGE_SIZE, h1, &is_new, &phys), OK);
    close(fd);
    assert_true(is_new);

    fd = open(TEST_SRC "/b.txt", O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(object_store_file_ex(store, fd, LARGE_SIZE, h2, &is_new, &phys), OK);
    close(fd);
    assert_false(is_new);

    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
}

/* ------------------------------------------------------------------ */
/* Packed large objects                                                 */
/* ------------------------------------------------------------------ */

/* Small compressible payloads take the LZ4 block codec. */
static void test_packed_small_lz4_streams_back(void **state)
{
    (void)state;
    const size_t size = 64u * 1024u;
    create_compressible_file(TEST_SRC "/small.txt", size);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/small.txt", hash);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed > 0);
    assert_false(is_loose(hash));

    assert_streams_match(hash, TEST_SRC "/small.txt", size);
}

/* Large compressible payloads take the LZ4 frame codec, which is a separate
 * branch of the pack read path. */
static void test_packed_large_lz4frame_streams_back(void **state)
{
    (void)state;
    create_compressible_file(TEST_SRC "/big.txt", LARGE_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/big.txt", hash);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    assert_streams_match(hash, TEST_SRC "/big.txt", LARGE_SIZE);
}

static void test_packed_mixed_codecs_in_one_pack(void **state)
{
    (void)state;
    const size_t small = 128u * 1024u;
    create_compressible_file(TEST_SRC "/mix_small.txt", small);
    create_compressible_file(TEST_SRC "/mix_large.txt", LARGE_SIZE);

    uint8_t hs[OBJECT_HASH_SIZE], hl[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/mix_small.txt", hs);
    store_path(TEST_SRC "/mix_large.txt", hl);

    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    assert_streams_match(hs, TEST_SRC "/mix_small.txt", small);
    assert_streams_match(hl, TEST_SRC "/mix_large.txt", LARGE_SIZE);
}

/* The pack cache is rebuilt lazily; invalidating it mid-life must not lose
 * the ability to resolve an already-packed object. */
static void test_pack_cache_survives_invalidation(void **state)
{
    (void)state;
    create_compressible_file(TEST_SRC "/cached.txt", 32u * 1024u);
    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/cached.txt", hash);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    assert_true(pack_object_exists(store, hash));
    pack_cache_invalidate(store);
    assert_true(pack_object_exists(store, hash));
    assert_streams_match(hash, TEST_SRC "/cached.txt", 32u * 1024u);
}

/* ------------------------------------------------------------------ */
/* Incompressible bailout                                               */
/* ------------------------------------------------------------------ */

/*
 * Incompressible large objects are left loose with a skip marker. Packing
 * them would cost a full compression pass to save nothing, and the marker is
 * what stops the next run from paying the probe again.
 */
static void test_incompressible_large_object_gets_skip_marker(void **state)
{
    (void)state;
    create_prng_file(TEST_SRC "/random_large.bin", LARGE_SIZE);
    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/random_large.bin", hash);

    object_header_t hdr;
    read_header(hash, &hdr);
    assert_int_equal(hdr.pack_skip_ver, 0);

    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    assert_true(is_loose(hash));
    read_header(hash, &hdr);
    assert_int_equal(hdr.pack_skip_ver, PROBER_VERSION);

    /* Second run must skip it outright rather than re-probe. */
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 0);
    assert_true(is_loose(hash));
}

/* Exactly at the stream threshold the object goes down the small-object
 * worker path, which carries its own copy of the probe. */
static void test_incompressible_boundary_object_gets_skip_marker(void **state)
{
    (void)state;
    create_prng_file(TEST_SRC "/random_16m.bin", BOUNDARY_SIZE);
    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/random_16m.bin", hash);

    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    assert_true(is_loose(hash));
    object_header_t hdr;
    read_header(hash, &hdr);
    assert_int_equal(hdr.pack_skip_ver, PROBER_VERSION);
}

/* A skipped object is still a readable object. */
static void test_skipped_object_still_loads(void **state)
{
    (void)state;
    create_prng_file(TEST_SRC "/skipped.bin", LARGE_SIZE);
    uint8_t hash[OBJECT_HASH_SIZE];
    store_path(TEST_SRC "/skipped.bin", hash);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);

    assert_streams_match(hash, TEST_SRC "/skipped.bin", LARGE_SIZE);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_large_object_refuses_in_memory_load, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_compressible_object_streams, setup, teardown),
        cmocka_unit_test_setup_teardown(test_streaming_store_reports_progress, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_dedups, setup, teardown),
        cmocka_unit_test_setup_teardown(test_packed_small_lz4_streams_back, setup, teardown),
        cmocka_unit_test_setup_teardown(test_packed_large_lz4frame_streams_back, setup, teardown),
        cmocka_unit_test_setup_teardown(test_packed_mixed_codecs_in_one_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_cache_survives_invalidation, setup, teardown),
        cmocka_unit_test_setup_teardown(test_incompressible_large_object_gets_skip_marker, setup, teardown),
        cmocka_unit_test_setup_teardown(test_incompressible_boundary_object_gets_skip_marker, setup, teardown),
        cmocka_unit_test_setup_teardown(test_skipped_object_still_loads, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
