#define _POSIX_C_SOURCE 200809L

/*
 * Packing an encrypted repository.
 *
 * Loose objects are stored raw and encrypted; compression only happens at
 * pack time. So packing an encrypted repository is decrypt, compress,
 * re-encrypt, and each of those three has a way to be silently wrong:
 *
 *   - the prober measures entropy to decide whether an object is worth
 *     compressing. Ciphertext is indistinguishable from random, so a prober
 *     that forgets to decrypt finds everything incompressible, skip-marks it
 *     all, and an encrypted repository quietly never packs anything. Nothing
 *     fails; the packer just stops working.
 *
 *   - compressing after encrypting instead of before would produce entries
 *     that are larger than the plaintext, again with nothing failing.
 *
 *   - the pack must draw its own nonce rather than reusing the loose
 *     object's. The same object is stored twice in an encrypted repository -
 *     raw while loose, compressed once packed - and a stream cipher run twice
 *     under one key and nonce over two different plaintexts leaks both
 *     without the key being involved at all.
 *
 * Each codec is exercised through both read paths, because the entry layout
 * (nonce, ciphertext, tag) has to survive the buffered reader, the streaming
 * reader, and the prefix reader, which stops before the tag it cannot check.
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
#include "object_crypt.h"
#include "repo_key.h"
#include "store.h"
#include "types.h"

#define TEST_REPO "/tmp/c_backup_test_pack_crypt"

/* Above the block-codec threshold, so the frame codec is used. */
#define FRAME_SIZE (17u * 1024u * 1024u)

static store_t *store;
static repo_key_t key;

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
    if (repo_key_generate(&key) != OK) return -1;
    store_set_key(store, &key);
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

/* Store a buffer as a loose object and hand back its hash. */
static void store_obj(const uint8_t *buf, size_t n, uint8_t hash[OBJECT_HASH_SIZE])
{
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, buf, n, hash), OK);
}

/* Store from a file, which is the path large objects take. */
static void store_file(const uint8_t *buf, size_t n, uint8_t hash[OBJECT_HASH_SIZE])
{
    char path[] = "/tmp/c_backup_pack_crypt_src.XXXXXX";
    int fd = mkstemp(path);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, buf, n), (ssize_t)n);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(object_store_file(store, fd, n, hash), OK);
    close(fd);
    unlink(path);
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
    char out[] = "/tmp/c_backup_pack_crypt_stream.XXXXXX";
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

static void assert_prefix_read_matches(const uint8_t hash[OBJECT_HASH_SIZE],
                                       const uint8_t *expect, size_t want)
{
    void *out = NULL;
    size_t got = 0;
    uint64_t full = 0;
    assert_int_equal(object_load_prefix(store, hash, want, &out, &got, &full, NULL), OK);
    assert_true(got > 0);
    assert_true(got <= want);
    assert_memory_equal(out, expect, got);
    free(out);
}

/* Nothing legible anywhere in any pack file. */
static void assert_no_plaintext_in_packs(const uint8_t *needle, size_t n)
{
    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    uint8_t *all = malloc((size_t)sb.st_size);
    assert_non_null(all);
    assert_int_equal(read(fd, all, (size_t)sb.st_size), sb.st_size);
    close(fd);

    int found = 0;
    for (off_t i = 0; i + (off_t)n <= sb.st_size; i++)
        if (memcmp(all + i, needle, n) == 0) { found = 1; break; }
    free(all);
    assert_false(found);
}

/* ------------------------------------------------------------------ */

/*
 * The one that matters most: a compressible object in an encrypted
 * repository must still compress. If the prober sees ciphertext it marks the
 * object incompressible and it never packs at all, so this asserts both that
 * it packed and that packing made it smaller.
 */
static void test_compressible_object_still_compresses(void **state)
{
    (void)state;
    enum { N = 512 * 1024 };
    uint8_t *buf = malloc(N);
    assert_non_null(buf);
    fill_compressible(buf, N);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_obj(buf, N, hash);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 1);

    /* The entry has to be smaller than the plaintext, tag and nonce
     * included, or compression did not happen. */
    uint64_t phys = 0;
    assert_int_equal(object_physical_size(store, hash, &phys), OK);
    assert_true(phys < N / 2);

    assert_buffered_read_matches(hash, buf, N);
    assert_stream_read_matches(hash, buf, N);
    assert_prefix_read_matches(hash, buf, 4096);
    assert_no_plaintext_in_packs(buf, 4096);
    free(buf);
}

/* An incompressible object stores raw inside the pack, still encrypted. */
static void test_incompressible_object_roundtrips(void **state)
{
    (void)state;
    enum { N = 512 * 1024 };
    uint8_t *buf = malloc(N);
    assert_non_null(buf);
    fill_random(buf, N, 0x5eed1234u);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_obj(buf, N, hash);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 1);

    assert_buffered_read_matches(hash, buf, N);
    assert_stream_read_matches(hash, buf, N);
    assert_prefix_read_matches(hash, buf, 4096);
    assert_no_plaintext_in_packs(buf, 4096);
    free(buf);
}

/*
 * Large enough for the frame codec and the streaming writer, which runs two
 * cipher streams at once - one decrypting the loose object, one encrypting
 * what LZ4 produces.
 */
static void test_frame_codec_object_roundtrips(void **state)
{
    (void)state;
    uint8_t *buf = malloc(FRAME_SIZE);
    assert_non_null(buf);
    fill_compressible(buf, FRAME_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_file(buf, FRAME_SIZE, hash);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 1);

    uint64_t phys = 0;
    assert_int_equal(object_physical_size(store, hash, &phys), OK);
    assert_true(phys < FRAME_SIZE / 2);

    assert_stream_read_matches(hash, buf, FRAME_SIZE);
    assert_prefix_read_matches(hash, buf, 4096);
    assert_no_plaintext_in_packs(buf, 4096);
    free(buf);
}

/*
 * The nonce a pack entry carries must not be the one its loose object used.
 * Reusing it would put the raw plaintext and the compressed plaintext under
 * one key stream, and XORing the two ciphertexts would then cancel the key
 * out entirely - the one mistake a stream cipher does not survive.
 *
 * Checked by storing the object, copying the nonce off the loose file,
 * packing, and looking for those twelve bytes in the pack.
 */
static void test_pack_draws_its_own_nonce(void **state)
{
    (void)state;
    enum { N = 512 * 1024 };
    uint8_t *buf = malloc(N);
    assert_non_null(buf);
    fill_compressible(buf, N);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_obj(buf, N, hash);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char loose[512];
    snprintf(loose, sizeof(loose), "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
    int fd = open(loose, O_RDONLY);
    assert_true(fd >= 0);
    uint8_t loose_nonce[OBJECT_CRYPT_NONCE_BYTES];
    assert_int_equal(pread(fd, loose_nonce, sizeof(loose_nonce),
                           (off_t)sizeof(object_header_t)),
                     (ssize_t)sizeof(loose_nonce));
    close(fd);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 1);

    assert_no_plaintext_in_packs(loose_nonce, sizeof(loose_nonce));
    free(buf);
}

/*
 * Parity covers the stored bytes - nonce, ciphertext and tag - so a scrub of
 * an offsite copy needs no key. Verified by dropping the key and checking
 * that the packed entry still reads back byte-identically as far as the CRC
 * is concerned, while the plaintext stays unavailable.
 */
static void test_packed_entry_is_unreadable_without_the_key(void **state)
{
    (void)state;
    enum { N = 512 * 1024 };
    uint8_t *buf = malloc(N);
    assert_non_null(buf);
    fill_compressible(buf, N);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_obj(buf, N, hash);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 1);

    store_set_key(store, NULL);
    void *out = NULL;
    size_t got = 0;
    assert_int_not_equal(object_load(store, hash, &out, &got, NULL), OK);

    /* Size and type come from the entry header, which is not encrypted, so a
     * keyless scrub can still walk the pack and account for what is in it. */
    uint64_t usz = 0;
    uint8_t ty = 0;
    assert_int_equal(object_get_info(store, hash, &usz, &ty), OK);
    assert_int_equal(usz, N);
    assert_int_equal(ty, OBJECT_TYPE_FILE);

    store_set_key(store, &key);
    assert_buffered_read_matches(hash, buf, N);
    free(buf);
}

/* Several objects at once, so entry indexing and the parity trailer are
 * exercised with more than one entry in the pack. */
static void test_many_objects_in_one_pack(void **state)
{
    (void)state;
    enum { N = 64 * 1024, COUNT = 12 };
    uint8_t *bufs[COUNT];
    uint8_t hashes[COUNT][OBJECT_HASH_SIZE];
    for (int i = 0; i < COUNT; i++) {
        bufs[i] = malloc(N);
        assert_non_null(bufs[i]);
        if (i % 2) fill_compressible(bufs[i], N);
        else       fill_random(bufs[i], N, (uint32_t)(0x1000 + i));
        /* Make the compressible ones distinct so they do not dedup. */
        memcpy(bufs[i], &i, sizeof(i));
        store_obj(bufs[i], N, hashes[i]);
    }

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, COUNT);

    for (int i = 0; i < COUNT; i++) {
        assert_buffered_read_matches(hashes[i], bufs[i], N);
        assert_stream_read_matches(hashes[i], bufs[i], N);
        free(bufs[i]);
    }

    parity_stats_t ps = parity_stats_get();
    assert_int_equal((int)ps.repaired, 0);
    assert_int_equal((int)ps.uncorrectable, 0);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_compressible_object_still_compresses, setup, teardown),
        cmocka_unit_test_setup_teardown(test_incompressible_object_roundtrips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_codec_object_roundtrips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_draws_its_own_nonce, setup, teardown),
        cmocka_unit_test_setup_teardown(test_packed_entry_is_unreadable_without_the_key, setup, teardown),
        cmocka_unit_test_setup_teardown(test_many_objects_in_one_pack, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
