#define _POSIX_C_SOURCE 200809L

/*
 * Objects in an encrypted repository.
 *
 * Every object is compressed, encrypted and parity-protected in one shot as
 * it is staged into a pack, in that order, and each step has a way to be
 * silently wrong:
 *
 *   - the prober measures entropy to decide whether an object is worth
 *     compressing. Ciphertext is indistinguishable from random, so a prober
 *     that ran after encryption would find everything incompressible and an
 *     encrypted repository would quietly never compress anything. Nothing
 *     fails; it just stores more.
 *
 *   - compressing after encrypting instead of before would likewise produce
 *     entries no smaller than the plaintext, again with nothing failing.
 *
 *   - every encryption must draw its own nonce, and the ciphertext must be
 *     bound to the name and type it is filed under (the AAD), so bytes moved
 *     under another name fail authentication instead of decrypting as
 *     something they are not.
 *
 * Each object is read back through the decoding reader and, where it says
 * something, the stored-form reader, which needs no key.
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
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_pack_crypt"

/* A file of several MiB, well inside the object bound. */
#define LARGE_SIZE (6u * 1024u * 1024u)

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

/* Stage a buffer through the store's own writer and hand back its hash. */
static void store_obj(const uint8_t *buf, size_t n, uint8_t hash[OBJECT_HASH_SIZE])
{
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, buf, n, hash), OK);
}

/* Store from a file, the path a backup's files take. */
static void store_file(const uint8_t *buf, size_t n, uint8_t hash[OBJECT_HASH_SIZE])
{
    char path[] = TEST_TMP "/c_backup_pack_crypt_src.XXXXXX";
    int fd = mkstemp(path);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, buf, n), (ssize_t)n);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(object_store_file_packed(store, fd, n, NULL, hash, NULL), OK);
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

/* The entry as stored: read without the key, checked against its parity. */
static object_stored_info_t stored_info(const uint8_t hash[OBJECT_HASH_SIZE], size_t *out_len,
                                        uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES])
{
    object_stored_info_t info;
    memset(&info, 0, sizeof(info));
    void *payload = NULL;
    size_t len = 0;
    int damaged = -1;
    assert_int_equal(object_read_stored(store, hash, &info, &payload, &len, &damaged), OK);
    assert_int_equal(damaged, 0);
    assert_true(len >= OBJECT_CRYPT_OVERHEAD);
    if (nonce) memcpy(nonce, payload, OBJECT_CRYPT_NONCE_BYTES);
    if (out_len) *out_len = len;
    free(payload);
    return info;
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
 * repository must still compress, so the entry has to come out smaller than
 * the plaintext, nonce and tag included.
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
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal((int)fake_index_count(), 1);

    uint64_t phys = 0;
    assert_int_equal(object_physical_size(store, hash, &phys, NULL), OK);
    assert_true(phys < N / 2);

    size_t slen = 0;
    object_stored_info_t info = stored_info(hash, &slen, NULL);
    assert_int_equal(info.compression, COMPRESS_LZ4);
    assert_int_equal(info.encryption, ENCRYPT_CHACHA20);
    assert_int_equal(info.uncompressed_size, N);

    assert_buffered_read_matches(hash, buf, N);
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
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal((int)fake_index_count(), 1);

    /* Raw, so the stored bytes are the plaintext's length plus nonce and tag. */
    size_t slen = 0;
    object_stored_info_t info = stored_info(hash, &slen, NULL);
    assert_int_equal(info.compression, COMPRESS_NONE);
    assert_int_equal(info.encryption, ENCRYPT_CHACHA20);
    assert_int_equal(slen, (size_t)N + OBJECT_CRYPT_OVERHEAD);

    assert_buffered_read_matches(hash, buf, N);
    assert_no_plaintext_in_packs(buf, 4096);
    free(buf);
}

/* A multi-MiB file stored from its descriptor compresses and encrypts the
 * same way. */
static void test_large_file_roundtrips(void **state)
{
    (void)state;
    uint8_t *buf = malloc(LARGE_SIZE);
    assert_non_null(buf);
    fill_compressible(buf, LARGE_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    store_file(buf, LARGE_SIZE, hash);
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal((int)fake_index_count(), 1);

    uint64_t phys = 0;
    assert_int_equal(object_physical_size(store, hash, &phys, NULL), OK);
    assert_true(phys < LARGE_SIZE / 2);

    assert_buffered_read_matches(hash, buf, LARGE_SIZE);
    assert_no_plaintext_in_packs(buf, 4096);
    free(buf);
}

/*
 * Every encryption draws its own nonce. Two objects under one key and one
 * nonce would put two plaintexts under one key stream, and XORing the
 * ciphertexts would cancel the key out entirely - the one mistake a stream
 * cipher does not survive. Identical plaintexts in two objects of different
 * types are the case a derived nonce would get wrong.
 */
static void test_every_entry_draws_its_own_nonce(void **state)
{
    (void)state;
    enum { N = 4096, COUNT = 16 };
    uint8_t buf[N];
    fill_compressible(buf, N);
    uint8_t hashes[COUNT][OBJECT_HASH_SIZE];
    for (int i = 0; i < COUNT; i++) {
        /* Half share one plaintext, as two types; half are distinct. */
        uint8_t type = (i % 2) ? OBJECT_TYPE_XATTR : OBJECT_TYPE_FILE;
        if (i >= 2) memcpy(buf, &i, sizeof(i));
        assert_int_equal(object_store(store, type, buf, N, hashes[i]), OK);
    }
    assert_int_equal(object_store_flush(store), OK);

    uint8_t nonces[COUNT][OBJECT_CRYPT_NONCE_BYTES];
    for (int i = 0; i < COUNT; i++)
        (void)stored_info(hashes[i], NULL, nonces[i]);
    for (int i = 0; i < COUNT; i++)
        for (int j = i + 1; j < COUNT; j++)
            assert_memory_not_equal(nonces[i], nonces[j], OBJECT_CRYPT_NONCE_BYTES);
}

/*
 * The ciphertext is bound to the name and type it is filed under. The stored
 * bytes of one object, written verbatim under another name - or under its own
 * name with another type - must fail authentication rather than decrypt: the
 * AAD is what stops a damaged or hostile index serving one object's bytes as
 * another's.
 */
static void test_ciphertext_is_bound_to_its_name_and_type(void **state)
{
    (void)state;
    const char *a = "the bytes that belong to object a";
    const char *b = "object b, which a must not impersonate";
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, a, strlen(a), ha), OK);
    object_name(OBJECT_TYPE_FILE, b, strlen(b), hb);
    assert_int_equal(object_store_flush(store), OK);

    object_stored_info_t info;
    memset(&info, 0, sizeof(info));
    void *payload = NULL;
    size_t len = 0;
    assert_int_equal(object_read_stored(store, ha, &info, &payload, &len, NULL), OK);

    /* A's stored bytes, verbatim, under B's name. */
    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, hb, info.type, info.compression, info.encryption,
                                     info.uncompressed_size, payload, len), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);

    void *out = NULL;
    size_t got = 0;
    assert_int_equal(object_load(store, hb, &out, &got, NULL), ERR_CORRUPT);
    assert_null(out);

    /* The same bytes under their own name but another type: rebuild the
     * repository with only that entry, so A's name resolves to it. */
    store_close(store);
    store = NULL;
    cleanup();
    assert_int_equal(store_init_dirs(TEST_REPO), OK);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    store_set_key(store, &key);
    fake_index_reset();
    fake_index_attach(store);
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, ha, OBJECT_TYPE_ACL, info.compression,
                                     info.encryption, info.uncompressed_size,
                                     payload, len), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);
    assert_int_equal(object_load(store, ha, &out, &got, NULL), ERR_CORRUPT);
    assert_null(out);

    /* And filed correctly, the same bytes decrypt: the refusals are about
     * the binding, not the bytes. */
    store_close(store);
    store = NULL;
    cleanup();
    assert_int_equal(store_init_dirs(TEST_REPO), OK);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    store_set_key(store, &key);
    fake_index_reset();
    fake_index_attach(store);
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, ha, info.type, info.compression,
                                     info.encryption, info.uncompressed_size,
                                     payload, len), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);
    assert_buffered_read_matches(ha, (const uint8_t *)a, strlen(a));
    free(payload);
}

/*
 * Parity covers the stored bytes - nonce, ciphertext and tag - so a scrub of
 * an offsite copy needs no key. Verified by dropping the key and checking
 * that the stored entry still reads back clean against its CRC, while the
 * plaintext stays unavailable.
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
    assert_int_equal(object_store_flush(store), OK);

    store_set_key(store, NULL);
    void *out = NULL;
    size_t got = 0;
    assert_int_not_equal(object_load(store, hash, &out, &got, NULL), OK);
    assert_null(out);

    /* The stored form needs no key, and its parity check passes. */
    size_t slen = 0;
    object_stored_info_t info = stored_info(hash, &slen, NULL);
    assert_int_equal(info.encryption, ENCRYPT_CHACHA20);

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

    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal((int)fake_index_count(), COUNT);

    for (int i = 0; i < COUNT; i++) {
        assert_buffered_read_matches(hashes[i], bufs[i], N);
        (void)stored_info(hashes[i], NULL, NULL);
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
        cmocka_unit_test_setup_teardown(test_large_file_roundtrips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_every_entry_draws_its_own_nonce, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ciphertext_is_bound_to_its_name_and_type, setup, teardown),
        cmocka_unit_test_setup_teardown(test_packed_entry_is_unreadable_without_the_key, setup, teardown),
        cmocka_unit_test_setup_teardown(test_many_objects_in_one_pack, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
