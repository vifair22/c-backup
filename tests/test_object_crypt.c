#define _POSIX_C_SOURCE 200809L

/*
 * Object payload encryption (repository format 3).
 *
 * One-shot ChaCha20-Poly1305 per object, under a key derived from the
 * repository key with HKDF, with the object's type and name as associated
 * data. The properties pinned here:
 *
 *   - a round trip works and every encryption takes a fresh nonce;
 *   - the wrong key, a flipped bit, or the right ciphertext under another
 *     name or type fails authentication: ciphertext is bound to the object
 *     it is filed under, so bytes moved between objects are refused where
 *     they are read rather than decrypted into something else;
 *   - the object key is not the repository key.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <openssl/evp.h>

#include "object.h"
#include "object_crypt.h"
#include "pack.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO  TEST_TMP "/c_backup_test_object_crypt"
#define TEST_REPO2 TEST_TMP "/c_backup_test_object_crypt2"

static repo_key_t make_key(uint8_t seed)
{
    repo_key_t k;
    memset(&k, 0, sizeof(k));
    for (int i = 0; i < REPO_KEY_BYTES; i++) k.key[i] = (uint8_t)(seed + i);
    repo_key_compute_id(&k);
    return k;
}

static object_aead_key_t make_aead(uint8_t seed)
{
    repo_key_t k = make_key(seed);
    object_aead_key_t a;
    assert_int_equal(object_aead_key_derive(&k, &a), OK);
    return a;
}

static void make_hash(uint8_t out[OBJECT_HASH_SIZE], uint8_t seed)
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) out[i] = (uint8_t)(seed * 7 + i);
}

static const char *PLAIN = "the quick brown fox jumps over the lazy dog";

static void test_round_trip(void **state)
{
    (void)state;
    object_aead_key_t k = make_aead(1);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 1);
    size_t len = strlen(PLAIN);
    uint8_t ct[128], pt[128];

    assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h, PLAIN, len, ct), OK);
    assert_memory_not_equal(ct + OBJECT_CRYPT_NONCE_BYTES, PLAIN, len);
    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h, ct, len, pt), OK);
    assert_memory_equal(pt, PLAIN, len);

    /* In place, as the pack reader decrypts. */
    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h, ct, len, ct), OK);
    assert_memory_equal(ct, PLAIN, len);
}

static void test_empty_payload(void **state)
{
    (void)state;
    object_aead_key_t k = make_aead(2);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 2);
    uint8_t ct[OBJECT_CRYPT_OVERHEAD], pt[1];
    assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h, "", 0, ct), OK);
    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h, ct, 0, pt), OK);
    /* Even an empty payload is authenticated: a forged tag fails. */
    ct[OBJECT_CRYPT_OVERHEAD - 1] ^= 1;
    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h, ct, 0, pt), ERR_CORRUPT);
}

/* A fresh random nonce per encryption: the same bytes never encrypt the
 * same way twice. Dedup does not need determinism, because a duplicate is
 * caught by its name before anything is encrypted. */
static void test_fresh_nonce_every_time(void **state)
{
    (void)state;
    object_aead_key_t k = make_aead(3);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 3);
    size_t len = strlen(PLAIN);
    uint8_t a[128], b[128];
    assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h, PLAIN, len, a), OK);
    assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h, PLAIN, len, b), OK);
    assert_memory_not_equal(a, b, OBJECT_CRYPT_NONCE_BYTES);
    assert_memory_not_equal(a, b, len + OBJECT_CRYPT_OVERHEAD);
}

static void test_wrong_key_fails(void **state)
{
    (void)state;
    object_aead_key_t k1 = make_aead(4), k2 = make_aead(5);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 4);
    size_t len = strlen(PLAIN);
    uint8_t ct[128], pt[128];
    assert_int_equal(object_encrypt(&k1, OBJECT_TYPE_FILE, h, PLAIN, len, ct), OK);
    assert_int_equal(object_decrypt(&k2, OBJECT_TYPE_FILE, h, ct, len, pt), ERR_CORRUPT);
    err_clear();
}

/* The associated data is type || name: the same ciphertext presented as
 * another object, or as the same name under another type, does not
 * authenticate. */
static void test_ciphertext_is_bound_to_name_and_type(void **state)
{
    (void)state;
    object_aead_key_t k = make_aead(6);
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    make_hash(h1, 6);
    make_hash(h2, 7);
    size_t len = strlen(PLAIN);
    uint8_t ct[128], pt[128];
    assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h1, PLAIN, len, ct), OK);

    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h2, ct, len, pt), ERR_CORRUPT);
    err_clear();
    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_TREE, h1, ct, len, pt), ERR_CORRUPT);
    err_clear();
    assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h1, ct, len, pt), OK);
}

static void test_tampering_fails(void **state)
{
    (void)state;
    object_aead_key_t k = make_aead(8);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 8);
    size_t len = strlen(PLAIN);
    uint8_t ct[128], pt[128];
    size_t total = len + OBJECT_CRYPT_OVERHEAD;
    for (size_t at = 0; at < total; at += 7) {
        assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h, PLAIN, len, ct), OK);
        ct[at] ^= 0x40;
        assert_int_equal(object_decrypt(&k, OBJECT_TYPE_FILE, h, ct, len, pt), ERR_CORRUPT);
        err_clear();
    }
}

/*
 * The object key is derived, not the repository key itself: the chunker
 * derives its gear table from the same key under its own label, and the two
 * must not share key material. The derivation is deterministic (the same
 * repository key always reads its objects) and distinct per key.
 */
static void test_object_key_is_derived(void **state)
{
    (void)state;
    repo_key_t rk = make_key(9);
    object_aead_key_t a, b, c;
    assert_int_equal(object_aead_key_derive(&rk, &a), OK);
    assert_int_equal(object_aead_key_derive(&rk, &b), OK);
    assert_memory_equal(a.key, b.key, OBJECT_AEAD_KEY_BYTES);
    assert_memory_not_equal(a.key, rk.key, OBJECT_AEAD_KEY_BYTES);
    repo_key_t other = make_key(10);
    assert_int_equal(object_aead_key_derive(&other, &c), OK);
    assert_memory_not_equal(a.key, c.key, OBJECT_AEAD_KEY_BYTES);

    /* Ciphertext made under the raw repository key with the same AAD does
     * not authenticate under the object key. */
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 9);
    object_aead_key_t raw;
    memcpy(raw.key, rk.key, OBJECT_AEAD_KEY_BYTES);
    size_t len = strlen(PLAIN);
    uint8_t ct[128], pt[128];
    assert_int_equal(object_encrypt(&raw, OBJECT_TYPE_FILE, h, PLAIN, len, ct), OK);
    assert_int_equal(object_decrypt(&a, OBJECT_TYPE_FILE, h, ct, len, pt), ERR_CORRUPT);
    err_clear();
    object_aead_key_wipe(&a);
    uint8_t zero[OBJECT_AEAD_KEY_BYTES] = { 0 };
    assert_memory_equal(a.key, zero, OBJECT_AEAD_KEY_BYTES);
}

static void test_bound_is_enforced(void **state)
{
    (void)state;
    object_aead_key_t k = make_aead(11);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 11);
    uint8_t one = 0;
    /* The length is checked before anything is touched. */
    assert_int_equal(object_encrypt(&k, OBJECT_TYPE_FILE, h, &one,
                                    (size_t)OBJECT_MAX_STORED_BYTES + 1, &one), ERR_TOO_LARGE);
    err_clear();
}

/* ---------- through the store ---------- */

static store_t *open_keyed(const char *path, const repo_key_t *k)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    int rc = system(cmd); (void)rc;
    assert_int_equal(store_init_dirs(path), OK);
    store_t *s = NULL;
    assert_int_equal(store_open(path, NULL, &s), OK);
    store_set_encrypted(s, 1);
    store_set_key(s, k);
    return s;
}

/*
 * The attack AAD exists for, at the repository level: an attacker with
 * write access to the packs (and no key) files one object's ciphertext
 * under another object's name, or under the same name with another type.
 * The stored writer copies bytes verbatim and gives them fresh parity, so
 * nothing below the AEAD notices; the AEAD refuses both.
 */
static void test_swapped_ciphertext_fails_to_load(void **state)
{
    (void)state;
    repo_key_t k = make_key(12);
    store_t *s1 = open_keyed(TEST_REPO, &k);

    const char *a = "alpha payload, same length as beta";
    const char *b = "beta payload, same length as alpha";
    assert_int_equal(strlen(a), strlen(b));
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(s1, OBJECT_TYPE_FILE, a, strlen(a), ha), OK);
    assert_int_equal(object_store(s1, OBJECT_TYPE_FILE, b, strlen(b), hb), OK);

    object_stored_info_t ib = { 0 };
    void *pb = NULL;
    size_t lb = 0;
    int damaged = 1;
    assert_int_equal(object_read_stored(s1, hb, &ib, &pb, &lb, &damaged), OK);
    assert_int_equal(damaged, 0);
    assert_int_equal(ib.encryption, ENCRYPT_CHACHA20);
    assert_int_equal(lb, strlen(b) + OBJECT_CRYPT_OVERHEAD);

    /* The honest copy loads in a second repository under the same key. */
    store_t *s2 = open_keyed(TEST_REPO2, &k);
    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(s2, &w), OK);
    assert_int_equal(pack_stored_add(w, hb, ib.type, ib.compression, ib.encryption,
                                     ib.uncompressed_size, pb, lb), OK);
    /* b's ciphertext filed as a. */
    assert_int_equal(pack_stored_add(w, ha, ib.type, ib.compression, ib.encryption,
                                     ib.uncompressed_size, pb, lb), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);

    void *out = NULL;
    size_t n = 0;
    assert_int_equal(object_load(s2, hb, &out, &n, NULL), OK);
    assert_memory_equal(out, b, n);
    free(out);
    out = NULL;
    assert_int_equal(object_load(s2, ha, &out, &n, NULL), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "authentication failed"));
    err_clear();
    assert_null(out);
    store_close(s2);

    /* b's own name, its ciphertext, but filed as another type. */
    s2 = open_keyed(TEST_REPO2, &k);
    assert_int_equal(pack_stored_open(s2, &w), OK);
    assert_int_equal(pack_stored_add(w, hb, OBJECT_TYPE_ACL, ib.compression, ib.encryption,
                                     ib.uncompressed_size, pb, lb), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);
    assert_int_equal(object_load(s2, hb, &out, &n, NULL), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "authentication failed"));
    err_clear();
    store_close(s2);

    free(pb);
    store_close(s1);
    int rc = system("rm -rf " TEST_REPO " " TEST_REPO2); (void)rc;
}

/* What lands in an encrypted repository's pack is ciphertext. */
static void test_store_writes_no_plaintext(void **state)
{
    (void)state;
    repo_key_t k = make_key(13);
    store_t *s = open_keyed(TEST_REPO, &k);
    const char *secret = "this sentence must not appear in any pack file";
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(s, OBJECT_TYPE_FILE, secret, strlen(secret), h), OK);
    store_close(s);
    int found = system("grep -rqF 'must not appear in any pack' " TEST_REPO "/packs");
    assert_int_not_equal(found, 0);
    int rc = system("rm -rf " TEST_REPO); (void)rc;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_round_trip),
        cmocka_unit_test(test_empty_payload),
        cmocka_unit_test(test_fresh_nonce_every_time),
        cmocka_unit_test(test_wrong_key_fails),
        cmocka_unit_test(test_ciphertext_is_bound_to_name_and_type),
        cmocka_unit_test(test_tampering_fails),
        cmocka_unit_test(test_object_key_is_derived),
        cmocka_unit_test(test_bound_is_enforced),
        cmocka_unit_test(test_swapped_ciphertext_fails_to_load),
        cmocka_unit_test(test_store_writes_no_plaintext),
    };
    return cmocka_run_group_tests_name("object_crypt", tests, NULL, NULL);
}
