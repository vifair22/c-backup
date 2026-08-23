#define _POSIX_C_SOURCE 200809L

/*
 * Object payload encryption.
 *
 * Two properties are load-bearing and pull against each other, so both are
 * pinned here: identical plaintext must encrypt identically, or dedup stops
 * working and an encrypted repository grows without bound; and the ciphertext
 * must be bound to the object it belongs to, or a payload moved between
 * objects decrypts into something that only fails later, further away, with a
 * worse message.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "object_crypt.h"
#include "types.h"

static repo_key_t make_key(uint8_t seed)
{
    repo_key_t k;
    memset(&k, 0, sizeof(k));
    for (int i = 0; i < REPO_KEY_BYTES; i++) k.key[i] = (uint8_t)(seed + i);
    repo_key_compute_id(&k);
    return k;
}

static void make_hash(uint8_t out[OBJECT_HASH_SIZE], uint8_t seed)
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) out[i] = (uint8_t)(seed * 7 + i);
}

static void test_round_trip(void **state)
{
    (void)state;
    repo_key_t k = make_key(1);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 1);

    const char *plain = "the quick brown fox jumps over the lazy dog";
    size_t len = strlen(plain);
    uint8_t ct[128], pt[128];

    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, ct), OK);
    /* Ciphertext is not plaintext, and carries a tag beyond the payload. */
    assert_memory_not_equal(ct, plain, len);
    assert_int_equal(object_decrypt(&k, h, OBJECT_FORM_LOOSE, ct, len, pt), OK);
    assert_memory_equal(pt, plain, len);
}

/*
 * The property dedup rests on. Encrypting the same bytes twice must give the
 * same ciphertext, or every backup stores a fresh copy of every unchanged
 * file and the repository grows without limit while nothing appears wrong.
 */
static void test_same_plaintext_encrypts_identically(void **state)
{
    (void)state;
    repo_key_t k = make_key(2);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 2);

    const char *plain = "dedup depends on this being stable";
    size_t len = strlen(plain);
    uint8_t a[128], b[128];

    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, b), OK);
    assert_memory_equal(a, b, len + OBJECT_CRYPT_TAG_BYTES);
}

/* Different content means a different hash means a different nonce. Two
 * plaintexts sharing a nonce under one key is the failure mode this scheme
 * has to avoid, so it is worth demonstrating rather than asserting. */
static void test_different_objects_produce_different_ciphertext(void **state)
{
    (void)state;
    repo_key_t k = make_key(3);
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    make_hash(h1, 3);
    make_hash(h2, 4);

    const char *plain = "identical bytes, different objects";
    size_t len = strlen(plain);
    uint8_t a[128], b[128];

    assert_int_equal(object_encrypt(&k, h1, OBJECT_FORM_LOOSE, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k, h2, OBJECT_FORM_LOOSE, plain, len, b), OK);
    assert_memory_not_equal(a, b, len);
}

static void test_different_keys_produce_different_ciphertext(void **state)
{
    (void)state;
    repo_key_t k1 = make_key(5), k2 = make_key(6);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 5);

    const char *plain = "one repository should not read another";
    size_t len = strlen(plain);
    uint8_t a[128], b[128];

    assert_int_equal(object_encrypt(&k1, h, OBJECT_FORM_LOOSE, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k2, h, OBJECT_FORM_LOOSE, plain, len, b), OK);
    assert_memory_not_equal(a, b, len);
}

static void test_wrong_key_fails_authentication(void **state)
{
    (void)state;
    repo_key_t right = make_key(7), wrong = make_key(8);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 7);

    const char *plain = "only the right key reads this";
    size_t len = strlen(plain);
    uint8_t ct[128], pt[128];
    assert_int_equal(object_encrypt(&right, h, OBJECT_FORM_LOOSE, plain, len, ct), OK);

    /* Not merely wrong output: a refusal. */
    assert_int_not_equal(object_decrypt(&wrong, h, OBJECT_FORM_LOOSE, ct, len, pt), OK);
    err_clear();
}

/*
 * The hash is associated data, so a payload lifted into another object's file
 * fails here rather than decrypting to garbage that fails a hash check
 * somewhere else with a less useful explanation.
 */
static void test_payload_moved_to_another_object_is_rejected(void **state)
{
    (void)state;
    repo_key_t k = make_key(9);
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    make_hash(h1, 9);
    make_hash(h2, 10);

    const char *plain = "filed under one name";
    size_t len = strlen(plain);
    uint8_t ct[128], pt[128];
    assert_int_equal(object_encrypt(&k, h1, OBJECT_FORM_LOOSE, plain, len, ct), OK);

    assert_int_not_equal(object_decrypt(&k, h2, OBJECT_FORM_LOOSE, ct, len, pt), OK);
    err_clear();
}

static void test_tampered_ciphertext_is_rejected(void **state)
{
    (void)state;
    repo_key_t k = make_key(11);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 11);

    const char *plain = "authenticated, not merely encrypted";
    size_t len = strlen(plain);
    uint8_t ct[128], pt[128];
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, ct), OK);

    ct[3] ^= 0x01;
    assert_int_not_equal(object_decrypt(&k, h, OBJECT_FORM_LOOSE, ct, len, pt), OK);
    err_clear();

    /* And a tampered tag, which is the other half of the same guarantee. */
    ct[3] ^= 0x01;
    ct[len + 2] ^= 0x01;
    assert_int_not_equal(object_decrypt(&k, h, OBJECT_FORM_LOOSE, ct, len, pt), OK);
    err_clear();
}

static void test_empty_payload(void **state)
{
    (void)state;
    repo_key_t k = make_key(12);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 12);

    uint8_t ct[OBJECT_CRYPT_TAG_BYTES], pt[1];
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, "", 0, ct), OK);
    assert_int_equal(object_decrypt(&k, h, OBJECT_FORM_LOOSE, ct, 0, pt), OK);
}

/*
 * The streaming form has to agree with the one-shot form byte for byte, or an
 * object written by one path and read by the other - which is exactly what
 * happens when a file crosses the size threshold - would fail to decrypt.
 */
static void test_streaming_matches_one_shot(void **state)
{
    (void)state;
    repo_key_t k = make_key(13);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 13);

    size_t len = 100000;
    uint8_t *plain = malloc(len);
    assert_non_null(plain);
    for (size_t i = 0; i < len; i++) plain[i] = (uint8_t)(i * 31 + 7);

    uint8_t *one = malloc(len + OBJECT_CRYPT_TAG_BYTES);
    uint8_t *str = malloc(len + OBJECT_CRYPT_TAG_BYTES);
    assert_non_null(one);
    assert_non_null(str);
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, one), OK);

    /* Deliberately uneven chunks: a real stream does not arrive in tidy
     * multiples of anything. */
    object_crypt_stream_t *s = NULL;
    assert_int_equal(object_crypt_stream_begin(&s, &k, h, OBJECT_FORM_LOOSE, 1), OK);
    size_t off = 0, chunk = 7000;
    while (off < len) {
        size_t n = (len - off < chunk) ? len - off : chunk;
        assert_int_equal(object_crypt_stream_update(s, plain + off, n, str + off), OK);
        off += n;
        chunk = chunk * 3 / 2 + 1;
    }
    assert_int_equal(object_crypt_stream_finish(s, str + len, NULL), OK);
    object_crypt_stream_free(s);

    assert_memory_equal(one, str, len + OBJECT_CRYPT_TAG_BYTES);
    free(plain); free(one); free(str);
}

static void test_streaming_decrypt_round_trip(void **state)
{
    (void)state;
    repo_key_t k = make_key(14);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 14);

    size_t len = 50000;
    uint8_t *plain = malloc(len), *ct = malloc(len + OBJECT_CRYPT_TAG_BYTES);
    uint8_t *out = malloc(len);
    assert_non_null(plain); assert_non_null(ct); assert_non_null(out);
    for (size_t i = 0; i < len; i++) plain[i] = (uint8_t)(i ^ 0x5A);
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, ct), OK);

    object_crypt_stream_t *s = NULL;
    assert_int_equal(object_crypt_stream_begin(&s, &k, h, OBJECT_FORM_LOOSE, 0), OK);
    size_t off = 0;
    while (off < len) {
        size_t n = (len - off < 4096) ? len - off : 4096;
        assert_int_equal(object_crypt_stream_update(s, ct + off, n, out + off), OK);
        off += n;
    }
    assert_int_equal(object_crypt_stream_finish(s, NULL, ct + len), OK);
    object_crypt_stream_free(s);

    assert_memory_equal(out, plain, len);
    free(plain); free(ct); free(out);
}

/* A streamed read only learns the payload is authentic at the very end, so
 * the failure has to arrive there rather than being silently skipped. */
static void test_streaming_detects_tampering_at_the_end(void **state)
{
    (void)state;
    repo_key_t k = make_key(15);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 15);

    size_t len = 20000;
    uint8_t *plain = malloc(len), *ct = malloc(len + OBJECT_CRYPT_TAG_BYTES);
    uint8_t *out = malloc(len);
    assert_non_null(plain); assert_non_null(ct); assert_non_null(out);
    memset(plain, 'x', len);
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, ct), OK);
    ct[len / 2] ^= 0xFF;

    object_crypt_stream_t *s = NULL;
    assert_int_equal(object_crypt_stream_begin(&s, &k, h, OBJECT_FORM_LOOSE, 0), OK);
    /* The chunks themselves decrypt without complaint; only the tag knows. */
    assert_int_equal(object_crypt_stream_update(s, ct, len, out), OK);
    assert_int_not_equal(object_crypt_stream_finish(s, NULL, ct + len), OK);
    err_clear();
    object_crypt_stream_free(s);
    free(plain); free(ct); free(out);
}

/*
 * The reason the form exists.
 *
 * An object is stored twice - raw while loose, compressed once packed - under
 * one content hash. Without the form in the derivation both would use the
 * same nonce, and for a stream cipher that is the one fatal mistake: XOR the
 * two ciphertexts and the keystream cancels out, leaving the XOR of the two
 * plaintexts with the key playing no part. This asserts the two forms produce
 * unrelated keystreams, which is what makes that impossible.
 */
static void test_storage_forms_do_not_share_a_keystream(void **state)
{
    (void)state;
    repo_key_t k = make_key(20);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 20);

    /* Two different plaintexts, as loose and packed really are: the second
     * stands in for the compressed form of the first. */
    uint8_t p1[64], p2[64];
    memset(p1, 'A', sizeof(p1));
    memset(p2, 'B', sizeof(p2));

    uint8_t c1[64 + OBJECT_CRYPT_TAG_BYTES], c2[64 + OBJECT_CRYPT_TAG_BYTES];
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE,  p1, sizeof(p1), c1), OK);
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_PACKED, p2, sizeof(p2), c2), OK);

    /*
     * Had they shared a keystream, c1 XOR c2 would equal p1 XOR p2 exactly -
     * here a constant 0x03 for every byte. Assert it does not.
     */
    int matches_plaintext_xor = 1;
    for (size_t i = 0; i < sizeof(p1); i++)
        if ((c1[i] ^ c2[i]) != (p1[i] ^ p2[i])) { matches_plaintext_xor = 0; break; }
    assert_false(matches_plaintext_xor);
}

/* And a payload encrypted for one form must not decrypt as the other. */
static void test_form_is_bound_into_the_ciphertext(void **state)
{
    (void)state;
    repo_key_t k = make_key(21);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 21);

    const char *plain = "stored loose";
    size_t len = strlen(plain);
    uint8_t ct[64], pt[64];
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_LOOSE, plain, len, ct), OK);
    assert_int_not_equal(object_decrypt(&k, h, OBJECT_FORM_PACKED, ct, len, pt), OK);
    err_clear();
}

/* Dedup still has to work within a form. */
static void test_dedup_holds_within_each_form(void **state)
{
    (void)state;
    repo_key_t k = make_key(22);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 22);

    const char *plain = "same bytes, same form, same ciphertext";
    size_t len = strlen(plain);
    uint8_t a[128], b[128];
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_PACKED, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k, h, OBJECT_FORM_PACKED, plain, len, b), OK);
    assert_memory_equal(a, b, len + OBJECT_CRYPT_TAG_BYTES);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_round_trip),
        cmocka_unit_test(test_same_plaintext_encrypts_identically),
        cmocka_unit_test(test_different_objects_produce_different_ciphertext),
        cmocka_unit_test(test_different_keys_produce_different_ciphertext),
        cmocka_unit_test(test_wrong_key_fails_authentication),
        cmocka_unit_test(test_payload_moved_to_another_object_is_rejected),
        cmocka_unit_test(test_tampered_ciphertext_is_rejected),
        cmocka_unit_test(test_empty_payload),
        cmocka_unit_test(test_streaming_matches_one_shot),
        cmocka_unit_test(test_streaming_decrypt_round_trip),
        cmocka_unit_test(test_streaming_detects_tampering_at_the_end),
        cmocka_unit_test(test_storage_forms_do_not_share_a_keystream),
        cmocka_unit_test(test_form_is_bound_into_the_ciphertext),
        cmocka_unit_test(test_dedup_holds_within_each_form),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
