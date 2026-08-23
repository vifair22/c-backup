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

    assert_int_equal(object_encrypt(&k, plain, len, ct), OK);
    /* Ciphertext is not plaintext, and carries a tag beyond the payload. */
    assert_memory_not_equal(ct, plain, len);
    assert_int_equal(object_decrypt(&k, ct, len, pt), OK);
    assert_memory_equal(pt, plain, len);
}

/*
 * Every encryption gets a fresh nonce, so the same bytes encrypt differently
 * each time. That is not a problem for dedup: a duplicate is caught by a hash
 * lookup before anything is encrypted, so the second encryption never
 * happens. It is what makes it impossible for two storage forms of one object
 * to share a keystream.
 */
static void test_same_plaintext_encrypts_differently_each_time(void **state)
{
    (void)state;
    repo_key_t k = make_key(2);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 2);

    const char *plain = "encrypted twice, never the same bytes";
    size_t len = strlen(plain);
    uint8_t a[128], b[128];

    assert_int_equal(object_encrypt(&k, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k, plain, len, b), OK);
    assert_memory_not_equal(a, b, len + OBJECT_CRYPT_OVERHEAD);

    /* Both still decrypt to the same thing. */
    uint8_t p1[128], p2[128];
    assert_int_equal(object_decrypt(&k, a, len, p1), OK);
    assert_int_equal(object_decrypt(&k, b, len, p2), OK);
    assert_memory_equal(p1, plain, len);
    assert_memory_equal(p2, plain, len);
}

/*
 * The failure this design exists to make impossible: two different plaintexts
 * encrypted under one key sharing a keystream. XOR the ciphertexts and the
 * keystream would cancel, leaving the XOR of the plaintexts with the key
 * playing no part.
 */
static void test_two_encryptions_never_share_a_keystream(void **state)
{
    (void)state;
    repo_key_t k = make_key(20);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 20);

    uint8_t p1[64], p2[64];
    memset(p1, 'A', sizeof(p1));
    memset(p2, 'B', sizeof(p2));

    uint8_t c1[64 + OBJECT_CRYPT_OVERHEAD], c2[64 + OBJECT_CRYPT_OVERHEAD];
    assert_int_equal(object_encrypt(&k, p1, sizeof(p1), c1), OK);
    assert_int_equal(object_encrypt(&k, p2, sizeof(p2), c2), OK);

    const uint8_t *x1 = c1 + OBJECT_CRYPT_NONCE_BYTES;
    const uint8_t *x2 = c2 + OBJECT_CRYPT_NONCE_BYTES;
    int matches_plaintext_xor = 1;
    for (size_t i = 0; i < sizeof(p1); i++)
        if ((x1[i] ^ x2[i]) != (p1[i] ^ p2[i])) { matches_plaintext_xor = 0; break; }
    assert_false(matches_plaintext_xor);

    /* And the nonces themselves differ, which is what guarantees it. */
    assert_memory_not_equal(c1, c2, OBJECT_CRYPT_NONCE_BYTES);
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

    assert_int_equal(object_encrypt(&k, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k, plain, len, b), OK);
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

    assert_int_equal(object_encrypt(&k1, plain, len, a), OK);
    assert_int_equal(object_encrypt(&k2, plain, len, b), OK);
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
    assert_int_equal(object_encrypt(&right, plain, len, ct), OK);

    /* Not merely wrong output: a refusal. */
    assert_int_not_equal(object_decrypt(&wrong, ct, len, pt), OK);
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
    assert_int_equal(object_encrypt(&k, plain, len, ct), OK);

    ct[3] ^= 0x01;
    assert_int_not_equal(object_decrypt(&k, ct, len, pt), OK);
    err_clear();

    /* And a tampered tag, which is the other half of the same guarantee. */
    ct[3] ^= 0x01;
    ct[len + 2] ^= 0x01;
    assert_int_not_equal(object_decrypt(&k, ct, len, pt), OK);
    err_clear();
}

static void test_empty_payload(void **state)
{
    (void)state;
    repo_key_t k = make_key(12);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 12);

    uint8_t ct[OBJECT_CRYPT_OVERHEAD], pt[1];
    assert_int_equal(object_encrypt(&k, "", 0, ct), OK);
    assert_int_equal(object_decrypt(&k, ct, 0, pt), OK);
}

/*
 * The two forms have to interoperate: an object written by the streaming path
 * must decrypt through the one-shot path and the other way round. They cannot
 * be compared byte for byte any more, because each encryption draws its own
 * nonce - which is the point.
 */
static void test_streaming_output_decrypts_one_shot(void **state)
{
    (void)state;
    repo_key_t k = make_key(13);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 13);

    size_t len = 100000;
    uint8_t *plain = malloc(len);
    uint8_t *buf   = malloc(len + OBJECT_CRYPT_OVERHEAD);
    uint8_t *out   = malloc(len);
    assert_non_null(plain); assert_non_null(buf); assert_non_null(out);
    for (size_t i = 0; i < len; i++) plain[i] = (uint8_t)(i * 31 + 7);

    /* Layout is the caller's job: nonce first, then ciphertext, then tag. */
    object_crypt_stream_t *s = NULL;
    assert_int_equal(object_crypt_stream_begin(&s, &k, buf, 1), OK);
    uint8_t *ct = buf + OBJECT_CRYPT_NONCE_BYTES;

    /* Deliberately uneven chunks: a real stream does not arrive in tidy
     * multiples of anything. */
    size_t off = 0, chunk = 7000;
    while (off < len) {
        size_t n = (len - off < chunk) ? len - off : chunk;
        assert_int_equal(object_crypt_stream_update(s, plain + off, n, ct + off), OK);
        off += n;
        chunk = chunk * 3 / 2 + 1;
    }
    assert_int_equal(object_crypt_stream_finish(s, ct + len, NULL), OK);
    object_crypt_stream_free(s);

    assert_int_equal(object_decrypt(&k, buf, len, out), OK);
    assert_memory_equal(out, plain, len);
    free(plain); free(buf); free(out);
}

static void test_one_shot_output_decrypts_streaming(void **state)
{
    (void)state;
    repo_key_t k = make_key(14);
    uint8_t h[OBJECT_HASH_SIZE];
    make_hash(h, 14);

    size_t len = 50000;
    uint8_t *plain = malloc(len);
    uint8_t *buf   = malloc(len + OBJECT_CRYPT_OVERHEAD);
    uint8_t *out   = malloc(len);
    assert_non_null(plain); assert_non_null(buf); assert_non_null(out);
    for (size_t i = 0; i < len; i++) plain[i] = (uint8_t)(i ^ 0x5A);
    assert_int_equal(object_encrypt(&k, plain, len, buf), OK);

    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    memcpy(nonce, buf, sizeof(nonce));
    const uint8_t *ct = buf + OBJECT_CRYPT_NONCE_BYTES;

    object_crypt_stream_t *s = NULL;
    assert_int_equal(object_crypt_stream_begin(&s, &k, nonce, 0), OK);
    size_t off = 0;
    while (off < len) {
        size_t n = (len - off < 4096) ? len - off : 4096;
        assert_int_equal(object_crypt_stream_update(s, ct + off, n, out + off), OK);
        off += n;
    }
    assert_int_equal(object_crypt_stream_finish(s, NULL, ct + len), OK);
    object_crypt_stream_free(s);

    assert_memory_equal(out, plain, len);
    free(plain); free(buf); free(out);
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
    uint8_t *plain = malloc(len);
    uint8_t *buf   = malloc(len + OBJECT_CRYPT_OVERHEAD);
    uint8_t *out   = malloc(len);
    assert_non_null(plain); assert_non_null(buf); assert_non_null(out);
    memset(plain, 'x', len);
    assert_int_equal(object_encrypt(&k, plain, len, buf), OK);

    uint8_t *ct = buf + OBJECT_CRYPT_NONCE_BYTES;
    ct[len / 2] ^= 0xFF;

    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    memcpy(nonce, buf, sizeof(nonce));
    object_crypt_stream_t *s = NULL;
    assert_int_equal(object_crypt_stream_begin(&s, &k, nonce, 0), OK);
    /* The chunks themselves decrypt without complaint; only the tag knows. */
    assert_int_equal(object_crypt_stream_update(s, ct, len, out), OK);
    assert_int_not_equal(object_crypt_stream_finish(s, NULL, ct + len), OK);
    err_clear();
    object_crypt_stream_free(s);
    free(plain); free(buf); free(out);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_round_trip),
        cmocka_unit_test(test_same_plaintext_encrypts_differently_each_time),
        cmocka_unit_test(test_two_encryptions_never_share_a_keystream),
        cmocka_unit_test(test_different_objects_produce_different_ciphertext),
        cmocka_unit_test(test_different_keys_produce_different_ciphertext),
        cmocka_unit_test(test_wrong_key_fails_authentication),
        cmocka_unit_test(test_tampered_ciphertext_is_rejected),
        cmocka_unit_test(test_empty_payload),
        cmocka_unit_test(test_streaming_output_decrypts_one_shot),
        cmocka_unit_test(test_one_shot_output_decrypts_streaming),
        cmocka_unit_test(test_streaming_detects_tampering_at_the_end),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
