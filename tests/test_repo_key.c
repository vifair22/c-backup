#define _POSIX_C_SOURCE 200809L

/*
 * Repository keys.
 *
 * The property that matters most here is not secrecy but recoverability: a
 * key that cannot be reproduced turns every copy of a backup into noise, and
 * a backup tool that can lose the ability to read its own repositories has
 * failed at the only thing it does.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "repo_key.h"

#define TEST_DIR "/tmp/c_backup_test_repokey"
#define TEST_UUID "3f2504e0-4f89-11d3-9a0c-0305e82c3301"

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return mkdir(TEST_DIR, 0700) == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return 0;
}

static void test_generated_keys_differ(void **state)
{
    (void)state;
    repo_key_t a, b;
    assert_int_equal(repo_key_generate(&a), OK);
    assert_int_equal(repo_key_generate(&b), OK);
    assert_memory_not_equal(a.key, b.key, REPO_KEY_BYTES);
    assert_string_not_equal(a.id, b.id);
    /* The fingerprint identifies the key without being the key. */
    char hex[REPO_KEY_BYTES * 2 + 1];
    repo_key_to_hex(&a, hex);
    assert_null(strstr(hex, a.id));
    repo_key_wipe(&a);
    repo_key_wipe(&b);
}

static void test_id_is_stable_for_the_same_key(void **state)
{
    (void)state;
    repo_key_t a, b;
    assert_int_equal(repo_key_generate(&a), OK);
    char hex[REPO_KEY_BYTES * 2 + 1];
    repo_key_to_hex(&a, hex);
    assert_int_equal(repo_key_from_hex(hex, &b), OK);
    assert_memory_equal(a.key, b.key, REPO_KEY_BYTES);
    assert_string_equal(a.id, b.id);
}

/* An escrowed key is pasted by a human, so surrounding whitespace is normal
 * and a rejection there would be a support call during a restore. */
static void test_hex_round_trip_tolerates_whitespace(void **state)
{
    (void)state;
    repo_key_t a, b;
    assert_int_equal(repo_key_generate(&a), OK);
    char hex[REPO_KEY_BYTES * 2 + 1];
    repo_key_to_hex(&a, hex);

    char padded[REPO_KEY_BYTES * 2 + 16];
    snprintf(padded, sizeof(padded), "  %s\n", hex);
    assert_int_equal(repo_key_from_hex(padded, &b), OK);
    assert_memory_equal(a.key, b.key, REPO_KEY_BYTES);
}

static void test_malformed_key_is_rejected(void **state)
{
    (void)state;
    repo_key_t k;
    assert_int_not_equal(repo_key_from_hex("abc", &k), OK);
    err_clear();
    assert_int_not_equal(repo_key_from_hex("zz" "00000000000000000000000000000000"
                                           "000000000000000000000000000000", &k), OK);
    err_clear();
}

static void test_keyfile_round_trip(void **state)
{
    (void)state;
    repo_key_t a, b;
    assert_int_equal(repo_key_generate(&a), OK);
    assert_int_equal(repo_key_save(TEST_DIR, TEST_UUID, &a), OK);
    assert_true(repo_key_exists(TEST_DIR, TEST_UUID));
    assert_int_equal(repo_key_load(TEST_DIR, TEST_UUID, &b), OK);
    assert_memory_equal(a.key, b.key, REPO_KEY_BYTES);
    assert_string_equal(a.id, b.id);
}

/* The key is the one file on the host that must not be world-readable. */
static void test_keyfile_is_private(void **state)
{
    (void)state;
    repo_key_t k;
    assert_int_equal(repo_key_generate(&k), OK);
    assert_int_equal(repo_key_save(TEST_DIR, TEST_UUID, &k), OK);

    char path[512];
    snprintf(path, sizeof(path), "%s/%s.key", TEST_DIR, TEST_UUID);
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0600);
}

static void test_missing_key_is_reported_not_invented(void **state)
{
    (void)state;
    repo_key_t k;
    assert_int_not_equal(repo_key_load(TEST_DIR, "no-such-uuid", &k), OK);
    err_clear();
    assert_false(repo_key_exists(TEST_DIR, "no-such-uuid"));
}

static void test_key_removal(void **state)
{
    (void)state;
    repo_key_t k;
    assert_int_equal(repo_key_generate(&k), OK);
    assert_int_equal(repo_key_save(TEST_DIR, TEST_UUID, &k), OK);
    assert_int_equal(repo_key_remove(TEST_DIR, TEST_UUID), OK);
    assert_false(repo_key_exists(TEST_DIR, TEST_UUID));
    /* Removing what is not there is not an error: the caller wanted it gone. */
    assert_int_equal(repo_key_remove(TEST_DIR, TEST_UUID), OK);
}

/*
 * The passphrase path is what makes an offsite repository recoverable with no
 * surviving host, so it has to round-trip exactly.
 */
static void test_passphrase_wrap_round_trip(void **state)
{
    (void)state;
    repo_key_t a, b;
    assert_int_equal(repo_key_generate(&a), OK);

    repo_kdf_params_t p;
    char wrapped[REPO_KEY_WRAPPED_HEX];
    assert_int_equal(repo_key_wrap(&a, "correct horse battery staple", &p, wrapped), OK);

    assert_int_equal(repo_key_unwrap(wrapped, &p, "correct horse battery staple", &b), OK);
    assert_memory_equal(a.key, b.key, REPO_KEY_BYTES);
    assert_string_equal(a.id, b.id);
}

/* A wrong passphrase must fail here, at the tag check, rather than yielding a
 * plausible key that turns every object into a corruption report. */
static void test_wrong_passphrase_fails_cleanly(void **state)
{
    (void)state;
    repo_key_t a, b;
    assert_int_equal(repo_key_generate(&a), OK);
    repo_kdf_params_t p;
    char wrapped[REPO_KEY_WRAPPED_HEX];
    assert_int_equal(repo_key_wrap(&a, "the right one", &p, wrapped), OK);

    assert_int_not_equal(repo_key_unwrap(wrapped, &p, "the wrong one", &b), OK);
    err_clear();
}

/* Wrapping the same key twice must not produce the same blob: a fresh salt
 * and nonce each time is what keeps two repositories from revealing that they
 * share a passphrase. */
static void test_wrapping_is_salted(void **state)
{
    (void)state;
    repo_key_t k;
    assert_int_equal(repo_key_generate(&k), OK);

    repo_kdf_params_t p1, p2;
    char w1[REPO_KEY_WRAPPED_HEX], w2[REPO_KEY_WRAPPED_HEX];
    assert_int_equal(repo_key_wrap(&k, "same passphrase", &p1, w1), OK);
    assert_int_equal(repo_key_wrap(&k, "same passphrase", &p2, w2), OK);

    assert_string_not_equal(p1.salt_hex, p2.salt_hex);
    assert_string_not_equal(w1, w2);

    /* Both still unwrap to the same key. */
    repo_key_t a, b;
    assert_int_equal(repo_key_unwrap(w1, &p1, "same passphrase", &a), OK);
    assert_int_equal(repo_key_unwrap(w2, &p2, "same passphrase", &b), OK);
    assert_memory_equal(a.key, b.key, REPO_KEY_BYTES);
}

static void test_corrupt_wrapped_blob_is_rejected(void **state)
{
    (void)state;
    repo_key_t k, out;
    assert_int_equal(repo_key_generate(&k), OK);
    repo_kdf_params_t p;
    char wrapped[REPO_KEY_WRAPPED_HEX];
    assert_int_equal(repo_key_wrap(&k, "pass", &p, wrapped), OK);

    /* Flip one hex digit in the ciphertext: the AEAD tag must catch it. */
    wrapped[30] = (wrapped[30] == 'a') ? 'b' : 'a';
    assert_int_not_equal(repo_key_unwrap(wrapped, &p, "pass", &out), OK);
    err_clear();

    assert_int_not_equal(repo_key_unwrap("deadbeef", &p, "pass", &out), OK);
    err_clear();
}

static void test_mode_names_round_trip(void **state)
{
    (void)state;
    repo_enc_mode_t m;
    assert_int_equal(repo_enc_mode_parse("none", &m), 0);
    assert_int_equal(m, REPO_ENC_NONE);
    assert_int_equal(repo_enc_mode_parse("system", &m), 0);
    assert_int_equal(m, REPO_ENC_SYSTEM);
    assert_int_equal(repo_enc_mode_parse("passphrase", &m), 0);
    assert_int_equal(m, REPO_ENC_PASSPHRASE);
    assert_int_not_equal(repo_enc_mode_parse("sideways", &m), 0);
    assert_string_equal(repo_enc_mode_name(REPO_ENC_SYSTEM), "system");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_generated_keys_differ, setup, teardown),
        cmocka_unit_test_setup_teardown(test_id_is_stable_for_the_same_key, setup, teardown),
        cmocka_unit_test_setup_teardown(test_hex_round_trip_tolerates_whitespace, setup, teardown),
        cmocka_unit_test_setup_teardown(test_malformed_key_is_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_keyfile_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_keyfile_is_private, setup, teardown),
        cmocka_unit_test_setup_teardown(test_missing_key_is_reported_not_invented, setup, teardown),
        cmocka_unit_test_setup_teardown(test_key_removal, setup, teardown),
        cmocka_unit_test_setup_teardown(test_passphrase_wrap_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wrong_passphrase_fails_cleanly, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wrapping_is_salted, setup, teardown),
        cmocka_unit_test_setup_teardown(test_corrupt_wrapped_blob_is_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_mode_names_round_trip, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
