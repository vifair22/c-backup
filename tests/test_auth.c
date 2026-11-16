#define _POSIX_C_SOURCE 200809L

/*
 * Authentication.
 *
 * The primitives are lifted from airies-ups, but the schema underneath them
 * is not: this project stores the admin hash in auth_config rather than a
 * dedicated table, and session timestamps as integer epochs rather than
 * SQLite datetime strings. Both differences silently broke login when the
 * code was first dropped in, and auth_is_setup's deliberate fail-closed
 * behaviour reported "configured" on a database where the query could not
 * even run. These tests exist mostly to keep that class of mismatch loud.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "auth.h"
#include "login_throttle.h"
#include "test_tmp.h"

#define TEST_DB TEST_TMP "/c_backup_test_auth.db"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;

static int setup(void **state)
{
    (void)state;
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (auth_init(db) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db);
    db = NULL;
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    return 0;
}

/*
 * On a migrated but unconfigured database this must be false. It returned
 * true before auth.c was adapted, because it queried a table this schema
 * does not have and fell back to its fail-closed default. That is the right
 * default and the wrong answer, which is exactly why it needs a test.
 */
static void test_fresh_db_is_not_setup(void **state)
{
    (void)state;
    assert_false(auth_is_setup(db));
}

static void test_setup_then_configured(void **state)
{
    (void)state;
    assert_int_equal(auth_set_password(db, "correcthorsebattery"), 0);
    assert_true(auth_is_setup(db));
}

static void test_password_verification(void **state)
{
    (void)state;
    assert_int_equal(auth_set_password(db, "correcthorsebattery"), 0);
    assert_true(auth_verify_password(db, "correcthorsebattery"));
    assert_false(auth_verify_password(db, "wrong"));
    assert_false(auth_verify_password(db, ""));
}

/* The stored value must not be the password. */
static void test_password_is_hashed(void **state)
{
    (void)state;
    assert_int_equal(auth_set_password(db, "correcthorsebattery"), 0);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT value FROM auth_config WHERE key = 'password_hash'", NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_null(strstr(r->rows[0][0], "correcthorsebattery"));
    assert_true(strlen(r->rows[0][0]) > 20);
    db_result_free(r);
}

static void test_password_change(void **state)
{
    (void)state;
    assert_int_equal(auth_set_password(db, "firstpassword"), 0);
    assert_int_equal(auth_set_password(db, "secondpassword"), 0);
    assert_false(auth_verify_password(db, "firstpassword"));
    assert_true(auth_verify_password(db, "secondpassword"));
}

static void test_token_roundtrip(void **state)
{
    (void)state;
    assert_int_equal(auth_set_password(db, "correcthorsebattery"), 0);

    char *token = auth_create_token(db, 24);
    assert_non_null(token);
    assert_true(strlen(token) >= 32);
    assert_true(auth_validate_token(db, token));

    /* Bearer prefix is accepted, since that is how it arrives on the wire. */
    char bearer[128];
    snprintf(bearer, sizeof(bearer), "Bearer %s", token);
    assert_true(auth_validate_token(db, bearer));

    free(token);
}

static void test_unknown_token_rejected(void **state)
{
    (void)state;
    assert_false(auth_validate_token(db, "not-a-real-token"));
    assert_false(auth_validate_token(db, ""));
    assert_false(auth_validate_token(db, NULL));
}

static void test_tokens_are_distinct(void **state)
{
    (void)state;
    char *a = auth_create_token(db, 24);
    char *b = auth_create_token(db, 24);
    assert_non_null(a);
    assert_non_null(b);
    assert_string_not_equal(a, b);
    free(a); free(b);
}

static void test_revoked_token_rejected(void **state)
{
    (void)state;
    char *token = auth_create_token(db, 24);
    assert_non_null(token);
    assert_true(auth_validate_token(db, token));
    auth_revoke_token(db, token);
    assert_false(auth_validate_token(db, token));
    free(token);
}

/* Revoking something that was never issued must not fault or throw. */
static void test_revoke_unknown_is_safe(void **state)
{
    (void)state;
    auth_revoke_token(db, "never-issued");
    auth_revoke_token(db, NULL);
}

/*
 * Expiry is compared against integer epoch seconds. If a future change
 * reintroduces datetime strings on either side, string-vs-number comparison
 * makes every token look valid or every token look expired, and neither
 * failure announces itself.
 */
static void test_expired_token_rejected(void **state)
{
    (void)state;
    char *token = auth_create_token(db, 24);
    assert_non_null(token);
    assert_true(auth_validate_token(db, token));

    char past[32];
    snprintf(past, sizeof(past), "%lld", (long long)(time(NULL) - 3600));
    assert_int_equal(db_execute_non_query(db,
        "UPDATE sessions SET expires_at = ? WHERE token = ?",
        (const char *[]){ past, token, NULL }, NULL), 0);

    assert_false(auth_validate_token(db, token));
    free(token);
}

static void test_cleanup_removes_only_expired(void **state)
{
    (void)state;
    char *live = auth_create_token(db, 24);
    char *dead = auth_create_token(db, 24);
    assert_non_null(live);
    assert_non_null(dead);

    char past[32];
    snprintf(past, sizeof(past), "%lld", (long long)(time(NULL) - 3600));
    assert_int_equal(db_execute_non_query(db,
        "UPDATE sessions SET expires_at = ? WHERE token = ?",
        (const char *[]){ past, dead, NULL }, NULL), 0);

    auth_cleanup_sessions(db);

    assert_true(auth_validate_token(db, live));
    assert_false(auth_validate_token(db, dead));
    free(live); free(dead);
}

/* Validating a session slides its expiry forward, so an active user is not
 * logged out mid-use. */
static void test_session_expiry_slides(void **state)
{
    (void)state;
    char *token = auth_create_token(db, 1);
    assert_non_null(token);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT expires_at FROM sessions WHERE token = ?",
                                (const char *[]){ token, NULL }, &r), 0);
    long long before = atoll(r->rows[0][0]);
    db_result_free(r);

    assert_true(auth_validate_token(db, token));

    r = NULL;
    assert_int_equal(db_execute(db, "SELECT expires_at FROM sessions WHERE token = ?",
                                (const char *[]){ token, NULL }, &r), 0);
    long long after = atoll(r->rows[0][0]);
    db_result_free(r);

    assert_true(after > before);
    free(token);
}

/*
 * B053. A password change revoked nothing, so a session opened with the old
 * password kept working. Revoking everything but the caller's own token is
 * what the password route does; with no token to keep, everything goes.
 */
static void test_revoke_all_spares_only_the_caller(void **state)
{
    (void)state;
    char *mine  = auth_create_token(db, 24);
    char *other = auth_create_token(db, 24);
    char *third = auth_create_token(db, 24);
    assert_non_null(mine); assert_non_null(other); assert_non_null(third);

    char bearer[96];
    snprintf(bearer, sizeof(bearer), "Bearer %s", mine);
    assert_int_equal(auth_revoke_all(db, bearer), 2);
    assert_true(auth_validate_token(db, mine));
    assert_false(auth_validate_token(db, other));
    assert_false(auth_validate_token(db, third));

    assert_int_equal(auth_revoke_all(db, NULL), 1);
    assert_false(auth_validate_token(db, mine));
    assert_int_equal(auth_revoke_all(db, ""), 0);
    free(mine); free(other); free(third);
}

/*
 * B054. A hundred wrong passwords in under three seconds, then the right
 * one, all succeeded. The first few failures are free, then each one doubles
 * the lockout for that address, and quiet time forgets it.
 */
static void test_throttle_backs_off_per_address(void **state)
{
    (void)state;
    login_throttle_reset();
    time_t t = 1000000;
    for (int i = 0; i < LOGIN_THROTTLE_FREE; i++) {
        assert_int_equal(login_throttle_check("10.0.0.1", t), 0);
        login_throttle_fail("10.0.0.1", t);
    }
    assert_int_equal(login_throttle_check("10.0.0.1", t), 0);
    login_throttle_fail("10.0.0.1", t);            /* first paid failure */
    assert_int_equal(login_throttle_check("10.0.0.1", t), 1);
    assert_int_equal(login_throttle_check("10.0.0.2", t), 0);   /* others unaffected */

    login_throttle_fail("10.0.0.1", t + 1);
    assert_int_equal(login_throttle_check("10.0.0.1", t + 1), 2);
    login_throttle_fail("10.0.0.1", t + 3);
    assert_int_equal(login_throttle_check("10.0.0.1", t + 3), 4);

    /* Capped, however long it keeps failing. */
    for (int i = 0; i < 40; i++) login_throttle_fail("10.0.0.1", t + 10);
    long w = login_throttle_check("10.0.0.1", t + 10);
    assert_true(w > 0 && w <= LOGIN_THROTTLE_MAX_DELAY);

    /* Success forgets it. */
    login_throttle_success("10.0.0.1");
    assert_int_equal(login_throttle_check("10.0.0.1", t + 10), 0);

    /* And so does going quiet. */
    login_throttle_reset();
    for (int i = 0; i < LOGIN_THROTTLE_FREE + 3; i++) login_throttle_fail("10.0.0.3", t);
    assert_true(login_throttle_check("10.0.0.3", t) > 0);
    time_t later = t + LOGIN_THROTTLE_FORGET + LOGIN_THROTTLE_MAX_DELAY + 1;
    assert_int_equal(login_throttle_check("10.0.0.3", later), 0);
    login_throttle_fail("10.0.0.3", later);
    assert_int_equal(login_throttle_check("10.0.0.3", later), 0);  /* counting from 1 again */
}

/* Rotating addresses must not escape the cap, and memory stays a fixed
 * table however many addresses fail. */
static void test_throttle_global_cap_and_bounded_table(void **state)
{
    (void)state;
    login_throttle_reset();
    time_t t = 2000000;
    char addr[32];
    for (int i = 0; i < LOGIN_THROTTLE_GLOBAL_CAP; i++) {
        snprintf(addr, sizeof(addr), "192.168.%d.%d", i / 250, i % 250);
        login_throttle_fail(addr, t);
    }
    assert_true(login_throttle_check("172.16.0.1", t) > 0);    /* a fresh address too */
    assert_true(login_throttle_check(NULL, t) > 0);
    /* It drains. */
    assert_int_equal(login_throttle_check("172.16.0.1",
                                          t + LOGIN_THROTTLE_GLOBAL_DRAIN), 0);

    /* Far more addresses than slots: nothing grows, and the newest address
     * is still tracked after the oldest were evicted. */
    login_throttle_reset();
    for (int i = 0; i < LOGIN_THROTTLE_SLOTS * 4; i++) {
        snprintf(addr, sizeof(addr), "10.%d.%d.1", i / 250, i % 250);
        login_throttle_fail(addr, t + i * 10);   /* slow enough to stay under the cap */
    }
    time_t end = t + LOGIN_THROTTLE_SLOTS * 4 * 10;
    for (int i = 0; i < LOGIN_THROTTLE_FREE; i++) login_throttle_fail(addr, end);
    assert_true(login_throttle_check(addr, end) > 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_throttle_backs_off_per_address),
        cmocka_unit_test(test_throttle_global_cap_and_bounded_table),
        cmocka_unit_test_setup_teardown(test_revoke_all_spares_only_the_caller, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fresh_db_is_not_setup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_setup_then_configured, setup, teardown),
        cmocka_unit_test_setup_teardown(test_password_verification, setup, teardown),
        cmocka_unit_test_setup_teardown(test_password_is_hashed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_password_change, setup, teardown),
        cmocka_unit_test_setup_teardown(test_token_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unknown_token_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tokens_are_distinct, setup, teardown),
        cmocka_unit_test_setup_teardown(test_revoked_token_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_revoke_unknown_is_safe, setup, teardown),
        cmocka_unit_test_setup_teardown(test_expired_token_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cleanup_removes_only_expired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_session_expiry_slides, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
