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

#define TEST_DB "/tmp/c_backup_test_auth.db"

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

int main(void)
{
    const struct CMUnitTest tests[] = {
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
