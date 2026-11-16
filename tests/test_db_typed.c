#define _POSIX_C_SOURCE 200809L

/*
 * What c-backup relies on from c-utils' typed parameters (c-utils 1.7).
 *
 * Every hash column holds raw bytes (NIT-12) and absent values are real
 * NULLs, which the string-array API could do neither of. These pin down
 * the behaviour the call sites assume, so a c-utils change that broke it
 * fails here rather than as a hash that matches nothing: a BLOB binds its
 * raw bytes, NULL is a value rather than the end of the list, integers bind
 * as integers, numbered placeholders resolve as SQLite numbers them, text
 * cannot escape its parameter, and db_result_is_null tells NULL from "".
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

#include "db_typed.h"
#include "test_tmp.h"

#define TEST_DB TEST_TMP "/c_backup_test_db_typed.db"

static cutils_db_t *db;

static int setup(void **state)
{
    (void)state;
    unlink(TEST_DB);
    if (db_open(&db, TEST_DB) != 0) return -1;
    return db_exec_raw(db, "CREATE TABLE t (b BLOB, i INTEGER, s TEXT, d REAL)");
}

static int teardown(void **state)
{
    (void)state;
    db_close(db);
    unlink(TEST_DB);
    unlink(TEST_DB "-wal");
    unlink(TEST_DB "-shm");
    return 0;
}

static char *one(const char *sql)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, sql, NULL, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    char *v = strdup(r->rows[0][0]);
    db_result_free(r);
    return v;
}

static void test_blob_binds_raw_bytes(void **state)
{
    (void)state;
    uint8_t h[32];
    for (int i = 0; i < 32; i++) h[i] = (uint8_t)(i * 7);   /* includes a 0 byte */
    db_param_t p[] = { DB_HASH(h), DB_INT64(1) };
    assert_int_equal(db_execute_non_query_typed(db, "INSERT INTO t (b, i) VALUES (?, ?)",
                                                p, DB_NPARAMS(p), NULL), 0);
    char *v = one("SELECT typeof(b) || ':' || length(b) || ':' || lower(hex(b)) FROM t");
    assert_string_equal(v, "blob:32:00070e151c232a31383f464d545b626970777e858c939aa1a8afb6bdc4cbd2d9");
    free(v);

    /* And it matches as a key. */
    db_result_t *r = NULL;
    db_param_t q[] = { DB_HASH(h) };
    assert_int_equal(db_execute_typed(db, "SELECT i FROM t WHERE b = ?", q, 1, &r), 0);
    assert_int_equal(r->nrows, 1);
    db_result_free(r);
}

static void test_null_is_a_value_not_a_terminator(void **state)
{
    (void)state;
    uint8_t h[32] = { 1 };
    db_param_t p[] = { DB_HASH_OR_NULL(0, h), DB_INT64(2), DB_TEXT(NULL), DB_NULLP };
    assert_int_equal(db_execute_non_query_typed(db,
        "INSERT INTO t (b, i, s, d) VALUES (?, ?, ?, ?)", p, DB_NPARAMS(p), NULL), 0);
    char *v = one("SELECT (b IS NULL) || (s IS NULL) || (d IS NULL) || i FROM t");
    assert_string_equal(v, "1112");
    free(v);
    db_param_t q[] = { DB_INT64(3), DB_NULLP, DB_TEXT("x") };
    assert_int_equal(db_execute_non_query_typed(db,
        "INSERT INTO t (i, b, s) VALUES (?, ?, ?)", q, DB_NPARAMS(q), NULL), 0);
    v = one("SELECT s FROM t WHERE i = 3");
    assert_string_equal(v, "x");
    free(v);
}

static void test_result_null_is_not_empty_text(void **state)
{
    (void)state;
    db_param_t p[] = { DB_TEXT(""), DB_INT64(1) };
    assert_int_equal(db_execute_non_query_typed(db,
        "INSERT INTO t (s, i) VALUES (?, ?)", p, DB_NPARAMS(p), NULL), 0);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT s, d, MIN(d) FROM t", NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "");
    assert_string_equal(r->rows[0][1], "");
    assert_false(db_result_is_null(r, 0, 0));
    assert_true(db_result_is_null(r, 0, 1));
    assert_true(db_result_is_null(r, 0, 2));
    db_result_free(r);
}

static void test_numbers_and_negatives(void **state)
{
    (void)state;
    db_param_t p[] = { DB_INT64(INT64_MIN), DB_INT64(-5) };
    db_result_t *r = NULL;
    assert_int_equal(db_execute_typed(db, "SELECT ?1, 10-?2, typeof(?1)", p, 2, &r), 0);
    assert_string_equal(r->rows[0][0], "-9223372036854775808");
    assert_string_equal(r->rows[0][1], "15");
    assert_string_equal(r->rows[0][2], "integer");
    db_result_free(r);
    db_param_t d[] = { DB_DOUBLE(0.5) };
    assert_int_equal(db_execute_typed(db, "SELECT ? * 4", d, 1, &r), 0);
    assert_string_equal(r->rows[0][0], "2.0");
    db_result_free(r);
}

static void test_placeholders_and_text(void **state)
{
    (void)state;
    db_param_t p[] = { DB_TEXT("a'b"), DB_INT64(7), DB_TEXT("third") };
    db_result_t *r = NULL;
    /* '?' in a literal is a literal; ?2 is used twice; the bare ? after ?2
     * is number 3. */
    assert_int_equal(db_execute_typed(db, "SELECT '?' || ?1, ?2 + ?2, ?",
                                      p, DB_NPARAMS(p), &r), 0);
    assert_string_equal(r->rows[0][0], "?a'b");
    assert_string_equal(r->rows[0][1], "14");
    assert_string_equal(r->rows[0][2], "third");
    db_result_free(r);

    /* Text stays a bound parameter: a quote in it cannot end a literal. */
    db_param_t q[] = { DB_TEXT("x'); DROP TABLE t; --") };
    assert_int_equal(db_execute_typed(db, "SELECT ?", q, 1, &r), 0);
    assert_string_equal(r->rows[0][0], "x'); DROP TABLE t; --");
    db_result_free(r);
    char *v = one("SELECT COUNT(*) FROM sqlite_master WHERE name = 't'");
    assert_string_equal(v, "1");
    free(v);
}

/*
 * An empty blob and NULL are different values, and only the pointer says
 * which: from c-utils 1.7.1 a NULL pointer binds NULL whatever the length.
 * Every blob c-backup binds is a 32-byte name from an array (DB_HASH), and
 * an absent one is DB_NULLP (DB_HASH_OR_NULL), so neither depends on what
 * DB_BLOB does with NULL; this holds the two spellings that are meant.
 */
static void test_empty_blob_and_null_are_distinct(void **state)
{
    (void)state;
    static const uint8_t none[1] = { 0 };
    uint8_t h[32] = { 7 };
    db_param_t p[] = { DB_BLOB(none, 0), DB_INT64(1) };
    assert_int_equal(db_execute_non_query_typed(db, "INSERT INTO t (b, i) VALUES (?, ?)",
                                                p, DB_NPARAMS(p), NULL), 0);
    db_param_t q[] = { DB_HASH_OR_NULL(0, h), DB_INT64(2) };
    assert_int_equal(db_execute_non_query_typed(db, "INSERT INTO t (b, i) VALUES (?, ?)",
                                                q, DB_NPARAMS(q), NULL), 0);
    db_param_t s[] = { DB_HASH_OR_NULL(1, h), DB_INT64(3) };
    assert_int_equal(db_execute_non_query_typed(db, "INSERT INTO t (b, i) VALUES (?, ?)",
                                                s, DB_NPARAMS(s), NULL), 0);
    char *v = one("SELECT group_concat(typeof(b) || ':' || COALESCE(length(b), -1), ',')"
                  " FROM (SELECT b FROM t ORDER BY i)");
    assert_string_equal(v, "blob:0,null:-1,blob:32");
    free(v);
}

/* More values than placeholders is a caller bug, refused before it runs. */
static void test_extra_parameters_fail(void **state)
{
    (void)state;
    db_param_t p[] = { DB_INT64(1), DB_INT64(2) };
    db_result_t *r = NULL;
    assert_int_not_equal(db_execute_typed(db, "SELECT ?", p, 2, &r), 0);
    assert_null(r);
    assert_int_not_equal(db_execute_non_query_typed(db, "DELETE FROM t WHERE i = ?",
                                                    p, 2, NULL), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_blob_binds_raw_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_null_is_a_value_not_a_terminator, setup, teardown),
        cmocka_unit_test_setup_teardown(test_result_null_is_not_empty_text, setup, teardown),
        cmocka_unit_test_setup_teardown(test_numbers_and_negatives, setup, teardown),
        cmocka_unit_test_setup_teardown(test_placeholders_and_text, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_blob_and_null_are_distinct, setup, teardown),
        cmocka_unit_test_setup_teardown(test_extra_parameters_fail, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
