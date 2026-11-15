#define _POSIX_C_SOURCE 200809L

/*
 * Database text to typed JSON (src/api/routes/json_row.c).
 *
 * The HTTP e2e suite proves no route serves a quoted number. This proves the
 * conversion itself at the edges a fixture never reaches: the NULL column
 * c-utils reads back as "", integers past 2^53, a REAL in an integer column,
 * and text that is not a number at all.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>
#include <cutils/json.h>

#include "json_row.h"

/* One element under "a", serialised, so a test compares JSON text. */
static char *elem_json(const jcol_t *cols, size_t n, char **row)
{
    cutils_json_resp_t *resp = NULL;
    assert_int_equal(json_resp_new(&resp), 0);
    cutils_json_elem_t e;
    assert_int_equal(json_resp_array_append_begin(resp, "a", &e), 0);
    assert_int_equal(jrow_elem_add_row(&e, cols, n, row), 0);
    json_elem_commit(&e);
    char *body = NULL;
    size_t len = 0;
    assert_int_equal(json_resp_finalize(resp, &body, &len), 0);
    json_resp_free(resp);
    return body;
}

static char *resp_json(const char *key, jcol_kind_t kind, const char *text)
{
    cutils_json_resp_t *resp = NULL;
    assert_int_equal(json_resp_new(&resp), 0);
    assert_int_equal(jrow_resp_add(resp, key, kind, text), 0);
    char *body = NULL;
    size_t len = 0;
    assert_int_equal(json_resp_finalize(resp, &body, &len), 0);
    json_resp_free(resp);
    return body;
}

static void expect_resp(const char *key, jcol_kind_t kind, const char *text,
                        const char *want)
{
    char *got = resp_json(key, kind, text);
    assert_string_equal(got, want);
    free(got);
}

static void test_integers(void **state)
{
    (void)state;
    expect_resp("n", JCOL_INT, "0", "{\"n\":0}");
    expect_resp("n", JCOL_INT, "42", "{\"n\":42}");
    expect_resp("n", JCOL_INT, "-7", "{\"n\":-7}");
    /* cJSON prints 15 significant digits, so this is the widest integer
     * that comes out digit for digit. Every id, count, byte size and epoch
     * second the schema holds is far below it. */
    expect_resp("n", JCOL_INT, "999999999999999", "{\"n\":999999999999999}");
}

static void test_past_two_to_the_53(void **state)
{
    (void)state;
    /* Refused by the c-utils integer adder; served as the nearest double
     * rather than failing the whole response. */
    char *got = resp_json("n", JCOL_INT, "1794728493967432470");
    assert_non_null(strstr(got, "\"n\":1.79"));
    assert_null(strchr(got + 5, '"'));
    free(got);
}

static void test_null_and_junk(void **state)
{
    (void)state;
    /* A NULL column is "" from c-utils, never a NULL pointer; both mean
     * "no value". */
    expect_resp("n", JCOL_INT, "", "{\"n\":null}");
    expect_resp("n", JCOL_INT, NULL, "{\"n\":null}");
    expect_resp("b", JCOL_BOOL, "", "{\"b\":null}");
    expect_resp("s", JCOL_STR_NULL, "", "{\"s\":null}");
    /* Text in a numeric column is a corrupt row; null, not an invented 0. */
    expect_resp("n", JCOL_INT, "banana", "{\"n\":null}");
    expect_resp("n", JCOL_INT, "12abc", "{\"n\":null}");
    expect_resp("b", JCOL_BOOL, "maybe", "{\"b\":null}");
}

static void test_reals(void **state)
{
    (void)state;
    expect_resp("n", JCOL_INT, "1.5", "{\"n\":1.5}");
}

static void test_flags(void **state)
{
    (void)state;
    expect_resp("b", JCOL_BOOL, "1", "{\"b\":true}");
    expect_resp("b", JCOL_BOOL, "0", "{\"b\":false}");
    expect_resp("b", JCOL_BOOL, "2", "{\"b\":true}");
    expect_resp("b", JCOL_BOOL, "true", "{\"b\":true}");
    expect_resp("b", JCOL_BOOL, "false", "{\"b\":false}");
}

static void test_strings(void **state)
{
    (void)state;
    expect_resp("s", JCOL_STR, "", "{\"s\":\"\"}");
    expect_resp("s", JCOL_STR, "123", "{\"s\":\"123\"}");
    expect_resp("s", JCOL_STR_NULL, "x\"y", "{\"s\":\"x\\\"y\"}");
}

static void test_row(void **state)
{
    (void)state;
    /* Columns out of order and one skipped: the spec, not the row, decides
     * the shape. */
    static const jcol_t cols[] = {
        { "id", 0, JCOL_INT }, { "name", 2, JCOL_STR },
        { "flag", 3, JCOL_BOOL }, { "label", 4, JCOL_STR_NULL },
    };
    char *row[] = { "5", "ignored", "a.txt", "1", "" };
    char *got = elem_json(cols, JROW_N(cols), row);
    assert_string_equal(got,
        "{\"a\":[{\"id\":5,\"name\":\"a.txt\",\"flag\":true,\"label\":null}]}");
    free(got);
}

static void test_resp_row(void **state)
{
    (void)state;
    static const jcol_t cols[] = { { "n", 1, JCOL_INT }, { "s", 0, JCOL_STR } };
    char *row[] = { "hi", "3" };
    cutils_json_resp_t *resp = NULL;
    assert_int_equal(json_resp_new(&resp), 0);
    assert_int_equal(jrow_resp_add_row(resp, cols, JROW_N(cols), row), 0);
    cutils_json_elem_t e;
    assert_int_equal(json_resp_array_append_begin(resp, "a", &e), 0);
    assert_int_equal(jrow_elem_add(&e, "x", JCOL_INT, "9"), 0);
    json_elem_commit(&e);
    char *body = NULL;
    size_t len = 0;
    assert_int_equal(json_resp_finalize(resp, &body, &len), 0);
    json_resp_free(resp);
    assert_string_equal(body, "{\"n\":3,\"s\":\"hi\",\"a\":[{\"x\":9}]}");
    free(body);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_integers),
        cmocka_unit_test(test_past_two_to_the_53),
        cmocka_unit_test(test_null_and_junk),
        cmocka_unit_test(test_reals),
        cmocka_unit_test(test_flags),
        cmocka_unit_test(test_strings),
        cmocka_unit_test(test_row),
        cmocka_unit_test(test_resp_row),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
