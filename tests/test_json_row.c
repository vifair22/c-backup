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
#include <stdbool.h>
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

/*
 * NIT-1: request flags. A boolean is the spelling written now; 0 and 1 are
 * still read, because grep schedules stored before the change hold them.
 * Anything else is refused, and absence leaves the default alone.
 */
static int flag_of(const char *body, int *out)
{
    cutils_json_req_t *req = NULL;
    assert_int_equal(json_req_parse(body, strlen(body), &req), 0);
    int rc = jreq_flag(req, "f", out);
    json_req_free(req);
    return rc;
}

static void test_request_flags(void **state)
{
    (void)state;
    int v = 7;
    assert_int_equal(flag_of("{\"f\":true}", &v), 0);  assert_int_equal(v, 1);
    assert_int_equal(flag_of("{\"f\":false}", &v), 0); assert_int_equal(v, 0);
    assert_int_equal(flag_of("{\"f\":1}", &v), 0);     assert_int_equal(v, 1);
    assert_int_equal(flag_of("{\"f\":0}", &v), 0);     assert_int_equal(v, 0);
    v = 7;
    assert_int_equal(flag_of("{}", &v), 0);              assert_int_equal(v, 7);
    assert_int_equal(flag_of("{\"f\":2}", &v), -1);
    assert_int_equal(flag_of("{\"f\":\"yes\"}", &v), -1);
    assert_int_equal(flag_of("{\"f\":null}", &v), -1);
    assert_int_equal(v, 7);
}

/*
 * B112: typed request fields. A present field of the wrong type used to read
 * as absent, so the route said "is required" (or quietly used a default)
 * about something the client had sent. Each case here pins the verdict and
 * the message, since the message is what the client acts on.
 */
static cutils_json_req_t *req_of(const char *body)
{
    cutils_json_req_t *req = NULL;
    assert_int_equal(json_req_parse(body, strlen(body), &req), 0);
    return req;
}

static void test_request_types(void **state)
{
    (void)state;
    static const struct { const char *body; jreq_type_t t; } cases[] = {
        { "{}",               JREQ_T_ABSENT },
        { "{\"f\":null}",     JREQ_T_NULL   },
        { "{\"f\":true}",     JREQ_T_BOOL   },
        { "{\"f\":3}",        JREQ_T_NUMBER },
        { "{\"f\":-1.5}",     JREQ_T_NUMBER },
        { "{\"f\":1e999}",    JREQ_T_HUGE   },
        { "{\"f\":-1e999}",   JREQ_T_HUGE   },
        { "{\"f\":\"x\"}",    JREQ_T_STRING },
        { "{\"f\":\"\"}",     JREQ_T_STRING },
        { "{\"f\":[]}",       JREQ_T_ARRAY  },
        { "{\"f\":[1]}",      JREQ_T_ARRAY  },
        { "{\"f\":{}}",       JREQ_T_OBJECT },
        { "{\"f\":{\"a\":1}}", JREQ_T_OBJECT },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cutils_json_req_t *req = req_of(cases[i].body);
        if (jreq_type(req, "f") != cases[i].t) fail_msg("%s", cases[i].body);
        json_req_free(req);
    }
}

static void test_request_strings(void **state)
{
    (void)state;
    static const char *const wrong[] = {
        "{\"label\":[\"a\"]}", "{\"label\":{}}", "{\"label\":5}",
        "{\"label\":true}", "{\"label\":null}",
    };
    for (size_t i = 0; i < sizeof(wrong) / sizeof(wrong[0]); i++) {
        cutils_json_req_t *req = req_of(wrong[i]);
        char *v = NULL;
        jreq_err_t e;
        assert_int_equal(jreq_str(req, "label", &v, &e), JREQ_BAD);
        assert_null(v);
        assert_string_equal(e.msg, "label must be a string");
        json_req_free(req);
    }

    cutils_json_req_t *req = req_of("{\"label\":\"nightly\"}");
    char *v = NULL;
    jreq_err_t e;
    assert_int_equal(jreq_str(req, "label", &v, &e), JREQ_OK);
    assert_string_equal(v, "nightly");
    free(v);
    v = NULL;
    assert_int_equal(jreq_str(req, "other", &v, &e), JREQ_ABSENT);
    assert_null(v);
    json_req_free(req);
}

static void test_request_bools(void **state)
{
    (void)state;
    cutils_json_req_t *req = req_of("{\"t\":true,\"f\":false,\"n\":1,\"s\":\"true\"}");
    bool b = false;
    jreq_err_t e;
    assert_int_equal(jreq_bool(req, "t", &b, &e), JREQ_OK); assert_true(b);
    assert_int_equal(jreq_bool(req, "f", &b, &e), JREQ_OK); assert_false(b);
    assert_int_equal(jreq_bool(req, "x", &b, &e), JREQ_ABSENT);
    assert_int_equal(jreq_bool(req, "n", &b, &e), JREQ_BAD);
    assert_string_equal(e.msg, "n must be true or false");
    assert_int_equal(jreq_bool(req, "s", &b, &e), JREQ_BAD);
    json_req_free(req);
}

static int u32_of(const char *body, uint32_t *out, jreq_err_t *e)
{
    cutils_json_req_t *req = req_of(body);
    int rc = jreq_u32(req, "snap_id", out, 1, UINT32_MAX, e);
    json_req_free(req);
    return rc;
}

static void test_request_u32(void **state)
{
    (void)state;
    uint32_t v = 7;
    jreq_err_t e;
    assert_int_equal(u32_of("{\"snap_id\":3}", &v, &e), JREQ_OK);
    assert_int_equal(v, 3);
    assert_int_equal(u32_of("{\"snap_id\":4294967295}", &v, &e), JREQ_OK);
    assert_int_equal(v, UINT32_MAX);
    v = 7;
    assert_int_equal(u32_of("{}", &v, &e), JREQ_ABSENT);
    assert_int_equal(v, 7);

    static const struct { const char *body; const char *msg; } bad[] = {
        { "{\"snap_id\":1e999}",
          "snap_id is out of range; it must be from 1 to 4294967295" },
        { "{\"snap_id\":-1e999}",
          "snap_id is out of range; it must be from 1 to 4294967295" },
        { "{\"snap_id\":-1}",
          "snap_id is out of range; it must be from 1 to 4294967295" },
        { "{\"snap_id\":0}",
          "snap_id is out of range; it must be from 1 to 4294967295" },
        { "{\"snap_id\":4294967296}",
          "snap_id is out of range; it must be from 1 to 4294967295" },
        { "{\"snap_id\":99999999999999999999}",
          "snap_id is out of range; it must be from 1 to 4294967295" },
        { "{\"snap_id\":1.5}",  "snap_id must be a whole number" },
        { "{\"snap_id\":-0.5}", "snap_id must be a whole number" },
        { "{\"snap_id\":\"3\"}", "snap_id must be a number" },
        { "{\"snap_id\":true}",  "snap_id must be a number" },
        { "{\"snap_id\":null}",  "snap_id must be a number" },
        { "{\"snap_id\":[3]}",   "snap_id must be a number" },
        { "{\"snap_id\":{}}",    "snap_id must be a number" },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        v = 7;
        if (u32_of(bad[i].body, &v, &e) != JREQ_BAD) fail_msg("%s", bad[i].body);
        assert_string_equal(e.msg, bad[i].msg);
        assert_int_equal(v, 7);
    }
}

static void test_request_wide_ints(void **state)
{
    (void)state;
    jreq_err_t e;
    cutils_json_req_t *req = req_of(
        "{\"a\":9007199254740992,\"b\":9007199254740994,\"c\":-12,"
        "\"d\":-9007199254740994,\"t\":{\"seconds\":1e999}}");

    uint64_t u = 0;
    assert_int_equal(jreq_u64(req, "a", &u, 0, UINT64_MAX, &e), JREQ_OK);
    assert_true(u == 9007199254740992ULL);
    /* Past 2^53 the value read is not the value sent, so it is refused and
     * the message gives the bound that actually applies. */
    assert_int_equal(jreq_u64(req, "b", &u, 0, UINT64_MAX, &e), JREQ_BAD);
    assert_string_equal(e.msg, "b is out of range; it must be from 0 to 9007199254740992");
    assert_int_equal(jreq_u64(req, "c", &u, 0, UINT64_MAX, &e), JREQ_BAD);

    int64_t s = 0;
    assert_int_equal(jreq_i64(req, "c", &s, INT64_MIN, INT64_MAX, &e), JREQ_OK);
    assert_true(s == -12);
    assert_int_equal(jreq_i64(req, "d", &s, INT64_MIN, INT64_MAX, &e), JREQ_BAD);
    assert_string_equal(e.msg, "d is out of range; it must be from "
                               "-9007199254740992 to 9007199254740992");

    /* A dot path is named as given, so a nested field says where it is. */
    assert_int_equal(jreq_u64(req, "t.seconds", &u, 1, UINT64_MAX, &e), JREQ_BAD);
    assert_string_equal(e.msg, "t.seconds is out of range; it must be from 1 "
                               "to 9007199254740992");
    json_req_free(req);
}

static void test_request_string_lists(void **state)
{
    (void)state;
    jreq_err_t e;
    char **v = NULL;
    size_t n = 0;

    cutils_json_req_t *req = req_of("{\"p\":[\"/a\",\"/b\"],\"e\":[]}");
    assert_int_equal(jreq_strv(req, "p", 8, &v, &n, &e), JREQ_OK);
    assert_int_equal(n, 2);
    assert_string_equal(v[0], "/a");
    assert_string_equal(v[1], "/b");
    jreq_strv_free(v, n);
    v = NULL;
    assert_int_equal(jreq_strv(req, "e", 8, &v, &n, &e), JREQ_OK);
    assert_int_equal(n, 0);
    assert_null(v);
    assert_int_equal(jreq_strv(req, "x", 8, &v, &n, &e), JREQ_ABSENT);
    assert_int_equal(jreq_strv(req, "p", 1, &v, &n, &e), JREQ_BAD);
    assert_string_equal(e.msg, "p takes at most 1 entries");
    json_req_free(req);

    req = req_of("{\"p\":[\"/a\",5],\"q\":\"/a\",\"r\":[null]}");
    assert_int_equal(jreq_strv(req, "p", 8, &v, &n, &e), JREQ_BAD);
    assert_string_equal(e.msg, "p must be a list of strings; entry 2 is not one");
    assert_int_equal(jreq_strv(req, "q", 8, &v, &n, &e), JREQ_BAD);
    assert_string_equal(e.msg, "q must be a list of strings");
    assert_int_equal(jreq_strv(req, "r", 8, &v, &n, &e), JREQ_BAD);
    json_req_free(req);
}

/*
 * Query-string numbers go through the same verdict as a body field, so the
 * two word a mistake the same way.
 */
static void test_query_numbers(void **state)
{
    (void)state;
    double d = 0.0;
    static const char *const nums[] = { "0", "7", "-1", "1.5", "1e3", "1E+2",
                                        "-0.0", "1e999", "99999999999999999999" };
    for (size_t i = 0; i < sizeof(nums) / sizeof(nums[0]); i++)
        if (jquery_number(nums[i], &d) != 0) fail_msg("%s", nums[i]);
    static const char *const junk[] = { "", "-", "+5", " 3", "3 ", "lots", "0x10",
                                        "010", "1.", ".5", "1e", "inf", "nan", "1,2" };
    for (size_t i = 0; i < sizeof(junk) / sizeof(junk[0]); i++)
        if (jquery_number(junk[i], &d) != -1) fail_msg("'%s'", junk[i]);

    int64_t v = 9;
    jreq_err_t e;
    assert_int_equal(jquery_i64(NULL, "snapshot", 1, UINT32_MAX, &v, &e), JREQ_ABSENT);
    assert_int_equal(jquery_i64("12", "snapshot", 1, UINT32_MAX, &v, &e), JREQ_OK);
    assert_true(v == 12);
    assert_int_equal(jquery_i64("1e2", "snapshot", 1, UINT32_MAX, &v, &e), JREQ_OK);
    assert_true(v == 100);

    static const struct { const char *text; const char *msg; } bad[] = {
        { "1e999", "snapshot is out of range; it must be from 1 to 4294967295" },
        { "-1",    "snapshot is out of range; it must be from 1 to 4294967295" },
        { "0",     "snapshot is out of range; it must be from 1 to 4294967295" },
        { "99999999999999999999",
                   "snapshot is out of range; it must be from 1 to 4294967295" },
        { "1.5",   "snapshot must be a whole number" },
        { "abc",   "snapshot must be a whole number" },
        { "",      "snapshot must be a whole number" },
        { "+5",    "snapshot must be a whole number" },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        v = 9;
        if (jquery_i64(bad[i].text, "snapshot", 1, UINT32_MAX, &v, &e) != JREQ_BAD)
            fail_msg("'%s'", bad[i].text);
        assert_string_equal(e.msg, bad[i].msg);
        assert_true(v == 9);
    }
    /* Wider than a double holds exactly: narrowed, and the message says so. */
    assert_int_equal(jquery_i64("9007199254740994", "parent", 0, INT64_MAX, &v, &e),
                     JREQ_BAD);
    assert_string_equal(e.msg, "parent is out of range; it must be from 0 to 9007199254740992");
}

/* One body through jreq_parse_body: its verdict, and the message on a BAD. */
static void expect_body(const char *body, size_t len, int want, const char *msg)
{
    cutils_json_req_t *r = (cutils_json_req_t *)(uintptr_t)1;
    jreq_err_t e;
    e.msg[0] = '\0';
    int rc = jreq_parse_body(body, len, &r, &e);
    assert_int_equal(rc, want);
    if (want == JREQ_OK) {
        assert_non_null(r);
        json_req_free(r);
    } else {
        assert_null(r);
        assert_string_equal(e.msg, msg);
    }
}

#define BODY(s) s, sizeof(s) - 1

/* B115: a string holding a NUL is refused before cJSON cuts it short. */
static void test_body_nul(void **state)
{
    (void)state;
    const char *rule = "label must be at most 256 bytes of printable text";
    expect_body(BODY("{\"label\":\"nul\\u0000here\"}"), JREQ_BAD, rule);
    expect_body(BODY("{\"label\":\"x\\U0000\"}"), JREQ_BAD, "invalid JSON"); /* not an escape */
    expect_body(BODY("{\"params\":{\"label\":\"a\\u0000\"}}"), JREQ_BAD, rule);
    expect_body(BODY("{\"paths\":[\"/a\",\"/b\\u0000c\"]}"), JREQ_BAD,
                "paths must not contain a NUL character");
    expect_body(BODY("{\"a\":{\"b\":1},\"note\":\"x\\u0000\"}"), JREQ_BAD,
                "note must not contain a NUL character");
    expect_body(BODY("{\"trigger\":{\"type\":\"calendar\",\"at\":\"\\u0000\"}}"),
                JREQ_BAD, "at must not contain a NUL character");
    expect_body(BODY("{\"a\\u0000b\":1}"), JREQ_BAD,
                "a member name must not contain a NUL character");
    expect_body(BODY("[\"\\u0000\"]"), JREQ_BAD,
                "a string must not contain a NUL character");
    /* A raw NUL inside a string is the same thing, unescaped. */
    expect_body(BODY("{\"label\":\"a\0b\"}"), JREQ_BAD, rule);
    /* An escaped backslash followed by u0000 is text, not a NUL. */
    expect_body(BODY("{\"label\":\"a\\\\u0000\"}"), JREQ_OK, NULL);
    /* A NUL among other escapes is found; other escapes pass. */
    expect_body(BODY("{\"label\":\"\\u0001\\u0100\\\"\\u00000\"}"), JREQ_BAD, rule);
    expect_body(BODY("{\"label\":\"\\u0010\\\"q\"}"), JREQ_OK, NULL);
    /* Malformed JSON is still the parser's to refuse. */
    expect_body(BODY("{\"label\":"), JREQ_BAD, "invalid JSON");
    expect_body(BODY("{\"a\":1}x"), JREQ_BAD, "invalid JSON");
    expect_body(BODY("{\"a\":1}\0"), JREQ_BAD, "invalid JSON");
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
        cmocka_unit_test(test_request_flags),
        cmocka_unit_test(test_request_types),
        cmocka_unit_test(test_request_strings),
        cmocka_unit_test(test_request_bools),
        cmocka_unit_test(test_request_u32),
        cmocka_unit_test(test_request_wide_ints),
        cmocka_unit_test(test_request_string_lists),
        cmocka_unit_test(test_query_numbers),
        cmocka_unit_test(test_body_nul),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
