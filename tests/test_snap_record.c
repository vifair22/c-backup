#define _POSIX_C_SOURCE 200809L

/*
 * The snapshot record and the label rules (OWN-33).
 *
 * The record was formatted into a 512-byte stack buffer and written for
 * snprintf's return value, so a long label put stack memory into the
 * repository; and the label was not escaped, so a quote made a record that
 * no reindex could read. These pin the rules every entry point now shares
 * and the encoder both writers use.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <cmocka.h>
#include <cutils/json.h>

#include "snap_record.h"

#define HEX64 "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

static void fill(char *buf, size_t n, char c)
{
    memset(buf, c, n);
    buf[n] = '\0';
}

/* ---------- labels ---------- */

static void test_label_length_limit(void **state)
{
    (void)state;
    char buf[SNAPSHOT_LABEL_MAX + 2];
    fill(buf, SNAPSHOT_LABEL_MAX, 'a');
    assert_int_equal(snapshot_label_check(buf), 0);
    fill(buf, SNAPSHOT_LABEL_MAX + 1, 'a');
    assert_int_equal(snapshot_label_check(buf), -1);

    char big[1001];
    fill(big, 1000, 'x');
    assert_int_equal(snapshot_label_check(big), -1);

    /* The limit is bytes, not characters: 128 two-byte characters fit,
     * one more does not. */
    char wide[2 * 129 + 1];
    for (int i = 0; i < 129; i++) { wide[2 * i] = '\xc3'; wide[2 * i + 1] = '\xa9'; }
    wide[2 * 128] = '\0';
    assert_int_equal(snapshot_label_check(wide), 0);
    wide[2 * 128] = '\xc3';
    wide[2 * 129] = '\0';
    assert_int_equal(snapshot_label_check(wide), -1);
}

static void test_label_accepts_printable_text(void **state)
{
    (void)state;
    assert_int_equal(snapshot_label_check(NULL), 0);
    assert_int_equal(snapshot_label_check(""), 0);
    assert_int_equal(snapshot_label_check("nightly"), 0);
    assert_int_equal(snapshot_label_check("say \"hi\" C:\\dir {}[]:,"), 0);
    /* U+00E9, U+2603, U+1F600: two, three and four bytes. */
    assert_int_equal(snapshot_label_check("caf\xc3\xa9 \xe2\x98\x83 \xf0\x9f\x98\x80"), 0);
    /* The highest scalar value. */
    assert_int_equal(snapshot_label_check("\xf4\x8f\xbf\xbf"), 0);
}

static void test_label_refuses_control_characters(void **state)
{
    (void)state;
    assert_int_equal(snapshot_label_check("a\nb"), -1);
    assert_int_equal(snapshot_label_check("a\tb"), -1);
    assert_int_equal(snapshot_label_check("a\rb"), -1);
    assert_int_equal(snapshot_label_check("\x1b[31mred"), -1);
    assert_int_equal(snapshot_label_check("a\x7f"), -1);
    /* C1: U+0085 (NEL) and U+009B (CSI), as UTF-8. */
    assert_int_equal(snapshot_label_check("a\xc2\x85"), -1);
    assert_int_equal(snapshot_label_check("a\xc2\x9b"), -1);
    /* U+00A0 is the first character past C1, and printable. */
    assert_int_equal(snapshot_label_check("a\xc2\xa0"), 0);
}

static void test_label_refuses_malformed_utf8(void **state)
{
    (void)state;
    assert_int_equal(snapshot_label_check("\xff"), -1);           /* never valid */
    assert_int_equal(snapshot_label_check("\x80"), -1);           /* lone continuation */
    assert_int_equal(snapshot_label_check("\xc3"), -1);           /* truncated */
    assert_int_equal(snapshot_label_check("\xe2\x98"), -1);       /* truncated */
    assert_int_equal(snapshot_label_check("\xc0\xaf"), -1);       /* overlong '/' */
    assert_int_equal(snapshot_label_check("\xe0\x80\xaf"), -1);   /* overlong */
    assert_int_equal(snapshot_label_check("\xed\xa0\x80"), -1);   /* surrogate */
    assert_int_equal(snapshot_label_check("\xf4\x90\x80\x80"), -1); /* past U+10FFFF */
    assert_int_equal(snapshot_label_check("\xc3\x28"), -1);       /* bad continuation */
}

/* ---------- the record ---------- */

static char *label_of(const char *rec, size_t len)
{
    cutils_json_req_t *req = NULL;
    assert_int_equal(json_req_parse(rec, len, &req), 0);
    char *label = NULL;
    assert_int_equal(json_req_get_str(req, "label", &label), 0);
    json_req_free(req);
    return label;
}

static void test_record_shape_is_unchanged_for_a_plain_label(void **state)
{
    (void)state;
    size_t len = 0;
    char *rec = snapshot_record_encode(7, 1700000000, HEX64, "nightly", &len);
    assert_non_null(rec);
    const char *want = "{\"id\":7,\"ts\":1700000000,\"root\":\"" HEX64
                       "\",\"label\":\"nightly\"}\n";
    assert_int_equal(len, strlen(want));
    assert_string_equal(rec, want);
    free(rec);

    rec = snapshot_record_encode(1, -86400, HEX64, NULL, &len);
    assert_non_null(rec);
    assert_string_equal(rec, "{\"id\":1,\"ts\":-86400,\"root\":\"" HEX64
                             "\",\"label\":\"\"}\n");
    free(rec);
}

static void test_record_escapes_and_round_trips(void **state)
{
    (void)state;
    const char *label = "say \"hi\" C:\\dir\\ caf\xc3\xa9 \xe2\x98\x83 \xf0\x9f\x98\x80";
    size_t len = 0;
    char *rec = snapshot_record_encode(2, 1, HEX64, label, &len);
    assert_non_null(rec);
    assert_non_null(strstr(rec, "say \\\"hi\\\" C:\\\\dir\\\\ "));
    char *back = label_of(rec, len);
    assert_string_equal(back, label);
    free(back);
    free(rec);
}

/* A label stored before the rules, carried by replication: still valid
 * JSON, with control characters escaped and a bad byte replaced. */
static void test_record_is_valid_json_for_any_label(void **state)
{
    (void)state;
    size_t len = 0;
    char *rec = snapshot_record_encode(3, 1, HEX64, "a\nb\x01\x7f\xff" "c", &len);
    assert_non_null(rec);
    assert_non_null(strstr(rec, "\"label\":\"a\\nb\\u0001\\u007f\\ufffdc\"}\n"));
    char *back = label_of(rec, len);
    assert_string_equal(back, "a\nb\x01\x7f\xef\xbf\xbd" "c");
    free(back);
    free(rec);
}

/*
 * At the limit, with the worst a valid label can do (every byte a quote,
 * each escaped to two), the record is exactly what it should be: one
 * newline-terminated JSON object, nothing after it. This is the size that
 * used to overrun the 512-byte buffer.
 */
static void test_record_at_the_label_limit_is_exact(void **state)
{
    (void)state;
    char label[SNAPSHOT_LABEL_MAX + 1];
    fill(label, SNAPSHOT_LABEL_MAX, '"');
    assert_int_equal(snapshot_label_check(label), 0);

    size_t len = 0;
    char *rec = snapshot_record_encode(9223372036854775807LL, -9223372036854775807LL - 1,
                                       HEX64, label, &len);
    assert_non_null(rec);
    size_t want = strlen("{\"id\":9223372036854775807,\"ts\":-9223372036854775808,"
                         "\"root\":\"" HEX64 "\",\"label\":\"\"}\n")
                  + 2 * SNAPSHOT_LABEL_MAX;
    assert_int_equal(len, want);
    assert_int_equal(strlen(rec), len);
    assert_true(len > 512);
    assert_int_equal(rec[len - 1], '\n');
    char *back = label_of(rec, len);
    assert_string_equal(back, label);
    free(back);
    free(rec);
}

static void test_record_too_large_is_refused(void **state)
{
    (void)state;
    /* Every byte bad, so six out per byte in: well past the cap. */
    size_t n = SNAPSHOT_RECORD_MAX / 4;
    char *label = malloc(n + 1);
    assert_non_null(label);
    memset(label, 0xff, n);
    label[n] = '\0';
    size_t len = 0;
    errno = 0;
    assert_null(snapshot_record_encode(1, 1, HEX64, label, &len));
    assert_int_equal(errno, E2BIG);
    free(label);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_label_length_limit),
        cmocka_unit_test(test_label_accepts_printable_text),
        cmocka_unit_test(test_label_refuses_control_characters),
        cmocka_unit_test(test_label_refuses_malformed_utf8),
        cmocka_unit_test(test_record_shape_is_unchanged_for_a_plain_label),
        cmocka_unit_test(test_record_escapes_and_round_trips),
        cmocka_unit_test(test_record_is_valid_json_for_any_label),
        cmocka_unit_test(test_record_at_the_label_limit_is_exact),
        cmocka_unit_test(test_record_too_large_is_refused),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
