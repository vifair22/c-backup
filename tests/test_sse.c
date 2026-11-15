#define _POSIX_C_SOURCE 200809L

/*
 * Server-sent events decoder (src/cli/sse.c), behind `jobs watch`.
 *
 * The CLI e2e suite proves `jobs watch` prints bare JSON lines against the
 * daemon's own framing. This covers what that fixture never sends: CRLF line
 * ends, comments, several data lines in one event, a frame split across reads
 * at every byte, an event the stream cut off, and an oversized line.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "sse.h"

typedef struct {
    char   out[4096];   /* "<event>|<data>\n" per event */
    size_t len;
    int    calls;
    int    stop_after;  /* refuse the event after this many; 0 never */
} rec_t;

static status_t record(void *ud, const char *event, const char *data)
{
    rec_t *r = ud;
    if (r->stop_after && r->calls >= r->stop_after)
        return set_error(ERR_IO, "stopped");
    r->calls++;
    int n = snprintf(r->out + r->len, sizeof(r->out) - r->len, "%s|%s\n", event, data);
    assert_true(n > 0 && (size_t)n < sizeof(r->out) - r->len);
    r->len += (size_t)n;
    return OK;
}

static void feed_all(const char *s, rec_t *r, size_t step)
{
    sse_parser_t p;
    sse_init(&p, record, r);
    size_t n = strlen(s);
    for (size_t i = 0; i < n; i += step) {
        size_t take = n - i < step ? n - i : step;
        assert_int_equal(sse_feed(&p, s + i, take), OK);
    }
    sse_free(&p);
}

static void test_daemon_frames(void **state)
{
    (void)state;
    rec_t r = { 0 };
    feed_all("data: {\"id\":7,\"state\":\"running\"}\n\n"
             "data: {\"id\":7,\"state\":\"completed\"}\n\n", &r, 4096);
    assert_int_equal(r.calls, 2);
    assert_string_equal(r.out, "|{\"id\":7,\"state\":\"running\"}\n"
                               "|{\"id\":7,\"state\":\"completed\"}\n");
}

static void test_named_events(void **state)
{
    (void)state;
    rec_t r = { 0 };
    feed_all("event: error\ndata: {\"error\":\"no such job\"}\n\n"
             "event: timeout\ndata: {}\n\n"
             "data: plain\n\n", &r, 4096);
    /* The name belongs to one event only. */
    assert_string_equal(r.out, "error|{\"error\":\"no such job\"}\n"
                               "timeout|{}\n"
                               "|plain\n");
}

static void test_crlf_comments_and_fields(void **state)
{
    (void)state;
    rec_t r = { 0 };
    feed_all(": keepalive\r\n"
             "id: 3\r\n"
             "retry: 1000\r\n"
             "data:nospace\r\n"
             "\r\n"
             "data\r\n"
             "\r\n", &r, 4096);
    /* No space after the colon is still a value; a bare "data" field is an
     * empty one, and still an event. */
    assert_int_equal(r.calls, 2);
    assert_string_equal(r.out, "|nospace\n|\n");
}

static void test_multiline_data(void **state)
{
    (void)state;
    rec_t r = { 0 };
    feed_all("data: a\ndata: b\ndata:  c\n\n", &r, 4096);
    /* Only one leading space is the separator. */
    assert_string_equal(r.out, "|a\nb\n c\n");
}

static void test_split_at_every_byte(void **state)
{
    (void)state;
    const char *s = "event: error\r\ndata: {\"x\":1}\r\n\r\ndata: {\"y\":2}\n\n";
    rec_t whole = { 0 };
    feed_all(s, &whole, 4096);
    for (size_t step = 1; step < 8; step++) {
        rec_t r = { 0 };
        feed_all(s, &r, step);
        assert_string_equal(r.out, whole.out);
    }
}

static void test_no_event_without_data(void **state)
{
    (void)state;
    rec_t r = { 0 };
    feed_all("\n\nevent: x\n\n: c\n\n", &r, 4096);
    assert_int_equal(r.calls, 0);
}

static void test_truncated_event_discarded(void **state)
{
    (void)state;
    rec_t r = { 0 };
    feed_all("data: done\n\ndata: cut off", &r, 4096);
    assert_int_equal(r.calls, 1);
    assert_string_equal(r.out, "|done\n");
}

static void test_callback_stops_feed(void **state)
{
    (void)state;
    rec_t r = { .stop_after = 1 };
    sse_parser_t p;
    sse_init(&p, record, &r);
    const char *s = "data: 1\n\ndata: 2\n\ndata: 3\n\n";
    assert_int_equal(sse_feed(&p, s, strlen(s)), ERR_IO);
    assert_string_equal(err_msg(), "stopped");
    assert_int_equal(r.calls, 1);
    sse_free(&p);
}

static void test_oversized_line_refused(void **state)
{
    (void)state;
    rec_t r = { 0 };
    sse_parser_t p;
    sse_init(&p, record, &r);
    char *big = malloc(SSE_MAX_LINE + 16);
    assert_non_null(big);
    memset(big, 'x', SSE_MAX_LINE + 16);
    assert_int_equal(sse_feed(&p, big, SSE_MAX_LINE + 16), ERR_TOO_LARGE);
    free(big);
    sse_free(&p);
    assert_int_equal(r.calls, 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_daemon_frames),
        cmocka_unit_test(test_named_events),
        cmocka_unit_test(test_crlf_comments_and_fields),
        cmocka_unit_test(test_multiline_data),
        cmocka_unit_test(test_split_at_every_byte),
        cmocka_unit_test(test_no_event_without_data),
        cmocka_unit_test(test_truncated_event_discarded),
        cmocka_unit_test(test_callback_stops_feed),
        cmocka_unit_test(test_oversized_line_refused),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
