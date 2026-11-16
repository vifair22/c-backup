/*
 * The capped hook log (src/helper/hooklog.c): under the cap it is the
 * output verbatim; over it the head and tail survive around one line
 * saying how much was dropped, whatever the write sizes.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "hooklog.h"

static void test_under_cap(void **state)
{
    (void)state;
    hooklog_t l;
    memset(&l, 0, sizeof(l));
    l.cap = 64;
    assert_int_equal(hooklog_append(&l, "hello ", 6), 0);
    assert_int_equal(hooklog_append(&l, "world", 5), 0);
    size_t n = 0;
    char *s = hooklog_render(&l, &n);
    assert_string_equal(s, "hello world");
    assert_int_equal(n, 11);
    free(s);
    hooklog_free(&l);
}

static void test_head_and_tail(void **state)
{
    (void)state;
    /* 1000 bytes "0123456789..." in writes of every size from 1 to 13. */
    char all[1000];
    for (size_t i = 0; i < sizeof(all); i++) all[i] = (char)('0' + i % 10);
    for (size_t step = 1; step <= 13; step++) {
        hooklog_t l;
        memset(&l, 0, sizeof(l));
        l.cap = 100;
        for (size_t o = 0; o < sizeof(all); o += step)
            assert_int_equal(hooklog_append(&l, all + o, o + step > sizeof(all) ? sizeof(all) - o : step), 0);
        size_t n = 0;
        char *s = hooklog_render(&l, &n);
        assert_memory_equal(s, all, 50);
        assert_memory_equal(s + n - 50, all + 950, 50);
        assert_non_null(strstr(s, "900 bytes of output dropped"));
        free(s);
        hooklog_free(&l);
    }
    /* One write bigger than the whole cap. */
    hooklog_t l;
    memset(&l, 0, sizeof(l));
    l.cap = 100;
    assert_int_equal(hooklog_append(&l, all, sizeof(all)), 0);
    size_t n = 0;
    char *s = hooklog_render(&l, &n);
    assert_memory_equal(s, all, 50);
    assert_memory_equal(s + n - 50, all + 950, 50);
    free(s);
    hooklog_free(&l);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_under_cap),
        cmocka_unit_test(test_head_and_tail),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
