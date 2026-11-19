#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

/*
 * The daemon's stop starts when it is asked for (B225), and a part of it
 * the daemon does not control is bounded (B226).
 *
 * The main loop slept a second between looks at a flag, so a stop waited
 * up to a second before it began. A stop request now wakes the wait. The
 * Pushover drain at the end of a stop could hold it 15 s per message with
 * Pushover unreachable; run_within gives up on a function at a deadline.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "shutdown_wait.h"

static uint64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

static void sleep_ms(unsigned ms)
{
    struct timespec ts = { (time_t)(ms / 1000u), (long)(ms % 1000u) * 1000000L };
    nanosleep(&ts, NULL);
}

static void *request_after_100ms(void *arg)
{
    (void)arg;
    sleep_ms(100);
    stop_request();
    return NULL;
}

/* A wait with no request runs its full length and says no stop. */
static void test_wait_without_request_times_out(void **state)
{
    (void)state;
    stop_reset_for_test();
    uint64_t t0 = now_ms();
    assert_int_equal(stop_wait(150), 0);
    assert_true(now_ms() - t0 >= 140);
    assert_int_equal(stop_requested(), 0);
}

/* The heart of B225: a request ends a long wait at once, not at the end of
 * a polling interval. The wait is 10 s, the request comes at 100 ms. */
static void test_request_wakes_the_wait(void **state)
{
    (void)state;
    stop_reset_for_test();
    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, request_after_100ms, NULL), 0);
    uint64_t t0 = now_ms();
    assert_int_equal(stop_wait(10000), 1);
    uint64_t took = now_ms() - t0;
    pthread_join(t, NULL);
    assert_true(took < 2000);
    assert_int_equal(stop_requested(), 1);
}

/* A request made before anyone waits is not lost: a signal that arrives
 * during start-up still stops the loop on its first wait. */
static void test_request_before_wait(void **state)
{
    (void)state;
    stop_reset_for_test();
    stop_request();
    uint64_t t0 = now_ms();
    assert_int_equal(stop_wait(10000), 1);
    assert_true(now_ms() - t0 < 1000);
}

static atomic_int g_calls;
static atomic_int g_slow_done;

static void quick(void) { atomic_fetch_add(&g_calls, 1); }

static void slow(void)
{
    sleep_ms(1500);
    atomic_store(&g_slow_done, 1);
}

static void test_run_within_in_time(void **state)
{
    (void)state;
    atomic_store(&g_calls, 0);
    assert_int_equal(run_within(quick, 5000), 0);
    assert_int_equal(atomic_load(&g_calls), 1);
}

/* B226: a function that outlives its budget is left running and the caller
 * gets control back at the budget, not when the function returns. */
static void test_run_within_gives_up_at_the_deadline(void **state)
{
    (void)state;
    atomic_store(&g_slow_done, 0);
    uint64_t t0 = now_ms();
    assert_int_equal(run_within(slow, 100), -1);
    uint64_t took = now_ms() - t0;
    assert_true(took >= 90);
    assert_true(took < 1000);
    assert_int_equal(atomic_load(&g_slow_done), 0);
    /* It goes on, detached, and finishes on its own. */
    for (int i = 0; i < 100 && !atomic_load(&g_slow_done); i++) sleep_ms(50);
    assert_int_equal(atomic_load(&g_slow_done), 1);
    sleep_ms(50);   /* let its thread drop the shared record */
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_wait_without_request_times_out),
        cmocka_unit_test(test_request_wakes_the_wait),
        cmocka_unit_test(test_request_before_wait),
        cmocka_unit_test(test_run_within_in_time),
        cmocka_unit_test(test_run_within_gives_up_at_the_deadline),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
