#define _POSIX_C_SOURCE 200809L

/*
 * Trigger computation.
 *
 * Calendar arithmetic is where schedulers go wrong, and the failures are
 * quiet: a backup that silently stops running for a month, or runs twice on
 * the day the clocks change. The suite pins TZ to zones with known DST rules
 * so those cases are reproducible rather than dependent on where the tests
 * happen to run.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <cmocka.h>

#include "trigger.h"

static void set_tz(const char *tz)
{
    setenv("TZ", tz, 1);
    tzset();
}

static time_t at_local(int y, int mo, int d, int h, int mi)
{
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year = y - 1900; tm.tm_mon = mo - 1; tm.tm_mday = d;
    tm.tm_hour = h; tm.tm_min = mi; tm.tm_isdst = -1;
    return mktime(&tm);
}

static void fmt(time_t t, char *buf, size_t n)
{
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(buf, n, "%Y-%m-%d %H:%M %Z", &tm);
}

static void assert_fires_at(time_t got, int y, int mo, int d, int h, int mi)
{
    struct tm tm;
    localtime_r(&got, &tm);
    char buf[64];
    fmt(got, buf, sizeof(buf));
    assert_int_equal(tm.tm_year + 1900, y);
    assert_int_equal(tm.tm_mon + 1, mo);
    assert_int_equal(tm.tm_mday, d);
    assert_int_equal(tm.tm_hour, h);
    assert_int_equal(tm.tm_min, mi);
}

/* ---------- parsing ---------- */

static void test_parse_interval(void **state)
{
    (void)state;
    trigger_t t;
    assert_int_equal(trigger_parse("{\"type\":\"interval\",\"seconds\":14400}", &t), OK);
    assert_int_equal(t.type, TRIGGER_INTERVAL);
    assert_int_equal(t.seconds, 14400);
}

static void test_parse_calendar_forms(void **state)
{
    (void)state;
    trigger_t t;

    assert_int_equal(trigger_parse(
        "{\"type\":\"calendar\",\"every\":\"day\",\"at\":\"02:00\"}", &t), OK);
    assert_int_equal(t.every, CAL_DAY);
    assert_int_equal(t.at_hour, 2);

    assert_int_equal(trigger_parse(
        "{\"type\":\"calendar\",\"every\":\"week\",\"on\":\"sunday\",\"at\":\"03:30\"}", &t), OK);
    assert_int_equal(t.every, CAL_WEEK);
    assert_int_equal(t.on_weekday, 0);
    assert_int_equal(t.at_min, 30);

    assert_int_equal(trigger_parse(
        "{\"type\":\"calendar\",\"every\":\"month\",\"on_day\":1,\"at\":\"04:00\"}", &t), OK);
    assert_int_equal(t.every, CAL_MONTH);
    assert_int_equal(t.on_day, 1);

    assert_int_equal(trigger_parse(
        "{\"type\":\"calendar\",\"every\":\"year\",\"on_month\":1,\"on_day\":1,\"at\":\"05:00\"}", &t), OK);
    assert_int_equal(t.every, CAL_YEAR);
    assert_int_equal(t.on_month, 1);
}

static void test_parse_rejects_nonsense(void **state)
{
    (void)state;
    trigger_t t;
    assert_int_not_equal(trigger_parse("not json", &t), OK);
    assert_int_not_equal(trigger_parse("{\"type\":\"cron\"}", &t), OK);
    assert_int_not_equal(trigger_parse("{\"type\":\"interval\"}", &t), OK);
    assert_int_not_equal(trigger_parse(
        "{\"type\":\"calendar\",\"every\":\"day\",\"at\":\"25:00\"}", &t), OK);
    assert_int_not_equal(trigger_parse(
        "{\"type\":\"calendar\",\"every\":\"week\",\"on\":\"caturday\",\"at\":\"01:00\"}", &t), OK);
}

/* ---------- interval ---------- */

/*
 * Anchored to the last fire, not to wall-clock multiples. A four-hour backup
 * that runs long must not become due the instant it finishes.
 */
static void test_interval_anchors_to_last_fire(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_INTERVAL, .seconds = 3600 };

    time_t last = at_local(2024, 6, 1, 10, 0);
    time_t now  = at_local(2024, 6, 1, 10, 30);
    assert_int_equal(trigger_next_fire(&t, now, last), last + 3600);
}

/* A daemon down for days must produce one future instant, not a backlog. */
static void test_interval_skips_past_missed_windows(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_INTERVAL, .seconds = 3600 };

    time_t last = at_local(2024, 6, 1, 10, 0);
    time_t now  = at_local(2024, 6, 4, 10, 0);   /* three days later */
    time_t next = trigger_next_fire(&t, now, last);
    assert_true(next > now);
    assert_true(next <= now + 3600);
}

static void test_interval_without_history(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_INTERVAL, .seconds = 900 };
    time_t now = at_local(2024, 6, 1, 10, 0);
    assert_int_equal(trigger_next_fire(&t, now, 0), now + 900);
}

/* ---------- daily ---------- */

static void test_daily_same_day_then_next(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_DAY, .at_hour = 2 };

    /* Before today's time: today. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 1, 1, 0), 0),
                    2024, 6, 1, 2, 0);
    /* After it: tomorrow. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 1, 3, 0), 0),
                    2024, 6, 2, 2, 0);
}

static void test_daily_rolls_over_month_and_year(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_DAY, .at_hour = 2 };

    assert_fires_at(trigger_next_fire(&t, at_local(2024, 1, 31, 3, 0), 0),
                    2024, 2, 1, 2, 0);
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 12, 31, 3, 0), 0),
                    2025, 1, 1, 2, 0);
}

/* ---------- DST ---------- */

/*
 * Spring forward: US Eastern jumps 02:00 to 03:00 on 2024-03-10, so 02:30
 * does not exist that day. The trigger must land on a real instant at or
 * after the requested time rather than skipping the day.
 */
static void test_dst_spring_forward_lands_on_a_real_instant(void **state)
{
    (void)state;
    set_tz("America/New_York");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_DAY,
                    .at_hour = 2, .at_min = 30 };

    time_t before = at_local(2024, 3, 10, 0, 30);
    time_t next = trigger_next_fire(&t, before, 0);
    assert_true(next > before);

    struct tm tm;
    localtime_r(&next, &tm);
    /* Whatever the zone resolves it to, it must be a genuine instant on the
     * requested day and not silently a day later. */
    assert_int_equal(tm.tm_mday, 10);
    assert_true(tm.tm_hour >= 2);
}

/*
 * Fall back: 01:30 occurs twice on 2024-11-03 in US Eastern. The schedule
 * must fire once for that calendar day, not once per occurrence.
 */
static void test_dst_fall_back_fires_once_per_day(void **state)
{
    (void)state;
    set_tz("America/New_York");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_DAY,
                    .at_hour = 1, .at_min = 30 };

    time_t start = at_local(2024, 11, 3, 0, 30);
    time_t first = trigger_next_fire(&t, start, 0);
    time_t second = trigger_next_fire(&t, first, 0);

    struct tm a, b;
    localtime_r(&first, &a);
    localtime_r(&second, &b);
    assert_int_equal(a.tm_mday, 3);
    /* The next fire is the following day, not the repeated hour. */
    assert_int_equal(b.tm_mday, 4);
}

/* ---------- weekly ---------- */

static void test_weekly_finds_the_weekday(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_WEEK,
                    .on_weekday = 0, .at_hour = 3 };

    /* 2024-06-05 is a Wednesday; the next Sunday is the 9th. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 5, 12, 0), 0),
                    2024, 6, 9, 3, 0);
}

static void test_weekly_same_day_before_time(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_WEEK,
                    .on_weekday = 0, .at_hour = 3 };

    /* Sunday 2024-06-09 at 01:00: fires later the same day. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 9, 1, 0), 0),
                    2024, 6, 9, 3, 0);
    /* Same Sunday at 04:00: next Sunday. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 9, 4, 0), 0),
                    2024, 6, 16, 3, 0);
}

/* ---------- monthly ---------- */

/*
 * "The 31st of every month" has to mean something in February. Clamping to
 * the last day is the least surprising reading: the user asked for the end of
 * the month, so February gets the end of February rather than being skipped
 * or spilling into March.
 */
static void test_monthly_clamps_to_end_of_february(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_MONTH,
                    .on_day = 31, .at_hour = 4 };

    /* 2024 is a leap year: 29 February. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 2, 1, 0, 0), 0),
                    2024, 2, 29, 4, 0);
    /* 2023 is not. */
    assert_fires_at(trigger_next_fire(&t, at_local(2023, 2, 1, 0, 0), 0),
                    2023, 2, 28, 4, 0);
    /* 30-day months clamp too. */
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 4, 1, 0, 0), 0),
                    2024, 4, 30, 4, 0);
}

static void test_monthly_advances_to_next_month(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_MONTH,
                    .on_day = 1, .at_hour = 4 };

    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 1, 5, 0), 0),
                    2024, 7, 1, 4, 0);
    assert_fires_at(trigger_next_fire(&t, at_local(2024, 12, 1, 5, 0), 0),
                    2025, 1, 1, 4, 0);
}

/* ---------- yearly ---------- */

static void test_yearly(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_YEAR,
                    .on_month = 1, .on_day = 1, .at_hour = 5 };

    assert_fires_at(trigger_next_fire(&t, at_local(2024, 6, 1, 0, 0), 0),
                    2025, 1, 1, 5, 0);
}

/* 29 February exists only in leap years; a yearly trigger set for it must
 * still fire every year rather than only every fourth. */
static void test_yearly_leap_day_clamps(void **state)
{
    (void)state;
    set_tz("UTC");
    trigger_t t = { .type = TRIGGER_CALENDAR, .every = CAL_YEAR,
                    .on_month = 2, .on_day = 29, .at_hour = 5 };

    assert_fires_at(trigger_next_fire(&t, at_local(2024, 1, 1, 0, 0), 0),
                    2024, 2, 29, 5, 0);
    assert_fires_at(trigger_next_fire(&t, at_local(2025, 1, 1, 0, 0), 0),
                    2025, 2, 28, 5, 0);
}

/* Every trigger must return a strictly future instant, or the scheduler
 * spins firing the same schedule forever. */
static void test_next_fire_is_always_in_the_future(void **state)
{
    (void)state;
    set_tz("America/New_York");
    time_t probes[] = {
        at_local(2024, 3, 10, 2, 0),   /* spring forward */
        at_local(2024, 11, 3, 1, 0),   /* fall back */
        at_local(2024, 2, 29, 0, 0),   /* leap day */
        at_local(2024, 12, 31, 23, 59),
    };
    trigger_t triggers[] = {
        { .type = TRIGGER_CALENDAR, .every = CAL_DAY,   .at_hour = 2, .at_min = 30 },
        { .type = TRIGGER_CALENDAR, .every = CAL_WEEK,  .on_weekday = 3, .at_hour = 1 },
        { .type = TRIGGER_CALENDAR, .every = CAL_MONTH, .on_day = 31, .at_hour = 4 },
        { .type = TRIGGER_CALENDAR, .every = CAL_YEAR,  .on_month = 2, .on_day = 29, .at_hour = 5 },
        { .type = TRIGGER_INTERVAL, .seconds = 3600 },
    };
    for (size_t p = 0; p < sizeof(probes)/sizeof(probes[0]); p++)
        for (size_t t = 0; t < sizeof(triggers)/sizeof(triggers[0]); t++) {
            time_t next = trigger_next_fire(&triggers[t], probes[p], 0);
            assert_true(next > probes[p]);
        }
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_parse_interval),
        cmocka_unit_test(test_parse_calendar_forms),
        cmocka_unit_test(test_parse_rejects_nonsense),
        cmocka_unit_test(test_interval_anchors_to_last_fire),
        cmocka_unit_test(test_interval_skips_past_missed_windows),
        cmocka_unit_test(test_interval_without_history),
        cmocka_unit_test(test_daily_same_day_then_next),
        cmocka_unit_test(test_daily_rolls_over_month_and_year),
        cmocka_unit_test(test_dst_spring_forward_lands_on_a_real_instant),
        cmocka_unit_test(test_dst_fall_back_fires_once_per_day),
        cmocka_unit_test(test_weekly_finds_the_weekday),
        cmocka_unit_test(test_weekly_same_day_before_time),
        cmocka_unit_test(test_monthly_clamps_to_end_of_february),
        cmocka_unit_test(test_monthly_advances_to_next_month),
        cmocka_unit_test(test_yearly),
        cmocka_unit_test(test_yearly_leap_day_clamps),
        cmocka_unit_test(test_next_fire_is_always_in_the_future),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
