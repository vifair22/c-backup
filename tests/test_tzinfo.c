/*
 * tzinfo: naming the zone calendar schedules run in (UAT lows).
 *
 * Every case runs against a fake zoneinfo tree and fake /etc files under the
 * scratch directory, so nothing depends on the host's zone. The process TZ
 * is pinned to UTC, which is what the abbreviation fallback reports.
 */
#define _POSIX_C_SOURCE 200809L

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "test_tmp.h"
#include "tzinfo.h"

#define ROOT  TEST_TMP "/c_backup_test_tzinfo"
#define ZONES ROOT "/zoneinfo"
#define LT    ROOT "/localtime"
#define TZF   ROOT "/timezone"

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static int setup(void **state)
{
    (void)state;
    setenv("TZ", "UTC", 1);
    tzset();
    if (system("rm -rf '" ROOT "'") != 0) return -1;
    if (mkdir(ROOT, 0755) || mkdir(ZONES, 0755) || mkdir(ZONES "/America", 0755) ||
        mkdir(ZONES "/Etc", 0755))
        return -1;
    put(ZONES "/America/Chicago", "TZif chicago bytes");
    put(ZONES "/Etc/UTC", "TZif utc bytes");
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    return system("rm -rf '" ROOT "'") == 0 ? 0 : -1;
}

static void name_is(const char *tz, const char *want)
{
    char out[128];
    tz_name_at(tz, LT, TZF, ZONES, out, sizeof(out));
    assert_string_equal(out, want);
}

/* A Docker bind mount: /etc/localtime is Chicago's file, /etc/timezone
 * still says what the image was built with. It used to report Etc/UTC. */
static void test_bind_mounted_localtime_is_not_named_from_timezone(void **state)
{
    (void)state;
    put(LT, "TZif chicago bytes");
    put(TZF, "Etc/UTC\n");
    /* No name to be had: the abbreviation localtime reports. */
    name_is(NULL, "UTC");

    /* /etc/timezone agreeing with the mounted file is a name. */
    put(TZF, "America/Chicago\n");
    name_is(NULL, "America/Chicago");
}

static void test_symlinked_localtime_names_its_target(void **state)
{
    (void)state;
    assert_int_equal(symlink(ZONES "/America/Chicago", LT), 0);
    put(TZF, "Etc/UTC\n");
    name_is(NULL, "America/Chicago");
}

static void test_no_localtime_uses_timezone(void **state)
{
    (void)state;
    put(TZF, "America/Chicago\n");
    name_is(NULL, "America/Chicago");
}

/* An unresolvable TZ is UTC to the C library, and is reported as UTC
 * rather than as the string given. */
static void test_tz_env(void **state)
{
    (void)state;
    name_is("America/Chicago", "America/Chicago");
    name_is(":America/Chicago", "America/Chicago");
    name_is("America/Chicgo", "UTC");
    name_is("Nowhere", "UTC");
    name_is("CST6CDT", "CST6CDT");
}

static void test_tz_known(void **state)
{
    (void)state;
    assert_true(tz_known(NULL, ZONES));
    assert_true(tz_known("", ZONES));
    assert_true(tz_known("America/Chicago", ZONES));
    assert_true(tz_known(":America/Chicago", ZONES));
    assert_true(tz_known(ZONES "/Etc/UTC", ZONES));
    assert_true(tz_known("UTC", ZONES));
    assert_true(tz_known("GMT", ZONES));
    assert_true(tz_known("CST6CDT", ZONES));
    assert_true(tz_known("EST5EDT,M3.2.0,M11.1.0", ZONES));
    assert_true(tz_known("<+0530>-5:30", ZONES));
    assert_false(tz_known("America/Chicgo", ZONES));
    assert_false(tz_known("Chicago", ZONES));
    assert_false(tz_known("America", ZONES));          /* a directory */
    assert_false(tz_known("../zoneinfo/America/Chicago", ZONES));
    assert_false(tz_known(":CST6CDT", ZONES));         /* ':' means a file */
    assert_false(tz_known("AB1", ZONES));              /* name too short */
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_bind_mounted_localtime_is_not_named_from_timezone, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlinked_localtime_names_its_target, setup, teardown),
        cmocka_unit_test_setup_teardown(test_no_localtime_uses_timezone, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tz_env, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tz_known, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
