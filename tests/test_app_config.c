#define _POSIX_C_SOURCE 200809L

/*
 * What the daemon refuses in config.yaml beyond c-utils' type checks
 * (app_config_check, UAT B081). Each case here used to start the daemon on
 * something other than what the file said; the API e2e covers the same
 * through the real binary, this pins the check function itself.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/config.h>
#include <cutils/error.h>

#include "app_config.h"
#include "test_tmp.h"

#define TEST_CFG TEST_TMP "/c_backup_test_app_config.yaml"

static int load(const char *body)
{
    FILE *f = fopen(TEST_CFG, "w");
    assert_non_null(f);
    fprintf(f, "db:\n  path: /tmp/x.db\n%s", body);
    fclose(f);
    config_init_opts_t o = { .validate = CFG_VALIDATE_STRICT,
                             .check = app_config_check };
    cutils_config_t *cfg = NULL;
    int rc = config_init_opts(&cfg, "c-backup-test", TEST_CFG,
                              CFG_FIRST_RUN_CONTINUE, app_file_keys,
                              app_sections, &o);
    config_free(cfg);
    return rc;
}

static void refused(const char *body, const char *needle)
{
    assert_int_equal(load(body), CUTILS_ERR_CONFIG);
    if (!strstr(cutils_get_error(), needle))
        fail_msg("error '%s' lacks '%s'", cutils_get_error(), needle);
}

static void test_defaults_pass(void **state)
{
    (void)state;
    assert_int_equal(load(""), CUTILS_OK);
}

static void test_edges_pass(void **state)
{
    (void)state;
    assert_int_equal(load("http:\n  port: 0\n  bind: 0.0.0.0\n"
                          "jobs:\n  max_parallel: 256\n"
                          "auth:\n  cookie_secure: 1\n"
                          "scheduler:\n  enabled: 0\n"
                          "retention:\n  keep_snaps: 0\n  keep_yearly: 2147483647\n"
                          "backup:\n  file_cache_match: relaxed\n"),
                     CUTILS_OK);
    assert_int_equal(load("http:\n  port: 65535\n  bind: \"\"\n"
                          "jobs:\n  max_parallel: 1\n"
                          "backup:\n  file_cache_match: auto\n"), CUTILS_OK);
}

static void test_ranges_refused(void **state)
{
    (void)state;
    refused("http:\n  port: 99999\n", "http.port: '99999' is not between 0 and 65535");
    refused("http:\n  port: -1\n", "http.port: '-1'");
    refused("jobs:\n  max_parallel: 0\n", "jobs.max_parallel: '0' is not between 1 and 256");
    refused("jobs:\n  max_parallel: 257\n", "jobs.max_parallel: '257'");
    refused("auth:\n  cookie_secure: 2\n", "auth.cookie_secure: '2' is not between 0 and 1");
    refused("scheduler:\n  enabled: -1\n", "scheduler.enabled: '-1'");
    refused("retention:\n  keep_daily: -1\n", "retention.keep_daily: '-1'");
    refused("retention:\n  keep_snaps: -5\n", "retention.keep_snaps: '-5'");
    /* The large-file pool went with format 3; its key is no longer known. */
    refused("backup:\n  store_workers: 2\n", "unknown key 'backup.store_workers'");
}

static void test_bind_and_mode_refused(void **state)
{
    (void)state;
    refused("http:\n  bind: not-an-ip\n", "http.bind: 'not-an-ip' is not an IPv4 address");
    refused("http:\n  bind: localhost\n", "http.bind: 'localhost'");
    refused("http:\n  bind: \"::1\"\n", "http.bind: '::1'");
    refused("backup:\n  file_cache_match: fast\n",
            "backup.file_cache_match: 'fast' is not one of auto, strict, relaxed");
}

/* One run reports the lot, with log.level among them. */
static void test_every_problem_in_one_run(void **state)
{
    (void)state;
    refused("http:\n  port: abc\nlog:\n  level: verbose\n"
            "jobs:\n  max_parallel: -1\n",
            "http.port: 'abc' is not an integer (and 2 more, printed above)");
}

static int teardown(void **state)
{
    (void)state;
    unlink(TEST_CFG);
    return 0;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_teardown(test_defaults_pass, teardown),
        cmocka_unit_test_teardown(test_edges_pass, teardown),
        cmocka_unit_test_teardown(test_ranges_refused, teardown),
        cmocka_unit_test_teardown(test_bind_and_mode_refused, teardown),
        cmocka_unit_test_teardown(test_every_problem_in_one_run, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
