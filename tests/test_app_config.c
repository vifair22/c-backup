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

/* db_body goes into the db section beside db.path, which every file
 * needs; body after it. */
static int load_db(const char *db_body, const char *body)
{
    FILE *f = fopen(TEST_CFG, "w");
    assert_non_null(f);
    fprintf(f, "db:\n  path: /tmp/x.db\n%s%s", db_body, body);
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

static int load(const char *body) { return load_db("", body); }

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
                          "jobs:\n  max_parallel: 256\n  read_budget_mb: 48\n"
                          "vm:\n  export_max_conns: 0\n"
                          "auth:\n  cookie_secure: 1\n"
                          "scheduler:\n  enabled: 0\n"
                          "retention:\n  keep_snaps: 0\n  keep_yearly: 2147483647\n"
                          "backup:\n  file_cache_match: relaxed\n  unsettled: skip\n"),
                     CUTILS_OK);
    assert_int_equal(load("http:\n  port: 65535\n  bind: \"\"\n"
                          "jobs:\n  max_parallel: 1\n"
                          "backup:\n  file_cache_match: auto\n  unsettled: store\n"), CUTILS_OK);
}

static void test_ranges_refused(void **state)
{
    (void)state;
    refused("http:\n  port: 99999\n", "http.port: '99999' is not between 0 and 65535");
    refused("http:\n  port: -1\n", "http.port: '-1'");
    refused("jobs:\n  max_parallel: 0\n", "jobs.max_parallel: '0' is not between 1 and 256");
    refused("jobs:\n  max_parallel: 257\n", "jobs.max_parallel: '257'");
    /* A read budget smaller than one largest object could never read it. */
    refused("jobs:\n  read_budget_mb: 47\n",
            "jobs.read_budget_mb: '47' MiB is under 48 MiB, the largest object");
    refused("jobs:\n  read_budget_mb: 0\n", "jobs.read_budget_mb: '0' MiB");
    refused("jobs:\n  read_budget_mb: 1048577\n", "jobs.read_budget_mb: '1048577'");
    /* Each NBD export connection may hold one 16 MiB object of the budget
     * beside the 48 a job needs: four want 112 MiB, or exports off. */
    refused("jobs:\n  read_budget_mb: 64\n",
            "vm.export_max_conns: '4' needs jobs.read_budget_mb of at least 4 x 16 MiB + 48 MiB = 112 MiB");
    refused("jobs:\n  read_budget_mb: 111\n", "set jobs.read_budget_mb >= 112, or lower vm.export_max_conns");
    assert_int_equal(load("jobs:\n  read_budget_mb: 112\n"), CUTILS_OK);
    assert_int_equal(load("jobs:\n  read_budget_mb: 64\nvm:\n  export_max_conns: 1\n"), CUTILS_OK);
    refused("vm:\n  export_max_conns: 65\n", "vm.export_max_conns: '65'");
    refused("auth:\n  cookie_secure: 2\n", "auth.cookie_secure: '2' is not between 0 and 1");
    refused("scheduler:\n  enabled: -1\n", "scheduler.enabled: '-1'");
    refused("retention:\n  keep_daily: -1\n", "retention.keep_daily: '-1'");
    refused("retention:\n  keep_snaps: -5\n", "retention.keep_snaps: '-5'");
    refused("backup:\n  store_workers: 65\n",
            "backup.store_workers: '65' is not between 0 and 64");
}

static void test_bind_and_mode_refused(void **state)
{
    (void)state;
    refused("http:\n  bind: not-an-ip\n", "http.bind: 'not-an-ip' is not an IPv4 address");
    refused("http:\n  bind: localhost\n", "http.bind: 'localhost'");
    refused("http:\n  bind: \"::1\"\n", "http.bind: '::1'");
    refused("backup:\n  file_cache_match: fast\n",
            "backup.file_cache_match: 'fast' is not one of auto, strict, relaxed");
    refused("backup:\n  unsettled: maybe\n",
            "backup.unsettled: 'maybe' is not one of store, skip");
}

/* An empty value is unset to c-utils, which reads back the default, so
 * `unsettled: ""` used to start the daemon as store (NIT-18). Every key
 * whose values are named or numeric refuses it, however it is spelled. */
static void test_empty_refused(void **state)
{
    (void)state;
    refused("backup:\n  unsettled: \"\"\n",
            "backup.unsettled: an empty value is not allowed; set one of store, "
            "skip, or leave the key out for the default (store)");
    refused("backup:\n  unsettled:\n", "backup.unsettled: an empty value");
    refused("backup:\n  unsettled:   # later\n", "backup.unsettled: an empty value");
    refused("backup:\n  unsettled: \"\"\r\n", "backup.unsettled: an empty value");
    refused("backup:\n  file_cache_match: \"\"\n",
            "backup.file_cache_match: an empty value is not allowed; set one of "
            "auto, strict, relaxed");
    refused("log:\n  level: \"\"\n", "log.level: an empty value");
    refused("http:\n  port:\n", "http.port: an empty value is not allowed; set a "
                                "whole number, or leave the key out for the "
                                "default (8765)");
    refused("retention:\n  keep_daily: \"\"\n", "retention.keep_daily: an empty value");
    refused("backup:\n  store_workers:\n", "backup.store_workers: an empty value");
    assert_int_equal(load_db("  synchronous: \"\"\n", ""), CUTILS_ERR_CONFIG);
    if (!strstr(cutils_get_error(), "db.synchronous: an empty value"))
        fail_msg("error '%s'", cutils_get_error());

    /* Through the environment too: there it reads back as "". */
    setenv("C_BACKUP_TEST_BACKUP_UNSETTLED", "", 1);
    refused("", "C_BACKUP_TEST_BACKUP_UNSETTLED (backup.unsettled): an empty value");
    unsetenv("C_BACKUP_TEST_BACKUP_UNSETTLED");

    /* Where empty has a meaning, or a value merely contains a quote or a
     * hash, it still starts. */
    assert_int_equal(load("http:\n  bind: \"\"\npushover:\n  token: \"\"\n"), CUTILS_OK);
    assert_int_equal(load("backup:\n  unsettled: \"skip\"  # quoted\n"), CUTILS_OK);
    assert_int_equal(load("backup:\n  unsettled: store # why\n"), CUTILS_OK);
}

/* One run reports the lot, with log.level among them. */
/* The state.db settings (state_db.h): the documented values pass, the
 * edges of each range pass, and anything else stops the daemon. */
static void test_db_settings(void **state)
{
    (void)state;
    assert_int_equal(load_db("  synchronous: full\n  cache_mb: 2\n"
                             "  wal_autocheckpoint: 1000\n", ""), CUTILS_OK);
    assert_int_equal(load_db("  synchronous: normal\n  cache_mb: 65536\n"
                             "  wal_autocheckpoint: 1048576\n", ""), CUTILS_OK);
    assert_int_equal(load_db("  synchronous: off\n", ""), CUTILS_ERR_CONFIG);
    if (!strstr(cutils_get_error(), "db.synchronous: 'off' is not one of normal, full"))
        fail_msg("error '%s'", cutils_get_error());
    assert_int_equal(load_db("  cache_mb: 1\n", ""), CUTILS_ERR_CONFIG);
    if (!strstr(cutils_get_error(), "db.cache_mb: '1' is not between 2 and 65536"))
        fail_msg("error '%s'", cutils_get_error());
    assert_int_equal(load_db("  wal_autocheckpoint: 999\n", ""), CUTILS_ERR_CONFIG);
    if (!strstr(cutils_get_error(), "db.wal_autocheckpoint: '999' is not between 1000 and 1048576"))
        fail_msg("error '%s'", cutils_get_error());
}

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
        cmocka_unit_test_teardown(test_db_settings, teardown),
        cmocka_unit_test_teardown(test_empty_refused, teardown),
        cmocka_unit_test_teardown(test_every_problem_in_one_run, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
