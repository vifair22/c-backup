#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * units.probe as detection asks for it (src/units/unit_detect.c,
 * docs/db-detection.md 8.1): a provider without the operation (libvirt,
 * section 10) is never called and answers no probe, a probe that fails is
 * logged and dropped (principle 5), one that answers is handed over, and
 * detection switched off asks nothing. And the one re-read a Nextcloud
 * config.php gets, through its tree's read root.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "db_findings.h"
#include "unit_detect.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_unit_detect"

static int calls;
static status_t answer;

static status_t probe_fn(sp_ctx_t *ctx, const char *unit_id, sp_unit_probe_t **out)
{
    (void)ctx;
    (void)unit_id;
    calls++;
    if (answer != OK) return set_error(answer, "probe refused");
    *out = calloc(1, sizeof(**out));
    return *out ? OK : set_error(ERR_NOMEM, "probe");
}

static void test_probe_for_detection(void **state)
{
    (void)state;
    db_detect_settings_t on = { 1, 1024, 1 }, off = { 0, 1024, 1 };
    db_detect_settings_set(&on);
    sp_provider_ops_t ops;
    memset(&ops, 0, sizeof(ops));
    ops.name = "libvirt";

    /* No unit_probe (a VM provider): no call, no probe. */
    calls = 0;
    assert_null(unit_detect_probe(&ops, NULL, "vm1", "libvirt:vm1"));
    assert_null(unit_detect_probe(NULL, NULL, "vm1", "libvirt:vm1"));
    assert_int_equal(calls, 0);

    ops.name = "docker";
    ops.unit_probe = probe_fn;
    answer = OK;
    sp_unit_probe_t *p = unit_detect_probe(&ops, NULL, "pg", "docker:pg");
    assert_non_null(p);
    assert_int_equal(calls, 1);
    sp_unit_probe_free(p);

    /* A failure is dropped, and the error with it. */
    answer = ERR_IO;
    assert_null(unit_detect_probe(&ops, NULL, "pg", "docker:pg"));
    assert_int_equal(calls, 2);
    assert_int_equal(err_msg()[0], '\0');

    /* Detection off: nothing is asked. */
    db_detect_settings_set(&off);
    answer = OK;
    assert_null(unit_detect_probe(&ops, NULL, "pg", "docker:pg"));
    assert_int_equal(calls, 2);
    db_detect_settings_set(&on);
}

static void test_reread(void **state)
{
    (void)state;
    int rc = system("rm -rf '" TEST_DIR "' && mkdir -p '" TEST_DIR "/snap/nc/config'");
    assert_int_equal(rc, 0);
    const char *cfg = "<?php $CONFIG = array ( 'dbtype' => 'mysql', 'dbhost' => 'db', );\n";
    int fd = open(TEST_DIR "/snap/nc/config/config.php", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, cfg, strlen(cfg)), (ssize_t)strlen(cfg));
    close(fd);

    db_detector_t *d = NULL;
    assert_int_equal(db_detect_begin(NULL, &d), OK);
    /* Recorded at /live, read at the snapshot; the cache vouched for it. */
    const char *rec_path = "/live/nc/config/config.php";
    db_detect_carry_add(d, rec_path, DB_HEAD_CODE(DB_HEAD_NEXTCLOUD, 2));
    assert_int_equal(db_detect_cached(d, rec_path, 70), 1);
    sp_unit_probe_t pr;
    sp_probe_container_t c;
    memset(&pr, 0, sizeof(pr));
    memset(&c, 0, sizeof(c));
    snprintf(c.name, sizeof(c.name), "nc");
    snprintf(c.aliases[0], sizeof(c.aliases[0]), "db");
    c.n_aliases = 1;
    pr.v = &c;
    pr.n = 1;
    db_detect_unit(d, &pr);
    const char *rec[] = { "/elsewhere", "/live" };
    const char *rd[] = { TEST_DIR "/nowhere", TEST_DIR "/snap" };
    unit_detect_reread(d, rec, rd, 2);
    unit_detect_reread(NULL, rec, rd, 2);
    unit_detect_reread(d, NULL, rd, 2);
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(d, NULL, &f), OK);
    assert_int_equal(f->n, 1);
    assert_non_null(strstr(f->v[0].evidence, "\"resolved\":\"this unit\""));
    db_findings_free(f);
    db_detect_free(d);

    /* A path under no tree, or a file that is not there, is skipped. */
    assert_int_equal(db_detect_begin(NULL, &d), OK);
    db_detect_carry_add(d, "/other/config/config.php", DB_HEAD_CODE(DB_HEAD_NEXTCLOUD, 2));
    db_detect_carry_add(d, "/live/gone/config/config.php", DB_HEAD_CODE(DB_HEAD_NEXTCLOUD, 2));
    assert_int_equal(db_detect_cached(d, "/other/config/config.php", 1), 1);
    assert_int_equal(db_detect_cached(d, "/live/gone/config/config.php", 1), 1);
    unit_detect_reread(d, rec, rd, 2);
    assert_int_equal(db_detect_finish(d, NULL, &f), OK);
    for (size_t i = 0; i < f->n; i++)
        assert_non_null(strstr(f->v[i].evidence, "dbhost not read"));
    db_findings_free(f);
    db_detect_free(d);
    rc = system("rm -rf '" TEST_DIR "'");
    (void)rc;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_probe_for_detection),
        cmocka_unit_test(test_reread),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
