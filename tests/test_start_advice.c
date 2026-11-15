/*
 * The CLI's "daemon not running" advice (B080, B103).
 *
 * The e2e suite can only see the host it runs on, which is one row of the
 * table. A container booting systemd with the unit installed is the case
 * UAT hit: the advice said "docker start" for a daemon systemd runs.
 */
#define _GNU_SOURCE

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#include "start_advice.h"

static int has(const char *s, const char *needle) { return strstr(s, needle) != NULL; }

static void test_decision_table(void **state)
{
    (void)state;
    const struct {
        host_facts_t f;
        int systemctl, docker;
        const char *exact;
    } rows[] = {
        /* in_container, systemd_booted, unit_installed */
        { { 0, 0, 0 }, 0, 0, "          Start c-backupd.\n" },
        { { 0, 1, 0 }, 1, 0, NULL },
        { { 0, 1, 1 }, 1, 0, NULL },
        { { 1, 0, 0 }, 0, 1, NULL },
        { { 1, 0, 1 }, 0, 1, NULL },  /* a unit file, but nothing to run it */
        { { 1, 1, 1 }, 1, 0, NULL },  /* B103: systemd in a container */
        { { 1, 1, 0 }, 1, 1, NULL },  /* cannot tell: both */
    };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        const char *a = start_advice(&rows[i].f);
        assert_non_null(a);
        assert_int_equal(has(a, "systemctl start c-backupd"), rows[i].systemctl);
        assert_int_equal(has(a, "docker start c-backupd"), rows[i].docker);
        if (rows[i].exact) assert_string_equal(a, rows[i].exact);
        assert_int_equal(a[strlen(a) - 1], '\n');
    }
}

static void touch(const char *root, const char *rel)
{
    char p[512];
    snprintf(p, sizeof(p), "%s%s", root, rel);
    /* mkdir -p of the parent, then the file. */
    for (char *s = p + strlen(root) + 1; *s; s++) {
        if (*s != '/') continue;
        *s = '\0';
        mkdir(p, 0755);
        *s = '/';
    }
    FILE *f = fopen(p, "w");
    assert_non_null(f);
    fclose(f);
}

static void mkdirs(const char *root, const char *rel)
{
    char p[512];
    snprintf(p, sizeof(p), "%s%s/x", root, rel);
    touch(root, p + strlen(root));
}

static void test_probe_reads_the_tree(void **state)
{
    (void)state;
    char root[] = "/tmp/cb_advice_XXXXXX";
    assert_non_null(mkdtemp(root));
    unsetenv("container");

    host_facts_t f;
    host_facts_probe(root, &f);
    assert_int_equal(f.in_container, 0);
    assert_int_equal(f.systemd_booted, 0);
    assert_int_equal(f.unit_installed, 0);

    touch(root, "/.dockerenv");
    mkdirs(root, "/run/systemd/system");
    host_facts_probe(root, &f);
    assert_int_equal(f.in_container, 1);
    assert_int_equal(f.systemd_booted, 1);
    assert_int_equal(f.unit_installed, 0);

    touch(root, "/etc/systemd/system/c-backupd.service");
    host_facts_probe(root, &f);
    assert_int_equal(f.unit_installed, 1);
    assert_true(has(start_advice(&f), "systemctl start c-backupd"));
    assert_false(has(start_advice(&f), "docker"));

    char cmd[128];
    snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", root);
    assert_int_equal(system(cmd), 0);

    /* Each packaged unit location counts, and podman's marker and the
     * environment variable count as a container. */
    const char *units[] = { "/usr/lib/systemd/system/c-backupd.service",
                            "/lib/systemd/system/c-backupd.service",
                            "/usr/local/lib/systemd/system/c-backupd.service" };
    for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); i++) {
        char r[] = "/tmp/cb_advice_XXXXXX";
        assert_non_null(mkdtemp(r));
        touch(r, units[i]);
        touch(r, "/run/.containerenv");
        host_facts_probe(r, &f);
        assert_int_equal(f.unit_installed, 1);
        assert_int_equal(f.in_container, 1);
        snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", r);
        assert_int_equal(system(cmd), 0);
    }

    char r[] = "/tmp/cb_advice_XXXXXX";
    assert_non_null(mkdtemp(r));
    setenv("container", "podman", 1);
    host_facts_probe(r, &f);
    assert_int_equal(f.in_container, 1);
    unsetenv("container");
    rmdir(r);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_decision_table),
        cmocka_unit_test(test_probe_reads_the_tree),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
