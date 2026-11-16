#define _GNU_SOURCE

/*
 * Mount classification and /mnt/user resolution (src/helper/mounts.c,
 * docs/container-backup.md 4.2) over a fake Unraid layout: a share on one
 * pool, on one array disk, split across both, missing, behind a symlink,
 * plus the exclusions by type, driver and root.
 */

#include <limits.h>
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

#include "host_tool.h"
#include "mounts.h"
#include "test_tmp.h"

#define D   TEST_TMP "/c_backup_test_mounts"
#define MNT D "/mnt"

static host_tools_t tools;
static mount_cfg_t cfg;

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "' && mkdir -p '" MNT "/user/appdata' '" MNT "/cache/appdata/a' "
               "'" MNT "/disk1/appdata/b' '" MNT "/cache/appdata/c' '" MNT "/disk1/appdata/c' "
               "'" MNT "/cache/Photos/lib' '" MNT "/disks/NVR' && "
               "ln -s ../Photos/lib '" MNT "/cache/appdata/link'") != 0) return -1;
    host_tools_defaults(&tools);
    tools.nsenter = 0;
    tools.host_root[0] = '\0';
    memset(&cfg, 0, sizeof(cfg));
    cfg.tools = &tools;
    static char r1[] = MNT "/cache/appdata", r2[] = MNT "/user/appdata";
    cfg.include_roots[0] = r1;
    cfg.include_roots[1] = r2;
    cfg.n_include = 2;
    snprintf(cfg.user_root, sizeof(cfg.user_root), MNT "/user");
    snprintf(cfg.mnt_root, sizeof(cfg.mnt_root), MNT);
    return 0;
}

static void test_resolution(void **state)
{
    (void)state;
    mount_class_t m;
    assert_int_equal(mount_classify(&cfg, "bind", MNT "/user/appdata/a", NULL, &m), OK);
    assert_int_equal(m.verdict, MOUNT_INCLUDED);
    assert_string_equal(m.recorded, MNT "/user/appdata/a");
    assert_string_equal(m.real, MNT "/cache/appdata/a");
    assert_string_equal(m.reason, "");

    assert_int_equal(mount_classify(&cfg, "bind", MNT "/user/appdata/b", NULL, &m), OK);
    assert_string_equal(m.real, MNT "/disk1/appdata/b");
    assert_string_equal(m.reason, "array");

    assert_int_equal(mount_classify(&cfg, "bind", MNT "/user/appdata/c", NULL, &m), OK);
    assert_string_equal(m.reason, "share-split");
    assert_string_equal(m.real, MNT "/user/appdata/c");

    assert_int_not_equal(mount_classify(&cfg, "bind", MNT "/user/appdata/gone", NULL, &m), OK);
    assert_non_null(strstr(err_msg(), "no pool or disk"));
    assert_int_not_equal(mount_classify(&cfg, "bind", MNT "/cache/appdata/gone", NULL, &m), OK);
    assert_non_null(strstr(err_msg(), "missing"));
    err_clear();

    /* A symlink: recorded as bound, read where it points. */
    assert_int_equal(mount_classify(&cfg, "bind", MNT "/cache/appdata/link", NULL, &m), OK);
    assert_string_equal(m.recorded, MNT "/cache/appdata/link");
    assert_string_equal(m.real, MNT "/cache/Photos/lib");
}

static void test_exclusions(void **state)
{
    (void)state;
    mount_class_t m;
    assert_int_equal(mount_classify(&cfg, "bind", MNT "/disks/NVR", NULL, &m), OK);
    assert_int_equal(m.verdict, MOUNT_EXCLUDED);
    assert_string_equal(m.reason, "outside-include-roots");
    assert_int_equal(mount_classify(&cfg, "bind", "/var/run/docker.sock", NULL, &m), OK);
    assert_string_equal(m.reason, "system");
    assert_int_equal(mount_classify(&cfg, "bind", "/etc/localtime", NULL, &m), OK);
    assert_string_equal(m.reason, "system");
    assert_int_equal(mount_classify(&cfg, "tmpfs", "", NULL, &m), OK);
    assert_string_equal(m.reason, "system");
    assert_int_equal(mount_classify(&cfg, "volume", "/var/lib/docker/volumes/v/_data", "nfs-plugin", &m), OK);
    assert_string_equal(m.reason, "remote-volume");
    /* A local volume is data wherever it lives. */
    assert_int_equal(mount_classify(&cfg, "volume", MNT "/cache/Photos/lib", "local", &m), OK);
    assert_int_equal(m.verdict, MOUNT_INCLUDED);
    assert_string_equal(m.real, MNT "/cache/Photos/lib");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup(test_resolution, setup),
        cmocka_unit_test_setup(test_exclusions, setup),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
