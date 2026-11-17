#define _GNU_SOURCE

/*
 * Mount classification and /mnt/user resolution (src/helper/mounts.c,
 * docs/container-backup.md 4.2) over a fake Unraid layout: a share on one
 * pool, on one array disk, split across both, missing, behind a symlink,
 * plus the exclusions by type, driver and root; and bind mounts resolved
 * through a fake mount table to the filesystem's own mount (B170).
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

/* ---- bind mounts through the mount table (B170) ---- */

#define MI D "/mountinfo"

static int write_mountinfo(void)
{
    FILE *f = fopen(MI, "w");
    if (!f) return -1;
    fputs("22 1 0:21 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n"
          /* ZFS on FS1: cache/system at its mountpoint, and Unraid's bind of
           * its docker directory at /var/lib/docker. */
          "30 22 0:45 / " MNT "/cache/system rw,relatime shared:2 - zfs cache/system rw,xattr\n"
          "40 22 0:45 /docker " D "/var/lib/docker rw,relatime shared:2 - zfs cache/system rw,xattr\n"
          /* btrfs: the top level at the pool, a bind of a directory deep in it. */
          "50 22 0:50 / " MNT "/pool rw,relatime - btrfs /dev/sdb1 rw,subvolid=5,subvol=/\n"
          "51 22 0:50 /sub/dir " D "/var/b rw,relatime - btrfs /dev/sdb1 rw,subvolid=5,subvol=/\n"
          /* docker.img: a loop device mounted nowhere else. */
          "60 22 7:2 / " D "/var/lib/img rw,relatime - btrfs /dev/loop2 rw\n"
          /* Escaped names. */
          "70 22 0:45 /with\\040space " D "/var/sp rw - zfs cache/system rw\n"
          /* A filesystem mounted outside /mnt first and under it second:
           * the pool path wins. */
          "80 22 0:60 / /srv/other rw - xfs /dev/sdc1 rw\n"
          "81 22 0:60 / " MNT "/disk9 rw - xfs /dev/sdc1 rw\n"
          "82 22 0:60 /a " D "/var/c rw - xfs /dev/sdc1 rw\n"
          /* A mount stacked over another at one point: the later one counts. */
          "90 22 0:45 /old " D "/var/stack rw - zfs cache/system rw\n"
          "91 22 0:50 /new " D "/var/stack rw - btrfs /dev/sdb1 rw\n"
          /* A bind of a subdirectory of the filesystem's own subvolume mount. */
          "95 22 0:70 /@data " MNT "/sv rw - btrfs /dev/sdd1 rw,subvol=/@data\n"
          "96 22 0:70 /@data/x " D "/var/sv rw - btrfs /dev/sdd1 rw,subvol=/@data\n"
          /* Docker's directory on a user share: a bind of shfs. */
          "100 22 0:80 / " MNT "/user rw,nosuid - fuse.shfs shfs rw\n"
          "101 22 0:80 /system/docker " D "/var/lib/d2 rw,nosuid - fuse.shfs shfs rw\n"
          "garbage line\n",
          f);
    return fclose(f);
}

static void unbind_is(const char *in, const char *want, int want_loop)
{
    char out[PATH_MAX];
    int loop = -1;
    assert_int_equal(mount_unbind(MI, MNT, in, out, sizeof(out), &loop), OK);
    assert_string_equal(out, want);
    assert_int_equal(loop, want_loop);
}

static void test_unbind(void **state)
{
    (void)state;
    assert_int_equal(write_mountinfo(), 0);
    unbind_is(D "/var/lib/docker/volumes/v/_data", MNT "/cache/system/docker/volumes/v/_data", 0);
    unbind_is(D "/var/lib/docker", MNT "/cache/system/docker", 0);
    unbind_is(D "/var/b/x/y", MNT "/pool/sub/dir/x/y", 0);
    unbind_is(D "/var/lib/img/volumes/v/_data", D "/var/lib/img/volumes/v/_data", 1);
    unbind_is(D "/var/sp/f", MNT "/cache/system/with space/f", 0);
    unbind_is(D "/var/c/q", MNT "/disk9/a/q", 0);
    unbind_is(D "/var/stack/z", MNT "/pool/new/z", 0);
    unbind_is(D "/var/sv/k", MNT "/sv/x/k", 0);
    unbind_is(D "/var/lib/d2/v", MNT "/user/system/docker/v", 0);
    /* Not on a bind: unchanged, the filesystem's own mount included. */
    unbind_is(MNT "/cache/system/docker", MNT "/cache/system/docker", 0);
    unbind_is(D "/var/libx", D "/var/libx", 0);
    unbind_is("/", "/", 0);

    char out[16];
    int loop = 0;
    assert_int_not_equal(mount_unbind(MI, MNT, D "/var/lib/docker/volumes/v/_data", out, sizeof(out), &loop), OK);
    assert_int_not_equal(mount_unbind(D "/no-such-mountinfo", MNT, "/x", out, sizeof(out), &loop), OK);
    err_clear();
}

static void test_classify_bind(void **state)
{
    (void)state;
    assert_int_equal(write_mountinfo(), 0);
    assert_int_equal(system("mkdir -p '" D "/var/lib/docker/volumes/v/_data' "
                            "'" MNT "/cache/system/docker/volumes/v/_data' "
                            "'" D "/var/lib/docker/volumes/orphan/_data' "
                            "'" D "/var/lib/img/volumes/w/_data' "
                            "'" D "/var/lib/d2/volumes/x/_data' "
                            "'" MNT "/user/system/docker/volumes/x/_data' "
                            "'" MNT "/cache/system/docker/volumes/x/_data'"), 0);
    mount_cfg_t c = cfg;
    snprintf(c.mountinfo, sizeof(c.mountinfo), MI);
    mount_class_t m;
    /* A named volume under /var/lib/docker: read through the pool. */
    assert_int_equal(mount_classify(&c, "volume", D "/var/lib/docker/volumes/v/_data", "local", &m), OK);
    assert_int_equal(m.verdict, MOUNT_INCLUDED);
    assert_string_equal(m.recorded, D "/var/lib/docker/volumes/v/_data");
    assert_string_equal(m.real, MNT "/cache/system/docker/volumes/v/_data");
    assert_string_equal(m.reason, "");
    /* The table names a source the host does not have: refused, not guessed. */
    assert_int_not_equal(mount_classify(&c, "volume", D "/var/lib/docker/volumes/orphan/_data", "local", &m), OK);
    assert_non_null(strstr(err_msg(), "does not exist"));
    err_clear();
    /* docker.img: left where it is, and said so. */
    assert_int_equal(mount_classify(&c, "volume", D "/var/lib/img/volumes/w/_data", "local", &m), OK);
    assert_string_equal(m.real, D "/var/lib/img/volumes/w/_data");
    assert_string_equal(m.reason, "loop-image");
    /* A bind of a share lands on shfs, and goes on to the one pool. */
    assert_int_equal(mount_classify(&c, "volume", D "/var/lib/d2/volumes/x/_data", "local", &m), OK);
    assert_string_equal(m.real, MNT "/cache/system/docker/volumes/x/_data");
    assert_string_equal(m.recorded, D "/var/lib/d2/volumes/x/_data");
    /* Without a table nothing moves. */
    assert_int_equal(mount_classify(&cfg, "volume", D "/var/lib/docker/volumes/v/_data", "local", &m), OK);
    assert_string_equal(m.real, D "/var/lib/docker/volumes/v/_data");
    /* An unreadable table fails the classification rather than guessing. */
    snprintf(c.mountinfo, sizeof(c.mountinfo), D "/no-such-mountinfo");
    assert_int_not_equal(mount_classify(&c, "volume", D "/var/lib/docker/volumes/v/_data", "local", &m), OK);
    err_clear();

    host_tools_t t;
    memset(&t, 0, sizeof(t));
    char mi[64];
    mount_default_mountinfo(&t, mi, sizeof(mi));
    assert_string_equal(mi, "/proc/self/mountinfo");
    snprintf(t.host_root, sizeof(t.host_root), "/proc/1/root");
    mount_default_mountinfo(&t, mi, sizeof(mi));
    assert_string_equal(mi, "/proc/1/mountinfo");
}

/* ---- the helper's own layout (B186) ---- */

static void put_file(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

#define SELF D "/self.mi"
#define HOST D "/host.mi"
#define ROOTLN "22 1 0:21 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n"

static void test_layout(void **state)
{
    (void)state;
    const char *paths[] = { "/mnt/cache/system/c-backup-helper", "/run/c-backup-helper", "", NULL };
    const size_t np = 4;

    /* The documented layout: the pool rslave, nothing bound under it. */
    put_file(SELF, "1 0 0:50 / / rw - overlay overlay rw\n"
                   "2 1 0:45 / /mnt/cache rw master:5 - zfs cache rw\n"
                   "3 2 0:46 / /mnt/cache/system rw master:6 - zfs cache/system rw\n"
                   "4 1 0:30 /c-backup-helper /run/c-backup-helper rw - tmpfs tmpfs rw\n");
    put_file(HOST, ROOTLN "30 22 0:45 / /mnt/cache rw shared:5 - zfs cache rw\n"
                   "31 30 0:46 / /mnt/cache/system rw shared:6 - zfs cache/system rw\n");
    assert_int_equal(mount_layout_check(SELF, HOST, 1, paths, np), OK);
    /* The state directory as a dataset's own mountpoint, once: fine. */
    put_file(HOST, ROOTLN "30 22 0:45 / /mnt/cache rw shared:5 - zfs cache rw\n"
                   "31 30 0:47 / /mnt/cache/system/c-backup-helper rw shared:7 - zfs cache/system/h rw\n");
    assert_int_equal(mount_layout_check(SELF, HOST, 1, paths, np), OK);

    /* The old layout: the state directory bound under an rshared pool. */
    put_file(SELF, "1 0 0:50 / / rw shared:9 - overlay overlay rw\n"
                   "2 1 0:45 / /mnt/cache rw shared:5 - zfs cache rw\n"
                   "5 2 0:46 /c-backup-helper /mnt/cache/system/c-backup-helper rw shared:6 - zfs cache/system rw\n");
    put_file(HOST, ROOTLN "30 22 0:45 / /mnt/cache rw shared:5 - zfs cache rw\n");
    assert_int_equal(mount_layout_check(SELF, HOST, 1, paths, np), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "bound separately under /mnt/cache, which is mounted rshared"));
    assert_non_null(strstr(err_msg(), "-v /mnt/cache:/mnt/cache:rslave"));
    err_clear();
    /* On the host itself the same table is a dataset under a shared pool. */
    assert_int_equal(mount_layout_check(SELF, NULL, 0, paths, np), OK);

    /* Stacked on the host after three starts: refused, with the cleanup. */
    put_file(SELF, "1 0 0:50 / / rw - overlay overlay rw\n"
                   "2 1 0:45 / /mnt/cache rw master:5 - zfs cache rw\n");
    put_file(HOST, ROOTLN "30 22 0:45 / /mnt/cache rw shared:5 - zfs cache rw\n"
                   "40 30 0:46 /c-backup-helper /mnt/cache/system/c-backup-helper rw shared:6 - zfs cache/system rw\n"
                   "41 40 0:46 /c-backup-helper /mnt/cache/system/c-backup-helper rw shared:6 - zfs cache/system rw\n"
                   "42 41 0:46 /c-backup-helper /mnt/cache/system/c-backup-helper rw shared:6 - zfs cache/system rw\n");
    assert_int_equal(mount_layout_check(SELF, HOST, 1, paths, np), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "/mnt/cache/system/c-backup-helper is mounted 3 times in the host's mount table"));
    assert_non_null(strstr(err_msg(), "umount -l"));
    err_clear();
    /* Stacked in the helper's own table, host unread: refused too. */
    put_file(SELF, "1 0 0:50 / / rw - overlay overlay rw\n"
                   "4 1 0:30 /x /run/c-backup-helper rw - tmpfs tmpfs rw\n"
                   "5 4 0:30 /x /run/c-backup-helper rw - tmpfs tmpfs rw\n");
    assert_int_equal(mount_layout_check(SELF, NULL, 0, paths, np), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "mounted 2 times in the helper's mount table"));
    err_clear();
    /* An unreadable host table is warned about, the helper's own checked. */
    put_file(SELF, "1 0 0:50 / / rw - overlay overlay rw\n");
    assert_int_equal(mount_layout_check(SELF, D "/none.mi", 1, paths, np), OK);
    /* The helper's own table that cannot be read fails rather than passing. */
    assert_int_not_equal(mount_layout_check(D "/none.mi", NULL, 0, paths, np), OK);
    err_clear();
    /* The process's own namespace answers something. */
    int c = mount_in_container();
    assert_true(c == 0 || c == 1);
}

/* The suite's scratch goes with it: tests/scratch-guard.sh fails a suite
 * that leaves anything behind (SCRATCH-1). Setup removes it too, so a
 * killed run is no obstacle to the next. */
static int group_cleanup(void **state)
{
    (void)state;
    int rc = system("rm -rf '" D "'");
    (void)rc;
    return 0;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup(test_resolution, setup),
        cmocka_unit_test_setup(test_exclusions, setup),
        cmocka_unit_test_setup(test_unbind, setup),
        cmocka_unit_test_setup(test_classify_bind, setup),
        cmocka_unit_test_setup(test_layout, setup),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
