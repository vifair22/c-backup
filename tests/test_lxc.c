#define _GNU_SOURCE

/*
 * The LXC provider (src/helper/lxc_backend.c) over stand-in lxc-* tools
 * and zfs (tests/fake_host): enumeration, the config and the rootfs and
 * bind mounts as items, a running container frozen around one snapshot of
 * both datasets and thawed, a stopped one offline, and the sweep thawing
 * what a dead capture left frozen.
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

#include "lxc_backend.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "test_tmp.h"

#define D      TEST_TMP "/c_backup_test_lxc"
#define STATE  D "/state"
#define MNT    D "/mnt"
#define LXCP   MNT "/cache/lxc"

static helper_policy_t pol;
static helper_backend_t be;

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static int count(const char *path, const char *needle)
{
    FILE *f = fopen(path, "r");
    char line[512];
    int n = 0;
    while (f && fgets(line, sizeof(line), f)) if (strstr(line, needle)) n++;
    if (f) fclose(f);
    return n;
}

static int init_with(const char *nsenter);

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "' && mkdir -p '" STATE "' '" LXCP "/DevBox/rootfs/etc' '" LXCP "/Parked/rootfs' "
               "'" MNT "/cache/appdata/dev' '" MNT "/user'") != 0) return -1;
    put(LXCP "/DevBox/config", "lxc.uts.name = DevBox\nlxc.rootfs.path = dir:" LXCP "/DevBox/rootfs\n"
        "lxc.mount.entry = " MNT "/cache/appdata/dev srv/dev none bind,create=dir 0 0\n"
        "lxc.mount.entry = proc proc proc nodev 0 0\n");
    put(LXCP "/DevBox/rootfs/etc/hostname", "devbox\n");
    put(MNT "/cache/appdata/dev/work", "w1");
    put(LXCP "/Parked/config", "lxc.rootfs.path = " LXCP "/Parked/rootfs\n");
    put(STATE "/lxc-DevBox", "RUNNING\n");
    put(STATE "/datasets", "cache/lxc\t" LXCP "\ncache/appdata\t" MNT "/cache/appdata\n");
    put(D "/mountinfo", "22 1 0:21 / / rw - ext4 /dev/sda1 rw\n");
    unlink(STATE "/subvol_list");
    setenv("FAKE_HOST_STATE", STATE, 1);
    return init_with("nsenter = 0\n");
}

/* The backend over a policy whose [host] nsenter settings are nsenter. */
static int init_with(const char *nsenter)
{
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    char text[4096];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = 1000\ndaemon_id = 0badf00d\nstate_dir = " STATE "\n"
             "[snapshot]\nzfs_domains = cache/lxc, cache/appdata\nbtrfs_domains = " D "/bpool\n"
             "[host]\n%shost_root =\nzfs = %s/tests/fake_host/zfs\nbtrfs = %s/tests/fake_host/btrfs\n"
             "lxc_dir = %s/tests/fake_host/lxc\n"
             "user_root = " MNT "/user\nmnt_root = " MNT "\nboot_config = " D "/boot\nmountinfo = " D "/mountinfo\n"
             "[docker]\ninclude_roots = " MNT "/cache/appdata\n"
             "[lxc]\nenabled = 1\npath = " LXCP "\n", nsenter, cwd, cwd, cwd);
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) return -1;
    return lxc_backend_init(&pol, &be) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    lxc_backend_fini(&be);
    helper_policy_free(&pol);
    return 0;
}

static status_t on_unit(void *ctx, const char *id, const char *name, int st)
{
    (void)name; (void)st;
    int *n = ctx;
    (*n)++;
    return strcmp(id, "DevBox") == 0 || strcmp(id, "Parked") == 0 ? OK : set_error(ERR_INVALID, "unexpected %s", id);
}

static void test_capture(void **state)
{
    (void)state;
    int n = 0;
    assert_int_equal(be.ops->list_units(be.ctx, on_unit, &n), OK);
    assert_int_equal(n, 2);
    sp_unit_desc_t *d = NULL;
    assert_int_equal(be.ops->describe(be.ctx, "DevBox", &d), OK);
    assert_int_equal(d->state, SP_STATE_RUNNING);
    assert_int_equal(d->n_items, 3);     /* config, rootfs, the bind mount; not proc */
    const sp_item_desc_t *rootfs = NULL, *mnt = NULL;
    for (size_t i = 0; i < d->n_items; i++) {
        if (strcmp(d->items[i].id, "rootfs") == 0) rootfs = &d->items[i];
        if (strcmp(d->items[i].id, "mount:0") == 0) mnt = &d->items[i];
    }
    assert_non_null(rootfs);
    assert_non_null(mnt);
    assert_string_equal(rootfs->tree_path, LXCP "/DevBox/rootfs");

    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = 51;
    q.require = SP_CONSIST_CRASH;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    status_t bst = be.ops->begin(be.ctx, d, &q, &s, &info);
    if (bst != OK) fail_msg("begin: %s", err_msg());
    assert_int_equal(info->achieved, SP_CONSIST_CRASH);
    assert_true(info->consist_flags & SP_CF_FROZEN);
    assert_int_equal(count(STATE "/lxc.log", "lxc-freeze"), 1);
    assert_int_equal(count(STATE "/lxc.log", "lxc-unfreeze"), 1);
    /* Both datasets in one command: one instant. */
    assert_int_equal(count(STATE "/zfs.log", "zfs snapshot"), 1);
    put(MNT "/cache/appdata/dev/work", "w2");
    char root[PATH_MAX], f[PATH_MAX + 8];
    assert_int_equal(be.ops->tree_root(s, mnt, root, sizeof(root)), OK);
    snprintf(f, sizeof(f), "%s/work", root);
    FILE *fp = fopen(f, "r");
    char buf[8] = "";
    assert_non_null(fp);
    assert_non_null(fgets(buf, sizeof(buf), fp));
    fclose(fp);
    assert_string_equal(buf, "w1");
    void *data = NULL;
    size_t len = 0;
    assert_int_equal(be.ops->blob_read(s, &d->items[0], &data, &len), OK);
    assert_non_null(memmem(data, len, "lxc.uts.name = DevBox", 21));
    free(data);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);

    assert_int_equal(be.ops->describe(be.ctx, "Parked", &d), OK);
    assert_int_equal(be.ops->begin(be.ctx, d, &q, &s, &info), OK);
    assert_int_equal(info->achieved, SP_CONSIST_OFFLINE);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
}

/* A freeze the watchdog undid before the thaw: the snapshot saw a running
 * container. One instant still makes it crash-consistent, but the flags
 * and a warning say the freeze did not last, and nothing is unfrozen. */
/*
 * B231: a bind mount with a child dataset below it, and a rootfs on btrfs
 * with a subvolume inside (Docker's btrfs driver in a container makes one
 * per layer), read empty directories from their snapshots. Both are flagged
 * at describe, read live, warned about by name, and the level drops.
 */
static void test_nested_below(void **state)
{
    (void)state;
    assert_int_equal(system("mkdir -p '" MNT "/cache/appdata/dev/child' '" D "/bpool/lxc/Nest/rootfs/var/lib/docker/btrfs/subvolumes/l1' "
                            "'" LXCP "/Nest'"), 0);
    put(LXCP "/Nest/config", "lxc.rootfs.path = dir:" D "/bpool/lxc/Nest/rootfs\n");
    put(STATE "/lxc-Nest", "STOPPED\n");
    put(D "/mountinfo", "22 1 0:21 / / rw - ext4 /dev/sda1 rw\n"
                        "40 22 0:40 / " MNT "/cache/appdata/dev/child rw - zfs cache/appdata/dev/child rw\n");
    put(STATE "/subvol_list", "ID 258 gen 9 top level 5 path lxc/Nest/rootfs/var/lib/docker/btrfs/subvolumes/l1\n"
                              "ID 259 gen 9 top level 5 path lxc/Other\n");
    sp_unit_desc_t *d = NULL;
    assert_int_equal(be.ops->describe(be.ctx, "DevBox", &d), OK);
    const sp_item_desc_t *m = NULL, *rf = NULL;
    for (size_t i = 0; i < d->n_items; i++) {
        if (d->items[i].source && strcmp(d->items[i].source, MNT "/cache/appdata/dev") == 0) m = &d->items[i];
        if (strcmp(d->items[i].id, "rootfs") == 0) rf = &d->items[i];
    }
    assert_non_null(m);
    assert_false(m->caps & SP_CAP_SNAPSHOT);
    assert_true(rf->caps & SP_CAP_SNAPSHOT);      /* nothing below DevBox's rootfs */
    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = 7;
    q.require = SP_CONSIST_INCONSISTENT;
    q.daemon8 = "0badf00d";
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    assert_int_equal(be.ops->begin(be.ctx, d, &q, &s, &info), OK);
    assert_int_equal(info->achieved, SP_CONSIST_INCONSISTENT);
    int named = 0;
    for (size_t i = 0; i < info->n_warnings; i++)
        if (strstr(info->warnings[i], MNT "/cache/appdata/dev has 1 mount(s)") &&
            strstr(info->warnings[i], "/dev/child")) named = 1;
    assert_true(named);
    char root[PATH_MAX];
    assert_int_equal(be.ops->tree_root(s, m, root, sizeof(root)), OK);
    assert_string_equal(root, MNT "/cache/appdata/dev");
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);

    /* The btrfs rootfs with a subvolume inside it. */
    assert_int_equal(be.ops->describe(be.ctx, "Nest", &d), OK);
    rf = NULL;
    for (size_t i = 0; i < d->n_items; i++) if (strcmp(d->items[i].id, "rootfs") == 0) rf = &d->items[i];
    assert_non_null(rf);
    assert_false(rf->caps & SP_CAP_SNAPSHOT);
    assert_int_equal(be.ops->begin(be.ctx, d, &q, &s, &info), OK);
    named = 0;
    for (size_t i = 0; i < info->n_warnings; i++)
        if (strstr(info->warnings[i], "rootfs has 1 mount(s) or btrfs subvolume(s)") &&
            strstr(info->warnings[i], "subvolumes/l1")) named = 1;
    assert_true(named);
    assert_int_equal(be.ops->tree_root(s, rf, root, sizeof(root)), OK);
    assert_string_equal(root, D "/bpool/lxc/Nest/rootfs");
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);

    /* A pool whose subvolumes cannot be listed is not trusted: the describe
     * fails rather than guessing. */
    setenv("FAKE_BTRFS_FAIL", "list", 1);
    assert_int_not_equal(be.ops->describe(be.ctx, "Nest", &d), OK);
    unsetenv("FAKE_BTRFS_FAIL");
    err_clear();
}

static void test_freeze_lost(void **state)
{
    (void)state;
    put(STATE "/lxc-DevBox.thaw", "");
    sp_unit_desc_t *d = NULL;
    assert_int_equal(be.ops->describe(be.ctx, "DevBox", &d), OK);
    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = 52;
    q.require = SP_CONSIST_CRASH;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    status_t bst = be.ops->begin(be.ctx, d, &q, &s, &info);
    unlink(STATE "/lxc-DevBox.thaw");
    if (bst != OK) fail_msg("begin: %s", err_msg());
    assert_int_equal(info->achieved, SP_CONSIST_CRASH);
    assert_false(info->consist_flags & SP_CF_FROZEN);
    assert_true(info->consist_flags & SP_CF_WATCHDOG_RELEASED);
    assert_true(info->n_warnings >= 1);
    assert_non_null(strstr(info->warnings[info->n_warnings - 1], "watchdog"));
    assert_int_equal(count(STATE "/lxc.log", "lxc-unfreeze"), 0);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
}

static void test_sweep_thaws(void **state)
{
    (void)state;
    put(STATE "/lxc-DevBox", "FROZEN\n");
    sp_intent_t in = { 1, "DevBox", SP_HOLD_FREEZE, "lxc:DevBox", 3 };
    int rel = 0;
    sp_sweep_req_t rq;
    memset(&rq, 0, sizeof(rq));
    rq.daemon8 = "0badf00d";
    rq.intents = &in;
    rq.n_intents = 1;
    rq.released = &rel;
    int absent = 9;
    rq.absent = &absent;
    sp_sweep_report_t rep;
    assert_int_equal(be.ops->sweep(be.ctx, &rq, &rep), OK);
    assert_int_equal(rel, 1);
    assert_int_equal(absent, 0);
    assert_int_equal(be.unit_state(be.ctx, "DevBox"), SP_STATE_RUNNING);
    /* Again, with nothing frozen: released as a no-op, and said so. */
    rel = 0;
    assert_int_equal(be.ops->sweep(be.ctx, &rq, &rep), OK);
    assert_int_equal(rel, 1);
    assert_int_equal(absent, 1);
}

static void test_define(void **state)
{
    (void)state;
    const char *def = "{\"config\":\"lxc.uts.name = DevBox\\nlxc.rootfs.path = dir:/old/rootfs\\n"
                      "lxc.net.0.hwaddr = 00:16:3e:aa:bb:cc\\nlxc.net.0.link = br0\\n\"%s}";
    char buf[1024];
    cJSON *res = NULL;
    snprintf(buf, sizeof(buf), def, "");
    assert_int_not_equal(be.define(be.ctx, "DevBox", buf, &pol, &res), OK);   /* exists */
    assert_non_null(strstr(err_msg(), "exists"));
    err_clear();
    snprintf(buf, sizeof(buf), def, ",\"replace\":true");
    assert_int_not_equal(be.define(be.ctx, "DevBox", buf, &pol, &res), OK);   /* running */
    err_clear();
    /* B212: the config keeps the original's mode, never the umask's
     * 0644 for a 0640 original. */
    mode_t um = umask(022);
    assert_int_equal(chmod(LXCP "/DevBox/config", 0640), 0);
    snprintf(buf, sizeof(buf), def, ",\"name\":\"devbox2\"");
    assert_int_equal(be.define(be.ctx, "DevBox", buf, &pol, &res), OK);
    cJSON_Delete(res);
    struct stat sb;
    assert_int_equal(stat(LXCP "/devbox2/config", &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0640);
    FILE *f = fopen(LXCP "/devbox2/config", "r");
    assert_non_null(f);
    char cfg[1024];
    size_t n = fread(cfg, 1, sizeof(cfg) - 1, f);
    fclose(f);
    cfg[n] = '\0';
    assert_non_null(strstr(cfg, "lxc.uts.name = devbox2"));
    assert_non_null(strstr(cfg, "lxc.rootfs.path = dir:" LXCP "/devbox2/rootfs"));
    assert_null(strstr(cfg, "hwaddr"));
    assert_non_null(strstr(cfg, "lxc.net.0.link = br0"));
    /* Replacing the stopped one: the old config moved aside. */
    put(STATE "/lxc-Parked", "STOPPED\n");
    snprintf(buf, sizeof(buf), def, ",\"replace\":true,\"keep_macs\":true");
    res = NULL;
    assert_int_equal(be.define(be.ctx, "Parked", buf, &pol, &res), OK);
    assert_non_null(strstr(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "previous_config")), "config.cbackup-old-"));
    cJSON_Delete(res);
    umask(um);
}

/* B212: the config's mode is the original's, the one replaced or (a copy)
 * the original unit's, so never wider than it was; 0640, LXC's own, when
 * no original is on the host. Never the umask's 0644. */
static void test_define_config_mode(void **state)
{
    (void)state;
    const char *def = "{\"config\":\"lxc.uts.name = X\\nlxc.rootfs.path = dir:/old/rootfs\\n\"%s}";
    char buf[1024];
    cJSON *res = NULL;
    struct stat sb;
    mode_t um = umask(022);
    /* Replacing: the replaced config's 0600 stays 0600. */
    put(STATE "/lxc-Parked", "STOPPED\n");
    assert_int_equal(chmod(LXCP "/Parked/config", 0600), 0);
    snprintf(buf, sizeof(buf), def, ",\"replace\":true");
    assert_int_equal(be.define(be.ctx, "Parked", buf, &pol, &res), OK);
    cJSON_Delete(res);
    res = NULL;
    assert_int_equal(stat(LXCP "/Parked/config", &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0600);
    /* A copy of a unit no longer on the host: 0640. */
    snprintf(buf, sizeof(buf), def, ",\"name\":\"gone2\"");
    assert_int_equal(be.define(be.ctx, "Gone", buf, &pol, &res), OK);
    cJSON_Delete(res);
    assert_int_equal(stat(LXCP "/gone2/config", &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0640);
    umask(um);
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

/*
 * B190: lxc-* run in the host's cgroup namespace as well as its mount and
 * network ones. From the helper's own, lxc-start failed "Failed to open
 * 10/../..: Invalid cross-device link", so a restore could not start the
 * container again. Starting a running container or stopping a stopped one
 * is done, not an error, as Docker's 304.
 */
static void test_host_namespaces(void **state)
{
    (void)state;
    lxc_backend_fini(&be);
    helper_policy_free(&pol);
    char cwd[PATH_MAX], ns[PATH_MAX + 64];
    assert_non_null(getcwd(cwd, sizeof(cwd)));
    snprintf(ns, sizeof(ns), "nsenter = 1\nnsenter_bin = %s/tests/fake_host/nsenter\n", cwd);
    assert_int_equal(init_with(ns), 0);
    assert_int_equal(be.set_running(be.ctx, "DevBox", 0), OK);
    assert_int_equal(be.set_running(be.ctx, "DevBox", 0), OK);     /* already stopped */
    assert_int_equal(count(STATE "/lxc.log", "lxc-stop"), 1);
    assert_int_equal(be.set_running(be.ctx, "DevBox", 1), OK);
    assert_int_equal(be.set_running(be.ctx, "DevBox", 1), OK);     /* already running */
    assert_int_equal(count(STATE "/lxc.log", "lxc-start"), 1);
    assert_int_equal(count(STATE "/nsenter.log", "-t 1 -m -n -C -- lxc-start"), 1);
    assert_int_equal(count(STATE "/nsenter.log", "-t 1 -m -n -C -- lxc-stop"), 1);
    assert_int_equal(count(STATE "/nsenter.log", "-- lxc-"), count(STATE "/nsenter.log", "-m -n -C -- lxc-"));
}

/*
 * B191: an LXC mount's snapshot domain, which the helper stages an
 * in-place restore in: the rootfs's dataset and a bind entry's. Only
 * Docker located one, so every in-place LXC restore was refused.
 */
static void test_mount_root(void **state)
{
    (void)state;
    char root[PATH_MAX];
    assert_non_null(be.mount_root);
    assert_int_equal(be.mount_root(be.ctx, LXCP "/DevBox/rootfs", root, sizeof(root)), OK);
    assert_string_equal(root, LXCP);
    assert_int_equal(be.mount_root(be.ctx, MNT "/cache/appdata/dev", root, sizeof(root)), OK);
    assert_string_equal(root, MNT "/cache/appdata");
    assert_int_equal(mkdir(D "/elsewhere", 0700), 0);
    assert_int_equal(be.mount_root(be.ctx, D "/elsewhere", root, sizeof(root)), ERR_NOT_FOUND);
    assert_non_null(strstr(err_msg(), "no snapshot domain"));
    err_clear();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_capture, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nested_below, setup, teardown),
        cmocka_unit_test_setup_teardown(test_freeze_lost, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sweep_thaws, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define_config_mode, setup, teardown),
        cmocka_unit_test_setup_teardown(test_host_namespaces, setup, teardown),
        cmocka_unit_test_setup_teardown(test_mount_root, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
