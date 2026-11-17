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
    setenv("FAKE_HOST_STATE", STATE, 1);
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    char text[4096];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = 1000\ndaemon_id = 0badf00d\nstate_dir = " STATE "\n"
             "[snapshot]\nzfs_domains = cache/lxc, cache/appdata\n"
             "[host]\nnsenter = 0\nhost_root =\nzfs = %s/tests/fake_host/zfs\nlxc_dir = %s/tests/fake_host/lxc\n"
             "user_root = " MNT "/user\nmnt_root = " MNT "\nboot_config = " D "/boot\n"
             "[docker]\ninclude_roots = " MNT "/cache/appdata\n"
             "[lxc]\nenabled = 1\npath = " LXCP "\n", cwd, cwd);
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
    sp_sweep_report_t rep;
    assert_int_equal(be.ops->sweep(be.ctx, &rq, &rep), OK);
    assert_int_equal(rel, 1);
    assert_int_equal(be.unit_state(be.ctx, "DevBox"), SP_STATE_RUNNING);
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
    snprintf(buf, sizeof(buf), def, ",\"name\":\"devbox2\"");
    assert_int_equal(be.define(be.ctx, "DevBox", buf, &pol, &res), OK);
    cJSON_Delete(res);
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
        cmocka_unit_test_setup_teardown(test_capture, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sweep_thaws, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
