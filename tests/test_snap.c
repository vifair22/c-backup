#define _GNU_SOURCE

/*
 * The helper's capture snapshots (src/helper/snap.c) over stand-ins for the
 * host's zfs and btrfs (tests/fake_host): domain resolution against the
 * policy, one command per ZFS pool, read paths into .zfs and the btrfs
 * snapshot directory, the ownership proofs before every destroy, the hold
 * journal kept in step, the sweep, and the exported-share refusal.
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

#include <cutils/log.h>

#include "helper_policy.h"
#include "host_tool.h"
#include "snap.h"
#include "test_tmp.h"

#define D      TEST_TMP "/c_backup_test_snap"
#define STATE  D "/state"
#define CACHE  D "/cache"
#define POOL   D "/pool"
#define BOOT   D "/boot"

static helper_policy_t pol;
static host_tools_t tools;
static snap_ctx_t ctx;
static int64_t next_hold;
static int holds_open;

static void sh(const char *cmd)
{
    int rc = system(cmd);
    assert_int_equal(rc, 0);
}

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static int has(const char *path, const char *text)
{
    char buf[256] = "";
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strcmp(buf, text) == 0;
}

static int count_lines(const char *path, const char *needle)
{
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char line[4096];
    int n = 0;
    while (fgets(line, sizeof(line), f)) if (strstr(line, needle)) n++;
    fclose(f);
    return n;
}

static status_t rec(void *c, const char *kind, const char *detail, int64_t *id)
{
    (void)c; (void)kind; (void)detail;
    *id = ++next_hold;
    holds_open++;
    return OK;
}

static status_t clr(void *c, int64_t id)
{
    (void)c; (void)id;
    holds_open--;
    return OK;
}

static const sp_holds_t holds = { rec, clr, NULL };

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "'") != 0) return -1;
    if (mkdir(D, 0700) || mkdir(STATE, 0700) || mkdir(CACHE, 0700) || mkdir(CACHE "/appdata", 0700) ||
        mkdir(CACHE "/appdata/app", 0700) || mkdir(CACHE "/Photos", 0700) || mkdir(CACHE "/other", 0700) ||
        mkdir(POOL, 0700) || mkdir(POOL "/appdata", 0700) || mkdir(POOL "/system", 0700) ||
        mkdir(BOOT, 0700) || mkdir(BOOT "/shares", 0700)) return -1;
    put(CACHE "/appdata/app/db", "v1");
    put(CACHE "/Photos/p.jpg", "photo");
    put(POOL "/appdata/x", "pool-v1");
    put(STATE "/datasets",
        "cache\t" CACHE "\n"
        "cache/appdata\t" CACHE "/appdata\n"
        "cache/Photos\t" CACHE "/Photos\n"
        "tank/media\t" D "/tank\n");
    mkdir(D "/tank", 0700);
    setenv("FAKE_HOST_STATE", STATE, 1);
    unsetenv("FAKE_ZFS_FAIL");
    unsetenv("FAKE_BTRFS_FAIL");
    const char *text =
        "[helper]\ndaemon_uid = 1000\ndaemon_id = 0badf00d\nstate_dir = " STATE "\n"
        "[snapshot]\nzfs_domains = cache/appdata, cache/Photos, tank/media\n"
        "btrfs_domains = " POOL "\n";
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) return -1;
    host_tools_defaults(&tools);
    tools.nsenter = 0;
    tools.host_root[0] = '\0';
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    snprintf(tools.zfs, sizeof(tools.zfs), "%s/tests/fake_host/zfs", cwd);
    snprintf(tools.btrfs, sizeof(tools.btrfs), "%s/tests/fake_host/btrfs", cwd);
    memset(&ctx, 0, sizeof(ctx));
    ctx.tools = &tools;
    ctx.pol = &pol;
    snprintf(ctx.boot_config, sizeof(ctx.boot_config), "%s", BOOT);
    holds_open = 0;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    helper_policy_free(&pol);
    return 0;
}

static void test_domains(void **state)
{
    (void)state;
    snap_dom_t d;
    assert_int_equal(snap_domain_of(&ctx, CACHE "/appdata/app", &d), OK);
    assert_int_equal(d.kind, SNAP_ZFS);
    assert_string_equal(d.dataset, "cache/appdata");
    assert_string_equal(d.root, CACHE "/appdata");
    /* In the pool's root dataset, which the policy does not list: not the
     * parent, nothing at all. */
    assert_int_equal(snap_domain_of(&ctx, CACHE "/other", &d), OK);
    assert_int_equal(d.kind, SNAP_NONE);
    assert_string_equal(d.unlisted, "cache");     /* named, for the job's warning (B253) */
    assert_int_equal(snap_domain_of(&ctx, POOL "/appdata/x", &d), OK);
    assert_int_equal(d.kind, SNAP_BTRFS);
    assert_string_equal(d.root, POOL);
    assert_int_equal(snap_domain_of(&ctx, "/elsewhere", &d), OK);
    assert_int_equal(d.kind, SNAP_NONE);
    assert_string_equal(d.unlisted, "");

    snap_dom_t ds[3];
    assert_int_equal(snap_domain_of(&ctx, CACHE "/appdata", &ds[0]), OK);
    assert_int_equal(snap_domain_of(&ctx, CACHE "/Photos", &ds[1]), OK);
    assert_int_equal(snap_instants(ds, 2), 1);          /* one pool, one instant */
    assert_int_equal(snap_domain_of(&ctx, D "/tank", &ds[2]), OK);
    assert_int_equal(snap_instants(ds, 3), 2);          /* two pools */
}

/* Every dataset of a pool in one command; read through .zfs; destroyed
 * after the owner check; the journal in step. */
static void test_zfs_capture(void **state)
{
    (void)state;
    snap_set_t s;
    memset(&s, 0, sizeof(s));
    snap_dom_t d;
    assert_int_equal(snap_domain_of(&ctx, CACHE "/appdata/app", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);          /* deduplicated */
    assert_int_equal(snap_domain_of(&ctx, CACHE "/Photos", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    assert_int_equal(s.n, 2);
    assert_int_equal(snap_take(&ctx, &s, "0badf00d", 7, &holds), OK);
    assert_int_equal(holds_open, 2);
    assert_int_equal(count_lines(STATE "/zfs.log", "zfs snapshot -o com.cbackup:owner=0badf00d"), 1);
    assert_string_equal(s.names[0], s.names[1]);

    put(CACHE "/appdata/app/db", "v2");                 /* live moves on */
    char rp[PATH_MAX];
    assert_int_equal(snap_read_path(&ctx, &s, CACHE "/appdata/app", rp, sizeof(rp)), OK);
    char want[PATH_MAX];
    snprintf(want, sizeof(want), CACHE "/appdata/.zfs/snapshot/%s/app", s.names[0]);
    assert_string_equal(rp, want);
    char f[PATH_MAX + 8];
    snprintf(f, sizeof(f), "%s/db", rp);
    assert_true(has(f, "v1"));
    assert_int_not_equal(snap_read_path(&ctx, &s, "/elsewhere", rp, sizeof(rp)), OK);

    assert_int_equal(snap_release(&ctx, &s, "0badf00d", &holds), OK);
    assert_int_equal(holds_open, 0);
    struct stat sb;
    assert_int_not_equal(stat(want, &sb), 0);
    /* Releasing what is already gone is fine. */
    char det[PATH_MAX];
    snprintf(det, sizeof(det), "zfs:cache/appdata@%s", s.names[0]);
    assert_int_equal(snap_release_detail(&ctx, det, "0badf00d"), OK);
}

static void test_zfs_failure_and_sweep(void **state)
{
    (void)state;
    snap_set_t s;
    memset(&s, 0, sizeof(s));
    snap_dom_t d;
    assert_int_equal(snap_domain_of(&ctx, CACHE "/appdata", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    setenv("FAKE_ZFS_FAIL", "snapshot", 1);
    assert_int_not_equal(snap_take(&ctx, &s, "0badf00d", 8, &holds), OK);
    assert_non_null(strstr(err_msg(), "injected failure"));
    assert_int_equal(holds_open, 0);
    unsetenv("FAKE_ZFS_FAIL");

    /* Leftovers: ours from a dead job, ours from a live one, a foreign
     * owner wearing our name, another daemon's. */
    snap_set_t a, b;
    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    assert_int_equal(snap_set_add(&a, &d), OK);
    assert_int_equal(snap_set_add(&b, &d), OK);
    assert_int_equal(snap_take(&ctx, &a, "0badf00d", 21, NULL), OK);
    assert_int_equal(snap_take(&ctx, &b, "0badf00d", 22, NULL), OK);
    FILE *f = fopen(STATE "/snaps", "a");
    assert_non_null(f);
    fprintf(f, "cache/appdata@cbk-0badf00d-23-0\tsomeoneelse\n");
    fprintf(f, "cache/appdata@cbk-deadbeef-5-0\tdeadbeef\n");
    fprintf(f, "cache/other@cbk-0badf00d-24-0\t0badf00d\n");
    fclose(f);
    int64_t live[] = { 22 };
    unsigned removed = 0, foreign = 0;
    assert_int_equal(snap_sweep(&ctx, "0badf00d", live, 1, &removed, &foreign), OK);
    assert_int_equal(removed, 1);
    assert_int_equal(foreign, 2);
    assert_int_equal(count_lines(STATE "/snaps", "cbk-0badf00d-21-"), 0);
    assert_int_equal(count_lines(STATE "/snaps", "cbk-0badf00d-22-"), 1);
    assert_int_equal(count_lines(STATE "/snaps", "someoneelse"), 1);
    assert_int_equal(count_lines(STATE "/snaps", "cache/other@"), 1);   /* not in the policy */
    /* A foreign owner is refused by name too. */
    assert_int_not_equal(snap_release_detail(&ctx, "zfs:cache/appdata@cbk-0badf00d-23-0", "0badf00d"), OK);
    assert_int_not_equal(snap_release_detail(&ctx, "zfs:cache/appdata@manual", "0badf00d"), OK);
    err_clear();
    assert_int_equal(snap_release(&ctx, &b, "0badf00d", NULL), OK);
}

static void test_btrfs_capture(void **state)
{
    (void)state;
    put(BOOT "/shares/system.cfg", "shareUseCache=\"only\"\nshareExport=\"-\"\nshareExportNFS=\"-\"\n");
    snap_set_t s;
    memset(&s, 0, sizeof(s));
    snap_dom_t d;
    assert_int_equal(snap_domain_of(&ctx, POOL "/appdata/x", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    assert_int_equal(snap_take(&ctx, &s, "0badf00d", 9, &holds), OK);
    assert_int_equal(holds_open, 1);
    put(POOL "/appdata/x", "pool-v2");
    char rp[PATH_MAX];
    assert_int_equal(snap_read_path(&ctx, &s, POOL "/appdata/x", rp, sizeof(rp)), OK);
    char want[PATH_MAX];
    snprintf(want, sizeof(want), POOL "/system/.cbackup-snapshots/%s/appdata/x", s.names[0]);
    assert_string_equal(rp, want);
    assert_true(has(rp, "pool-v1"));
    assert_int_equal(snap_release(&ctx, &s, "0badf00d", &holds), OK);
    assert_int_equal(holds_open, 0);
    struct stat sb;
    assert_int_not_equal(stat(rp, &sb), 0);

    /* Not ours by the uuid check: a read-only subvolume with our name whose
     * parent is something else is left alone. */
    sh("mkdir -p '" POOL "/system/.cbackup-snapshots/cbk-0badf00d-30-0'");
    FILE *f = fopen(STATE "/subvols", "a");
    assert_non_null(f);
    fprintf(f, "%s\tx-uuid\tnot-the-pool\ttrue\n", POOL "/system/.cbackup-snapshots/cbk-0badf00d-30-0");
    fclose(f);
    assert_int_not_equal(snap_release_detail(&ctx, "btrfs:" POOL "/system/.cbackup-snapshots/cbk-0badf00d-30-0",
                                             "0badf00d"), OK);
    assert_non_null(strstr(err_msg(), "left alone"));
    err_clear();
    /* And a sweep: our dead job's snapshot goes, the impostor stays. */
    snap_set_t a;
    memset(&a, 0, sizeof(a));
    assert_int_equal(snap_set_add(&a, &d), OK);
    assert_int_equal(snap_take(&ctx, &a, "0badf00d", 31, NULL), OK);
    unsigned removed = 0, foreign = 0;
    assert_int_equal(snap_sweep(&ctx, "0badf00d", NULL, 0, &removed, &foreign), OK);
    assert_int_equal(removed, 1);
    assert_int_equal(stat(POOL "/system/.cbackup-snapshots/cbk-0badf00d-30-0", &sb), 0);
}

static int fake_subvol(const snap_ctx_t *c, const char *path)
{
    (void)c;
    return strcmp(path, POOL "/appdata/vm") == 0;
}

/* A nested subvolume is its own domain (the top-level snapshot shows it
 * empty), snapshotted into the pool's directory, and counts as a second
 * instant. */
static void test_btrfs_nested(void **state)
{
    (void)state;
    sh("mkdir -p '" POOL "/appdata/vm'");
    put(POOL "/appdata/vm/disk", "vm-v1");
    FILE *f = fopen(STATE "/subvols", "a");
    assert_non_null(f);
    fprintf(f, "%s\tsub-uuid\t-\tfalse\n", POOL "/appdata/vm");
    fclose(f);
    ctx.is_subvol = fake_subvol;
    snap_dom_t a, b;
    assert_int_equal(snap_domain_of(&ctx, POOL "/appdata/vm/disk", &a), OK);
    assert_int_equal(a.kind, SNAP_BTRFS);
    assert_string_equal(a.root, POOL "/appdata/vm");
    assert_string_equal(a.pool, POOL);
    assert_int_equal(snap_domain_of(&ctx, POOL "/appdata/x", &b), OK);
    assert_string_equal(b.root, POOL);
    snap_set_t s;
    memset(&s, 0, sizeof(s));
    assert_int_equal(snap_set_add(&s, &a), OK);
    assert_int_equal(snap_set_add(&s, &b), OK);
    assert_int_equal(snap_instants(s.doms, s.n), 2);
    assert_int_equal(snap_take(&ctx, &s, "0badf00d", 12, &holds), OK);
    char rp[PATH_MAX], want[PATH_MAX];
    assert_int_equal(snap_read_path(&ctx, &s, POOL "/appdata/vm/disk", rp, sizeof(rp)), OK);
    snprintf(want, sizeof(want), POOL "/system/.cbackup-snapshots/%s/disk", s.names[0]);
    assert_string_equal(rp, want);
    assert_true(has(rp, "vm-v1"));
    assert_int_equal(snap_release(&ctx, &s, "0badf00d", &holds), OK);
    assert_int_equal(holds_open, 0);
}

static char g_log[8192];

static void capture_log(const char *ts, const char *level, const char *func, const char *message,
                        void *ud)
{
    (void)ts; (void)func; (void)ud;
    size_t have = strlen(g_log);
    snprintf(g_log + have, sizeof(g_log) - have, "%s %s\n", level, message);
}

static int logged(const char *text)
{
    return strstr(g_log, text) != NULL;
}

/*
 * HELPER-SNAP-POLICY: every snapshot the helper creates or destroys is one
 * INFO line naming the snapshot and its dataset or subvolume, so an
 * operator can see from the helper's log alone which datasets a capture
 * touched, and for how long. A release of one already gone logs nothing.
 */
static void test_snapshots_are_logged(void **state)
{
    (void)state;
    log_level_t was = log_get_level();
    log_set_level(LOG_INFO);
    g_log[0] = '\0';
    int h = log_stream_register(capture_log, NULL);
    assert_true(h >= 0);

    snap_set_t s;
    memset(&s, 0, sizeof(s));
    snap_dom_t d;
    assert_int_equal(snap_domain_of(&ctx, CACHE "/appdata/app", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    assert_int_equal(snap_domain_of(&ctx, POOL "/appdata/x", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    assert_int_equal(snap_take(&ctx, &s, "0badf00d", 40, &holds), OK);
    char z[256], b[PATH_MAX + 128], zd[256], bd[PATH_MAX + 128];
    snprintf(z, sizeof(z), "snap: created ZFS snapshot cache/appdata@%s of dataset cache/appdata",
             s.names[0]);
    snprintf(b, sizeof(b), "snap: created btrfs snapshot " POOL "/system/.cbackup-snapshots/%s "
             "of subvolume " POOL, s.names[1]);
    snprintf(zd, sizeof(zd), "snap: destroyed ZFS snapshot cache/appdata@%s of dataset "
             "cache/appdata", s.names[0]);
    snprintf(bd, sizeof(bd), "snap: destroyed btrfs snapshot " POOL "/system/.cbackup-snapshots/%s "
             "in pool " POOL, s.names[1]);
    if (!logged(z) || !logged(b)) fail_msg("no create lines:\n%s", g_log);
    if (logged("snap: destroyed")) fail_msg("destroyed early:\n%s", g_log);
    assert_int_equal(snap_release(&ctx, &s, "0badf00d", &holds), OK);
    if (!logged(zd) || !logged(bd)) fail_msg("no destroy lines:\n%s", g_log);

    /* Already gone: no line. */
    g_log[0] = '\0';
    char det[256];
    snprintf(det, sizeof(det), "zfs:cache/appdata@%s", s.names[0]);
    assert_int_equal(snap_release_detail(&ctx, det, "0badf00d"), OK);
    if (logged("snap: destroyed")) fail_msg("a release of nothing logged:\n%s", g_log);

    /* The sweep's destroys are logged the same way. */
    snap_set_t a;
    memset(&a, 0, sizeof(a));
    assert_int_equal(snap_domain_of(&ctx, CACHE "/Photos", &d), OK);
    assert_int_equal(snap_set_add(&a, &d), OK);
    assert_int_equal(snap_take(&ctx, &a, "0badf00d", 41, NULL), OK);
    unsigned removed = 0, foreign = 0;
    assert_int_equal(snap_sweep(&ctx, "0badf00d", NULL, 0, &removed, &foreign), OK);
    assert_int_equal(removed, 1);
    snprintf(zd, sizeof(zd), "info snap: destroyed ZFS snapshot cache/Photos@%s of dataset "
             "cache/Photos", a.names[0]);
    log_stream_unregister(h);
    log_set_level(was);
    if (!logged(zd)) fail_msg("no sweep destroy line:\n%s", g_log);
}

static void test_exported_share_refused(void **state)
{
    (void)state;
    put(BOOT "/shares/system.cfg", "shareUseCache=\"only\"\nshareExport=\"e\"\n");
    snap_set_t s;
    memset(&s, 0, sizeof(s));
    snap_dom_t d;
    assert_int_equal(snap_domain_of(&ctx, POOL "/appdata", &d), OK);
    assert_int_equal(snap_set_add(&s, &d), OK);
    assert_int_not_equal(snap_take(&ctx, &s, "0badf00d", 10, &holds), OK);
    assert_non_null(strstr(err_msg(), "exported"));
    assert_int_equal(holds_open, 0);
    put(BOOT "/shares/system.cfg", "shareExportNFS=\"e\"\n");
    assert_int_not_equal(snap_check_snapdir(&ctx, POOL), OK);
    /* At the top of the pool rather than in a share: refused as well. */
    snprintf(ctx.btrfs_snapdir, sizeof(ctx.btrfs_snapdir), POOL "/.cbackup-snapshots");
    assert_int_not_equal(snap_check_snapdir(&ctx, POOL), OK);
    assert_non_null(strstr(err_msg(), "inside a share"));
    err_clear();
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
        cmocka_unit_test_setup_teardown(test_domains, setup, teardown),
        cmocka_unit_test_setup_teardown(test_zfs_capture, setup, teardown),
        cmocka_unit_test_setup_teardown(test_zfs_failure_and_sweep, setup, teardown),
        cmocka_unit_test_setup_teardown(test_btrfs_capture, setup, teardown),
        cmocka_unit_test_setup_teardown(test_btrfs_nested, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exported_share_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshots_are_logged, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
