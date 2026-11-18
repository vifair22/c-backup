#define _GNU_SOURCE

/*
 * capture.begin_paths (docs/db-detection.md 9.4) through the helper's
 * server, with the paths backend over stand-ins for the host's zfs and
 * btrfs (tests/fake_host): each path resolved by the helper and placed in
 * its policy domain, one snapshot command per ZFS pool, read roots inside
 * the helper's own snapshots, paths it cannot snapshot reported with the
 * reason, mounts below a path listed, and the snapshots released on every
 * way a capture ends: capture.end, lease.release, the lease running out, a
 * failed take, and a helper that died holding them (the start-up sweep).
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
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include "artefact.h"
#include "helper_backend.h"
#include "helper_policy.h"
#include "helper_server.h"
#include "paths_backend.h"
#include "test_tmp.h"

#define D      TEST_TMP "/c_backup_test_begin_paths"
#define HSTATE D "/hstate"
#define FSTATE D "/fstate"
#define CACHE  D "/cache"
#define POOL   D "/pool"
#define MINFO  D "/mountinfo"

static helper_policy_t  pol;
static helper_backend_t pb;
static helper_server_t *srv;
static char             fake_dir[1024];

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static char *slurp(const char *path)
{
    static char buf[65536];
    buf[0] = '\0';
    FILE *f = fopen(path, "r");
    if (!f) return buf;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static int count(const char *path, const char *needle)
{
    int n = 0;
    for (const char *p = slurp(path); (p = strstr(p, needle)) != NULL; p++) n++;
    return n;
}

static void hstart(unsigned capture_s, unsigned per_hour)
{
    char text[16384];
    snprintf(text, sizeof(text),
        "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = " HSTATE "\n"
        "capture_max_s = %u\nmax_captures_per_hour = %u\n"
        "[snapshot]\nzfs_domains = cache/appdata, cache/Photos\n"
        "btrfs_domains = " POOL "\n"
        "[host]\nnsenter = 0\nhost_root =\nzfs = %s/zfs\nbtrfs = %s/btrfs\n"
        "realpath = /usr/bin/realpath\nmountinfo = " MINFO "\n",
        (unsigned)getuid(), capture_s, per_hour, fake_dir, fake_dir);
    assert_int_equal(helper_policy_parse(text, &pol), OK);
    assert_int_equal(paths_backend_init(&pol, &pb), OK);
    helper_server_cfg_t cfg = { &pol, NULL, 0, 50, &pb, 1 };
    assert_int_equal(helper_server_start(&cfg, &srv), OK);
}

static void hstop(void)
{
    if (!srv) return;
    helper_server_stop(srv);
    srv = NULL;
    paths_backend_fini(&pb);
    helper_policy_free(&pol);
}

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "'") != 0) return -1;
    if (mkdir(D, 0700) || mkdir(HSTATE, 0700) || mkdir(FSTATE, 0700) || mkdir(CACHE, 0700) ||
        mkdir(CACHE "/appdata", 0700) || mkdir(CACHE "/appdata/app", 0700) ||
        mkdir(CACHE "/appdata/app/sub", 0700) || mkdir(CACHE "/Photos", 0700) ||
        mkdir(CACHE "/other", 0700) || mkdir(POOL, 0700) || mkdir(POOL "/appdata", 0700) ||
        mkdir(POOL "/system", 0700)) return -1;
    put(CACHE "/appdata/app/db", "v1");
    put(CACHE "/Photos/p.jpg", "photo");
    put(CACHE "/other/o", "not in the policy");
    put(POOL "/appdata/x", "pool-v1");
    put(FSTATE "/datasets",
        "cache\t" CACHE "\n"
        "cache/appdata\t" CACHE "/appdata\n"
        "cache/Photos\t" CACHE "/Photos\n");
    /* The host's mount table: the root, a filesystem mounted below a
     * source, and a ZFS snapshot automount, which is never data. */
    put(MINFO,
        "20 1 0:20 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n"
        "30 20 0:30 / " CACHE "/appdata/app/sub rw,relatime - tmpfs tmpfs rw\n"
        "31 20 0:31 / " CACHE "/appdata/.zfs/snapshot/foreign rw,relatime - zfs cache/appdata@foreign rw\n");
    setenv("FAKE_HOST_STATE", FSTATE, 1);
    unsetenv("FAKE_ZFS_FAIL");
    unsetenv("FAKE_BTRFS_FAIL");
    char cwd[512];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    snprintf(fake_dir, sizeof(fake_dir), "%s/tests/fake_host", cwd);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    hstop();
    if (system("rm -rf '" D "'") != 0) return -1;
    return 0;
}

static cJSON *call(const char *op, const char *args)
{
    char req[8192];
    snprintf(req, sizeof(req), "{\"v\":1,\"id\":9,\"op\":\"%s\",\"args\":%s}", op, args);
    char *resp = NULL;
    int pfd = -1;
    helper_server_handle(srv, req, strlen(req), &resp, &pfd);
    assert_non_null(resp);
    assert_int_equal(pfd, -1);
    cJSON *o = cJSON_Parse(resp);
    free(resp);
    assert_non_null(o);
    return o;
}

static int ok(const cJSON *o) { return cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "ok")); }

static const cJSON *result(const cJSON *o) { return cJSON_GetObjectItemCaseSensitive(o, "result"); }

static const char *field(const cJSON *res, const char *map, const char *key)
{
    return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(res, map), key));
}

static void end_capture(const char *op, const char *tok)
{
    char a[128];
    snprintf(a, sizeof(a), "{\"capture\":\"%s\"}", tok);
    cJSON *o = call(op, a);
    assert_true(ok(o));
    cJSON_Delete(o);
}

/* ------------------------------------------------------------------ */

/* Two datasets of one pool: one snapshot command, both read roots inside
 * it, holding what the files held at that instant. */
static void test_zfs_one_instant(void **state)
{
    (void)state;
    hstart(86400, 60);
    cJSON *o = call("capture.begin_paths", "{\"paths\":[\"" CACHE "/appdata/app\",\"" CACHE "/Photos\"]}");
    assert_true(ok(o));
    const cJSON *res = result(o);
    const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture"));
    assert_non_null(tok);
    assert_int_equal(count(FSTATE "/zfs.log", "zfs snapshot"), 1);
    assert_int_equal(count(FSTATE "/zfs.log", "com.cbackup:owner=0badf00d"), 1);
    assert_string_equal(field(res, "kinds", CACHE "/appdata/app"), "zfs");
    assert_string_equal(field(res, "kinds", CACHE "/Photos"), "zfs");

    char name[ARTEFACT_NAME_MAX], want[PATH_MAX];
    snprintf(name, sizeof(name), "%s", field(res, "snapshots", "cache/appdata"));
    assert_string_equal(field(res, "snapshots", "cache/Photos"), name);
    snprintf(want, sizeof(want), CACHE "/appdata/.zfs/snapshot/%s/app", name);
    const char *rr = field(res, "read_roots", CACHE "/appdata/app");
    assert_string_equal(rr, want);
    snprintf(want, sizeof(want), CACHE "/Photos/.zfs/snapshot/%s", name);
    assert_string_equal(field(res, "read_roots", CACHE "/Photos"), want);

    /* The live file moves on; the read root keeps the instant. */
    put(CACHE "/appdata/app/db", "v2");
    char f[PATH_MAX + 8];
    snprintf(f, sizeof(f), "%s/db", rr);
    assert_string_equal(slurp(f), "v1");

    /* The filesystem below the source is reported; the automount is not. */
    const cJSON *n = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(res, "nested"),
                                                      CACHE "/appdata/app");
    assert_non_null(n);
    assert_int_equal((int)cJSON_GetNumberValue(cJSON_GetObjectItemCaseSensitive(n, "total")), 1);
    assert_string_equal(cJSON_GetStringValue(cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(n, "mounts"), 0)),
                        CACHE "/appdata/app/sub");
    assert_null(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(res, "nested"), CACHE "/Photos"));

    /* Each snapshot journalled; capture.end destroys both and clears them. */
    assert_int_equal(helper_server_captures(srv), 1);
    assert_int_equal(helper_server_holds(srv), 2);
    end_capture("capture.end", tok);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_string_equal(slurp(FSTATE "/snaps"), "");
    snprintf(want, sizeof(want), CACHE "/appdata/.zfs/snapshot/%s", name);
    assert_int_equal(access(want, F_OK), -1);
    cJSON_Delete(o);
}

/* btrfs: the pool's top level, read under the snapshot directory; released
 * by lease.release, which ends a capture with no unit sessions. */
static void test_btrfs_and_lease_release(void **state)
{
    (void)state;
    hstart(86400, 60);
    cJSON *o = call("capture.begin_paths", "{\"paths\":[\"" POOL "/appdata\"]}");
    assert_true(ok(o));
    const cJSON *res = result(o);
    assert_string_equal(field(res, "kinds", POOL "/appdata"), "btrfs");
    const char *name = field(res, "snapshots", POOL);
    assert_non_null(name);
    char want[PATH_MAX];
    snprintf(want, sizeof(want), POOL "/system/.cbackup-snapshots/%s/appdata", name);
    assert_string_equal(field(res, "read_roots", POOL "/appdata"), want);
    put(POOL "/appdata/x", "pool-v2");
    char f[PATH_MAX + 8];
    snprintf(f, sizeof(f), "%s/x", want);
    assert_string_equal(slurp(f), "pool-v1");
    end_capture("lease.release", cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture")));
    snprintf(want, sizeof(want), POOL "/system/.cbackup-snapshots/%s", name);
    assert_int_equal(access(want, F_OK), -1);
    assert_int_equal(helper_server_holds(srv), 0);
    cJSON_Delete(o);
}

/* A request mixing paths it can and cannot snapshot: the capture covers
 * the first, and each of the others comes back with its reason. */
static void test_unsnapshottable_paths_are_reported(void **state)
{
    (void)state;
    hstart(86400, 60);
    cJSON *o = call("capture.begin_paths",
                    "{\"paths\":[\"" CACHE "/appdata/app\",\"" CACHE "/other\",\"" D "/missing\",\"/proc/self\","
                    "\"" CACHE "/appdata/app\"]}");
    assert_true(ok(o));
    const cJSON *res = result(o);
    assert_non_null(cJSON_GetObjectItemCaseSensitive(res, "capture"));
    assert_int_equal(cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(res, "read_roots")), 1);
    assert_non_null(strstr(field(res, "live", CACHE "/other"), "no ZFS dataset or btrfs pool"));
    assert_non_null(strstr(field(res, "live", D "/missing"), "cannot be resolved"));
    assert_non_null(strstr(field(res, "live", "/proc/self"), "host facility"));
    assert_int_equal(cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(res, "live")), 3);
    end_capture("capture.end", cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture")));
    cJSON_Delete(o);
}

/* Nothing it can snapshot: no capture, nothing taken, and the hourly
 * budget not spent on it. */
static void test_nothing_to_snapshot_takes_no_capture(void **state)
{
    (void)state;
    hstart(86400, 1);
    cJSON *o = call("capture.begin_paths", "{\"paths\":[\"" CACHE "/other\"]}");
    assert_true(ok(o));
    assert_null(cJSON_GetObjectItemCaseSensitive(result(o), "capture"));
    assert_int_equal(count(FSTATE "/zfs.log", "zfs snapshot"), 0);
    cJSON_Delete(o);
    o = call("capture.begin_paths", "{\"paths\":[\"" CACHE "/Photos\"]}");
    assert_true(ok(o));
    const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(result(o), "capture"));
    assert_non_null(tok);
    end_capture("capture.end", tok);
    cJSON_Delete(o);
    /* The one capture an hour is spent now. */
    o = call("capture.begin_paths", "{\"paths\":[\"" CACHE "/Photos\"]}");
    assert_false(ok(o));
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
                            cJSON_GetObjectItemCaseSensitive(o, "error"), "code")), "busy");
    cJSON_Delete(o);
}

/* A snapshot that fails leaves no capture, no hold and no snapshot. */
static void test_failed_take_leaves_nothing(void **state)
{
    (void)state;
    hstart(86400, 60);
    setenv("FAKE_BTRFS_FAIL", "snapshot", 1);
    cJSON *o = call("capture.begin_paths", "{\"paths\":[\"" CACHE "/appdata/app\",\"" POOL "/appdata\"]}");
    unsetenv("FAKE_BTRFS_FAIL");
    assert_false(ok(o));
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
                            cJSON_GetObjectItemCaseSensitive(o, "error"), "code")), "failed");
    cJSON_Delete(o);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    /* The ZFS snapshot taken before the btrfs one failed is gone again. */
    assert_int_equal(count(FSTATE "/zfs.log", "zfs snapshot"), 1);
    assert_string_equal(slurp(FSTATE "/snaps"), "");
}

/* A capture nobody ends is destroyed when its lease runs out. */
static void test_lease_expiry_destroys(void **state)
{
    (void)state;
    hstart(1, 60);
    cJSON *o = call("capture.begin_paths", "{\"paths\":[\"" CACHE "/Photos\"]}");
    assert_true(ok(o));
    cJSON_Delete(o);
    assert_int_not_equal(strlen(slurp(FSTATE "/snaps")), 0);
    assert_true(helper_server_tick(srv, ((int64_t)time(NULL) + 5) * 1000) >= 1);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_string_equal(slurp(FSTATE "/snaps"), "");
}

/*
 * A helper that died holding a paths capture: its journal names the
 * snapshot, and an unjournalled one of ours is also left on the pool. The
 * next start releases the first and sweeps the second, with no unit
 * provider configured at all.
 */
static void test_startup_sweep_after_a_crash(void **state)
{
    (void)state;
    char n1[ARTEFACT_NAME_MAX], n2[ARTEFACT_NAME_MAX], cmd[PATH_MAX * 2];
    assert_int_equal(artefact_name(n1, sizeof(n1), "0badf00d", 41, 0), 0);
    assert_int_equal(artefact_name(n2, sizeof(n2), "0badf00d", 42, 0), 0);
    snprintf(cmd, sizeof(cmd), "%s/zfs snapshot -o com.cbackup:owner=0badf00d cache/appdata@%s cache/Photos@%s",
             fake_dir, n1, n2);
    assert_int_equal(system(cmd), 0);
    char j[1024];
    snprintf(j, sizeof(j), "[{\"id\":1,\"unit\":\"paths:capture\",\"kind\":\"fs_snapshot\","
             "\"detail\":\"zfs:cache/appdata@%s\",\"created\":1}]", n1);
    put(HSTATE "/holds.json", j);
    hstart(86400, 60);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_string_equal(slurp(FSTATE "/snaps"), "");

    /* And hello lists no "paths" provider, but says it can snapshot paths. */
    cJSON *o = call("hello", "{\"client_version\":1}");
    assert_true(ok(o));
    const cJSON *p;
    cJSON_ArrayForEach(p, cJSON_GetObjectItemCaseSensitive(result(o), "providers"))
        assert_string_not_equal(cJSON_GetStringValue(p), "paths");
    assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(result(o), "path_snapshots")));
    cJSON_Delete(o);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_zfs_one_instant, setup, teardown),
        cmocka_unit_test_setup_teardown(test_btrfs_and_lease_release, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unsnapshottable_paths_are_reported, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nothing_to_snapshot_takes_no_capture, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_take_leaves_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lease_expiry_destroys, setup, teardown),
        cmocka_unit_test_setup_teardown(test_startup_sweep_after_a_crash, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
