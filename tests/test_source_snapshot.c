#define _GNU_SOURCE

/*
 * Source snapshot mode's daemon side (src/ops/source_snapshot.c) against
 * the helper's server with its paths backend over the stand-in zfs, on a
 * real socket through the daemon's helper client: modes read from the
 * repository's configuration; strict sources that cannot be snapshotted
 * fail with every path and reason named and nothing left held; warn sources
 * read live with the warning and result naming them; test dir domains and
 * mounts below a source never pass for a snapshot; the capture ended.
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
#include <cutils/db.h>

#include "helper_backend.h"
#include "helper_fixture.h"
#include "helper_policy.h"
#include "helper_server.h"
#include "paths_backend.h"
#include "remote_provider.h"
#include "db_write.h"
#include "repo.h"
#include "source_snapshot.h"
#include "test_tmp.h"

#define D      TEST_TMP "/c_backup_test_srcsnap"
#define HSTATE D "/hstate"
#define FSTATE D "/fstate"
#define DS     D "/zfs/data"
#define PLAIN  D "/plain"
#define DIRDOM D "/dirdom"
#define MINFO  D "/mountinfo"
#define SOCK   D "/h.sock"
#define DBP    D "/state.db"

extern const db_migration_t app_migrations[];

static helper_policy_t  pol;
static helper_backend_t pb;
static helper_server_t *srv;
static cutils_db_t     *db;
static int64_t          repo_id;
static char             fake_dir[1024];
static const remote_backoff_t k_fast = { 50, 200, 200 };

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static char *slurp(const char *path)
{
    static char buf[16384];
    buf[0] = '\0';
    FILE *f = fopen(path, "r");
    if (!f) return buf;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "'") != 0) return -1;
    if (mkdir(D, 0700) || mkdir(HSTATE, 0700) || mkdir(FSTATE, 0700) || mkdir(D "/zfs", 0700) ||
        mkdir(DS, 0700) || mkdir(DS "/app", 0700) || mkdir(DS "/app/sub", 0700) ||
        mkdir(DS "/other", 0700) || mkdir(PLAIN, 0700) || mkdir(DIRDOM, 0700) ||
        mkdir(DS "/domains", 0700)) return -1;
    put(DS "/app/f", "f");
    put(FSTATE "/datasets", "tank/data\t" DS "\n");
    put(MINFO,
        "20 1 0:20 / / rw - ext4 /dev/root rw\n"
        "30 20 0:30 / " DS "/other rw - tmpfs tmpfs rw\n"
        /* FS1's /mnt/cache/domains: a child dataset per VM. */
        "31 20 0:31 / " DS "/domains/vm1 rw - zfs tank/data/domains/vm1 rw\n"
        "32 20 0:32 / " DS "/domains/vm2 rw - zfs tank/data/domains/vm2 rw\n"
        "33 20 0:33 / " DS "/domains/vm3 rw - zfs tank/data/domains/vm3 rw\n"
        "34 20 0:34 / " DS "/domains/vm4 rw - zfs tank/data/domains/vm4 rw\n");
    setenv("FAKE_HOST_STATE", FSTATE, 1);
    char cwd[512];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    snprintf(fake_dir, sizeof(fake_dir), "%s/tests/fake_host", cwd);
    char text[16384];
    snprintf(text, sizeof(text),
        "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = " HSTATE "\nsocket = " SOCK "\n"
        "[snapshot]\nzfs_domains = tank/data\ndir_domains = " DIRDOM "\n"
        "[host]\nnsenter = 0\nhost_root =\nzfs = %s/zfs\nrealpath = /usr/bin/realpath\n"
        "mountinfo = " MINFO "\n", (unsigned)getuid(), fake_dir);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (paths_backend_init(&pol, &pb) != OK) return -1;
    helper_server_cfg_t cfg = { &pol, NULL, 1, 50, &pb, 1 };
    if (helper_server_start(&cfg, &srv) != OK) return -1;
    if (db_open(&db, DBP) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, D "/repo", "t", &repo_id) != OK) return -1;
    return remote_start(SOCK, "0badf00d", &k_fast) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    remote_stop();
    if (srv) helper_server_stop(srv);
    srv = NULL;
    paths_backend_fini(&pb);
    helper_policy_free(&pol);
    db_close(db);
    if (system("rm -rf '" D "'") != 0) return -1;
    return 0;
}

static void drained(void)
{
    for (int i = 0; i < 200 && (helper_server_captures(srv) || helper_server_holds(srv)); i++) {
        struct timespec ts = { 0, 10 * 1000000L };
        nanosleep(&ts, NULL);
    }
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_string_equal(slurp(FSTATE "/snaps"), "");
}

/* ------------------------------------------------------------------ */

static void test_modes(void **state)
{
    (void)state;
    assert_int_equal(src_snap_mode_parse("strict"), SRC_SNAP_STRICT);
    assert_int_equal(src_snap_mode_parse("warn"), SRC_SNAP_WARN);
    assert_int_equal(src_snap_mode_parse("off"), SRC_SNAP_OFF);
    assert_int_equal(src_snap_mode_parse("on"), -1);
    assert_int_equal(src_snap_mode_parse(NULL), -1);
    assert_string_equal(src_snap_mode_name(SRC_SNAP_WARN), "warn");
    assert_string_equal(src_snap_mode_name(SRC_SNAP_OFF), "off");

    char r[32];
    snprintf(r, sizeof(r), "%lld", (long long)repo_id);
    assert_int_equal(db_write(db, "INSERT INTO repo_backup_paths (repo_id, kind, value) VALUES"
                                              " (?, 'source', '/a'), (?, 'source', '/b'), (?, 'source', '/c'),"
                                              " (?, '" SRC_SNAP_KIND_STRICT "', '/a'),"
                                              " (?, '" SRC_SNAP_KIND_WARN "', '/b')",
                                          (const char *[]){ r, r, r, r, r, NULL }, NULL), 0);
    const char *paths[] = { "/c", "/b", "/a" };
    int modes[3];
    assert_int_equal(src_snap_modes_load(db, repo_id, paths, 3, modes), OK);
    assert_int_equal(modes[0], SRC_SNAP_OFF);
    assert_int_equal(modes[1], SRC_SNAP_WARN);
    assert_int_equal(modes[2], SRC_SNAP_STRICT);
}

/* No source opted in: nothing asked of the helper, nothing to remap. */
static void test_nothing_opted_in(void **state)
{
    (void)state;
    const char *paths[] = { DS "/app" };
    int modes[] = { SRC_SNAP_OFF };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, 1, 0, &s), OK);
    assert_null(s.read_paths);
    assert_null(s.capture);
    assert_null(src_snap_result_json(&s, paths));
    char w[256];
    src_snap_describe_live(&s, paths, w, sizeof(w));
    assert_string_equal(w, "");
    src_snap_end(&s);
    assert_int_equal(helper_server_captures(srv), 0);
}

/* A strict source on ZFS: read under the snapshot, the capture ended. */
static void test_strict_snapshot(void **state)
{
    (void)state;
    const char *paths[] = { PLAIN, DS "/app" };
    int modes[] = { SRC_SNAP_OFF, SRC_SNAP_STRICT };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, 2, 0, &s), OK);
    assert_non_null(s.capture);
    assert_non_null(s.read_paths);
    assert_null(s.read_paths[0]);
    assert_non_null(strstr(s.read_paths[1], DS "/.zfs/snapshot/cbk-0badf00d-"));
    char f[PATH_MAX + 8];
    snprintf(f, sizeof(f), "%s/f", s.read_paths[1]);
    assert_string_equal(slurp(f), "f");
    assert_int_equal(s.n_snapshot, 1);
    assert_int_equal(s.n_live, 0);
    char *js = src_snap_result_json(&s, paths);
    assert_non_null(js);
    assert_string_equal(js, ",\"source_snapshots\":{\"" DS "/app\":\"zfs\"},\"live_sources\":[]");
    free(js);
    assert_int_equal(helper_server_captures(srv), 1);
    src_snap_end(&s);
    drained();
}

/* Strict sources that cannot be snapshotted fail, each named with its
 * reason, and the capture taken for the one that could is ended. */
static void test_strict_failure_names_each(void **state)
{
    (void)state;
    const char *paths[] = { DS "/app", PLAIN, DIRDOM, DS "/missing" };
    int modes[] = { SRC_SNAP_STRICT, SRC_SNAP_STRICT, SRC_SNAP_STRICT, SRC_SNAP_STRICT };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, 4, 0, &s), ERR_INVALID);
    const char *m = err_msg();
    assert_non_null(strstr(m, "3 source(s) in strict snapshot mode"));
    assert_non_null(strstr(m, PLAIN ": "));
    assert_non_null(strstr(m, "no ZFS dataset or btrfs pool"));
    assert_non_null(strstr(m, DIRDOM ": the helper's policy places it in a dir domain"));
    assert_non_null(strstr(m, D "/zfs/data/missing: cannot be resolved"));
    assert_null(strstr(m, DS "/app:"));
    err_clear();
    assert_null(s.p);
    drained();
}

/* Warn sources are read live, and said: the warning and the result name
 * each with its reason. */
static void test_warn_reads_live_and_says_so(void **state)
{
    (void)state;
    const char *paths[] = { DS "/app", PLAIN };
    int modes[] = { SRC_SNAP_STRICT, SRC_SNAP_WARN };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, 2, 0, &s), OK);
    assert_int_equal(s.n_snapshot, 1);
    assert_int_equal(s.n_live, 1);
    assert_null(s.read_paths[1]);
    char w[2048];
    src_snap_describe_live(&s, paths, w, sizeof(w));
    assert_non_null(strstr(w, "1 source(s) in snapshot mode were read live"));
    assert_non_null(strstr(w, PLAIN ": "));
    char *js = src_snap_result_json(&s, paths);
    assert_non_null(strstr(js, "\"live_sources\":[{\"path\":\"" PLAIN "\",\"reason\":\""));
    free(js);
    src_snap_end(&s);
    drained();

    /* Every warn source live: no capture is left to the end. */
    const char *one[] = { PLAIN };
    int wm[] = { SRC_SNAP_WARN };
    assert_int_equal(src_snap_begin(one, wm, 1, 0, &s), OK);
    assert_null(s.capture);
    assert_null(s.read_paths);
    src_snap_end(&s);
    drained();
}

/* A mount below a source shows empty in its snapshot: with the source
 * crossing mounts it is not read from the snapshot; with one_filesystem,
 * which skips the mount live as well, it is. */
static void test_mount_below(void **state)
{
    (void)state;
    const char *paths[] = { DS };
    int modes[] = { SRC_SNAP_WARN };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, 1, 0, &s), OK);
    assert_int_equal(s.n_live, 1);
    assert_non_null(strstr(s.p[0].reason, "lie below it (" DS "/other, " DS "/domains/vm1, " DS
                                          "/domains/vm2, ...)"));
    assert_non_null(strstr(s.p[0].reason, "5 mount(s)"));
    src_snap_end(&s);
    drained();

    /* Strict: the refusal names them and says what to do. */
    const char *dom[] = { DS "/domains" };
    int strict[] = { SRC_SNAP_STRICT };
    assert_int_equal(src_snap_begin(dom, strict, 1, 0, &s), ERR_INVALID);
    const char *m = err_msg();
    assert_non_null(strstr(m, DS "/domains: 4 mount(s) or btrfs subvolume(s) lie below it (" DS "/domains/vm1, "
                              DS "/domains/vm2, " DS "/domains/vm3, ...)"));
    assert_non_null(strstr(m, "Add each as a source of its own in snapshot mode"));
    assert_non_null(strstr(m, "warn mode to read it live"));
    err_clear();
    drained();
    assert_int_equal(src_snap_begin(paths, modes, 1, 1, &s), OK);
    assert_int_equal(s.n_snapshot, 1);
    assert_string_equal(s.p[0].kind, "zfs");
    src_snap_end(&s);
    drained();
}

/* The helper away: nothing is snapshotted, and the reason says so; a
 * strict source fails rather than being read live. */
static void test_helper_away(void **state)
{
    (void)state;
    helper_server_stop(srv);
    srv = NULL;
    const char *paths[] = { DS "/app" };
    int strict[] = { SRC_SNAP_STRICT }, warn[] = { SRC_SNAP_WARN };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, strict, 1, 0, &s), ERR_INVALID);
    assert_non_null(strstr(err_msg(), DS "/app: the helper took no snapshot"));
    err_clear();
    assert_int_equal(src_snap_begin(paths, warn, 1, 0, &s), OK);
    assert_int_equal(s.n_live, 1);
    assert_non_null(strstr(s.p[0].reason, "the helper took no snapshot"));
    src_snap_end(&s);
}

/* More than capture.begin_paths takes in one request: the rest are not
 * snapshotted, and say why. */
static void test_too_many(void **state)
{
    (void)state;
    enum { N = SRC_SNAP_PATHS_MAX + 1 };
    char buf[N][64];
    const char *paths[N];
    int modes[N];
    for (int i = 0; i < N; i++) {
        snprintf(buf[i], sizeof(buf[i]), "/nonexistent-cbk/%d", i);
        paths[i] = buf[i];
        modes[i] = SRC_SNAP_WARN;
    }
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, N, 0, &s), OK);
    assert_int_equal(s.n_live, N);
    assert_non_null(strstr(s.p[N - 1].reason, "more than 64 sources in snapshot mode"));
    src_snap_end(&s);
    drained();
}

/* The lease runs out during the read: the helper destroys the snapshot,
 * and the end finds the capture gone and says so. */
static void test_lease_lost(void **state)
{
    (void)state;
    const char *paths[] = { DS "/app" };
    int modes[] = { SRC_SNAP_STRICT };
    src_snap_t s;
    assert_int_equal(src_snap_begin(paths, modes, 1, 0, &s), OK);
    assert_true(helper_server_tick(srv, ((int64_t)time(NULL) + 7 * 86400) * 1000) >= 1);
    assert_string_equal(slurp(FSTATE "/snaps"), "");
    src_snap_release(&s);
    assert_true(s.lease_lost);
    char w[2048];
    src_snap_describe_live(&s, paths, w, sizeof(w));
    assert_non_null(strstr(w, "capture_max_s lease ran out"));
    char *js = src_snap_result_json(&s, paths);
    assert_non_null(strstr(js, "\"source_snapshot_lease_lost\":true"));
    free(js);
    src_snap_end(&s);
    drained();
}

int main(void)
{
    /* The helper refuses daemon_uid 0 and checks the peer: as root (CI)
     * the suite runs as a fixed other uid, as the helper suites do. */
    if (hf_unroot() != 0) { perror("cannot leave root for the helper suites"); return 1; }
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_modes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nothing_opted_in, setup, teardown),
        cmocka_unit_test_setup_teardown(test_strict_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_strict_failure_names_each, setup, teardown),
        cmocka_unit_test_setup_teardown(test_warn_reads_live_and_says_so, setup, teardown),
        cmocka_unit_test_setup_teardown(test_mount_below, setup, teardown),
        cmocka_unit_test_setup_teardown(test_helper_away, setup, teardown),
        cmocka_unit_test_setup_teardown(test_too_many, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lease_lost, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
