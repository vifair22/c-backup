#define _GNU_SOURCE

/*
 * packs/ on a filesystem of its own (B163).
 *
 * Manual section 5.3 allows packs/ to be its own mount, and B156's checks
 * were written for exactly that deployment. But every pack was staged in
 * <repo>/tmp and renamed into packs/, and rename(2) does not cross
 * filesystems: with packs/ mounted, every backup failed with EXDEV
 * ("Invalid cross-device link") at its first pack, and gc's deletion
 * marker and a replica's packs would have done the same. Packs and markers
 * are staged in packs/.staging now. Here packs/ is a tmpfs:
 *
 *   backup     three backups, each pack installed on the mount, every
 *              snapshot restoring exactly, nothing left in either staging
 *              directory
 *   gc         a prune and a gc that rewrites, removes or merges packs on
 *              the mount, and the survivors still restore
 *   replicate  a replica whose packs/ is a mount takes every object, and a
 *              second run finds them all there
 *
 * Mounting needs privilege, so main() enters a user and mount namespace of
 * its own first (unshare -rm, without the binary). Where that is refused -
 * a container without user namespaces, a hardened kernel - every test
 * skips rather than passing having proved nothing.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/mount.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "gc.h"
#include "pack.h"
#include "pack_presence.h"
#include "prune.h"
#include "replicate.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_packs_mount"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_REPL TEST_DIR "/replica"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_OUT  TEST_DIR "/out"

extern const db_migration_t app_migrations[];

static int            g_ns;      /* 1 when mounting is possible */
static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void sh(const char *fmt, const char *arg)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), fmt, arg, arg);
    int rc = system(cmd); (void)rc;
}

static int write_str(const char *path, const char *s)
{
    int fd = open(path, O_WRONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    ssize_t n = write(fd, s, strlen(s));
    close(fd);
    return n == (ssize_t)strlen(s) ? 0 : -1;
}

/* Root in a user namespace of our own, with a private mount namespace. */
static int enter_namespace(void)
{
    uid_t uid = getuid();
    gid_t gid = getgid();
    if (unshare(CLONE_NEWUSER | CLONE_NEWNS) != 0) return -1;
    char map[64];
    (void)write_str("/proc/self/setgroups", "deny");
    snprintf(map, sizeof(map), "0 %u 1", (unsigned)uid);
    if (write_str("/proc/self/uid_map", map) != 0) return -1;
    snprintf(map, sizeof(map), "0 %u 1", (unsigned)gid);
    if (write_str("/proc/self/gid_map", map) != 0) return -1;
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) return -1;
    return 0;
}

static int mount_packs(const char *repo)
{
    char p[512];
    snprintf(p, sizeof(p), "%s/packs", repo);
    return mount("tmpfs", p, "tmpfs", MS_NOSUID | MS_NODEV, "size=256m,mode=0755");
}

static void unmount_packs(const char *repo)
{
    char p[512];
    snprintf(p, sizeof(p), "%s/packs", repo);
    (void)umount2(p, MNT_DETACH);
}

static int other_device(const char *repo)
{
    char p[512];
    struct stat a, b;
    snprintf(p, sizeof(p), "%s/packs", repo);
    return stat(repo, &a) == 0 && stat(p, &b) == 0 && a.st_dev != b.st_dev;
}

static void write_file(const char *name, uint32_t seed, size_t n)
{
    char p[512];
    snprintf(p, sizeof(p), TEST_SRC "/%s", name);
    unsigned char *b = malloc(n);
    assert_non_null(b);
    uint32_t x = seed | 1u;
    for (size_t i = 0; i < n; i++) { x ^= x << 13; x ^= x >> 17; x ^= x << 5; b[i] = (unsigned char)x; }
    FILE *f = fopen(p, "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(b, 1, n, f), n);
    fclose(f);
    free(b);
}

static long long scalar(const char *sql)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    long long v = -1;
    const char *p[] = { repo, NULL };
    if (db_execute(db, sql, strchr(sql, '?') ? p : NULL, &r) == 0 && r && r->nrows == 1)
        v = atoll(r->rows[0][0]);
    db_result_free(r);
    return v;
}

/* Regular files under dir whose name starts with prefix; -1 when dir is
 * missing. */
static int count_prefixed(const char *dir, const char *prefix)
{
    DIR *d = opendir(dir);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL)
        if (strncmp(e->d_name, prefix, strlen(prefix)) == 0) n++;
    closedir(d);
    return n;
}

/* Nothing staged is left behind in either staging directory. */
static void assert_no_staging(const char *repo)
{
    char p[512];
    snprintf(p, sizeof(p), "%s/" STORE_PACK_STAGING, repo);
    int n = count_prefixed(p, "pack-dat.");
    assert_true(n <= 0);
    n = count_prefixed(p, "deleting.");
    assert_true(n <= 0);
    snprintf(p, sizeof(p), "%s/tmp", repo);
    assert_int_equal(count_prefixed(p, "pack-dat."), 0);
}

static int setup(void **state)
{
    (void)state;
    if (!g_ns) return 0;
    sh("rm -rf '%s'", TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0 || mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "t", &repo_id) != OK) return -1;
    if (mount_packs(TEST_REPO) != 0) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (!g_ns) return 0;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    unmount_packs(TEST_REPL);
    unmount_packs(TEST_REPO);
    sh("rm -rf '%s'", TEST_DIR);
    return 0;
}

static int64_t backup_ok(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    status_t st = backup_run(store, db, repo_id, &o, &br);
    int64_t id = br.snapshot_id;
    backup_result_free(&br);
    if (st != OK) fail_msg("backup: %s", err_msg());
    return id;
}

/* The snapshot restored, compared with what the source held when it was
 * taken: kept in want/, a copy per snapshot. */
static void keep_source(int64_t snap)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s/want%lld' && cp -a '%s' '%s/want%lld'",
             TEST_DIR, (long long)snap, TEST_SRC, TEST_DIR, (long long)snap);
    assert_int_equal(system(cmd), 0);
}

static void assert_restores(int64_t snap)
{
    sh("rm -rf '%s'; mkdir '%s'", TEST_OUT);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    if (restore_run(store, db, repo_id, snap, TEST_OUT, &ro, &rr) != OK)
        fail_msg("snapshot %lld does not restore: %s", (long long)snap, err_msg());
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s/want%lld' '%s%s' >/dev/null",
             TEST_DIR, (long long)snap, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
}

static void three_backups(int64_t ids[3])
{
    write_file("a", 1, 400000);
    ids[0] = backup_ok();
    keep_source(ids[0]);
    write_file("b", 2, 500000);
    ids[1] = backup_ok();
    keep_source(ids[1]);
    assert_int_equal(unlink(TEST_SRC "/a"), 0);
    write_file("c", 3, 600000);
    ids[2] = backup_ok();
    keep_source(ids[2]);
}

static void test_backup_and_restore(void **state)
{
    (void)state;
    if (!g_ns) skip();
    assert_true(other_device(TEST_REPO));

    int64_t ids[3];
    three_backups(ids);
    assert_true(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?") >= 3);

    /* Every pack the index names is a file on the mount. */
    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db, "SELECT id FROM packs WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    for (int i = 0; r && i < r->nrows; i++) {
        char p[512];
        struct stat sb, root;
        assert_int_equal(pack_dat_path_resolve(p, sizeof(p), TEST_REPO,
                                               (uint32_t)strtoul(r->rows[i][0], NULL, 10)), 0);
        assert_int_equal(stat(p, &sb), 0);
        assert_int_equal(stat(TEST_REPO, &root), 0);
        assert_true(sb.st_dev != root.st_dev);
    }
    db_result_free(r);
    assert_no_staging(TEST_REPO);

    for (int i = 0; i < 3; i++) assert_restores(ids[i]);

    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_missing, 0);
}

static void test_gc_on_the_mount(void **state)
{
    (void)state;
    if (!g_ns) skip();
    int64_t ids[3];
    three_backups(ids);
    assert_int_equal(prune_snapshot(store, db, repo_id, ids[0]), OK);

    /* A staging file a killed writer left on the mount goes too. */
    char p[512];
    snprintf(p, sizeof(p), TEST_REPO "/" STORE_PACK_STAGING "/pack-dat.Kill01");
    FILE *f = fopen(p, "wb");
    assert_non_null(f);
    fputs("half a pack", f);
    fclose(f);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    if (gc_run(store, db, repo_id, 0, NULL, &g) != OK) fail_msg("gc: %s", err_msg());
    assert_true(g.packs_rewritten + g.packs_deleted + g.packs_coalesced > 0);
    assert_true(g.tmp_files_removed >= 1);
    assert_no_staging(TEST_REPO);
    assert_restores(ids[1]);
    assert_restores(ids[2]);

    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_missing, 0);
}

static void test_replicate_into_a_mounted_packs(void **state)
{
    (void)state;
    if (!g_ns) skip();
    int64_t ids[3];
    three_backups(ids);
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);

    /* The replica's layout first, then its packs/ on a mount of its own:
     * the replica starts empty there and takes every object again. */
    replica_sink_t sink;
    replicate_result_t rr;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &m, &sink), OK);
    sink.v->close(sink.ctx);
    assert_int_equal(mount_packs(TEST_REPL), 0);
    assert_true(other_device(TEST_REPL));

    assert_int_equal(replica_sink_local_open(TEST_REPL, &m, &sink), OK);
    memset(&rr, 0, sizeof(rr));
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    if (st != OK) fail_msg("replicate: %s", err_msg());
    assert_true(rr.objects_sent > 0);
    assert_no_staging(TEST_REPL);

    /* Everything it was sent is installed on the mount and found there. */
    assert_int_equal(replica_sink_local_open(TEST_REPL, &m, &sink), OK);
    memset(&rr, 0, sizeof(rr));
    st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    if (st != OK) fail_msg("replicate again: %s", err_msg());
    assert_int_equal(rr.objects_sent, 0);
}

int main(void)
{
    g_ns = enter_namespace() == 0;
    if (!g_ns)
        fprintf(stderr, "test_packs_mount: no user namespace (%s); every test skips\n",
                strerror(errno));
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_backup_and_restore, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_on_the_mount, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicate_into_a_mounted_packs, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
