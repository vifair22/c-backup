#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

/*
 * A pack file the index still names, gone (B150).
 *
 * The UAT crash matrix left one: pack 0 missing, its 297 rows surviving,
 * and the next backup deduplicating against them, storing nothing,
 * reporting success, and leaving a snapshot that would not restore. Here:
 *
 *   restored    a missing pack's objects marked absent, verify reporting
 *               exactly those missing, the next backup storing them again,
 *               every snapshot restorable and verify clean, an alert raised
 *   unmounted   packs/ gone or unreadable: nothing checked, nothing
 *               changed, the backup refused as unavailable
 *   transient   a pack that cannot be stat'ed for another reason: its rows
 *               stay
 *   copy        an objects row naming the missing pack while its locating
 *               row is in another: it keeps resolving
 *   brake       more missing than the brake allows: nothing marked, the
 *               repository degraded and backups refused, until a repair
 *               accepts the loss; the thresholds as set, at their edges,
 *               and a lone missing pack healing while others are present
 *   B156        pack numbers never reused: the highest pack dropped, a
 *               backup, the original back - no collision, indexed again,
 *               everything restorable; every pack missing (one of one,
 *               the UAT's unmounted packs/) unavailable and never healed;
 *               packs/ replaced under the index, and a real move
 *               accepted by the repair; an empty root; a copy
 *               that fails its proof losing to one that passes; the
 *               pack-ids mark after a lost index and a lost file
 *   B164/B165   two roots swapped: each unavailable naming the other's
 *               uuid, no repair overriding it, nothing recorded or
 *               written; the all-missing repair refused under another
 *               repository's marker
 *   FLAKE-28    the repair route queues a job: submitted while another
 *               job holds the repository, it waits and completes after
 *               it, where it used to answer 503
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "object.h"
#include "pack.h"
#include "pack_ids.h"
#include "index_reconcile.h"
#include "pack_presence.h"
#include "prune.h"
#include "reindex.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "job_manager.h"
#include "routes.h"
#include "server.h"
#include "test_tmp.h"

#include <pthread.h>
#include <stdatomic.h>

#define TEST_DIR  TEST_TMP "/c_backup_test_pack_presence"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_OUT  TEST_DIR "/out"

extern const db_migration_t app_migrations[];

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

static int setup(void **state)
{
    (void)state;
    sh("rm -rf '%s'", TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0 || mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "t", &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    pack_presence_set_brake(5, 10);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    pack_presence_set_brake(5, 10);
    sh("chmod -R u+rwx '%s' 2>/dev/null; rm -rf '%s'", TEST_DIR);
    return 0;
}

static status_t backup(int64_t *out_id)
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
    if (out_id) *out_id = br.snapshot_id;
    backup_result_free(&br);
    return st;
}

static int64_t backup_ok(void)
{
    int64_t id = 0;
    if (backup(&id) != OK) fail_msg("backup: %s", err_msg());
    return id;
}

/* The pack holding name's content, by the index. */
static uint32_t pack_of(const char *name)
{
    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT po.pack_id FROM nodes n JOIN pack_objects po"
             " ON po.repo_id = n.repo_id AND po.hash = n.content_hash"
             " WHERE n.repo_id = ? AND n.name = '%s' LIMIT 1", name);
    long long v = scalar(sql);
    assert_true(v >= 0);
    return (uint32_t)v;
}

static void pack_path(uint32_t num, char *out, size_t n)
{
    assert_int_equal(pack_dat_path_resolve(out, n, TEST_REPO, num), 0);
}

static void remove_pack(uint32_t num)
{
    char p[512];
    pack_path(num, p, sizeof(p));
    assert_int_equal(unlink(p), 0);
    pack_cache_invalidate(store);
}

/* Each source file, as the snapshot restored it, against the source. */
static void assert_restores(int64_t snap)
{
    sh("rm -rf '%s'; mkdir '%s'", TEST_OUT);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    if (restore_run(store, db, repo_id, snap, TEST_OUT, &ro, &rr) != OK)
        fail_msg("snapshot %lld does not restore: %s", (long long)snap, err_msg());
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null", TEST_SRC, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
}

static verify_result_t verify(void)
{
    verify_result_t v;
    memset(&v, 0, sizeof(v));
    (void)verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &v);
    err_clear();
    return v;
}

/* Three backups, a pack each. */
static void three_packs(int64_t ids[3])
{
    write_file("a", 1, 40000);
    ids[0] = backup_ok();
    write_file("b", 2, 50000);
    ids[1] = backup_ok();
    write_file("c", 3, 60000);
    ids[2] = backup_ok();
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 3);
}

static void after_the_heal(int64_t s4, int64_t old, int ancestor_touched);

static void test_missing_pack_is_stored_again(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);   /* the automatic path, below the brake */
    int64_t ids[3];
    three_packs(ids);
    /* The newest backup's pack: c and the third snapshot's trees. The
     * source has not changed since, as after the UAT's cut, so the next
     * backup can make every one of them again. */
    uint32_t p = pack_of("c");
    long long held = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ? AND pack_id = "
                            "(SELECT po.pack_id FROM nodes n JOIN pack_objects po ON po.repo_id ="
                            " n.repo_id AND po.hash = n.content_hash WHERE n.name = 'c' LIMIT 1)");
    assert_true(held > 0);
    remove_pack(p);

    uint64_t alerts = pack_presence_alerts();
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_false(pp.braked);
    assert_int_equal(pp.packs_missing, 1);
    assert_int_equal(pp.packs_dropped, 1);
    assert_int_equal(pp.objects_absent, (uint64_t)held);
    assert_int_equal(pack_presence_alerts(), alerts + 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"),
                     held);
    assert_int_equal(scalar("SELECT COUNT(*) FROM journal WHERE repo_id = ? AND operation = 'missing-pack'"), 1);

    /* verify names exactly those, and what they cost. */
    verify_result_t v = verify();
    assert_int_equal(v.missing, (uint64_t)held);
    assert_true(v.affected_files >= 1);
    assert_true(v.affected_snapshots >= 1);

    /* The next backup reads c again and stores it, and the trees. */
    int64_t s4 = backup_ok();
    after_the_heal(s4, ids[2], 0);
}

/*
 * What the backup after a heal can and cannot bring back (FLAKE-24).
 *
 * Every object the new snapshot names has to be present: the backup made
 * each one again. Objects only the old snapshot named stay absent unless the
 * new one happens to make the same bytes, and the trees of the source's
 * ancestors - scan_tree records every directory up to / - are only the same
 * bytes when those directories are unchanged. The scratch root, ~/.cache and
 * $HOME change whenever anything else on the machine touches them, so the
 * old snapshot's trees for them were sometimes not made again: three of them
 * when ~/.cache moved, which is FLAKE-24, and deterministically when the
 * test touches an ancestor itself. Those stay absent, verify names exactly
 * them, and the old snapshot is restorable only when none is left.
 */
static void after_the_heal(int64_t s4, int64_t old, int ancestor_touched)
{
    char sql[1024];
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM objects o WHERE o.repo_id = ? AND o.location = 'absent'"
             " AND (o.hash IN (SELECT content_hash FROM nodes WHERE repo_id = o.repo_id"
             "                 AND snap_id = %lld)"
             "   OR o.hash IN (SELECT subtree_hash FROM nodes WHERE repo_id = o.repo_id"
             "                 AND snap_id = %lld)"
             "   OR o.hash IN (SELECT root_tree_hash FROM snapshots WHERE repo_id = o.repo_id"
             "                 AND id = %lld))", (long long)s4, (long long)s4, (long long)s4);
    assert_int_equal(scalar(sql), 0);
    assert_restores(s4);

    long long left = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'");
    if (ancestor_touched) assert_true(left > 0);
    verify_result_t v = verify();
    assert_int_equal(v.missing, (uint64_t)left);
    if (left == 0) {
        assert_restores(old);    /* the old snapshot names the same objects */
        assert_int_equal(v.ok, v.checked);
    }
}

static void test_missing_pack_ancestor_changed(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    remove_pack(pack_of("c"));
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_dropped, 1);
    /* An ancestor of the source changes, as ~/.cache does whenever anything
     * on the machine creates or removes an entry in it. */
    struct timespec ts[2] = { { 0, UTIME_OMIT }, { 1000000000, 0 } };
    assert_int_equal(utimensat(AT_FDCWD, TEST_DIR, ts, 0), 0);
    int64_t s4 = backup_ok();
    after_the_heal(s4, ids[2], 1);
}

/*
 * An older snapshot's own trees, in a missing pack, are nothing a later
 * backup of a changed source makes again: that loss stays, and verify
 * names exactly the objects still absent, while the newest snapshot and
 * everything its source still has are whole.
 *
 * And that snapshot still restores, exactly: a restore reads the index's
 * nodes, not the trees (NIT-36). So the loss is trees alone, which no file
 * names, and the run's totals say what its findings say: no file, no
 * snapshot. They used to say "affecting 2 file(s)" over findings that each
 * said 0.
 */
static void test_an_older_snapshots_loss_is_reported(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    remove_pack(pack_of("b"));
    int64_t s4 = backup_ok();
    assert_restores(s4);
    assert_restores(ids[2]);
    {   /* its trees are gone; its files are not */
        sh("rm -rf '%s'; mkdir '%s'", TEST_OUT);
        restore_opts_t ro;
        memset(&ro, 0, sizeof(ro));
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        if (restore_run(store, db, repo_id, ids[1], TEST_OUT, &ro, &rr) != OK)
            fail_msg("snapshot %lld does not restore: %s", (long long)ids[1], err_msg());
    }
    long long gone = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'");
    assert_true(gone > 0);
    verify_result_t v = verify();
    assert_int_equal(v.missing, (uint64_t)gone);
    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT COALESCE(SUM(affected_files), 0) FROM verify_findings WHERE run_id = %lld",
             (long long)v.run_id);
    assert_int_equal(scalar(sql), 0);
    assert_int_equal(v.affected_files, 0);
    assert_int_equal(v.affected_snapshots, 0);
}

/* The backup does the check itself, before its dedup. */
static void test_backup_checks_before_dedup(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    remove_pack(pack_of("c"));
    uint64_t alerts = pack_presence_alerts();
    int64_t s4 = backup_ok();
    assert_int_equal(pack_presence_alerts(), alerts + 1);
    assert_restores(s4);
    verify_result_t v = verify();
    assert_int_equal(v.missing, 0);
}

static void test_unmounted_packs_dir_changes_nothing(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    long long rows = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    assert_int_equal(rename(TEST_REPO "/packs", TEST_DIR "/packs-away"), 0);
    pack_cache_invalidate(store);

    uint64_t alerts = pack_presence_alerts();
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);   /* even accepting */
    assert_true(pp.unavailable);
    assert_int_equal(pp.packs_missing, 0);
    assert_int_equal(backup(NULL), ERR_UNAVAILABLE);
    err_clear();
    assert_int_equal(pack_presence_check_all(db), OK);

    /* Unreadable is the same as gone. */
    assert_int_equal(rename(TEST_DIR "/packs-away", TEST_REPO "/packs"), 0);
    if (geteuid() != 0) {
        assert_int_equal(chmod(TEST_REPO "/packs", 0), 0);
        assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
        assert_true(pp.unavailable);
        assert_int_equal(chmod(TEST_REPO "/packs", 0755), 0);
    }

    assert_int_equal(pack_presence_alerts(), alerts);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), rows);
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 3);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    assert_false(pack_presence_degraded(db, repo_id, NULL, 0));
    assert_restores(ids[2]);
}

/* A shard that cannot be searched answers EACCES, not ENOENT: not a loss. */
static void test_transient_error_leaves_rows(void **state)
{
    (void)state;
    if (geteuid() == 0) skip();
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    char p[512];
    pack_path(pack_of("a"), p, sizeof(p));
    char *s = strrchr(p, '/');
    *s = '\0';
    assert_int_equal(chmod(p, 0), 0);
    pack_presence_t pp;
    status_t st = pack_presence_check(db, repo_id, 0, &pp);
    assert_int_equal(chmod(p, 0755), 0);
    assert_int_equal(st, OK);
    assert_int_equal(pp.packs_missing, 0);
    assert_true(pp.packs_unknown >= 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 3);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
}

/* An objects row naming the missing pack, located in another: it keeps its
 * copy rather than becoming absent. */
static void test_another_copy_still_resolves(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    uint32_t pa = pack_of("a"), pc = pack_of("c");
    char pas[32];
    snprintf(pas, sizeof(pas), "%u", pa);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE objects SET location = ? WHERE repo_id = ? AND hash ="
        " (SELECT content_hash FROM nodes WHERE name = 'c' LIMIT 1)",
        (const char *[]){ pas, repo, NULL }, NULL), 0);
    remove_pack(pa);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_dropped, 1);
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM objects o JOIN nodes n ON n.content_hash = o.hash"
             " WHERE o.repo_id = ? AND n.name = 'c' AND o.location = '%u'", pc);
    assert_true(scalar(sql) >= 1);
    backup_ok();
    assert_restores(ids[2]);
}

static void test_brake_degrades_until_repaired(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    remove_pack(pack_of("c"));
    remove_pack(pack_of("b"));       /* 2 of 3: two, and over 5% */
    uint64_t alerts = pack_presence_alerts();
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.braked);
    assert_true(pp.degraded);
    assert_int_equal(pp.packs_dropped, 0);
    assert_int_equal(pack_presence_alerts(), alerts + 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    char why[256] = "";
    assert_true(pack_presence_degraded(db, repo_id, why, sizeof(why)));
    assert_non_null(strstr(why, "2 of 3 packs missing"));

    /* Backups are refused, and a second check does not alert again. */
    assert_int_equal(backup(NULL), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "--accept-missing"));
    err_clear();
    assert_int_equal(pack_presence_alerts(), alerts + 1);

    /* The repair accepts the loss; the next backup stores b again. */
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);
    assert_int_equal(pp.packs_dropped, 2);
    assert_false(pp.degraded);
    assert_false(pack_presence_degraded(db, repo_id, NULL, 0));
    int64_t s4 = backup_ok();
    assert_restores(s4);
    assert_restores(ids[2]);
    /* The second snapshot's own trees went with its pack; verify names
     * exactly what stayed lost. */
    long long gone = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'");
    verify_result_t v = verify();
    assert_int_equal(v.missing, (uint64_t)gone);
}

/* The floor: a lone missing pack heals on its own, whatever share of the
 * repository it is, while another pack is present: one of two. */
static void test_a_lone_missing_pack_heals(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    int64_t s1 = backup_ok();
    write_file("b", 2, 40000);
    int64_t s2 = backup_ok();
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 2);
    remove_pack(pack_of("b"));        /* 1 of 2: 50% */
    uint64_t alerts = pack_presence_alerts();
    int64_t s3 = backup_ok();
    assert_int_equal(pack_presence_alerts(), alerts + 1);
    assert_false(pack_presence_degraded(db, repo_id, NULL, 0));
    assert_restores(s3);
    assert_restores(s2);
    (void)s1;
    verify_result_t v = verify();
    assert_int_equal(v.missing, 0);
    assert_int_equal(v.ok, v.checked);
}

/*
 * One pack of one, missing, is every pack: unavailable, never healed
 * (B156). It used to heal - the floor above - and an unmounted packs/ on a
 * one-pack repository is exactly that. Only an operator's accept_missing
 * takes the loss.
 */
static void test_every_pack_missing_is_unavailable(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    int64_t s1 = backup_ok();
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 1);
    remove_pack(pack_of("a"));        /* 1 of 1 */
    uint64_t alerts = pack_presence_alerts();
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.unavailable);
    assert_non_null(strstr(pp.why, "none of the 1 packs"));
    assert_int_equal(pp.packs_dropped, 0);
    assert_int_equal(backup(NULL), ERR_UNAVAILABLE);
    err_clear();
    assert_int_equal(pack_presence_alerts(), alerts);
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    assert_int_equal(pack_presence_available(db, repo_id, NULL, 0), 0);
    assert_int_equal(pack_presence_require(db, repo_id), ERR_UNAVAILABLE);
    err_clear();

    /* The operator's repair accepts it, and backups resume. */
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_dropped, 1);
    int64_t s2 = backup_ok();
    assert_restores(s2);
    assert_restores(s1);
}

/* A remount: every pack back, and the degraded state goes on its own. */
static void test_degraded_clears_when_the_packs_return(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    char p[512], q[512], away[600], away2[600];
    pack_path(pack_of("b"), p, sizeof(p));
    pack_path(pack_of("c"), q, sizeof(q));
    snprintf(away, sizeof(away), TEST_DIR "/away.dat");
    snprintf(away2, sizeof(away2), TEST_DIR "/away2.dat");
    assert_int_equal(rename(p, away), 0);
    assert_int_equal(rename(q, away2), 0);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.degraded);
    assert_int_equal(rename(away, p), 0);
    assert_int_equal(rename(away2, q), 0);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.degraded);
    backup_ok();
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
}

/* The thresholds as set, at their edges: more than pct percent, or more
 * than packs, trips it; exactly at either does not. */
static void test_brake_thresholds(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    write_file("d", 4, 30000);
    backup_ok();                      /* four packs */
    remove_pack(pack_of("a"));
    remove_pack(pack_of("b"));        /* 2 of 4 = 50% */
    pack_presence_t pp;

    pack_presence_set_brake(50, 1000);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.braked);          /* not more than 50% */
    assert_int_equal(pp.packs_dropped, 2);
}

static void test_brake_count_threshold(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    remove_pack(pack_of("a"));
    remove_pack(pack_of("b"));        /* two missing */
    pack_presence_t pp;
    pack_presence_set_brake(100, 1);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.braked);           /* more than one */
    pack_presence_set_brake(66, 2);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.braked);           /* 2 of 3 is more than 66% */
    pack_presence_set_brake(67, 2);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.braked);          /* neither: marked, and degraded cleared */
    assert_int_equal(pp.packs_dropped, 2);
    assert_false(pp.degraded);
}

/* ---- B156 ---------------------------------------------------------- */

/* Where a pack waits while it is "missing": inside the repository, not in
 * TEST_DIR, whose mtime is part of every snapshot's trees (the source's
 * ancestors are recorded), so a file appearing there would change them. */
#define AWAY TEST_REPO "/away.dat"

/*
 * A reindex here may fail one snapshot: the dropped pack held its trees,
 * and an unchanged source does not always remake them, because the
 * source's ancestors (TEST_DIR, whose mtime the database's own files move)
 * are recorded in them. That is the reindex reporting a real loss; the
 * mark is still restored and reported.
 */
static void reindex_lenient(reindex_result_t *rx)
{
    status_t st = reindex_repo(store, db, repo_id, NULL, rx);
    if (st != OK && st != ERR_CORRUPT) fail_msg("reindex: %s", err_msg());
    err_clear();
}

static int64_t mark_now(void)
{
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    int64_t next = -1;
    if (pack_ids_read(TEST_REPO, m.uuid, &next) != OK) { err_clear(); return -1; }
    return next;
}

static long long max_pack(void)
{
    return scalar("SELECT MAX(id) FROM packs WHERE repo_id = ?");
}

static void move_pack(uint32_t num, const char *to)
{
    char p[512];
    pack_path(num, p, sizeof(p));
    assert_int_equal(rename(p, to), 0);
    pack_cache_invalidate(store);
}

static void put_back_pack(uint32_t num, const char *from)
{
    char p[512];
    pack_path(num, p, sizeof(p));
    assert_int_equal(rename(from, p), 0);
    pack_cache_invalidate(store);
}

/*
 * The UAT's M4 sequence, without the mount: the highest pack dropped as
 * missing, a backup, then the original back. The backup's pack takes a new
 * number, so nothing collides; the returning pack is indexed again; every
 * snapshot restores and verify is clean.
 */
static void test_a_dropped_pack_number_is_never_reused(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    uint32_t top = (uint32_t)max_pack();
    assert_int_equal(pack_of("c"), top);
    assert_true(mark_now() > (int64_t)top);
    move_pack(top, AWAY);

    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_dropped, 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_tombstones WHERE repo_id = ?"), 1);

    int64_t s4 = backup_ok();                    /* stores c again */
    long long fresh = max_pack();
    assert_true(fresh > (long long)top);         /* not the dropped number */
    assert_true(mark_now() > fresh);
    char p[512];
    pack_path(top, p, sizeof(p));
    struct stat sb;
    assert_int_equal(stat(p, &sb), -1);          /* nothing written under it */

    put_back_pack(top, AWAY);
    uint64_t restored_before = 0;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_returned, 1);
    assert_int_equal(pp.entries_unproven, 0);
    (void)restored_before;
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_tombstones WHERE repo_id = ?"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM journal WHERE repo_id = ? AND operation = 'pack-returned'"), 1);

    assert_restores(s4);
    assert_restores(ids[2]);
    verify_result_t v = verify();
    assert_int_equal(v.missing, 0);
    assert_int_equal(v.unrecoverable, 0);
    assert_int_equal(v.ok, v.checked);
    /* And a backup after it all stores nothing new for unchanged files. */
    backup_ok();
    v = verify();
    assert_int_equal(v.ok, v.checked);
}

/* The returning pack's objects came back before any backup re-stored them:
 * the older snapshot's lost trees with them. */
static void test_a_returning_pack_brings_its_objects_back(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    uint32_t pb = pack_of("b");
    move_pack(pb, AWAY);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    long long gone = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'");
    assert_true(gone > 0);
    put_back_pack(pb, AWAY);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_returned, 1);
    assert_int_equal(pp.objects_restored, (uint64_t)gone);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    verify_result_t v = verify();
    assert_int_equal(v.ok, v.checked);
}

/* Overwrite len bytes of pack num at off with a pattern parity cannot undo. */
static void ruin_pack(uint32_t num, uint64_t off, size_t len)
{
    char p[512];
    pack_path(num, p, sizeof(p));
    FILE *f = fopen(p, "r+b");
    assert_non_null(f);
    assert_int_equal(fseeko(f, (off_t)off, SEEK_SET), 0);
    for (size_t i = 0; i < len; i++) fputc((int)((i * 131u + 7u) & 0xff), f);
    fclose(f);
    pack_cache_invalidate(store);
}

/*
 * A returning pack whose objects a later backup stored again, where the
 * copy the index has is damaged beyond its parity: the returning copy
 * proves good and the index moves to it. Nothing is trusted by name.
 */
static void test_a_returning_good_copy_beats_a_bad_indexed_one(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    write_file("a", 1, 40000);
    backup_ok();
    write_file("c", 3, 200000);
    int64_t s2 = backup_ok();
    uint32_t pc = pack_of("c");
    move_pack(pc, AWAY);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    int64_t s3 = backup_ok();                   /* stores c again, in a new pack */
    uint32_t pc2 = pack_of("c");
    assert_true(pc2 != pc);

    /* Ruin the new copy of c's content, past what parity can correct. */
    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT po.offset FROM nodes n JOIN pack_objects po ON po.repo_id = n.repo_id"
             " AND po.hash = n.content_hash WHERE n.repo_id = ? AND n.name = 'c' LIMIT 1");
    long long off = scalar(sql);
    assert_true(off >= 0);
    ruin_pack(pc2, (uint64_t)off + 51, 60000);

    put_back_pack(pc, AWAY);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_returned, 1);
    assert_int_equal(pack_of("c"), pc);         /* the index follows the good copy */
    assert_restores(s3);
    assert_restores(s2);
}

/*
 * The other way round: the returning pack is the damaged one. Its bad
 * entry is left out and the good copy a later backup stored stays indexed.
 */
static void test_a_returning_bad_copy_is_left_out(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    write_file("a", 1, 40000);
    backup_ok();
    write_file("c", 3, 200000);
    backup_ok();
    uint32_t pc = pack_of("c");
    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT po.offset FROM nodes n JOIN pack_objects po ON po.repo_id = n.repo_id"
             " AND po.hash = n.content_hash WHERE n.repo_id = ? AND n.name = 'c' LIMIT 1");
    long long off = scalar(sql);
    move_pack(pc, AWAY);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    int64_t s3 = backup_ok();
    uint32_t pc2 = pack_of("c");

    put_back_pack(pc, AWAY);
    ruin_pack(pc, (uint64_t)off + 51, 60000);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(pp.packs_returned, 1);
    assert_true(pp.entries_unproven >= 1);
    assert_int_equal(pack_of("c"), pc2);
    assert_restores(s3);
}

/*
 * A pack file that is not what the index says it is - another pack's bytes
 * under its name, as a reused number left them - is caught by verify by
 * hash, never passed as ok.
 */
static void test_verify_catches_a_pack_that_is_not_what_the_index_says(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    char pa[512], pc[512];
    pack_path(pack_of("a"), pa, sizeof(pa));
    pack_path(pack_of("c"), pc, sizeof(pc));
    char cmd[1200];
    snprintf(cmd, sizeof(cmd), "cp '%s' '%s'", pa, pc);
    assert_int_equal(system(cmd), 0);
    pack_cache_invalidate(store);
    verify_result_t v = verify();
    assert_true(v.unrecoverable + v.missing > 0);
    assert_true(v.ok < v.checked);
}

/*
 * UAT e6: a one-pack repository whose packs/ is a mountpoint. Unmounted,
 * packs/ is an empty directory, and not the one it was: the repository is
 * unavailable, the backup refused, nothing healed or marked. Remounted,
 * everything is as it was.
 */
static void test_uat_e6_unmounted_packs_mountpoint(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    int64_t s1 = backup_ok();
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);   /* records packs/ */
    assert_int_equal(scalar("SELECT COUNT(*) FROM recovery_state WHERE repo_id = ?"
                            " AND key = 'packs-identity'"), 1);
    long long rows = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");

    assert_int_equal(rename(TEST_REPO "/packs", TEST_DIR "/packs-mounted"), 0);
    assert_int_equal(mkdir(TEST_REPO "/packs", 0755), 0);           /* the bare mountpoint */
    pack_cache_invalidate(store);

    uint64_t alerts = pack_presence_alerts();
    assert_int_equal(backup(NULL), ERR_UNAVAILABLE);
    err_clear();
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.unavailable);
    assert_int_equal(pp.packs_dropped, 0);
    assert_int_equal(pack_presence_available(db, repo_id, NULL, 0), 0);
    assert_int_equal(pack_presence_check_all(db), OK);
    assert_int_equal(pack_presence_alerts(), alerts);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_tombstones WHERE repo_id = ?"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 1);
    /* Nothing was written into the mountpoint. */
    DIR *d = opendir(TEST_REPO "/packs");
    assert_non_null(d);
    int entries = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) if (e->d_name[0] != '.') entries++;
    closedir(d);
    assert_int_equal(entries, 0);

    /* Remount. */
    assert_int_equal(rmdir(TEST_REPO "/packs"), 0);
    assert_int_equal(rename(TEST_DIR "/packs-mounted", TEST_REPO "/packs"), 0);
    pack_cache_invalidate(store);
    assert_int_equal(pack_presence_available(db, repo_id, NULL, 0), 1);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_missing, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), rows);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    assert_restores(s1);
    int64_t s2 = backup_ok();
    assert_restores(s2);
    verify_result_t v = verify();
    assert_int_equal(v.ok, v.checked);
}

/* packs/ replaced with one that is missing a pack (the wrong share
 * mounted): unavailable, not one pack lost. The same directory moved with
 * every pack in it is a renumbered device: recorded, nothing else. */
static void test_a_replaced_packs_dir(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    char was[64];
    {
        db_result_t *r = NULL;
        char repo[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
        assert_int_equal(db_execute(db, "SELECT value FROM recovery_state WHERE repo_id = ?"
                                        " AND key = 'packs-identity'",
                                    (const char *[]){ repo, NULL }, &r), 0);
        assert_int_equal(r->nrows, 1);
        snprintf(was, sizeof(was), "%s", r->rows[0][0]);
        db_result_free(r);
    }
    /* A copy of packs/, a new inode, holding every pack: fine. */
    sh("cp -a '%s/packs' '%s/packs-copy'", TEST_REPO);
    assert_int_equal(rename(TEST_REPO "/packs", TEST_DIR "/packs-orig"), 0);
    assert_int_equal(rename(TEST_REPO "/packs-copy", TEST_REPO "/packs"), 0);
    pack_cache_invalidate(store);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_missing, 0);
    char sqlw[256];
    snprintf(sqlw, sizeof(sqlw), "SELECT COUNT(*) FROM recovery_state WHERE repo_id = ?"
             " AND key = 'packs-identity' AND value <> '%s'", was);
    assert_int_equal(scalar(sqlw), 1);            /* the new one, recorded */

    /* Another copy, missing one pack: unavailable, nothing dropped. */
    sh("cp -a '%s/packs' '%s/packs-short'", TEST_REPO);
    char p[512], q[600];
    pack_path(pack_of("a"), p, sizeof(p));
    snprintf(q, sizeof(q), "%s", p);
    char *rel = strstr(q, "/packs/");
    assert_non_null(rel);
    char shortp[700];
    snprintf(shortp, sizeof(shortp), TEST_REPO "/packs-short/%s", rel + strlen("/packs/"));
    assert_int_equal(unlink(shortp), 0);
    sh("rm -rf '%s/packs-old'", TEST_DIR);
    assert_int_equal(rename(TEST_REPO "/packs", TEST_DIR "/packs-old"), 0);
    assert_int_equal(rename(TEST_REPO "/packs-short", TEST_REPO "/packs"), 0);
    pack_cache_invalidate(store);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.unavailable);
    assert_non_null(strstr(pp.why, "not the directory it was"));
    assert_int_equal(pp.packs_dropped, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 3);
    assert_int_equal(backup(NULL), ERR_UNAVAILABLE);
    err_clear();
}

/* The repository root an empty mountpoint: no marker, unavailable, and
 * the refusal touches nothing. */
static void test_an_empty_repository_root(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    backup_ok();
    assert_int_equal(rename(TEST_REPO, TEST_DIR "/repo-mounted"), 0);
    assert_int_equal(mkdir(TEST_REPO, 0755), 0);
    char why[512] = "";
    assert_int_equal(pack_presence_available(db, repo_id, why, sizeof(why)), 0);
    assert_non_null(strstr(why, ".cbackup"));
    assert_int_equal(pack_presence_require(db, repo_id), ERR_UNAVAILABLE);
    assert_non_null(strstr(err_msg(), "nothing was read or written"));
    err_clear();
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);   /* even accepting */
    assert_true(pp.unavailable);
    DIR *d = opendir(TEST_REPO);
    assert_non_null(d);
    int entries = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL)
        if (strcmp(e->d_name, ".") != 0 && strcmp(e->d_name, "..") != 0) entries++;
    closedir(d);
    assert_int_equal(entries, 0);
    assert_int_equal(rmdir(TEST_REPO), 0);
    assert_int_equal(rename(TEST_DIR "/repo-mounted", TEST_REPO), 0);
    assert_int_equal(pack_presence_available(db, repo_id, NULL, 0), 1);
}

/*
 * After a lost index: the dropped pack's tombstone is gone with it, but the
 * repository's mark is not, so the next number is still above it. And with
 * the mark file lost instead, the index's tombstone is the floor.
 */
static void test_the_mark_survives_a_lost_index(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    uint32_t top = (uint32_t)max_pack();
    move_pack(top, AWAY);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);

    /* The index loses everything about packs, as a rebuilt database. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM pack_tombstones WHERE repo_id = ?",
                                          (const char *[]){ repo, NULL }, NULL), 0);
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    reindex_lenient(&rx);
    assert_true(rx.pack_mark > (int64_t)top);
    assert_false(rx.pack_ids_repaired);

    store_index_free(idx);
    store_close(store);
    assert_int_equal(repo_store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
    write_file("d", 4, 30000);
    backup_ok();
    assert_true(max_pack() > (long long)top);
}

static void test_the_tombstone_floors_a_lost_mark(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    uint32_t top = (uint32_t)max_pack();
    move_pack(top, AWAY);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_int_equal(unlink(TEST_REPO "/" PACK_IDS_FILE), 0);
    store_index_free(idx);
    store_close(store);
    assert_int_equal(repo_store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
    backup_ok();            /* stores c and the lost trees again */
    assert_true(max_pack() > (long long)top);
    assert_true(mark_now() > max_pack());         /* rewritten */

    /* A reindex rewrites a mark that is behind. */
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    assert_int_equal(pack_ids_write(TEST_REPO, m.uuid, 1), OK);
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    reindex_lenient(&rx);
    assert_true(rx.pack_ids_repaired);
    assert_true(mark_now() > max_pack());
    assert_true(mark_now() > (int64_t)top);
}

/*
 * packs/ really moved - a rebuilt pool, packs restored to a new disk - and
 * one pack did not come with it. Unavailable, with a reason naming the
 * cause and the repair; the repair refuses while the root's marker is not
 * the registered one, then accepts the new device:inode and the loss; the
 * next check is ordinary and the next backup stores the lost file again.
 */
static void test_a_moved_packs_dir_is_accepted_by_repair(void **state)
{
    (void)state;
    pack_presence_set_brake(100, 1000);
    int64_t ids[3];
    three_packs(ids);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);   /* records packs/ */

    /* The new filesystem: a copy of packs/ (a new inode), without c's pack. */
    sh("cp -a '%s/packs' '%s/packs-new'", TEST_REPO);
    char p[512], moved[700];
    pack_path(pack_of("c"), p, sizeof(p));
    snprintf(moved, sizeof(moved), TEST_REPO "/packs-new/%s", strstr(p, "/packs/") + strlen("/packs/"));
    assert_int_equal(unlink(moved), 0);
    assert_int_equal(rename(TEST_REPO "/packs", TEST_DIR "/packs-old"), 0);
    assert_int_equal(rename(TEST_REPO "/packs-new", TEST_REPO "/packs"), 0);
    pack_cache_invalidate(store);

    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.unavailable);
    assert_non_null(strstr(pp.why, "not the directory it was"));
    assert_non_null(strstr(pp.why, "really moved"));
    char fix[96];
    snprintf(fix, sizeof(fix), "c-backup repo repair %lld --accept-missing", (long long)repo_id);
    assert_non_null(strstr(pp.why, fix));
    assert_int_equal(backup(NULL), ERR_UNAVAILABLE);
    err_clear();

    /* Not this repository's marker: still refused, nothing dropped. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT uuid FROM repos WHERE id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    char uuid[64];
    snprintf(uuid, sizeof(uuid), "%s", r->rows[0][0]);
    db_result_free(r);
    assert_int_equal(db_execute_non_query(db, "UPDATE repos SET uuid = 'someone-else' WHERE id = ?",
                                          (const char *[]){ repo, NULL }, NULL), 0);
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);
    assert_true(pp.unavailable);
    assert_non_null(strstr(pp.why, "not someone-else"));
    assert_non_null(strstr(pp.why, "holds repository"));
    assert_int_equal(pp.packs_dropped, 0);
    assert_int_equal(db_execute_non_query(db, "UPDATE repos SET uuid = ? WHERE id = ?",
                                          (const char *[]){ uuid, repo, NULL }, NULL), 0);

    /* The operator's repair: the loss and the new identity accepted. */
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_dropped, 1);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pack_presence_available(db, repo_id, NULL, 0), 1);

    int64_t s4 = backup_ok();                    /* stores c again */
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects o JOIN nodes n ON n.content_hash = o.hash"
                            " WHERE o.repo_id = ? AND n.name = 'c' AND o.location = 'absent'"), 0);
    assert_restores(s4);
}

/* ---- a path that holds another repository (B164, B165) ----------- */

#define TEST_REPO2 TEST_DIR "/repo2"
#define TEST_SRC2  TEST_DIR "/src2"

static void uuid_of(int64_t id, char *out, size_t n)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT uuid FROM repos WHERE id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(out, n, "%s", r->rows[0][0]);
    db_result_free(r);
}

static void identity_of(int64_t id, char *out, size_t n)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COALESCE(MAX(value), '') FROM recovery_state"
                                    " WHERE repo_id = ? AND key = 'packs-identity'",
                                (const char *[]){ repo, NULL }, &r), 0);
    snprintf(out, n, "%s", r && r->nrows == 1 ? r->rows[0][0] : "");
    db_result_free(r);
}

/* Every file under dir with its size, mtime and inode: what "nothing was
 * written" is checked against. */
static char *listing(const char *dir)
{
    char cmd[600];
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && find . -printf '%%p %%y %%s %%T@ %%i\n' | LC_ALL=C sort", dir);
    FILE *f = popen(cmd, "r");
    assert_non_null(f);
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    assert_non_null(buf);
    size_t n;
    while ((n = fread(buf + len, 1, cap - len - 1, f)) > 0) {
        len += n;
        if (cap - len < 1024) { cap *= 2; buf = realloc(buf, cap); assert_non_null(buf); }
    }
    pclose(f);
    buf[len] = '\0';
    return buf;
}

static status_t backup_into(store_t *st, int64_t id, const char *src)
{
    const char *paths[] = { src };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    status_t s = backup_run(st, db, id, &o, &br);
    backup_result_free(&br);
    return s;
}

static void swap_roots(void)
{
    assert_int_equal(rename(TEST_REPO, TEST_DIR "/swap"), 0);
    assert_int_equal(rename(TEST_REPO2, TEST_REPO), 0);
    assert_int_equal(rename(TEST_DIR "/swap", TEST_REPO2), 0);
}

/*
 * Two repositories of one pack each, their roots swapped on disk: both
 * found a pack-0000000000.dat where the index expected one and stayed ok,
 * and each accepted the other's packs/ as its own moved one. Verify then
 * called most of each unrecoverable and restores failed their hashes.
 * Now each is unavailable, saying whose repository its path holds; the
 * repair cannot override that; nothing is recorded and neither repository
 * is written to; swapped back, both are as they were.
 */
static void test_swapped_roots_are_unavailable(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    int64_t s1 = backup_ok();

    int64_t id2 = 0;
    store_t *st2 = NULL;
    store_index_t *idx2 = NULL;
    assert_int_equal(repo_register(db, TEST_REPO2, "t2", &id2), OK);
    assert_int_equal(repo_store_open(TEST_REPO2, NULL, &st2), OK);
    assert_int_equal(store_index_bind(st2, db, id2, &idx2), OK);
    assert_int_equal(mkdir(TEST_SRC2, 0700), 0);
    sh("head -c 50000 /dev/urandom > '%s/z' #%s", TEST_SRC2);
    assert_int_equal(backup_into(st2, id2, TEST_SRC2), OK);
    sh("head -c 100 /dev/urandom > '%s/y' #%s", TEST_SRC2);
    assert_int_equal(backup_into(st2, id2, TEST_SRC2), OK);   /* its snapshot 2 */

    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_checked, 1);
    assert_int_equal(pack_presence_check(db, id2, 0, &pp), OK);
    assert_false(pp.unavailable);

    char u1[64], u2[64], i1[64], i2[64], j1[64], j2[64];
    uuid_of(repo_id, u1, sizeof(u1));
    uuid_of(id2, u2, sizeof(u2));
    identity_of(repo_id, i1, sizeof(i1));
    identity_of(id2, i2, sizeof(i2));
    assert_true(i1[0] && i2[0]);

    swap_roots();
    pack_cache_invalidate(store);
    pack_cache_invalidate(st2);
    char *l1 = listing(TEST_REPO), *l2 = listing(TEST_REPO2);

    const struct { int64_t id; const char *mine, *theirs; } cases[2] = {
        { repo_id, u1, u2 }, { id2, u2, u1 },
    };
    for (int k = 0; k < 2; k++) {
        char want[200];
        snprintf(want, sizeof(want), "holds repository %s, not %s", cases[k].theirs, cases[k].mine);
        assert_int_equal(pack_presence_check(db, cases[k].id, 0, &pp), OK);
        assert_true(pp.unavailable);
        assert_non_null(strstr(pp.why, want));
        /* The operator's repair does not get to accept another repository. */
        assert_int_equal(pack_presence_check(db, cases[k].id, 1, &pp), OK);
        assert_true(pp.unavailable);
        assert_int_equal(pp.packs_dropped, 0);
        char why[PACK_PRESENCE_WHY_MAX];
        assert_int_equal(pack_presence_available(db, cases[k].id, why, sizeof(why)), 0);
        assert_non_null(strstr(why, want));
        assert_int_equal(pack_presence_require(db, cases[k].id), ERR_UNAVAILABLE);
        assert_non_null(strstr(err_msg(), want));
        err_clear();
    }
    assert_int_equal(backup(NULL), ERR_UNAVAILABLE);
    err_clear();
    assert_int_equal(backup_into(st2, id2, TEST_SRC2), ERR_UNAVAILABLE);
    err_clear();

    /*
     * The start-up paths that read or write a repository by its path pass
     * it by too: reconcile would have read repo2's snapshot 2 as one this
     * repository's index lost and queued a reindex of the wrong data, and
     * finishing an interrupted prune would have unlinked repo2's record.
     */
    reconcile_result_t rec;
    assert_int_equal(index_reconcile(db, NULL, NULL, &rec), OK);
    assert_int_equal(rec.repos_unreadable, 2);
    assert_int_equal(rec.repos_missing, 0);
    assert_int_equal(rec.tags_restored, 0);
    char rid[32];
    snprintf(rid, sizeof(rid), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO recovery_state (repo_id, key, value) VALUES (?, 'prune-pending-9', '9')",
        (const char *[]){ rid, NULL }, NULL), 0);
    sh("cp '%s/snapshots/1' '%s/snapshots/9'", TEST_REPO);
    free(l1);
    l1 = listing(TEST_REPO);   /* with the planted record */
    assert_int_equal(prune_finish_pending(db, repo_id, NULL), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM recovery_state WHERE repo_id = ?"
                            " AND key = 'prune-pending-9'"), 1);

    /* Nothing recorded, nothing dropped, nothing written. */
    identity_of(repo_id, j1, sizeof(j1));
    identity_of(id2, j2, sizeof(j2));
    assert_string_equal(i1, j1);
    assert_string_equal(i2, j2);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE location = 'absent'"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_tombstones"), 0);
    char *m1 = listing(TEST_REPO), *m2 = listing(TEST_REPO2);
    assert_string_equal(l1, m1);
    assert_string_equal(l2, m2);
    free(l1); free(l2); free(m1); free(m2);

    swap_roots();
    pack_cache_invalidate(store);
    pack_cache_invalidate(st2);
    assert_int_equal(unlink(TEST_REPO2 "/snapshots/9"), 0);
    assert_int_equal(pack_presence_available(db, repo_id, NULL, 0), 1);
    assert_int_equal(pack_presence_available(db, id2, NULL, 0), 1);
    assert_restores(s1);
    verify_result_t v = verify();
    assert_int_equal(v.missing, 0);
    assert_int_equal(v.unrecoverable, 0);
    assert_int_equal(v.checked, v.ok);

    store_index_free(idx2);
    store_close(st2);
}

/*
 * Every pack missing, and the root's marker is another repository's: the
 * all-missing repair checked no marker and would have dropped every pack
 * of this one on the strength of a path (B165). Refused, nothing dropped;
 * with this repository's marker back, the same repair goes through.
 */
static void test_all_missing_repair_needs_this_marker(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    int64_t s1 = backup_ok();
    (void)s1;
    remove_pack(pack_of("a"));

    char own[PATH_MAX];
    snprintf(own, sizeof(own), "%s/" CBACKUP_MARKER, TEST_DIR);
    assert_int_equal(rename(TEST_REPO "/" CBACKUP_MARKER, own), 0);
    repo_marker_t other, m;
    memset(&other, 0, sizeof(other));
    assert_int_equal(mkdir(TEST_REPO2, 0700), 0);
    assert_int_equal(repo_marker_write(TEST_REPO2, &other), OK);
    assert_int_equal(rename(TEST_REPO2 "/" CBACKUP_MARKER, TEST_REPO "/" CBACKUP_MARKER), 0);
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);

    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);
    assert_true(pp.unavailable);
    assert_non_null(strstr(pp.why, "holds repository"));
    assert_non_null(strstr(pp.why, m.uuid));
    assert_int_equal(pp.packs_dropped, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);

    assert_int_equal(rename(own, TEST_REPO "/" CBACKUP_MARKER), 0);
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.unavailable);
    assert_non_null(strstr(pp.why, "none of the 1 packs"));
    assert_int_equal(pack_presence_check(db, repo_id, 1, &pp), OK);
    assert_false(pp.unavailable);
    assert_int_equal(pp.packs_dropped, 1);
}

/*
 * FLAKE-28: a repair submitted while another job holds the repository.
 * The route tried the repo lock itself and answered 503 "repository is
 * busy" whenever anything held it - in CI, a scheduled gc. It is a job
 * now: accepted, queued behind the holder, and run once the holder ends.
 */
static atomic_int g_hold_open;
static atomic_int g_hold_running;

static status_t holding_job(void *arg, atomic_int *cancel,
                            void (*progress)(const char *, uint64_t, uint64_t, void *),
                            void *progress_ctx)
{
    (void)arg; (void)cancel; (void)progress; (void)progress_ctx;
    g_hold_running = 1;
    for (int i = 0; i < 1000 && !g_hold_open; i++) usleep(10000);
    return OK;
}

static int job_reaches(int64_t id, const char *want)
{
    char q[96];
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs WHERE id = %lld AND state = '%s'",
             (long long)id, want);
    for (int i = 0; i < 1000; i++) {
        if (scalar(q) == 1) return 1;
        usleep(5000);
    }
    return 0;
}

static void test_a_repair_waits_for_the_job_holding_the_repo(void **state)
{
    (void)state;
    int64_t ids[3];
    three_packs(ids);
    remove_pack(pack_of("c"));
    remove_pack(pack_of("b"));       /* braked: only a repair accepts it */
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(db, repo_id, 0, &pp), OK);
    assert_true(pp.degraded);

    job_manager_t *jm = NULL;
    assert_int_equal(job_manager_start(db, 2, &jm), OK);
    routes_snapshots_init(db, jm);
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);

    g_hold_open = 0;
    g_hold_running = 0;
    int64_t holder = 0;
    assert_int_equal(job_submit(jm, repo_id, "gc", NULL, holding_job, NULL, NULL, &holder), OK);
    for (int i = 0; i < 1000 && !g_hold_running; i++) usleep(5000);
    assert_true(g_hold_running);

    char url[64];
    snprintf(url, sizeof(url), "/repos/%lld/repair", (long long)repo_id);
    const char *body_in = "{\"accept_missing\":true}";
    api_request_t req = { .method = API_POST, .url = url, .body = body_in,
                          .body_len = strlen(body_in), .is_local = 1, .client_addr = "" };
    api_response_t r = api_server_dispatch(srv, &req);
    assert_int_equal(r.status, 202);
    assert_non_null(r.body);
    const char *jp = strstr(r.body, "\"job_id\":");
    assert_non_null(jp);
    int64_t repair = strtoll(jp + 9, NULL, 10);
    free(r.body);
    free(r.set_cookie);

    /* Queued behind the holder, nothing accepted yet. */
    assert_true(job_reaches(repair, "queued"));
    usleep(200 * 1000);
    assert_true(job_reaches(repair, "queued"));
    assert_true(pack_presence_degraded(db, repo_id, NULL, 0));

    g_hold_open = 1;
    assert_true(job_reaches(holder, "completed"));
    assert_true(job_reaches(repair, "completed"));
    char q[160];
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs a, jobs b WHERE a.id = %lld"
             " AND b.id = %lld AND a.started >= b.ended", (long long)repair, (long long)holder);
    assert_int_equal(scalar(q), 1);
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs WHERE id = %lld AND result LIKE"
             " '%%\"packs_dropped\":2,%%'", (long long)repair);
    assert_int_equal(scalar(q), 1);
    assert_false(pack_presence_degraded(db, repo_id, NULL, 0));
    assert_int_equal(scalar("SELECT COUNT(*) FROM journal WHERE repo_id = ?"
                            " AND operation = 'repair' AND result = 'success'"), 1);

    api_server_stop(srv);
    job_manager_stop(jm);
    routes_snapshots_init(NULL, NULL);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_missing_pack_is_stored_again, setup, teardown),
        cmocka_unit_test_setup_teardown(test_missing_pack_ancestor_changed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_an_older_snapshots_loss_is_reported, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_checks_before_dedup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unmounted_packs_dir_changes_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_transient_error_leaves_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_another_copy_still_resolves, setup, teardown),
        cmocka_unit_test_setup_teardown(test_brake_degrades_until_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_degraded_clears_when_the_packs_return, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_lone_missing_pack_heals, setup, teardown),
        cmocka_unit_test_setup_teardown(test_brake_thresholds, setup, teardown),
        cmocka_unit_test_setup_teardown(test_brake_count_threshold, setup, teardown),
        cmocka_unit_test_setup_teardown(test_every_pack_missing_is_unavailable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_dropped_pack_number_is_never_reused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_returning_pack_brings_its_objects_back, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_returning_good_copy_beats_a_bad_indexed_one, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_returning_bad_copy_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_catches_a_pack_that_is_not_what_the_index_says, setup, teardown),
        cmocka_unit_test_setup_teardown(test_uat_e6_unmounted_packs_mountpoint, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_replaced_packs_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_moved_packs_dir_is_accepted_by_repair, setup, teardown),
        cmocka_unit_test_setup_teardown(test_an_empty_repository_root, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_mark_survives_a_lost_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_tombstone_floors_a_lost_mark, setup, teardown),
        cmocka_unit_test_setup_teardown(test_swapped_roots_are_unavailable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_all_missing_repair_needs_this_marker, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_repair_waits_for_the_job_holding_the_repo, setup, teardown),
    };
    return cmocka_run_group_tests_name("pack_presence", tests, NULL, NULL);
}
