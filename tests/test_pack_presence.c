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
 *               and a lone missing pack healing whatever its share
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "object.h"
#include "pack.h"
#include "pack_presence.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

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
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'"), 0);
    assert_restores(s4);
    assert_restores(ids[2]);     /* the old snapshot names the same objects */
    v = verify();
    assert_int_equal(v.missing, 0);
    assert_int_equal(v.ok, v.checked);
}

/*
 * An older snapshot's own trees, in a missing pack, are nothing a later
 * backup of a changed source makes again: that loss stays, and verify
 * names exactly the objects still absent, while the newest snapshot and
 * everything its source still has are whole.
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
    long long gone = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'");
    assert_true(gone > 0);
    verify_result_t v = verify();
    assert_int_equal(v.missing, (uint64_t)gone);
    assert_true(v.affected_snapshots >= 1);
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
    assert_int_equal(backup(NULL), ERR_IO);
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
 * repository it is - here all of it, the UAT's case. */
static void test_a_lone_missing_pack_heals(void **state)
{
    (void)state;
    write_file("a", 1, 40000);
    int64_t s1 = backup_ok();
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 1);
    remove_pack(pack_of("a"));        /* 1 of 1 */
    uint64_t alerts = pack_presence_alerts();
    int64_t s2 = backup_ok();
    assert_int_equal(pack_presence_alerts(), alerts + 1);
    assert_false(pack_presence_degraded(db, repo_id, NULL, 0));
    assert_restores(s2);
    assert_restores(s1);
    verify_result_t v = verify();
    assert_int_equal(v.missing, 0);
    assert_int_equal(v.ok, v.checked);
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_missing_pack_is_stored_again, setup, teardown),
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
    };
    return cmocka_run_group_tests_name("pack_presence", tests, NULL, NULL);
}
