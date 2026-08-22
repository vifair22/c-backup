#define _POSIX_C_SOURCE 200809L

/*
 * GFS retention.
 *
 * The calendar boundaries and tier promotion were carried over unchanged from
 * the pre-rewrite implementation; what changed is where the data comes from
 * and where results go. These tests exercise the ported plumbing against
 * synthetic snapshot rows with controlled timestamps, which is the only way
 * to test calendar behaviour without waiting a year.
 *
 * Tier flags are a bitfield because a snapshot can hold several at once: the
 * last Sunday of December is that day's daily, that week's weekly, that
 * month's monthly and that year's yearly simultaneously. Collapsing it to a
 * single tier would lose the membership that keeps it alive once the daily
 * window rolls past.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "gfs.h"
#include "repo.h"
#include "store.h"

#define TEST_DIR  "/tmp/c_backup_test_gfs"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static store_t     *store;
static int64_t      repo_id;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_close(store); store = NULL;
    db_close(db);       db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/*
 * Insert a snapshot at a controlled instant. Records on disk are written too,
 * because pruning unlinks them and a missing file would mask a prune that did
 * not actually happen.
 */
static void add_snapshot(int id, time_t ts)
{
    char repo[32], sid[32], tsb[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%d", id);
    snprintf(tsb,  sizeof(tsb),  "%lld", (long long)ts);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO snapshots (repo_id, id, ts, root_tree_hash, gfs_flags)"
        " VALUES (?, ?, ?, X'00', 0)",
        (const char *[]){ repo, sid, tsb, NULL }, NULL), 0);

    char path[512];
    snprintf(path, sizeof(path), "%s/snapshots/%d", TEST_REPO, id);
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fprintf(f, "{\"id\":%d,\"ts\":%lld,\"root\":\"%064d\"}\n", id, (long long)ts, 0);
    fclose(f);
}

static uint32_t flags_of(int id)
{
    char repo[32], sid[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%d", id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT gfs_flags FROM snapshots WHERE repo_id = ? AND id = ?",
                   (const char *[]){ repo, sid, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0xFFFFFFFFu;   /* absent */
    }
    uint32_t v = (uint32_t)strtoul(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int snapshot_count(void)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT COUNT(*) FROM snapshots WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0) return -1;
    int n = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return n;
}

/*
 * 2024-01-07 was a Sunday. The anchor is noon rather than midnight, and the
 * suite pins TZ=UTC in main(), because cal_day() maps timestamps to *local*
 * calendar days: a UTC-midnight Sunday is Saturday evening west of Greenwich,
 * which silently shifts every tier boundary by a day and makes the results
 * depend on where the test happens to run.
 */
#define SUNDAY_2024_01_07 (1704585600L + 43200L)
#define DAY 86400L

/* ------------------------------------------------------------------ */

static void test_daily_flag_is_set(void **state)
{
    (void)state;
    for (int i = 0; i < 3; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);

    gfs_retention_t ret = { 0, 30, 8, 12, 3 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 3, 0, 1, 1, NULL), OK);

    /*
     * Each closed day gets its daily. The newest snapshot does not: its day
     * is still open and a later backup today would supersede it. Promoting it
     * early would mean two dailies for one day the moment that happened.
     */
    assert_true(flags_of(1) & GFS_DAILY);
    assert_true(flags_of(2) & GFS_DAILY);
    assert_int_equal(flags_of(3), 0);
}

/* Two backups on one day: only the later is that day's daily. */
static void test_last_of_day_wins(void **state)
{
    (void)state;
    add_snapshot(1, SUNDAY_2024_01_07 + 3600);
    add_snapshot(2, SUNDAY_2024_01_07 + 7200);

    /* Three snapshots: two on day one, one on day two. Day one is closed, so
     * exactly one of its two is that day's daily, and it is the later. */
    add_snapshot(3, SUNDAY_2024_01_07 + DAY);

    gfs_retention_t ret = { 0, 30, 8, 12, 3 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 3, 0, 1, 1, NULL), OK);

    assert_false(flags_of(1) & GFS_DAILY);
    assert_true(flags_of(2) & GFS_DAILY);
}

/* Sunday's daily is promoted to weekly. */
static void test_sunday_is_promoted_to_weekly(void **state)
{
    (void)state;
    for (int i = 0; i < 8; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);

    gfs_retention_t ret = { 0, 30, 8, 12, 3 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 8, 0, 1, 1, NULL), OK);

    /* Snapshot 1 is the Sunday; the following weekdays are not. */
    assert_true(flags_of(1) & GFS_WEEKLY);
    for (int i = 2; i <= 7; i++)
        assert_false(flags_of(i) & GFS_WEEKLY);
}

/* A snapshot can hold several tiers at once. */
static void test_flags_are_a_bitfield(void **state)
{
    (void)state;
    for (int i = 0; i < 40; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);

    gfs_retention_t ret = { 0, 60, 12, 12, 3 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 40, 0, 1, 1, NULL), OK);

    int multi = 0;
    for (int i = 1; i <= 40; i++) {
        uint32_t f = flags_of(i);
        if (f == 0xFFFFFFFFu) continue;
        int tiers = ((f & GFS_DAILY) != 0) + ((f & GFS_WEEKLY) != 0)
                  + ((f & GFS_MONTHLY) != 0) + ((f & GFS_YEARLY) != 0);
        if (tiers > 1) multi++;
    }
    assert_true(multi > 0);
}

/* A dry run reports without deleting. */
static void test_dry_run_prunes_nothing(void **state)
{
    (void)state;
    for (int i = 0; i < 20; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);
    int before = snapshot_count();

    gfs_retention_t ret = { 3, 2, 1, 1, 1 };
    uint32_t pruned = 0;
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 20, 1, 1, 1, &pruned), OK);

    assert_true(pruned > 0);
    assert_int_equal(snapshot_count(), before);
}

static void test_prune_removes_expired_snapshots(void **state)
{
    (void)state;
    for (int i = 0; i < 20; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);
    int before = snapshot_count();

    gfs_retention_t ret = { 3, 2, 1, 1, 1 };
    uint32_t pruned = 0;
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 20, 0, 1, 1, &pruned), OK);

    assert_true(pruned > 0);
    assert_true(snapshot_count() < before);
    /* The newest is never pruned: it is the one just taken. */
    assert_int_not_equal(flags_of(20), 0xFFFFFFFFu);
}

/*
 * A tagged snapshot survives regardless of retention. Tags are how a user
 * says "keep this one", and quietly deleting it because a window rolled past
 * would be the worst kind of surprise.
 */
static void test_tagged_snapshots_are_preserved(void **state)
{
    (void)state;
    for (int i = 0; i < 20; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO tags (repo_id, name, snap_id, created_at)"
        " VALUES (?, 'milestone', 2, 0)",
        (const char *[]){ repo, NULL }, NULL), 0);

    gfs_retention_t ret = { 3, 2, 1, 1, 1 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 20, 0, 1, 1, NULL), OK);

    assert_int_not_equal(flags_of(2), 0xFFFFFFFFu);
}

/* Retention of zero for a tier disables pruning at that tier. */
static void test_zero_retention_keeps_everything(void **state)
{
    (void)state;
    for (int i = 0; i < 15; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);
    int before = snapshot_count();

    gfs_retention_t ret = { 0, 0, 0, 0, 0 };
    uint32_t pruned = 0;
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 15, 0, 1, 1, &pruned), OK);

    assert_int_equal(pruned, 0);
    assert_int_equal(snapshot_count(), before);
}

static void test_empty_repo_is_not_an_error(void **state)
{
    (void)state;
    gfs_retention_t ret = { 3, 2, 1, 1, 1 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 0, 0, 1, 1, NULL), OK);
}

/* Running twice must be stable: the second pass changes nothing. */
static void test_gfs_is_idempotent(void **state)
{
    (void)state;
    for (int i = 0; i < 12; i++)
        add_snapshot(i + 1, SUNDAY_2024_01_07 + i * DAY);

    gfs_retention_t ret = { 0, 30, 8, 12, 3 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 12, 0, 1, 1, NULL), OK);

    uint32_t snapshot_flags[13];
    for (int i = 1; i <= 12; i++) snapshot_flags[i] = flags_of(i);

    assert_int_equal(gfs_run(store, db, repo_id, &ret, 12, 0, 1, 1, NULL), OK);
    for (int i = 1; i <= 12; i++)
        assert_int_equal(flags_of(i), snapshot_flags[i]);
}

int main(void)
{
    /* Tier boundaries are local calendar days, so an unpinned timezone makes
     * these results depend on the machine running them. */
    setenv("TZ", "UTC", 1);
    tzset();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_daily_flag_is_set, setup, teardown),
        cmocka_unit_test_setup_teardown(test_last_of_day_wins, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sunday_is_promoted_to_weekly, setup, teardown),
        cmocka_unit_test_setup_teardown(test_flags_are_a_bitfield, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dry_run_prunes_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_removes_expired_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tagged_snapshots_are_preserved, setup, teardown),
        cmocka_unit_test_setup_teardown(test_zero_retention_keeps_everything, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_repo_is_not_an_error, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_is_idempotent, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
