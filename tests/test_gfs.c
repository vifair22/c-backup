#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/gfs.h"
#include "../src/policy.h"
#include "../src/repo.h"
#include "../src/snapshot.h"

#define TEST_REPO "/tmp/c_backup_gfs_repo"
#define TEST_SRC  "/tmp/c_backup_gfs_src"
#define TEST_TIMEOUT_SEC 30

/*
 * 2026-03 timestamps (UTC noon = day*86400 + 43200).
 * Calendar reference: March 1 2026 = day 20513 (Sunday).
 *   Mar 4=Wed, Mar 11=Wed, Mar 16=Mon, Mar 17=Tue, Mar 18=Wed,
 *   Mar 19=Thu, Mar 20=Fri, Mar 21=Sat, Mar 22=Sun, Mar 23=Mon, Mar 24=Tue.
 */
#define TS_MAR04_NOON  1772625600ULL  /* Wed Mar  4 2026 12:00 UTC */
#define TS_MAR11_NOON  1773230400ULL  /* Wed Mar 11 2026 12:00 UTC */
#define TS_MAR16_NOON  1773662400ULL  /* Mon Mar 16 2026 12:00 UTC */
#define TS_MAR17_NOON  1773748800ULL  /* Tue Mar 17 2026 12:00 UTC */
#define TS_MAR18_NOON  1773835200ULL  /* Wed Mar 18 2026 12:00 UTC */
#define TS_MAR19_NOON  1773921600ULL  /* Thu Mar 19 2026 12:00 UTC */
#define TS_MAR20_NOON  1774008000ULL  /* Fri Mar 20 2026 12:00 UTC */
#define TS_MAR21_NOON  1774094400ULL  /* Sat Mar 21 2026 12:00 UTC */
#define TS_MAR22_NOON  1774180800ULL  /* Sun Mar 22 2026 12:00 UTC */
#define TS_MAR23_0900  1774256400ULL  /* Mon Mar 23 2026 09:00 UTC */
#define TS_MAR23_1000  1774260000ULL  /* Mon Mar 23 2026 10:00 UTC */
#define TS_MAR23_1100  1774263600ULL  /* Mon Mar 23 2026 11:00 UTC */
#define TS_MAR23_NOON  1774267200ULL  /* Mon Mar 23 2026 12:00 UTC */
#define TS_MAR24_1100  1774350000ULL  /* Tue Mar 24 2026 11:00 UTC */

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static status_t set_snap_ts(uint32_t snap_id, uint64_t ts) {
    snapshot_t *s = NULL;
    status_t st = snapshot_load(repo, snap_id, &s);
    if (st != OK) return st;
    s->created_sec = ts;
    st = snapshot_write(repo, s);
    snapshot_free(s);
    return st;
}

static int setup(void **state) {
    (void)state;
    alarm(TEST_TIMEOUT_SEC);
    (void)system("rm -rf " TEST_REPO " " TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file(TEST_SRC "/f.txt", "v1\n");
    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    alarm(0);
    repo_close(repo);
    (void)system("rm -rf " TEST_REPO " " TEST_SRC);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static uint32_t make_snap(const char *content) {
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    write_file(TEST_SRC "/f.txt", content);
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    uint32_t id = 0;
    snapshot_read_head(repo, &id);
    return id;
}

static uint32_t flags_of(uint32_t id) {
    uint32_t f = 0;
    snapshot_read_gfs_flags(repo, id, &f);
    return f;
}

static policy_t make_policy(int snaps, int daily, int weekly, int monthly, int yearly) {
    policy_t p;
    memset(&p, 0, sizeof(p));
    p.keep_snaps   = snaps;
    p.keep_daily   = daily;
    p.keep_weekly  = weekly;
    p.keep_monthly = monthly;
    p.keep_yearly  = yearly;
    return p;
}

/* Simulate incremental post-backup gfs_run for a specific snap. */
static void run_gfs(uint32_t snap_id, const policy_t *pol) {
    assert_int_equal(gfs_run(repo, pol, snap_id, 0, 1, 0, NULL), OK);
}

/* ------------------------------------------------------------------ */
/* Tests                                                               */
/* ------------------------------------------------------------------ */

static void test_gfs_flags_str(void **state) {
    (void)state;
    char buf[64];
    gfs_flags_str(0, buf, sizeof(buf));
    assert_string_equal(buf, "none");
    gfs_flags_str(GFS_DAILY | GFS_WEEKLY | GFS_MONTHLY | GFS_YEARLY, buf, sizeof(buf));
    assert_string_equal(buf, "daily+weekly+monthly+yearly");
}

/*
 * Two snaps on the same calendar day: no day is closed yet, nothing elected.
 */
static void test_same_day_no_election(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a");
    uint32_t s2 = make_snap("b");
    assert_int_equal(set_snap_ts(s1, TS_MAR23_0900), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR23_1000), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    run_gfs(s2, &pol);

    assert_int_equal(flags_of(s1), 0);
    assert_int_equal(flags_of(s2), 0);
}

/*
 * Three snaps on Monday, fourth on Tuesday.
 * When Tuesday's snap triggers gfs_run, Monday is now a closed day.
 * The latest snap of Monday (s3) should be elected as daily anchor.
 */
static void test_daily_election(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a");
    uint32_t s2 = make_snap("b");
    uint32_t s3 = make_snap("c");
    uint32_t s4 = make_snap("d");
    assert_int_equal(set_snap_ts(s1, TS_MAR23_0900), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR23_1000), OK);
    assert_int_equal(set_snap_ts(s3, TS_MAR23_1100), OK);
    assert_int_equal(set_snap_ts(s4, TS_MAR24_1100), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    run_gfs(s4, &pol);

    assert_true((flags_of(s3) & GFS_DAILY) != 0);  /* latest Monday snap */
    assert_int_equal(flags_of(s1) & GFS_DAILY, 0);
    assert_int_equal(flags_of(s2) & GFS_DAILY, 0);
    assert_int_equal(flags_of(s4) & GFS_DAILY, 0); /* today, not closed */
}

/*
 * Saturday snap, Sunday snap, Monday snap (incremental runs for each).
 * Saturday should become daily when Sunday's run fires.
 * Sunday should become daily+weekly when Monday's run fires
 * (Sunday closes the Mon-Sun week).
 */
static void test_weekly_election_on_sunday(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Sat Mar 21 */
    uint32_t s2 = make_snap("b"); /* Sun Mar 22 */
    uint32_t s3 = make_snap("c"); /* Mon Mar 23 */
    assert_int_equal(set_snap_ts(s1, TS_MAR21_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR22_NOON), OK);
    assert_int_equal(set_snap_ts(s3, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    run_gfs(s2, &pol); /* closes Saturday: s1 gets daily */
    run_gfs(s3, &pol); /* closes Sunday:   s2 gets daily+weekly */

    assert_true((flags_of(s1) & GFS_DAILY)  != 0);
    assert_int_equal(flags_of(s1) & GFS_WEEKLY, 0);
    assert_true((flags_of(s2) & GFS_DAILY)  != 0);
    assert_true((flags_of(s2) & GFS_WEEKLY) != 0);
}

/*
 * Friday snap, Monday snap (no Saturday or Sunday backup).
 * The week's Sunday was missed but Friday is still the latest daily
 * within the Mon-Sun window, so it should be elected as weekly anchor.
 */
static void test_weekly_election_missed_sunday(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Fri Mar 20 */
    uint32_t s2 = make_snap("b"); /* Mon Mar 23 */
    assert_int_equal(set_snap_ts(s1, TS_MAR20_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    run_gfs(s2, &pol);

    assert_true((flags_of(s1) & GFS_DAILY)  != 0);
    assert_true((flags_of(s1) & GFS_WEEKLY) != 0);
}

/*
 * Two snaps on consecutive Wednesdays, then Monday.
 * Each Wednesday should become the weekly anchor for its own Mon-Sun window.
 * Requires incremental runs for each snap so both weeks are processed.
 */
static void test_weekly_multi_gap(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Wed Mar 11 */
    uint32_t s2 = make_snap("b"); /* Wed Mar 18 */
    uint32_t s3 = make_snap("c"); /* Mon Mar 23 */
    assert_int_equal(set_snap_ts(s1, TS_MAR11_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR18_NOON), OK);
    assert_int_equal(set_snap_ts(s3, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    run_gfs(s2, &pol); /* week Mar 9-15: s1 is best daily → weekly */
    run_gfs(s3, &pol); /* week Mar 16-22: s2 is best daily → weekly */

    assert_true((flags_of(s1) & GFS_WEEKLY) != 0);
    assert_true((flags_of(s2) & GFS_WEEKLY) != 0);
}

/*
 * Wednesday snap, then Monday 3 weeks later (gap covers 3 full weeks).
 * Only the first week (Mar 2-8) has a snap; the next two are empty.
 * Empty weeks produce no weekly anchor.
 */
static void test_weekly_empty_week_skipped(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Wed Mar  4 */
    uint32_t s2 = make_snap("b"); /* Mon Mar 23 */
    assert_int_equal(set_snap_ts(s1, TS_MAR04_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    run_gfs(s2, &pol);

    /* Mar 4 (Wed) is the only snap; its week (Mar 2-8) gets a weekly anchor */
    assert_true((flags_of(s1) & GFS_WEEKLY) != 0);
    /* s2 is today (Monday), not yet closed — no flags */
    assert_int_equal(flags_of(s2), 0);
}

/*
 * full_scan=1 recomputes GFS flags from scratch across all history.
 * Simulate a situation where incremental runs were missed: create
 * Sat/Sun/Mon snaps but only run gfs with full_scan on Monday.
 * Both Sat and Sun should be retroactively elected.
 */
static void test_full_scan_retroactive(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Sat Mar 21 */
    uint32_t s2 = make_snap("b"); /* Sun Mar 22 */
    uint32_t s3 = make_snap("c"); /* Mon Mar 23 */
    assert_int_equal(set_snap_ts(s1, TS_MAR21_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR22_NOON), OK);
    assert_int_equal(set_snap_ts(s3, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);
    assert_int_equal(gfs_run(repo, &pol, s3, 0, 1, 1, NULL), OK); /* full_scan=1 */

    assert_true((flags_of(s1) & GFS_DAILY)  != 0);
    assert_true((flags_of(s2) & GFS_DAILY)  != 0);
    assert_true((flags_of(s2) & GFS_WEEKLY) != 0);
    assert_int_equal(flags_of(s3) & GFS_DAILY, 0); /* today, not closed */
}

/*
 * Incremental recovery: if gfs_run was missed for a snap (e.g. crash
 * between snapshot commit and gfs_run), the next incremental run must
 * detect the unflagged snap on a closed day and extend backward to
 * cover it.  Without this, a missed day stays unflagged forever.
 *
 * Simulate: 3 snaps on Mon/Tue/Wed, but only run incremental gfs for
 * snap 3 (skipping snap 2).  Snap 1 should still get daily because
 * the incremental scan extends backward to cover the unflagged day.
 */
static void test_incremental_recovery(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Mon Mar 16 */
    uint32_t s2 = make_snap("b"); /* Tue Mar 17 */
    uint32_t s3 = make_snap("c"); /* Wed Mar 18 */
    assert_int_equal(set_snap_ts(s1, TS_MAR16_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR17_NOON), OK);
    assert_int_equal(set_snap_ts(s3, TS_MAR18_NOON), OK);

    policy_t pol = make_policy(10, 7, 4, 3, 1);

    /* Only run gfs for snap 1 (no previous → scan covers epoch..day1,
     * but day1 is excluded → nothing flagged). */
    run_gfs(s1, &pol);
    assert_int_equal(flags_of(s1), 0);

    /* Skip gfs_run for snap 2 entirely (simulates crash). */

    /* Run gfs for snap 3.  Normal incremental would scan [day2, day3)
     * which only covers Tue → snap 2 gets daily but snap 1 does NOT.
     * With recovery, scan extends back to day1 → snap 1 also gets daily. */
    run_gfs(s3, &pol);

    assert_true((flags_of(s1) & GFS_DAILY) != 0);  /* recovered */
    assert_true((flags_of(s2) & GFS_DAILY) != 0);
    assert_int_equal(flags_of(s3) & GFS_DAILY, 0);  /* today, not closed */
}

/*
 * Dry run must not write any flags or delete any snaps.
 */
static void test_dry_run_does_not_modify(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a");
    uint32_t s2 = make_snap("b");
    assert_int_equal(set_snap_ts(s1, TS_MAR21_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(1, 1, 1, 1, 1);
    assert_int_equal(gfs_run(repo, &pol, s2, 1, 1, 0, NULL), OK); /* dry_run=1 */

    assert_int_equal(flags_of(s1), 0);
    snapshot_t *s = NULL;
    assert_int_equal(snapshot_load(repo, s1, &s), OK);
    snapshot_free(s);
}

/*
 * Prune: with keep_daily=1 the oldest daily anchors should be expired
 * and removed once enough newer daily anchors exist.
 * Setup: 4 snaps Mon-Thu, 5th snap on the following Monday.
 * After full_scan gfs_run: Mon-Thu all get daily, Thu also gets weekly
 * (best daily in Mar 16-22 window). With keep_daily=1: Mon and Tue are
 * expired (2 and 1 newer daily anchors respectively >= keep=1) and pruned.
 */
static void test_prune_expired_daily(void **state) {
    (void)state;
    uint32_t s1 = make_snap("a"); /* Mon Mar 16 */
    uint32_t s2 = make_snap("b"); /* Tue Mar 17 */
    uint32_t s3 = make_snap("c"); /* Wed Mar 18 */
    uint32_t s4 = make_snap("d"); /* Thu Mar 19 */
    uint32_t s5 = make_snap("e"); /* Mon Mar 23 */
    assert_int_equal(set_snap_ts(s1, TS_MAR16_NOON), OK);
    assert_int_equal(set_snap_ts(s2, TS_MAR17_NOON), OK);
    assert_int_equal(set_snap_ts(s3, TS_MAR18_NOON), OK);
    assert_int_equal(set_snap_ts(s4, TS_MAR19_NOON), OK);
    assert_int_equal(set_snap_ts(s5, TS_MAR23_NOON), OK);

    policy_t pol = make_policy(1, 1, 0, 0, 0);
    assert_int_equal(gfs_run(repo, &pol, s5, 0, 1, 1, NULL), OK); /* full_scan */

    snapshot_t *s = NULL;
    assert_true(snapshot_load(repo, s1, &s) != OK); /* expired daily, pruned */
    assert_true(snapshot_load(repo, s2, &s) != OK); /* expired daily, pruned */
    assert_int_equal(snapshot_load(repo, s3, &s), OK); snapshot_free(s);
    assert_int_equal(snapshot_load(repo, s4, &s), OK); snapshot_free(s);
    assert_int_equal(snapshot_load(repo, s5, &s), OK); snapshot_free(s);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gfs_flags_str,                  setup, teardown),
        cmocka_unit_test_setup_teardown(test_same_day_no_election,           setup, teardown),
        cmocka_unit_test_setup_teardown(test_daily_election,                 setup, teardown),
        cmocka_unit_test_setup_teardown(test_weekly_election_on_sunday,      setup, teardown),
        cmocka_unit_test_setup_teardown(test_weekly_election_missed_sunday,  setup, teardown),
        cmocka_unit_test_setup_teardown(test_weekly_multi_gap,               setup, teardown),
        cmocka_unit_test_setup_teardown(test_weekly_empty_week_skipped,      setup, teardown),
        cmocka_unit_test_setup_teardown(test_full_scan_retroactive,          setup, teardown),
        cmocka_unit_test_setup_teardown(test_incremental_recovery,           setup, teardown),
        cmocka_unit_test_setup_teardown(test_dry_run_does_not_modify,        setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_expired_daily,            setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
