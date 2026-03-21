#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/snapshot.h"
#include "../src/policy.h"
#include "../src/gfs.h"
#include "../src/tag.h"
#include "../src/gc.h"

#define TEST_REPO "/tmp/c_backup_gfs_repo"
#define TEST_SRC  "/tmp/c_backup_gfs_src"

/*
 * Known UTC timestamps for boundary-detection unit tests.
 *
 * 2025-01-01 is Wednesday (wday=3), so:
 *   2025-01-04 = Saturday   (wday=6)
 *   2025-01-05 = Sunday     (wday=0) — NOT last Sunday of January
 *   2025-01-25 = Saturday   (wday=6)
 *   2025-01-26 = Sunday     (wday=0) — last Sunday of January 2025
 *
 * 2024-01-01 is Monday (wday=1), so:
 *   2024-12-28 = Saturday   (wday=6)
 *   2024-12-29 = Sunday     (wday=0) — last Sunday of December 2024 → yearly
 */
#define TS_SAT_2025_01_04  UINT64_C(1735948800)   /* 2025-01-04 00:00 UTC */
#define TS_SUN_2025_01_05  UINT64_C(1736035200)   /* 2025-01-05 00:00 UTC */
#define TS_MON_2025_01_06  UINT64_C(1736121600)   /* 2025-01-06 00:00 UTC */
#define TS_SAT_2025_01_25  UINT64_C(1737763200)   /* 2025-01-25 00:00 UTC */
#define TS_SUN_2025_01_26  UINT64_C(1737849600)   /* 2025-01-26 00:00 UTC */
#define TS_SAT_2024_12_28  UINT64_C(1735344000)   /* 2024-12-28 00:00 UTC */
#define TS_SUN_2024_12_29  UINT64_C(1735430400)   /* 2024-12-29 00:00 UTC */

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC);
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "initial");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO " " TEST_SRC);
    return 0;
}

/* Create n incremental backups, modifying a.txt each time. */
static void make_backups(int n) {
    const char *paths[] = { TEST_SRC };
    for (int i = 0; i < n; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "content %d", i + 1);
        write_file(TEST_SRC "/a.txt", buf);
        assert_int_equal(backup_run(repo, paths, 1), OK);
    }
}

/* Patch snap's created_sec on disk (load → modify → write). */
static void patch_snap_ts(uint32_t snap_id, uint64_t new_ts) {
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, snap_id, &snap), OK);
    snap->created_sec = new_ts;
    assert_int_equal(snapshot_write(repo, snap), OK);
    snapshot_free(snap);
}

static int snap_exists(uint32_t snap_id) {
    char path[256];
    snprintf(path, sizeof(path), TEST_REPO "/snapshots/%08u.snap", snap_id);
    return access(path, F_OK) == 0;
}

/* ================================================================== */
/* 1. gfs_detect_boundaries — pure unit tests (no repo)               */
/* ================================================================== */

static void test_detect_no_boundary_same_day(void **state) {
    (void)state;
    /* Two timestamps on the same calendar day → no flags */
    uint64_t t0 = TS_SUN_2025_01_05;
    uint64_t t1 = TS_SUN_2025_01_05 + 3600; /* one hour later */
    assert_int_equal(gfs_detect_boundaries(t0, t1), 0);
}

static void test_detect_daily_only(void **state) {
    (void)state;
    /* Saturday → Monday: day changed, new day is NOT Sunday → only daily */
    uint32_t flags = gfs_detect_boundaries(TS_SAT_2025_01_04, TS_MON_2025_01_06);
    assert_true(flags & GFS_DAILY);
    assert_false(flags & GFS_WEEKLY);
    assert_false(flags & GFS_MONTHLY);
    assert_false(flags & GFS_YEARLY);
}

static void test_detect_daily_and_weekly(void **state) {
    (void)state;
    /* Saturday → Sunday (not last Sunday of month): daily + weekly */
    uint32_t flags = gfs_detect_boundaries(TS_SAT_2025_01_04, TS_SUN_2025_01_05);
    assert_true(flags & GFS_DAILY);
    assert_true(flags & GFS_WEEKLY);
    assert_false(flags & GFS_MONTHLY);
    assert_false(flags & GFS_YEARLY);
}

static void test_detect_daily_weekly_monthly(void **state) {
    (void)state;
    /* Saturday → last Sunday of January 2025: daily + weekly + monthly, not yearly */
    uint32_t flags = gfs_detect_boundaries(TS_SAT_2025_01_25, TS_SUN_2025_01_26);
    assert_true(flags & GFS_DAILY);
    assert_true(flags & GFS_WEEKLY);
    assert_true(flags & GFS_MONTHLY);
    assert_false(flags & GFS_YEARLY);
}

static void test_detect_all_tiers_december(void **state) {
    (void)state;
    /* Saturday Dec 28 → Sunday Dec 29 2024: last Sunday of December → all four tiers */
    uint32_t flags = gfs_detect_boundaries(TS_SAT_2024_12_28, TS_SUN_2024_12_29);
    assert_true(flags & GFS_DAILY);
    assert_true(flags & GFS_WEEKLY);
    assert_true(flags & GFS_MONTHLY);
    assert_true(flags & GFS_YEARLY);
}

static void test_detect_first_backup_on_sunday(void **state) {
    (void)state;
    /* First backup (prev_ts=0) on a Sunday that is NOT the last Sunday → daily + weekly */
    uint32_t flags = gfs_detect_boundaries(0, TS_SUN_2025_01_05);
    assert_true(flags & GFS_DAILY);
    assert_true(flags & GFS_WEEKLY);
    assert_false(flags & GFS_MONTHLY);
    assert_false(flags & GFS_YEARLY);
}

static void test_detect_first_backup_on_weekday(void **state) {
    (void)state;
    /* First backup on a non-Sunday → daily only */
    uint32_t flags = gfs_detect_boundaries(0, TS_MON_2025_01_06);
    assert_true(flags & GFS_DAILY);
    assert_false(flags & GFS_WEEKLY);
}

static void test_detect_new_ts_zero(void **state) {
    (void)state;
    /* new_ts == 0 → always returns 0 regardless of prev */
    assert_int_equal(gfs_detect_boundaries(TS_SAT_2025_01_04, 0), 0);
    assert_int_equal(gfs_detect_boundaries(0, 0), 0);
}

/* ================================================================== */
/* 2. gfs_effective_keep_revs — pure unit tests                       */
/* ================================================================== */

static void test_effective_keep_revs_no_anchors(void **state) {
    (void)state;
    /* No GFS anchors: result == max(keep_revs, 0) == keep_revs */
    policy_t pol = {0};
    pol.keep_revs = 5;

    gfs_snap_info_t snaps[3] = {
        {1, 1000, 0, 0},
        {2, 2000, 0, 0},
        {3, 3000, 0, 0},
    };
    uint32_t eff = gfs_effective_keep_revs(&pol, 3, snaps, 3);
    assert_int_equal(eff, 5);
}

static void test_effective_keep_revs_extends(void **state) {
    (void)state;
    /* GFS anchor at id=1, head=10, distance=9 > keep_revs=3 → result == 9 */
    policy_t pol = {0};
    pol.keep_revs = 3;

    gfs_snap_info_t snaps[3] = {
        {1,  1000, GFS_DAILY,  0},
        {5,  5000, 0,          0},
        {10, 9000, 0,          0},
    };
    uint32_t eff = gfs_effective_keep_revs(&pol, 10, snaps, 3);
    assert_int_equal(eff, 9);
}

static void test_effective_keep_revs_zero_keep_revs(void **state) {
    (void)state;
    /* keep_revs=0 defaults to base=1 */
    policy_t pol = {0};
    pol.keep_revs = 0;

    gfs_snap_info_t snaps[1] = {{1, 1000, 0, 0}};
    uint32_t eff = gfs_effective_keep_revs(&pol, 1, snaps, 1);
    assert_int_equal(eff, 1);
}

/* ================================================================== */
/* 3. GFS flags persistence                                            */
/* ================================================================== */

static void test_gfs_flags_persist(void **state) {
    (void)state;
    make_backups(1);

    /* Initially no GFS flags */
    uint32_t flags = 0xFFFFFFFF;
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_int_equal(flags, 0u);

    /* Set daily+weekly */
    assert_int_equal(snapshot_set_gfs_flags(repo, 1, GFS_DAILY | GFS_WEEKLY), OK);

    /* Verify persisted */
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_int_equal(flags, GFS_DAILY | GFS_WEEKLY);

    /* OR in monthly — existing bits preserved */
    assert_int_equal(snapshot_set_gfs_flags(repo, 1, GFS_MONTHLY), OK);
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_int_equal(flags, GFS_DAILY | GFS_WEEKLY | GFS_MONTHLY);

    /* Full load also reflects flags */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_int_equal(snap->gfs_flags, GFS_DAILY | GFS_WEEKLY | GFS_MONTHLY);
    snapshot_free(snap);
}

/* ================================================================== */
/* 4. snapshot_synthesize_gfs                                          */
/* ================================================================== */

static void test_synthesize_gfs_creates_and_flags(void **state) {
    (void)state;
    make_backups(3);

    /* Prune snap 1 away */
    uint32_t pruned = 0;
    assert_int_equal(repo_prune(repo, 2, &pruned, 0), OK);
    assert_false(snap_exists(1));

    /* synthesize_gfs should create the snap file and set the GFS flags */
    assert_int_equal(snapshot_synthesize_gfs(repo, 1, GFS_DAILY | GFS_WEEKLY), OK);
    assert_true(snap_exists(1));

    uint32_t flags = 0;
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_int_equal(flags, GFS_DAILY | GFS_WEEKLY);
}

/* ================================================================== */
/* 5. gfs_run — pruning behaviour                                     */
/* ================================================================== */

static void test_gfs_run_prunes_non_gfs_snaps(void **state) {
    (void)state;
    make_backups(5);

    /* No GFS anchors, keep_revs=2 → snaps 1 and 2 are pruned */
    policy_t pol = {0};
    pol.keep_revs = 2;

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    assert_false(snap_exists(1));
    assert_false(snap_exists(2));
    assert_true(snap_exists(3));
    assert_true(snap_exists(4));
    assert_true(snap_exists(5));
}

static void test_gfs_run_keeps_gfs_anchor(void **state) {
    (void)state;
    make_backups(5);

    /* Manually flag snap 2 as a GFS daily anchor */
    assert_int_equal(snapshot_set_gfs_flags(repo, 2, GFS_DAILY), OK);

    /* keep_revs=1; effective = max(1, 5-2=3) = 3 → window_start=2
     * snap 1: non-GFS, id < 2 → pruned
     * snap 2: GFS flag → kept
     * snaps 3-5: within window → kept */
    policy_t pol = {0};
    pol.keep_revs = 1;

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    assert_false(snap_exists(1));
    assert_true(snap_exists(2));
    assert_true(snap_exists(3));
    assert_true(snap_exists(4));
    assert_true(snap_exists(5));
}

static void test_gfs_run_keeps_preserved_snap(void **state) {
    (void)state;
    make_backups(5);

    /* Tag snap 2 as preserved */
    assert_int_equal(tag_set(repo, "important", 2, 1), OK);

    /* keep_revs=1, no GFS anchors → window_start=4
     * snaps 1,2,3 are outside window; snap 2 is preserved → not pruned */
    policy_t pol = {0};
    pol.keep_revs = 1;

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    assert_false(snap_exists(1));
    assert_true(snap_exists(2));   /* preserved */
    assert_false(snap_exists(3));
    assert_true(snap_exists(4));
    assert_true(snap_exists(5));
}

/* ================================================================== */
/* 6. gfs_run — daily boundary detection flags the previous snap      */
/* ================================================================== */

static void test_gfs_run_flags_daily_anchor(void **state) {
    (void)state;
    make_backups(2);

    /* Patch snap 1 to yesterday so a daily boundary is detected when
     * gfs_run processes snap 2 (which stays at the current time). */
    uint64_t now_ts   = (uint64_t)time(NULL);
    uint64_t yesterday = now_ts - 86400;
    patch_snap_ts(1, yesterday);

    policy_t pol = {0};
    pol.keep_revs = 5;   /* keep everything so pruning doesn't interfere */

    assert_int_equal(gfs_run(repo, &pol, 2, 0, 1), OK);

    /* Snap 1 (from yesterday) should now have at least GFS_DAILY set */
    uint32_t flags = 0;
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_true(flags & GFS_DAILY);
}

/* ================================================================== */
/* 7. gfs_run dry-run does not modify anything                        */
/* ================================================================== */

static void test_gfs_run_dry_run_no_changes(void **state) {
    (void)state;
    make_backups(5);

    policy_t pol = {0};
    pol.keep_revs = 1;   /* would normally prune snaps 1-3 */

    assert_int_equal(gfs_run(repo, &pol, 5, /*dry_run=*/1, 1), OK);

    /* All snap files must still be present */
    for (uint32_t id = 1; id <= 5; id++)
        assert_true(snap_exists(id));
}

/* ================================================================== */
/* 8. Tier-expiry: keep_daily caps daily-tier anchors                 */
/* ================================================================== */

/* Helper: set GFS_DAILY flag on snap_id */
static void set_daily(uint32_t snap_id) {
    assert_int_equal(snapshot_set_gfs_flags(repo, snap_id, GFS_DAILY), OK);
}

static void test_tier_expiry_daily_cap(void **state) {
    (void)state;
    make_backups(5);

    /* Flag snaps 1, 2, 3 as daily-tier anchors */
    set_daily(1); set_daily(2); set_daily(3);

    /* keep_daily=2: snap 3 and 2 are the 2 newest daily anchors → snap 1 is expired.
     * keep_revs=0 (→ base=1), so rev_window_start = 5-1 = 4.
     * Snap 1: expired daily, id < 4 → pruned.
     * Snap 2: expired daily, id < 4 → pruned (2 newer dailies: 2 and 3 → count=2 >= keep=2).
     * Wait — snap 3 is newest daily (0 newer), snap 2 has 1 newer (snap3), snap 1 has 2 newer.
     * keep_daily=2 → expired when tier_newer_count >= 2 → only snap 1 is expired.
     * Snap 2: tier_newer_count=1 < 2 → NOT expired → kept as GFS anchor.
     * Snap 3: tier_newer_count=0 < 2 → NOT expired → kept as GFS anchor.
     * Snap 1: tier_newer_count=2 >= 2 → expired → pruned (id=1 < rev_window_start=4). */
    policy_t pol = {0};
    pol.keep_daily = 2;
    pol.keep_revs  = 1;  /* rev_window_start = 5 - 1 = 4 */

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    assert_false(snap_exists(1));  /* expired daily, outside rev window */
    assert_true(snap_exists(2));   /* live daily anchor (1 newer daily) */
    assert_true(snap_exists(3));   /* live daily anchor (0 newer dailies) */
    assert_true(snap_exists(4));   /* within rev window */
    assert_true(snap_exists(5));   /* HEAD */
}

static void test_tier_expiry_within_rev_window_kept(void **state) {
    (void)state;
    /* Expired daily anchors that fall inside the rev window must NOT be pruned. */
    make_backups(5);

    set_daily(1); set_daily(2); set_daily(3);

    /* keep_daily=1, keep_revs=5 → rev_window_start = max(1, 5-5=0) = 1.
     * All snaps are within the window → none pruned even though snaps 1 and 2 are expired. */
    policy_t pol = {0};
    pol.keep_daily = 1;
    pol.keep_revs  = 5;

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    for (uint32_t id = 1; id <= 5; id++)
        assert_true(snap_exists(id));
}

static void test_tier_expiry_zero_means_unlimited(void **state) {
    (void)state;
    /* keep_daily=0 → no cap; all daily anchors are kept regardless of count. */
    make_backups(5);

    for (uint32_t id = 1; id <= 4; id++) set_daily(id);

    policy_t pol = {0};
    pol.keep_daily = 0;   /* unlimited */
    pol.keep_revs  = 1;   /* rev_window_start = 4 */

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    /* Snaps 1-4 all have daily flag and keep_daily=0 → none expired → all kept */
    for (uint32_t id = 1; id <= 5; id++)
        assert_true(snap_exists(id));
}

static void test_tier_expiry_weekly_cap(void **state) {
    (void)state;
    /* keep_weekly=1: only the newest weekly anchor is retained. */
    make_backups(5);

    /* Flag snaps 1 and 3 as weekly-tier (daily+weekly) */
    assert_int_equal(snapshot_set_gfs_flags(repo, 1, GFS_DAILY | GFS_WEEKLY), OK);
    assert_int_equal(snapshot_set_gfs_flags(repo, 3, GFS_DAILY | GFS_WEEKLY), OK);

    /* keep_weekly=1: snap 3 is newest weekly (0 newer) → live.
     * Snap 1: 1 newer weekly (snap 3) >= keep_weekly=1 → expired.
     * keep_revs=1 → rev_window_start = 4.
     * Snap 1: expired weekly, id=1 < 4 → pruned. */
    policy_t pol = {0};
    pol.keep_weekly = 1;
    pol.keep_revs   = 1;

    assert_int_equal(gfs_run(repo, &pol, 5, 0, 1), OK);

    assert_false(snap_exists(1));  /* expired weekly, outside rev window */
    assert_false(snap_exists(2));  /* non-GFS, outside rev window (id=2 < 4) → pruned */
    assert_true(snap_exists(3));   /* live weekly anchor */
    assert_true(snap_exists(4));   /* within rev window */
    assert_true(snap_exists(5));   /* HEAD */
}

/* ================================================================== */
/* main                                                                */
/* ================================================================== */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Boundary detection */
        cmocka_unit_test(test_detect_no_boundary_same_day),
        cmocka_unit_test(test_detect_daily_only),
        cmocka_unit_test(test_detect_daily_and_weekly),
        cmocka_unit_test(test_detect_daily_weekly_monthly),
        cmocka_unit_test(test_detect_all_tiers_december),
        cmocka_unit_test(test_detect_first_backup_on_sunday),
        cmocka_unit_test(test_detect_first_backup_on_weekday),
        cmocka_unit_test(test_detect_new_ts_zero),

        /* Effective keep-revs */
        cmocka_unit_test(test_effective_keep_revs_no_anchors),
        cmocka_unit_test(test_effective_keep_revs_extends),
        cmocka_unit_test(test_effective_keep_revs_zero_keep_revs),

        /* Integration: flags */
        cmocka_unit_test_setup_teardown(test_gfs_flags_persist,              setup, teardown),
        cmocka_unit_test_setup_teardown(test_synthesize_gfs_creates_and_flags, setup, teardown),

        /* Integration: gfs_run */
        cmocka_unit_test_setup_teardown(test_gfs_run_prunes_non_gfs_snaps,   setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_run_keeps_gfs_anchor,       setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_run_keeps_preserved_snap,   setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_run_flags_daily_anchor,     setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_run_dry_run_no_changes,     setup, teardown),

        /* Integration: tier expiry */
        cmocka_unit_test_setup_teardown(test_tier_expiry_daily_cap,           setup, teardown),
        cmocka_unit_test_setup_teardown(test_tier_expiry_within_rev_window_kept, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tier_expiry_zero_means_unlimited, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tier_expiry_weekly_cap,          setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
