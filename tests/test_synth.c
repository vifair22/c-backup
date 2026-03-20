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

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/snapshot.h"
#include "../src/restore.h"
#include "../src/gc.h"
#include "../src/synth.h"

#define TEST_REPO "/tmp/c_backup_synth_repo"
#define TEST_SRC  "/tmp/c_backup_synth_src"
#define TEST_DEST "/tmp/c_backup_synth_dest"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "version one");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    return 0;
}

/*
 * Synthesize a checkpoint for a pruned snapshot and verify it can be
 * restored without touching the reverse chain.
 */
static void test_synthesize_then_restore(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    /* Three distinct backups */
    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 1 */
    write_file(TEST_SRC "/a.txt", "version two");
    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 2 */
    write_file(TEST_SRC "/a.txt", "version three");
    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 3 */

    /* Synthesize snap 2 (not pruned yet, but we can still synthesize) */
    assert_int_equal(snapshot_synthesize(repo, 2), OK);

    /* Snapshot 2 file must now be loadable */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    assert_non_null(snap);
    snapshot_free(snap);

    /* Restore snap 2 to a fresh dest — should work via the snap file directly */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);

    /* Verify restored content */
    char rpath[256];
    snprintf(rpath, sizeof(rpath), TEST_DEST "/c_backup_synth_src/a.txt");
    FILE *f = fopen(rpath, "r");
    assert_non_null(f);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    assert_string_equal(buf, "version two");

    system("rm -rf " TEST_DEST);
}

/*
 * After prune removes snap 1, synthesize it and restore.
 */
static void test_synthesize_pruned_snapshot(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 1 */
    write_file(TEST_SRC "/a.txt", "version two");
    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 2 */
    write_file(TEST_SRC "/a.txt", "version three");
    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 3 */

    /* Prune to 2 — removes snap 1 */
    uint32_t pruned = 0;
    assert_int_equal(repo_prune(repo, 2, &pruned, 0), OK);
    assert_int_equal(pruned, 1u);

    /* Snap 1 file is gone */
    char snap1[256];
    snprintf(snap1, sizeof(snap1), TEST_REPO "/snapshots/00000001.snap");
    assert_int_not_equal(access(snap1, F_OK), 0);

    /* Synthesize snap 1 from the reverse chain */
    assert_int_equal(snapshot_synthesize(repo, 1), OK);

    /* Snap 1 file now exists */
    assert_int_equal(access(snap1, F_OK), 0);

    /* Restore snap 1 and verify content */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char rpath[256];
    snprintf(rpath, sizeof(rpath), TEST_DEST "/c_backup_synth_src/a.txt");
    FILE *f = fopen(rpath, "r");
    assert_non_null(f);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    assert_string_equal(buf, "version one");

    system("rm -rf " TEST_DEST);
}

/*
 * snapshot_synthesize_every places checkpoints at regular intervals.
 */
static void test_synthesize_every(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    /* Create 5 distinct snapshots */
    for (int v = 1; v <= 5; v++) {
        char content[32];
        snprintf(content, sizeof(content), "version %d", v);
        write_file(TEST_SRC "/a.txt", content);
        assert_int_equal(backup_run(repo, paths, 1), OK);
    }

    /* Prune to keep only the latest 2 (removes 1-3) */
    uint32_t pruned = 0;
    assert_int_equal(repo_prune(repo, 2, &pruned, 0), OK);
    assert_int_equal(pruned, 3u);

    /* Synthesize every 2nd snapshot */
    uint32_t count = 0;
    assert_int_equal(snapshot_synthesize_every(repo, 2, &count), OK);
    /* Snap 2 and 4 are multiples of 2; snap 2 was pruned so will be synthesized.
     * Snap 4 was also pruned so will be synthesized. */
    assert_true(count >= 2u);

    /* Snaps 2 and 4 must now be loadable */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    snapshot_free(snap);
    assert_int_equal(snapshot_load(repo, 4, &snap), OK);
    snapshot_free(snap);
}

/*
 * Calling synthesize on a snapshot that already has a file is a no-op.
 */
static void test_synthesize_noop_when_exists(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 1 */
    write_file(TEST_SRC "/a.txt", "version two");
    assert_int_equal(backup_run(repo, paths, 1), OK);               /* snap 2 */

    /* Snap 2 already has a file; synthesize must succeed and be a no-op */
    assert_int_equal(snapshot_synthesize(repo, 2), OK);

    /* Still loads correctly */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    snapshot_free(snap);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_synthesize_then_restore,    setup, teardown),
        cmocka_unit_test_setup_teardown(test_synthesize_pruned_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_synthesize_every,           setup, teardown),
        cmocka_unit_test_setup_teardown(test_synthesize_noop_when_exists, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
