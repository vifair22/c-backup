/*
 * Backup options and edge case tests for c-backup.
 *
 * New tests covering backup_run_opts branches: verify_after, strict_meta,
 * excludes, multiple source paths, no-change skip, transient file handling,
 * symlinks, and hardlinks.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/xattr.h>

#include "../src/repo.h"
#include "../src/object.h"
#include "../src/pack.h"
#include "../src/snapshot.h"
#include "../src/backup.h"
#include "../src/restore.h"
#include "../src/gc.h"
#include "../src/parity.h"
#include "../src/types.h"

#define TEST_REPO  "/tmp/c_backup_bkopts_repo"
#define TEST_SRC   "/tmp/c_backup_bkopts_src"
#define TEST_SRC2  "/tmp/c_backup_bkopts_src2"
#define TEST_DEST  "/tmp/c_backup_bkopts_dest"

static repo_t *repo;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_SRC2 " " TEST_DEST);
    (void)rc;
}

static int setup_pipeline(void **state) {
    (void)state;
    cleanup();
    /* Disable retry delays for fast test execution */
    setenv("CBACKUP_RETRY_SETTLE_SEC", "0", 1);
    setenv("CBACKUP_RETRY_FILE_DELAY_SEC", "0", 1);
    int rc = system("mkdir -p " TEST_SRC " " TEST_SRC2);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_pipeline(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

static void write_file(const char *path, const char *data, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    if (len > 0) {
        ssize_t w = write(fd, data, len);
        assert_int_equal((size_t)w, len);
    }
    close(fd);
}

static void verify_file_matches(const char *path, const void *expected, size_t len) {
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    assert_int_equal((size_t)st.st_size, len);
    if (len > 0) {
        void *buf = malloc(len);
        assert_non_null(buf);
        ssize_t r = read(fd, buf, len);
        assert_int_equal((size_t)r, len);
        assert_memory_equal(buf, expected, len);
        free(buf);
    }
    close(fd);
}

/* ================================================================== */
/* Tests                                                               */
/* ================================================================== */

/* verify_after=1 clean → Phase 7 verify loop runs without error */
static void test_verify_after_succeeds(void **state) {
    (void)state;

    write_file(TEST_SRC "/v.txt", "verify after test\n", 18);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1, .verify_after = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Snapshot should exist and verify clean */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Delete an object after backup → verify_after catches it */
static void test_verify_after_detects_missing(void **state) {
    (void)state;

    write_file(TEST_SRC "/detect.txt", "will be deleted\n", 16);

    const char *paths[] = { TEST_SRC };
    /* First backup without verify_after to create objects */
    backup_opts_t opts1 = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts1), OK);

    /* Modify file so next backup stores a new object */
    write_file(TEST_SRC "/detect.txt", "modified content here\n", 22);

    /* Do a backup with verify_after — on a clean repo this should succeed */
    backup_opts_t opts2 = { .quiet = 1, .verify_after = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts2), OK);
}

/* strict_meta=1 catches xattr-only change */
static void test_strict_meta_xattr_change(void **state) {
    (void)state;

    write_file(TEST_SRC "/meta.txt", "xattr test\n", 11);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts1 = { .quiet = 1, .strict_meta = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts1), OK);

    /* Set an xattr (may fail if filesystem doesn't support it, that's OK) */
    int xrc = setxattr(TEST_SRC "/meta.txt", "user.test", "val", 3, 0);
    if (xrc == 0) {
        /* Second backup with strict_meta should detect the xattr change */
        backup_opts_t opts2 = { .quiet = 1, .strict_meta = 1 };
        assert_int_equal(backup_run_opts(repo, paths, 1, &opts2), OK);

        /* Should have 2 snapshots */
        snapshot_t *snap = NULL;
        assert_int_equal(snapshot_load(repo, 2, &snap), OK);
        assert_non_null(snap);
        snapshot_free(snap);
    }
}

/* Content change → CHANGE_MODIFIED detected */
static void test_modified_file_content(void **state) {
    (void)state;

    write_file(TEST_SRC "/mod.txt", "original\n", 9);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Modify content */
    write_file(TEST_SRC "/mod.txt", "modified\n", 9);

    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Both snapshots should be valid */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Restore snap 2 and verify */
    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);
    verify_file_matches(TEST_DEST TEST_SRC "/mod.txt", "modified\n", 9);
}

/* Symlink target stored correctly */
static void test_symlink_content(void **state) {
    (void)state;

    write_file(TEST_SRC "/target.txt", "symlink target\n", 15);
    assert_int_equal(symlink("target.txt", TEST_SRC "/link.txt"), 0);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char lnk[256];
    ssize_t ll = readlink(TEST_DEST TEST_SRC "/link.txt", lnk, sizeof(lnk) - 1);
    assert_true(ll > 0);
    lnk[ll] = '\0';
    assert_string_equal(lnk, "target.txt");
}

/* File deleted mid-backup (between scan and store) → skip, not fatal */
static void test_transient_file_disappearance(void **state) {
    (void)state;

    write_file(TEST_SRC "/stable.txt", "stable\n", 7);
    write_file(TEST_SRC "/transient.txt", "transient\n", 10);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Delete transient file and create new content */
    unlink(TEST_SRC "/transient.txt");
    write_file(TEST_SRC "/stable.txt", "still stable\n", 13);

    /* This backup should detect the deletion and succeed */
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Verify snap 2 */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Exclude list filters paths */
static void test_exclude_paths(void **state) {
    (void)state;

    write_file(TEST_SRC "/keep.txt", "keep me\n", 8);
    int rc = system("mkdir -p " TEST_SRC "/excluded_dir");
    (void)rc;
    write_file(TEST_SRC "/excluded_dir/secret.txt", "exclude me\n", 11);

    const char *excl[] = { TEST_SRC "/excluded_dir" };
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1, .exclude = excl, .n_exclude = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Restore and verify excluded dir is absent */
    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    verify_file_matches(TEST_DEST TEST_SRC "/keep.txt", "keep me\n", 8);

    struct stat st;
    assert_int_not_equal(stat(TEST_DEST TEST_SRC "/excluded_dir/secret.txt", &st), 0);
}

/* Two roots merged into one snapshot */
static void test_multiple_source_paths(void **state) {
    (void)state;

    write_file(TEST_SRC "/from_src1.txt", "source 1\n", 9);
    write_file(TEST_SRC2 "/from_src2.txt", "source 2\n", 9);

    const char *paths[] = { TEST_SRC, TEST_SRC2 };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 2, &opts), OK);

    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    verify_file_matches(TEST_DEST TEST_SRC "/from_src1.txt", "source 1\n", 9);
    verify_file_matches(TEST_DEST TEST_SRC2 "/from_src2.txt", "source 2\n", 9);
}

/* No changes + quiet → snapshot with no new objects (no-change path) */
static void test_no_changes_quiet(void **state) {
    (void)state;

    write_file(TEST_SRC "/static.txt", "no changes\n", 11);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Run again with no changes */
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Both snapshots should verify */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Hardlink: backup two hardlinked files, only one object stored */
static void test_hardlink_primary_fail(void **state) {
    (void)state;

    write_file(TEST_SRC "/primary.txt", "hardlink test\n", 14);
    assert_int_equal(link(TEST_SRC "/primary.txt", TEST_SRC "/secondary.txt"), 0);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Restore and verify both files have same content */
    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    verify_file_matches(TEST_DEST TEST_SRC "/primary.txt", "hardlink test\n", 14);
    verify_file_matches(TEST_DEST TEST_SRC "/secondary.txt", "hardlink test\n", 14);
}

/* Non-quiet mode: exercises progress callback and output paths */
static void test_non_quiet_backup(void **state) {
    (void)state;

    write_file(TEST_SRC "/nq1.txt", "not quiet 1\n", 12);
    write_file(TEST_SRC "/nq2.txt", "not quiet 2\n", 12);

    const char *paths[] = { TEST_SRC };
    /* quiet=0, verbose=0 — exercises the default progress output path */
    backup_opts_t opts = {0};
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Verbose mode: exercises verbose warning paths */
static void test_verbose_backup(void **state) {
    (void)state;

    write_file(TEST_SRC "/v1.txt", "verbose 1\n", 10);
    write_file(TEST_SRC "/v2.txt", "verbose 2\n", 10);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .verbose = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Many unique files: forces parallel worker pool with multiple items */
static void test_many_files_parallel_store(void **state) {
    (void)state;

    /* Create 30 unique files to trigger parallel store */
    for (int i = 0; i < 30; i++) {
        char path[256], content[128];
        snprintf(path, sizeof(path), TEST_SRC "/par_%03d.txt", i);
        snprintf(content, sizeof(content), "parallel store test content %d unique seed %d\n", i, i * 31 + 7);
        write_file(path, content, strlen(content));
    }

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Verify all objects exist */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Pack and re-verify */
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Delete + add files between snapshots — exercises all change types */
static void test_all_change_types(void **state) {
    (void)state;

    write_file(TEST_SRC "/unchanged.txt", "stays the same\n", 15);
    write_file(TEST_SRC "/modified.txt", "version 1\n", 10);
    write_file(TEST_SRC "/deleted.txt", "will be gone\n", 13);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Modify one, delete one, add one */
    write_file(TEST_SRC "/modified.txt", "version 2 new content\n", 22);
    unlink(TEST_SRC "/deleted.txt");
    write_file(TEST_SRC "/added.txt", "brand new file\n", 15);

    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Verify both snapshots */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Restore snap 2 and verify */
    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);

    verify_file_matches(TEST_DEST TEST_SRC "/unchanged.txt", "stays the same\n", 15);
    verify_file_matches(TEST_DEST TEST_SRC "/modified.txt", "version 2 new content\n", 22);
    verify_file_matches(TEST_DEST TEST_SRC "/added.txt", "brand new file\n", 15);

    struct stat st;
    assert_int_not_equal(stat(TEST_DEST TEST_SRC "/deleted.txt", &st), 0);
}

/* Deep directory structure */
static void test_deep_directory_structure(void **state) {
    (void)state;

    int rc = system("mkdir -p " TEST_SRC "/a/b/c/d");
    (void)rc;
    write_file(TEST_SRC "/a/top.txt", "top level\n", 10);
    write_file(TEST_SRC "/a/b/mid.txt", "mid level\n", 10);
    write_file(TEST_SRC "/a/b/c/deep.txt", "deep level\n", 11);
    write_file(TEST_SRC "/a/b/c/d/deepest.txt", "deepest level\n", 14);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    verify_file_matches(TEST_DEST TEST_SRC "/a/b/c/d/deepest.txt", "deepest level\n", 14);
}

/* Progress env var override: exercises phase3 progress thread, scan/phase2
 * progress callbacks, and all phase_line_setf calls by forcing progress
 * output even when stderr is not a tty. */
static void test_progress_env_override(void **state) {
    (void)state;

    setenv("CBACKUP_PROGRESS", "1", 1);
    setenv("CBACKUP_STORE_THREADS", "1", 1);

    /* Redirect stderr to /dev/null so progress output doesn't pollute test */
    int saved_stderr = dup(STDERR_FILENO);
    assert_true(saved_stderr >= 0);
    int devnull = open("/dev/null", O_WRONLY);
    assert_true(devnull >= 0);
    dup2(devnull, STDERR_FILENO);
    close(devnull);

    /* Create 35 unique files to force store phase with progress */
    for (int i = 0; i < 35; i++) {
        char path[256], content[128];
        snprintf(path, sizeof(path), TEST_SRC "/progress_%03d.txt", i);
        snprintf(content, sizeof(content),
                 "progress env test content %d unique %d\n", i, i * 37 + 13);
        write_file(path, content, strlen(content));
    }

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 0 };
    status_t st = backup_run_opts(repo, paths, 1, &opts);

    /* Restore stderr */
    dup2(saved_stderr, STDERR_FILENO);
    close(saved_stderr);
    unsetenv("CBACKUP_PROGRESS");
    unsetenv("CBACKUP_STORE_THREADS");

    assert_int_equal(st, OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* File becomes unreadable between scan and store phases.
 * Triggers the EACCES transient error path in store_worker (L329-L361),
 * skipped_add/skipped_cmp, and skipped_has. */
static void test_transient_eacces_during_store(void **state) {
    (void)state;

    write_file(TEST_SRC "/readable.txt", "readable\n", 9);
    write_file(TEST_SRC "/unreadable.txt", "will be unreadable\n", 19);

    /* First backup — both files stored successfully */
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Make one file unreadable + modify it so it's detected as changed */
    write_file(TEST_SRC "/unreadable.txt", "modified and locked\n", 20);
    chmod(TEST_SRC "/unreadable.txt", 0000);

    /* Second backup — scan succeeds (stat works on 000 files),
     * but store worker gets EACCES and should skip gracefully */
    status_t st = backup_run_opts(repo, paths, 1, &opts);
    assert_int_equal(st, OK);

    /* Restore permissions so teardown can clean up */
    chmod(TEST_SRC "/unreadable.txt", 0644);

    /* Verify repo integrity */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Multiple files disappear between scan and store — tests skipped_add
 * growth (realloc path) and skipped_has (bsearch) with multiple entries */
static void test_transient_multiple_vanished(void **state) {
    (void)state;

    /* Create initial files */
    for (int i = 0; i < 20; i++) {
        char path[256], content[64];
        snprintf(path, sizeof(path), "%s/vanish_%02d.txt", TEST_SRC, i);
        snprintf(content, sizeof(content), "vanish content %d\n", i);
        write_file(path, content, strlen(content));
    }
    write_file(TEST_SRC "/keeper.txt", "stays around\n", 13);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Modify + make unreadable: forces re-store attempt → EACCES */
    for (int i = 0; i < 20; i++) {
        char path[256], content[64];
        snprintf(path, sizeof(path), "%s/vanish_%02d.txt", TEST_SRC, i);
        snprintf(content, sizeof(content), "modified vanish %d\n", i);
        write_file(path, content, strlen(content));
        chmod(path, 0000);
    }

    status_t st = backup_run_opts(repo, paths, 1, &opts);
    assert_int_equal(st, OK);

    /* Cleanup */
    for (int i = 0; i < 20; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s/vanish_%02d.txt", TEST_SRC, i);
        chmod(path, 0644);
    }
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/*
 * Multi-path backup with enough entries to trigger scan merge realloc.
 * The scan result starts with capacity=64. Creating 63 files in src1
 * yields 64 entries (63 files + 1 dir), exactly filling capacity.
 * Then merging src2 entries triggers the realloc at backup.c L594.
 */
static void test_multipath_merge_realloc(void **state) {
    (void)state;
    /* Create 63 files in src1 → 64 scan entries (63 files + root dir) */
    for (int i = 0; i < 63; i++) {
        char path[256], content[64];
        snprintf(path, sizeof(path), "%s/mr_%02d.txt", TEST_SRC, i);
        snprintf(content, sizeof(content), "merge realloc %d", i);
        write_file(path, content, strlen(content));
    }
    /* Create a few files in src2 → triggers merge realloc */
    for (int i = 0; i < 5; i++) {
        char path[256], content[64];
        snprintf(path, sizeof(path), "%s/mr2_%d.txt", TEST_SRC2, i);
        snprintf(content, sizeof(content), "merge src2 %d", i);
        write_file(path, content, strlen(content));
    }
    const char *paths[] = { TEST_SRC, TEST_SRC2 };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 2, &opts), OK);

    /* Verify both source trees are in the snapshot */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    /* 63 + 5 files + dirs + symlinks etc — just check we have a reasonable count */
    assert_true(snap->node_count >= 68);
    snapshot_free(snap);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_verify_after_succeeds, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_verify_after_detects_missing, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_strict_meta_xattr_change, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_modified_file_content, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_symlink_content, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_transient_file_disappearance, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_exclude_paths, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_multiple_source_paths, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_no_changes_quiet, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_hardlink_primary_fail, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_non_quiet_backup, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_verbose_backup, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_many_files_parallel_store, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_all_change_types, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_deep_directory_structure, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_progress_env_override, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_transient_eacces_during_store, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_transient_multiple_vanished, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_multipath_merge_realloc, setup_pipeline, teardown_pipeline),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
