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
#include "../src/restore.h"
#include "../src/snapshot.h"
#include "../src/gc.h"
#include "../src/pack.h"
#include "../src/ls.h"

#define TEST_REPO "/tmp/c_backup_depth_repo"
#define TEST_SRC  "/tmp/c_backup_depth_src"
#define TEST_DEST "/tmp/c_backup_depth_dest"

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *read_file_str(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f); rewind(f);
    if (sz <= 0) { fclose(f); return strdup(""); }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t got = fread(buf, 1, (size_t)sz, f);
    buf[got] = '\0';
    fclose(f);
    return buf;
}

/* mkdir -p equivalent (one level at a time) */
static void mkdirs(const char *path) {
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    mkdir(TEST_SRC, 0755);
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

/* ------------------------------------------------------------------ */
/* Directory depth tests                                               */
/* ------------------------------------------------------------------ */

/*
 * Back up a 10-level deep directory tree, restore it, and verify the
 * deepest file content is intact.
 */
static void test_deep_directory_tree(void **state) {
    (void)state;

    /* Build: src/a/b/c/d/e/f/g/h/i/deep.txt */
    char deep_dir[512];
    snprintf(deep_dir, sizeof(deep_dir),
             TEST_SRC "/a/b/c/d/e/f/g/h/i");
    mkdirs(deep_dir);

    char deep_file[512];
    snprintf(deep_file, sizeof(deep_file), "%s/deep.txt", deep_dir);
    write_file(deep_file, "i am very deep");

    /* Also place files at intermediate levels */
    write_file(TEST_SRC "/a/level1.txt",           "level 1");
    write_file(TEST_SRC "/a/b/level2.txt",         "level 2");
    write_file(TEST_SRC "/a/b/c/level3.txt",       "level 3");
    write_file(TEST_SRC "/a/b/c/d/e/level5.txt",   "level 5");

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Restore and verify deepest file */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char rpath[512];
    snprintf(rpath, sizeof(rpath),
             TEST_DEST "/c_backup_depth_src/a/b/c/d/e/f/g/h/i/deep.txt");
    char *content = read_file_str(rpath);
    assert_non_null(content);
    assert_string_equal(content, "i am very deep");
    free(content);

    /* Verify intermediate files too */
    snprintf(rpath, sizeof(rpath),
             TEST_DEST "/c_backup_depth_src/a/b/c/level3.txt");
    content = read_file_str(rpath);
    assert_non_null(content);
    assert_string_equal(content, "level 3");
    free(content);

    assert_int_equal(repo_verify(repo), OK);
}

/*
 * Deep tree with multiple files at every level — backup, modify one
 * deep file, second backup, verify both snapshots restore correctly.
 */
static void test_deep_tree_incremental(void **state) {
    (void)state;

    /* Initial tree: 6 levels, two files per level */
    char dirs[6][512];
    snprintf(dirs[0], sizeof(dirs[0]), TEST_SRC "/l1");
    snprintf(dirs[1], sizeof(dirs[1]), TEST_SRC "/l1/l2");
    snprintf(dirs[2], sizeof(dirs[2]), TEST_SRC "/l1/l2/l3");
    snprintf(dirs[3], sizeof(dirs[3]), TEST_SRC "/l1/l2/l3/l4");
    snprintf(dirs[4], sizeof(dirs[4]), TEST_SRC "/l1/l2/l3/l4/l5");
    snprintf(dirs[5], sizeof(dirs[5]), TEST_SRC "/l1/l2/l3/l4/l5/l6");
    for (int i = 0; i < 6; i++) mkdir(dirs[i], 0755);

    for (int i = 0; i < 6; i++) {
        char p[512];
        snprintf(p, sizeof(p), "%s/file_a.txt", dirs[i]);
        write_file(p, "original a");
        snprintf(p, sizeof(p), "%s/file_b.txt", dirs[i]);
        write_file(p, "original b");
    }

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 1 */

    /* Modify the deepest file_a.txt */
    char deep_a[512];
    snprintf(deep_a, sizeof(deep_a),
             TEST_SRC "/l1/l2/l3/l4/l5/l6/file_a.txt");
    write_file(deep_a, "modified a");
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 2 */

    /* Restore snap 1 — deep file_a must still say "original a" */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot_at(repo, 1, TEST_DEST), OK);

    char rpath[512];
    snprintf(rpath, sizeof(rpath),
             TEST_DEST "/c_backup_depth_src/l1/l2/l3/l4/l5/l6/file_a.txt");
    char *content = read_file_str(rpath);
    assert_non_null(content);
    assert_string_equal(content, "original a");
    free(content);

    /* Restore snap 2 — deep file_a must say "modified a" */
    system("rm -rf " TEST_DEST);
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);
    content = read_file_str(rpath);
    assert_non_null(content);
    assert_string_equal(content, "modified a");
    free(content);

    assert_int_equal(repo_verify(repo), OK);
}

/*
 * ls navigates a deep tree correctly.
 */
static void test_ls_deep_tree(void **state) {
    (void)state;

    mkdirs(TEST_SRC "/x/y/z");
    write_file(TEST_SRC "/x/y/z/leaf.txt", "leaf content");

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* ls at each depth must succeed */
    assert_int_equal(snapshot_ls(repo, 1, ""), OK);
    assert_int_equal(snapshot_ls(repo, 1, "c_backup_depth_src"), OK);
    assert_int_equal(snapshot_ls(repo, 1, "c_backup_depth_src/x"), OK);
    assert_int_equal(snapshot_ls(repo, 1, "c_backup_depth_src/x/y"), OK);
    assert_int_equal(snapshot_ls(repo, 1, "c_backup_depth_src/x/y/z"), OK);

    /* ls on a non-existent path must fail */
    assert_int_not_equal(snapshot_ls(repo, 1, "c_backup_depth_src/x/nope"), OK);
}

/* ------------------------------------------------------------------ */
/* Snapshot depth tests                                                */
/* ------------------------------------------------------------------ */

#define N_SNAPS 20

/*
 * Create N_SNAPS snapshots, each writing a unique content to a tracked
 * file.  Verify that restore_snapshot_at correctly returns the content
 * from every individual snapshot.
 */
static void test_many_snapshots_restore(void **state) {
    (void)state;

    write_file(TEST_SRC "/tracked.txt", "snap 0 content");   /* initial */
    const char *paths[] = { TEST_SRC };

    for (int i = 1; i <= N_SNAPS; i++) {
        char content[64];
        snprintf(content, sizeof(content), "snap %d content", i);
        write_file(TEST_SRC "/tracked.txt", content);
        assert_int_equal(backup_run(repo, paths, 1), OK);
    }

    /* Verify every snapshot restores the correct content */
    for (int i = 1; i <= N_SNAPS; i++) {
        system("rm -rf " TEST_DEST);
        mkdir(TEST_DEST, 0755);
        assert_int_equal(restore_snapshot_at(repo, (uint32_t)i, TEST_DEST), OK);

        char rpath[512];
        snprintf(rpath, sizeof(rpath),
                 TEST_DEST "/c_backup_depth_src/tracked.txt");
        char *got = read_file_str(rpath);
        assert_non_null(got);

        char expected[64];
        snprintf(expected, sizeof(expected), "snap %d content", i);
        assert_string_equal(got, expected);
        free(got);
    }

    assert_int_equal(repo_verify(repo), OK);
}

/*
 * Create N_SNAPS snapshots, prune to keep only the last 5, then verify
 * that restore-at via the reverse chain still recovers every pruned snapshot.
 */
static void test_many_snapshots_prune_restore_at(void **state) {
    (void)state;

    const char *paths[] = { TEST_SRC };
    for (int i = 1; i <= N_SNAPS; i++) {
        char content[64];
        snprintf(content, sizeof(content), "snap %d content", i);
        write_file(TEST_SRC "/tracked.txt", content);
        assert_int_equal(backup_run(repo, paths, 1), OK);
    }

    uint32_t pruned = 0;
    assert_int_equal(repo_prune(repo, 5, &pruned, 0), OK);
    assert_int_equal(pruned, (uint32_t)(N_SNAPS - 5));

    assert_int_equal(repo_verify(repo), OK);

    /* All snapshots — including pruned ones — must be restorable via
     * the reverse chain */
    for (int i = 1; i <= N_SNAPS; i++) {
        system("rm -rf " TEST_DEST);
        mkdir(TEST_DEST, 0755);
        assert_int_equal(restore_snapshot_at(repo, (uint32_t)i, TEST_DEST), OK);

        char rpath[512];
        snprintf(rpath, sizeof(rpath),
                 TEST_DEST "/c_backup_depth_src/tracked.txt");
        char *got = read_file_str(rpath);
        assert_non_null(got);

        char expected[64];
        snprintf(expected, sizeof(expected), "snap %d content", i);
        assert_string_equal(got, expected);
        free(got);
    }
}

/*
 * Full lifecycle stress: N_SNAPS backups across a small mutating tree,
 * pack after every 5 snapshots, prune to keep 5 at the end, verify.
 */
static void test_snapshot_depth_full_lifecycle(void **state) {
    (void)state;

    mkdirs(TEST_SRC "/a/b");
    write_file(TEST_SRC "/root.txt",  "root");
    write_file(TEST_SRC "/a/mid.txt", "mid");
    write_file(TEST_SRC "/a/b/deep.txt", "deep");

    const char *paths[] = { TEST_SRC };

    for (int i = 1; i <= N_SNAPS; i++) {
        /* Rotate the content of one of the three files each round */
        char buf[64];
        if      (i % 3 == 1) { snprintf(buf, sizeof(buf), "root v%d",  i); write_file(TEST_SRC "/root.txt",  buf); }
        else if (i % 3 == 2) { snprintf(buf, sizeof(buf), "mid v%d",   i); write_file(TEST_SRC "/a/mid.txt", buf); }
        else                  { snprintf(buf, sizeof(buf), "deep v%d",  i); write_file(TEST_SRC "/a/b/deep.txt", buf); }

        assert_int_equal(backup_run(repo, paths, 1), OK);

        /* Pack every 5 snapshots */
        if (i % 5 == 0)
            assert_int_equal(repo_pack(repo, NULL), OK);
    }

    /* Prune, keeping last 5 */
    assert_int_equal(repo_prune(repo, 5, NULL, 0), OK);
    assert_int_equal(repo_verify(repo), OK);

    /* Latest restore must work */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    /* Historical restore of snap 1 via reverse chain must also work */
    system("rm -rf " TEST_DEST);
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot_at(repo, 1, TEST_DEST), OK);

    char rpath[512];
    snprintf(rpath, sizeof(rpath),
             TEST_DEST "/c_backup_depth_src/root.txt");
    char *got = read_file_str(rpath);
    assert_non_null(got);
    assert_string_equal(got, "root v1");
    free(got);
}

/* ------------------------------------------------------------------ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_deep_directory_tree,           setup, teardown),
        cmocka_unit_test_setup_teardown(test_deep_tree_incremental,         setup, teardown),
        cmocka_unit_test_setup_teardown(test_ls_deep_tree,                  setup, teardown),
        cmocka_unit_test_setup_teardown(test_many_snapshots_restore,        setup, teardown),
        cmocka_unit_test_setup_teardown(test_many_snapshots_prune_restore_at, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_depth_full_lifecycle, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
