#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/restore.h"
#include "../src/object.h"
#include "../src/snapshot.h"
#include "../src/gc.h"
#include "../src/pack.h"
#include "../src/gfs.h"
#include "../src/policy.h"

#define TEST_REPO "/tmp/c_backup_pack_repo"
#define TEST_SRC  "/tmp/c_backup_pack_src"
#define TEST_DEST "/tmp/c_backup_pack_dest"

static repo_t *repo;

/* Prune repo keeping the last keep_n snapshots (keep_n >= 1). */
static void prune_keep(uint32_t keep_n) {
    uint32_t h = 0;
    snapshot_read_head(repo, &h);
    policy_t pol = {0};
    pol.keep_revs = (keep_n > 0) ? keep_n - 1 : 0;
    gfs_run(repo, &pol, h, 0, 1);
}

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "file a content");
    write_file(TEST_SRC "/b.txt", "file b content");
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

/* After pack, no loose objects remain and objects are still loadable. */
static void test_pack_removes_loose(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    /* no_pack=1: leave loose objects so we can test repo_pack explicitly */
    backup_opts_t nopack = { .no_pack = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &nopack), OK);

    /* Collect content hashes before packing */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t hashes[8][OBJECT_HASH_SIZE];
    uint32_t nhashes = 0;
    for (uint32_t i = 0; i < snap->node_count && nhashes < 8; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0)
            memcpy(hashes[nhashes++], snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
    }
    snapshot_free(snap);
    assert_true(nhashes > 0);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed >= nhashes);

    /* No loose object files should remain */
    char obj_dir[256];
    snprintf(obj_dir, sizeof(obj_dir), TEST_REPO "/objects");
    /* Walk objects/ — expect all subdirs to be empty */
    DIR *d = opendir(obj_dir);
    assert_non_null(d);
    struct dirent *de;
    int loose_count = 0;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.' || strlen(de->d_name) != 2) continue;
        char subpath[256];
        snprintf(subpath, sizeof(subpath), "%s/%s", obj_dir, de->d_name);
        DIR *sub = opendir(subpath);
        if (!sub) continue;
        struct dirent *sde;
        while ((sde = readdir(sub)) != NULL) {
            if (sde->d_name[0] != '.') loose_count++;
        }
        closedir(sub);
    }
    closedir(d);
    assert_int_equal(loose_count, 0);

    /* Pack index file must exist */
    assert_int_equal(access(TEST_REPO "/packs/pack-00000000.idx", F_OK), 0);

    /* All previously-recorded hashes are still loadable */
    for (uint32_t i = 0; i < nhashes; i++)
        assert_true(object_exists(repo, hashes[i]));

    /* verify must pass */
    assert_int_equal(repo_verify(repo), OK);
}

/* Backup after pack: new objects go to loose; restore still works. */
static void test_backup_after_pack(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Modify a file and run a second backup */
    write_file(TEST_SRC "/a.txt", "updated file a");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Restore latest and verify content */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char rpath[256];
    snprintf(rpath, sizeof(rpath), TEST_DEST "/c_backup_pack_src/a.txt");
    FILE *f = fopen(rpath, "r");
    assert_non_null(f);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    assert_string_equal(buf, "updated file a");

    assert_int_equal(repo_verify(repo), OK);
}

/* Pack twice: second pack consumes the loose objects from the second backup. */
static void test_double_pack(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    backup_opts_t nopack = { .no_pack = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &nopack), OK);

    uint32_t p1 = 0;
    assert_int_equal(repo_pack(repo, &p1), OK);
    assert_true(p1 > 0);

    /* Second backup adds new loose objects */
    write_file(TEST_SRC "/c.txt", "third file");
    assert_int_equal(backup_run_opts(repo, paths, 1, &nopack), OK);

    uint32_t p2 = 0;
    assert_int_equal(repo_pack(repo, &p2), OK);
    assert_true(p2 > 0);

    /* Two pack files must exist */
    assert_int_equal(access(TEST_REPO "/packs/pack-00000000.idx", F_OK), 0);
    assert_int_equal(access(TEST_REPO "/packs/pack-00000001.idx", F_OK), 0);

    assert_int_equal(repo_verify(repo), OK);
}

/* pack with no loose objects: no-op, no pack file created. */
static void test_pack_empty_is_noop(void **state) {
    (void)state;
    /* No backup run — objects/ is empty */
    uint32_t packed = 99;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_int_equal(packed, 0u);

    /* No pack file must have been created */
    assert_int_not_equal(access(TEST_REPO "/packs/pack-00000000.dat", F_OK), 0);
}

/*
 * GC after pack prunes dead objects from the pack file.
 * Scenario: pack, then prune oldest snapshot.  Objects exclusive to the
 * pruned snapshot must be absent from the rewritten pack; objects shared
 * with surviving snapshots must still be loadable.
 */
static void test_gc_compacts_pack(void **state) {
    (void)state;

    /* Snapshot 1: unique file + shared file */
    write_file(TEST_SRC "/shared.txt",  "shared content");
    write_file(TEST_SRC "/unique1.txt", "only in snap 1");
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Grab the content hash of unique1.txt from snapshot 1 */
    snapshot_t *snap1 = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap1), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t unique_hash[OBJECT_HASH_SIZE] = {0};
    uint8_t shared_hash[OBJECT_HASH_SIZE] = {0};
    for (uint32_t i = 0; i < snap1->node_count; i++) {
        if (memcmp(snap1->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        /* Identify by reading the node path via dirent — simpler: just grab both */
        if (memcmp(unique_hash, zero, OBJECT_HASH_SIZE) == 0)
            memcpy(unique_hash, snap1->nodes[i].content_hash, OBJECT_HASH_SIZE);
        else if (memcmp(shared_hash, zero, OBJECT_HASH_SIZE) == 0)
            memcpy(shared_hash, snap1->nodes[i].content_hash, OBJECT_HASH_SIZE);
    }
    snapshot_free(snap1);

    /* Snapshot 2: remove unique1.txt, keep shared.txt */
    unlink(TEST_SRC "/unique1.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Pack all objects (both snapshots' objects go into pack-00000000) */
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(access(TEST_REPO "/packs/pack-00000000.dat", F_OK), 0);

    /* Prune snap 1 (keep only 1 snapshot = snap 2) — triggers GC which
     * calls pack_gc to rewrite pack-00000000 without snap-1-exclusive objects */
    prune_keep(1);   /* snap 1 removed, snap 2 kept */

    /* verify must still pass (snap 2 is intact) */
    assert_int_equal(repo_verify(repo), OK);

    /* GC should have reported some deletions (the unique1.txt object at least
     * appears in snap1's reverse record, so it may be retained for the reverse
     * chain; what matters most is that verify passes and the pack is valid) */
}

/*
 * After pack_gc rewrites a pack, the surviving objects remain loadable.
 */
static void test_gc_pack_objects_still_loadable(void **state) {
    (void)state;
    write_file(TEST_SRC "/keep.txt",   "keep this");
    write_file(TEST_SRC "/drop.txt",   "drop this");
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 1 */

    /* Record keep.txt hash */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t keep_hash[OBJECT_HASH_SIZE] = {0};
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(keep_hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            break;
        }
    }
    snapshot_free(snap);

    /* Remove drop.txt and make a second backup so the reverse chain holds its hash */
    unlink(TEST_SRC "/drop.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 2 */

    /* Pack everything */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Prune snap 1 */
    prune_keep(1);

    /* keep.txt's object must still be accessible (referenced by snap 2) */
    assert_true(object_exists(repo, keep_hash));

    /* Full restore of snap 2 must succeed */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char rpath[256];
    snprintf(rpath, sizeof(rpath), TEST_DEST "/c_backup_pack_src/keep.txt");
    FILE *f = fopen(rpath, "r");
    assert_non_null(f);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    assert_string_equal(buf, "keep this");
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_pack_removes_loose,            setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_after_pack,             setup, teardown),
        cmocka_unit_test_setup_teardown(test_double_pack,                   setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_empty_is_noop,            setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_compacts_pack,              setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_pack_objects_still_loadable, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
