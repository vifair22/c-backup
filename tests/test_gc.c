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
#include "../src/object.h"
#include "../src/snapshot.h"
#include "../src/restore.h"
#include "../src/gc.h"

#define TEST_REPO "/tmp/c_backup_gc_repo"
#define TEST_SRC  "/tmp/c_backup_gc_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC);
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
    system("rm -rf " TEST_REPO " " TEST_SRC);
    return 0;
}

/* GC keeps all objects that are referenced by a snapshot. */
static void test_gc_keeps_referenced(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Collect content hashes from snapshot 1 */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);

    uint8_t hashes[16][OBJECT_HASH_SIZE];
    uint32_t nhashes = 0;
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    for (uint32_t i = 0; i < snap->node_count && nhashes < 16; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0)
            memcpy(hashes[nhashes++], snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
    }
    snapshot_free(snap);
    assert_true(nhashes > 0);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    /* Nothing should be deleted — everything is referenced */
    assert_int_equal(deleted, 0u);
    assert_true(kept >= nhashes);

    /* All previously-recorded hashes still exist */
    for (uint32_t i = 0; i < nhashes; i++)
        assert_true(object_exists(repo, hashes[i]));

    /* Verify confirms integrity */
    assert_int_equal(repo_verify(repo), OK);
}

/* GC deletes objects that are not referenced by any snapshot. */
static void test_gc_removes_orphans(void **state) {
    (void)state;
    /* Store an orphan object before any backup */
    const char *orphan_data = "i am an orphaned object that no snapshot references";
    uint8_t orphan_hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE,
                                  orphan_data, strlen(orphan_data),
                                  orphan_hash), OK);
    assert_true(object_exists(repo, orphan_hash));

    /* Run a backup — snapshot does not reference the orphan.
     * no_pack=1: skip auto-pack so the orphan is still a loose object
     * when we call repo_gc explicitly below. */
    const char *paths[] = { TEST_SRC };
    backup_opts_t nopack = { .no_pack = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &nopack), OK);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    /* Orphan should be gone */
    assert_int_equal(deleted, 1u);
    assert_false(object_exists(repo, orphan_hash));

    /* Backup objects must survive and verify must pass */
    assert_int_equal(repo_verify(repo), OK);
}

/* verify detects a missing (manually deleted) object. */
static void test_verify_detects_corruption(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    /* no_pack=1: keep objects loose so we can delete one by path below */
    backup_opts_t nopack = { .no_pack = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &nopack), OK);

    /* Find a content hash from the snapshot */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t victim[OBJECT_HASH_SIZE] = {0};
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(victim, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            break;
        }
    }
    snapshot_free(snap);

    /* Manually delete the object file */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(victim, hex);
    char obj_path[256];
    snprintf(obj_path, sizeof(obj_path),
             TEST_REPO "/objects/%.2s/%s", hex, hex + 2);
    assert_int_equal(unlink(obj_path), 0);

    /* verify should now report an error */
    assert_int_not_equal(repo_verify(repo), OK);
}

/* prune removes old snapshot files but keeps reverse records intact. */
static void test_prune_removes_old_snapshots(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    /* Three backups with distinct content so each produces a new snapshot */
    assert_int_equal(backup_run(repo, paths, 1), OK);
    write_file(TEST_SRC "/a.txt", "file a v2");
    assert_int_equal(backup_run(repo, paths, 1), OK);
    write_file(TEST_SRC "/a.txt", "file a v3");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t pruned = 0;
    assert_int_equal(repo_prune(repo, 2, &pruned, 0), OK);
    assert_int_equal(pruned, 1u);   /* snap 1 removed, 2 and 3 kept */

    /* Snapshot 1 file must be gone */
    char snap1[256];
    snprintf(snap1, sizeof(snap1), TEST_REPO "/snapshots/00000001.snap");
    assert_int_not_equal(access(snap1, F_OK), 0);

    /* Snapshots 2 and 3 must still be readable */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    snapshot_free(snap);
    assert_int_equal(snapshot_load(repo, 3, &snap), OK);
    snapshot_free(snap);

    /* Verify must pass (only surviving snapshots checked) */
    assert_int_equal(repo_verify(repo), OK);
}

/* After pruning, restore-at reconstructs a pruned snapshot via the reverse chain. */
static void test_prune_then_restore_at(void **state) {
    (void)state;

    /* Write distinct content for each backup */
    write_file(TEST_SRC "/msg.txt", "version one");
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 1 */

    write_file(TEST_SRC "/msg.txt", "version two");
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 2 */

    write_file(TEST_SRC "/msg.txt", "version three");
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snap 3 */

    /* Prune: keep only the latest 2 (removes snap 1) */
    uint32_t pruned = 0;
    assert_int_equal(repo_prune(repo, 2, &pruned, 0), OK);
    assert_int_equal(pruned, 1u);

    /* Restore snap 1 via reverse chain into a temp dest */
    char dest[256];
    snprintf(dest, sizeof(dest), "%s_restore_at", TEST_REPO);
    system("rm -rf " TEST_REPO "_restore_at");
    mkdir(dest, 0755);

    assert_int_equal(restore_snapshot_at(repo, 1, dest), OK);

    /* Verify the restored content matches the original snap 1 content */
    char rpath[256];
    snprintf(rpath, sizeof(rpath), "%s/c_backup_gc_src/msg.txt", dest);
    FILE *f = fopen(rpath, "r");
    assert_non_null(f);
    char buf[64] = {0};
    fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    assert_string_equal(buf, "version one");

    system("rm -rf " TEST_REPO "_restore_at");
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gc_keeps_referenced,       setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_removes_orphans,        setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_detects_corruption, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_removes_old_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_then_restore_at,     setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
