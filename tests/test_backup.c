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

#define TEST_REPO "/tmp/c_backup_bkup_test_repo"
#define TEST_SRC  "/tmp/c_backup_bkup_test_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static void write_bytes(const char *path, const void *buf, size_t len) {
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    if (len > 0) assert_int_equal(fwrite(buf, 1, len, f), len);
    fclose(f);
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "hello");
    write_file(TEST_SRC "/b.txt", "world");
    mkdir(TEST_SRC "/sub", 0755);
    write_file(TEST_SRC "/sub/c.txt", "nested");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

static void test_first_backup_creates_manifest(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);
    assert_true(snap->node_count >= 4);
    snapshot_free(snap);
}

static void test_incremental_backup_advances_head(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    sleep(1);
    write_file(TEST_SRC "/a.txt", "hello modified");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 2u);
}

static void test_unchanged_backup_is_skipped(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);
}

static void test_deleted_file_not_in_next_manifest(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);
    unlink(TEST_SRC "/b.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    pathmap_t *pm = NULL;
    assert_int_equal(pathmap_build(snap, &pm), OK);
    snapshot_free(snap);

    assert_null(pathmap_lookup(pm, "tmp/c_backup_bkup_test_src/b.txt"));
    pathmap_free(pm);
}

static void test_metadata_only_change_keeps_content_hash(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);

    snapshot_t *snap1 = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap1), OK);
    pathmap_t *pm1 = NULL;
    assert_int_equal(pathmap_build(snap1, &pm1), OK);
    const node_t *old = pathmap_lookup(pm1, "tmp/c_backup_bkup_test_src/a.txt");
    assert_non_null(old);
    uint8_t old_hash[OBJECT_HASH_SIZE];
    memcpy(old_hash, old->content_hash, OBJECT_HASH_SIZE);
    uint32_t old_mode = old->mode;
    pathmap_free(pm1);
    snapshot_free(snap1);

    assert_int_equal(chmod(TEST_SRC "/a.txt", 0600), 0);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    snapshot_t *snap2 = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap2), OK);
    pathmap_t *pm2 = NULL;
    assert_int_equal(pathmap_build(snap2, &pm2), OK);
    const node_t *now = pathmap_lookup(pm2, "tmp/c_backup_bkup_test_src/a.txt");
    assert_non_null(now);
    assert_memory_equal(now->content_hash, old_hash, OBJECT_HASH_SIZE);
    assert_true(now->mode != old_mode);
    pathmap_free(pm2);
    snapshot_free(snap2);
}

static void test_hardlink_paths_share_same_node_id(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(link(TEST_SRC "/a.txt", TEST_SRC "/a-link.txt"), 0);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    pathmap_t *pm = NULL;
    assert_int_equal(pathmap_build(snap, &pm), OK);

    const node_t *a = pathmap_lookup(pm, "tmp/c_backup_bkup_test_src/a.txt");
    const node_t *alink = pathmap_lookup(pm, "tmp/c_backup_bkup_test_src/a-link.txt");
    assert_non_null(a);
    assert_non_null(alink);
    assert_int_equal(a->node_id, alink->node_id);
    assert_memory_equal(a->content_hash, alink->content_hash, OBJECT_HASH_SIZE);

    pathmap_free(pm);
    snapshot_free(snap);
}

static void test_corrupt_previous_snapshot_still_allows_new_backup(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);

    const unsigned char bad_magic[4] = {0, 0, 0, 0};
    write_bytes(TEST_REPO "/snapshots/00000001.snap", bad_magic, sizeof(bad_magic));

    write_file(TEST_SRC "/a.txt", "hello again");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 2u);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    snapshot_free(snap);
}

static void test_backup_opts_empty_source_list_is_invalid(void **state) {
    (void)state;
    backup_opts_t opts = {0};
    assert_int_equal(backup_run_opts(repo, NULL, 0, &opts), ERR_INVALID);
}

static void test_backup_opts_null_source_array_with_positive_count_is_invalid(void **state) {
    (void)state;
    backup_opts_t opts = {0};
    assert_int_equal(backup_run_opts(repo, NULL, 1, &opts), ERR_INVALID);
}

static void test_backup_opts_null_repo_is_invalid(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = {0};
    assert_int_equal(backup_run_opts(NULL, paths, 1, &opts), ERR_INVALID);
}

static void test_strict_meta_no_change_still_skips_snapshot(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };

    assert_int_equal(backup_run(repo, paths, 1), OK);

    backup_opts_t opts = {0};
    opts.strict_meta = 1;
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_first_backup_creates_manifest, setup, teardown),
        cmocka_unit_test_setup_teardown(test_incremental_backup_advances_head, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unchanged_backup_is_skipped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleted_file_not_in_next_manifest, setup, teardown),
        cmocka_unit_test_setup_teardown(test_metadata_only_change_keeps_content_hash, setup, teardown),
        cmocka_unit_test_setup_teardown(test_hardlink_paths_share_same_node_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_corrupt_previous_snapshot_still_allows_new_backup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_opts_empty_source_list_is_invalid, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_opts_null_source_array_with_positive_count_is_invalid, setup, teardown),
        cmocka_unit_test_setup_teardown(test_strict_meta_no_change_still_skips_snapshot, setup, teardown),
        cmocka_unit_test(test_backup_opts_null_repo_is_invalid),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
