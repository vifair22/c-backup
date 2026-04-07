/*
 * Error/failure path tests for c-backup.
 *
 * Moved from test_data_paths.c: load errors, truncation, missing objects,
 * bad args, init failures, snapshot edge cases.
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

#include "repo.h"
#include "object.h"
#include "pack.h"
#include "snapshot.h"
#include "backup.h"
#include "restore.h"
#include "gc.h"
#include "parity.h"
#include "types.h"

#define TEST_REPO  "/tmp/c_backup_errpath_repo"
#define TEST_SRC   "/tmp/c_backup_errpath_src"
#define TEST_DEST  "/tmp/c_backup_errpath_dest"

static repo_t *repo;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
}

static int setup_basic(void **state) {
    (void)state;
    cleanup();
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_basic(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

static int setup_pipeline(void **state) {
    (void)state;
    cleanup();
    int rc = system("mkdir -p " TEST_SRC);
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

static void write_file(const char *path, const void *data, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    if (len > 0) {
        ssize_t w = write(fd, data, len);
        assert_int_equal((size_t)w, len);
    }
    close(fd);
}

static void object_hash_path(const uint8_t hash[OBJECT_HASH_SIZE], char out[512]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    snprintf(out, 512, "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
}

/* ================================================================== */
/* Tests                                                               */
/* ================================================================== */

static void test_fail_load_missing(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xDE, OBJECT_HASH_SIZE);
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, missing, &out, &out_len, NULL), ERR_NOT_FOUND);
}

static void test_fail_truncated_object(void **state) {
    (void)state;
    const char *data = "will be truncated";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)sizeof(object_header_t) + 1), 0);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    status_t st = object_load(repo, hash, &out, &out_len, NULL);
    assert_int_not_equal(st, OK);
    free(out);
}

static void test_fail_physical_size_null(void **state) {
    (void)state;
    const char *data = "physical size null arg";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    assert_int_equal(object_physical_size(repo, hash, NULL), ERR_INVALID);
}

static void test_fail_physical_size_missing(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xBB, OBJECT_HASH_SIZE);
    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(repo, missing, &bytes), ERR_NOT_FOUND);
}

static void test_fail_snapshot_missing(void **state) {
    (void)state;
    snapshot_t *snap = NULL;
    assert_int_not_equal(snapshot_load(repo, 999, &snap), OK);
}

static void test_fail_pack_load_no_packs(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xCC, OBJECT_HASH_SIZE);
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(pack_object_load(repo, missing, &out, &out_len, NULL), ERR_NOT_FOUND);
}

static void test_fail_init_nonempty(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_errpath_nonempty";
    int rc = system("rm -rf /tmp/c_backup_errpath_nonempty");
    (void)rc;
    assert_int_equal(mkdir(path, 0755), 0);

    char marker[256];
    snprintf(marker, sizeof(marker), "%s/existing.txt", path);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fputs("block init\n", f);
    fclose(f);

    assert_int_not_equal(repo_init(path), OK);

    rc = system("rm -rf /tmp/c_backup_errpath_nonempty");
    (void)rc;
}

static void test_snapshot_empty(void **state) {
    (void)state;
    snapshot_t snap = {0};
    snap.snap_id = 1;
    snap.created_sec = 1000;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->snap_id, 1);
    assert_int_equal(loaded->node_count, 0);
    assert_int_equal(loaded->dirent_count, 0);
    snapshot_free(loaded);
}

static void test_snapshot_delete_verify(void **state) {
    (void)state;

    write_file(TEST_SRC "/x.txt", "data\n", 5);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    assert_int_equal(snapshot_delete(repo, 1), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_fail_load_missing, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_truncated_object, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_physical_size_null, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_physical_size_missing, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_snapshot_missing, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_pack_load_no_packs, setup_basic, teardown_basic),
        cmocka_unit_test(test_fail_init_nonempty),
        cmocka_unit_test_setup_teardown(test_snapshot_empty, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_snapshot_delete_verify, setup_pipeline, teardown_pipeline),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
