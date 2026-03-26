/*
 * Prune resume-pending tests for c-backup.
 *
 * Moved from test_gc_pack.c.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/gc.h"
#include "../src/object.h"
#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/pack.h"
#include "../src/tag.h"
#include "../src/parity.h"

#define TEST_REPO "/tmp/c_backup_prune_repo"
#define TEST_SRC  "/tmp/c_backup_prune_src"

static repo_t *repo;

static void write_file_str(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;

    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file_str(TEST_SRC "/a.txt", "alpha\n");
    write_file_str(TEST_SRC "/b.txt", "beta\n");

    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

/* No prune-pending file → returns OK, existing snapshot untouched. */
static void test_repo_prune_resume_pending_no_file_is_noop(void **state) {
    (void)state;
    assert_int_equal(repo_prune_resume_pending(repo), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    snapshot_free(snap);
}

/* prune-pending lists snapshot 1 → it gets deleted and the file is removed. */
static void test_repo_prune_resume_pending_completes_deletions(void **state) {
    (void)state;
    char pending_path[256];
    snprintf(pending_path, sizeof(pending_path), "%s/prune-pending", TEST_REPO);
    FILE *pf = fopen(pending_path, "w");
    assert_non_null(pf);
    fprintf(pf, "1\n");
    fclose(pf);

    assert_int_equal(repo_prune_resume_pending(repo), OK);

    struct stat st;
    assert_int_not_equal(stat(pending_path, &st), 0);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), ERR_NOT_FOUND);
}

/* prune-pending lists a tag-preserved snapshot → it must be skipped. */
static void test_repo_prune_resume_pending_skips_preserved_snap(void **state) {
    (void)state;
    assert_int_equal(tag_set(repo, "keepme", 1, 1), OK);

    char pending_path[256];
    snprintf(pending_path, sizeof(pending_path), "%s/prune-pending", TEST_REPO);
    FILE *pf = fopen(pending_path, "w");
    assert_non_null(pf);
    fprintf(pf, "1\n");
    fclose(pf);

    assert_int_equal(repo_prune_resume_pending(repo), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    snapshot_free(snap);
}

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_repo_prune_resume_pending_no_file_is_noop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_prune_resume_pending_completes_deletions, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_prune_resume_pending_skips_preserved_snap, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
