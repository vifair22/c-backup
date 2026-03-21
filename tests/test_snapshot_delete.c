#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/tag.h"

#define TEST_REPO "/tmp/c_backup_snapshot_delete_repo"
#define TEST_SRC  "/tmp/c_backup_snapshot_delete_src"

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static int run_cmd(const char *fmt, ...) {
    char cmd[2048];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    assert_true(n > 0 && (size_t)n < sizeof(cmd));

    int rc = system(cmd);
    if (rc == -1) return -1;
    if (WIFEXITED(rc)) return WEXITSTATUS(rc);
    return -1;
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file(TEST_SRC "/a.txt", "one");

    assert_int_equal(run_cmd("./build/backup init --repo %s --keep-snaps 10 >/dev/null 2>&1", TEST_REPO), 0);
    assert_int_equal(run_cmd("./build/backup run --repo %s --path %s >/dev/null 2>&1", TEST_REPO, TEST_SRC), 0);
    write_file(TEST_SRC "/a.txt", "two");
    assert_int_equal(run_cmd("./build/backup run --repo %s --path %s >/dev/null 2>&1", TEST_REPO, TEST_SRC), 0);
    assert_int_equal(run_cmd("./build/backup tag --repo %s set --snapshot 1 --name keepme >/dev/null 2>&1", TEST_REPO), 0);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

static void test_snapshot_delete_refuses_tagged_without_force(void **state) {
    (void)state;
    int rc = run_cmd("./build/backup snapshot --repo %s delete --snapshot 1 >/dev/null 2>&1", TEST_REPO);
    assert_int_not_equal(rc, 0);

    repo_t *repo = NULL;
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    snapshot_free(snap);
    repo_close(repo);
}

static void test_snapshot_delete_dry_run_no_changes(void **state) {
    (void)state;
    assert_int_equal(run_cmd("./build/backup snapshot --repo %s delete --snapshot 1 --force --dry-run >/dev/null 2>&1", TEST_REPO), 0);

    repo_t *repo = NULL;
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    snapshot_free(snap);
    repo_close(repo);
}

static void test_snapshot_delete_force_removes_snapshot_and_tag(void **state) {
    (void)state;
    assert_int_equal(run_cmd("./build/backup snapshot --repo %s delete --snapshot 1 --force --no-gc >/dev/null 2>&1", TEST_REPO), 0);

    repo_t *repo = NULL;
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), ERR_NOT_FOUND);

    uint32_t tagged = 0;
    assert_int_equal(tag_get(repo, "keepme", &tagged), ERR_NOT_FOUND);
    repo_close(repo);
}

static void test_snapshot_delete_force_head_updates_head(void **state) {
    (void)state;
    assert_int_equal(run_cmd("./build/backup snapshot --repo %s delete --snapshot 1 --force --no-gc >/dev/null 2>&1", TEST_REPO), 0);
    assert_int_equal(run_cmd("./build/backup snapshot --repo %s delete --snapshot 2 --force --no-gc >/dev/null 2>&1", TEST_REPO), 0);

    repo_t *repo = NULL;
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    uint32_t head = 999;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 0u);
    repo_close(repo);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_snapshot_delete_refuses_tagged_without_force, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_delete_dry_run_no_changes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_delete_force_removes_snapshot_and_tag, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_delete_force_head_updates_head, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
