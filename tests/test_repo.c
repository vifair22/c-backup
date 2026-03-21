#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"

#define TEST_REPO "/tmp/c_backup_repo_repo"

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO);
    return 0;
}

static void test_repo_init_rejects_non_dir(void **state) {
    (void)state;
    FILE *f = fopen(TEST_REPO, "w");
    assert_non_null(f);
    fputs("x", f);
    fclose(f);
    assert_int_not_equal(repo_init(TEST_REPO), OK);
}

static void test_repo_open_detects_bad_format(void **state) {
    (void)state;
    assert_int_equal(mkdir(TEST_REPO, 0755), 0);

    FILE *f = fopen(TEST_REPO "/format", "w");
    assert_non_null(f);
    fputs("bad-format\n", f);
    fclose(f);

    repo_t *repo = NULL;
    assert_int_equal(repo_open(TEST_REPO, &repo), ERR_CORRUPT);
}

static void test_repo_lock_prunes_tmp_and_relock(void **state) {
    (void)state;
    assert_int_equal(repo_init(TEST_REPO), OK);

    char tmp_file[256];
    snprintf(tmp_file, sizeof(tmp_file), "%s/tmp/stale.tmp", TEST_REPO);
    FILE *f = fopen(tmp_file, "w");
    assert_non_null(f);
    fputs("tmp", f);
    fclose(f);

    repo_t *repo = NULL;
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);

    assert_int_equal(repo_lock(repo), OK);
    assert_int_equal(access(tmp_file, F_OK), -1);

    /* already locked by us */
    assert_int_equal(repo_lock(repo), OK);
    assert_int_equal(repo_lock_shared(repo), OK);

    repo_unlock(repo);
    repo_unlock(repo); /* no-op branch */
    repo_close(repo);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_repo_init_rejects_non_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_open_detects_bad_format, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_lock_prunes_tmp_and_relock, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
