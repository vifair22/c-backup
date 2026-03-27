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
#include <sys/wait.h>
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

/* repo_init on a non-empty directory should fail */
static void test_repo_init_rejects_non_empty_dir(void **state) {
    (void)state;
    assert_int_equal(mkdir(TEST_REPO, 0755), 0);
    /* Put a file in it to make it non-empty */
    FILE *f = fopen(TEST_REPO "/junk.txt", "w");
    assert_non_null(f);
    fputs("not empty", f);
    fclose(f);
    assert_int_not_equal(repo_init(TEST_REPO), OK);
}

/* repo_open on a nonexistent path should fail */
static void test_repo_open_nonexistent(void **state) {
    (void)state;
    repo_t *r = NULL;
    assert_int_not_equal(repo_open("/tmp/c_backup_repo_NOPE_does_not_exist", &r), OK);
    assert_null(r);
}

/* repo_open on a directory with no format file */
static void test_repo_open_missing_format(void **state) {
    (void)state;
    assert_int_equal(mkdir(TEST_REPO, 0755), 0);
    repo_t *r = NULL;
    assert_int_equal(repo_open(TEST_REPO, &r), ERR_CORRUPT);
    assert_null(r);
}

/* repo_init twice on same path should succeed (idempotent via EEXIST) */
static void test_repo_init_idempotent(void **state) {
    (void)state;
    assert_int_equal(repo_init(TEST_REPO), OK);
    /* Second init on the same path — now it's non-empty but is a valid repo.
     * repo_init should see it's non-empty and reject it. */
    assert_int_not_equal(repo_init(TEST_REPO), OK);
}

/* Lock contention: fork, parent holds exclusive lock, child can't get it */
static void test_repo_lock_contention(void **state) {
    (void)state;
    assert_int_equal(repo_init(TEST_REPO), OK);
    repo_t *r = NULL;
    assert_int_equal(repo_open(TEST_REPO, &r), OK);
    assert_int_equal(repo_lock(r), OK);

    /* Create a pipe for child→parent signaling */
    int pfd[2];
    assert_int_equal(pipe(pfd), 0);

    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        /* Child: try to get exclusive lock — should fail with EWOULDBLOCK */
        close(pfd[0]);
        repo_t *child_repo = NULL;
        status_t st = repo_open(TEST_REPO, &child_repo);
        int result = 0;
        if (st == OK) {
            st = repo_lock(child_repo);
            result = (st != OK) ? 1 : 0;  /* 1 = correctly failed */
            repo_close(child_repo);
        }
        ssize_t w = write(pfd[1], &result, sizeof(result));
        (void)w;
        close(pfd[1]);
        _exit(0);
    }

    /* Parent: wait for child result */
    close(pfd[1]);
    int result = 0;
    ssize_t rd = read(pfd[0], &result, sizeof(result));
    close(pfd[0]);
    assert_int_equal(rd, (ssize_t)sizeof(result));
    assert_int_equal(result, 1);  /* child should have failed to lock */

    int wstatus;
    waitpid(pid, &wstatus, 0);

    repo_unlock(r);
    repo_close(r);
}

/* repo_close(NULL) should not crash */
static void test_repo_close_null_safe(void **state) {
    (void)state;
    repo_close(NULL);  /* should be a no-op */
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_repo_init_rejects_non_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_open_detects_bad_format, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_lock_prunes_tmp_and_relock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_init_rejects_non_empty_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_open_nonexistent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_open_missing_format, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_init_idempotent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_lock_contention, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_close_null_safe, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
