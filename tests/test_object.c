#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/object.h"

#define TEST_REPO "/tmp/c_backup_test_repo"

static repo_t *repo;

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO);
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO);
    return 0;
}

static void test_store_and_load(void **state) {
    (void)state;
    const char *data = "Hello, snapshot backup!";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE,
                                  data, len, hash), OK);

    void *out = NULL; size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

static void test_dedup(void **state) {
    (void)state;
    const char *data = "duplicate content";
    size_t len = strlen(data);
    uint8_t hash1[OBJECT_HASH_SIZE] = {0};
    uint8_t hash2[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash1), OK);
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash2), OK);
    assert_memory_equal(hash1, hash2, OBJECT_HASH_SIZE);
}

static void test_object_exists(void **state) {
    (void)state;
    const char *data = "existence check";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE,
                                  data, strlen(data), hash), OK);
    assert_true(object_exists(repo, hash));
}

static void test_repo_init_requires_empty_dir(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_nonempty_repo";
    system("rm -rf /tmp/c_backup_nonempty_repo");
    assert_int_equal(mkdir(path, 0755), 0);

    char marker[256];
    snprintf(marker, sizeof(marker), "%s/keep.txt", path);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fputs("do not clobber\n", f);
    fclose(f);

    assert_int_not_equal(repo_init(path), OK);

    system("rm -rf /tmp/c_backup_nonempty_repo");
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_store_and_load, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dedup,          setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_exists,  setup, teardown),
        cmocka_unit_test(test_repo_init_requires_empty_dir),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
