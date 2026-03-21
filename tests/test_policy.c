#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

#include "../src/repo.h"
#include "../src/policy.h"

#define TEST_REPO "/tmp/c_backup_policy_repo"

static repo_t *repo;

static char *read_file_all(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    if (sz > 0 && fread(buf, 1, (size_t)sz, f) != (size_t)sz) {
        free(buf);
        fclose(f);
        return NULL;
    }
    buf[sz] = '\0';
    fclose(f);
    return buf;
}

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

static void test_policy_defaults(void **state) {
    (void)state;
    policy_t p;
    policy_init_defaults(&p);
    assert_int_equal(p.auto_pack, 1);
    assert_int_equal(p.auto_gc, 1);
    assert_int_equal(p.auto_prune, 1);
    assert_int_equal(p.keep_snaps, 1);
    assert_int_equal(p.verify_after, 0);
    assert_int_equal(p.strict_meta, 0);
}

static void test_policy_save_load_roundtrip(void **state) {
    (void)state;
    policy_t p;
    policy_init_defaults(&p);

    p.n_paths = 2;
    p.paths = calloc((size_t)p.n_paths, sizeof(char *));
    assert_non_null(p.paths);
    p.paths[0] = strdup("/home/a");
    p.paths[1] = strdup("/etc");
    assert_non_null(p.paths[0]);
    assert_non_null(p.paths[1]);

    p.n_exclude = 2;
    p.exclude = calloc((size_t)p.n_exclude, sizeof(char *));
    assert_non_null(p.exclude);
    p.exclude[0] = strdup("*.tmp");
    p.exclude[1] = strdup(".git");
    assert_non_null(p.exclude[0]);
    assert_non_null(p.exclude[1]);

    p.keep_snaps = 7;
    p.keep_daily = 2;
    p.keep_weekly = 1;
    p.keep_monthly = 3;
    p.keep_yearly = 4;
    p.auto_pack = 0;
    p.auto_gc = 0;
    p.auto_prune = 1;
    p.verify_after = 1;
    p.strict_meta = 1;

    assert_int_equal(policy_save(repo, &p), OK);

    policy_t *loaded = NULL;
    assert_int_equal(policy_load(repo, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->n_paths, 2);
    assert_string_equal(loaded->paths[0], "/home/a");
    assert_string_equal(loaded->paths[1], "/etc");
    assert_int_equal(loaded->n_exclude, 2);
    assert_string_equal(loaded->exclude[0], "*.tmp");
    assert_string_equal(loaded->exclude[1], ".git");
    assert_int_equal(loaded->keep_snaps, 7);
    assert_int_equal(loaded->keep_daily, 2);
    assert_int_equal(loaded->keep_weekly, 1);
    assert_int_equal(loaded->keep_monthly, 3);
    assert_int_equal(loaded->keep_yearly, 4);
    assert_int_equal(loaded->auto_pack, 0);
    assert_int_equal(loaded->auto_gc, 0);
    assert_int_equal(loaded->auto_prune, 1);
    assert_int_equal(loaded->verify_after, 1);
    assert_int_equal(loaded->strict_meta, 1);
    policy_free(loaded);

    free(p.paths[0]);
    free(p.paths[1]);
    free(p.paths);
    free(p.exclude[0]);
    free(p.exclude[1]);
    free(p.exclude);
}

static void test_policy_invalid_numbers_keep_defaults(void **state) {
    (void)state;
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs("keep_snaps = -4\n", f);
    fputs("auto_pack = false\n", f);
    fclose(f);

    policy_t *loaded = NULL;
    assert_int_equal(policy_load(repo, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->keep_snaps, 1);
    assert_int_equal(loaded->auto_pack, 0);
    assert_int_equal(loaded->auto_gc, 1);
    policy_free(loaded);
}

static void test_policy_template_no_overwrite(void **state) {
    (void)state;
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs("paths = /custom\n", f);
    fclose(f);

    assert_int_equal(policy_write_template(repo), OK);

    char *content = read_file_all(path);
    assert_non_null(content);
    assert_non_null(strstr(content, "paths = /custom"));
    free(content);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_policy_defaults, setup, teardown),
        cmocka_unit_test_setup_teardown(test_policy_save_load_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_policy_invalid_numbers_keep_defaults, setup, teardown),
        cmocka_unit_test_setup_teardown(test_policy_template_no_overwrite, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
