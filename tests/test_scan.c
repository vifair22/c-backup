#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include "scan.h"

#define TEST_ROOT1 "/tmp/c_backup_scan_root1"
#define TEST_ROOT2 "/tmp/c_backup_scan_root2"
#define TEST_MISS  "/tmp/c_backup_scan_missing"

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_ROOT1 " " TEST_ROOT2 " " TEST_MISS);
    (void)rc;

    mkdir(TEST_ROOT1, 0755);
    mkdir(TEST_ROOT1 "/sub", 0755);
    write_file(TEST_ROOT1 "/keep.txt", "keep");
    write_file(TEST_ROOT1 "/skip.tmp", "skip");
    write_file(TEST_ROOT1 "/sub/nested.txt", "nested");
    if (symlink("keep.txt", TEST_ROOT1 "/lnk") != 0) return -1;

    mkdir(TEST_ROOT2, 0755);
    /* hard-link to TEST_ROOT1/keep.txt */
    if (link(TEST_ROOT1 "/keep.txt", TEST_ROOT2 "/link_to_keep") != 0) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_ROOT1 " " TEST_ROOT2 " " TEST_MISS);
    (void)rc;
    return 0;
}

static void test_scan_excludes_and_symlink(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    const char *ex[] = { TEST_ROOT1 "/skip.tmp" };
    scan_opts_t opts = { .exclude = ex, .n_exclude = 1 };
    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &res), OK);
    assert_non_null(res);

    int saw_keep = 0, saw_skip = 0, saw_symlink = 0;
    for (uint32_t i = 0; i < res->count; i++) {
        const scan_entry_t *e = &res->entries[i];
        if (strstr(e->path, "keep.txt")) saw_keep = 1;
        if (strstr(e->path, "skip.tmp")) saw_skip = 1;
        if (strstr(e->path, "/lnk") && e->node.type == NODE_TYPE_SYMLINK)
            saw_symlink = 1;
    }
    assert_true(saw_keep);
    assert_false(saw_skip);
    assert_true(saw_symlink);

    scan_result_free(res);
    scan_imap_free(imap);
}

static void test_scan_excludes_absolute_path(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    const char *ex[] = { TEST_ROOT1 "/sub" };
    scan_opts_t opts = { .exclude = ex, .n_exclude = 1 };
    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &res), OK);
    assert_non_null(res);

    int saw_nested = 0;
    int saw_keep = 0;
    for (uint32_t i = 0; i < res->count; i++) {
        const scan_entry_t *e = &res->entries[i];
        if (strstr(e->path, "sub/nested.txt")) saw_nested = 1;
        if (strstr(e->path, "keep.txt")) saw_keep = 1;
    }

    assert_false(saw_nested);
    assert_true(saw_keep);

    scan_result_free(res);
    scan_imap_free(imap);
}

static void test_scan_hardlink_across_roots(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    scan_result_t *r1 = NULL, *r2 = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, NULL, &r1), OK);
    assert_int_equal(scan_tree(TEST_ROOT2, imap, NULL, &r2), OK);
    assert_non_null(r1);
    assert_non_null(r2);

    int saw_hardlink = 0;
    for (uint32_t i = 0; i < r2->count; i++) {
        if (strstr(r2->entries[i].path, "link_to_keep") &&
            r2->entries[i].hardlink_to_node_id != 0) {
            saw_hardlink = 1;
            break;
        }
    }
    assert_true(saw_hardlink);

    scan_result_free(r1);
    scan_result_free(r2);
    scan_imap_free(imap);
}

static void test_scan_missing_root_is_nonfatal(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_MISS, imap, NULL, &res), OK);
    assert_non_null(res);
    for (uint32_t i = 0; i < res->count; i++) {
        assert_null(strstr(res->entries[i].path, "c_backup_scan_missing"));
    }

    /* Missing root should produce a warning */
    assert_true(res->warn_count > 0);
    int found_lstat_warn = 0;
    for (uint32_t i = 0; i < res->warn_count; i++) {
        if (strstr(res->warnings[i], "lstat failed") &&
            strstr(res->warnings[i], "c_backup_scan_missing"))
            found_lstat_warn = 1;
    }
    assert_true(found_lstat_warn);

    scan_result_free(res);
    scan_imap_free(imap);
}

static void test_scan_root_excluded_returns_empty(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    const char *ex[] = { TEST_ROOT1 };
    scan_opts_t opts = { .exclude = ex, .n_exclude = 1 };
    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &res), OK);
    assert_non_null(res);
    int saw_payload = 0;
    for (uint32_t i = 0; i < res->count; i++) {
        const char *p = res->entries[i].path;
        if (strstr(p, "keep.txt") || strstr(p, "nested.txt") || strstr(p, "/lnk") ||
            strstr(p, "skip.tmp")) {
            saw_payload = 1;
            break;
        }
    }
    assert_false(saw_payload);

    scan_result_free(res);
    scan_imap_free(imap);
}

static void test_scan_collect_meta_off_and_collect_on_demand(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    scan_opts_t opts = { .collect_meta = 0 };
    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &res), OK);
    assert_non_null(res);

    scan_entry_t *file = NULL;
    for (uint32_t i = 0; i < res->count; i++) {
        if (res->entries[i].node.type == NODE_TYPE_REG) {
            file = &res->entries[i];
            break;
        }
    }
    assert_non_null(file);
    assert_null(file->xattr_data);
    assert_null(file->acl_data);
    assert_int_equal(file->xattr_len, 0);
    assert_int_equal(file->acl_len, 0);

    assert_int_equal(scan_entry_collect_metadata(file), OK);
    assert_int_equal(scan_entry_collect_metadata(file), OK);

    scan_result_free(res);
    scan_imap_free(imap);
}

static void test_scan_warns_on_unreadable_dir(void **state) {
    (void)state;
    /* Create a directory we can't read */
    mkdir(TEST_ROOT1 "/noperm", 0000);

    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, NULL, &res), OK);
    assert_non_null(res);

    int found_warn = 0;
    for (uint32_t i = 0; i < res->warn_count; i++) {
        if (strstr(res->warnings[i], "cannot open directory") &&
            strstr(res->warnings[i], "noperm"))
            found_warn = 1;
    }
    assert_true(found_warn);

    scan_result_free(res);
    scan_imap_free(imap);
    chmod(TEST_ROOT1 "/noperm", 0755);
}

static void test_scan_entry_collect_metadata_invalid_args(void **state) {
    (void)state;
    assert_int_equal(scan_entry_collect_metadata(NULL), ERR_INVALID);

    scan_entry_t e;
    memset(&e, 0, sizeof(e));
    assert_int_equal(scan_entry_collect_metadata(&e), ERR_INVALID);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_scan_excludes_and_symlink, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_excludes_absolute_path, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_hardlink_across_roots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_missing_root_is_nonfatal, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_root_excluded_returns_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_collect_meta_off_and_collect_on_demand, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_warns_on_unreadable_dir, setup, teardown),
        cmocka_unit_test(test_scan_entry_collect_metadata_invalid_args),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
