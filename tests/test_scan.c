#define _GNU_SOURCE
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

#include "scan.h"
#include "util.h"
#include "test_tmp.h"

#define TEST_ROOT1 TEST_TMP "/c_backup_scan_root1"
#define TEST_ROOT2 TEST_TMP "/c_backup_scan_root2"
#define TEST_MISS  TEST_TMP "/c_backup_scan_missing"

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

/*
 * Patterns that are not absolute paths.
 *
 * These were accepted by every caller and then silently dropped: the matcher
 * skipped anything not starting with '/', so --exclude '*.tmp' excluded
 * nothing and said nothing about it. Whoever wrote the pattern believed the
 * files were out of the backup, and the only evidence otherwise was the
 * repository size months later.
 */
static void scan_with(const char **ex, int n, scan_result_t **out) {
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);
    scan_opts_t opts = { .exclude = ex, .n_exclude = n };
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, out), OK);
    assert_non_null(*out);
    scan_imap_free(imap);
}

static int saw(const scan_result_t *res, const char *needle) {
    for (uint32_t i = 0; i < res->count; i++)
        if (strstr(res->entries[i].path, needle)) return 1;
    return 0;
}

static void test_exclude_by_glob_on_basename(void **state) {
    (void)state;
    const char *ex[] = { "*.tmp" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "skip.tmp"));
    assert_true(saw(res, "keep.txt"));
    scan_result_free(res);
}

static void test_exclude_by_bare_name_at_any_depth(void **state) {
    (void)state;
    /* "sub" names a directory one level down; the pattern knows nothing
     * about where the tree is rooted. */
    const char *ex[] = { "sub" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "sub/nested.txt"));
    assert_true(saw(res, "keep.txt"));
    scan_result_free(res);
}

/* A directory match prunes the subtree rather than filtering its entries
 * one at a time on the way out. */
static void test_exclude_of_directory_prunes_subtree(void **state) {
    (void)state;
    mkdir(TEST_ROOT1 "/sub/deeper", 0755);
    write_file(TEST_ROOT1 "/sub/deeper/leaf.txt", "leaf");
    const char *ex[] = { "sub" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "deeper"));
    assert_false(saw(res, "leaf.txt"));
    scan_result_free(res);
}

static void test_exclude_by_path_fragment(void **state) {
    (void)state;
    const char *ex[] = { "sub/nested.txt" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "nested.txt"));
    assert_true(saw(res, "keep.txt"));
    scan_result_free(res);
}

static void test_exclude_absolute_glob(void **state) {
    (void)state;
    const char *ex[] = { TEST_ROOT1 "/*.tmp" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "skip.tmp"));
    assert_true(saw(res, "keep.txt"));
    scan_result_free(res);
}

/* A trailing slash is how everyone writes a directory in a .gitignore. */
static void test_exclude_tolerates_trailing_slash(void **state) {
    (void)state;
    const char *ex[] = { "sub/" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "nested.txt"));
    scan_result_free(res);
}

/* An absolute literal still matches on component boundaries only, so a
 * pattern must not swallow a sibling that merely starts the same way. */
static void test_absolute_exclude_stops_at_component_boundary(void **state) {
    (void)state;
    mkdir(TEST_ROOT1 "/subterranean", 0755);
    write_file(TEST_ROOT1 "/subterranean/x.txt", "x");
    const char *ex[] = { TEST_ROOT1 "/sub" };
    scan_result_t *res = NULL;
    scan_with(ex, 1, &res);
    assert_false(saw(res, "sub/nested.txt"));
    assert_true(saw(res, "subterranean/x.txt"));
    scan_result_free(res);
}

/*
 * An absolute pattern is normalised before it is matched.
 *
 * "/a/../a/b" names /a/b to anyone reading it and matched nothing, as did a
 * doubled slash or a "./": the scan's paths are clean and the pattern was
 * compared as written. An exclude that silently excludes nothing is the
 * failure the comment above is_excluded describes. Each spelling is tried
 * on its own, against both the literal form and the absolute glob form.
 */
static void test_absolute_exclude_is_normalised(void **state) {
    (void)state;
    const char *literal[] = {
        TEST_TMP "/c_backup_scan_root1/../c_backup_scan_root1/sub",
        "/" TEST_TMP "//c_backup_scan_root1///sub",
        TEST_TMP "/./c_backup_scan_root1/./sub/.",
        TEST_TMP "/c_backup_scan_root1/sub/x/..",
    };
    for (size_t i = 0; i < sizeof(literal) / sizeof(literal[0]); i++) {
        scan_result_t *res = NULL;
        scan_with(&literal[i], 1, &res);
        if (saw(res, "nested.txt"))
            fail_msg("pattern %s did not exclude sub/", literal[i]);
        assert_true(saw(res, "keep.txt"));
        scan_result_free(res);
    }

    const char *glob[] = { TEST_TMP "/c_backup_scan_root1/sub/../*.tmp" };
    scan_result_t *res = NULL;
    scan_with(glob, 1, &res);
    assert_false(saw(res, "skip.tmp"));
    assert_true(saw(res, "keep.txt"));
    scan_result_free(res);

    /* And the normaliser on its own, at the edges. */
    char out[64];
    assert_int_equal(path_normalize("/", out, sizeof(out)), 0);
    assert_string_equal(out, "/");
    assert_int_equal(path_normalize("/..", out, sizeof(out)), 0);
    assert_string_equal(out, "/");
    assert_int_equal(path_normalize("/a/..", out, sizeof(out)), 0);
    assert_string_equal(out, "/");
    assert_int_equal(path_normalize("/../a//b/./c/", out, sizeof(out)), 0);
    assert_string_equal(out, "/a/b/c");
    assert_int_equal(path_normalize("/a/b/../../c", out, sizeof(out)), 0);
    assert_string_equal(out, "/c");
    assert_int_equal(path_normalize("relative", out, sizeof(out)), -1);
    assert_int_equal(path_normalize("/abcdef", out, 4), -1);
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

/*
 * PERF-ACL-SKIP: the scan reads a POSIX ACL only when listxattr names one.
 * Every shape that carries an ACL must still arrive with it: an access ACL
 * alone, an access ACL beside a user xattr, a directory's default ACL
 * alone; and a plain file must still have none.
 */
static const scan_entry_t *entry_named(const scan_result_t *res, const char *path) {
    for (uint32_t i = 0; i < res->count; i++)
        if (strcmp(res->entries[i].path, path) == 0) return &res->entries[i];
    fail_msg("no entry %s", path);
    return NULL;
}

static int acl_blob_has(const scan_entry_t *e, uint8_t kind) {
    for (size_t off = 0; e->acl_data && off + 5 <= e->acl_len;) {
        uint8_t k = e->acl_data[off];
        uint32_t len = (uint32_t)e->acl_data[off + 1] | (uint32_t)e->acl_data[off + 2] << 8 |
                       (uint32_t)e->acl_data[off + 3] << 16 | (uint32_t)e->acl_data[off + 4] << 24;
        if (k == kind) return 1;
        off += 5 + len;
    }
    return 0;
}

static void test_scan_captures_acls_only_where_listed(void **state) {
    (void)state;
    write_file(TEST_ROOT1 "/acl_only.txt", "a");
    write_file(TEST_ROOT1 "/acl_and_xattr.txt", "b");
    write_file(TEST_ROOT1 "/plain.txt", "c");
    assert_int_equal(mkdir(TEST_ROOT1 "/dflt", 0755), 0);
    acl_t own = acl_from_text("u::rw-,g::r--,o::r--,u:1234:rw-,m::rw-");
    acl_t dflt = acl_from_text("u::rwx,g::r-x,o::---,g:4321:r-x,m::r-x");
    assert_non_null(own);
    assert_non_null(dflt);
    int rc = acl_set_file(TEST_ROOT1 "/acl_only.txt", ACL_TYPE_ACCESS, own);
    if (rc == 0) rc = acl_set_file(TEST_ROOT1 "/acl_and_xattr.txt", ACL_TYPE_ACCESS, own);
    if (rc == 0) rc = acl_set_file(TEST_ROOT1 "/dflt", ACL_TYPE_DEFAULT, dflt);
    acl_free(own);
    acl_free(dflt);
    if (rc != 0) skip();   /* no ACL support where the tests run */
    if (setxattr(TEST_ROOT1 "/acl_and_xattr.txt", "user.note", "v", 1, 0) != 0) skip();

    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);
    scan_opts_t opts = { .collect_meta = 1 };
    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &res), OK);

    const scan_entry_t *a = entry_named(res, TEST_ROOT1 "/acl_only.txt");
    assert_true(acl_blob_has(a, ACL_BLOB_ACCESS));
    assert_null(a->xattr_data);

    const scan_entry_t *b = entry_named(res, TEST_ROOT1 "/acl_and_xattr.txt");
    assert_true(acl_blob_has(b, ACL_BLOB_ACCESS));
    assert_non_null(b->xattr_data);
    /* The ACL travels on its own channel, never inside the xattr blob. */
    assert_null(memmem(b->xattr_data, b->xattr_len, "posix_acl", 9));

    const scan_entry_t *d = entry_named(res, TEST_ROOT1 "/dflt");
    assert_true(acl_blob_has(d, ACL_BLOB_DEFAULT));
    assert_false(acl_blob_has(d, ACL_BLOB_ACCESS));

    const scan_entry_t *c = entry_named(res, TEST_ROOT1 "/plain.txt");
    assert_null(c->acl_data);
    assert_int_equal(c->acl_len, 0);

    scan_result_free(res);
    scan_imap_free(imap);
}

/*
 * B128 at the scan: a directory swapped for a symlink after its lstat and
 * before it is opened was followed by opendir(path), and the scan recorded
 * whatever the link pointed at under the source's names. The progress
 * callback fires as each entry is appended, which is exactly that window
 * for a directory; a first scan finds which callback that is.
 */
typedef struct { uint32_t at; int fired; } dirswap_ctx_t;

static void dirswap_cb(uint32_t count, void *vctx) {
    dirswap_ctx_t *c = vctx;
    if (c->fired || count != c->at) return;
    c->fired = 1;
    mkdir(TEST_MISS, 0755);
    write_file(TEST_MISS "/secret.txt", "not under the source");
    assert_int_equal(rename(TEST_ROOT1 "/sub", TEST_ROOT2 "/sub.aside"), 0);
    assert_int_equal(symlink(TEST_MISS, TEST_ROOT1 "/sub"), 0);
}

static void test_scan_does_not_follow_a_directory_swapped_for_a_symlink(void **state) {
    (void)state;
    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);
    scan_result_t *res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, NULL, &res), OK);
    dirswap_ctx_t c = { 0, 0 };
    for (uint32_t i = 0; i < res->count; i++)
        if (strcmp(res->entries[i].path, TEST_ROOT1 "/sub") == 0) c.at = i + 1;
    assert_true(c.at > 0);
    scan_result_free(res);
    scan_imap_free(imap);

    imap = scan_imap_new();
    assert_non_null(imap);
    scan_opts_t opts = { .progress_cb = dirswap_cb, .progress_ctx = &c,
                         .progress_every = 1, .collect_meta = 1 };
    res = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &res), OK);
    assert_true(c.fired);

    int saw_sub = 0;
    for (uint32_t i = 0; i < res->count; i++) {
        const scan_entry_t *e = &res->entries[i];
        if (strstr(e->path, "secret.txt") || strstr(e->path, "nested.txt"))
            fail_msg("scan reached %s through the swapped-in symlink", e->path);
        if (strcmp(e->path, TEST_ROOT1 "/sub") == 0) {
            saw_sub = 1;
            assert_int_equal(e->node.type, NODE_TYPE_DIR);
        }
    }
    assert_true(saw_sub);
    int warned = 0;
    for (uint32_t i = 0; i < res->warn_count; i++)
        if (strstr(res->warnings[i], TEST_ROOT1 "/sub") &&
            strstr(res->warnings[i], "now a symlink"))
            warned = 1;
    assert_true(warned);
    scan_result_free(res);
    scan_imap_free(imap);
}

static void test_scan_warns_on_unreadable_dir(void **state) {
    (void)state;
    /*
     * Root ignores the permission bits, so there is no such thing as a
     * directory it cannot read and nothing to warn about. CI runs as root,
     * which is why this failed there and passed everywhere else - invisibly,
     * until the test target started reporting unit failures at all.
     */
    if (geteuid() == 0) skip();

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

/*
 * one_filesystem must be evaluated per scan_tree call, not once per options
 * struct. backup_run reuses one scan_opts_t across every source root, and the
 * root device used to be cached in that struct, so the second root inherited
 * the first root's device and was skipped whole as a "mount point". Nothing
 * failed: the backup succeeded, missing an entire source tree.
 *
 * /dev/shm is a separate tmpfs from /tmp on Linux and inside a container, so
 * two roots there are genuinely on different devices. If they are not, the
 * test cannot say anything and says so rather than passing quietly.
 */
static void test_one_filesystem_is_per_root(void **state) {
    (void)state;

    const char *shm_root = "/dev/shm/c_backup_scan_shm";
    int rc = system("rm -rf /dev/shm/c_backup_scan_shm");
    (void)rc;
    if (mkdir(shm_root, 0755) != 0) {
        print_message("skipped: cannot create %s\n", shm_root);
        return;
    }
    write_file("/dev/shm/c_backup_scan_shm/shm.txt", "on the other device");

    struct stat a, b;
    assert_int_equal(stat(TEST_ROOT1, &a), 0);
    assert_int_equal(stat(shm_root, &b), 0);
    if (a.st_dev == b.st_dev) {
        print_message("skipped: %s and %s share a device\n", TEST_ROOT1, shm_root);
        rc = system("rm -rf /dev/shm/c_backup_scan_shm");
        (void)rc;
        return;
    }

    scan_imap_t *imap = scan_imap_new();
    assert_non_null(imap);

    /* One options struct, two roots, exactly as backup_run does it. */
    scan_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.one_filesystem = 1;

    scan_result_t *first = NULL;
    assert_int_equal(scan_tree(TEST_ROOT1, imap, &opts, &first), OK);
    assert_non_null(first);
    assert_true(first->count > 0);

    scan_result_t *second = NULL;
    assert_int_equal(scan_tree(shm_root, imap, &opts, &second), OK);
    assert_non_null(second);

    int found = 0;
    for (uint32_t i = 0; i < second->count; i++) {
        if (second->entries[i].path && strstr(second->entries[i].path, "shm.txt"))
            found = 1;
    }
    assert_true(found);

    scan_result_free(first);
    scan_result_free(second);
    scan_imap_free(imap);
    rc = system("rm -rf /dev/shm/c_backup_scan_shm");
    (void)rc;
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
        cmocka_unit_test_setup_teardown(test_exclude_by_glob_on_basename, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exclude_by_bare_name_at_any_depth, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exclude_of_directory_prunes_subtree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exclude_by_path_fragment, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exclude_absolute_glob, setup, teardown),
        cmocka_unit_test_setup_teardown(test_exclude_tolerates_trailing_slash, setup, teardown),
        cmocka_unit_test_setup_teardown(test_absolute_exclude_stops_at_component_boundary, setup, teardown),
        cmocka_unit_test_setup_teardown(test_absolute_exclude_is_normalised, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_root_excluded_returns_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_captures_acls_only_where_listed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_collect_meta_off_and_collect_on_demand, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_warns_on_unreadable_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_does_not_follow_a_directory_swapped_for_a_symlink, setup, teardown),
        cmocka_unit_test_setup_teardown(test_one_filesystem_is_per_root, setup, teardown),
        cmocka_unit_test(test_scan_entry_collect_metadata_invalid_args),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
