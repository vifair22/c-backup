#define _POSIX_C_SOURCE 200809L

/*
 * The reserved top-level names (src/common/reserved.h): .cbackup-unit and
 * .cbackup-captures. What makes either one trustworthy in a snapshot root is
 * that no file backup can produce it, so these are the cases that would let
 * one through: a source at or under either name however it is spelled, a
 * scan of `/` walking into one, and a synthetic entry asked for by anything
 * that is not the unit assembler.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "repo.h"
#include "reserved.h"
#include "scan.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_reserved"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

extern const db_migration_t app_migrations[];

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void touch(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static void test_names(void **state)
{
    (void)state;
    assert_true(reserved_top_name(".cbackup-unit"));
    assert_true(reserved_top_name(".cbackup-captures"));
    assert_false(reserved_top_name(".cbackup"));
    assert_false(reserved_top_name(".cbackup-units"));
    assert_false(reserved_top_name("cbackup-unit"));
    assert_false(reserved_top_name(NULL));
}

static void test_source_refusal(void **state)
{
    (void)state;
    char why[512];
    static const char *refused[] = {
        "/.cbackup-unit", "/.cbackup-unit/", "/.cbackup-unit/unit.json",
        "//.cbackup-unit", "/./.cbackup-unit/x", "/a/../.cbackup-unit",
        "/a/b/../../.cbackup-unit/config", "/../.cbackup-unit",
        "/.cbackup-captures", "/.cbackup-captures/mnt/x/app.db",
        "/x/./../.cbackup-captures", "///.cbackup-captures//index.json",
    };
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        why[0] = '\0';
        assert_int_equal(reserved_source_refused(refused[i], why, sizeof(why)), -1);
        assert_non_null(strstr(why, "reserved"));
    }
    static const char *allowed[] = {
        "/", "/home", "/home/.cbackup-unit", "/mnt/x/.cbackup-captures",
        "/.cbackup", "/.cbackup-units", "/.cbackup-unit-x", "/a/.cbackup-unit/../b",
    };
    for (size_t i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++)
        assert_int_equal(reserved_source_refused(allowed[i], why, sizeof(why)), 0);
}

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "t", &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    return mkdir(TEST_SRC, 0700);
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx);
    store_close(store);
    db_close(db);
    rmrf(TEST_DIR);
    return 0;
}

static long long snapshots(void)
{
    char r[32];
    snprintf(r, sizeof(r), "%lld", (long long)repo_id);
    db_result_t *res = NULL;
    long long n = -1;
    if (db_execute(db, "SELECT COUNT(*) FROM snapshots WHERE repo_id = ?",
                   (const char *[]){ r, NULL }, &res) == 0 && res && res->nrows == 1)
        n = strtoll(res->rows[0][0], NULL, 10);
    db_result_free(res);
    return n;
}

/* The engine's own guard, whatever the routes let through. */
static void test_backup_refuses(void **state)
{
    (void)state;
    static const char *srcs[] = {
        "/.cbackup-unit", "/.cbackup-captures/x", "/tmp/../.cbackup-unit",
    };
    for (size_t i = 0; i < sizeof(srcs) / sizeof(srcs[0]); i++) {
        const char *paths[] = { srcs[i] };
        backup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.paths = paths;
        o.n_paths = 1;
        assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);
        assert_non_null(strstr(err_msg(), "reserved"));
    }
    /* Mixed with a good source, still refused, and nothing written. */
    touch(TEST_SRC "/a", "a");
    const char *paths[] = { TEST_SRC, "/.cbackup-captures" };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 2;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);
    assert_int_equal(snapshots(), 0);
}

static status_t no_open(void *ctx, size_t i, cdc_source_t *src, uint64_t *size)
{
    (void)ctx; (void)i; (void)src; (void)size;
    return set_error(ERR_IO, "must not be called");
}

/* A synthetic entry is the unit assembler's alone, and only under the
 * unit directory, never under the captures one. */
static void test_synthetic_gated(void **state)
{
    (void)state;
    touch(TEST_SRC "/a", "a");
    const char *paths[] = { TEST_SRC };
    backup_synth_ops_t ops = { no_open, NULL, NULL, NULL, NULL };
    backup_synth_t s = { "/.cbackup-unit/unit.json", 0600, 0, 0, 0, 1, 0 };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.synth = &s;
    o.n_synth = 1;
    o.synth_ops = &ops;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);

    o.unit_snapshot = 1;
    static const char *bad[] = {
        "/.cbackup-captures/index.json", "/etc/passwd", "/.cbackup-unit/../x",
        "/.cbackup-unitx/a",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        s.path = bad[i];
        assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);
    }
    assert_int_equal(snapshots(), 0);
}

static int has_path(const scan_result_t *r, const char *p)
{
    for (uint32_t i = 0; i < r->count; i++)
        if (strcmp(r->entries[i].path, p) == 0) return 1;
    return 0;
}

/*
 * A walk whose recorded root is `/` drops both names at the top and
 * nowhere else. Scanning the real `/` is not something a test can do, so the
 * same walk is driven through scan_tree_as onto `/`, which applies the rule
 * by the recorded path exactly as a source of `/` does.
 */
static void test_scan_drops_at_top(void **state)
{
    (void)state;
    mkdir(TEST_SRC "/.cbackup-unit", 0700);
    touch(TEST_SRC "/.cbackup-unit/unit.json", "{\"forged\":true}");
    mkdir(TEST_SRC "/.cbackup-captures", 0700);
    touch(TEST_SRC "/.cbackup-captures/index.json", "{}");
    mkdir(TEST_SRC "/keep", 0700);
    mkdir(TEST_SRC "/keep/.cbackup-unit", 0700);
    touch(TEST_SRC "/keep/.cbackup-unit/x", "x");
    touch(TEST_SRC "/plain", "p");

    scan_imap_t *im = scan_imap_new();
    scan_result_t *r = NULL;
    assert_int_equal(scan_tree_as(TEST_SRC, "/", im, NULL, &r), OK);
    assert_non_null(r);
    assert_false(has_path(r, "/.cbackup-unit"));
    assert_false(has_path(r, "/.cbackup-unit/unit.json"));
    assert_false(has_path(r, "/.cbackup-captures"));
    assert_false(has_path(r, "/.cbackup-captures/index.json"));
    assert_true(has_path(r, "/plain"));
    assert_true(has_path(r, "/keep/.cbackup-unit/x"));
    assert_true(r->warn_count >= 2);
    for (uint32_t i = 0; i < r->count; i++)
        if (strcmp(r->entries[i].path, "/plain") == 0)
            assert_string_equal(r->entries[i].read_path, TEST_SRC "/plain");
    scan_result_free(r);
    scan_imap_free(im);

    /* An ordinary scan of a directory that is not `/` keeps them: they are
     * ordinary names anywhere but the top. */
    im = scan_imap_new();
    r = NULL;
    assert_int_equal(scan_tree(TEST_SRC, im, NULL, &r), OK);
    assert_true(has_path(r, TEST_SRC "/.cbackup-unit/unit.json"));
    assert_true(has_path(r, TEST_SRC "/.cbackup-captures/index.json"));
    scan_result_free(r);
    scan_imap_free(im);
}

/* Snapshot directories are never entered, at any depth, whatever the root:
 * a file backup of a pool must not read the capture snapshots in it. Their
 * contents are what a unit reads, from the snapshot path given as a root. */
static void test_scan_skips_snapshot_dirs(void **state)
{
    (void)state;
    mkdir(TEST_SRC "/.zfs", 0700);
    mkdir(TEST_SRC "/.zfs/snapshot", 0700);
    touch(TEST_SRC "/.zfs/snapshot/f", "s");
    mkdir(TEST_SRC "/system", 0700);
    mkdir(TEST_SRC "/system/.cbackup-snapshots", 0700);
    mkdir(TEST_SRC "/system/.cbackup-snapshots/cbk-0badf00d-1-0", 0700);
    touch(TEST_SRC "/system/.cbackup-snapshots/cbk-0badf00d-1-0/g", "s");
    touch(TEST_SRC "/system/kept", "k");
    scan_imap_t *im = scan_imap_new();
    scan_result_t *r = NULL;
    assert_int_equal(scan_tree(TEST_SRC, im, NULL, &r), OK);
    assert_true(has_path(r, TEST_SRC "/system/kept"));
    assert_false(has_path(r, TEST_SRC "/.zfs"));
    assert_false(has_path(r, TEST_SRC "/.zfs/snapshot/f"));
    assert_false(has_path(r, TEST_SRC "/system/.cbackup-snapshots"));
    assert_false(has_path(r, TEST_SRC "/system/.cbackup-snapshots/cbk-0badf00d-1-0/g"));
    scan_result_free(r);
    /* A unit reads from inside one, given as its root. */
    r = NULL;
    assert_int_equal(scan_tree(TEST_SRC "/system/.cbackup-snapshots/cbk-0badf00d-1-0", im, NULL, &r), OK);
    assert_true(has_path(r, TEST_SRC "/system/.cbackup-snapshots/cbk-0badf00d-1-0/g"));
    scan_result_free(r);
    scan_imap_free(im);
    assert_true(reserved_snapshot_dir(".zfs"));
    assert_true(reserved_snapshot_dir(".cbackup-snapshots"));
    assert_false(reserved_snapshot_dir("zfs"));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_names),
        cmocka_unit_test(test_source_refusal),
        cmocka_unit_test_setup_teardown(test_backup_refuses, setup, teardown),
        cmocka_unit_test_setup_teardown(test_synthetic_gated, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_drops_at_top, setup, teardown),
        cmocka_unit_test_setup_teardown(test_scan_skips_snapshot_dirs, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
