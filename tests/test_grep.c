#define _POSIX_C_SOURCE 200809L

/*
 * Content search across a snapshot.
 *
 * The pre-daemon CLI had this; the rewrite left only filename search behind,
 * so "which backup still has that config line" had no answer short of
 * restoring a snapshot and running grep(1) over it.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "grep.h"
#include "pack.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_grep"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/conf", 0700) != 0) return -1;
    write_file(TEST_SRC "/notes.txt",
               "first line\nthe secret token is swordfish\nlast line\n");
    write_file(TEST_SRC "/conf/app.ini",
               "[server]\nport = 8080\ntoken = swordfish\n");
    write_file(TEST_SRC "/other.txt", "nothing interesting here\n");
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static int64_t run_backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    return br.snapshot_id;
}

static grep_opts_t opts_for(const char *pattern)
{
    grep_opts_t o;
    memset(&o, 0, sizeof(o));
    o.pattern = pattern;
    o.max_hits_per_file = 10;
    return o;
}

/* Every recorded hit for a run, as one blob, so a test can assert on paths
 * and excerpts without threading a row cursor through each case. */
static char *hits_text(int64_t run_id)
{
    char run[32];
    snprintf(run, sizeof(run), "%lld", (long long)run_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT path, line_no, excerpt FROM grep_hits WHERE run_id = ? ORDER BY id",
        (const char *[]){ run, NULL }, &r), 0);
    size_t cap = 4096, len = 0;
    char *buf = calloc(1, cap);
    assert_non_null(buf);
    for (int i = 0; i < r->nrows; i++) {
        int n = snprintf(buf + len, cap - len, "%s:%s:%s\n",
                         r->rows[i][0], r->rows[i][1], r->rows[i][2]);
        if (n < 0 || (size_t)n >= cap - len) break;
        len += (size_t)n;
    }
    db_result_free(r);
    return buf;
}

/* ------------------------------------------------------------------ */

static void test_finds_a_string_in_two_files(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("swordfish");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);

    assert_int_equal(res.files_matched, 2);
    assert_int_equal(res.hits, 2);
    assert_true(res.files_scanned >= 3);

    char *hits = hits_text(res.run_id);
    assert_non_null(strstr(hits, "notes.txt"));
    assert_non_null(strstr(hits, "app.ini"));
    /* The line number and the line itself, which is what makes a hit list
     * useful rather than a list of filenames. */
    assert_non_null(strstr(hits, ":2:"));
    assert_non_null(strstr(hits, "the secret token is swordfish"));
    free(hits);
}

static void test_no_match_reports_nothing(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    grep_opts_t o = opts_for("zzzznotpresent");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 0);
    assert_int_equal(res.hits, 0);
    assert_true(res.files_scanned > 0);
}

/* It is a regular expression, not a substring search. */
static void test_regex_syntax(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    grep_opts_t o = opts_for("^port = [0-9]+$");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 1);
    char *hits = hits_text(res.run_id);
    assert_non_null(strstr(hits, "app.ini"));
    free(hits);
}

static void test_bad_pattern_is_rejected(void **state)
{
    (void)state;
    run_backup();
    grep_opts_t o = opts_for("unclosed[");
    grep_result_t res;
    assert_int_not_equal(grep_run(store, db, repo_id, 1, &o, NULL, NULL, NULL, &res), OK);
    err_clear();
}

static void test_ignore_case(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("SWORDFISH");
    grep_result_t sensitive;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &sensitive), OK);
    assert_int_equal(sensitive.files_matched, 0);

    o.ignore_case = 1;
    grep_result_t insensitive;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &insensitive), OK);
    assert_int_equal(insensitive.files_matched, 2);
}

static void test_path_prefix_scopes_the_search(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("swordfish");
    /* Paths are stored with the leading slash stripped, and the prefix is
     * accepted either way. */
    o.path_prefix = TEST_SRC "/conf";
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 1);
    char *hits = hits_text(res.run_id);
    assert_non_null(strstr(hits, "app.ini"));
    assert_null(strstr(hits, "notes.txt"));
    free(hits);
}

/*
 * A prefix decides what gets read, not what gets kept.
 *
 * This is the property that was wrong. The prefix used to be tested after
 * every object in the snapshot had already been enumerated, and each
 * candidate cost a query plus a parent walk per referencing node before
 * being discarded - so scoping a search made it slower, and scoping it to
 * something broad made it much slower. Measured against a real repository:
 * 29 seconds unscoped became over 110 scoped to the whole tree.
 *
 * files_scanned is what proves it. Correctness was never the problem, so a
 * test that only checks the hits passes either way.
 */
static void test_path_prefix_reads_only_the_subtree(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    grep_opts_t all = opts_for("swordfish");
    grep_result_t ra;
    assert_int_equal(grep_run(store, db, repo_id, snap, &all, NULL, NULL, NULL, &ra), OK);
    assert_int_equal(ra.files_scanned, 3);   /* notes.txt, other.txt, conf/app.ini */

    grep_opts_t one = opts_for("swordfish");
    one.path_prefix = TEST_SRC "/conf";
    grep_result_t ro;
    assert_int_equal(grep_run(store, db, repo_id, snap, &one, NULL, NULL, NULL, &ro), OK);
    assert_int_equal(ro.files_matched, 1);
    /* The two outside the prefix are never opened. */
    assert_int_equal(ro.files_scanned, 1);
}

/*
 * Both spellings of the same path.
 *
 * Search results carry an absolute path and grep hits do not, so somebody
 * copying a path from one screen into the other would otherwise get silence
 * rather than results.
 */
static void test_path_prefix_accepts_either_slash_form(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    const char *forms[] = { TEST_SRC "/conf", TEST_SRC "/conf/" };
    for (size_t i = 0; i < sizeof(forms) / sizeof(forms[0]); i++) {
        grep_opts_t o = opts_for("swordfish");
        o.path_prefix = forms[i];
        grep_result_t res;
        assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
        assert_int_equal(res.files_scanned, 1);
        assert_int_equal(res.files_matched, 1);
    }
}

/* A prefix naming a file searches that file and nothing else. */
static void test_path_prefix_can_name_a_single_file(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("swordfish");
    o.path_prefix = TEST_SRC "/notes.txt";
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_scanned, 1);
    assert_int_equal(res.files_matched, 1);
}

/*
 * A path that is not in the snapshot says so.
 *
 * Returning an empty result would be indistinguishable from "the pattern is
 * not there", which sends somebody looking for the wrong thing - and a typo
 * in a path is a far more likely explanation than a file having vanished.
 */
static void test_unknown_path_prefix_is_an_error(void **state)
{
    (void)state;
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("swordfish");
    o.path_prefix = TEST_SRC "/nope";
    grep_result_t res;
    assert_int_not_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    err_clear();
}

/* A search that has found enough stops reading the repository. */
static void test_max_files_stops_early(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    grep_opts_t o = opts_for("swordfish");
    o.max_files = 1;
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 1);
    assert_true(res.truncated);
}

/* Packed content is the normal case, since a backup packs on completion. */
static void test_searches_packed_content(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed > 0);

    grep_opts_t o = opts_for("swordfish");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 2);
}

/*
 * A file with NUL bytes is reported without spilling its contents into the
 * excerpt, the way grep(1) says "Binary file X matches".
 */
static void test_binary_file_matches_without_an_excerpt(void **state)
{
    (void)state;
    FILE *f = fopen(TEST_SRC "/blob.bin", "wb");
    assert_non_null(f);
    const char payload[] = "\x00\x01\x02needle\x00\x03";
    assert_int_equal(fwrite(payload, 1, sizeof(payload) - 1, f), sizeof(payload) - 1);
    fclose(f);
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("needle");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 1);
    char *hits = hits_text(res.run_id);
    assert_non_null(strstr(hits, "blob.bin"));
    assert_non_null(strstr(hits, "(binary file)"));
    free(hits);
}

/*
 * One object at several paths is read once, not once per path: dedup means
 * identical files share content, and re-reading it would multiply the cost of
 * a search by however many copies a tree happens to hold.
 */
static void test_duplicate_content_is_scanned_once(void **state)
{
    (void)state;
    write_file(TEST_SRC "/copy_a.txt", "shared marker line\n");
    write_file(TEST_SRC "/copy_b.txt", "shared marker line\n");
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("shared marker");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    /* Both copies are the same object, so one scan and one match. */
    assert_int_equal(res.files_matched, 1);
    assert_int_equal(res.hits, 1);
}

/* A cancelled search still closes its run row, marked incomplete. */
static void test_cancel_marks_the_run_incomplete(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    volatile int cancel = 1;
    grep_opts_t o = opts_for("swordfish");
    grep_result_t res;
    assert_int_not_equal(grep_run(store, db, repo_id, snap, &o, &cancel, NULL, NULL, &res), OK);
    err_clear();

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT completed FROM grep_runs ORDER BY id DESC LIMIT 1", NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);
}

/*
 * A line longer than the matcher's buffer is searched in pieces rather than
 * buffered whole. Minified assets are megabytes on one line, and allocating
 * whatever a file contains is how a backup tool runs a machine out of memory.
 */
static void test_very_long_line_does_not_buffer_without_bound(void **state)
{
    (void)state;
    FILE *f = fopen(TEST_SRC "/minified.js", "wb");
    assert_non_null(f);
    for (int i = 0; i < 300000; i++) fputs("abcdefghij", f);   /* 3 MB, no newline */
    fputs("tailmarker\n", f);
    fclose(f);
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("tailmarker");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_finds_a_string_in_two_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_no_match_reports_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_regex_syntax, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bad_pattern_is_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ignore_case, setup, teardown),
        cmocka_unit_test_setup_teardown(test_path_prefix_scopes_the_search, setup, teardown),
        cmocka_unit_test_setup_teardown(test_path_prefix_reads_only_the_subtree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_path_prefix_accepts_either_slash_form, setup, teardown),
        cmocka_unit_test_setup_teardown(test_path_prefix_can_name_a_single_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unknown_path_prefix_is_an_error, setup, teardown),
        cmocka_unit_test_setup_teardown(test_max_files_stops_early, setup, teardown),
        cmocka_unit_test_setup_teardown(test_searches_packed_content, setup, teardown),
        cmocka_unit_test_setup_teardown(test_binary_file_matches_without_an_excerpt, setup, teardown),
        cmocka_unit_test_setup_teardown(test_duplicate_content_is_scanned_once, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_marks_the_run_incomplete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_very_long_line_does_not_buffer_without_bound, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
