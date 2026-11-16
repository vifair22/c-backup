#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64

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

#include <pthread.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "grep.h"
#include "object.h"
#include "pack.h"
#include "repo.h"
#include "repo_key.h"
#include "store.h"
#include "store_index.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_grep"
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
    /* Absolute, like name search, so a hit can be looked up as given. */
    assert_non_null(strstr(hits, TEST_SRC "/notes.txt:"));
    assert_int_equal(hits[0], '/');
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

/*
 * Not covered here: that a cancel is noticed *while* a file is being read
 * rather than only between files.
 *
 * That was the bug - the flag was tested once per object, so a search that
 * had reached a large file ignored the cancel until it finished, which
 * against a real 13.8 GB file was 65 seconds of apparently nothing, and was
 * reported as cancel being broken. It is fixed in scan_object's streaming
 * loop, and verified by hand: cancelling mid-file went from up to 65
 * seconds to 1.
 *
 * The attempt to pin it down here is deliberately absent rather than
 * quietly weakened. Both behaviours end in a cancelled run, so only elapsed
 * time distinguishes them, and the objects are read in pack order - which
 * means a fixture cannot reliably arrange for the cancel to land inside the
 * large file rather than between two small ones. The version tried first
 * passed with the fix removed, which is worse than having no test at all:
 * it claims a guarantee it is not making.
 */

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

/*
 * Packed content is not a special case, it is the only case: a backup writes
 * its objects into packs as it produces them.
 *
 * This used to back up, then call repo_pack and assert it had work to do. It
 * has none now, and that is the assertion worth making instead - a repo_pack
 * immediately after a backup should find nothing loose to collect, because
 * nothing was written loose.
 */
static void test_searches_packed_content(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 0);

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
    assert_non_null(strstr(hits, "(binary file matches)"));
    free(hits);
}

/* Write `lines` lines of "<word> N\n" to path. */
static void write_lines(const char *path, const char *word, int lines)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    for (int i = 0; i < lines; i++) fprintf(f, "%s %d\n", word, i);
    fclose(f);
}

static long count_hit_rows(int64_t run_id)
{
    char run[32];
    snprintf(run, sizeof(run), "%lld", (long long)run_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM grep_hits WHERE run_id = ?",
                                (const char *[]){ run, NULL }, &r), 0);
    long n = strtol(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

/* A run record's column, as a number. */
static long run_column(int64_t run_id, const char *col)
{
    char run[32], sql[128];
    snprintf(run, sizeof(run), "%lld", (long long)run_id);
    snprintf(sql, sizeof(sql), "SELECT %s FROM grep_runs WHERE id = ?", col);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, sql, (const char *[]){ run, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    long n = strtol(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

/*
 * B067: a binary that matches everywhere is one hit, not one per line. A
 * 10 MiB binary gave 40,776 "(binary file)" hits, each read and matched.
 */
static void test_binary_file_is_one_hit_however_often_it_matches(void **state)
{
    (void)state;
    FILE *f = fopen(TEST_SRC "/many.bin", "wb");
    assert_non_null(f);
    for (int i = 0; i < 5000; i++) {
        fputc('\0', f);
        fputs("needle\n", f);
    }
    fclose(f);
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("needle");
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 1);
    assert_int_equal(res.hits, 1);
    assert_int_equal(count_hit_rows(res.run_id), 1);
    assert_false(res.truncated);
}

/* B067: one log matching on every line stops at the per-file cap, counts
 * no more than it recorded, and says it was capped. */
static void test_per_file_cap_stops_reading_the_file(void **state)
{
    (void)state;
    write_lines(TEST_SRC "/big.log", "repeat", 20000);
    write_lines(TEST_SRC "/small.log", "repeat", 3);
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("repeat");
    o.max_hits_per_file = 50;
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 2);
    assert_int_equal(res.hits, 53);
    assert_int_equal(count_hit_rows(res.run_id), 53);
    assert_int_equal(res.files_capped, 1);
    assert_false(res.truncated);
    /* NIT-2: the run record says so too, not only the job result, so a run
     * listed later can show that a file holds more than was recorded. */
    assert_int_equal(run_column(res.run_id, "files_capped"), 1);
    assert_int_equal(run_column(res.run_id, "truncated"), 0);

    /* 0 is the default, not unlimited. */
    o.max_hits_per_file = 0;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.hits, GREP_DEFAULT_HITS_PER_FILE + 3);
}

/* B067: --limit capped files only; a total cap ends the search and marks
 * it truncated so "no more matches" and "stopped looking" differ. */
static void test_total_hit_cap_truncates_the_search(void **state)
{
    (void)state;
    for (int i = 0; i < 6; i++) {
        char p[256];
        snprintf(p, sizeof(p), TEST_SRC "/part%d.log", i);
        write_lines(p, "chunkline", 10 + i);   /* distinct content per file */
    }
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("chunkline");
    o.max_hits_per_file = 5;
    o.max_hits = 12;
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.hits, 12);
    assert_int_equal(count_hit_rows(res.run_id), 12);
    assert_true(res.truncated);
    assert_int_equal(res.files_matched, 3);
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
    /* Both copies are one object, read once: the bytes say so. */
    assert_int_equal(res.bytes_scanned,
                     strlen("shared marker line\n") + strlen("first line\nthe secret token "
                     "is swordfish\nlast line\n") + strlen("[server]\nport = 8080\ntoken "
                     "= swordfish\n") + strlen("nothing interesting here\n"));
    /* B117: but both are files holding the match, and both are named. */
    assert_int_equal(res.files_scanned, 5);
    assert_int_equal(res.files_matched, 2);
    assert_int_equal(res.hits, 2);
    assert_false(res.truncated);
    char *t = hits_text(res.run_id);
    assert_non_null(strstr(t, "/copy_a.txt:1:shared marker line"));
    assert_non_null(strstr(t, "/copy_b.txt:1:shared marker line"));
    free(t);
}

/*
 * B117: the paths holding a matched object are bounded by the caps like any
 * other matching file, and a path left out is a truncated search.
 */
static void test_duplicate_content_respects_the_caps(void **state)
{
    (void)state;
    write_file(TEST_SRC "/copy_a.txt", "dup one\ndup two\n");
    write_file(TEST_SRC "/copy_b.txt", "dup one\ndup two\n");
    write_file(TEST_SRC "/copy_c.txt", "dup one\ndup two\n");
    int64_t snap = run_backup();

    /* max_files 2: two of the three named, and the run says it stopped. */
    grep_opts_t o = opts_for("dup");
    o.max_files = 2;
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 2);
    assert_int_equal(res.hits, 4);
    assert_int_equal(count_hit_rows(res.run_id), 4);
    assert_true(res.truncated);

    /* max_hits 5: the third path is recorded with the one hit left. */
    o = opts_for("dup");
    o.max_hits = 5;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.hits, 5);
    assert_int_equal(count_hit_rows(res.run_id), 5);
    assert_int_equal(res.files_matched, 3);
    assert_true(res.truncated);

    /* Exactly enough room for all three: nothing left out. */
    o = opts_for("dup");
    o.max_files = 3;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 3);
    assert_int_equal(res.hits, 6);
    assert_false(res.truncated);

    /* A per-file cap applies to every path, and each counts as capped. */
    o = opts_for("dup");
    o.max_hits_per_file = 1;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 3);
    assert_int_equal(res.hits, 3);
    assert_int_equal(res.files_capped, 3);
    assert_false(res.truncated);
}

/* B117: with a path prefix, only the paths inside it are named or counted. */
static void test_duplicate_content_stays_in_scope(void **state)
{
    (void)state;
    write_file(TEST_SRC "/conf/dup.txt", "scoped dup\n");
    write_file(TEST_SRC "/conf/dup2.txt", "scoped dup\n");
    write_file(TEST_SRC "/dup_outside.txt", "scoped dup\n");
    int64_t snap = run_backup();

    grep_opts_t o = opts_for("scoped dup");
    o.path_prefix = TEST_SRC "/conf";
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.files_matched, 2);
    assert_int_equal(res.files_scanned, 3);   /* app.ini, dup.txt, dup2.txt */
    char *t = hits_text(res.run_id);
    assert_non_null(strstr(t, "/conf/dup.txt:1:"));
    assert_non_null(strstr(t, "/conf/dup2.txt:1:"));
    assert_null(strstr(t, "dup_outside"));
    free(t);
}

/* A cancelled search still closes its run row, marked incomplete. */
static void test_cancel_marks_the_run_incomplete(void **state)
{
    (void)state;
    int64_t snap = run_backup();
    atomic_int cancel = 1;
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

/* ------------------------------------------------------------------ */
/* Matches only from verified bytes (OWN-36)                           */
/* ------------------------------------------------------------------ */

/* Over GREP_INMEM_MAX, so it is loose and streamed through the pipe, with
 * the needle on its first line: a reader that stops at its hit cap never
 * reaches the tag or hash at the end. */
#define BIG_TEXT ((size_t)20 * 1024 * 1024)

static void write_big_text(const char *path)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs("the needle is on the first line\n", f);
    size_t n = 32;
    unsigned i = 0;
    while (n < BIG_TEXT) {
        int w = fprintf(f, "filler line %u with nothing worth finding\n", i++);
        assert_true(w > 0);
        n += (size_t)w;
    }
    fclose(f);
}

static void damage_object_of(const char *name, off_t from_end)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT content_hash FROM nodes WHERE repo_id = ? AND name = ?",
        (const char *[]){ repo, name, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    char path[600];
    snprintf(path, sizeof(path), "%s/objects/%.2s/%s", TEST_REPO,
             r->rows[0][0], r->rows[0][0] + 2);
    db_result_free(r);

    FILE *f = fopen(path, "r+b");
    assert_non_null(f);
    object_header_t hdr;
    assert_int_equal(fread(&hdr, sizeof(hdr), 1, f), 1);
    off_t off = (off_t)sizeof(hdr) + (off_t)hdr.compressed_size - from_end;
    assert_int_equal(fseeko(f, off, SEEK_SET), 0);
    int c = fgetc(f);
    assert_true(c != EOF);
    assert_int_equal(fseeko(f, off, SEEK_SET), 0);
    assert_int_not_equal(fputc(c ^ 0x5a, f), EOF);
    fclose(f);
}

static void check_damaged_large_object_is_skipped(int keyed)
{
    repo_key_t key;
    if (keyed) {
        assert_int_equal(repo_key_generate(&key), OK);
        store_set_key(store, &key);
    }
    write_big_text(TEST_SRC "/big.txt");
    int64_t snap = run_backup();
    /* Near the end: far past where the hit cap used to stop the read. */
    damage_object_of("big.txt", 100);

    grep_opts_t o = opts_for("needle|swordfish");
    o.max_hits_per_file = 1;
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);

    char *text = hits_text(res.run_id);
    assert_null(strstr(text, "big.txt"));
    assert_non_null(strstr(text, "notes.txt"));
    free(text);
    assert_int_equal(res.files_skipped, 1);
    assert_int_equal(res.hits, 2);
    assert_non_null(strstr(res.skip_reason, "big.txt"));

    char run[32];
    snprintf(run, sizeof(run), "%lld", (long long)res.run_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT files_skipped, skip_reason FROM grep_runs WHERE id = ?",
        (const char *[]){ run, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "1");
    assert_non_null(strstr(r->rows[0][1], "big.txt"));
    db_result_free(r);
    if (keyed) store_set_key(store, NULL);
}

/* A large plaintext object damaged near its end records no match from its
 * first line and is reported skipped, with the reason on the run. */
static void test_damaged_large_object_records_no_matches(void **state)
{
    (void)state;
    check_damaged_large_object_is_skipped(0);
}

/* The same for a tag failure: matches decrypted from tampered ciphertext
 * are not the file's, and were recorded before the tag was ever read. */
static void test_tampered_encrypted_object_records_no_matches(void **state)
{
    (void)state;
    check_damaged_large_object_is_skipped(1);
}

/* An undamaged large object still matches, and is not skipped, though it
 * is now read to the end after its first hit. */
static void test_intact_large_object_still_matches(void **state)
{
    (void)state;
    write_big_text(TEST_SRC "/big.txt");
    int64_t snap = run_backup();
    grep_opts_t o = opts_for("needle");
    o.max_hits_per_file = 1;
    grep_result_t res;
    assert_int_equal(grep_run(store, db, repo_id, snap, &o, NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.hits, 1);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.skip_reason[0], '\0');
    assert_int_equal(run_column(res.run_id, "files_capped"), 1);
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
        cmocka_unit_test_setup_teardown(test_binary_file_is_one_hit_however_often_it_matches, setup, teardown),
        cmocka_unit_test_setup_teardown(test_per_file_cap_stops_reading_the_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_total_hit_cap_truncates_the_search, setup, teardown),
        cmocka_unit_test_setup_teardown(test_duplicate_content_is_scanned_once, setup, teardown),
        cmocka_unit_test_setup_teardown(test_duplicate_content_respects_the_caps, setup, teardown),
        cmocka_unit_test_setup_teardown(test_duplicate_content_stays_in_scope, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_marks_the_run_incomplete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_very_long_line_does_not_buffer_without_bound, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_large_object_records_no_matches, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tampered_encrypted_object_records_no_matches, setup, teardown),
        cmocka_unit_test_setup_teardown(test_intact_large_object_still_matches, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
