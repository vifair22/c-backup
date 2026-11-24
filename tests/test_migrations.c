#define _XOPEN_SOURCE 700

/*
 * The migration set as AppGuard runs it at start-up (B244).
 *
 * c-utils applies the whole compiled set in one transaction, a savepoint per
 * migration, and records each one in system_migrations inside that same
 * transaction. So a start either applies every pending migration or none,
 * and a database at any recorded prefix of the set resumes from where it
 * stopped. This suite pins both, and that a failing migration is reported
 * by its own error rather than by the rollback that follows it: SQLite had
 * already rolled the transaction back, and the ROLLBACK issued after it
 * failed with "cannot rollback - no transaction is active", which was all
 * the operator saw.
 *
 * It also pins where SQLite's temporary files go: the state database's
 * directory, chosen as the database is opened, so that the migrations,
 * which are the first statements on it, already have somewhere writable
 * on a read-only root (sqlite_tmpdir.h). tests/test_privdrop_e2e.sh and the
 * image check in CI run the daemon itself with no writable /tmp.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/error.h>

#include <sqlite3.h>

#include "sqlite_tmpdir.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_migrations"
#define TEST_DB  TEST_DIR "/state.db"
/* make test runs from the repository root. */
#define STATE_AT_008 "tests/fixtures/state-at-008.sql"

extern const db_migration_t app_migrations[];

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return mkdir(TEST_DIR, 0700);
}

static int teardown(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return 0;
}

static cutils_db_t *open_db(const char *path)
{
    cutils_db_t *h = NULL;
    assert_int_equal(db_open(&h, path), 0);
    assert_int_equal(db_exec_raw(h, "PRAGMA journal_mode=WAL"), 0);
    assert_int_equal(db_run_lib_migrations(h), 0);
    return h;
}

static long long scalar(cutils_db_t *h, const char *sql)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(h, sql, NULL, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    long long v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static long long app_recorded(cutils_db_t *h)
{
    return scalar(h, "SELECT COUNT(*) FROM system_migrations WHERE filename NOT LIKE '\\_lib/%' "
                     "ESCAPE '\\'");
}

/*
 * A migration that makes SQLite roll the whole transaction back by itself,
 * as the temp-file I/O error did in B244: RAISE(ROLLBACK) in a trigger ends
 * the transaction, not just the statement.
 */
static void test_a_failed_migration_reports_its_own_error_and_applies_nothing(void **state)
{
    (void)state;
    static const db_migration_t bad[] = {
        { "b244_first.sql", "CREATE TABLE b244_kept (x INTEGER);" },
        { "b244_second.sql",
          "CREATE TABLE b244_t (x INTEGER);"
          "CREATE TRIGGER b244_trg BEFORE INSERT ON b244_t"
          " BEGIN SELECT RAISE(ROLLBACK, 'b244 cause'); END;"
          "INSERT INTO b244_t VALUES (1);" },
        { NULL, NULL },
    };
    cutils_db_t *h = open_db(TEST_DB);

    assert_int_not_equal(db_run_compiled_migrations(h, bad), 0);
    const char *err = cutils_get_error();
    assert_non_null(strstr(err, "b244_second.sql"));
    assert_non_null(strstr(err, "b244 cause"));
    assert_null(strstr(err, "no transaction is active"));

    /* Nothing of the set survives, the migration before the failing one
     * included, and the handle is usable: no transaction left open. */
    assert_int_equal(app_recorded(h), 0);
    assert_int_equal(scalar(h, "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'b244%'"), 0);
    assert_int_equal(db_run_compiled_migrations(h, app_migrations), 0);
    db_close(h);
}

static int count_app_migrations(void)
{
    int n = 0;
    while (app_migrations[n].name) n++;
    return n;
}

/* The schema as text, for comparing two databases. */
static char *schema_of(cutils_db_t *h)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(h, "SELECT type || ' ' || name || ' ' || COALESCE(sql, '')"
                                   " FROM sqlite_master WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\'"
                                   " ORDER BY type, name", NULL, &r), 0);
    size_t len = 1;
    for (int i = 0; i < r->nrows; i++) len += strlen(r->rows[i][0]) + 1;
    char *out = calloc(1, len);
    assert_non_null(out);
    for (int i = 0; i < r->nrows; i++) {
        strcat(out, r->rows[i][0]);
        strcat(out, "\n");
    }
    db_result_free(r);
    return out;
}

/* The first k migrations of the set, as an older build would have run. */
static db_migration_t *prefix_of(int k)
{
    db_migration_t *p = calloc((size_t)k + 1, sizeof(*p));
    assert_non_null(p);
    for (int i = 0; i < k; i++) p[i] = app_migrations[i];
    return p;
}

static int index_of(const char *name)
{
    for (int i = 0; app_migrations[i].name; i++)
        if (strcmp(app_migrations[i].name, name) == 0) return i;
    fail_msg("no migration %s", name);
    return -1;
}

/*
 * A database a start stopped at after any number of migrations - an older
 * build's, or one whose next start failed - finishes on the next start with
 * exactly the schema a fresh database gets.
 */
static void test_a_database_at_any_point_of_the_set_resumes(void **state)
{
    (void)state;
    int n = count_app_migrations();
    assert_true(n > 1);

    cutils_db_t *h = open_db(TEST_DB);
    assert_int_equal(db_run_compiled_migrations(h, app_migrations), 0);
    char *want = schema_of(h);
    db_close(h);

    for (int k = 0; k < n; k++) {
        rmrf(TEST_DIR);
        assert_int_equal(mkdir(TEST_DIR, 0700), 0);
        db_migration_t *p = prefix_of(k);
        h = open_db(TEST_DB);
        assert_int_equal(db_run_compiled_migrations(h, p), 0);
        assert_int_equal(app_recorded(h), k);
        db_close(h);
        free(p);

        h = open_db(TEST_DB);
        assert_int_equal(db_run_compiled_migrations(h, app_migrations), 0);
        assert_int_equal(app_recorded(h), n);
        char *got = schema_of(h);
        if (strcmp(want, got) != 0)
            fail_msg("resumed after %d migration(s), the schema differs:\n%s\nwant:\n%s",
                     k, got, want);
        free(got);
        db_close(h);
    }
    free(want);
}

static char *slurp(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) fail_msg("cannot open %s", path);
    char *buf = NULL;
    size_t cap = 0, len = 0, got;
    do {
        if (len + 4096 + 1 > cap) {
            cap = cap ? cap * 2 : 65536;
            buf = realloc(buf, cap);
            assert_non_null(buf);
        }
        got = fread(buf + len, 1, cap - len - 1, f);
        len += got;
    } while (got > 0);
    fclose(f);
    buf[len] = 0;
    return buf;
}

/*
 * FS1's upgrade: a database at 008 holding several repositories' journals,
 * whose per-repository ids repeat, through 009 (the journal rebuild) and
 * the rest. Every entry is kept, in order; a repository keeps its ids unless
 * a lower repository used them too, and the others are renumbered above the
 * highest id.
 */
static void test_an_upgrade_from_008_keeps_every_journal_entry(void **state)
{
    (void)state;
    /* The same fixture the privilege-drop e2e and the image check start the
     * daemon on, so this also proves it is a database at 008. */
    cutils_db_t *h = NULL;
    assert_int_equal(db_open(&h, TEST_DB), 0);
    char *sql = slurp(STATE_AT_008);
    assert_int_equal(db_exec_raw(h, sql), 0);
    free(sql);
    assert_int_equal(app_recorded(h), index_of("008_id_reserve.sql") + 1);
    db_close(h);

    h = open_db(TEST_DB);
    assert_int_equal(db_run_compiled_migrations(h, app_migrations), 0);
    assert_int_equal(app_recorded(h), count_app_migrations());

    db_result_t *r = NULL;
    assert_int_equal(db_execute(h, "SELECT repo_id, id, message FROM journal ORDER BY id",
                                NULL, &r), 0);
    static const char *want[][3] = {
        { "1", "1", "r1 a" }, { "1", "2", "r1 b" }, { "1", "3", "r1 c" },
        { "3", "10", "r3 a" }, { "3", "11", "r3 b" },
        { "2", "12", "r2 a" }, { "2", "13", "r2 b" },
    };
    assert_int_equal(r->nrows, 7);
    for (int i = 0; i < 7; i++)
        for (int c = 0; c < 3; c++)
            assert_string_equal(r->rows[i][c], want[i][c]);
    db_result_free(r);

    /* AUTOINCREMENT from here: the next entry is above every one kept. */
    r = NULL;
    assert_int_equal(db_execute(h, "INSERT INTO journal (repo_id, ts, source, operation, result)"
                                   " VALUES (1, 600, 'api', 'backup', 'success') RETURNING id",
                                NULL, &r), 0);
    assert_int_equal(strtoll(r->rows[0][0], NULL, 10), 14);
    db_result_free(r);
    db_close(h);
}

/*
 * 016 moves the content hash lookups from nodes_by_hash into node_hashes:
 * every file row with content, of every snapshot, gets its row, and nothing
 * else does (directories carry subtree_hash, symlinks and empty files none).
 */
static void test_016_carries_every_file_hash_over(void **state)
{
    (void)state;
    int k = index_of("016_node_hashes.sql");
    db_migration_t *p = prefix_of(k);
    cutils_db_t *h = open_db(TEST_DB);
    assert_int_equal(db_run_compiled_migrations(h, p), 0);
    free(p);
    assert_int_equal(db_exec_raw(h,
        "INSERT INTO repos (id, uuid, path, added_at) VALUES (1, 'u', '/r', 0);"
        "INSERT INTO nodes (repo_id, id, snap_id, parent_node_id, name, type, mode,"
        "  uid, gid, mtime_ns, size, content_hash, subtree_hash) VALUES"
        " (1, 1, 1, 0, 'd', 2, 16877, 0, 0, 0, 0, NULL, 'aa'),"
        " (1, 2, 1, 1, 'f', 1, 33188, 0, 0, 0, 3, 'bb', NULL),"
        " (1, 3, 1, 1, 'e', 1, 33188, 0, 0, 0, 0, NULL, NULL),"
        " (1, 4, 1, 1, 'l', 3, 41471, 0, 0, 0, 0, NULL, NULL),"
        " (1, 5, 2, 0, 'd', 2, 16877, 0, 0, 0, 0, NULL, 'aa'),"
        " (1, 6, 2, 5, 'f', 1, 33188, 0, 0, 0, 3, 'bb', NULL),"
        " (1, 7, 2, 5, 'g', 1, 33188, 0, 0, 0, 3, 'cc', NULL);"), 0);
    assert_int_equal(db_run_compiled_migrations(h, app_migrations), 0);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(h, "SELECT snap_id, content_hash, node_id FROM node_hashes"
                                   " ORDER BY snap_id, content_hash, node_id", NULL, &r), 0);
    static const char *want[][3] = { { "1", "bb", "2" }, { "2", "bb", "6" }, { "2", "cc", "7" } };
    assert_int_equal(r->nrows, 3);
    for (int i = 0; i < 3; i++)
        for (int c = 0; c < 3; c++)
            assert_string_equal(r->rows[i][c], want[i][c]);
    db_result_free(r);
    assert_int_equal(scalar(h, "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'nodes_by_hash'"), 0);
    /* The content set: each distinct hash once. */
    assert_int_equal(scalar(h, "SELECT COUNT(*) FROM node_content"), 2);
    assert_int_equal(scalar(h, "SELECT COUNT(*) FROM node_content WHERE content_hash IN ('bb', 'cc')"), 2);
    db_close(h);
}

/* What the daemon does before AppGuard opens state.db. */
static void reset_tmpdir(void)
{
    sqlite_tmpdir_uninstall();
    sqlite3_free(sqlite3_temp_directory);
    sqlite3_temp_directory = NULL;
}

static void test_temporary_files_go_beside_the_database_from_its_first_statement(void **state)
{
    (void)state;
    reset_tmpdir();
    unsetenv("SQLITE_TMPDIR");
    assert_int_equal(sqlite_tmpdir_install(), 0);

    /* Set by the open itself, before any statement: the migrations that
     * follow already spill there. */
    cutils_db_t *h = NULL;
    assert_int_equal(db_open(&h, TEST_DB), 0);
    char real[PATH_MAX];
    assert_non_null(realpath(TEST_DIR, real));
    assert_non_null(sqlite3_temp_directory);
    assert_string_equal(sqlite3_temp_directory, real);
    assert_ptr_equal(sqlite_tmpdir_chosen(), sqlite3_temp_directory);

    /* A later database does not move it: there is one location per
     * process, and the first database opened is the state database. */
    cutils_db_t *other = NULL;
    assert_int_equal(mkdir(TEST_DIR "/other", 0700), 0);
    assert_int_equal(db_open(&other, TEST_DIR "/other/x.db"), 0);
    assert_string_equal(sqlite3_temp_directory, real);
    db_close(other);
    db_close(h);
    reset_tmpdir();
}

static void test_sqlite_tmpdir_set_by_the_operator_wins(void **state)
{
    (void)state;
    reset_tmpdir();
    setenv("SQLITE_TMPDIR", TEST_DIR, 1);
    assert_int_equal(sqlite_tmpdir_install(), 0);
    cutils_db_t *h = NULL;
    assert_int_equal(mkdir(TEST_DIR "/s", 0700), 0);
    assert_int_equal(db_open(&h, TEST_DIR "/s/state.db"), 0);
    assert_null(sqlite3_temp_directory);
    assert_null(sqlite_tmpdir_chosen());
    db_close(h);
    unsetenv("SQLITE_TMPDIR");
    reset_tmpdir();
}

static void test_a_memory_database_chooses_nothing(void **state)
{
    (void)state;
    reset_tmpdir();
    unsetenv("SQLITE_TMPDIR");
    assert_int_equal(sqlite_tmpdir_install(), 0);
    cutils_db_t *h = NULL;
    assert_int_equal(db_open(&h, ":memory:"), 0);
    assert_null(sqlite3_temp_directory);
    db_close(h);
    reset_tmpdir();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(
            test_a_failed_migration_reports_its_own_error_and_applies_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_database_at_any_point_of_the_set_resumes,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_016_carries_every_file_hash_over, setup, teardown),
        cmocka_unit_test_setup_teardown(test_an_upgrade_from_008_keeps_every_journal_entry,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_temporary_files_go_beside_the_database_from_its_first_statement,
            setup, teardown),
        cmocka_unit_test_setup_teardown(test_sqlite_tmpdir_set_by_the_operator_wins,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_memory_database_chooses_nothing, setup, teardown),
    };
    return cmocka_run_group_tests_name("migrations", tests, NULL, NULL);
}
