#define _POSIX_C_SOURCE 200809L

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
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/error.h>

#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_migrations"
#define TEST_DB  TEST_DIR "/state.db"

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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(
            test_a_failed_migration_reports_its_own_error_and_applies_nothing, setup, teardown),
    };
    return cmocka_run_group_tests_name("migrations", tests, NULL, NULL);
}
