#define _POSIX_C_SOURCE 200809L

/*
 * Reserved id marks (B217, src/common/id_reserve.h).
 *
 * A power cut takes back commits made in the last second or so, and with
 * them sqlite_sequence's high-water mark, so a job id a client already had
 * was issued again after the restart. tests/test_powercut_fault.c runs that
 * through the disk model; this suite pins the parts it cannot reach cheaply:
 * a clean stop skips no ids, an unclean one skips past whatever was handed
 * out, the sync comes once per half block and before the id is returned, a
 * failed sync hands out no id, and other databases and rolled-back
 * transactions leave the marks alone.
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

#include "db_txn.h"
#include "db_write.h"
#include "id_reserve.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_id_reserve"
#define TEST_DB  TEST_DIR "/state.db"
#define OTHER_DB TEST_DIR "/other.db"
#define BLOCK    8

extern const db_migration_t app_migrations[];

static cutils_db_t *db;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static int open_migrated(const char *path, cutils_db_t **out)
{
    if (db_open(out, path) != 0) return -1;
    if (db_exec_raw(*out, "PRAGMA journal_mode=WAL") != 0) return -1;
    if (db_run_lib_migrations(*out) != 0) return -1;
    return db_run_compiled_migrations(*out, app_migrations);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    return open_migrated(TEST_DB, &db);
}

static int teardown(void **state)
{
    (void)state;
    id_reserve_forget();
    db_txn_set_durable_hooks(NULL, NULL);
    db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
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

/* A job row, as job_submit writes it; its id, or 0 when the write failed. */
static int64_t new_job(cutils_db_t *h)
{
    db_result_t *r = NULL;
    int64_t id = 0;
    if (db_write_returning(h, "INSERT INTO jobs (action, state, origin)"
                              " VALUES ('t', 'queued', 'api') RETURNING id",
                           NULL, &r) == 0 && r && r->nrows == 1)
        id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return id;
}

/* What a power cut leaves: the newest rows and the sequence's last steps
 * gone, the marks as they were last made durable. */
static void lose_back_to(int64_t keep)
{
    char sql[160];
    snprintf(sql, sizeof(sql), "DELETE FROM jobs WHERE id > %lld", (long long)keep);
    assert_int_equal(db_write(db, sql, NULL, NULL), 0);
    snprintf(sql, sizeof(sql), "UPDATE sqlite_sequence SET seq = %lld WHERE name = 'jobs'",
             (long long)keep);
    assert_int_equal(db_write(db, sql, NULL, NULL), 0);
}

/* Every table whose id leaves the daemon is covered, found from the
 * schema, not from a list here or there. */
static void test_every_autoincrement_table_is_covered(void **state)
{
    (void)state;
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    static const char *const tables[] = { "repos", "schedules", "jobs", "verify_runs",
                                          "grep_runs", "exports", "unit_intents" };
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++)
        assert_int_equal(id_reserve_durable(tables[i]), BLOCK);
    assert_int_equal(id_reserve_durable("snapshots"), 0);
    assert_int_equal(scalar(db, "SELECT COUNT(*) FROM id_reserve"),
                     (long long)(sizeof(tables) / sizeof(tables[0])));
}

/* Every id returned is under a durable mark, and the sync that keeps it
 * so comes once per half block, not per insert. */
static void test_ids_stay_under_the_durable_mark(void **state)
{
    (void)state;
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    unsigned long d0 = db_txn_durable_count();
    for (int i = 1; i <= 40; i++) {
        int64_t id = new_job(db);
        assert_int_equal(id, i);
        assert_true(id_reserve_durable("jobs") >= id);
        assert_true(id_reserve_durable("jobs") >= id + BLOCK / 2);
    }
    unsigned long syncs = db_txn_durable_count() - d0;
    print_message("40 ids, %lu syncs\n", syncs);
    assert_true(syncs >= 40 / BLOCK);
    assert_true(syncs <= 40 / (BLOCK / 2) + 1);
}

/* A clean stop brings the marks down, so a clean restart skips nothing. */
static void test_a_clean_restart_skips_nothing(void **state)
{
    (void)state;
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    for (int i = 0; i < 5; i++) assert_true(new_job(db) > 0);
    id_reserve_close(db);
    assert_int_equal(scalar(db, "SELECT high FROM id_reserve WHERE tbl = 'jobs'"), 5);
    assert_int_equal(id_reserve_durable("jobs"), 0);
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    assert_int_equal(new_job(db), 6);
}

/* An unclean one: whatever the cut took back, the next id is above every
 * id handed out before it. */
static void test_an_unclean_restart_skips_past_what_was_handed_out(void **state)
{
    (void)state;
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    int64_t last = 0;
    for (int i = 0; i < 11; i++) last = new_job(db);
    assert_int_equal(last, 11);
    id_reserve_forget();
    lose_back_to(3);
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    int64_t next = new_job(db);
    print_message("handed out up to %lld, next after the cut %lld\n", (long long)last,
                  (long long)next);
    assert_true(next > last);
    assert_true(next <= last + BLOCK + 1);
    /* And without the marks it would have been 4. */
}

/* A sync that fails hands out no id: the commit reports failure, so
 * job_submit answers nobody with it. Once syncs work again, ids resume
 * above it. */
static int g_fail_syncs;
static status_t gate(void)
{
    return g_fail_syncs ? set_error(ERR_IO, "sync refused") : OK;
}

static void test_a_failed_sync_hands_out_no_id(void **state)
{
    (void)state;
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    int64_t mark = id_reserve_durable("jobs");
    db_txn_set_durable_hooks(gate, NULL);
    g_fail_syncs = 1;
    /* Under the durable mark nothing needs a sync, and those ids are safe;
     * every id above it is refused, though its row is committed. */
    int issued = 0, refused = 0;
    for (int i = 0; i < 3 * BLOCK; i++) {
        int64_t id = new_job(db);
        if (id > 0) {
            assert_true(id <= mark);
            issued++;
        } else {
            assert_non_null(strstr(err_msg(), "reservation is not durable"));
            refused++;
        }
        err_clear();
    }
    print_message("with syncs failing: %d ids issued under mark %lld, %d refused\n",
                  issued, (long long)mark, refused);
    assert_int_equal(issued, (int)mark);
    assert_int_equal(refused, 3 * BLOCK - (int)mark);
    assert_int_equal(id_reserve_durable("jobs"), mark);

    g_fail_syncs = 0;
    int64_t next = new_job(db);
    assert_int_equal(next, 3 * BLOCK + 1);
    assert_true(id_reserve_durable("jobs") >= next);
    db_txn_set_durable_hooks(NULL, NULL);
}

/* A transaction rolled back hands nothing out and writes no mark; another
 * database's inserts are not this one's ids. */
static void test_rollbacks_and_other_databases_leave_the_marks_alone(void **state)
{
    (void)state;
    assert_int_equal(id_reserve_open(db, BLOCK), OK);
    long long mark = scalar(db, "SELECT high FROM id_reserve WHERE tbl = 'jobs'");
    for (int i = 0; i < 3 * BLOCK; i++) {
        assert_int_equal(db_txn_begin(db), OK);
        db_result_t *r = NULL;
        assert_int_equal(db_write_returning(db, "INSERT INTO jobs (action, state, origin)"
                                                " VALUES ('t', 'queued', 'api') RETURNING id",
                                            NULL, &r), 0);
        db_result_free(r);
        db_txn_rollback(db);
    }
    assert_int_equal(scalar(db, "SELECT high FROM id_reserve WHERE tbl = 'jobs'"), mark);

    cutils_db_t *other = NULL;
    assert_int_equal(open_migrated(OTHER_DB, &other), 0);
    unsigned long d0 = db_txn_durable_count();
    for (int i = 0; i < 3 * BLOCK; i++) assert_true(new_job(other) > 0);
    assert_int_equal(db_txn_durable_count(), d0);
    assert_int_equal(scalar(other, "SELECT COUNT(*) FROM id_reserve"), 0);
    db_close(other);
    assert_int_equal(scalar(db, "SELECT high FROM id_reserve WHERE tbl = 'jobs'"), mark);
    assert_int_equal(id_reserve_durable("jobs"), mark);
}

int main(void)
{
    /* The update hook that sees each insert, on every connection, as the
     * daemon installs it. */
    db_txn_guard_install();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_every_autoincrement_table_is_covered, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ids_stay_under_the_durable_mark, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_clean_restart_skips_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_an_unclean_restart_skips_past_what_was_handed_out,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_failed_sync_hands_out_no_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rollbacks_and_other_databases_leave_the_marks_alone,
                                        setup, teardown),
    };
    return cmocka_run_group_tests_name("id_reserve", tests, NULL, NULL);
}
