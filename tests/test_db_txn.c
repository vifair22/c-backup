#define _POSIX_C_SOURCE 200809L

/*
 * One writer at a time, without the second one failing.
 *
 * A transaction is a process-wide resource: one c-utils handle serves the whole
 * daemon and SQLite has no nested transactions, so a BEGIN issued while another
 * is open fails outright. Nine sites across backup, gc, prune, reindex,
 * store_index and the metadata routes begin transactions, and only one of them
 * used to take a lock first.
 *
 * The user-visible symptom was a PUT /repos/<id>/sources during a running
 * backup returning 500 {"error":"cannot begin"}, intermittently - depending on
 * whether a batch happened to be open at that instant. Tags, notes and policy
 * shared the exposure.
 *
 * These tests are written so the interleaving is forced rather than raced for.
 * A test that merely runs two writers and hopes they collide passes on a good
 * day whether or not the bug is present, which is how this survived as long as
 * it did.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "db_txn.h"

#define TEST_DIR "/tmp/c_backup_test_dbtxn"
#define TEST_DB  TEST_DIR "/state.db"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;

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
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* ------------------------------------------------------------------ */

/*
 * The forced interleaving: the second writer begins while the first is
 * demonstrably still inside its transaction.
 *
 * The holder signals once it is inside and does not commit until the contender
 * has reported that its own begin returned. Without the lock the contender's
 * BEGIN fails immediately and the recorded status is an error; with it the
 * contender blocks until the holder commits and then succeeds. Either way the
 * test completes, so a failure here is a real result and not a hang.
 */
struct handoff {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             holder_inside;   /* holder has begun */
    int             contender_done;  /* contender's begin has returned */
    status_t        contender_status;
};

static void *contender_main(void *arg)
{
    struct handoff *h = arg;

    pthread_mutex_lock(&h->mu);
    while (!h->holder_inside)
        pthread_cond_wait(&h->cv, &h->mu);
    pthread_mutex_unlock(&h->mu);

    /* This is the PUT /sources arriving mid-backup. */
    status_t st = db_txn_begin(db);
    if (st == OK) {
        db_execute_non_query(db,
            "INSERT INTO config (key, value) VALUES ('contender', '1')"
            " ON CONFLICT(key) DO UPDATE SET value = '1'", NULL, NULL);
        st = db_txn_commit(db);
    }

    pthread_mutex_lock(&h->mu);
    h->contender_status = st;
    h->contender_done = 1;
    pthread_cond_broadcast(&h->cv);
    pthread_mutex_unlock(&h->mu);
    return NULL;
}

static void test_a_second_writer_waits_instead_of_failing(void **state)
{
    (void)state;

    struct handoff h;
    memset(&h, 0, sizeof(h));
    pthread_mutex_init(&h.mu, NULL);
    pthread_cond_init(&h.cv, NULL);
    h.contender_status = ERR_IO;

    assert_int_equal(db_txn_begin(db), OK);

    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, contender_main, &h), 0);

    pthread_mutex_lock(&h.mu);
    h.holder_inside = 1;
    pthread_cond_broadcast(&h.cv);
    pthread_mutex_unlock(&h.mu);

    /*
     * Give the contender long enough to have reached its begin and, if the
     * lock is absent, to have failed and recorded that. Waiting on
     * contender_done instead would deadlock in the fixed build, where it
     * cannot return until this transaction commits - so the wait is on the
     * clock, and what the test asserts is the status it eventually reports.
     */
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 200 * 1000 * 1000 };
    nanosleep(&ts, NULL);

    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(pthread_join(t, NULL), 0);

    /* The contender must have got its transaction, not "cannot begin". */
    assert_int_equal(h.contender_status, OK);

    pthread_mutex_destroy(&h.mu);
    pthread_cond_destroy(&h.cv);
}

/*
 * A rollback has to release the lock too, or one failed write wedges every
 * writer for the life of the process. This is the path most easily got wrong,
 * because it is the one no successful run exercises.
 */
static void test_rollback_releases_the_lock(void **state)
{
    (void)state;

    assert_int_equal(db_txn_begin(db), OK);
    db_txn_rollback(db);

    /* If the rollback leaked the lock this second begin never returns; the
     * suite would hang rather than fail, which is itself the signal. */
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_txn_commit(db), OK);
}

/*
 * Sequential begin/commit cycles must not leak either. Cheap to assert and it
 * catches an unbalanced unlock, which on most pthread implementations is
 * undefined rather than loud.
 */
static void test_repeated_cycles_do_not_leak_the_lock(void **state)
{
    (void)state;

    for (int i = 0; i < 64; i++) {
        assert_int_equal(db_txn_begin(db), OK);
        assert_int_equal(db_txn_commit(db), OK);
    }
    assert_int_equal(db_txn_begin(db), OK);
    db_txn_rollback(db);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_a_second_writer_waits_instead_of_failing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rollback_releases_the_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repeated_cycles_do_not_leak_the_lock, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
