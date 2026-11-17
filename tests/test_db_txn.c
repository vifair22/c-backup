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
#include "db_write.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_dbtxn"
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

/* Wait, bounded at 10 s, until another thread is queued in db_txn_begin.
 * A broken count fails the test rather than hanging it. */
static void wait_for_waiter(void)
{
    int seen = 0;
    for (int i = 0; i < 5000 && !seen; i++) {
        if (db_txn_waiters() >= 1) { seen = 1; break; }
        struct timespec ts = { 0, 2 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    assert_true(seen);
}

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
     * Wait until the contender is queued in its begin. Waiting on
     * contender_done instead would deadlock, since it cannot return until
     * this transaction commits; a fixed sleep raced on a loaded runner
     * (FLAKE-15), so the wait is on the queue itself.
     */
    wait_for_waiter();

    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(pthread_join(t, NULL), 0);

    /* The contender must have got its transaction, not "cannot begin". */
    assert_int_equal(h.contender_status, OK);

    pthread_mutex_destroy(&h.mu);
    pthread_cond_destroy(&h.cv);
}

/*
 * B107: a writer already waiting goes before the holder's next transaction.
 *
 * A backup commits one batch and begins the next within microseconds. With a
 * bare mutex the waiting writer was woken but the backup thread usually took
 * the lock back first, so a sources write could lose to batch after batch.
 * Here the holder commits and immediately begins again; the contender, queued
 * well before that commit, must have finished its own transaction by the
 * time the holder's second begin returns.
 */
static void test_a_waiting_writer_goes_first(void **state)
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

    /* Until the contender is queued in its begin (FLAKE-15: a fixed sleep
     * was not always long enough on a loaded runner). */
    wait_for_waiter();

    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(db_txn_begin(db), OK);
    pthread_mutex_lock(&h.mu);
    int done = h.contender_done;
    pthread_mutex_unlock(&h.mu);
    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(pthread_join(t, NULL), 0);

    assert_true(done);
    assert_int_equal(h.contender_status, OK);

    pthread_mutex_destroy(&h.mu);
    pthread_cond_destroy(&h.cv);
}

/*
 * OWN-16: a holder can tell whether anybody is queued behind it.
 *
 * The backup holds its batch for as long as nobody else wants the lock and
 * lets it go as soon as somebody does, so db_txn_waiters has to read zero
 * with nobody queued - or every check is a commit, which is B107's cost -
 * and non-zero once a writer is blocked in its begin, or the writer waits
 * for the whole batch.
 */
static void test_waiters_counts_queued_writers(void **state)
{
    (void)state;

    assert_int_equal(db_txn_waiters(), 0);
    assert_int_equal(db_txn_begin(db), OK);
    /* Holding is not waiting. */
    assert_int_equal(db_txn_waiters(), 0);

    struct handoff h;
    memset(&h, 0, sizeof(h));
    pthread_mutex_init(&h.mu, NULL);
    pthread_cond_init(&h.cv, NULL);
    h.contender_status = ERR_IO;
    h.holder_inside = 1;

    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, contender_main, &h), 0);

    /* Until the contender is queued; bounded so a broken count fails the
     * test rather than hanging it. */
    int seen = 0;
    for (int i = 0; i < 500 && !seen; i++) {
        if (db_txn_waiters() == 1) { seen = 1; break; }
        struct timespec ts = { 0, 2 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    assert_true(seen);

    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(pthread_join(t, NULL), 0);
    assert_int_equal(h.contender_status, OK);
    /* Served and gone. */
    assert_int_equal(db_txn_waiters(), 0);

    pthread_mutex_destroy(&h.mu);
    pthread_cond_destroy(&h.cv);
}

/*
 * A second begin on the thread that holds the lock fails instead of queuing
 * behind its own ticket for ever. The backup holds its batch across more
 * than it did (OWN-16), so a seal or any other transaction begun from inside
 * one is a hang one refactor away; this turns it into an error.
 */
static void test_a_nested_begin_fails_instead_of_hanging(void **state)
{
    (void)state;

    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_txn_begin(db), ERR_BUSY);
    /* The failed attempt left the outer transaction and the lock as they
     * were: commit works, and the lock is free afterwards. */
    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_txn_commit(db), OK);
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

/*
 * B142: making the index durable is a full checkpoint of the WAL, so after
 * it the committed rows are in the database file itself, synced, and the
 * WAL holds nothing a power cut could take back. It needs the write lock
 * and refuses to run inside a transaction rather than deadlock or
 * checkpoint half a transaction.
 */
static void test_make_durable_checkpoints_everything(void **state)
{
    (void)state;
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/x', 'x', 'u-x', 1)", NULL, NULL), 0);
    assert_int_equal(db_txn_make_durable(db), ERR_BUSY);
    err_clear();
    assert_int_equal(db_txn_commit(db), OK);

    assert_int_equal(db_txn_make_durable(db), OK);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "PRAGMA wal_checkpoint(PASSIVE)", NULL, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    /* Nothing left in the WAL to copy: everything was checkpointed. */
    assert_string_equal(r->rows[0][0], "0");
    assert_string_equal(r->rows[0][1], r->rows[0][2]);
    db_result_free(r);
    /* And the lock was released. */
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_txn_commit(db), OK);
}

/* ------------------------------------------------------------------ */
/* B157: writes outside a transaction                                  */
/* ------------------------------------------------------------------ */

static long count_rows(const char *sql)
{
    db_result_t *r = NULL;
    long n = -1;
    if (db_execute(db, sql, NULL, &r) == 0 && r && r->nrows == 1)
        n = strtol(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

static void seed_staged(int n)
{
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/r', 'r', 'u-r', 1)",
        NULL, NULL), 0);
    for (int i = 0; i < n; i++) {
        char h[16];
        snprintf(h, sizeof(h), "s%04d", i);
        assert_int_equal(db_execute_non_query(db,
            "INSERT INTO objects (repo_id, hash, location, type) VALUES (1, ?, 'staged', 1)",
            (const char *[]){ h, NULL }, NULL), 0);
    }
    /* And one the drop must leave alone. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO objects (repo_id, hash, location, type) VALUES (1, 'packed', '7', 1)",
        NULL, NULL), 0);
}

struct dropper {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             done;
    status_t        st;
};

static void *dropper_main(void *arg)
{
    struct dropper *d = arg;
    status_t st = store_index_drop_staged(db, 1);
    pthread_mutex_lock(&d->mu);
    d->st = st;
    d->done = 1;
    pthread_cond_broadcast(&d->cv);
    pthread_mutex_unlock(&d->mu);
    return NULL;
}

/*
 * The bug, forced: a backup drops staged rows while another thread holds a
 * db_txn transaction that it then rolls back.
 *
 * store_index_drop_staged issued a plain DELETE. On the one connection every
 * thread shares, that statement ran inside the holder's open transaction, so
 * it returned at once, reported success, and was undone by the holder's
 * rollback: the staged rows came back, naming bytes in a pack that was
 * discarded. Through db_write it queues for the lock instead, runs after the
 * rollback in a transaction of its own, and its effect is all there.
 *
 * Three things are asserted, each of which the old code fails: the drop
 * waited (it is seen queued behind the holder), the holder saw nothing of it
 * while it waited (not half-applied into someone else's transaction), and
 * after the rollback every staged row is gone (not lost) while the holder's
 * own rolled-back row did not survive on the drop's commit.
 */
static void test_drop_staged_waits_for_a_foreign_transaction(void **state)
{
    (void)state;
    seed_staged(50);

    struct dropper d;
    memset(&d, 0, sizeof(d));
    pthread_mutex_init(&d.mu, NULL);
    pthread_cond_init(&d.cv, NULL);
    d.st = ERR_IO;

    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO objects (repo_id, hash, location, type) VALUES (1, 'holder', '9', 1)",
        NULL, NULL), 0);

    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, dropper_main, &d), 0);

    /* It waited: the old DELETE never queued, it ran in this transaction. */
    wait_for_waiter();
    pthread_mutex_lock(&d.mu);
    int early = d.done;
    pthread_mutex_unlock(&d.mu);
    assert_false(early);
    /* Nothing of it is in this transaction. */
    assert_int_equal(count_rows("SELECT COUNT(*) FROM objects WHERE location = 'staged'"), 50);

    db_txn_rollback(db);
    assert_int_equal(pthread_join(t, NULL), 0);
    assert_int_equal(d.st, OK);

    /* Not lost to the rollback, and complete. */
    assert_int_equal(count_rows("SELECT COUNT(*) FROM objects WHERE location = 'staged'"), 0);
    assert_int_equal(count_rows("SELECT COUNT(*) FROM objects WHERE hash = 'packed'"), 1);
    /* The holder's row went with its rollback; the drop's commit did not
     * carry it. */
    assert_int_equal(count_rows("SELECT COUNT(*) FROM objects WHERE hash = 'holder'"), 0);

    pthread_mutex_destroy(&d.mu);
    pthread_cond_destroy(&d.cv);
}

/*
 * A thread that holds the lock asked for its own transaction: db_write
 * there runs inside it, and rolls back with it. Without that, on_progress
 * from inside a backup batch, or journal_record from inside a prune, would
 * try a nested begin and fail.
 */
static void test_db_write_joins_the_callers_transaction(void **state)
{
    (void)state;
    assert_false(db_txn_held());
    assert_int_equal(db_txn_begin(db), OK);
    assert_true(db_txn_held());
    assert_int_equal(db_write(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/j', 'j', 'u-j', 1)",
        NULL, NULL), 0);
    db_result_t *r = NULL;
    assert_int_equal(db_write_returning(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/k', 'k', 'u-k', 1)"
        " RETURNING id", NULL, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    db_result_free(r);
    db_txn_rollback(db);
    assert_false(db_txn_held());
    assert_int_equal(count_rows("SELECT COUNT(*) FROM repos"), 0);

    /* On its own it commits, and leaves the lock free. */
    assert_int_equal(db_write(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/j', 'j', 'u-j', 1)",
        NULL, NULL), 0);
    assert_false(db_txn_held());
    assert_int_equal(count_rows("SELECT COUNT(*) FROM repos"), 1);
    /* A failing statement fails the call and rolls back its transaction. */
    assert_int_not_equal(db_write(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/j', 'j', 'u-j', 1)",
        NULL, NULL), 0);
    assert_false(db_txn_held());
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_txn_commit(db), OK);
}

struct batch_contender {
    pthread_mutex_t mu;
    int             done;
};

static void *batch_contender_main(void *arg)
{
    struct batch_contender *c = arg;
    int rc = db_write(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/c', 'c', 'u-c', 1)",
        NULL, NULL);
    pthread_mutex_lock(&c->mu);
    c->done = rc == 0 ? 1 : -1;
    pthread_mutex_unlock(&c->mu);
    return NULL;
}

/*
 * A batched loop lets the lock go at its next step once a writer queues,
 * so a sweep of many rows delays a route by one statement, not by the
 * whole sweep.
 */
static void test_a_write_batch_yields_to_a_waiter(void **state)
{
    (void)state;
    db_write_batch_t wb;
    assert_int_equal(db_write_batch_open(&wb, db), 0);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/b', 'b', 'u-b', 1)",
        NULL, NULL), 0);

    struct batch_contender c = { PTHREAD_MUTEX_INITIALIZER, 0 };
    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, batch_contender_main, &c), 0);
    wait_for_waiter();

    /* Commits, and the reopen queues behind the waiter: the waiter's row is
     * committed by the time the batch has its lock back. (Its done flag is
     * set after it lets go, so the row is what is asked, not the flag.) */
    assert_int_equal(db_write_batch_next(&wb), 0);
    assert_int_equal(count_rows("SELECT COUNT(*) FROM repos WHERE path = '/c'"), 1);
    assert_int_equal(db_write_batch_close(&wb, 1), 0);
    assert_int_equal(pthread_join(t, NULL), 0);
    assert_int_equal(c.done, 1);
    assert_int_equal(count_rows("SELECT COUNT(*) FROM repos"), 2);
    assert_false(db_txn_held());
}

/*
 * The guard: once armed, a row changed by a thread that does not hold the
 * write lock is counted (and in the end-to-end suites, aborts the daemon).
 * One made through db_write or inside db_txn is not, and nothing is
 * counted while the guard is not armed, which is start-up.
 */
static void test_guard_reports_a_write_outside_the_lock(void **state)
{
    (void)state;
    unsigned long v0 = db_txn_guard_violations();

    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/1', '1', 'u-1', 1)",
        NULL, NULL), 0);
    assert_int_equal(db_txn_guard_violations(), v0);

    db_txn_guard_arm(1);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/2', '2', 'u-2', 1)",
        NULL, NULL), 0);
    assert_int_equal(db_txn_guard_violations(), v0 + 1);

    assert_int_equal(db_write(db,
        "UPDATE repos SET label = 'x'", NULL, NULL), 0);
    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_execute_non_query(db,
        "DELETE FROM repos WHERE path = '/1'", NULL, NULL), 0);
    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(store_index_drop_staged(db, 1), OK);
    assert_int_equal(db_txn_guard_violations(), v0 + 1);
    db_txn_guard_arm(0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_a_second_writer_waits_instead_of_failing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_waiting_writer_goes_first, setup, teardown),
        cmocka_unit_test_setup_teardown(test_waiters_counts_queued_writers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_nested_begin_fails_instead_of_hanging, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rollback_releases_the_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repeated_cycles_do_not_leak_the_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_make_durable_checkpoints_everything, setup, teardown),
        cmocka_unit_test_setup_teardown(test_drop_staged_waits_for_a_foreign_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_write_joins_the_callers_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_write_batch_yields_to_a_waiter, setup, teardown),
        cmocka_unit_test_setup_teardown(test_guard_reports_a_write_outside_the_lock, setup, teardown),
    };
    /* Before any test opens its database, so every connection is hooked. */
    db_txn_guard_install();
    return cmocka_run_group_tests(tests, NULL, NULL);
}
