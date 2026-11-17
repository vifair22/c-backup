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
#include <stdatomic.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/log.h>

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

typedef struct {
    atomic_int done;
    int        rc;
} worker_t;

static void *durable_worker(void *arg)
{
    worker_t *w = arg;
    w->rc = db_txn_make_durable(db);
    atomic_store(&w->done, 1);
    return NULL;
}

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
    /* Its row, not its done flag: the contender sets the flag after its
     * commit has released the lock, so this begin can win the gap and see
     * the flag still clear although the contender did go first. The row
     * is in the database before that release, so seeing it here proves the
     * order. */
    db_result_t *r = NULL;
    int rc = db_execute(db, "SELECT value FROM config WHERE key = 'contender'", NULL, &r);
    int went_first = rc == 0 && r && r->nrows == 1 && strcmp(r->rows[0][0], "1") == 0;
    db_result_free(r);
    assert_int_equal(db_txn_commit(db), OK);
    assert_int_equal(pthread_join(t, NULL), 0);

    assert_true(went_first);
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
 * B142, B144: making the index durable syncs the WAL, the file every
 * commit so far has written its frames into. It does not checkpoint
 * (copying the WAL into the database is not needed for durability and was
 * seconds of every writer's time under the write lock), and refuses to run
 * inside a transaction, whose commit has not happened yet.
 */
static void test_make_durable_syncs_the_wal(void **state)
{
    (void)state;
    char wal[512];
    assert_int_equal(db_txn_wal_path(db, wal, sizeof(wal)), 0);
    assert_string_equal(wal, TEST_DB "-wal");

    assert_int_equal(db_txn_begin(db), OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/x', 'x', 'u-x', 1)", NULL, NULL), 0);
    assert_int_equal(db_txn_make_durable(db), ERR_BUSY);
    err_clear();
    assert_int_equal(db_txn_commit(db), OK);

    struct stat before, after;
    assert_int_equal(stat(TEST_DB, &before), 0);
    unsigned long n = db_txn_durable_count();
    assert_int_equal(db_txn_make_durable(db), OK);
    assert_int_equal(db_txn_durable_count(), n + 1);
    assert_int_equal(stat(TEST_DB, &after), 0);
    /* Nothing was copied into the database file: the commit is durable
     * where it is, in the WAL. */
    assert_true(before.st_size == after.st_size &&
                before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
                before.st_mtim.tv_nsec == after.st_mtim.tv_nsec);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "PRAGMA wal_checkpoint(PASSIVE)", NULL, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    assert_true(strtol(r->rows[0][1], NULL, 10) > 0);    /* frames were in the WAL */
    db_result_free(r);
}

/*
 * make_durable waits for an open transaction to commit and then syncs with
 * no lock held (B144). The wait is what makes a write issued outside
 * db_txn durable: on the one shared connection it joins whatever
 * transaction another thread has open, here the main thread's, and is not
 * committed until that is. The sync used to be a checkpoint taken under
 * the write lock, which every writer queued behind.
 */
static int g_begin_in_done = -1;

static void begin_in_done(void)
{
    /* With the lock still held by this thread, begin would refuse as a
     * nested transaction (ERR_BUSY); held by another, it would hang. */
    g_begin_in_done = db_txn_begin(db);
    if (g_begin_in_done == OK) db_txn_rollback(db);
}

static void test_make_durable_waits_for_the_open_transaction(void **state)
{
    (void)state;
    worker_t w;
    memset(&w, 0, sizeof(w));
    w.rc = -1;
    pthread_t th;
    assert_int_equal(db_txn_begin(db), OK);        /* a backup's batch */
    /* A bare write from the API thread: it joins the open transaction. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (path, label, uuid, added_at) VALUES ('/y', 'y', 'u-y', 1)", NULL, NULL), 0);
    db_txn_set_durable_hooks(NULL, begin_in_done);
    assert_int_equal(pthread_create(&th, NULL, durable_worker, &w), 0);
    for (int i = 0; i < 1000 && db_txn_waiters() == 0; i++) {
        struct timespec ts = { 0, 1000000L };
        nanosleep(&ts, NULL);
    }
    assert_int_equal(db_txn_waiters(), 1);
    struct timespec ts = { 0, 100 * 1000000L };
    nanosleep(&ts, NULL);
    assert_int_equal(atomic_load(&w.done), 0);     /* not before the commit */
    assert_int_equal(db_txn_commit(db), OK);
    pthread_join(th, NULL);
    db_txn_set_durable_hooks(NULL, NULL);
    assert_int_equal(w.rc, OK);
    assert_int_equal(g_begin_in_done, OK);         /* the sync held no lock */
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

/* ------------------------------------------------------------------ */
/* CUTILS-2: c-utils' own writers take the lock (c-utils 1.8.0)        */
/* ------------------------------------------------------------------ */

static long marker_rows(const char *marker)
{
    char sql[160];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM logs WHERE message = '%s'", marker);
    return count_rows(sql);
}

/* Bounded at 10 s: a line that never lands fails the test, not hangs it. */
static void wait_for_marker(const char *marker)
{
    int seen = 0;
    for (int i = 0; i < 5000 && !seen; i++) {
        if (marker_rows(marker) == 1) { seen = 1; break; }
        struct timespec ts = { 0, 2 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    assert_true(seen);
}

/*
 * A log line written while a transaction is open lands after it, whether
 * that transaction commits or rolls back.
 *
 * The log writer is a c-utils thread inserting into logs on this same
 * connection. Before the lock was handed to c-utils, its INSERT joined
 * whatever transaction was open: a line logged during a backup batch that
 * then failed was rolled back with the batch. Now the writer queues for
 * the lock (it is seen waiting), nothing of it is inside the transaction,
 * and once the transaction rolls back the line is written on its own.
 */
static void test_a_log_line_waits_out_a_rolled_back_transaction(void **state)
{
    (void)state;
    assert_int_equal(db_txn_install_cutils_lock(db), OK);
    assert_int_equal(log_init(db, LOG_INFO, 0), 0);
    unsigned long v0 = db_txn_guard_violations();
    db_txn_guard_arm(1);

    assert_int_equal(db_txn_begin(db), OK);
    log_info("b157-marker-rollback");
    wait_for_waiter();                          /* the log writer, queued */
    assert_int_equal(marker_rows("b157-marker-rollback"), 0);
    db_txn_rollback(db);
    wait_for_marker("b157-marker-rollback");    /* not lost with the rollback */

    db_txn_guard_arm(0);
    assert_int_equal(db_txn_guard_violations(), v0);   /* every row under the lock */
    log_shutdown();
    assert_int_equal(db_set_write_lock(db, NULL, NULL, NULL), 0);
}

static void *log_from_thread(void *arg)
{
    (void)arg;
    log_info("b157-marker-thread");
    return NULL;
}

/*
 * The same from a thread that is not the holder: its line waits for the
 * holder's commit rather than joining the holder's transaction, and does
 * not hold the holder up either (logging only queues the line).
 */
static void test_a_log_line_from_another_thread_waits_for_the_commit(void **state)
{
    (void)state;
    assert_int_equal(db_txn_install_cutils_lock(db), OK);
    assert_int_equal(log_init(db, LOG_INFO, 0), 0);
    unsigned long v0 = db_txn_guard_violations();
    db_txn_guard_arm(1);

    assert_int_equal(db_txn_begin(db), OK);
    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, log_from_thread, NULL), 0);
    assert_int_equal(pthread_join(t, NULL), 0);  /* returned while we hold it */
    wait_for_waiter();
    assert_int_equal(marker_rows("b157-marker-thread"), 0);
    assert_int_equal(db_txn_commit(db), OK);
    wait_for_marker("b157-marker-thread");

    db_txn_guard_arm(0);
    assert_int_equal(db_txn_guard_violations(), v0);
    log_shutdown();
    assert_int_equal(db_set_write_lock(db, NULL, NULL, NULL), 0);
}

/* ---------- B144-REST: the commit budget ---------- */

static char g_blob[3001];

/* Rows of about 3 KB each through one batch that asks should_end at every
 * row, as the backup's do; returns the rows each commit held. */
static int run_batches(int total, int *per_commit, int max_commits)
{
    memset(g_blob, 'b', sizeof(g_blob) - 1);
    g_blob[sizeof(g_blob) - 1] = '\0';
    int n = 0, in = 0;
    db_txn_batch_t b;
    memset(&b, 0, sizeof(b));
    assert_int_equal(db_txn_begin(db), OK);
    db_txn_batch_opened(&b);
    for (int i = 0; i < total; i++) {
        assert_int_equal(db_write(db, "INSERT INTO budget (v) VALUES (?)",
                                  (const char *[]){ g_blob, NULL }, NULL), 0);
        in++;
        if (db_txn_batch_should_end(&b)) {
            assert_int_equal(db_txn_commit(db), OK);
            if (n < max_commits) per_commit[n] = in;
            n++;
            in = 0;
            assert_int_equal(db_txn_begin(db), OK);
            db_txn_batch_opened(&b);
        }
    }
    assert_int_equal(db_txn_commit(db), OK);
    return n;
}

static void budget_setup(void)
{
    assert_int_equal(db_exec_raw(db, "CREATE TABLE budget (id INTEGER PRIMARY KEY, v TEXT);"
                                     " PRAGMA wal_autocheckpoint=0;"), 0);
    db_txn_commit_stats_reset();
    db_txn_track_wal(db);
}

static void budget_teardown(void)
{
    db_txn_track_wal(NULL);
    db_txn_set_frame_cost_ns(0);
    db_txn_commit_stats_reset();
}

/*
 * A batch ends at the commit budget. At 1 ms a frame the budget is 150
 * pages, so the first batch (two frames a row assumed) holds 75 rows; the
 * ones after learn what a row really costs and stay near 150 frames. At
 * 100 ms a frame the budget is the 16-page floor.
 */
static void test_a_batch_ends_at_the_commit_budget(void **state)
{
    (void)state;
    budget_setup();
    db_txn_set_frame_cost_ns(1e6);
    int per[64];
    int n = run_batches(1500, per, 64);
    db_txn_commit_stats_t cs;
    db_txn_commit_stats(&cs);
    print_message("  1 ms/frame: %d commits, first %d rows, then %d; max %u frames, %lu capped\n",
                  n, per[0], n > 2 ? per[2] : 0, cs.max_frames, cs.capped);
    assert_true(n >= 5);
    assert_int_equal(per[0], 75);
    assert_true(cs.capped >= 5);
    assert_true(cs.max_frames <= 150 * 2);

    db_txn_commit_stats_reset();
    db_txn_set_frame_cost_ns(100e6);
    n = run_batches(300, per, 64);
    db_txn_commit_stats(&cs);
    print_message("  100 ms/frame: %d commits, %d rows each; max %u frames\n",
                  n, per[1], cs.max_frames);
    assert_true(per[1] <= DB_TXN_COMMIT_PAGES_MIN);
    assert_true(cs.max_frames <= DB_TXN_COMMIT_PAGES_MIN * 3);
    budget_teardown();
}

/* Measured rather than forced: on this machine's disk a frame costs far
 * less than 150 ms / 256, so the cap is the page ceiling, which bounds the
 * COMMIT written just after a load spike. */
static void test_the_page_ceiling_bounds_every_commit(void **state)
{
    (void)state;
    budget_setup();
    int per[64];
    int n = run_batches(3000, per, 64);
    db_txn_commit_stats_t cs;
    db_txn_commit_stats(&cs);
    print_message("  measured: %d commits, max %u frames, %.1f us/frame\n",
                  n, cs.max_frames, cs.ns_per_frame / 1e3);
    assert_true(n >= 3000 / (DB_TXN_COMMIT_PAGES_MAX * 2));
    assert_true(cs.max_frames <= DB_TXN_COMMIT_PAGES_MAX * 2);
    budget_teardown();
}

/* Without the wal-index to learn from, nothing is capped: batches end as
 * they did before (OWN-16's "with nobody waiting, a batch is ended by its
 * size"). */
static void test_no_tracking_means_no_cap(void **state)
{
    (void)state;
    assert_int_equal(db_exec_raw(db, "CREATE TABLE budget (id INTEGER PRIMARY KEY, v TEXT)"), 0);
    db_txn_set_frame_cost_ns(100e6);
    int per[4];
    assert_int_equal(run_batches(500, per, 4), 0);
    db_txn_set_frame_cost_ns(0);
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
        cmocka_unit_test_setup_teardown(test_make_durable_syncs_the_wal, setup, teardown),
        cmocka_unit_test_setup_teardown(test_make_durable_waits_for_the_open_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_drop_staged_waits_for_a_foreign_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_db_write_joins_the_callers_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_write_batch_yields_to_a_waiter, setup, teardown),
        cmocka_unit_test_setup_teardown(test_guard_reports_a_write_outside_the_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_log_line_waits_out_a_rolled_back_transaction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_log_line_from_another_thread_waits_for_the_commit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_batch_ends_at_the_commit_budget, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_page_ceiling_bounds_every_commit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_no_tracking_means_no_cap, setup, teardown),
    };
    /* Before any test opens its database, so every connection is hooked.
     * The guard test counts violations rather than aborting on one, so the
     * abort the end-to-end suites ask for must not reach it from a caller's
     * environment. */
    unsetenv("C_BACKUP_TXN_GUARD");
    db_txn_guard_install();
    return cmocka_run_group_tests(tests, NULL, NULL);
}
