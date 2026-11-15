#define _POSIX_C_SOURCE 200809L

/*
 * A replica never holds a snapshot record whose objects are not installed.
 *
 * The local sink used to stage every object of a run into one pack and
 * install it only when the sink closed - after replicate_run had already
 * written every snapshot record and returned. A failed install was logged
 * from close and nothing else: the run reported OK and the replica held
 * records naming objects that did not exist, which every later run skipped
 * as complete.
 *
 * These tests make the install fail (a rename into the replica's packs/,
 * refused by the fault injector), interrupt a run part-way through a
 * snapshot, and kill one outright, and after each prove three things the
 * operator relies on: the run said it failed, the replica opens and
 * verifies clean with only whole snapshots in it, and the next run
 * completes it.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "fault_inject.h"
#include "reindex.h"
#include "replicate.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"

#define TEST_DIR   "/tmp/c_backup_test_replica_fault"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_RDB   TEST_DIR "/replica-state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_REPL  TEST_DIR "/replica"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static repo_marker_t  marker;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, const char *body)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(body, f);
    fclose(f);
}

static void disarm(void)
{
    fault_rename_dst = NULL;
    fault_rename_skip = 0;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    disarm();
    fault_rename_refused = 0;
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (repo_marker_read(TEST_REPO, &marker) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    disarm();
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
}

/* Three snapshots, each bringing objects the one before did not have. */
static void three_snapshots(void)
{
    write_file(TEST_SRC "/a", "first snapshot's file, with its own bytes");
    backup();
    write_file(TEST_SRC "/b", "second snapshot adds this one");
    backup();
    write_file(TEST_SRC "/c", "and the third adds a third");
    backup();
}

static int record_exists(int64_t id)
{
    char p[256];
    snprintf(p, sizeof(p), TEST_REPL "/snapshots/%lld", (long long)id);
    struct stat sb;
    return stat(p, &sb) == 0;
}

static status_t replicate_once(replicate_result_t *rr)
{
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &sink), OK);
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, rr);
    sink.v->close(sink.ctx);
    return st;
}

/*
 * What an operator does with a replica: register it with a daemon of its
 * own, reindex, verify deep. Every snapshot record must reindex, and every
 * object any of them names must be present and intact. Returns how many
 * snapshots the replica holds.
 */
static uint64_t assert_replica_verifies(void)
{
    rmrf(TEST_RDB);
    cutils_db_t *rdb = NULL;
    assert_int_equal(db_open(&rdb, TEST_RDB), 0);
    assert_int_equal(db_run_lib_migrations(rdb), 0);
    assert_int_equal(db_run_compiled_migrations(rdb, app_migrations), 0);
    int64_t rid = 0;
    assert_int_equal(repo_register(rdb, TEST_REPL, "replica", &rid), OK);

    store_t *rs = NULL;
    assert_int_equal(store_open(TEST_REPL, NULL, &rs), OK);
    store_index_t *ri = NULL;
    assert_int_equal(store_index_bind(rs, rdb, rid, &ri), OK);

    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(rs, rdb, rid, NULL, &rx), OK);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.packs_unreadable, 0);

    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(rs, rdb, rid, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.recoverable, 0);

    store_index_free(ri);
    store_close(rs);
    db_close(rdb);
    return rx.snapshots;
}

/* ------------------------------------------------------------------ */

/*
 * OWN-11. The second snapshot's pack cannot be installed: the run must say
 * so, snapshot 2 and everything after it must have no record, snapshot 1
 * must be whole, and the next run must finish the job.
 */
static void test_failed_install_fails_the_run(void **state)
{
    (void)state;
    three_snapshots();

    fault_rename_dst = TEST_REPL "/packs/";
    fault_rename_skip = 1;            /* snapshot 1's pack goes in */
    replicate_result_t rr;
    status_t st = replicate_once(&rr);
    disarm();

    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "snapshot 2"));
    err_clear();
    assert_true(fault_rename_refused > 0);
    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(record_exists(1));
    assert_false(record_exists(2));
    assert_false(record_exists(3));
    assert_int_equal(assert_replica_verifies(), 1);

    /* Nothing of the failed install is left staged. */
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "test -z \"$(ls -A '%s/tmp')\"", TEST_REPL);
    assert_int_equal(system(cmd), 0);

    st = replicate_once(&rr);
    assert_int_equal(st, OK);
    assert_int_equal(rr.snapshots_skipped, 1);
    assert_int_equal(rr.snapshots_sent, 2);
    assert_true(record_exists(2));
    assert_true(record_exists(3));
    assert_int_equal(assert_replica_verifies(), 3);
}

/* The very first install failing leaves a replica with no snapshots at all,
 * not one with three hollow ones. */
static void test_failed_first_install_writes_no_record(void **state)
{
    (void)state;
    three_snapshots();

    fault_rename_dst = TEST_REPL "/packs/";
    replicate_result_t rr;
    status_t st = replicate_once(&rr);
    disarm();
    assert_int_not_equal(st, OK);
    err_clear();
    assert_int_equal(rr.snapshots_sent, 0);
    assert_false(record_exists(1));
    assert_int_equal(assert_replica_verifies(), 0);

    assert_int_equal(replicate_once(&rr), OK);
    assert_int_equal(rr.snapshots_sent, 3);
    assert_int_equal(assert_replica_verifies(), 3);
}

/*
 * A sink that drops the link part-way through a snapshot: put_object
 * starts failing once the given snapshot's objects begin to arrive.
 */
typedef struct {
    replica_sink_t inner;
    int            commits;
    int            fail_in_snapshot;  /* 0-based count of commits seen */
    int            puts_in_target;
    int            puts_allowed;
    int            die;               /* _exit instead of failing */
} drop_ctx_t;

static status_t d_has_objects(void *v, const uint8_t *h, size_t n, uint8_t *out)
{
    drop_ctx_t *d = v;
    return d->inner.v->has_objects(d->inner.ctx, h, n, out);
}

static status_t d_put_object(void *v, const replica_object_t *o)
{
    drop_ctx_t *d = v;
    if (d->commits == d->fail_in_snapshot &&
        d->puts_in_target++ >= d->puts_allowed) {
        if (d->die) _exit(0);
        return set_error(ERR_IO, "link dropped");
    }
    return d->inner.v->put_object(d->inner.ctx, o);
}

static status_t d_commit(void *v, const replica_snapshot_t *s)
{
    drop_ctx_t *d = v;
    status_t st = d->inner.v->commit_snapshot(d->inner.ctx, s);
    if (st == OK) d->commits++;
    return st;
}

static status_t d_has_snapshot(void *v, int64_t id,
                               const uint8_t root[OBJECT_HASH_SIZE], int *out)
{
    drop_ctx_t *d = v;
    return d->inner.v->has_snapshot(d->inner.ctx, id, root, out);
}

static void d_close(void *v)
{
    drop_ctx_t *d = v;
    d->inner.v->close(d->inner.ctx);
}

static const replica_sink_vtbl_t drop_vtbl = {
    .has_objects = d_has_objects, .put_object = d_put_object,
    .commit_snapshot = d_commit, .has_snapshot = d_has_snapshot,
    .close = d_close,
};

/*
 * Interrupted inside snapshot 2, after some of its objects were staged.
 * close installs what was staged as unreferenced objects; the replica
 * holds snapshot 1 only, verifies clean, and the next run completes it.
 */
static void test_interrupted_run_leaves_whole_snapshots(void **state)
{
    (void)state;
    three_snapshots();

    drop_ctx_t d;
    memset(&d, 0, sizeof(d));
    d.fail_in_snapshot = 1;
    d.puts_allowed = 1;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &d.inner), OK);
    replica_sink_t sink = { .v = &drop_vtbl, .ctx = &d };
    replicate_result_t rr;
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    assert_int_not_equal(st, OK);
    err_clear();

    assert_true(record_exists(1));
    assert_false(record_exists(2));
    assert_int_equal(assert_replica_verifies(), 1);

    assert_int_equal(replicate_once(&rr), OK);
    assert_int_equal(rr.snapshots_sent, 2);
    assert_int_equal(assert_replica_verifies(), 3);
}

/*
 * Killed inside snapshot 2, so close never runs: snapshot 2's staging is
 * left in the replica's tmp/ and never installed. The child opens its own
 * database and store rather than using the parent's handles.
 */
static void test_killed_run_leaves_whole_snapshots(void **state)
{
    (void)state;
    three_snapshots();

    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        cutils_db_t *cdb = NULL;
        store_t *cs = NULL;
        store_index_t *ci = NULL;
        if (db_open(&cdb, TEST_DB) != 0 ||
            store_open(TEST_REPO, NULL, &cs) != OK ||
            store_index_bind(cs, cdb, repo_id, &ci) != OK)
            _exit(2);
        drop_ctx_t d;
        memset(&d, 0, sizeof(d));
        d.fail_in_snapshot = 1;
        d.puts_allowed = 1;
        d.die = 1;
        if (replica_sink_local_open(TEST_REPL, &marker, &d.inner) != OK) _exit(3);
        replica_sink_t sink = { .v = &drop_vtbl, .ctx = &d };
        replicate_result_t rr;
        (void)replicate_run(cs, cdb, repo_id, &sink, NULL, NULL, NULL, &rr);
        _exit(4);   /* never reached when the kill lands */
    }
    int wst = 0;
    assert_int_equal(waitpid(pid, &wst, 0), pid);
    assert_true(WIFEXITED(wst));
    assert_int_equal(WEXITSTATUS(wst), 0);

    assert_true(record_exists(1));
    assert_false(record_exists(2));
    assert_int_equal(assert_replica_verifies(), 1);

    replicate_result_t rr;
    assert_int_equal(replicate_once(&rr), OK);
    assert_int_equal(rr.snapshots_skipped, 1);
    assert_int_equal(rr.snapshots_sent, 2);
    assert_int_equal(assert_replica_verifies(), 3);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_failed_install_fails_the_run, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_first_install_writes_no_record, setup, teardown),
        cmocka_unit_test_setup_teardown(test_interrupted_run_leaves_whole_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_run_leaves_whole_snapshots, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
