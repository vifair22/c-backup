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
 *
 * Since OWN-19 a snapshot larger than the seal is written as
 * several packs, the earlier ones installed mid-snapshot as they fill. The
 * last three tests hold that to the same promise: every pack a record
 * depends on is in place before the record, and an install that fails at
 * either kind of seal - mid-snapshot or at the commit - fails the run
 * naming the snapshot and leaves no record of it.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "fault_inject.h"
#include "pack.h"
#include "reindex.h"
#include "replicate.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_replica_fault"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_RDB   TEST_DIR "/replica-state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_REPL  TEST_DIR "/replica"

/* The seal moved down from 256 MiB (pack_test_set_seal) and the files with
 * it, a sixteenth of the size: the same snapshot past the seal, in two
 * packs, without the 900 MiB of scratch the full size cost. */
#define SEAL_BYTES ((uint64_t)16 << 20)
#define BIG_FILES  18        /* of 1 MiB: 18 MiB, two packs at the seal */
#define BIG_BYTES  ((size_t)1 << 20)

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
    pack_test_set_seal(SEAL_BYTES);
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
    pack_test_set_seal(0);
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

/* Incompressible, so a pack grows by what is put in it; every file gets its
 * own seed, so nothing dedups. */
static void fill_random(uint8_t *buf, size_t n, uint64_t seed)
{
    uint64_t r = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i + 8 <= n; i += 8) {
        r ^= r >> 12; r ^= r << 25; r ^= r >> 27;
        uint64_t v = r * 0x2545F4914F6CDD1Dull;
        memcpy(buf + i, &v, sizeof(v));
    }
}

/* A small first snapshot, then a second past the seal: 18 MiB of files the
 * first does not have, which replicate as two packs. */
static void small_then_big_snapshot(void)
{
    write_file(TEST_SRC "/a", "first snapshot's file, with its own bytes");
    backup();

    const size_t n = BIG_BYTES;
    uint8_t *buf = malloc(n);
    assert_non_null(buf);
    for (int i = 0; i < BIG_FILES; i++) {
        char path[128];
        snprintf(path, sizeof(path), TEST_SRC "/big%02d.bin", i);
        fill_random(buf, n, 500u + (uint64_t)i);
        int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        assert_true(fd >= 0);
        size_t off = 0;
        while (off < n) {
            ssize_t w = write(fd, buf + off, n - off);
            assert_true(w > 0);
            off += (size_t)w;
        }
        close(fd);
    }
    free(buf);
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
static uint64_t assert_replica_verifies_packs(int *out_packs, uint64_t *out_max_body);

static uint64_t assert_replica_verifies(void)
{
    return assert_replica_verifies_packs(NULL, NULL);
}

/* The same, also reporting how many packs the replica holds and the largest
 * one's entry bytes (headers included), which is what the seal measures. */
static uint64_t assert_replica_verifies_packs(int *out_packs, uint64_t *out_max_body)
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

    if (out_packs || out_max_body) {
        char repo[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)rid);
        db_result_t *r = NULL;
        assert_int_equal(db_execute(rdb,
            "SELECT pack_id, SUM(comp_size + 51) FROM pack_objects"
            " WHERE repo_id = ? GROUP BY pack_id",
            (const char *[]){ repo, NULL }, &r), 0);
        int n = r ? r->nrows : 0;
        uint64_t mx = 0;
        for (int i = 0; i < n; i++) {
            uint64_t b = strtoull(r->rows[i][1], NULL, 10);
            if (b > mx) mx = b;
        }
        db_result_free(r);
        if (out_packs)    *out_packs = n;
        if (out_max_body) *out_max_body = mx;
    }

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
    snprintf(cmd, sizeof(cmd), "test -z \"$(ls -A '%s/tmp' | grep -vx .lock)\"", TEST_REPL);
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

/* ------------------------------------------------------------------ */

static int replica_tmp_is_empty(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "test -z \"$(ls -A '%s/tmp' | grep -vx .lock)\"", TEST_REPL);
    return system(cmd) == 0;
}

/*
 * OWN-19. A snapshot past the seal replicates as several packs, none past
 * it, and the replica reindexes and verifies like any other.
 */
static void test_a_snapshot_past_the_seal_writes_several_packs(void **state)
{
    (void)state;
    small_then_big_snapshot();

    replicate_result_t rr;
    assert_int_equal(replicate_once(&rr), OK);
    assert_int_equal(rr.snapshots_sent, 2);

    int packs = 0;
    uint64_t max_body = 0;
    assert_int_equal(assert_replica_verifies_packs(&packs, &max_body), 2);
    /* Snapshot 1's pack, then the 18 MiB of snapshot 2 in two. */
    assert_true(packs >= 3);
    assert_true(max_body <= SEAL_BYTES);
}

/*
 * The second snapshot's second pack - installed at the commit, after its
 * first was installed mid-snapshot - cannot go in. The run fails naming
 * snapshot 2 and writes no record of it, though half its objects are in
 * place; the next run sends only what is missing and completes it.
 */
static void test_failed_install_of_a_snapshots_last_pack(void **state)
{
    (void)state;
    small_then_big_snapshot();

    fault_rename_dst = TEST_REPL "/packs/";
    fault_rename_skip = 2;            /* snapshot 1's pack, snapshot 2's first */
    replicate_result_t rr;
    status_t st = replicate_once(&rr);
    disarm();

    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "snapshot 2"));
    err_clear();
    assert_int_equal(fault_rename_refused, 1);
    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(record_exists(1));
    assert_false(record_exists(2));
    assert_int_equal(assert_replica_verifies(), 1);
    assert_true(replica_tmp_is_empty());
    uint64_t first_bytes = rr.bytes_sent;

    st = replicate_once(&rr);
    assert_int_equal(st, OK);
    assert_int_equal(rr.snapshots_skipped, 1);
    assert_int_equal(rr.snapshots_sent, 1);
    /* The pack that did go in is reused, not sent again. */
    assert_true(rr.bytes_sent < first_bytes);
    assert_true(record_exists(2));

    int packs = 0;
    uint64_t max_body = 0;
    assert_int_equal(assert_replica_verifies_packs(&packs, &max_body), 2);
    assert_true(max_body <= SEAL_BYTES);
}

/*
 * The install that fails is the one at the seal, inside put_object, before
 * the snapshot's last objects have even been read. Same outcome: the run
 * fails naming the snapshot, no record, nothing left staged.
 */
static void test_failed_install_at_the_seal_mid_snapshot(void **state)
{
    (void)state;
    small_then_big_snapshot();

    fault_rename_dst = TEST_REPL "/packs/";
    fault_rename_skip = 1;            /* snapshot 1's pack */
    replicate_result_t rr;
    status_t st = replicate_once(&rr);
    disarm();

    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "snapshot 2"));
    assert_non_null(strstr(err_msg(), "full pack"));
    err_clear();
    assert_int_equal(fault_rename_refused, 1);
    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(record_exists(1));
    assert_false(record_exists(2));
    assert_int_equal(assert_replica_verifies(), 1);
    assert_true(replica_tmp_is_empty());

    assert_int_equal(replicate_once(&rr), OK);
    assert_int_equal(rr.snapshots_sent, 1);
    int packs = 0;
    uint64_t max_body = 0;
    assert_int_equal(assert_replica_verifies_packs(&packs, &max_body), 2);
    assert_true(max_body <= SEAL_BYTES);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_failed_install_fails_the_run, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_first_install_writes_no_record, setup, teardown),
        cmocka_unit_test_setup_teardown(test_interrupted_run_leaves_whole_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_run_leaves_whole_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_snapshot_past_the_seal_writes_several_packs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_install_of_a_snapshots_last_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_install_at_the_seal_mid_snapshot, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
