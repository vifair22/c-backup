#define _POSIX_C_SOURCE 200809L

/*
 * The start-up reconcile: snapshots the repository holds and the index
 * lost are found, and found cheaply.
 *
 * state.db runs with synchronous=NORMAL, so a power cut can take the
 * newest commits with it, and a backup's commits are the last thing it
 * does: the mark and the record are already durable in the repository. The
 * lost tail is simulated here exactly that way - the newest snapshot's
 * rows deleted, its record left - and the reconcile has to find it, hand
 * it to submit, and then, after the reindex submit would have run, find
 * nothing. Equally it must not invent work: a mark ahead of every record
 * (the newest snapshots pruned) is normal and costs no reindex.
 *
 * tests/test_cli_e2e.sh runs the same case against a real daemon start.
 */

#include <pthread.h>
#include <stdatomic.h>
#include <setjmp.h>
#include <time.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "gc.h"
#include "prune.h"
#include "restore.h"
#include "verify.h"
#include "db_txn.h"
#include "routes.h"
#include "index_reconcile.h"
#include "reindex.h"
#include "repo.h"
#include "replicate.h"
#include "repo_key.h"
#include "repo_tags.h"
#include "snapshot_ids.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"
#include "full_vfs.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_index_reconcile"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_KEYS TEST_DIR "/keys"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo[32];

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

static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static void exec_repo(const char *sql)
{
    assert_int_equal(db_execute_non_query(db, sql, (const char *[]){ repo, NULL }, NULL), 0);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = REPO_ENC_NONE;
    if (repo_register_ex(db, TEST_REPO, "test", &enc, &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    write_file(TEST_SRC "/a.txt", "first\n");
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void backup_once(const char *content)
{
    write_file(TEST_SRC "/a.txt", content);
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
}

/* What a power cut takes: the newest snapshot's rows, every table a backup
 * writes for it, and the id mark the same commit raised. The record file
 * and the repository's snapshot-ids were durable first and stay. */
static void lose_newest_snapshot_rows(void)
{
    long long newest = scalar("SELECT MAX(id) FROM snapshots WHERE repo_id = ?");
    assert_true(newest > 0);
    char id[32];
    snprintf(id, sizeof(id), "%lld", newest);
    const char *p[] = { repo, id, NULL };
    assert_int_equal(db_execute_non_query(db,
        "DELETE FROM nodes WHERE repo_id = ? AND snap_id = ?", p, NULL), 0);
    assert_int_equal(db_execute_non_query(db,
        "DELETE FROM snapshots WHERE repo_id = ? AND id = ?", p, NULL), 0);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE snapshot_id_seq SET last_id = ? - 1 WHERE repo_id = ?",
        (const char *[]){ id, repo, NULL }, NULL), 0);
}

typedef struct {
    int     calls;
    int64_t repo_id;
} submit_log_t;

static status_t fake_submit(int64_t rid, void *ctx, int64_t *out_job_id)
{
    submit_log_t *s = ctx;
    s->calls++;
    s->repo_id = rid;
    *out_job_id = 77;
    return OK;
}

static void test_complete_index_submits_nothing(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    uint64_t missing = 99;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 0);

    submit_log_t log = { 0, 0 };
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.repos_checked, 1);
    assert_int_equal(rr.repos_missing, 0);
    assert_int_equal(log.calls, 0);
}

static void test_lost_tail_is_found_and_reindex_puts_it_back(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    backup_once("three\n");
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 3);
    lose_newest_snapshot_rows();
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 2);

    uint64_t missing = 0;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 1);

    submit_log_t log = { 0, 0 };
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.repos_missing, 1);
    assert_int_equal(rr.snapshots_missing, 1);
    assert_int_equal(rr.jobs_submitted, 1);
    assert_int_equal(log.calls, 1);
    assert_int_equal(log.repo_id, repo_id);

    /* What the submitted job does. */
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 3);
    assert_int_equal(scalar("SELECT MAX(id) FROM snapshots WHERE repo_id = ?"), 3);
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 0);

    /* And the next backup numbers past it, not onto it. */
    backup_once("four\n");
    assert_int_equal(scalar("SELECT MAX(id) FROM snapshots WHERE repo_id = ?"), 4);
}

/*
 * B195: a backup whose index cannot commit, state.db's filesystem being
 * full, after its snapshot record reached the repository. The record is
 * complete and stays; the run fails saying which snapshot was written and
 * how its index comes back, and reconcile does find it.
 *
 * The disk fills the moment index_nodes prepares its first node insert (an
 * authorizer on the connection sees it), which is where UAT met it.
 */
static atomic_int fill_on_nodes;

static int fill_authorizer(void *arg, int action, const char *a1, const char *a2,
                           const char *a3, const char *a4)
{
    (void)arg; (void)a2; (void)a3; (void)a4;
    if (action == SQLITE_INSERT && a1 && strcmp(a1, "nodes") == 0 &&
        atomic_load(&fill_on_nodes))
        full_vfs_arm(1);
    return SQLITE_OK;
}

/* Node rows changed per transaction, for the batching test: the most one
 * transaction changed since the counters were reset. */
static atomic_long rows_in_txn, rows_max;

static void count_row(void *arg, int op, const char *dbn, const char *table, sqlite3_int64 rowid)
{
    (void)arg; (void)op; (void)dbn; (void)rowid;
    if (table && strcmp(table, "nodes") == 0) atomic_fetch_add(&rows_in_txn, 1);
}

static int count_commit(void *arg)
{
    (void)arg;
    long n = atomic_exchange(&rows_in_txn, 0);
    if (n > atomic_load(&rows_max)) atomic_store(&rows_max, n);
    return 0;
}

static int fill_ext(sqlite3 *conn, char **err, const sqlite3_api_routines *api)
{
    (void)err; (void)api;
    sqlite3_set_authorizer(conn, fill_authorizer, NULL);
    sqlite3_update_hook(conn, count_row, NULL);
    sqlite3_commit_hook(conn, count_commit, NULL);
    return SQLITE_OK;
}

static void check_unindexed_snapshot(const backup_result_t *res, status_t st)
{
    print_message("backup: %s\n", err_msg());
    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "database or disk is full"));
    assert_non_null(strstr(err_msg(), "snapshot 1 was written to the repository"));
    assert_non_null(strstr(err_msg(), "c-backup reindex"));
    assert_int_equal(res->snapshot_id, 1);
    assert_int_equal(res->snapshot_unindexed, 1);

    /* The record is there, whole; the index has no snapshot. */
    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/snapshots/1", &sb), 0);
    assert_true(sb.st_size > 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);

    /* What the next start does with it. */
    uint64_t missing = 0;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 1);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 1);
    assert_true(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 1") > 0);
}

static void test_a_full_disk_in_index_nodes_reports_the_written_snapshot(void **state)
{
    (void)state;
    write_file(TEST_SRC "/a.txt", "full\n");
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    err_clear();
    atomic_store(&fill_on_nodes, 1);
    status_t st = backup_run(store, db, repo_id, &o, &res);
    atomic_store(&fill_on_nodes, 0);
    full_vfs_arm(0);
    check_unindexed_snapshot(&res, st);
    backup_result_free(&res);
}

/* The same when it is the snapshot row's commit that fails. */
static status_t fill_hook(void *ctx, cutils_db_t *hdb, int64_t snap_id)
{
    (void)ctx; (void)hdb; (void)snap_id;
    full_vfs_arm(1);
    return OK;
}

static void test_a_full_disk_at_the_snapshot_row_reports_the_written_snapshot(void **state)
{
    (void)state;
    write_file(TEST_SRC "/a.txt", "full\n");
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.index_hook = fill_hook;
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    err_clear();
    status_t st = backup_run(store, db, repo_id, &o, &res);
    full_vfs_arm(0);
    check_unindexed_snapshot(&res, st);
    backup_result_free(&res);
}

/*
 * B200: a gap below the highest indexed id. Snapshot 1 is left unindexed
 * the B195 way and holds a file nothing later has; later snapshots are
 * indexed after it. Reconcile used to look only above the highest indexed
 * id and so never saw it, and gc, which marks from the index, swept what
 * only snapshot 1 holds.
 */
#define GAP_BYTES  (300u * 1024u)
#define GAP_FILE   TEST_SRC "/only-in-one.bin"

static void gap_content(uint8_t *buf, size_t n)
{
    uint64_t x = 0x9e3779b97f4a7c15ull;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        buf[i] = (uint8_t)x;
    }
}

/* Snapshot 1 unindexed, holding GAP_FILE; then `later` indexed backups
 * without it, each a small pack of its own. */
static void make_gap(int later)
{
    uint8_t *buf = malloc(GAP_BYTES);
    assert_non_null(buf);
    gap_content(buf, GAP_BYTES);
    FILE *f = fopen(GAP_FILE, "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(buf, 1, GAP_BYTES, f), GAP_BYTES);
    fclose(f);
    free(buf);

    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    atomic_store(&fill_on_nodes, 1);
    status_t st = backup_run(store, db, repo_id, &o, &res);
    atomic_store(&fill_on_nodes, 0);
    full_vfs_arm(0);
    assert_int_not_equal(st, OK);
    assert_int_equal(res.snapshot_unindexed, 1);
    assert_int_equal(res.snapshot_id, 1);
    backup_result_free(&res);
    err_clear();

    assert_int_equal(unlink(GAP_FILE), 0);
    for (int k = 0; k < later; k++) {
        char c[32];
        snprintf(c, sizeof(c), "later %d\n", k);
        backup_once(c);
    }
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id = 1"), 0);
    assert_int_equal(scalar("SELECT MAX(id) FROM snapshots WHERE repo_id = ?"), later + 1);
}

/* Snapshot 1 of the repository st/rdb/rid restores byte for byte. */
static void gap_restores(store_t *st, cutils_db_t *rdb, int64_t rid)
{
    rmrf(TEST_DIR "/out");
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(st, rdb, rid, 1, TEST_DIR "/out", &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);

    uint8_t *want = malloc(GAP_BYTES), *got = malloc(GAP_BYTES + 1);
    assert_non_null(want);
    assert_non_null(got);
    gap_content(want, GAP_BYTES);
    FILE *f = fopen(TEST_DIR "/out" GAP_FILE, "rb");
    assert_non_null(f);
    assert_int_equal(fread(got, 1, GAP_BYTES + 1, f), GAP_BYTES);
    fclose(f);
    assert_memory_equal(got, want, GAP_BYTES);
    free(want);
    free(got);
}

/* Snapshot 1 restores byte for byte, and a deep verify of everything the
 * index lists is clean. */
static void gap_intact(void)
{
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id = 1"), 1);
    gap_restores(store, db, repo_id);

    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_true(vr.checked > 0);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.recoverable, 0);
}

static status_t gc_once(gc_result_t *g, int dry)
{
    memset(g, 0, sizeof(*g));
    return gc_run(store, db, repo_id, dry, NULL, g);
}

/* A restart finds the gap and the reindex it asks for indexes it. */
static void test_a_gap_below_the_highest_id_is_found_at_start(void **state)
{
    (void)state;
    make_gap(2);
    uint64_t missing = 0;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 1);
    submit_log_t log = { 0, 0 };
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.snapshots_missing, 1);
    assert_int_equal(log.calls, 1);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 0);
    gap_intact();
}

/* gc indexes the gap before it marks: nothing of snapshot 1 is swept. */
static void test_gc_indexes_a_gap_before_it_sweeps(void **state)
{
    (void)state;
    make_gap(2);
    gc_result_t g;
    assert_int_equal(gc_once(&g, 0), OK);
    assert_int_equal(g.snapshots_indexed, 1);
    gap_intact();
    /* And again, with nothing left to index. */
    assert_int_equal(gc_once(&g, 0), OK);
    assert_int_equal(g.snapshots_indexed, 0);
    gap_intact();
}

/* A prune of a later snapshot, then gc: the gap still holds. */
static void test_prune_then_gc_keeps_a_gap(void **state)
{
    (void)state;
    make_gap(3);
    assert_int_equal(prune_snapshot(store, db, repo_id, 2), OK);
    gc_result_t g;
    assert_int_equal(gc_once(&g, 0), OK);
    assert_int_equal(g.snapshots_indexed, 1);
    assert_true(g.swept > 0);           /* snapshot 2's own content went */
    gap_intact();
}

/* Many small packs, merged by gc's coalescing pass, the gap's among them. */
static void test_coalescing_keeps_a_gap(void **state)
{
    (void)state;
    make_gap(8);
    gc_result_t g;
    assert_int_equal(gc_once(&g, 0), OK);
    assert_int_equal(g.snapshots_indexed, 1);
    print_message("coalesced %u packs\n", g.packs_coalesced);
    assert_true(g.packs_coalesced > 0);
    gap_intact();
}

/* Replication chooses from the index, so it indexes the gap first: the
 * replica holds snapshot 1, and it restores from there. */
static void test_replication_carries_a_gap(void **state)
{
    (void)state;
    make_gap(2);
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_DIR "/replica", &m, &sink), OK);
    replicate_result_t rr;
    memset(&rr, 0, sizeof(rr));
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    assert_int_equal(st, OK);
    assert_int_equal(rr.snapshots_sent, 3);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id = 1"), 1);

    /* The replica, as its own daemon would see it: registered in a
     * database of its own (it shares the source's identity) and reindexed. */
    cutils_db_t *rdb = NULL;
    assert_int_equal(db_open(&rdb, TEST_DIR "/replica.db"), 0);
    assert_int_equal(db_run_lib_migrations(rdb), 0);
    assert_int_equal(db_run_compiled_migrations(rdb, app_migrations), 0);
    int64_t rid = 0;
    assert_int_equal(repo_register(rdb, TEST_DIR "/replica", "replica", &rid), OK);
    store_t *rst = NULL;
    assert_int_equal(repo_store_open(TEST_DIR "/replica", NULL, &rst), OK);
    store_index_t *ridx = NULL;
    assert_int_equal(store_index_bind(rst, rdb, rid, &ridx), OK);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(rst, rdb, rid, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 3);
    gap_restores(rst, rdb, rid);
    store_index_free(ridx);
    store_close(rst);
    db_close(rdb);
}

/* Indexing a gap commits in batches (B200): a large snapshot's walk does
 * not hold the write lock for all of it, as index_nodes does not. */
static void test_indexing_a_gap_commits_in_batches(void **state)
{
    (void)state;
    char path[256];
    assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
    for (int i = 0; i < 6000; i++) {
        snprintf(path, sizeof(path), TEST_SRC "/many/f%05d", i);
        write_file(path, "x");
    }
    backup_once("big\n");
    long long id = scalar("SELECT MAX(id) FROM snapshots WHERE repo_id = ?");
    lose_newest_snapshot_rows();
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), id - 1);

    atomic_store(&rows_in_txn, 0);
    atomic_store(&rows_max, 0);
    uint64_t n = 0;
    assert_int_equal(reindex_gaps(store, db, repo_id, "test", &n), OK);
    assert_int_equal(n, 1);
    long mx = atomic_load(&rows_max);
    print_message("most node rows in one transaction: %ld\n", mx);
    /* One transaction held all 6,000; a batch holds at most 2,000. */
    assert_true(mx > 0);
    assert_true(mx <= 2000);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), id);
    char sql[128];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = %lld", id);
    assert_true(scalar(sql) >= 6000);
}

/* A record that cannot be indexed refuses the gc, naming it, and nothing
 * is swept around it. A dry run refuses while any gap is there. */
static void test_gc_refuses_a_gap_it_cannot_index(void **state)
{
    (void)state;
    make_gap(1);
    write_file(TEST_REPO "/snapshots/77", "not a snapshot record\n");
    long long objects = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?");

    gc_result_t g;
    err_clear();
    assert_int_not_equal(gc_once(&g, 1), OK);
    print_message("dry run: %s\n", err_msg());
    assert_non_null(strstr(err_msg(), "gc --dry-run refused: snapshot 1"));
    err_clear();
    assert_int_not_equal(gc_once(&g, 0), OK);
    print_message("gc: %s\n", err_msg());
    assert_non_null(strstr(err_msg(), "gc refused: snapshot 77"));
    assert_non_null(strstr(err_msg(), "snapshots/77"));
    err_clear();
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), objects);

    /* The bad record removed, gc goes through and the real gap holds. */
    assert_int_equal(unlink(TEST_REPO "/snapshots/77"), 0);
    assert_int_equal(gc_once(&g, 0), OK);
    gap_intact();
}

/* Several lost, and a power cut that also took the newest snapshot rows of
 * an older commit: everything above the index's highest id is counted. */
static void test_several_lost(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    backup_once("three\n");
    lose_newest_snapshot_rows();
    lose_newest_snapshot_rows();
    uint64_t missing = 0;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 2);
}

/* The newest snapshot pruned: the mark is ahead of every record, which is
 * how ids are kept from being reused. Nothing is missing. */
static void test_pruned_top_is_not_missing(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    lose_newest_snapshot_rows();
    assert_int_equal(unlink(TEST_REPO "/snapshots/2"), 0);
    int64_t mark = 0;
    assert_int_equal(snapshot_ids_read_repo(TEST_REPO, &mark), OK);
    assert_int_equal(mark, 2);

    uint64_t missing = 9;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 0);
}

/* No usable mark: the directory listing answers, and still only counts
 * records above the index. Files that are not records do not count. */
static void test_damaged_mark_falls_back_to_listing(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    lose_newest_snapshot_rows();
    write_file(TEST_REPO "/" SNAPSHOT_IDS_FILE, "not json\n");
    write_file(TEST_REPO "/snapshots/notes.txt", "x\n");
    write_file(TEST_REPO "/snapshots/007", "x\n");
    uint64_t missing = 0;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 1);
}

/* A repository that is not there (an offline mount) is skipped, counted,
 * and never stops the daemon. */
static void test_unreadable_repo_is_skipped(void **state)
{
    (void)state;
    backup_once("one\n");
    exec_repo("UPDATE repos SET path = '" TEST_DIR "/gone' WHERE id = ?");
    submit_log_t log = { 0, 0 };
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.repos_checked, 1);
    assert_int_equal(rr.repos_unreadable, 1);
    assert_int_equal(log.calls, 0);
}

/* A repository being removed is left alone: reindexing it would put back
 * the rows the purge is deleting. */
static void test_repo_being_removed_is_skipped(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    lose_newest_snapshot_rows();
    exec_repo("INSERT INTO repo_purge_pending (repo_id, queued_at) VALUES (?, 0)");
    submit_log_t log = { 0, 0 };
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.repos_checked, 0);
    assert_int_equal(log.calls, 0);
}

/*
 * V3-12 and B139: a tag the database lost after it was answered - the tag
 * row gone, the repository's tag list written and synced before the answer
 * - is put back by the next start. The list is the repository's own copy
 * and 0600 like the rest of it (NIT-27).
 */
static void test_lost_tag_is_restored_from_the_repository(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    exec_repo("INSERT INTO tags (repo_id, name, snap_id, created_at) VALUES (?, 'keep', 1, 5)");
    exec_repo("INSERT INTO tags (repo_id, name, snap_id, created_at) VALUES (?, 'also', 2, 6)");
    assert_int_equal(repo_tags_save(db, repo_id, TEST_REPO), OK);

    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/" REPO_TAGS_FILE, &sb), 0);
    assert_int_equal(sb.st_mode & 0777, 0600);

    /* What a power cut takes: the newest commit, here both tag rows. */
    exec_repo("DELETE FROM tags WHERE repo_id = ?");
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?"), 0);

    submit_log_t log = { 0, 0 };
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.tags_restored, 2);
    assert_int_equal(rr.repos_missing, 0);
    assert_int_equal(log.calls, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ? AND name = 'keep'"
                            " AND snap_id = 1"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ? AND name = 'also'"
                            " AND snap_id = 2"), 1);

    /* Nothing to do the next time: the database wins and is complete. */
    assert_int_equal(index_reconcile(db, fake_submit, &log, &rr), OK);
    assert_int_equal(rr.tags_restored, 0);
}

/* A tag the database still has is never overwritten from the file, which
 * may be older (the database is the copy every query uses). */
static void test_database_tag_wins(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    exec_repo("INSERT INTO tags (repo_id, name, snap_id, created_at) VALUES (?, 'keep', 1, 5)");
    assert_int_equal(repo_tags_save(db, repo_id, TEST_REPO), OK);
    exec_repo("UPDATE tags SET snap_id = 2 WHERE repo_id = ? AND name = 'keep'");
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, NULL, NULL, &rr), OK);
    assert_int_equal(rr.tags_restored, 0);
    assert_int_equal(scalar("SELECT snap_id FROM tags WHERE repo_id = ? AND name = 'keep'"), 2);
}

/* ---------- B144: the tag file is written outside the write lock ---------- */

static char *read_tags_file(void)
{
    FILE *f = fopen(TEST_REPO "/" REPO_TAGS_FILE, "rb");
    if (!f) return NULL;
    static char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static api_request_t tag_req(api_method_t m, const char *body)
{
    api_request_t q;
    memset(&q, 0, sizeof(q));
    q.method = m;
    q.url = "/";
    q.body = body;
    q.body_len = body ? strlen(body) : 0;
    q.is_local = 1;
    q.client_addr = "";
    return q;
}

typedef struct {
    int         del;
    const char *body;
    const char *name;
    int         status;
} tag_call_t;

static void *tag_thread(void *arg)
{
    tag_call_t *c = arg;
    api_request_t q = tag_req(c->del ? API_DELETE : API_POST, c->body);
    api_response_t r = c->del ? metadata_tags_delete(&q, repo_id, c->name)
                              : metadata_tags_post(&q, repo_id);
    c->status = r.status;
    free(r.body);
    return NULL;
}

/* Whether the tag file comes to contain (or lose) needle within 5 s. */
static int tags_file_becomes(const char *needle, int present)
{
    for (int i = 0; i < 500; i++) {
        const char *t = read_tags_file();
        int has = t && strstr(t, needle) != NULL;
        if (has == present) return 1;
        struct timespec ts = { 0, 10 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return 0;
}

/*
 * A tag edit writes and syncs the repository's file before it queues for
 * the write lock, not inside its transaction (B144): with another writer
 * holding the lock, as a backup's batch does, the file already shows the
 * change while the edit waits, and the row lands once the lock is free.
 * Before the fix the file was written inside the transaction, so it could
 * not change until the holder let go, and every writer behind the edit
 * waited out its two fsyncs.
 */
static void test_tag_file_is_written_before_the_write_lock(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    routes_metadata_init(db);

    assert_int_equal(db_txn_begin(db), OK);           /* the backup's batch */
    tag_call_t set = { .body = "{\"name\":\"keep\",\"snap_id\":2}" };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, tag_thread, &set), 0);
    assert_true(tags_file_becomes("\"keep\"", 1));
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?"), 0);
    assert_int_equal(db_txn_commit(db), OK);
    pthread_join(th, NULL);
    assert_int_equal(set.status, 200);
    assert_int_equal(scalar("SELECT snap_id FROM tags WHERE repo_id = ? AND name = 'keep'"), 2);

    /* The file holds the tagged snapshot's timestamp, which is what lets
     * a reconcile put the tag back. */
    exec_repo("DELETE FROM tags WHERE repo_id = ?");
    reconcile_result_t rr;
    assert_int_equal(index_reconcile(db, NULL, NULL, &rr), OK);
    assert_int_equal(rr.tags_restored, 1);
    assert_int_equal(scalar("SELECT snap_id FROM tags WHERE repo_id = ? AND name = 'keep'"), 2);

    /* Removing it: the same order. */
    assert_int_equal(db_txn_begin(db), OK);
    tag_call_t del = { .del = 1, .name = "keep" };
    assert_int_equal(pthread_create(&th, NULL, tag_thread, &del), 0);
    assert_true(tags_file_becomes("\"keep\"", 0));
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?"), 1);
    assert_int_equal(db_txn_commit(db), OK);
    pthread_join(th, NULL);
    assert_int_equal(del.status, 200);
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?"), 0);
}

/* The file is the list as it will be after the commit: other tags kept in
 * name order, a moved tag on its new snapshot with its first created_at,
 * and a refused edit (409) leaves the file alone. */
static void test_tag_file_tracks_every_edit(void **state)
{
    (void)state;
    backup_once("one\n");
    backup_once("two\n");
    routes_metadata_init(db);

    const char *bodies[] = {
        "{\"name\":\"m\",\"snap_id\":1}",
        "{\"name\":\"a\",\"snap_id\":1}",
        "{\"name\":\"z\",\"snap_id\":2}",
    };
    for (size_t i = 0; i < 3; i++) {
        tag_call_t c = { .body = bodies[i] };
        tag_thread(&c);
        assert_int_equal(c.status, 200);
    }
    long long created_m = scalar("SELECT created_at FROM tags WHERE repo_id = ? AND name = 'm'");
    const char *t = read_tags_file();
    assert_non_null(t);
    const char *a = strstr(t, "\"a\""), *m = strstr(t, "\"m\""), *z = strstr(t, "\"z\"");
    assert_true(a && m && z && a < m && m < z);

    char before[4096];
    snprintf(before, sizeof(before), "%s", t);
    tag_call_t refused = { .body = "{\"name\":\"m\",\"snap_id\":2}" };
    tag_thread(&refused);
    assert_int_equal(refused.status, 409);
    assert_string_equal(read_tags_file(), before);

    tag_call_t moved = { .body = "{\"name\":\"m\",\"snap_id\":2,\"move\":true}" };
    tag_thread(&moved);
    assert_int_equal(moved.status, 200);
    char want[128];
    snprintf(want, sizeof(want), "{\"name\":\"m\",\"snapshot\":2,");
    assert_non_null(strstr(read_tags_file(), want));
    snprintf(want, sizeof(want), "\"created_at\":%lld}", created_m);
    const char *mm = strstr(read_tags_file(), "\"m\"");
    assert_non_null(mm);
    assert_non_null(strstr(mm, want));

    /* What the routes wrote is exactly what a rewrite from the rows gives. */
    snprintf(before, sizeof(before), "%s", read_tags_file());
    assert_int_equal(repo_tags_save(db, repo_id, TEST_REPO), OK);
    assert_string_equal(read_tags_file(), before);

    tag_call_t gone = { .del = 1, .name = "nosuch" };
    tag_thread(&gone);
    assert_int_equal(gone.status, 404);
    assert_string_equal(read_tags_file(), before);
}

/*
 * B218: the daemon stopping does not wait for a backup's index. The cancel
 * arrives as the trees are built, after every object is stored and before
 * the record is written: the record is still written (the work is kept),
 * and the index stops at its first row, reported the B195 way, as a power
 * cut there would leave it. A user's cancel at the same point lets the
 * index finish: the snapshot is complete.
 */
static void cancel_after_trees(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    atomic_int *cancel = ctx;
    if (phase && strcmp(phase, "trees") == 0 && total > 0 && done == total &&
        atomic_load(&cancel[1]))
        atomic_store(&cancel[0], atomic_load(&cancel[1]));
}

static status_t backup_cancelled_after_trees(int how, backup_result_t *res)
{
    static const char *paths[] = { TEST_SRC };
    atomic_int cancel[2];
    atomic_init(&cancel[0], 0);
    atomic_init(&cancel[1], how);
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.cancel = &cancel[0];
    o.progress_cb = cancel_after_trees;
    o.progress_ctx = (void *)cancel;
    memset(res, 0, sizeof(*res));
    err_clear();
    status_t st = backup_run(store, db, repo_id, &o, res);
    assert_int_equal(atomic_load(&cancel[0]), how);
    return st;
}

static void test_a_stop_does_not_wait_for_the_index(void **state)
{
    (void)state;
    write_file(TEST_SRC "/a.txt", "stopping\n");
    write_file(TEST_SRC "/b.txt", "stopping too\n");
    backup_result_t res;
    status_t st = backup_cancelled_after_trees(CANCEL_SHUTDOWN, &res);
    print_message("backup: %s\n", err_msg());
    assert_int_equal(st, ERR_BUSY);
    assert_non_null(strstr(err_msg(), "the daemon is stopping"));
    assert_non_null(strstr(err_msg(), "snapshot 1 was written to the repository"));
    assert_int_equal(res.snapshot_id, 1);
    assert_int_equal(res.snapshot_unindexed, 1);
    backup_result_free(&res);
    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/snapshots/1", &sb), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);

    /* What the next start does with it. */
    uint64_t missing = 0;
    assert_int_equal(index_reconcile_count(db, repo_id, TEST_REPO, &missing), OK);
    assert_int_equal(missing, 1);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 1);
    assert_true(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 1") > 0);
}

static void test_a_user_cancel_lets_the_index_finish(void **state)
{
    (void)state;
    write_file(TEST_SRC "/a.txt", "finishing\n");
    backup_result_t res;
    status_t st = backup_cancelled_after_trees(CANCEL_USER, &res);
    assert_int_equal(st, OK);
    assert_int_equal(res.snapshot_id, 1);
    backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 1);
    assert_true(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 1") > 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_complete_index_submits_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lost_tail_is_found_and_reindex_puts_it_back, setup, teardown),
        cmocka_unit_test_setup_teardown(test_several_lost, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_gap_below_the_highest_id_is_found_at_start, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_indexes_a_gap_before_it_sweeps, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_then_gc_keeps_a_gap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coalescing_keeps_a_gap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replication_carries_a_gap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_indexing_a_gap_commits_in_batches, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_refuses_a_gap_it_cannot_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_full_disk_in_index_nodes_reports_the_written_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_full_disk_at_the_snapshot_row_reports_the_written_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_stop_does_not_wait_for_the_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_user_cancel_lets_the_index_finish, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pruned_top_is_not_missing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_mark_falls_back_to_listing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_repo_is_skipped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_being_removed_is_skipped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lost_tag_is_restored_from_the_repository, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tag_file_is_written_before_the_write_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tag_file_tracks_every_edit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_database_tag_wins, setup, teardown),
    };
    if (full_vfs_install() != 0) return 1;
    if (sqlite3_auto_extension((void (*)(void))fill_ext) != SQLITE_OK) return 1;
    return cmocka_run_group_tests(tests, NULL, NULL);
}
