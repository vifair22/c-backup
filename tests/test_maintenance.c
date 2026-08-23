#define _POSIX_C_SOURCE 200809L

/*
 * GC, prune and retention.
 *
 * GC is the operation with the worst failure mode in the whole system: a
 * sweep that is too aggressive deletes live data, and it does so quietly,
 * because nothing notices until a restore needs the object. The tests here
 * lean on that side deliberately, checking not just that garbage goes but
 * that shared and still-referenced objects stay.
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

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "gc.h"
#include "pack.h"
#include "prune.h"
#include "reindex.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_maint"
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
    write_file(TEST_SRC "/keep.txt", "keep me\n");
    write_file(TEST_SRC "/also.txt", "also keep\n");
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

static backup_opts_t basic_opts(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    return o;
}

static int count(const char *sql)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/* ------------------------------------------------------------------ */

/* Nothing is garbage while its snapshot exists. */
static void test_gc_keeps_everything_reachable(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    int before = count("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    assert_true(before > 0);

    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);
    assert_int_equal(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), before);
}

/* A dry run reports what would go without changing anything. */
static void test_gc_dry_run_changes_nothing(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    write_file(TEST_SRC "/keep.txt", "changed\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_int_equal(prune_snapshot(store, db, repo_id, br.snapshot_id), OK);

    int before = count("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 1, NULL, &g), OK);
    assert_true(g.swept > 0);
    assert_int_equal(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), before);
}

/* After pruning the only snapshot that referenced it, an object goes. */
static void test_gc_sweeps_after_prune(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first;
    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);

    write_file(TEST_SRC "/keep.txt", "different content entirely\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    int before = count("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    assert_int_equal(prune_snapshot(store, db, repo_id, first.snapshot_id), OK);

    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_true(g.swept > 0);
    assert_true(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?") < before);
}

/*
 * The case GC must never get wrong: an object present in two snapshots must
 * survive pruning one of them. Getting this backwards silently destroys data
 * that a restore will only miss much later.
 */
static void test_gc_keeps_objects_shared_by_snapshots(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first;
    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);
    /* also.txt is untouched, so both snapshots reference the same object. */
    write_file(TEST_SRC "/keep.txt", "only this one changed\n");
    backup_result_t second;
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    assert_int_equal(prune_snapshot(store, db, repo_id, first.snapshot_id), OK);
    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);

    /* The surviving snapshot must still restore completely. */
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, second.snapshot_id,
                                 TEST_DIR "/out", NULL, &rr), OK);
    assert_int_equal(rr.files, 2);
    assert_int_equal(rr.skipped, 0);
}

static void test_prune_removes_snapshot_and_rows(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    assert_true(count("SELECT COUNT(*) FROM nodes WHERE repo_id = ?") > 0);

    assert_int_equal(prune_snapshot(store, db, repo_id, br.snapshot_id), OK);

    assert_int_equal(count("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
    assert_int_equal(count("SELECT COUNT(*) FROM nodes WHERE repo_id = ?"), 0);

    /* The record on disk goes with it, or a reindex would resurrect it. */
    struct stat sb;
    char path[512];
    snprintf(path, sizeof(path), "%s/snapshots/%lld", TEST_REPO, (long long)br.snapshot_id);
    assert_int_not_equal(stat(path, &sb), 0);
}

/* The intent marker must not survive a successful prune, or every restart
 * would think a prune was interrupted. */
static void test_prune_clears_its_marker(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    assert_int_equal(prune_snapshot(store, db, repo_id, br.snapshot_id), OK);
    assert_int_equal(count("SELECT COUNT(*) FROM recovery_state WHERE repo_id = ?"), 0);
}

static void test_prune_of_unknown_snapshot_is_harmless(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    /* Deleting nothing is not an error; the end state is what was asked for. */
    assert_int_equal(prune_snapshot(store, db, repo_id, 999), OK);
    assert_int_equal(count("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 1);
}

/* GC must refuse to sweep when it cannot complete the mark, since an
 * incomplete mark set makes live objects look unreachable. */
/*
 * Packed objects are the case the sweep can get subtly wrong. Their bytes
 * stay until pack compaction, so it is tempting to leave the index alone
 * too, but a pack_objects row that outlives its object still resolves: a
 * restore would happily read bytes the sweep declared dead, and once
 * compaction rewrites the pack those bytes belong to something else.
 */
static void test_gc_clears_pack_locations_for_swept_objects(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first;
    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);

    /* Move everything into a pack, which populates pack_objects. */
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    int packed_rows = count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    assert_true(packed_rows > 0);

    write_file(TEST_SRC "/keep.txt", "content that displaces the first\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_int_equal(prune_snapshot(store, db, repo_id, first.snapshot_id), OK);

    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_true(g.swept > 0);

    /* No pack_objects row may survive its objects row. */
    assert_int_equal(count(
        "SELECT COUNT(*) FROM pack_objects po WHERE po.repo_id = ?"
        " AND NOT EXISTS (SELECT 1 FROM objects o"
        "                 WHERE o.repo_id = po.repo_id AND o.hash = po.hash)"), 0);
    assert_true(count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?") < packed_rows);
}

static void test_gc_refuses_to_sweep_on_unreadable_tree(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE snapshots SET root_tree_hash = X'00' WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, NULL), 0);

    int before = count("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    gc_result_t g;
    assert_int_not_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), before);
}

/* ---------- node id allocation ---------- */

static int64_t node_rows_for(int64_t snap)
{
    char repo[32], sn[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sn,   sizeof(sn),   "%lld", (long long)snap);
    db_result_t *r = NULL;
    int64_t n = -1;
    if (db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = ?",
                   (const char *[]){ repo, sn, NULL }, &r) == 0 && r && r->nrows == 1)
        n = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

/*
 * Two things allocate node ids: a backup, from the scan's own numbering, and
 * reindex, from MAX(id) in the table. They disagreed, and the insert was
 * INSERT OR REPLACE, so the first backup after a reindex overwrote an older
 * snapshot's rows with its own. That snapshot kept nodes_materialized = 1 and
 * had nothing left in it: browsing showed an empty tree and restoring failed
 * with "snapshot has no nodes", on a repository where the manifest trees were
 * perfectly intact the whole time.
 *
 * Nothing was permanently lost - another reindex rebuilds it - but a backup
 * silently emptying an older snapshot's index is not something to find out
 * about during a restore.
 */
static void test_backup_after_reindex_keeps_older_snapshots(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t b1, b2;
    assert_int_equal(backup_run(store, db, repo_id, &o, &b1), OK);
    write_file(TEST_SRC "/second.txt", "a second file\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, &b2), OK);

    int64_t n1 = node_rows_for(b1.snapshot_id);
    int64_t n2 = node_rows_for(b2.snapshot_id);
    assert_true(n1 > 0);
    assert_true(n2 > 0);

    /* Reindex renumbers from the table; the next backup must not collide. */
    assert_int_equal(reindex_snapshot(store, db, repo_id, b1.snapshot_id, NULL), OK);
    assert_int_equal(reindex_snapshot(store, db, repo_id, b2.snapshot_id, NULL), OK);

    backup_result_t b3;
    assert_int_equal(backup_run(store, db, repo_id, &o, &b3), OK);

    assert_int_equal(node_rows_for(b1.snapshot_id), n1);
    assert_int_equal(node_rows_for(b2.snapshot_id), n2);
    assert_true(node_rows_for(b3.snapshot_id) > 0);

    /* And the oldest still restores, which is what the rows are for. */
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, b1.snapshot_id,
                                 TEST_DIR "/out", NULL, &rr), OK);
    assert_true(rr.files > 0);
}

/* Every node id in a repository is unique, whoever allocated it. */
static void test_node_ids_do_not_repeat_across_snapshots(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    for (int i = 0; i < 3; i++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), TEST_SRC "/gen%d.txt", i);
        write_file(path, "generation\n");
        assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    }

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*), COUNT(DISTINCT id) FROM nodes WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], r->rows[0][1]);
    db_result_free(r);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gc_keeps_everything_reachable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_dry_run_changes_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_sweeps_after_prune, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_keeps_objects_shared_by_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_removes_snapshot_and_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_clears_its_marker, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_of_unknown_snapshot_is_harmless, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_clears_pack_locations_for_swept_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_refuses_to_sweep_on_unreadable_tree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_after_reindex_keeps_older_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_node_ids_do_not_repeat_across_snapshots, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
