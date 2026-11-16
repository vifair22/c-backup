#define _POSIX_C_SOURCE 200809L

/*
 * The node index is keyed by tree, not by snapshot (docs/index-reuse.md).
 *
 * A tree's entry rows are written once and shared by every snapshot that
 * holds the tree; each snapshot adds only its directory skeleton. These
 * tests hold the properties that makes true and the ones it must not break:
 * an unchanged backup writes no entry rows, a snapshot's contents read the
 * same whichever snapshot shares them, deleting a snapshot never takes rows
 * another still uses, reindex rebuilds the same index with the refcounts
 * gc's own mark produces, and what a stopped run leaves is cleaned up.
 *
 * Also here, because every one of these writes them: every hash column in
 * state.db holds a raw 32-byte BLOB (NIT-12), after backup, verify, gc and
 * reindex.
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

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "gc.h"
#include "node_index.h"
#include "prune.h"
#include "reindex.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_index_reuse"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo_s[32];

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
    snprintf(repo_s, sizeof(repo_s), "%lld", (long long)repo_id);
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    /* Eight directories of five files, and two identical directories, so a
     * tree held at two paths in one snapshot is covered too. */
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    char path[512];
    for (int d = 0; d < 8; d++) {
        snprintf(path, sizeof(path), "%s/dir%02d", TEST_SRC, d);
        if (mkdir(path, 0700) != 0) return -1;
        for (int f = 0; f < 5; f++) {
            char content[64];
            snprintf(path, sizeof(path), "%s/dir%02d/file%02d.txt", TEST_SRC, d, f);
            snprintf(content, sizeof(content), "content of dir %d file %d", d, f);
            write_file(path, content);
        }
    }
    /* The twins hold one file under two names (a hard link), so the two
     * directories' trees serialise to the same bytes: one tree, two paths. */
    if (mkdir(TEST_SRC "/twin0", 0700) != 0 || mkdir(TEST_SRC "/twin1", 0700) != 0)
        return -1;
    write_file(TEST_SRC "/twin0/same.txt", "identical");
    if (link(TEST_SRC "/twin0/same.txt", TEST_SRC "/twin1/same.txt") != 0) return -1;
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

static backup_result_t backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    return r;
}

static int64_t scalar(const char *sql)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, sql, (const char *[]){ repo_s, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    int64_t v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int64_t scalar_snap(const char *sql, int64_t snap)
{
    char s[32];
    snprintf(s, sizeof(s), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, sql, (const char *[]){ repo_s, s, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    int64_t v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

/* Everything a snapshot holds, as its paths and metadata in path order:
 * what browse, search, restore and grep all read, through the skeleton. */
static char *dump(int64_t snap)
{
    char s[32];
    snprintf(s, sizeof(s), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT d.path || '/' || n.name, n.type, n.mode, n.size, n.mtime_ns,"
        "       lower(hex(n.content_hash)), lower(hex(n.subtree_hash))"
        "  FROM snap_dirs d JOIN nodes n ON n.repo_id = d.repo_id"
        "   AND n.tree_hash = d.tree_hash"
        " WHERE d.repo_id = ? AND d.snap_id = ? ORDER BY 1",
        (const char *[]){ repo_s, s, NULL }, &r), 0);
    assert_non_null(r);
    size_t cap = 256, len = 0;
    char *out = malloc(cap);
    assert_non_null(out);
    out[0] = '\0';
    for (int i = 0; i < r->nrows; i++) {
        char line[2048];
        int w = snprintf(line, sizeof(line), "%s|%s|%s|%s|%s|%s|%s\n",
                         r->rows[i][0], r->rows[i][1], r->rows[i][2], r->rows[i][3],
                         r->rows[i][4], r->rows[i][5], r->rows[i][6]);
        assert_true(w > 0 && (size_t)w < sizeof(line));
        while (len + (size_t)w + 1 > cap) { cap *= 2; out = realloc(out, cap); assert_non_null(out); }
        memcpy(out + len, line, (size_t)w + 1);
        len += (size_t)w;
    }
    db_result_free(r);
    return out;
}

#define COUNT_NODES "SELECT COUNT(*) FROM nodes WHERE repo_id = ?"
#define COUNT_FTS   "SELECT COUNT(*) FROM node_fts WHERE repo_id = CAST(? AS INTEGER)"
#define COUNT_TREES "SELECT COUNT(*) FROM trees WHERE repo_id = ?"
#define COUNT_DIRS  "SELECT COUNT(*) FROM snap_dirs WHERE repo_id = ? AND snap_id = ?"
/* Entry rows no skeleton reaches: must stay zero after every operation. */
#define COUNT_UNREACHED \
    "SELECT COUNT(*) FROM nodes n WHERE n.repo_id = ?1 AND NOT EXISTS" \
    " (SELECT 1 FROM snap_dirs d WHERE d.repo_id = ?1 AND d.tree_hash = n.tree_hash)"

/* ------------------------------------------------------------------ */

static void test_unchanged_backup_writes_no_entry_rows(void **state)
{
    (void)state;
    backup_result_t r1 = backup();
    assert_true(r1.index_rows > 40);
    assert_true(r1.index_dirs > 10);
    int64_t nodes = scalar(COUNT_NODES);
    /* One row per entry of each distinct tree: the twins share theirs. */
    assert_int_equal(nodes, (int64_t)r1.index_rows);
    assert_int_equal(scalar(COUNT_FTS), nodes);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?"
                            " AND name = 'same.txt'"), 1);

    backup_result_t r2 = backup();
    assert_int_equal(r2.trees_new, 0);
    assert_int_equal(r2.index_rows, 0);
    assert_int_equal(r2.index_dirs, r1.index_dirs);
    assert_int_equal(scalar(COUNT_NODES), nodes);
    assert_int_equal(scalar(COUNT_FTS), nodes);
    assert_int_equal(scalar_snap(COUNT_DIRS, r2.snapshot_id), (int64_t)r1.index_dirs);

    /* Both snapshots read the same, through rows they share. */
    char *a = dump(r1.snapshot_id), *b = dump(r2.snapshot_id);
    assert_string_equal(a, b);
    assert_non_null(strstr(a, "/twin0/same.txt|"));
    assert_non_null(strstr(a, "/twin1/same.txt|"));
    free(a); free(b);
}

static void test_changed_file_writes_only_its_trees(void **state)
{
    (void)state;
    backup_result_t r1 = backup();
    char *before = dump(r1.snapshot_id);
    write_file(TEST_SRC "/dir03/file02.txt", "changed, and longer than it was");
    backup_result_t r2 = backup();

    /* dir03 and its ancestors: a handful of trees, not the snapshot. */
    assert_true(r2.index_rows > 0);
    assert_true(r2.index_rows < r1.index_rows / 2);
    assert_int_equal(scalar(COUNT_NODES), (int64_t)(r1.index_rows + r2.index_rows));

    char *b1 = dump(r1.snapshot_id), *b2 = dump(r2.snapshot_id);
    assert_string_equal(before, b1);          /* the old snapshot is untouched */
    assert_string_not_equal(b1, b2);

    node_ref_t ref;
    assert_int_equal(node_index_resolve(db, repo_id, r1.snapshot_id,
                                        TEST_SRC "/dir03/file02.txt", &ref), 1);
    int64_t old_node = ref.node_id;
    assert_false(ref.is_dir);
    assert_int_equal(node_index_resolve(db, repo_id, r2.snapshot_id,
                                        TEST_SRC "/dir03/file02.txt", &ref), 1);
    assert_true(ref.node_id != old_node);
    /* An unchanged file in an unchanged directory is the same row in both. */
    assert_int_equal(node_index_resolve(db, repo_id, r1.snapshot_id,
                                        TEST_SRC "/dir05/file01.txt", &ref), 1);
    old_node = ref.node_id;
    assert_int_equal(node_index_resolve(db, repo_id, r2.snapshot_id,
                                        TEST_SRC "/dir05/file01.txt", &ref), 1);
    assert_int_equal(ref.node_id, old_node);
    /* Directories are the snapshot's own. */
    int64_t d1, d2;
    assert_int_equal(node_index_resolve(db, repo_id, r1.snapshot_id, TEST_SRC "/dir05", &ref), 1);
    d1 = ref.dir_id;
    assert_int_equal(node_index_resolve(db, repo_id, r2.snapshot_id, TEST_SRC "/dir05", &ref), 1);
    d2 = ref.dir_id;
    assert_true(d1 != d2);
    assert_int_equal(node_index_resolve(db, repo_id, r2.snapshot_id, TEST_SRC "/nope", &ref), 0);
    assert_int_equal(node_index_resolve(db, repo_id, 999, TEST_SRC, &ref), -1);
    free(before); free(b1); free(b2);
}

static void test_prune_keeps_rows_another_snapshot_uses(void **state)
{
    (void)state;
    backup_result_t r1 = backup();
    write_file(TEST_SRC "/dir03/file02.txt", "changed again");
    backup_result_t r2 = backup();
    char *keep = dump(r2.snapshot_id);

    assert_int_equal(prune_snapshot(store, db, repo_id, r1.snapshot_id), OK);
    char *after = dump(r2.snapshot_id);
    assert_string_equal(keep, after);
    /* Only the trees snapshot 1 alone held went: the old dir03 and its
     * ancestors, as many rows as the second backup wrote for the new ones. */
    assert_int_equal(scalar(COUNT_NODES), (int64_t)r1.index_rows);
    assert_int_equal(scalar(COUNT_UNREACHED), 0);
    assert_int_equal(scalar(COUNT_FTS), scalar(COUNT_NODES));
    assert_int_equal(scalar_snap(COUNT_DIRS, r1.snapshot_id), 0);

    assert_int_equal(prune_snapshot(store, db, repo_id, r2.snapshot_id), OK);
    assert_int_equal(scalar(COUNT_NODES), 0);
    assert_int_equal(scalar(COUNT_FTS), 0);
    assert_int_equal(scalar(COUNT_TREES), 0);
    free(keep); free(after);
}

/* B046/B113's rule for every id that leaves the daemon. */
static void test_node_ids_are_never_reused(void **state)
{
    (void)state;
    backup_result_t r1 = backup();
    int64_t max_before = scalar("SELECT MAX(id) FROM snap_dirs WHERE repo_id = ?");
    int64_t max_node = scalar("SELECT MAX(id) FROM nodes WHERE repo_id = ?");
    if (max_node > max_before) max_before = max_node;
    assert_int_equal(prune_snapshot(store, db, repo_id, r1.snapshot_id), OK);
    assert_int_equal(scalar(COUNT_NODES), 0);
    backup();
    assert_true(scalar("SELECT MIN(id) FROM nodes WHERE repo_id = ?") > max_before);
    assert_true(scalar("SELECT MIN(id) FROM snap_dirs WHERE repo_id = ?") > max_before);
}

static db_result_t *refcounts(void)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT lower(hex(hash)), refcount FROM objects WHERE repo_id = ? ORDER BY hash",
        (const char *[]){ repo_s, NULL }, &r), 0);
    assert_non_null(r);
    return r;
}

static void test_reindex_rebuilds_the_same_index(void **state)
{
    (void)state;
    backup_result_t r1 = backup();
    write_file(TEST_SRC "/dir06/file00.txt", "a third version");
    backup_result_t r2 = backup();
    char *a1 = dump(r1.snapshot_id), *a2 = dump(r2.snapshot_id);
    int64_t nodes = scalar(COUNT_NODES);

    /* From nothing, as after a lost database. */
    assert_int_equal(db_exec_raw(db,
        "DELETE FROM node_fts; DELETE FROM nodes; DELETE FROM trees;"
        " DELETE FROM snap_dirs; DELETE FROM snapshots;"), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 2);
    /* Each distinct tree read once, however many snapshots hold it. */
    assert_int_equal((int64_t)rx.nodes, nodes);
    assert_int_equal(scalar(COUNT_NODES), nodes);
    assert_int_equal(scalar(COUNT_FTS), nodes);
    char *b1 = dump(r1.snapshot_id), *b2 = dump(r2.snapshot_id);
    assert_string_equal(a1, b1);
    assert_string_equal(a2, b2);
    assert_int_equal(scalar_snap("SELECT file_count FROM snapshots WHERE repo_id = ? AND id = ?",
                                 r2.snapshot_id), (int64_t)r2.files);

    /* And over an index that is already there: replaced, not doubled. */
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar(COUNT_NODES), nodes);
    assert_int_equal(scalar(COUNT_UNREACHED), 0);
    char *c2 = dump(r2.snapshot_id);
    assert_string_equal(a2, c2);

    /* The refcounts reindex derives from the skeleton are the ones gc's
     * mark counts by walking the trees. */
    db_result_t *from_reindex = refcounts();
    gc_result_t gr;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);
    db_result_t *from_gc = refcounts();
    assert_int_equal(from_reindex->nrows, from_gc->nrows);
    for (int i = 0; i < from_gc->nrows; i++) {
        assert_string_equal(from_reindex->rows[i][0], from_gc->rows[i][0]);
        assert_string_equal(from_reindex->rows[i][1], from_gc->rows[i][1]);
    }
    db_result_free(from_reindex);
    db_result_free(from_gc);
    free(a1); free(a2); free(b1); free(b2); free(c2);
}

static void test_stopped_runs_are_cleaned_up(void **state)
{
    (void)state;
    backup_result_t r1 = backup();
    int64_t nodes = scalar(COUNT_NODES);

    /* A skeleton a stopped backup left under its marker, for a snapshot
     * that never got its row; the next backup drops it. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO recovery_state (repo_id, key, value)"
        " VALUES (?, 'index-pending-99', '99')", (const char *[]){ repo_s, NULL }, NULL), 0);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO snap_dirs (id, repo_id, snap_id, parent_id, name, path, tree_hash)"
        " SELECT 900000, repo_id, 99, 0, '', '', tree_hash FROM snap_dirs"
        "  WHERE repo_id = ? AND parent_id = 0 LIMIT 1",
        (const char *[]){ repo_s, NULL }, NULL), 0);
    assert_int_equal(scalar_snap(COUNT_DIRS, 99), 1);
    backup_result_t r2 = backup();
    assert_int_equal(scalar_snap(COUNT_DIRS, 99), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM recovery_state WHERE repo_id = ?"
                            " AND key LIKE 'index-pending-%'"), 0);
    assert_int_equal(scalar_snap(COUNT_DIRS, r2.snapshot_id), (int64_t)r1.index_dirs);

    /* A complete tree no snapshot reaches, and a tree whose rows were cut
     * off before its completion mark: gc sweeps both. */
    assert_int_equal(db_exec_raw(db,
        "INSERT INTO trees (repo_id, hash, files, bytes)"
        " SELECT id, X'0000000000000000000000000000000000000000000000000000000000000001', 0, 0 FROM repos;"
        "INSERT INTO nodes (id, repo_id, tree_hash, name, type, mode, uid, gid,"
        "                   mtime_ns, size)"
        " SELECT 910000, id, X'0000000000000000000000000000000000000000000000000000000000000001', 'orphan', 1, 33188, 0, 0, 0, 0 FROM repos;"
        "INSERT INTO nodes (id, repo_id, tree_hash, name, type, mode, uid, gid,"
        "                   mtime_ns, size)"
        " SELECT 910001, id, X'0000000000000000000000000000000000000000000000000000000000000002', 'partial', 1, 33188, 0, 0, 0, 0 FROM repos;"
        "INSERT INTO node_fts (rowid, name, repo_id) SELECT 910000, 'orphan', id FROM repos;"
        "INSERT INTO node_fts (rowid, name, repo_id) SELECT 910001, 'partial', id FROM repos;"), 0);
    assert_int_equal(scalar(COUNT_NODES), nodes + 2);
    gc_result_t gr;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);
    assert_int_equal(scalar(COUNT_NODES), nodes);
    assert_int_equal(scalar(COUNT_FTS), nodes);
    assert_int_equal(scalar(COUNT_UNREACHED), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM trees t WHERE repo_id = ?1 AND NOT EXISTS"
                            " (SELECT 1 FROM snap_dirs d WHERE d.repo_id = ?1"
                            "   AND d.tree_hash = t.hash)"), 0);
}

/*
 * NIT-12: every hash column holds a raw 32-byte BLOB. The columns are found
 * from the schema rather than listed, so a hash column added later is held
 * to the same rule without anyone remembering this test.
 */
static void assert_hash_columns_raw(int need_findings)
{
    db_result_t *cols = NULL;
    assert_int_equal(db_execute(db,
        "SELECT m.name, p.name FROM sqlite_master m, pragma_table_info(m.name) p"
        " WHERE m.type = 'table' AND m.name NOT LIKE 'node_fts%'"
        "   AND m.name NOT LIKE 'sqlite_%'"
        "   AND (p.name = 'hash' OR p.name LIKE '%\\_hash' ESCAPE '\\')",
        NULL, &cols), 0);
    assert_non_null(cols);
    int seen = 0;
    for (int i = 0; i < cols->nrows; i++) {
        char sql[512];
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL"
                 " AND (typeof(%s) <> 'blob' OR length(%s) <> 32)",
                 cols->rows[i][0], cols->rows[i][1], cols->rows[i][1], cols->rows[i][1]);
        db_result_t *r = NULL;
        assert_int_equal(db_execute(db, sql, NULL, &r), 0);
        if (strcmp(r->rows[0][0], "0") != 0)
            fail_msg("%s.%s holds %s value(s) that are not 32-byte BLOBs",
                     cols->rows[i][0], cols->rows[i][1], r->rows[0][0]);
        db_result_free(r);
        seen++;
    }
    /* objects, pack_objects, file_cache, snapshots, verify_findings, trees,
     * snap_dirs and nodes' five. */
    assert_true(seen >= 12);
    db_result_free(cols);

    static const char *populated[] = {
        "SELECT COUNT(*) FROM objects WHERE repo_id = ?",
        "SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?",
        "SELECT COUNT(*) FROM file_cache WHERE repo_id = ?",
        "SELECT COUNT(*) FROM snapshots WHERE repo_id = ?",
        "SELECT COUNT(*) FROM trees WHERE repo_id = ?",
        "SELECT COUNT(*) FROM snap_dirs WHERE repo_id = ?",
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND content_hash IS NOT NULL",
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND subtree_hash IS NOT NULL",
        NULL
    };
    for (int i = 0; populated[i]; i++) assert_true(scalar(populated[i]) > 0);
    if (need_findings)
        assert_true(scalar("SELECT COUNT(*) FROM verify_findings WHERE repo_id = ?") > 0);
}

static void test_hash_columns_hold_raw_32_bytes(void **state)
{
    (void)state;
    backup();
    write_file(TEST_SRC "/dir01/file04.txt", "different bytes");
    backup();
    assert_hash_columns_raw(0);

    /* A finding, from an object the index names and no pack holds. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO objects (repo_id, hash, refcount, location, type)"
        " VALUES (?, X'0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',"
        "         0, 'absent', 1)", (const char *[]){ repo_s, NULL }, NULL), 0);
    verify_result_t vr;
    status_t vst = verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0, NULL, NULL, NULL, &vr);
    (void)vst;   /* a missing object fails the run; the finding is the point */
    assert_hash_columns_raw(1);

    gc_result_t gr;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);
    assert_hash_columns_raw(1);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_hash_columns_raw(1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_unchanged_backup_writes_no_entry_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_changed_file_writes_only_its_trees, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prune_keeps_rows_another_snapshot_uses, setup, teardown),
        cmocka_unit_test_setup_teardown(test_node_ids_are_never_reused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_rebuilds_the_same_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stopped_runs_are_cleaned_up, setup, teardown),
        cmocka_unit_test_setup_teardown(test_hash_columns_hold_raw_32_bytes, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
