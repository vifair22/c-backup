/*
 * Schema migration tests.
 *
 * The daemon's database is a derived index: losing it costs time, not data.
 * That only holds if it can actually be rebuilt, which starts with the schema
 * applying cleanly and carrying the indexes the query paths were designed
 * around. Two of those indexes exist for measured reasons and are easy to
 * drop by accident during a later migration, so they are asserted explicitly.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmocka.h>

#include <cutils/db.h>
#include <cutils/error.h>
#include "test_tmp.h"

extern const db_migration_t app_migrations[];

#define TEST_DB TEST_TMP "/c_backup_test_schema.db"

static void unlink_db(void)
{
    unlink(TEST_DB);
    unlink(TEST_DB "-wal");
    unlink(TEST_DB "-shm");
}

static cutils_db_t *fresh_db(void)
{
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_non_null(db);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    return db;
}

static int count_rows(cutils_db_t *db, const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(db, sql, NULL, &r) != CUTILS_OK) return -1;
    int n = r ? r->nrows : -1;
    db_result_free(r);
    return n;
}

static int object_exists(cutils_db_t *db, const char *type, const char *name)
{
    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT name FROM sqlite_master WHERE type='%s' AND name='%s'",
             type, name);
    return count_rows(db, sql) == 1;
}

/* ------------------------------------------------------------------ */

static void test_migrations_apply(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    const char *tables[] = {
        "repos", "repo_purge_pending", "schedules", "jobs", "sessions",
        "auth_config", "snapshots", "snapshot_id_seq", "node_id_seq", "nodes",
        "trees", "snap_dirs", "node_fts",
        "objects", "packs", "pack_objects", "tags", "notes",
        "policy_values", "repo_backup_paths", "journal", "recovery_state",
        "file_cache", "verify_runs", "verify_findings", "grep_runs",
        "grep_hits", NULL
    };
    for (int i = 0; tables[i]; i++)
        assert_true(object_exists(db, "table", tables[i]));

    /* Folded out of the baseline: nothing read or wrote them. */
    const char *gone[] = {
        "hardlink_groups", "sparse_regions", "xattr_blobs", "xattr_refs",
        "acl_blobs", "acl_refs", "job_history", NULL
    };
    for (int i = 0; gone[i]; i++)
        assert_false(object_exists(db, "table", gone[i]));
    assert_int_equal(count_rows(db, "SELECT name FROM pragma_table_info('nodes')"
                                    " WHERE name IN ('xattr_ref','acl_ref','sparse_ref')"), 0);

    db_close(db);
    unlink_db();
}

/*
 * Format 3 starts the schema again from one baseline: every state.db is
 * rebuilt (register and reindex), so there is no upgrade path to carry.
 * A second migration belongs after it; a renamed or rewritten baseline
 * would fail every existing database's checksum.
 */
static void test_one_baseline_migration(void **state)
{
    (void)state;
    assert_non_null(app_migrations[0].name);
    assert_string_equal(app_migrations[0].name, "001_v3_baseline.sql");
    assert_null(app_migrations[1].name);

    /* A database written before the baseline already has its tables, so
     * opening it fails rather than running on a schema it does not know. */
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    assert_int_equal(db_exec_raw(db, "CREATE TABLE repos (id INTEGER PRIMARY KEY)"),
                     CUTILS_OK);
    assert_int_not_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    db_close(db);
    unlink_db();
}

static void test_migrations_are_idempotent(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    /* A second pass must be a no-op rather than an error: the daemon runs
     * migrations on every start. */
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    assert_true(object_exists(db, "table", "snapshots"));

    db_close(db);
    unlink_db();
}

/*
 * The directory index must carry name as its trailing column. Without it,
 * ORDER BY name cannot be served from the index and SQLite sorts every child
 * before LIMIT applies: 1650 us for the first 500 of a 10k-entry directory
 * against 93 us with name included (PERF.md 2026-08-19). Directory listings
 * are name-ordered by definition, so losing this column is a silent 17x
 * regression on the most common query in the product. A directory's
 * entries are its tree's rows (docs/index-reuse.md), so the index leads
 * with the tree.
 */
static void test_child_index_includes_name(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT sql FROM sqlite_master WHERE type='index'"
        " AND name='nodes_by_tree'", NULL, &r), CUTILS_OK);
    assert_int_equal(r->nrows, 1);

    const char *ddl = r->rows[0][0];
    assert_non_null(ddl);
    assert_non_null(strstr(ddl, "tree_hash, name"));
    db_result_free(r);

    db_close(db);
    unlink_db();
}

/*
 * Filename search needs a trigram index. A B-tree cannot serve LIKE
 * '%needle%', and scanning nodes loses to the pre-rewrite .snap search path.
 * Backfilling this later means rewriting every node row in every repo, so it
 * has to be in the first migration.
 */
static void test_fts_search_works(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    assert_true(object_exists(db, "table", "node_fts"));

    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO node_fts(rowid,name,repo_id)"
        " VALUES (1,'quarterly_report.pdf',1)", NULL, NULL), CUTILS_OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO node_fts(rowid,name,repo_id)"
        " VALUES (2,'holiday_photo.jpg',1)", NULL, NULL), CUTILS_OK);

    /* Substring, not prefix: the whole reason for trigram tokenisation. */
    assert_int_equal(count_rows(db,
        "SELECT rowid FROM node_fts WHERE name MATCH 'terly_rep'"), 1);
    assert_int_equal(count_rows(db,
        "SELECT rowid FROM node_fts WHERE name MATCH 'nothinghere'"), 0);

    db_close(db);
    unlink_db();
}

/*
 * root_tree_hash is the entry point into the manifest tree graph and the
 * column that makes the database disposable. A snapshot without one cannot
 * be reindexed, so the schema must refuse to record it.
 */
static void test_snapshot_requires_root_tree(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id,uuid,path,added_at) VALUES (1,'u','/tmp/r',0)",
        NULL, NULL), CUTILS_OK);

    assert_int_not_equal(db_execute_non_query(db,
        "INSERT INTO snapshots (repo_id,id,ts,root_tree_hash)"
        " VALUES (1,1,0,NULL)", NULL, NULL), CUTILS_OK);

    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO snapshots (repo_id,id,ts,root_tree_hash)"
        " VALUES (1,1,0,X'00112233')", NULL, NULL), CUTILS_OK);

    db_close(db);
    unlink_db();
}

/* Deleting a repo must take its derived rows with it; a stale index over a
 * repo that no longer exists is worse than no index. */
static void test_repo_delete_cascades(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    assert_int_equal(db_exec_raw(db, "PRAGMA foreign_keys = ON"), CUTILS_OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id,uuid,path,added_at) VALUES (1,'u','/tmp/r',0)",
        NULL, NULL), CUTILS_OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO snapshots (repo_id,id,ts,root_tree_hash)"
        " VALUES (1,1,0,X'00')", NULL, NULL), CUTILS_OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO objects (repo_id,hash,location,type)"
        " VALUES (1,X'aa','loose',1)", NULL, NULL), CUTILS_OK);

    assert_int_equal(db_execute_non_query(db,
        "DELETE FROM repos WHERE id=1", NULL, NULL), CUTILS_OK);

    assert_int_equal(count_rows(db, "SELECT id FROM snapshots"), 0);
    assert_int_equal(count_rows(db, "SELECT hash FROM objects"), 0);

    db_close(db);
    unlink_db();
}

static int64_t insert_repo(cutils_db_t *db, const char *uuid)
{
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (uuid, path, added_at) VALUES (?, '/tmp/r', 0)",
        (const char *[]){ uuid, NULL }, NULL), CUTILS_OK);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT id FROM repos WHERE uuid = ?",
                                (const char *[]){ uuid, NULL }, &r), CUTILS_OK);
    int64_t id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return id;
}

/* B046: removing the newest repository and adding another handed out the
 * same id, and with it the old repository's leftover rows. */
static void test_repo_ids_are_not_reused(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();
    assert_int_equal(insert_repo(db, "u1"), 1);
    assert_int_equal(insert_repo(db, "u2"), 2);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM repos WHERE id = 2",
                                          NULL, NULL), CUTILS_OK);
    assert_int_equal(insert_repo(db, "u3"), 3);
    db_close(db);
    unlink_db();
}


static int64_t scalar64(cutils_db_t *db, const char *sql)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, sql, NULL, &r), CUTILS_OK);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    int64_t v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

/* One row's id from an INSERT ... RETURNING id. */
#define JOB_INSERT    "INSERT INTO jobs (repo_id, action, state) VALUES (1, 'gc', 'queued') RETURNING id"
#define SCHED_INSERT  "INSERT INTO schedules (repo_id, action, trigger_type, trigger_spec," \
                      " next_due_at) VALUES (1, 'gc', 'interval', '{}', 0) RETURNING id"
#define GREP_INSERT   "INSERT INTO grep_runs (repo_id, snap_id, pattern, started_at)" \
                      " VALUES (1, 1, 'x', 0) RETURNING id"
#define VERIFY_INSERT "INSERT INTO verify_runs (repo_id, started_at, level)" \
                      " VALUES (1, 0, 'index') RETURNING id"

/*
 * B113: every table whose id leaves the daemon. Deleting the newest row and
 * inserting another handed out the deleted id again: the purge of a removed
 * repository, the 30-day job cleanup and deleting a schedule all did it.
 */
static void test_job_schedule_and_run_ids_are_not_reused(void **state)
{
    (void)state;
    static const struct { const char *table, *insert; } t[] = {
        { "jobs",        JOB_INSERT },
        { "schedules",   SCHED_INSERT },
        { "grep_runs",   GREP_INSERT },
        { "verify_runs", VERIFY_INSERT },
    };
    cutils_db_t *db = fresh_db();
    assert_int_equal(insert_repo(db, "u1"), 1);
    for (size_t i = 0; i < sizeof(t) / sizeof(t[0]); i++) {
        assert_int_equal(scalar64(db, t[i].insert), 1);
        assert_int_equal(scalar64(db, t[i].insert), 2);
        char del[128];
        snprintf(del, sizeof(del), "DELETE FROM %s", t[i].table);
        assert_int_equal(db_execute_non_query(db, del, NULL, NULL), CUTILS_OK);
        assert_int_equal(scalar64(db, t[i].insert), 3);
    }
    db_close(db);
    unlink_db();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_job_schedule_and_run_ids_are_not_reused),
        cmocka_unit_test(test_repo_ids_are_not_reused),
        cmocka_unit_test(test_migrations_apply),
        cmocka_unit_test(test_one_baseline_migration),
        cmocka_unit_test(test_migrations_are_idempotent),
        cmocka_unit_test(test_child_index_includes_name),
        cmocka_unit_test(test_fts_search_works),
        cmocka_unit_test(test_snapshot_requires_root_tree),
        cmocka_unit_test(test_repo_delete_cascades),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
