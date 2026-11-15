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

extern const db_migration_t app_migrations[];

#define TEST_DB "/tmp/c_backup_test_schema.db"

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
        "repos", "schedules", "jobs", "sessions", "auth_config",
        "snapshots", "nodes", "hardlink_groups", "sparse_regions",
        "xattr_blobs", "xattr_refs", "acl_blobs", "acl_refs",
        "objects", "packs", "pack_objects", "tags", "notes",
        "policy_values", "journal", "recovery_state", NULL
    };
    for (int i = 0; tables[i]; i++)
        assert_true(object_exists(db, "table", tables[i]));

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
 * The child index must carry name as its trailing column. Without it,
 * ORDER BY name cannot be served from the index and SQLite sorts every child
 * before LIMIT applies: 1650 us for the first 500 of a 10k-entry directory
 * against 93 us with name included (PERF.md 2026-08-19). Directory listings
 * are name-ordered by definition, so losing this column is a silent 17x
 * regression on the most common query in the product.
 */
static void test_child_index_includes_name(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT sql FROM sqlite_master WHERE type='index'"
        " AND name='nodes_by_snap_parent'", NULL, &r), CUTILS_OK);
    assert_int_equal(r->nrows, 1);

    const char *ddl = r->rows[0][0];
    assert_non_null(ddl);
    assert_non_null(strstr(ddl, "parent_node_id"));
    assert_non_null(strstr(ddl, "name"));
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
        "INSERT INTO node_fts(name,node_id,snap_id,repo_id)"
        " VALUES ('quarterly_report.pdf',1,1,1)", NULL, NULL), CUTILS_OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO node_fts(name,node_id,snap_id,repo_id)"
        " VALUES ('holiday_photo.jpg',2,1,1)", NULL, NULL), CUTILS_OK);

    /* Substring, not prefix: the whole reason for trigram tokenisation. */
    assert_int_equal(count_rows(db,
        "SELECT node_id FROM node_fts WHERE name MATCH 'terly_rep'"), 1);
    assert_int_equal(count_rows(db,
        "SELECT node_id FROM node_fts WHERE name MATCH 'nothinghere'"), 0);

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

/*
 * Migration 018 rebuilds repos. Every row must keep its id and columns, the
 * child rows that refer to those ids must survive the DROP, and the next id
 * must clear ids that only orphaned rows still use.
 */
static void test_repos_rebuild_keeps_rows_and_children(void **state)
{
    (void)state;
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);

    /* Everything before 018, as a database written by the previous release. */
    size_t n = 0;
    while (app_migrations[n].name && strncmp(app_migrations[n].name, "018_", 4) != 0) n++;
    assert_non_null(app_migrations[n].name);   /* 018 exists */
    db_migration_t *before = calloc(n + 1, sizeof(*before));
    assert_non_null(before);
    for (size_t i = 0; i < n; i++) before[i] = app_migrations[i];
    assert_int_equal(db_run_compiled_migrations(db, before), CUTILS_OK);
    free(before);

    assert_int_equal(db_exec_raw(db,
        "INSERT INTO repos (id, uuid, path, label, added_at, encryption, key_escrowed)"
        "  VALUES (1, 'ua', '/r/a', 'alpha', 11, 'system', 1),"
        "         (4, 'ub', '/r/b', 'beta',  12, 'none',   0);"
        "INSERT INTO snapshots (repo_id, id, ts, root_tree_hash) VALUES (4, 1, 0, X'00');"
        "INSERT INTO schedules (repo_id, action, trigger_type, trigger_spec, enabled,"
        "                       next_due_at) VALUES (1, 'gc', 'interval', '{}', 1, 0);"
        /* Left behind by a repository 7 removed before the upgrade. */
        "INSERT INTO journal (repo_id, id, ts, source, operation, result)"
        "  VALUES (7, 1, 0, 'cli', 'backup', 'success');"), CUTILS_OK);

    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT id, uuid, path, label, added_at, encryption, key_escrowed"
        "  FROM repos ORDER BY id", NULL, &r), CUTILS_OK);
    assert_int_equal(r->nrows, 2);
    assert_string_equal(r->rows[0][0], "1");
    assert_string_equal(r->rows[0][3], "alpha");
    assert_string_equal(r->rows[0][5], "system");
    assert_string_equal(r->rows[0][6], "1");
    assert_string_equal(r->rows[1][0], "4");
    assert_string_equal(r->rows[1][4], "12");
    db_result_free(r);

    assert_int_equal(count_rows(db, "SELECT id FROM snapshots WHERE repo_id = 4"), 1);
    assert_int_equal(count_rows(db, "SELECT id FROM schedules WHERE repo_id = 1"), 1);
    assert_int_equal(count_rows(db, "SELECT id FROM journal WHERE repo_id = 7"), 1);

    /* Above the orphan, not just above the survivors. */
    assert_int_equal(insert_repo(db, "uc"), 8);

    /* And the children still name repos as their parent. */
    assert_int_equal(count_rows(db,
        "SELECT 1 FROM pragma_foreign_key_list('snapshots') WHERE \"table\" = 'repos'"), 1);

    db_close(db);
    unlink_db();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_repo_ids_are_not_reused),
        cmocka_unit_test(test_repos_rebuild_keeps_rows_and_children),
        cmocka_unit_test(test_migrations_apply),
        cmocka_unit_test(test_migrations_are_idempotent),
        cmocka_unit_test(test_child_index_includes_name),
        cmocka_unit_test(test_fts_search_works),
        cmocka_unit_test(test_snapshot_requires_root_tree),
        cmocka_unit_test(test_repo_delete_cascades),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
