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
#include "node_queries.h"

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
        "auth_config", "snapshots", "snapshot_id_seq", "nodes", "node_fts",
        "objects", "packs", "pack_objects", "tags", "notes",
        "policy_values", "repo_backup_paths", "journal", "recovery_state",
        "file_cache", "verify_runs", "verify_findings", "grep_runs",
        "grep_hits", "snapshot_units", "snapshot_unit_items", "unit_policy",
        "unit_intents", "daemon_state", "pack_tombstones", "db_findings", NULL
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
    assert_int_equal(count_rows(db, "SELECT name FROM pragma_table_info('file_cache')"
                                    " WHERE name = 'db_head'"), 1);

    db_close(db);
    unlink_db();
}

/*
 * Format 3 starts the schema again from one baseline: every state.db is
 * rebuilt (register and reindex), so there is no upgrade path to carry.
 * A second migration belongs after it; a renamed or rewritten baseline
 * would fail every existing database's checksum.
 */
/*
 * Once the v3 image is cut, state.db files in the field hold daemon-only
 * state no reindex can bring back: schedules, tags, policies, sessions.
 * A schema change after that is a numbered migration applied on top, and
 * it must leave all of that alone. This builds a database at the baseline
 * alone, fills it the way a running daemon would, then applies the rest.
 */
static void test_units_migration_keeps_baseline_data(void **state)
{
    (void)state;
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    const db_migration_t baseline_only[] = { app_migrations[0], { NULL, NULL } };
    assert_string_equal(baseline_only[0].name, "001_v3_baseline.sql");
    assert_int_equal(db_run_compiled_migrations(db, baseline_only), CUTILS_OK);
    assert_false(object_exists(db, "table", "snapshot_units"));

    static const char *const fill[] = {
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (1, 'u-1', '/r', 'main', 1)",
        "INSERT INTO snapshots (repo_id, id, ts, root_tree_hash) VALUES (1, 7, 100, 'aa')",
        "INSERT INTO tags (repo_id, name, snap_id, created_at) VALUES (1, 'keep', 7, 5)",
        "INSERT INTO policy_values (repo_id, key, value) VALUES (1, 'retention.keep_daily', '9')",
        "INSERT INTO schedules (repo_id, action, params, trigger_type, trigger_spec, next_due_at)"
        " VALUES (1, 'gc', '', 'interval', '{\"type\":\"interval\",\"seconds\":60}', 200)",
        "INSERT INTO auth_config (key, value) VALUES ('admin_hash', 'x')",
        NULL,
    };
    for (int i = 0; fill[i]; i++) assert_int_equal(db_exec_raw(db, fill[i]), CUTILS_OK);

    /* Then the daemon is upgraded: every later migration applies. */
    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    assert_true(object_exists(db, "table", "snapshot_units"));
    assert_true(object_exists(db, "table", "unit_intents"));
    assert_true(object_exists(db, "table", "unit_tokens"));
    assert_true(object_exists(db, "table", "unit_incremental"));
    assert_int_equal(count_rows(db, "SELECT 1 FROM tags WHERE name = 'keep' AND snap_id = 7"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM policy_values WHERE value = '9'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM schedules WHERE action = 'gc'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM auth_config WHERE key = 'admin_hash'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM snapshots WHERE id = 7"), 1);
    /* An existing snapshot has no unit row: it stays a file snapshot. */
    assert_int_equal(count_rows(db, "SELECT 1 FROM snapshot_units"), 0);
    /* And the new tables work against the old rows. */
    assert_int_equal(db_exec_raw(db, "INSERT INTO unit_policy (repo_id, unit_key, key, value)"
                                     " VALUES (1, 'fake:u', 'retention.keep_daily', '3')"),
                     CUTILS_OK);
    db_close(db);
    unlink_db();
}

/*
 * B155: a schedule stored before created_at existed. A never-fired interval
 * schedule is given the instant its stored due time was counted from; one
 * that has fired, and a calendar one, keep NULL, since neither anchors to
 * its creation.
 */
static void test_created_at_backfill(void **state)
{
    (void)state;
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    const db_migration_t before[] = { app_migrations[0], app_migrations[1], { NULL, NULL } };
    assert_string_equal(before[1].name, "002_units.sql");
    assert_int_equal(db_run_compiled_migrations(db, before), CUTILS_OK);

    static const char *const fill[] = {
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (1, 'u-1', '/r', 'main', 1)",
        "INSERT INTO schedules (id, repo_id, action, trigger_type, trigger_spec, next_due_at)"
        " VALUES (1, 1, 'backup', 'interval', '{\"type\":\"interval\",\"seconds\":86400}', 1086400)",
        "INSERT INTO schedules (id, repo_id, action, trigger_type, trigger_spec, last_fired_at, next_due_at)"
        " VALUES (2, 1, 'gc', 'interval', '{\"type\":\"interval\",\"seconds\":60}', 500, 560)",
        "INSERT INTO schedules (id, repo_id, action, trigger_type, trigger_spec, next_due_at)"
        " VALUES (3, 1, 'verify', 'calendar',"
        " '{\"type\":\"calendar\",\"every\":\"day\",\"at\":\"03:00\"}', 900)",
        NULL,
    };
    for (int i = 0; fill[i]; i++) assert_int_equal(db_exec_raw(db, fill[i]), CUTILS_OK);

    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    assert_int_equal(count_rows(db, "SELECT 1 FROM schedules WHERE id = 1 AND created_at = 1000000"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM schedules WHERE id = 2 AND created_at IS NULL"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM schedules WHERE id = 3 AND created_at IS NULL"), 1);
    db_close(db);
    unlink_db();
}

/*
 * B217: journal ids were MAX(id) + 1 per repository and are AUTOINCREMENT
 * now. The rebuild keeps a repository's ids unless one of them is also
 * used by a repository with a lower id; those repositories' entries move,
 * in order, above every old id. So no id a client held names a different
 * entry, and each repository's order holds.
 */
static void test_journal_ids_rebuilt_without_reuse(void **state)
{
    (void)state;
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    db_migration_t before[9];
    int nb = 0;
    for (; app_migrations[nb].name &&
           strcmp(app_migrations[nb].name, "009_journal_autoincrement.sql") != 0; nb++)
        before[nb] = app_migrations[nb];
    assert_int_equal(nb, 7);
    before[nb] = (db_migration_t){ NULL, NULL };
    assert_int_equal(db_run_compiled_migrations(db, before), CUTILS_OK);

    static const char *const fill[] = {
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (1, 'u-1', '/a', 'a', 1)",
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (2, 'u-2', '/b', 'b', 1)",
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (3, 'u-3', '/c', 'c', 1)",
        /* Repo 1: ids 1-3. Repo 2: 1-2, colliding, so it moves. Repo 3:
         * 7-8, used nowhere lower, so it keeps them. */
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (1, 1, 10, 'api', 'a1', 'success')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (1, 2, 10, 'api', 'a2', 'success')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (1, 3, 11, 'api', 'a3', 'success')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (2, 1, 20, 'api', 'b1', 'success')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (2, 2, 20, 'api', 'b2', 'success')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (3, 7, 30, 'api', 'c7', 'success')",
        "INSERT INTO journal (repo_id, id, ts, source, operation, result) VALUES (3, 8, 31, 'api', 'c8', 'success')",
        NULL,
    };
    for (int i = 0; fill[i]; i++) assert_int_equal(db_exec_raw(db, fill[i]), CUTILS_OK);

    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal"), 7);
    /* Kept. */
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE repo_id = 1 AND id = 1 AND operation = 'a1'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE repo_id = 1 AND id = 3 AND operation = 'a3'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE repo_id = 3 AND id = 7 AND operation = 'c7'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE repo_id = 3 AND id = 8 AND operation = 'c8'"), 1);
    /* Moved above every old id, in their order. */
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE repo_id = 2 AND id = 9 AND operation = 'b1'"), 1);
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE repo_id = 2 AND id = 10 AND operation = 'b2'"), 1);
    /* The next entry goes above them all, and the route's index is back. */
    assert_int_equal(db_exec_raw(db, "INSERT INTO journal (repo_id, ts, source, operation, result)"
                                     " VALUES (1, 40, 'api', 'a4', 'success')"), CUTILS_OK);
    assert_int_equal(count_rows(db, "SELECT 1 FROM journal WHERE id = 11 AND operation = 'a4'"), 1);
    assert_true(object_exists(db, "index", "journal_by_ts"));
    assert_false(object_exists(db, "table", "journal_v1"));
    assert_false(object_exists(db, "table", "journal_mig_moved"));
    db_close(db);
    unlink_db();
}

/*
 * Node index generations: every snapshot already indexed when 010 applies
 * gets a generation of its own, and the sequence starts above them, so the
 * first rebuild after the upgrade draws a value no snapshot holds.
 */
static void test_node_index_gen_backfill(void **state)
{
    (void)state;
    unlink_db();
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), CUTILS_OK);
    assert_int_equal(db_run_lib_migrations(db), CUTILS_OK);
    db_migration_t before[10];
    int nb = 0;
    for (; app_migrations[nb].name &&
           strcmp(app_migrations[nb].name, "010_node_index_gen.sql") != 0; nb++)
        before[nb] = app_migrations[nb];
    assert_int_equal(nb, 8);
    before[nb] = (db_migration_t){ NULL, NULL };
    assert_int_equal(db_run_compiled_migrations(db, before), CUTILS_OK);

    static const char *const fill[] = {
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (1, 'u-1', '/a', 'a', 1)",
        "INSERT INTO repos (id, uuid, path, label, added_at) VALUES (2, 'u-2', '/b', 'b', 1)",
        "INSERT INTO snapshots (repo_id, id, ts, root_tree_hash) VALUES (1, 1, 1, x'00')",
        "INSERT INTO snapshots (repo_id, id, ts, root_tree_hash) VALUES (1, 5, 1, x'00')",
        "INSERT INTO snapshots (repo_id, id, ts, root_tree_hash) VALUES (2, 1, 1, x'00')",
        NULL,
    };
    for (int i = 0; fill[i]; i++) assert_int_equal(db_exec_raw(db, fill[i]), CUTILS_OK);

    assert_int_equal(db_run_compiled_migrations(db, app_migrations), CUTILS_OK);
    assert_int_equal(count_rows(db, "SELECT 1 FROM node_index_gen"), 3);
    assert_int_equal(count_rows(db, "SELECT DISTINCT gen FROM node_index_gen WHERE gen > 0"), 3);
    assert_int_equal(db_exec_raw(db, "INSERT INTO node_gen_seq DEFAULT VALUES"), CUTILS_OK);
    assert_int_equal(count_rows(db, "SELECT 1 FROM node_gen_seq"
                                    " WHERE gen > (SELECT MAX(gen) FROM node_index_gen)"), 1);
    assert_true(object_exists(db, "table", "node_index_gen"));
    db_close(db);
    unlink_db();
}

static void test_one_baseline_migration(void **state)
{
    (void)state;
    assert_non_null(app_migrations[0].name);
    assert_string_equal(app_migrations[0].name, "001_v3_baseline.sql");
    /* Units (source-providers.md), the first migration after it. */
    assert_non_null(app_migrations[1].name);
    assert_string_equal(app_migrations[1].name, "002_units.sql");
    /* NBD exports of unit disks (vm-backup.md 7.3). */
    assert_non_null(app_migrations[2].name);
    assert_string_equal(app_migrations[2].name, "003_exports.sql");
    /* Incremental VM backup's tokens (vm-backup.md 5.8). */
    assert_non_null(app_migrations[3].name);
    assert_string_equal(app_migrations[3].name, "004_unit_tokens.sql");
    /* B155: the schedule creation time, the never-fired interval anchor. */
    assert_non_null(app_migrations[4].name);
    assert_string_equal(app_migrations[4].name, "006_schedule_created_at.sql");
    /* Dropped packs' numbers (B156). */
    assert_non_null(app_migrations[5].name);
    assert_string_equal(app_migrations[5].name, "007_pack_tombstones.sql");
    /* Reserved id marks (B217). */
    assert_non_null(app_migrations[6].name);
    assert_string_equal(app_migrations[6].name, "008_id_reserve.sql");
    /* Journal ids never reissued (B217). */
    assert_non_null(app_migrations[7].name);
    assert_string_equal(app_migrations[7].name, "009_journal_autoincrement.sql");
    /* Node index generations. */
    assert_non_null(app_migrations[8].name);
    assert_string_equal(app_migrations[8].name, "010_node_index_gen.sql");
    /* Database findings and the file cache's head classification
     * (db-detection.md 7.1). */
    assert_non_null(app_migrations[9].name);
    assert_string_equal(app_migrations[9].name, "011_db_findings.sql");
    /* Their coverage in words, method and optional suggestion (7.4). */
    assert_non_null(app_migrations[10].name);
    assert_string_equal(app_migrations[10].name, "012_db_findings_detail.sql");
    /* The snapshot a running backup is writing (B238). */
    assert_non_null(app_migrations[11].name);
    assert_string_equal(app_migrations[11].name, "013_job_snapshot.sql");
    /* Every snapshot a running job writes, and how many (B267). */
    assert_non_null(app_migrations[12].name);
    assert_string_equal(app_migrations[12].name, "014_job_snapshots.sql");
    /* Partial indexes for verify's node rules (B268). */
    assert_non_null(app_migrations[13].name);
    assert_string_equal(app_migrations[13].name, "014_verify_node_rules.sql");
    assert_null(app_migrations[14].name);

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

/* ------------------------------------------------------------------ */
/* Query plans for the node index's read paths                         */
/* ------------------------------------------------------------------ */

/*
 * The index tests above prove the indexes exist. These prove the queries
 * use them, which is the part that fails silently: a query that stops using
 * its index still returns the right rows, and at fixture sizes it is no
 * slower. The UI e2e suite measured exactly that (FLAKE-11): over 2,402
 * names a LIKE scan of nodes took 0.5 ms and the route's FTS query 1.8 ms,
 * so no timing there can see the difference. The plan can.
 *
 * The SQL is node_queries.h, the same strings the route and grep execute.
 *
 * The data is the e2e fixture's shape at ten snapshots, ANALYZEd, so the
 * planner chooses with statistics the way it does on a real database rather
 * than from its no-stats defaults. Under STAT4 a bound value can change a
 * plan, so each search is planned with a broad query (every name matches)
 * and a narrow one (one name per snapshot).
 */

#define PLAN_SNAPS     10
#define PLAN_PER_SNAP  1200

static void seed_plan_data(cutils_db_t *db)
{
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO repos (id,uuid,path,added_at) VALUES (1,'u','/tmp/r',0)",
        NULL, NULL), CUTILS_OK);
    char snaps[16], per[16];
    snprintf(snaps, sizeof(snaps), "%d", PLAN_SNAPS);
    snprintf(per, sizeof(per), "%d", PLAN_PER_SNAP);
    /* The CASTs matter: c-utils binds every parameter as text, an integer
     * always compares below text, and an uncast bound never stops. */
    assert_int_equal(db_execute_non_query(db,
        "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s"
        "                         WHERE i < CAST(? AS INTEGER))"
        " INSERT INTO snapshots (repo_id,id,ts,root_tree_hash)"
        " SELECT 1, i, i, X'00' FROM s",
        (const char *[]){ snaps, NULL }, NULL), CUTILS_OK);
    /* Per snapshot: one directory at the root and every other node a file
     * in it. Node ids are global to the repository, as the daemon's are. */
    assert_int_equal(db_execute_non_query(db,
        "WITH RECURSIVE k(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM k"
        "                         WHERE i < CAST(?1 AS INTEGER) * ?2 - 1)"
        " INSERT INTO nodes (repo_id,id,snap_id,parent_node_id,name,type,mode,"
        "                    uid,gid,mtime_ns,size)"
        " SELECT 1, i + 1, i / ?2 + 1,"
        "        CASE WHEN i % ?2 = 0 THEN 0 ELSE (i / ?2) * ?2 + 1 END,"
        "        CASE WHEN i % ?2 = 0 THEN 'wide'"
        "             ELSE printf('f%05d.txt', i % ?2) END,"
        "        CASE WHEN i % ?2 = 0 THEN 2 ELSE 1 END, 420, 0, 0, i, 9"
        "   FROM k",
        (const char *[]){ snaps, per, NULL }, NULL), CUTILS_OK);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO node_fts (name,node_id,snap_id,repo_id)"
        " SELECT name, id, snap_id, repo_id FROM nodes", NULL, NULL), CUTILS_OK);
    assert_int_equal(db_exec_raw(db, "ANALYZE"), CUTILS_OK);
}

/*
 * What no plan here may contain, as a reason, or NULL when the line is clean.
 *
 *   SCAN of a table        every row read. The one SCAN allowed is the FTS
 *                          table driven by MATCH, whose idxStr carries an M;
 *                          driven by LIKE it carries an L instead.
 *   SEARCH on (repo_id=?)  a "search" that reads every node in the
 *                          repository. A LIKE over nodes plans exactly like
 *                          this, which is why "no SCAN" alone is not enough.
 *   TEMP B-TREE            a sort of every match before LIMIT applies: what
 *                          the name column in nodes_by_snap_parent and the
 *                          rowid order on node_fts exist to avoid.
 *   AUTOMATIC index        SQLite building an index per query because the
 *                          schema lacks the one it wants.
 *   ANY(column)            a skip-scan: the leading columns are not
 *                          constrained, so every distinct prefix is walked.
 *                          With statistics, a name lookup that lost its
 *                          parent plans as a SEARCH of nodes_by_snap_parent
 *                          like this, not as a SCAN.
 */
static const char *plan_fault(const char *line)
{
    static const char repo_only[] = "(repo_id=?)";
    const size_t ro = sizeof(repo_only) - 1;
    size_t n = strlen(line);

    if (strstr(line, "TEMP B-TREE")) return "sorts before LIMIT";
    if (strstr(line, "AUTOMATIC")) return "builds an index per query";
    if (strstr(line, "ANY(")) return "skip-scans an index";
    if (n >= ro && strcmp(line + n - ro, repo_only) == 0)
        return "reads every node in the repository";
    if (strncmp(line, "SCAN ", 5) == 0) {
        static const char vt_tag[] = "VIRTUAL TABLE INDEX ";
        const char *vt = strstr(line, vt_tag);
        if (!vt) return "scans a table";
        const char *idx = strchr(vt + sizeof(vt_tag) - 1, ':');
        if (!idx || !strchr(idx, 'M')) return "reads the FTS table without MATCH";
    }
    return NULL;
}

/* The plan of sql with these parameters, its detail lines joined with
 * newlines into buf. Returns the first fault, or NULL. */
static const char *plan_of(cutils_db_t *db, const char *sql, const char **params,
                           char *buf, size_t sz)
{
    char eqp[2048];
    assert_true((size_t)snprintf(eqp, sizeof(eqp), "EXPLAIN QUERY PLAN %s", sql)
                < sizeof(eqp));
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, eqp, params, &r), CUTILS_OK);
    assert_non_null(r);
    assert_true(r->ncols >= 4);

    const char *fault = NULL;
    size_t used = 0;
    buf[0] = '\0';
    for (int i = 0; i < r->nrows; i++) {
        const char *line = r->rows[i][3];
        int w = snprintf(buf + used, sz - used, "%s%s", i ? "\n" : "", line);
        if (w > 0 && (size_t)w < sz - used) used += (size_t)w;
        if (!fault) fault = plan_fault(line);  /* a literal, outlives r */
    }
    db_result_free(r);
    return fault;
}

static void expect_plan(cutils_db_t *db, const char *what, const char *sql,
                        const char **params, const char *const *must)
{
    char plan[2048];
    const char *fault = plan_of(db, sql, params, plan, sizeof(plan));
    if (fault) fail_msg("%s: %s\n%s", what, fault, plan);
    for (int i = 0; must[i]; i++)
        if (!strstr(plan, must[i]))
            fail_msg("%s: plan lacks \"%s\"\n%s", what, must[i], plan);
}

#define BY_PK      "(repo_id=? AND id=?)"
#define BY_PARENT  "nodes_by_snap_parent (repo_id=? AND snap_id=? AND parent_node_id=?"

static void test_node_queries_use_their_indexes(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();
    seed_plan_data(db);

    static const char *const search_must[] = {
        "SCAN f VIRTUAL TABLE INDEX ", "SEARCH n USING", BY_PK,
        /* The generation column: one primary-key lookup per row. */
        "SEARCH g USING INDEX sqlite_autoindex_node_index_gen_1 (repo_id=? AND snap_id=?)",
        NULL
    };
    const struct { const char *label, *q; } queries[] = {
        { "broad",  "\"txt\""    },
        { "narrow", "\"f00042\"" },
    };
    for (size_t i = 0; i < sizeof(queries) / sizeof(queries[0]); i++) {
        char what[96];
        snprintf(what, sizeof(what), "name search, every snapshot, %s", queries[i].label);
        expect_plan(db, what, NODE_SQL_SEARCH_ALL,
                    (const char *[]){ "1", "1", queries[i].q, "8000", NULL }, search_must);
        snprintf(what, sizeof(what), "name search, newer snapshot, %s", queries[i].label);
        expect_plan(db, what, NODE_SQL_SEARCH_SNAP_NEWEST,
                    (const char *[]){ "1", "1", queries[i].q, "9", "1000", NULL }, search_must);
        snprintf(what, sizeof(what), "name search, older snapshot, %s", queries[i].label);
        expect_plan(db, what, NODE_SQL_SEARCH_SNAP_OLDEST,
                    (const char *[]){ "1", "1", queries[i].q, "2", "1000", NULL }, search_must);
    }

    expect_plan(db, "search result ancestry", NODE_SQL_ANCESTOR,
                (const char *[]){ "1", "42", NULL },
                (const char *const[]){ "SEARCH nodes USING", BY_PK, NULL });

    /* The wide directory and the root, which the statistics say hold very
     * different numbers of rows. */
    const char *parents[] = { "1201", "0" };
    for (size_t i = 0; i < sizeof(parents) / sizeof(parents[0]); i++)
        expect_plan(db, "directory listing", NODE_SQL_LIST_DIR,
                    (const char *[]){ "1", "2", parents[i], "500", "0", NULL },
                    (const char *const[]){ "SEARCH n USING INDEX " BY_PARENT,
                                           "SEARCH c USING COVERING INDEX " BY_PARENT,
                                           NULL });

    expect_plan(db, "directory in snapshot", NODE_SQL_DIR_IN_SNAP,
                (const char *[]){ "1", "2", "1201", NULL },
                (const char *const[]){ "SEARCH nodes USING", BY_PK, NULL });
    expect_plan(db, "snapshot residency", NODE_SQL_SNAP_RESIDENT,
                (const char *[]){ "1", "2", NULL },
                (const char *const[]){ "SEARCH nodes USING COVERING INDEX "
                                       "nodes_by_snap_parent (repo_id=? AND snap_id=?)",
                                       NULL });
    expect_plan(db, "path step", NODE_SQL_PATH_STEP,
                (const char *[]){ "1", "2", "1201", "f00042.txt", NULL },
                (const char *const[]){ "SEARCH nodes USING INDEX " BY_PARENT
                                       " AND name=?)", NULL });

    db_close(db);
    unlink_db();
}

/*
 * The checker has to see the regressions it exists for, or the test above
 * passes by finding nothing. Each of these is a plausible edit to one of the
 * queries, and each must be refused.
 */
static void test_plan_check_catches_regressions(void **state)
{
    (void)state;
    cutils_db_t *db = fresh_db();
    seed_plan_data(db);

    struct { const char *what, *sql; const char *params[4]; } bad[] = {
        { "LIKE over nodes, the scan the trigram index replaced",
          "SELECT n.id FROM nodes n WHERE n.repo_id = ? AND n.name LIKE ?",
          { "1", "%txt%", NULL } },
        { "LIKE over the FTS table",
          "SELECT f.node_id FROM node_fts f WHERE f.repo_id = ? AND f.name LIKE ?"
          " ORDER BY f.rowid DESC LIMIT 10", { "1", "%txt%", NULL } },
        { "name lookup without its parent",
          "SELECT id FROM nodes WHERE name = ?", { "f00042.txt", NULL } },
        { "directory in another order",
          "SELECT id FROM nodes n WHERE n.repo_id = ? AND n.snap_id = ?"
          " AND n.parent_node_id = ? ORDER BY n.mtime_ns LIMIT 5",
          { "1", "2", "1201", NULL } },
        { "search ordered by name",
          "SELECT f.node_id FROM node_fts f WHERE f.name MATCH ?"
          " ORDER BY f.name LIMIT 10", { "\"txt\"", NULL } },
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char plan[2048];
        const char *fault = plan_of(db, bad[i].sql, bad[i].params, plan, sizeof(plan));
        if (!fault) fail_msg("not caught: %s\n%s", bad[i].what, plan);
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
        cmocka_unit_test(test_journal_ids_rebuilt_without_reuse),
        cmocka_unit_test(test_node_index_gen_backfill),
        cmocka_unit_test(test_created_at_backfill),
        cmocka_unit_test(test_units_migration_keeps_baseline_data),
        cmocka_unit_test(test_migrations_are_idempotent),
        cmocka_unit_test(test_child_index_includes_name),
        cmocka_unit_test(test_fts_search_works),
        cmocka_unit_test(test_snapshot_requires_root_tree),
        cmocka_unit_test(test_repo_delete_cascades),
        cmocka_unit_test(test_node_queries_use_their_indexes),
        cmocka_unit_test(test_plan_check_catches_regressions),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
