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
#include "db_txn.h"
#include "routes.h"
#include "index_reconcile.h"
#include "reindex.h"
#include "repo.h"
#include "repo_key.h"
#include "repo_tags.h"
#include "snapshot_ids.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_complete_index_submits_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lost_tail_is_found_and_reindex_puts_it_back, setup, teardown),
        cmocka_unit_test_setup_teardown(test_several_lost, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pruned_top_is_not_missing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_mark_falls_back_to_listing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_repo_is_skipped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_being_removed_is_skipped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lost_tag_is_restored_from_the_repository, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tag_file_is_written_before_the_write_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tag_file_tracks_every_edit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_database_tag_wins, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
