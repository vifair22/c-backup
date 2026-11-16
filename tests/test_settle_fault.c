#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * OWN-45-LIVE, decided as option B: a file that never settles while the
 * backup reads it is stored as the second read found it, and flagged.
 *
 * "Never settles" is made deterministic with the fault injector's read
 * hook: every read of the file appends to it, so the read and the re-read
 * both end on a file that moved. The hook fires on whatever thread reads,
 * which is what reaches the file workers and the small-file workers; the
 * progress callback test_settle_backup uses only reaches the backup thread.
 *
 * On each path the file must be stored, flagged in its tree entry (read
 * back from the stored tree object), listed in changed_unsettled, given no
 * file cache row, described in the journal and the alert text, warned
 * about by restore, noted by verify without failing it, and flagged again
 * after a reindex from nothing. backup.unsettled = skip leaves it out.
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
#include <limits.h>
#include <pthread.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/log.h>

#include "fault_inject.h"
#include "backup.h"
#include "manifest.h"
#include "object.h"
#include "reindex.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "routes.h"
#include "settle.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_settle_fault"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_OUT  TEST_DIR "/out"
#define TEST_KEYS TEST_DIR "/keys"

#define MiB ((size_t)1024 * 1024)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    fault_reset_all();
    fault_read_hook = NULL;
    fault_read_hook_path = NULL;
    settle_set_unsettled_policy(SETTLE_UNSETTLED_STORE_FLAGGED);
    rmrf(TEST_DIR);
    { int rc = system("mkdir -p '" TEST_DIR "'"); (void)rc; }
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_read_hook = NULL;
    fault_read_hook_path = NULL;
    settle_set_unsettled_policy(SETTLE_UNSETTLED_STORE_FLAGGED);
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void write_rand(const char *path, size_t n, uint64_t seed)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    uint64_t x = seed ? seed : 1;
    uint8_t buf[65536];
    while (n > 0) {
        for (size_t i = 0; i < sizeof(buf); i += 8) {
            x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
            uint64_t v = x * 0x2545F4914F6CDD1DULL;
            memcpy(buf + i, &v, 8);
        }
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        assert_int_equal(fwrite(buf, 1, k, f), k);
        n -= k;
    }
    fclose(f);
}

static void write_text(const char *path, const char *s)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    fputs(s, f);
    fclose(f);
}

/* Every read of the file appends to it: it never settles. Called on the
 * reading thread, so it takes care of its own serialisation. */
static pthread_mutex_t g_hook_mu = PTHREAD_MUTEX_INITIALIZER;
static int g_appends;

static void keep_writing(const char *path)
{
    pthread_mutex_lock(&g_hook_mu);
    int fd = open(path, O_WRONLY | O_APPEND);
    if (fd >= 0) {
        char tail[16];
        memset(tail, 'w', sizeof(tail));
        if (write(fd, tail, sizeof(tail)) == (ssize_t)sizeof(tail)) g_appends++;
        close(fd);
    }
    pthread_mutex_unlock(&g_hook_mu);
}

static char g_log[16384];
static pthread_mutex_t g_log_mu = PTHREAD_MUTEX_INITIALIZER;

static void capture(const char *ts, const char *level, const char *func,
                    const char *message, void *ud)
{
    (void)ts; (void)func; (void)ud;
    if (!strstr(message, "still changing") && !strstr(message, "never settled"))
        return;
    pthread_mutex_lock(&g_log_mu);
    size_t have = strlen(g_log);
    snprintf(g_log + have, sizeof(g_log) - have, "%s %s\n", level, message);
    pthread_mutex_unlock(&g_log_mu);
}

static int64_t scalar(const char *sql, const char *a1)
{
    db_result_t *r = NULL;
    if (db_execute(db, sql, (const char *[]){ a1, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    int64_t v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

/* The flags byte of a file's entry, read back from the stored tree object
 * of the directory holding it. */
static int tree_flags_of(int64_t snap, const char *dir, const char *name)
{
    char sid[32];
    snprintf(sid, sizeof(sid), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT subtree_hash FROM nodes WHERE snap_id = ?"
                                " AND name = ? AND type = 2",
                                (const char *[]){ sid, dir, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    uint8_t h[OBJECT_HASH_SIZE];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(r->rows[0][0] + i * 2, "%2x", &v), 1);
        h[i] = (uint8_t)v;
    }
    db_result_free(r);
    manifest_entry_t *ents = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, h, &ents, &n), OK);
    int flags = -1;
    for (uint32_t i = 0; i < n; i++)
        if (strcmp(ents[i].name, name) == 0) flags = ents[i].flags;
    manifest_free_entries(ents, n);
    return flags;
}

typedef enum { PATH_BACKUP_THREAD, PATH_FILE_WORKERS, PATH_SMALL_WORKERS } path_t;

/* Build the source for a path, the never-settling file in it, and return
 * that file's path and the worker count that reaches the path. */
static unsigned make_source(path_t which, char *busy, size_t busy_sz,
                            const char **dir, const char **name)
{
    char p[PATH_MAX];
    switch (which) {
    case PATH_BACKUP_THREAD:
        write_rand(TEST_SRC "/steady.bin", 2 * MiB, 3);
        snprintf(busy, busy_sz, TEST_SRC "/busy.bin");
        write_rand(busy, 24 * MiB + 1, 4);          /* several chunks */
        *dir = "src"; *name = "busy.bin";
        return 1;
    case PATH_FILE_WORKERS:
        /* Two large files, so eight workers start two file workers. */
        write_rand(TEST_SRC "/big-steady.bin", 20 * MiB, 5);
        snprintf(busy, busy_sz, TEST_SRC "/big-busy.bin");
        write_rand(busy, 24 * MiB + 1, 6);
        *dir = "src"; *name = "big-busy.bin";
        return 8;
    case PATH_SMALL_WORKERS:
        assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
        for (int i = 0; i < 600; i++) {
            snprintf(p, sizeof(p), TEST_SRC "/many/f%03d.txt", i);
            char body[48];
            snprintf(body, sizeof(body), "small file %d\n", i);
            write_text(p, body);
        }
        snprintf(busy, busy_sz, TEST_SRC "/many/busy.txt");
        write_rand(busy, 6000, 7);
        *dir = "many"; *name = "busy.txt";
        return 4;
    }
    return 1;
}

static backup_result_t backup_with_hook(unsigned workers, const char *busy)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    g_appends = 0;
    fault_read_hook_path = busy;
    fault_read_hook = keep_writing;
    backup_result_t res;
    status_t st = backup_run(store, db, repo_id, &o, &res);
    fault_read_hook = NULL;
    fault_read_hook_path = NULL;
    assert_int_equal(st, OK);
    assert_true(g_appends >= 2);
    return res;
}

static void run_never_settles(path_t which)
{
    char busy[PATH_MAX];
    const char *dir = NULL, *name = NULL;
    unsigned workers = make_source(which, busy, sizeof(busy), &dir, &name);

    g_log[0] = '\0';
    int h = log_stream_register(capture, NULL);
    backup_result_t res = backup_with_hook(workers, busy);

    /* Stored, not skipped, and listed as unsettled rather than changed. */
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.changed_unsettled, 1);
    assert_int_equal(res.unsettled_listed, 1);
    assert_string_equal(res.unsettled[0], busy);
    assert_int_equal(res.files_changed, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", name), 1);

    /* Flagged in the stored tree, and in the index row. */
    assert_int_equal(tree_flags_of(res.snapshot_id, dir, name), MANIFEST_FLAG_UNSETTLED);
    assert_int_equal(scalar("SELECT flags FROM nodes WHERE name = ?", name), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE flags != 0", NULL), 1);
    /* No cache row: the next backup reads it again. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM file_cache WHERE path = ?", busy), 0);

    /* The journal message and the alert name the count and the file. */
    char summary[PATH_MAX * 4], warning[PATH_MAX * 4];
    routes_backup_describe_doubts(&res, summary, sizeof(summary), warning, sizeof(warning));
    assert_non_null(strstr(summary, "1 stored while still changing (may be torn); first: "));
    assert_non_null(strstr(summary, busy));
    assert_non_null(strstr(warning, "1 file(s) were still changing"));
    assert_non_null(strstr(warning, "flagged as possibly torn; first: "));
    assert_non_null(strstr(warning, busy));

    /* Restore warns, once, for the file, and restores it. */
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, res.snapshot_id, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    log_stream_unregister(h);
    char want[PATH_MAX * 2];
    snprintf(want, sizeof(want), "warning restore: %s%s was still changing", TEST_OUT, busy);
    pthread_mutex_lock(&g_log_mu);
    int warned = strstr(g_log, want) != NULL;
    pthread_mutex_unlock(&g_log_mu);
    if (!warned) fail_msg("no restore warning for %s:\n%s", busy, g_log);

    /* Verify notes it and passes. */
    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing + vr.unrecoverable, 0);
    assert_int_equal(vr.unsettled_files, 1);
    char run[32];
    snprintf(run, sizeof(run), "%lld", (long long)vr.run_id);
    assert_int_equal(scalar("SELECT COUNT(*) FROM verify_findings WHERE run_id = ?"
                            " AND status LIKE 'notice: stored while still changing%'", run), 1);
    assert_int_equal(vr.affected_files, 0);

    /* The flag lives in the tree: a reindex from nothing brings it back. */
    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar("SELECT flags FROM nodes WHERE name = ?", name), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE flags != 0", NULL), 1);
    backup_result_free(&res);
}

static void test_never_settling_file_on_the_backup_thread(void **state)
{
    (void)state;
    run_never_settles(PATH_BACKUP_THREAD);
}

static void test_never_settling_file_on_the_file_workers(void **state)
{
    (void)state;
    run_never_settles(PATH_FILE_WORKERS);
}

static void test_never_settling_file_on_the_small_file_workers(void **state)
{
    (void)state;
    run_never_settles(PATH_SMALL_WORKERS);
}

/*
 * backup.unsettled = skip: the same file is left out, on every path, with
 * the B118 reason; = store puts it back. The values go through the same
 * parser the daemon uses at startup.
 */
static void run_policy_from_config(path_t which)
{
    char busy[PATH_MAX];
    const char *dir = NULL, *name = NULL;
    unsigned workers = make_source(which, busy, sizeof(busy), &dir, &name);

    int p = settle_unsettled_parse("skip");
    assert_int_equal(p, SETTLE_UNSETTLED_SKIP);
    settle_set_unsettled_policy((settle_unsettled_policy_t)p);
    backup_result_t res = backup_with_hook(workers, busy);
    assert_int_equal(res.changed_unsettled, 0);
    assert_int_equal(res.files_skipped, 1);
    assert_string_equal(res.skipped[0].path, busy);
    assert_non_null(strstr(res.skipped[0].reason, "again on a second read"));
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", name), 0);
    backup_result_free(&res);

    p = settle_unsettled_parse("store");
    assert_int_equal(p, SETTLE_UNSETTLED_STORE_FLAGGED);
    settle_set_unsettled_policy((settle_unsettled_policy_t)p);
    res = backup_with_hook(workers, busy);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.changed_unsettled, 1);
    assert_int_equal(tree_flags_of(res.snapshot_id, dir, name), MANIFEST_FLAG_UNSETTLED);
    backup_result_free(&res);
}

static void test_skip_policy_on_the_backup_thread(void **state)
{
    (void)state;
    run_policy_from_config(PATH_BACKUP_THREAD);
}

static void test_skip_policy_on_the_file_workers(void **state)
{
    (void)state;
    run_policy_from_config(PATH_FILE_WORKERS);
}

static void test_skip_policy_on_the_small_file_workers(void **state)
{
    (void)state;
    run_policy_from_config(PATH_SMALL_WORKERS);
}

static void test_policy_values(void **state)
{
    (void)state;
    assert_int_equal(settle_unsettled_parse("store"), SETTLE_UNSETTLED_STORE_FLAGGED);
    assert_int_equal(settle_unsettled_parse("skip"), SETTLE_UNSETTLED_SKIP);
    assert_int_equal(settle_unsettled_parse("maybe"), -1);
    assert_int_equal(settle_unsettled_parse(""), -1);
    assert_int_equal(settle_unsettled_parse(NULL), -1);
    /* The default, before anything sets it, is option B. */
    assert_int_equal(settle_unsettled_policy(), SETTLE_UNSETTLED_STORE_FLAGGED);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_policy_values, setup, teardown),
        cmocka_unit_test_setup_teardown(test_never_settling_file_on_the_backup_thread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_never_settling_file_on_the_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_never_settling_file_on_the_small_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_skip_policy_on_the_backup_thread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_skip_policy_on_the_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_skip_policy_on_the_small_file_workers, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
