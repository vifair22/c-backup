#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * Engine copies for plain file sources (docs/db-detection.md 9.2, 9.3,
 * 9.5, 11.1): SQLite's online backup through the daemon's library against a
 * database a writer thread keeps committing to, the rollback-journal rule,
 * a copy abandoned when the source stays locked, the raw file read once
 * when it has a copy, the copy and its index in the snapshot, a restore that
 * prefers the copy and leaves the stale -wal out, and the messages that
 * explain an unsettled database.
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
#include <stdatomic.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <sqlite3.h>

#include "backup.h"
#include "db_capture.h"
#include "db_findings.h"
#include "db_write.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "sqlite_copy.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_db_capture"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_OUT  TEST_DIR "/out"
#define TEST_KEYS TEST_DIR "/keys"
#define TEST_TMPD TEST_DIR "/scratch"

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
    rmrf(TEST_DIR);
    { int rc = system("mkdir -p '" TEST_SRC "' '" TEST_TMPD "'"); (void)rc; }
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    db_detect_settings_t s = { 1, 1024, 1 };
    db_detect_settings_set(&s);
    sqlite_copy_set_busy_ms(0);
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

static void exec_ok(sqlite3 *h, const char *sql)
{
    char *err = NULL;
    int rc = sqlite3_exec(h, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) fprintf(stderr, "sqlite: %s: %s\n", sql, err ? err : "?");
    sqlite3_free(err);
    assert_int_equal(rc, SQLITE_OK);
}

static sqlite3 *make_db(const char *path, int wal)
{
    sqlite3 *h = NULL;
    assert_int_equal(sqlite3_open(path, &h), SQLITE_OK);
    exec_ok(h, wal ? "PRAGMA journal_mode=WAL" : "PRAGMA journal_mode=DELETE");
    exec_ok(h, "CREATE TABLE ledger(id INTEGER PRIMARY KEY, pad BLOB)");
    exec_ok(h, "INSERT INTO ledger(pad) VALUES (randomblob(4000))");
    return h;
}

/* integrity_check and the highest ledger id of a database file, -1 when it
 * will not open or is damaged. */
static long long check_db(const char *path)
{
    sqlite3 *h = NULL;
    long long top = -1;
    if (sqlite3_open_v2(path, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(h); return -1; }
    sqlite3_stmt *q = NULL;
    if (sqlite3_prepare_v2(h, "PRAGMA integrity_check", -1, &q, NULL) == SQLITE_OK &&
        sqlite3_step(q) == SQLITE_ROW &&
        strcmp((const char *)sqlite3_column_text(q, 0), "ok") == 0) {
        sqlite3_finalize(q);
        q = NULL;
        if (sqlite3_prepare_v2(h, "SELECT MAX(id) FROM ledger", -1, &q, NULL) == SQLITE_OK &&
            sqlite3_step(q) == SQLITE_ROW)
            top = sqlite3_column_int64(q, 0);
    }
    sqlite3_finalize(q);
    sqlite3_close(h);
    return top;
}

typedef struct {
    const char *path;
    atomic_int  stop;
    atomic_llong committed;
} writer_t;

static void *writer_main(void *arg)
{
    writer_t *w = arg;
    sqlite3 *h = NULL;
    if (sqlite3_open(w->path, &h) != SQLITE_OK) return NULL;
    sqlite3_busy_timeout(h, 5000);
    while (!atomic_load(&w->stop)) {
        if (sqlite3_exec(h, "INSERT INTO ledger(pad) VALUES (randomblob(4000))", NULL, NULL, NULL) == SQLITE_OK)
            atomic_store(&w->committed, sqlite3_last_insert_rowid(h));
    }
    sqlite3_close(h);
    return NULL;
}

/* ------------------------------------------------------------------ */

static void test_copy_wal_under_writer(void **state)
{
    (void)state;
    sqlite3 *h = make_db(TEST_SRC "/app.db", 1);
    writer_t w = { TEST_SRC "/app.db", 0, 0 };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, writer_main, &w), 0);
    while (atomic_load(&w.committed) < 50) usleep(1000);
    for (int k = 0; k < 3; k++) {
        long long before = atomic_load(&w.committed);
        sqlite_copy_result_t r;
        sqlite_copy(TEST_SRC "/app.db", TEST_TMPD "/copy.db", 0, &r);
        assert_int_equal(r.outcome, SQLITE_COPY_OK);
        assert_int_equal(r.wal, 1);
        assert_true(r.bytes > 0);
        /* A committed state: whole, and at least what was committed
         * before the copy began. */
        long long top = check_db(TEST_TMPD "/copy.db");
        assert_true(top >= before);
    }
    atomic_store(&w.stop, 1);
    pthread_join(th, NULL);
    sqlite3_close(h);
    assert_string_equal(sqlite_copy_outcome_name(SQLITE_COPY_OK), "ok");
    assert_string_equal(sqlite_copy_outcome_name(SQLITE_COPY_ABANDONED), "abandoned");
}

static void test_copy_rollback_rule(void **state)
{
    (void)state;
    sqlite3 *h = make_db(TEST_SRC "/r.db", 0);
    sqlite3_close(h);
    sqlite_copy_result_t r;
    sqlite_copy(TEST_SRC "/r.db", TEST_TMPD "/r.copy", 0, &r);
    assert_int_equal(r.outcome, SQLITE_COPY_REFUSED);
    assert_non_null(strstr(r.reason, "allow_rollback_lock"));
    assert_int_equal(access(TEST_TMPD "/r.copy", F_OK), -1);
    sqlite_copy(TEST_SRC "/r.db", TEST_TMPD "/r.copy", 1, &r);
    assert_int_equal(r.outcome, SQLITE_COPY_OK);
    assert_int_equal(check_db(TEST_TMPD "/r.copy"), 1);

    /* Not a database, and no file at all. */
    FILE *f = fopen(TEST_SRC "/notes.db", "w");
    fputs("a text file, not a database, despite the name", f);
    fclose(f);
    sqlite_copy(TEST_SRC "/notes.db", TEST_TMPD "/n.copy", 1, &r);
    assert_int_not_equal(r.outcome, SQLITE_COPY_OK);
    assert_true(strlen(r.reason) > 0);
    sqlite_copy(TEST_SRC "/missing.db", TEST_TMPD "/m.copy", 1, &r);
    assert_int_not_equal(r.outcome, SQLITE_COPY_OK);
}

/* A rollback database another connection holds locked: three tries, then
 * abandoned, and nothing left behind. */
static void test_copy_abandoned(void **state)
{
    (void)state;
    sqlite3 *h = make_db(TEST_SRC "/busy.db", 0);
    exec_ok(h, "BEGIN EXCLUSIVE");
    exec_ok(h, "INSERT INTO ledger(pad) VALUES (zeroblob(10))");
    sqlite_copy_set_busy_ms(20);
    sqlite_copy_result_t r;
    sqlite_copy(TEST_SRC "/busy.db", TEST_TMPD "/b.copy", 1, &r);
    exec_ok(h, "COMMIT");
    sqlite3_close(h);
    assert_true(r.outcome == SQLITE_COPY_ABANDONED || r.outcome == SQLITE_COPY_REFUSED);
    if (r.outcome == SQLITE_COPY_ABANDONED) {
        assert_int_equal(r.tries, SQLITE_COPY_TRIES);
        assert_non_null(strstr(r.reason, "abandoned"));
    }
    assert_int_equal(access(TEST_TMPD "/b.copy", F_OK), -1);
}

/* ------------------------------------------------------------------ */
/* The raw file, read once                                              */
/* ------------------------------------------------------------------ */

static void write_rand(const char *path, size_t n, uint64_t seed, const char *mode)
{
    FILE *f = fopen(path, mode);
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

typedef struct { const char *path; int seen, changes; } grow_t;

static void grow_once(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    grow_t *g = ctx;
    if (strcmp(phase, "store") != 0 || total == 0 || done + 1 != total) return;
    if (++g->seen < 2 || g->changes) return;
    g->changes++;
    write_rand(g->path, 3 * MiB, 91, "ab");
}

static backup_result_t run_grow(int copied)
{
    const char *path = TEST_DIR "/big.db";
    write_rand(path, 24 * MiB + 3, 5, "wb");
    static const char *one[1];
    one[0] = path;
    grow_t g = { path, 0, 0 };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = one;
    o.n_paths = 1;
    o.store_workers = 1;
    o.progress_cb = grow_once;
    o.progress_ctx = &g;
    o.detect = db_detect_start();
    if (copied) db_detect_copy_add(o.detect, path);
    assert_int_equal(db_detect_is_copied(o.detect, path), copied);
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(g.changes, 1);
    assert_int_equal(db_detect_was_unsettled(o.detect, path), copied);
    db_detect_free(o.detect);
    return res;
}

static void test_raw_read_once_with_copy(void **state)
{
    (void)state;
    /* Without a copy: moved once, read again, stored as one version. */
    backup_result_t res = run_grow(0);
    assert_int_equal(res.files_changed, 1);
    assert_int_equal(res.changed_unsettled, 0);
    backup_result_free(&res);
    /* With one: read once and flagged, never re-read (9.5). */
    res = run_grow(1);
    assert_int_equal(res.changed_unsettled, 1);
    assert_int_equal(res.files_skipped, 0);
    backup_result_free(&res);
}

/* Verify's unsettled notice explains a detected database (9.5), and a
 * restore of it names its finding. */
static void test_verify_and_restore_explain(void **state)
{
    (void)state;
    backup_result_t res = run_grow(1);
    int64_t snap = res.snapshot_id;
    backup_result_free(&res);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_write(db, "INSERT INTO db_findings (repo_id, scope, engine, path, confidence,"
                                  " state, evidence, first_seen, last_seen)"
                                  " VALUES (?, 'source:" TEST_DIR "/big.db', 'sqlite', '" TEST_DIR "/big.db',"
                                  " 'live', 'new', '{}', 1, 1)",
                              (const char *[]){ repo, NULL }, NULL), 0);
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0, NULL, NULL, NULL, &vr), OK);
    assert_true(vr.unsettled_files >= 1);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT sample_paths FROM verify_findings WHERE repo_id = ?"
                                    " AND status LIKE 'notice: stored while changing: live SQLite%'",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], TEST_DIR "/big.db");
    db_result_free(r);
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.engine_copies, 0);
    assert_int_equal(rr.files, 1);
}

/* ------------------------------------------------------------------ */
/* A file backup with an engine copy                                    */
/* ------------------------------------------------------------------ */

static void enable_copy(const char *path, const char *params)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_write(db, "INSERT INTO db_findings (repo_id, scope, engine, path, confidence,"
                                  " state, evidence, first_seen, last_seen, template, params)"
                                  " VALUES (?, 'source:" TEST_SRC "', 'sqlite', ?, 'live', 'enabled',"
                                  " '{}', 1, 1, 'sqlite-backup', ?)",
                              (const char *[]){ repo, path, params, NULL }, NULL), 0);
}

typedef struct {
    backup_result_t res;
    db_findings_t  *f;
    char           *caps_json;
} run_t;

static run_t run_file_backup(int check_why)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    o.detect = db_detect_start();
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s/tmp", store_root(store));
    db_captures_t *caps = NULL;
    db_captures_plan(db, repo_id, paths, 1, tmp, o.detect, &caps);
    db_captures_apply(caps, &o);
    run_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r.res), OK);
    db_cap_root_t root = { TEST_SRC, DB_CAP_LIVE, 0 };
    db_capture_desc_t cap = { &root, 1, 0, 0, 0, 0 };
    assert_int_equal(db_detect_finish(o.detect, &cap, &r.f), OK);
    r.caps_json = db_captures_json(caps);
    /* The messages, while the plan is alive. */
    char why[1024];
    if (caps && check_why) {
        assert_int_equal(db_unsettled_why(r.f, NULL, caps, TEST_SRC "/app.db", why, sizeof(why)), 1);
        print_message("why: %s\n", why);
    }
    db_captures_free(caps);
    db_detect_free(o.detect);
    return r;
}

static void run_free(run_t *r)
{
    backup_result_free(&r->res);
    db_findings_free(r->f);
    free(r->caps_json);
}

static char *slurp(const char *path)
{
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    char *buf = calloc(1, 65536);
    size_t n = fread(buf, 1, 65535, f);
    buf[n] = '\0';
    fclose(f);
    return buf;
}

static void test_backup_with_engine_copy(void **state)
{
    (void)state;
    sqlite3 *h = make_db(TEST_SRC "/app.db", 1);
    writer_t w = { TEST_SRC "/app.db", 0, 0 };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, writer_main, &w), 0);
    while (atomic_load(&w.committed) < 50) usleep(1000);
    enable_copy(TEST_SRC "/app.db", NULL);

    long long before = atomic_load(&w.committed);
    run_t r = run_file_backup(1);
    atomic_store(&w.stop, 1);
    pthread_join(th, NULL);
    assert_non_null(r.caps_json);
    assert_non_null(strstr(r.caps_json, "\"outcome\":\"ok\""));
    assert_non_null(strstr(r.caps_json, "\"copy\":\"/.cbackup-captures" TEST_SRC "/app.db\""));
    /* Copies are not files the backup could not read. */
    assert_int_equal(r.res.files_skipped, 0);
    int64_t snap = r.res.snapshot_id;
    run_free(&r);

    /* As stored: the raw file, its -wal, and the copy and its index. */
    rmrf(TEST_OUT);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, &ro, &rr), OK);
    assert_int_equal(rr.engine_copies, 1);
    assert_int_equal(rr.engine_copies_used, 0);
    assert_int_equal(access(TEST_OUT TEST_SRC "/app.db-wal", F_OK), 0);
    long long top = check_db(TEST_OUT "/.cbackup-captures" TEST_SRC "/app.db");
    assert_true(top >= before);
    char *ix = slurp(TEST_OUT "/.cbackup-captures/index.json");
    assert_non_null(ix);
    assert_non_null(strstr(ix, "\"format\":\"cbackup-captures\""));
    assert_non_null(strstr(ix, "\"path\":\"" TEST_SRC "/app.db\""));
    assert_non_null(strstr(ix, "\"method\":\"sqlite-online-backup\""));
    assert_non_null(strstr(ix, "\"settle\":"));
    free(ix);

    /* Preferring the copy: the database comes back as the copy, whole, and
     * without the stale -wal that would corrupt it. */
    rmrf(TEST_OUT);
    ro.prefer_engine_copy = 1;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, &ro, &rr), OK);
    assert_int_equal(rr.engine_copies_used, 1);
    assert_true(rr.engine_siblings_left_out >= 1);
    assert_int_equal(access(TEST_OUT TEST_SRC "/app.db-wal", F_OK), -1);
    assert_int_equal(access(TEST_OUT TEST_SRC "/app.db-shm", F_OK), -1);
    top = check_db(TEST_OUT TEST_SRC "/app.db");
    assert_true(top >= before);

    /* A subpath restore of just the database prefers it too. */
    rmrf(TEST_OUT);
    ro.subpath = TEST_SRC "/app.db";
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, &ro, &rr), OK);
    assert_int_equal(rr.engine_copies_used, 1);
    assert_true(check_db(TEST_OUT TEST_SRC "/app.db") >= before);
    sqlite3_close(h);
}

/* A copy that cannot be made is left out, said, and costs nothing else. */
static void test_backup_copy_refused(void **state)
{
    (void)state;
    sqlite3 *h = make_db(TEST_SRC "/r.db", 0);
    sqlite3_close(h);
    enable_copy(TEST_SRC "/r.db", NULL);
    /* And one for a file that is gone: planned out. */
    enable_copy(TEST_SRC "/gone.db", NULL);
    run_t r = run_file_backup(0);
    assert_non_null(r.caps_json);
    assert_non_null(strstr(r.caps_json, "\"outcome\":\"refused\""));
    assert_non_null(strstr(r.caps_json, "\"copy\":null"));
    assert_null(strstr(r.caps_json, "gone.db"));
    assert_int_equal(r.res.files_skipped, 0);
    int64_t snap = r.res.snapshot_id;
    run_free(&r);
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(access(TEST_OUT "/.cbackup-captures" TEST_SRC "/r.db", F_OK), -1);
    char *ix = slurp(TEST_OUT "/.cbackup-captures/index.json");
    assert_non_null(ix);
    assert_non_null(strstr(ix, "allow_rollback_lock"));
    free(ix);

    /* Allowed: copied. */
    assert_int_equal(db_write(db, "UPDATE db_findings SET params = '{\"allow_rollback_lock\":true}'"
                                  " WHERE path = '" TEST_SRC "/r.db'", NULL, NULL), 0);
    r = run_file_backup(0);
    assert_non_null(strstr(r.caps_json, "\"outcome\":\"ok\""));
    run_free(&r);
    assert_true(db_captures_total(SQLITE_COPY_OK) >= 1);
    assert_true(db_captures_total(SQLITE_COPY_REFUSED) >= 1);
}

/* Nothing enabled, or detection off: no plan, no reserved directory. */
static void test_no_plan(void **state)
{
    (void)state;
    static const char *paths[] = { TEST_SRC };
    db_captures_t *c = (db_captures_t *)1;
    db_captures_plan(db, repo_id, paths, 1, TEST_TMPD, NULL, &c);
    assert_null(c);
    db_detector_t *d = db_detect_start();
    db_captures_plan(db, repo_id, paths, 1, TEST_TMPD, d, &c);
    assert_null(c);
    /* Enabled in another source's scope: not this backup's. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_write(db, "INSERT INTO db_findings (repo_id, scope, engine, path, confidence,"
                                  " state, evidence, first_seen, last_seen, template)"
                                  " VALUES (?, 'source:/elsewhere', 'sqlite', '/elsewhere/x.db', 'live',"
                                  " 'enabled', '{}', 1, 1, 'sqlite-backup')",
                              (const char *[]){ repo, NULL }, NULL), 0);
    db_captures_plan(db, repo_id, paths, 1, TEST_TMPD, d, &c);
    assert_null(c);
    db_detect_free(d);
    db_captures_apply(NULL, NULL);
    db_captures_free(NULL);
    assert_null(db_captures_json(NULL));
    assert_null(db_captures_find(NULL, "/x"));

    /* A file backup may carry synthetic entries only under the reserved
     * directory, and only when it says it is making engine copies. */
    backup_synth_t s = { "/elsewhere/x", 0600, 0, 0, 0, 0, 0 };
    backup_synth_ops_t ops = { NULL, NULL, NULL, NULL, NULL };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.synth = &s;
    o.n_synth = 1;
    o.synth_ops = &ops;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), ERR_INVALID);
    o.captures = 1;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), ERR_INVALID);
    s.path = "/.cbackup-unit/x";
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), ERR_INVALID);
    o.unit_snapshot = 1;
    s.path = "/.cbackup-captures/x";
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), ERR_INVALID);
}

/* The three explanations of 9.5, and none for a file that is no database. */
static void test_messages(void **state)
{
    (void)state;
    db_findings_t f = { NULL, 0, 0 };
    db_finding_t x;
    memset(&x, 0, sizeof(x));
    x.engine = DB_ENGINE_SQLITE;
    x.path = (char *)(uintptr_t)"/d/app.db";
    x.confidence = DB_CONF_LIVE;
    x.evidence = (char *)(uintptr_t)"{\"markers\":[\"-wal\"],\"journal_mode\":\"wal\"}";
    f.v = &x;
    f.n = 1;
    int64_t ids[1] = { 12 };
    char buf[1024];
    assert_int_equal(db_unsettled_why(&f, ids, NULL, "/d/app.db", buf, sizeof(buf)), 1);
    assert_non_null(strstr(buf, "live SQLite database (WAL mode, held open)"));
    assert_non_null(strstr(buf, "(finding 12)"));
    assert_int_equal(db_unsettled_why(&f, ids, NULL, "/d/other", buf, sizeof(buf)), 0);
    assert_string_equal(buf, "");
    /* A file inside a data directory. */
    x.engine = DB_ENGINE_POSTGRES;
    x.path = (char *)(uintptr_t)"/d/pg";
    assert_int_equal(db_unsettled_why(&f, ids, NULL, "/d/pg/base/1/2", buf, sizeof(buf)), 1);
    assert_non_null(strstr(buf, "part of a live PostgreSQL database at /d/pg"));
    assert_int_equal(db_unsettled_why(NULL, NULL, NULL, "/d/x", buf, sizeof(buf)), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_copy_wal_under_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_copy_rollback_rule, setup, teardown),
        cmocka_unit_test_setup_teardown(test_copy_abandoned, setup, teardown),
        cmocka_unit_test_setup_teardown(test_raw_read_once_with_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_and_restore_explain, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_with_engine_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_copy_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_no_plan, setup, teardown),
        cmocka_unit_test(test_messages),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
