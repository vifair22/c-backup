#define _POSIX_C_SOURCE 200809L

/*
 * Concurrent metadata writes against a running backup.
 *
 * B107 changed how the daemon's one write lock is shared: db_txn became a
 * FIFO ticket lock, and the backup gives the lock up mid-phase. Since OWN-16
 * it does so whenever a writer is queued (db_txn_waiters): between entries,
 * between reads of a file being stored, between trees, and between slices of
 * a wait on the large-file workers. Each of those is a point where another
 * thread's transaction lands in the middle of a backup. The unit tests
 * beside the fix force one interleaving each; this one throws the real write
 * paths at a real backup and checks the outcome.
 *
 * Four writers run for the whole backup and keep going until each has done a
 * minimum number of writes, at least MIN_DURING of them while the backup was
 * still running:
 *
 *   - sources: the real PUT /repos/<id>/sources handler, alternating lists
 *   - policy:  the real PUT /repos/<id>/policy handler, keep_daily = i
 *   - tags:    the real tag set and remove handlers on snapshot 1
 *   - raw:     db_txn_begin/insert/commit, the shape every other writer has
 *
 * The backup covers files up to near the 16 MiB object bound (two of 6 MiB
 * and one of 15 MiB) and 12,000 small ones, past the 2,000-entry
 * batch, so it crosses count commits, commits for a waiter mid-file and pack
 * seals.
 *
 * It was 3,000 while the suites ran in /tmp, which is a tmpfs on most
 * desktops and makes every fsync free. On a real disk (btrfs, here) the tag
 * and sources writers sync a file per write, and the backup over 3,000 files
 * finished inside eight of their writes. More small files make the backup
 * long enough to overlap MIN_DURING of them at the cost of a few MiB of
 * scratch, rather than loosening what overlap means.
 *
 * What is asserted: no handler returns anything but success, every write is
 * in the final state, no single write waited longer than the bound, the
 * backup's snapshot has every file, a deep verify finds nothing missing or
 * damaged, and no staged object rows are left behind.
 *
 * The wait bound defaults to 2 s and C_BACKUP_STRESS_WAIT_MS overrides it,
 * for sanitizer builds that run several times slower. Before B107 a sources
 * write waited for the whole store phase.
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
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "db_txn.h"
#include "repo.h"
#include "routes.h"
#include "server.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_dbstress"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"     /* what the stressed backup reads */
#define TEST_SRC2 TEST_DIR "/src2"    /* snapshot 1, for the tags */

#define N_SMALL   12000             /* past CACHE_BATCH, so count commits too */
#define N_FILES   (N_SMALL + 3)       /* plus two 6 MiB and one 15 MiB */
#define MIN_ITERS 40                  /* per writer, backup or no backup */
#define MIN_DURING 10                 /* per writer, while the backup runs */
#define MAX_ITERS 2000

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static atomic_int     backup_done;

typedef enum { W_SOURCES, W_POLICY, W_TAGS, W_RAW } writer_kind_t;

typedef struct {
    writer_kind_t kind;
    pthread_t     thr;
    int           iters;
    int           during;   /* writes finished before the backup did */
    int           errors;
    char          first_error[256];
    int64_t       max_wait_ms;
} writer_t;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

static void write_file(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static void write_big(const char *path, int mib)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    char *buf = malloc(1 << 20);
    assert_non_null(buf);
    for (int i = 0; i < mib; i++) {
        for (int j = 0; j < (1 << 20); j++)
            buf[j] = (char)((j * 31 + i * 7) ^ (j >> 9));
        assert_int_equal(write(fd, buf, 1 << 20), 1 << 20);
    }
    free(buf);
    close(fd);
}

static int scalar(const char *sql, const char *p1)
{
    db_result_t *r = NULL;
    const char *params[] = { p1, NULL };
    if (db_execute(db, sql, p1 ? params : NULL, &r) != 0) { db_result_free(r); return -1; }
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

static int setup(void **state)
{
    (void)state;
    atomic_store(&backup_done, 0);
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "stress", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    routes_metadata_init(db);

    if (mkdir(TEST_SRC, 0700) != 0 || mkdir(TEST_SRC2, 0700) != 0) return -1;
    write_file(TEST_SRC2 "/one.txt", "snapshot one\n");
    write_file(TEST_SRC2 "/two.txt", "snapshot one, second file\n");
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

/* ------------------------------------------------------------------ */

static void note_error(writer_t *w, const char *what, const api_response_t *r)
{
    if (w->errors++ == 0)
        snprintf(w->first_error, sizeof(w->first_error), "%s: %d %s",
                 what, r ? r->status : 0, (r && r->body) ? r->body : "");
}

static int call(writer_t *w, const char *what, api_response_t r)
{
    int ok = r.status >= 200 && r.status < 300;
    if (!ok) note_error(w, what, &r);
    free(r.body);
    free(r.set_cookie);
    return ok;
}

static api_request_t req_with(api_method_t m, const char *body)
{
    api_request_t q;
    memset(&q, 0, sizeof(q));
    q.method   = m;
    q.url      = "/";
    q.body     = body;
    q.body_len = body ? strlen(body) : 0;
    q.is_local = 1;
    q.client_addr = "";
    return q;
}

static void one_write(writer_t *w, int i)
{
    char body[512];
    switch (w->kind) {
    case W_SOURCES: {
        if (i % 2 == 0)
            snprintf(body, sizeof(body), "{\"sources\":[\"%s\"]}", TEST_SRC);
        else
            snprintf(body, sizeof(body), "{\"sources\":[\"%s\",\"%s\"],"
                     "\"exclude\":[\"*.tmp\"]}", TEST_SRC, TEST_SRC2);
        api_request_t q = req_with(API_PUT, body);
        call(w, "sources put", metadata_sources_put(&q, repo_id));
        break;
    }
    case W_POLICY: {
        snprintf(body, sizeof(body),
                 "{\"key\":\"retention.keep_daily\",\"value\":%d}", i + 1);
        api_request_t q = req_with(API_PUT, body);
        call(w, "policy put", metadata_policy_put(&q, repo_id));
        break;
    }
    case W_TAGS: {
        char name[32];
        snprintf(name, sizeof(name), "s%d", i);
        snprintf(body, sizeof(body), "{\"name\":\"%s\",\"snap_id\":1}", name);
        api_request_t q = req_with(API_POST, body);
        if (call(w, "tag set", metadata_tags_post(&q, repo_id)) && (i % 2)) {
            api_request_t d = req_with(API_DELETE, NULL);
            call(w, "tag remove", metadata_tags_delete(&d, repo_id, name));
        }
        break;
    }
    case W_RAW: {
        char key[32], val[16];
        snprintf(key, sizeof(key), "stress.%d", i);
        snprintf(val, sizeof(val), "%d", i);
        if (db_txn_begin(db) != OK) {
            note_error(w, "raw begin", NULL);
            break;
        }
        if (db_execute_non_query(db,
                "INSERT INTO config (key, value) VALUES (?, ?)",
                (const char *[]){ key, val, NULL }, NULL) != 0) {
            note_error(w, "raw insert", NULL);
            db_txn_rollback(db);
            break;
        }
        if (db_txn_commit(db) != OK) note_error(w, "raw commit", NULL);
        break;
    }
    }
}

static void *writer_main(void *arg)
{
    writer_t *w = arg;
    for (int i = 0; i < MAX_ITERS; i++) {
        if (i >= MIN_ITERS && atomic_load(&backup_done)) break;
        int64_t t0 = now_ms();
        one_write(w, i);
        int64_t took = now_ms() - t0;
        if (took > w->max_wait_ms) w->max_wait_ms = took;
        w->iters = i + 1;
        if (!atomic_load(&backup_done)) w->during++;
        struct timespec ts = { 0, 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return NULL;
}

/*
 * B144: a writer that queues while the node index is being written gets in
 * at the next row, not after the batch. index_nodes ended a transaction only
 * every 2,000 rows, so a tag or sources write that queued during indexing
 * waited for all of them, and under load that broke Manual 6.3's "well under
 * a second". Fewer files than one batch, so before the fix the writer could
 * only get in once indexing was over.
 *
 * The writer watches for the backup's first node rows. They are uncommitted,
 * but one handle serves every thread, so a read on it sees the open
 * transaction's rows: once there are some, index_nodes holds the lock
 * mid-batch. It then queues, and inside its own transaction records how many
 * node rows exist and whether the snapshot row, written only after the last
 * batch, is there yet.
 */
#define B144_FILES 1500

typedef struct {
    pthread_t thr;
    int       saw;        /* node rows when it decided to queue, -1 never */
    int       nodes_in;   /* node rows once inside its transaction */
    int       snap_in;    /* snapshot rows for snapshot 1 at that point */
    int       ok;
} b144_writer_t;

static void *b144_writer(void *arg)
{
    b144_writer_t *w = arg;
    w->saw = -1;
    /* Up to a minute for the backup to reach indexing; each poll takes the
     * shared handle's mutex, so it sleeps between them rather than spin. */
    for (int i = 0; i < 60000 && !atomic_load(&backup_done); i++) {
        int n = scalar("SELECT COUNT(*) FROM nodes WHERE snap_id = 1", NULL);
        if (n > 0) { w->saw = n; break; }
        struct timespec ts = { 0, 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    if (w->saw < 0) return NULL;
    if (db_txn_begin(db) != OK) return NULL;
    w->nodes_in = scalar("SELECT COUNT(*) FROM nodes WHERE snap_id = 1", NULL);
    w->snap_in  = scalar("SELECT COUNT(*) FROM snapshots WHERE id = 1", NULL);
    int rc = db_execute_non_query(db, "INSERT INTO config (key, value) VALUES ('b144', '1')",
                                  NULL, NULL);
    if (db_txn_commit(db) == OK && rc == 0) w->ok = 1;
    return NULL;
}

static void test_writer_gets_in_during_node_indexing(void **state)
{
    (void)state;
    for (int i = 0; i < B144_FILES; i++) {
        char path[128], body[64];
        snprintf(path, sizeof(path), TEST_SRC "/n%04d.txt", i);
        snprintf(body, sizeof(body), "node index file %d\n", i);
        write_file(path, body);
    }

    b144_writer_t w;
    memset(&w, 0, sizeof(w));
    assert_int_equal(pthread_create(&w.thr, NULL, b144_writer, &w), 0);

    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    status_t bst = backup_run(store, db, repo_id, &o, &res);
    atomic_store(&backup_done, 1);
    pthread_join(w.thr, NULL);
    assert_int_equal(bst, OK);
    assert_int_equal(res.snapshot_id, 1);

    int total = scalar("SELECT COUNT(*) FROM nodes WHERE snap_id = 1", NULL);
    fprintf(stderr, "b144: writer saw %d node rows, got in at %d of %d, snapshot row %d\n",
            w.saw, w.nodes_in, total, w.snap_in);
    assert_true(total > B144_FILES);
    assert_true(w.saw > 0);
    assert_int_equal(w.ok, 1);
    /* In before indexing finished, so before the batch would have ended... */
    assert_true(w.nodes_in < total);
    /* ...and before the snapshot claims its nodes: nodes_materialized is
     * still written last. */
    assert_int_equal(w.snap_in, 0);
    assert_int_equal(scalar("SELECT nodes_materialized FROM snapshots WHERE id = 1", NULL), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM config WHERE key = 'b144'", NULL), 1);
}

/* ------------------------------------------------------------------ */

static void test_writes_during_a_backup_all_land(void **state)
{
    (void)state;

    /* Snapshot 1, so the tag writer has something to tag. */
    static const char *p1[] = { TEST_SRC2 };
    backup_opts_t o1;
    memset(&o1, 0, sizeof(o1));
    o1.paths = p1;
    o1.n_paths = 1;
    assert_int_equal(backup_run(store, db, repo_id, &o1, NULL), OK);

    for (int i = 0; i < N_SMALL; i++) {
        char path[128], body[96];
        snprintf(path, sizeof(path), TEST_SRC "/small%03d.txt", i);
        snprintf(body, sizeof(body), "small file %d of the stress backup\n", i);
        write_file(path, body);
    }
    write_big(TEST_SRC "/big6a.bin", 6);
    write_big(TEST_SRC "/big6b.bin", 6);
    write_big(TEST_SRC "/big15.bin", 15);

    writer_t w[4];
    memset(w, 0, sizeof(w));
    w[0].kind = W_SOURCES;
    w[1].kind = W_POLICY;
    w[2].kind = W_TAGS;
    w[3].kind = W_RAW;
    for (int i = 0; i < 4; i++)
        assert_int_equal(pthread_create(&w[i].thr, NULL, writer_main, &w[i]), 0);

    static const char *p2[] = { TEST_SRC };
    backup_opts_t o2;
    memset(&o2, 0, sizeof(o2));
    o2.paths = p2;
    o2.n_paths = 1;
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    status_t bst = backup_run(store, db, repo_id, &o2, &res);
    atomic_store(&backup_done, 1);
    for (int i = 0; i < 4; i++) pthread_join(w[i].thr, NULL);

    const char *env = getenv("C_BACKUP_STRESS_WAIT_MS");
    int64_t bound = env ? atoll(env) : 2000;

    static const char *names[] = { "sources", "policy", "tags", "raw" };
    for (int i = 0; i < 4; i++) {
        fprintf(stderr, "stress: %-7s %4d writes (%d during the backup), max wait"
                " %lld ms, %d errors%s%s\n",
                names[i], w[i].iters, w[i].during, (long long)w[i].max_wait_ms,
                w[i].errors,
                w[i].errors ? ": " : "", w[i].first_error);
        assert_int_equal(w[i].errors, 0);
        assert_true(w[i].iters >= MIN_ITERS);
        /* The point of the test: the writes overlapped the backup. */
        assert_true(w[i].during >= MIN_DURING);
        assert_true(w[i].max_wait_ms <= bound);
    }

    /* The backup: complete and readable. */
    assert_int_equal(bst, OK);
    assert_int_equal(res.snapshot_id, 2);
    assert_int_equal((int)res.files, N_FILES);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE snap_id = 2"
                            " AND type = 1", NULL), N_FILES);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE location = 'staged'",
                            NULL), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes n WHERE n.snap_id = 2"
                            " AND n.content_hash IS NOT NULL AND NOT EXISTS"
                            " (SELECT 1 FROM objects o WHERE o.hash = n.content_hash)",
                            NULL), 0);
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_true(vr.checked > 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.recoverable, 0);

    /* Every write landed: the final state is the last write of each. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int last_src = w[0].iters - 1;
    assert_int_equal(scalar("SELECT COUNT(*) FROM repo_backup_paths WHERE repo_id = ?"
                            " AND kind = 'source'", repo), (last_src % 2) ? 2 : 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM repo_backup_paths WHERE repo_id = ?"
                            " AND kind = 'exclude'", repo), (last_src % 2) ? 1 : 0);
    assert_int_equal(scalar("SELECT value FROM policy_values WHERE repo_id = ?"
                            " AND key = 'retention.keep_daily'", repo), w[1].iters);
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?", repo),
                     (w[2].iters + 1) / 2);
    assert_int_equal(scalar("SELECT COUNT(*) FROM config WHERE key LIKE 'stress.%'",
                            NULL), w[3].iters);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_writes_during_a_backup_all_land, setup, teardown),
        cmocka_unit_test_setup_teardown(test_writer_gets_in_during_node_indexing, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
