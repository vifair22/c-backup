#define _GNU_SOURCE

/*
 * The pack commit's two phases (B144, store_index.c db_pack_commit).
 *
 * A pack's rows used to land in one transaction, which held the write lock
 * across every row of a pack while a tag or sources write waited. They now
 * land as new rows in batches that end for a queued writer, then the packs
 * row as the visibility marker together with every row that replaces a
 * mapping readers use. These pin the three things that has to keep true:
 *
 *   - a writer queued during the commit gets in before the marker;
 *   - an object readers can find stays findable throughout a commit that
 *     moves its mapping to the new pack;
 *   - a cut between batches leaves rows that start-up puts back to what a
 *     lost single transaction left: none visible, none orphaned.
 *
 * Real SQLite and the real store_index throughout, against a store whose
 * packs are real files.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include <cutils/db.h>

#include "db_txn.h"
#include "db_readers.h"
#include "sync_latch.h"
#include "db_write.h"
#include "object.h"
#include "pack.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_store_index"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define MAX_OBJ   8192

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo_s[32];

static uint8_t  hashes[MAX_OBJ][OBJECT_HASH_SIZE];
static uint8_t *datas[MAX_OBJ];
static size_t   lens[MAX_OBJ];
static size_t   nobj;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void fill(uint8_t *b, size_t n, uint32_t seed)
{
    seed |= 1u;
    for (size_t i = 0; i < n; i++) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        b[i] = (uint8_t)seed;
    }
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "si", &repo_id) != OK) return -1;
    snprintf(repo_s, sizeof(repo_s), "%lld", (long long)repo_id);
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    nobj = 0;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_set_commit_gate(NULL);
    store_index_set_pack_batch_rows(0);
    db_txn_guard_arm(0);
    db_readers_close();
    store_index_set_commit_fault(NULL);
    db_txn_set_frame_cost_ns(0);
    db_txn_track_wal(NULL);
    sync_latch_reset_for_test();
    if (store) { store_close(store); store = NULL; }
    if (idx) { store_index_free(idx); idx = NULL; }
    if (db) { db_close(db); db = NULL; }
    for (size_t i = 0; i < nobj; i++) { free(datas[i]); datas[i] = NULL; }
    nobj = 0;
    rmrf(TEST_DIR);
    return 0;
}

/* n new objects into one pack of their own; returns the first index. */
static size_t pack_of(size_t n)
{
    size_t first = nobj;
    for (size_t k = 0; k < n; k++) {
        size_t i = nobj++;
        assert_true(i < MAX_OBJ);
        lens[i] = 200 + (i * 977) % 3000;
        datas[i] = malloc(lens[i]);
        assert_non_null(datas[i]);
        fill(datas[i], lens[i], (uint32_t)(0xB144 + i * 7919));
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], hashes[i]),
                         OK);
    }
    assert_int_equal(object_store_flush(store), OK);
    return first;
}

static int readable(size_t i)
{
    void *out = NULL;
    size_t len = 0;
    status_t st = object_load(store, hashes[i], &out, &len, NULL);
    int ok = st == OK && len == lens[i] && memcmp(out, datas[i], len) == 0;
    free(out);
    if (st != OK) err_clear();
    return ok;
}

static long long count(const char *sql)
{
    db_result_t *r = NULL;
    const char *p[] = { repo_s, NULL };
    long long v = -1;
    if (db_execute(db, sql, p, &r) == 0 && r && r->nrows == 1) v = atoll(r->rows[0][0]);
    db_result_free(r);
    return v;
}

static uint32_t pack_of_object(size_t i)
{
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hashes[i], &pnum, &off), OK);
    return pnum;
}

/* Nothing names a pack that has no packs row. */
static void assert_no_orphans(void)
{
    assert_int_equal(count("SELECT COUNT(*) FROM pack_objects po WHERE po.repo_id = ?"
                           " AND NOT EXISTS (SELECT 1 FROM packs p WHERE p.repo_id = po.repo_id"
                           " AND p.id = po.pack_id)"), 0);
    assert_int_equal(count("SELECT COUNT(*) FROM objects o WHERE o.repo_id = ?"
                           " AND o.location NOT IN ('staged', 'absent') AND NOT EXISTS"
                           " (SELECT 1 FROM packs p WHERE p.repo_id = o.repo_id"
                           " AND CAST(p.id AS TEXT) = o.location)"), 0);
}

/* ------------------------------------------------------------------ */
/* A captured commit, to replay through store_index                    */
/* ------------------------------------------------------------------ */

typedef struct {
    pack_commit_info_t info;
    pack_commit_entry_t *entries;
    char filename[64];
    int got;
} capture_t;

static void capture_cb(void *user, const pack_commit_info_t *info)
{
    capture_t *c = user;
    c->info = *info;
    c->entries = malloc(info->entry_count * sizeof(*c->entries));
    assert_non_null(c->entries);
    memcpy(c->entries, info->entries, info->entry_count * sizeof(*c->entries));
    snprintf(c->filename, sizeof(c->filename), "%s", info->filename);
    c->info.entries = c->entries;
    c->info.filename = c->filename;
    c->got = 1;
}

/*
 * A pack holding one object the index already has in another pack (a
 * duplicate, as two workers or a gc can leave) and `fresh` new ones, written
 * with the index out of the way so the duplicate is stored again, and its
 * commit captured rather than applied. Returns the first new object's index.
 */
/* Knows nothing, so the store writes the duplicate again. With no resolver
 * at all it falls back to scanning the pack files, which finds it. */
static status_t locate_nothing(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                               store_location_t *out)
{
    (void)user; (void)hash; (void)out;
    return set_error(ERR_NOT_FOUND, "not found");
}

static size_t duplicate_pack(size_t dup, size_t fresh, capture_t *cap)
{
    memset(cap, 0, sizeof(*cap));
    store_set_locate_cb(store, locate_nothing, NULL);
    store_set_pack_commit_cb(store, capture_cb, cap);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[dup], lens[dup], h), OK);
    assert_memory_equal(h, hashes[dup], OBJECT_HASH_SIZE);
    size_t first = nobj;
    for (size_t k = 0; k < fresh; k++) {
        size_t i = nobj++;
        lens[i] = 500 + (i * 131) % 2000;
        datas[i] = malloc(lens[i]);
        assert_non_null(datas[i]);
        fill(datas[i], lens[i], (uint32_t)(0xD0B + i * 104729));
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], hashes[i]),
                         OK);
    }
    assert_int_equal(object_store_flush(store), OK);
    assert_true(cap->got);
    assert_int_equal(cap->info.entry_count, fresh + 1);
    /* Back to the daemon's index for everything that follows. */
    store_index_free(idx);
    idx = NULL;
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
    return first;
}

/* ------------------------------------------------------------------ */
/* 1. A queued writer gets in before the marker                        */
/* ------------------------------------------------------------------ */

#define WRITER_PACK 1500

typedef struct {
    int saw;         /* uncommitted pack rows seen before queueing; -1 never */
    long long rows_in, packs_in;
    int ok;
    atomic_int stop;
} writer_t;

static void *writer_main(void *arg)
{
    writer_t *w = arg;
    w->saw = -1;
    /* The rows are uncommitted, but one handle serves every thread, so a
     * read on it sees the open transaction's rows. Polls sleep: each takes
     * the handle's mutex. */
    for (int i = 0; i < 60000 && !atomic_load(&w->stop); i++) {
        long long n = count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
        if (n > 0) { w->saw = (int)n; break; }
        struct timespec ts = { 0, 200 * 1000 };
        nanosleep(&ts, NULL);
    }
    if (w->saw < 0) return NULL;
    if (db_txn_begin(db) != OK) return NULL;
    w->rows_in  = count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    w->packs_in = count("SELECT COUNT(*) FROM packs WHERE repo_id = ?");
    int rc = db_execute_non_query(db, "INSERT INTO config (key, value) VALUES ('b144p', '1')",
                                  NULL, NULL);
    if (db_txn_commit(db) == OK && rc == 0) w->ok = 1;
    return NULL;
}

static void test_queued_writer_gets_in_before_the_marker(void **state)
{
    (void)state;
    writer_t w;
    memset(&w, 0, sizeof(w));
    pthread_t thr;
    assert_int_equal(pthread_create(&thr, NULL, writer_main, &w), 0);
    pack_of(WRITER_PACK);
    atomic_store(&w.stop, 1);
    pthread_join(thr, NULL);

    fprintf(stderr, "pack commit: writer saw %d rows, got in at %lld of %d, packs rows %lld\n",
            w.saw, w.rows_in, WRITER_PACK, w.packs_in);
    assert_true(w.saw > 0);
    assert_int_equal(w.ok, 1);
    /* In mid-commit: before the last rows and before the packs row. */
    assert_true(w.rows_in < WRITER_PACK);
    assert_int_equal(w.packs_in, 0);
    /* And the commit finished whole. */
    assert_int_equal(count("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 1);
    assert_int_equal(count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), WRITER_PACK);
    for (size_t i = 0; i < nobj; i++) assert_true(readable(i));
    assert_no_orphans();
}

/* ------------------------------------------------------------------ */
/* 2. A replaced mapping stays readable throughout                     */
/* ------------------------------------------------------------------ */

static int g_gate_calls;
static int g_gate_unreadable;
static size_t g_watch_lo, g_watch_hi;   /* objects readable before the commit */

static status_t readable_gate(void)
{
    g_gate_calls++;
    for (size_t i = g_watch_lo; i < g_watch_hi; i++)
        if (!readable(i)) g_gate_unreadable++;
    return OK;
}

static void test_replaced_mapping_stays_readable_throughout(void **state)
{
    (void)state;
    pack_of(4);                               /* 0..3 in pack A */
    uint32_t pack_a = pack_of_object(1);
    capture_t cap;
    size_t fresh = duplicate_pack(1, 5, &cap);   /* object 1 again, plus 4..8 */

    /* Each new row its own batch, so the commit has several points at which
     * a reader could look; the gate looks at every one, before each commit. */
    store_index_set_pack_batch_rows(1);
    g_gate_calls = g_gate_unreadable = 0;
    g_watch_lo = 0;
    g_watch_hi = 4;
    store_index_set_commit_gate(readable_gate);
    store_emit_pack_commit(store, &cap.info);
    store_index_set_commit_gate(NULL);

    /* Five batches of new rows, then the marker. */
    assert_int_equal(g_gate_calls, 6);
    assert_int_equal(g_gate_unreadable, 0);
    /* The duplicate's row moved to the new pack with the marker, as the
     * single transaction always moved it; every object reads back. */
    assert_int_equal(pack_of_object(1), (uint32_t)cap.info.pack_id);
    assert_true(pack_of_object(0) == pack_a);
    for (size_t i = 0; i < nobj; i++) assert_true(readable(i));
    (void)fresh;
    assert_no_orphans();
    free(cap.entries);
}

/* ------------------------------------------------------------------ */
/* 3. A cut between batches leaves nothing after start-up              */
/* ------------------------------------------------------------------ */

static int g_cut_at, g_commits;

static status_t cut_gate(void)
{
    return ++g_commits == g_cut_at ? set_error(ERR_IO, "power cut") : OK;
}

static void test_cut_between_batches_leaves_nothing_after_startup(void **state)
{
    (void)state;
    pack_of(3);                               /* 0..2, committed before */
    capture_t cap;
    size_t fresh = duplicate_pack(0, 4, &cap); /* object 0 again, plus 3..6 */
    store_index_set_pack_batch_rows(1);

    /* Four batches and the marker: cut before each, then none. */
    for (int k = 1; k <= 6; k++) {
        char pk[32];
        snprintf(pk, sizeof(pk), "%llu", (unsigned long long)cap.info.pack_id);
        g_commits = 0;
        g_cut_at = k;
        store_index_set_commit_gate(cut_gate);
        store_emit_pack_commit(store, &cap.info);
        store_index_set_commit_gate(NULL);
        err_clear();

        /* What the restart does before anything reads. */
        assert_int_equal(store_index_drop_unannounced(db, repo_id), OK);
        assert_int_equal(store_index_drop_staged(db, repo_id), OK);
        assert_no_orphans();

        /* Everything readable before the commit still is. */
        for (size_t i = 0; i < fresh; i++)
            if (!readable(i)) fail_msg("cut at commit %d: object %zu unreadable", k, i);
        int marker = k == 6;
        for (size_t i = fresh; i < nobj; i++)
            assert_int_equal(readable(i), marker);
        if (!marker) {
            /* Exactly what a lost single transaction left: no rows at all
             * for the new pack, and the duplicate still in its old one. */
            db_result_t *r = NULL;
            const char *p[] = { repo_s, pk, NULL };
            assert_int_equal(db_execute(db, "SELECT (SELECT COUNT(*) FROM pack_objects"
                                            " WHERE repo_id = ?1 AND pack_id = ?2),"
                                            " (SELECT COUNT(*) FROM packs"
                                            " WHERE repo_id = ?1 AND id = ?2)", p, &r), 0);
            assert_int_equal(atoi(r->rows[0][0]), 0);
            assert_int_equal(atoi(r->rows[0][1]), 0);
            db_result_free(r);
            assert_true(pack_of_object(0) != (uint32_t)cap.info.pack_id);
        }
    }
    free(cap.entries);
}

/* A failed commit cleans up after itself, without a restart. */
static void test_failed_commit_cleans_up_without_a_restart(void **state)
{
    (void)state;
    pack_of(2);
    capture_t cap;
    size_t fresh = duplicate_pack(0, 4, &cap);
    store_index_set_pack_batch_rows(1);
    /* Make the third batch's insert fail: a trigger that refuses the row. */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(cap.entries[2].hash, hex);
    char sql[512];
    snprintf(sql, sizeof(sql),
             "CREATE TEMP TRIGGER refuse BEFORE INSERT ON pack_objects"
             " WHEN NEW.hash = '%s' BEGIN SELECT RAISE(ABORT, 'refused'); END", hex);
    assert_int_equal(db_exec_raw(db, sql), 0);
    store_emit_pack_commit(store, &cap.info);
    err_clear();
    assert_int_equal(db_exec_raw(db, "DROP TRIGGER refuse"), 0);

    /* No restart: the callback put its rows back itself. */
    assert_no_orphans();
    assert_int_equal(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                           " AND location NOT IN ('staged','absent')"), 2);
    for (size_t i = 0; i < fresh; i++) assert_true(readable(i));
    free(cap.entries);
}

/* ------------------------------------------------------------------ */
/* 5. The start-up cleanup stays linear in the index                   */
/* ------------------------------------------------------------------ */

static double mono_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/*
 * PERF-GC: the UPDATE's subquery named objects.repo_id, so SQLite reran it
 * per objects row, a scan of pack_objects each time. 14k objects made every
 * daemon start take 40 s, which is what UAT saw as a 40 s gc. At this size
 * that shape takes about 10 s; the uncorrelated one, milliseconds. The bound
 * is loose enough for a sanitizer build on a loaded runner.
 */
#define LINEAR_ROWS "10000"
static void test_drop_unannounced_is_linear(void **state)
{
    (void)state;
    const char *p[] = { repo_s, NULL };
    /* One announced pack holding every row, and an unannounced pack 999
     * holding a tenth of them under hashes of their own. */
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO packs (repo_id, id, filename, size, sealed_at, version)"
        " VALUES (?1, 1, 'pack-0000000001.dat', 0, 0, 1)", p, NULL), 0);
    assert_int_equal(db_execute_non_query(db,
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < " LINEAR_ROWS ")"
        " INSERT INTO pack_objects (repo_id, hash, pack_id, offset, comp_size, orig_size,"
        "                           entry_index)"
        " SELECT ?1, printf('%064d', i), CASE WHEN i % 10 = 0 THEN 999 ELSE 1 END, i, 1, 1, i"
        " FROM n", p, NULL), 0);
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size, location, type)"
        " SELECT repo_id, hash, 1, 1, 1, CAST(pack_id AS TEXT), 0 FROM pack_objects"
        " WHERE repo_id = ?1", p, NULL), 0);

    for (int whole = 0; whole <= 1; whole++) {
        double t0 = mono_s();
        assert_int_equal(store_index_drop_unannounced(db, whole ? 0 : repo_id), OK);
        double took = mono_s() - t0;
        if (took > 3.0) fail_msg("drop_unannounced took %.1f s over " LINEAR_ROWS " rows", took);
    }
    assert_int_equal(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                           " AND location = 'staged'"), 1000);
    assert_int_equal(count("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                           " AND location = '1'"), 9000);
    assert_int_equal(count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"
                           " AND pack_id = 999"), 0);
}

/* ------------------------------------------------------------------ */
/* B184: dedup lookups from read-only connections                       */
/* ------------------------------------------------------------------ */

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* A gate that holds a pack commit open, transaction and all, until told. */
static pthread_mutex_t g_gmu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_gcv = PTHREAD_COND_INITIALIZER;
static int             g_in_gate, g_release_gate;

static status_t pausing_gate(void)
{
    pthread_mutex_lock(&g_gmu);
    g_in_gate = 1;
    pthread_cond_broadcast(&g_gcv);
    while (!g_release_gate) pthread_cond_wait(&g_gcv, &g_gmu);
    pthread_mutex_unlock(&g_gmu);
    return OK;
}

struct roll { pack_backup_writer_t *w; status_t st; };

static void *roll_main(void *arg)
{
    struct roll *r = arg;
    r->st = pack_backup_rollover(r->w);
    return NULL;
}

/*
 * Two writers meet the same chunk while the first one's pack commit is
 * still open. A read-only connection does not see that commit's rows, so
 * if the lookup were the only record of a staged name the second writer
 * would store it again. The claim set is the record: the second writer is
 * told another writer has it, the reader answers "absent" meanwhile (it
 * sees only commits), and once the commit lands and the claim goes the
 * reader answers for it. Exactly one copy is stored, every lookup came
 * from a reader, and the guard has nothing to report.
 */
static void test_dedup_race_stores_one_copy(void **state)
{
    (void)state;
    assert_int_equal(db_readers_open(TEST_DB, 2), OK);
    unsigned long l0 = db_readers_lookups();
    unsigned long v0 = db_txn_guard_violations();
    db_txn_guard_arm(1);

    const size_t len = 9000;
    char *buf = malloc(len);
    assert_non_null(buf);
    fill((uint8_t *)buf, len, 0xB184);

    pack_claims_t *c = pack_claims_new();
    assert_non_null(c);
    pack_backup_writer_t *a = NULL, *b = NULL;
    assert_int_equal(pack_backup_open(store, &a), OK);
    assert_int_equal(pack_backup_open(store, &b), OK);
    pack_backup_share_claims(a, c);
    pack_backup_share_claims(b, c);

    uint8_t h[OBJECT_HASH_SIZE];
    int is_new = -1;
    assert_int_equal(pack_backup_add(a, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);

    /* a's commit, held open with its rows written and not committed. */
    g_in_gate = 0;
    g_release_gate = 0;
    store_index_set_commit_gate(pausing_gate);
    struct roll r = { a, ERR_IO };
    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, roll_main, &r), 0);
    pthread_mutex_lock(&g_gmu);
    while (!g_in_gate) pthread_cond_wait(&g_gcv, &g_gmu);
    pthread_mutex_unlock(&g_gmu);

    /* The reader sees only commits, and answers without waiting for a. */
    double t0 = now_s();
    assert_false(pack_object_exists(store, h));
    err_clear();
    assert_true(now_s() - t0 < 0.5);
    /* b is covered by a's claim, not by the index. */
    assert_int_equal(pack_backup_add(b, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_false(pack_backup_holds(b, h));

    pthread_mutex_lock(&g_gmu);
    g_release_gate = 1;
    pthread_cond_broadcast(&g_gcv);
    pthread_mutex_unlock(&g_gmu);
    assert_int_equal(pthread_join(t, NULL), 0);
    assert_int_equal(r.st, OK);
    store_index_set_commit_gate(NULL);

    /* Committed and the claim gone: the reader answers for it. */
    assert_true(pack_object_exists(store, h));
    assert_int_equal(pack_backup_add(b, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);

    uint32_t pa = 0, pb = 0;
    assert_int_equal(pack_backup_finish(a, &pa), OK);
    assert_int_equal(pack_backup_finish(b, &pb), OK);
    assert_int_equal(pb, 0);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) snprintf(hex + i * 2, 3, "%02x", h[i]);
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM pack_objects WHERE repo_id = ? AND hash = '%s'",
             hex);
    assert_int_equal(count(sql), 1);

    assert_true(db_readers_lookups() - l0 >= 3);
    db_txn_guard_arm(0);
    assert_int_equal(db_txn_guard_violations(), v0);
    pack_claims_free(c);
    free(buf);
}

/* A thread holding the write lock and, inside it, the shared connection's
 * mutex for as long as one heavy statement runs. */
static atomic_int g_busy_inside;
static double     g_busy_secs;

static void *busy_main(void *arg)
{
    (void)arg;
    assert_int_equal(db_txn_begin(db), OK);
    double t0 = now_s();
    atomic_store(&g_busy_inside, 1);
    db_result_t *r = NULL;
    (void)db_execute(db, "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c"
                         " WHERE x < 15000000) SELECT COUNT(*) FROM c", NULL, &r);
    db_result_free(r);
    g_busy_secs = now_s() - t0;
    db_txn_rollback(db);
    return NULL;
}

static double timed_locate(size_t i, pthread_t *t)
{
    atomic_store(&g_busy_inside, 0);
    assert_int_equal(pthread_create(t, NULL, busy_main, NULL), 0);
    while (!atomic_load(&g_busy_inside)) {
        struct timespec ts = { 0, 1000000L };
        nanosleep(&ts, NULL);
    }
    struct timespec ts = { 0, 100 * 1000000L };   /* into the statement */
    nanosleep(&ts, NULL);
    store_location_t loc;
    double t0 = now_s();
    assert_int_equal(store_locate(store, hashes[i], &loc), OK);
    return now_s() - t0;
}

/*
 * The point of the readers: a lookup does not wait for another thread's
 * transaction or for the shared connection's mutex. Timed against the
 * same lookup through the shared connection, which waits for the whole
 * statement, so the test shows the difference rather than assuming it.
 */
static void test_lookup_does_not_wait_for_a_writer(void **state)
{
    (void)state;
    size_t i = pack_of(1);
    unsigned long v0 = db_txn_guard_violations();
    db_txn_guard_arm(1);

    pthread_t t;
    double shared = timed_locate(i, &t);         /* no readers: shared connection */
    assert_int_equal(pthread_join(t, NULL), 0);

    assert_int_equal(db_readers_open(TEST_DB, 2), OK);
    double reader = timed_locate(i, &t);
    assert_int_equal(pthread_join(t, NULL), 0);
    fprintf(stderr, "lookup during a %.2f s statement: shared %.3f s, reader %.4f s\n",
            g_busy_secs, shared, reader);
    assert_true(g_busy_secs > 0.4);
    assert_true(shared > 0.25);                  /* the wait the readers remove */
    assert_true(reader < 0.1);

    db_txn_guard_arm(0);
    assert_int_equal(db_txn_guard_violations(), v0);
}

static int always_fail(void) { return 1; }

/*
 * A pack taken off its writer whose seal then fails, after a second writer
 * has referenced one of its names through the pack's claim. The seal says
 * so, its claims are counted abandoned - the count backup_run fails a run
 * on (store_content) - and no index row names the chunk, so nothing can
 * resolve it. Two ways a seal fails: its index commit (reported through
 * store_pack_commit_failed; before B184 this was logged and the seal still
 * reported success), and the repository's sync latch refusing it (B182).
 */
static void seal_fails_after_a_reference(int latch)
{
    const size_t len = 9000;
    char *buf = malloc(len);
    assert_non_null(buf);
    fill((uint8_t *)buf, len, latch ? 0xB185 : 0xB186);

    pack_claims_t *c = pack_claims_new();
    assert_non_null(c);
    pack_backup_writer_t *a = NULL, *b = NULL;
    assert_int_equal(pack_backup_open(store, &a), OK);
    assert_int_equal(pack_backup_open(store, &b), OK);
    pack_backup_share_claims(a, c);
    pack_backup_share_claims(b, c);

    uint8_t h[OBJECT_HASH_SIZE];
    int is_new = -1;
    assert_int_equal(pack_backup_add(a, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);
    pack_backup_sealing_t *seal = NULL;
    assert_int_equal(pack_backup_rollover_begin(a, &seal), OK);
    assert_non_null(seal);
    /* b references it: claimed by the pack being sealed. */
    assert_int_equal(pack_backup_add(b, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_int_equal(pack_claims_deferred(c), 1);

    if (latch) sync_latch_repo_trip(TEST_REPO, "pack-test.dat", EIO);
    else       store_index_set_commit_fault(always_fail);
    assert_int_not_equal(pack_backup_rollover_end(seal), OK);
    err_clear();
    store_index_set_commit_fault(NULL);
    sync_latch_reset_for_test();

    assert_int_equal(pack_claims_abandoned(c), 1);
    assert_false(pack_object_exists(store, h));
    err_clear();
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) snprintf(hex + i * 2, 3, "%02x", h[i]);
    char sql[256];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM pack_objects WHERE repo_id = ? AND hash = '%s'",
             hex);
    assert_int_equal(count(sql), 0);

    pack_backup_abort(a);
    pack_backup_abort(b);
    pack_claims_free(c);
    free(buf);
}

static void test_failed_commit_abandons_a_referenced_claim(void **state)
{
    (void)state;
    seal_fails_after_a_reference(0);
}

static void test_refused_seal_abandons_a_referenced_claim(void **state)
{
    (void)state;
    seal_fails_after_a_reference(1);
}

/* ------------------------------------------------------------------ */
/* B193: the marker in parts, and a request ahead of bulk commits      */
/* ------------------------------------------------------------------ */

/* n new objects stored and their pack's commit captured, not applied. */
static size_t capture_pack(size_t n, capture_t *cap)
{
    memset(cap, 0, sizeof(*cap));
    store_set_pack_commit_cb(store, capture_cb, cap);
    size_t first = nobj;
    for (size_t k = 0; k < n; k++) {
        size_t i = nobj++;
        assert_true(i < MAX_OBJ);
        lens[i] = 200 + (i * 613) % 1500;
        datas[i] = malloc(lens[i]);
        assert_non_null(datas[i]);
        fill(datas[i], lens[i], (uint32_t)(0xB193 + i * 6151));
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], hashes[i]),
                         OK);
    }
    assert_int_equal(object_store_flush(store), OK);
    assert_true(cap->got);
    assert_int_equal(cap->info.entry_count, n);
    store_index_free(idx);
    idx = NULL;
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
    return first;
}

/*
 * A pack whose every entry duplicates one already indexed elsewhere (what
 * FS2's 16.7 s commit was: chunks another thread had packed first) sets all
 * its rows aside for the marker. They now commit in parts after the packs
 * row, and at every part boundary every object is readable, the old copies
 * through their packs and the replaced ones through the new pack.
 */
static void test_set_aside_rows_commit_in_parts(void **state)
{
    (void)state;
    pack_of(40);                                  /* 0..39 in pack A */
    capture_t cap;
    memset(&cap, 0, sizeof(cap));
    store_set_locate_cb(store, locate_nothing, NULL);
    store_set_pack_commit_cb(store, capture_cb, &cap);
    for (size_t i = 0; i < 40; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], h), OK);
    }
    assert_int_equal(object_store_flush(store), OK);
    assert_true(cap.got);
    store_index_free(idx);
    idx = NULL;
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);

    store_index_set_pack_batch_rows(4);
    g_gate_calls = g_gate_unreadable = 0;
    g_watch_lo = 0;
    g_watch_hi = 40;
    store_index_set_commit_gate(readable_gate);
    assert_int_equal(store_emit_pack_commit(store, &cap.info), OK);
    store_index_set_commit_gate(NULL);
    free(cap.entries);
    /* The packs row and 40 replacing rows, four to a part. */
    assert_true(g_gate_calls >= 10);
    assert_int_equal(g_gate_unreadable, 0);
    assert_int_equal(count("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 2);
    for (size_t i = 0; i < 40; i++) assert_true(readable(i));
    assert_no_orphans();
}

/* A cut after the packs row and some of the replacing rows: every object
 * is still readable, and nothing names a pack that is gone. */
static int g_cut_after;
static status_t cut_after_gate(void)
{
    return --g_cut_after < 0 ? ERR_IO : OK;
}

static void test_cut_between_marker_parts_leaves_everything_readable(void **state)
{
    (void)state;
    pack_of(24);
    capture_t cap;
    memset(&cap, 0, sizeof(cap));
    store_set_locate_cb(store, locate_nothing, NULL);
    store_set_pack_commit_cb(store, capture_cb, &cap);
    for (size_t i = 0; i < 24; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], h), OK);
    }
    assert_int_equal(object_store_flush(store), OK);
    store_index_free(idx);
    idx = NULL;
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);

    store_index_set_pack_batch_rows(4);
    g_cut_after = 2;                /* the packs row's part and one more commit */
    store_index_set_commit_gate(cut_after_gate);
    store_emit_pack_commit(store, &cap.info);
    store_index_set_commit_gate(NULL);
    err_clear();
    free(cap.entries);
    assert_int_equal(count("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 2);
    /* Some hashes moved, some did not: both packs are there, so all read. */
    long long moved = count("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"
                            " AND pack_id = (SELECT MAX(id) FROM packs WHERE repo_id = ?1)");
    assert_true(moved > 0 && moved < 24);
    for (size_t i = 0; i < 24; i++) assert_true(readable(i));
    assert_no_orphans();
}

/*
 * A request ahead of bulk commits (B193). Three pack commits of 2,000
 * objects each - well over 1,000 WAL pages apiece - run at once, with every
 * WAL page priced at 40 ms, so the budget is its 16-page floor and each
 * part is a few rows. A tag-style write (db_txn_begin) that queues while
 * they run waits for the part holding the lock and nothing else: no bulk
 * transaction begins between its queueing and its turn, though three bulk
 * writers are waiting - except one the priority bound owes the turn.
 *
 * The count comes from the ticket, which records it from the moment the
 * request takes its ticket and knows why each bulk grant ahead of it was
 * made (FLAKE-27). Counting db_txn_bulk_count around db_txn_begin also
 * counted bulk parts the bound let go first, and any that began between
 * reading the count and taking the ticket. A stall of the whole runner
 * while a part holds the lock (a VM descheduled, a slow fsync) makes the
 * next bulk writer's wait exceed DB_TXN_BULK_DUE_MS, and the ticket then
 * rightly serves it before the request: one bulk transaction ahead after a
 * wait of a few ms, which is how CI failed. Under SIGSTOP pulses it was 202
 * of 1,168 requests, every one owed by the bound.
 */
typedef struct {
    capture_t *cap;
    status_t   st;
} replay_t;

static void *replay_main(void *arg)
{
    replay_t *r = arg;
    r->st = store_emit_pack_commit(store, &r->cap->info);
    return NULL;
}

static void test_a_request_goes_before_waiting_bulk_commits(void **state)
{
    (void)state;
    capture_t caps[3];
    for (int k = 0; k < 3; k++) capture_pack(2000, &caps[k]);
    db_txn_track_wal(db);
    db_txn_set_frame_cost_ns(40e6);

    replay_t rp[3];
    pthread_t th[3];
    unsigned long b_start = db_txn_bulk_count();
    for (int k = 0; k < 3; k++) {
        rp[k].cap = &caps[k];
        rp[k].st = ERR_IO;
        assert_int_equal(pthread_create(&th[k], NULL, replay_main, &rp[k]), 0);
    }
    /* Well into the bulk commits, all three running. */
    while (db_txn_bulk_count() < b_start + 30) {
        struct timespec ts = { 0, 1000000L };
        nanosleep(&ts, NULL);
    }
    int checks = 0;
    unsigned long ahead = 0, owed = 0, out_of_order = 0;
    double worst_ms = 0;
    for (int t = 0; t < 20; t++) {
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        assert_int_equal(db_txn_begin(db), OK);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        db_txn_wait_t w;
        db_txn_last_wait(&w);
        assert_int_equal(db_write(db, "INSERT OR REPLACE INTO config (key, value)"
                                      " VALUES ('b193tag', '1')", NULL, NULL), 0);
        assert_int_equal(db_txn_commit(db), OK);
        ahead += w.bulk_ahead;
        owed  += w.bulk_ahead_due;
        out_of_order += w.bulk_ahead - w.bulk_ahead_due;
        double ms = (double)(t1.tv_sec - t0.tv_sec) * 1e3 + (double)(t1.tv_nsec - t0.tv_nsec) / 1e6;
        if (ms > worst_ms) worst_ms = ms;
        checks++;
        struct timespec ts = { 0, 5 * 1000000L };
        nanosleep(&ts, NULL);
    }
    for (int k = 0; k < 3; k++) {
        pthread_join(th[k], NULL);
        assert_int_equal(rp[k].st, OK);
        free(caps[k].entries);
    }
    db_txn_commit_stats_t cs;
    db_txn_commit_stats(&cs);
    print_message("  %d request writes: worst lock wait %.1f ms, %lu bulk transactions served "
                  "ahead (%lu owed by the bound); bulk commits wrote at most %u WAL pages, %lu "
                  "ended by the budget\n", checks, worst_ms, ahead, owed, cs.max_frames,
                  cs.capped);
    assert_int_equal(out_of_order, 0);
    assert_true(cs.max_frames <= DB_TXN_COMMIT_PAGES_MIN * 4);
    assert_int_equal(count("SELECT COUNT(*) FROM packs WHERE repo_id = ?"), 3);
    for (size_t i = 0; i < nobj; i++) assert_true(readable(i));
    assert_no_orphans();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_queued_writer_gets_in_before_the_marker,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_replaced_mapping_stays_readable_throughout,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_cut_between_batches_leaves_nothing_after_startup,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_commit_cleans_up_without_a_restart,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_drop_unannounced_is_linear, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dedup_race_stores_one_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lookup_does_not_wait_for_a_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_commit_abandons_a_referenced_claim,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_refused_seal_abandons_a_referenced_claim,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_set_aside_rows_commit_in_parts, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cut_between_marker_parts_leaves_everything_readable,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_request_goes_before_waiting_bulk_commits,
                                        setup, teardown),
    };
    /* Before any test opens a connection, so each carries the guard's hook. */
    db_txn_guard_install();
    return cmocka_run_group_tests_name("store_index", tests, NULL, NULL);
}
