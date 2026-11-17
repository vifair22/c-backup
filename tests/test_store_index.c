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
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include <cutils/db.h>

#include "db_txn.h"
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
#define MAX_OBJ   2048

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
    };
    return cmocka_run_group_tests_name("store_index", tests, NULL, NULL);
}
