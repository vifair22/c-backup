#define _GNU_SOURCE

/*
 * Synthetic entries whose size is only known at their end (CDC_SIZE_STREAM,
 * a dump hook's stdout) and entries their producer disowns after the bytes
 * are stored (backup_synth_ops_t.keep): the stream lands with the size it
 * turned out to have, byte for byte, through several chunks and pieces; the
 * disowned one is not in the snapshot at all. Plus cdc_store_source over a
 * stream directly, including an empty one.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "cdc.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define D     TEST_TMP "/c_backup_test_synth_stream"
#define DB    D "/state.db"
#define REPO  D "/repo"
#define DEST  D "/dest"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static int64_t repo_id;
static store_t *store;
static store_index_t *idx;

/* A generated stream handed over in pieces of 100,000 bytes, as a pipe
 * would: each piece read into a buffer, then served by read(). */
typedef struct {
    uint64_t len, off;
    uint8_t  buf[100000];
    uint64_t buf_off;
    size_t   buf_len;
    int      closed;
} gen_t;

static uint8_t gen_byte(uint64_t o) { return (uint8_t)((o * 2654435761u) >> 13); }

static status_t gen_piece(void *ctx, uint64_t *s, uint64_t *e, int *got, int *more)
{
    gen_t *g = ctx;
    /* Nothing at all: no span. An open span closes with an empty piece. */
    if (g->closed || g->len == 0) { *got = 0; return OK; }
    size_t n = g->len - g->off < sizeof(g->buf) ? (size_t)(g->len - g->off) : sizeof(g->buf);
    for (size_t i = 0; i < n; i++) g->buf[i] = gen_byte(g->off + i);
    g->buf_off = g->off;
    g->buf_len = n;
    *s = g->off;
    *e = g->off + n;
    *got = 1;
    *more = n > 0;
    g->off += n;
    if (n == 0) g->closed = 1;
    return OK;
}

static status_t gen_span(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    (void)ctx; (void)s; (void)e; (void)got;
    return set_error(ERR_INVALID, "pieces only");
}

static status_t gen_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    gen_t *g = ctx;
    if (off < g->buf_off || off + n > g->buf_off + g->buf_len) return set_error(ERR_IO, "read outside the piece");
    memcpy(buf, g->buf + (off - g->buf_off), n);
    return OK;
}

static gen_t gens[2];
static int keep_second;

static status_t s_open(void *ctx, size_t i, cdc_source_t *src, uint64_t *size)
{
    (void)ctx;
    memset(&gens[i], 0, sizeof(gens[i]));
    gens[i].len = i == 0 ? 3 * 1048576 + 12345 : 777;
    *src = (cdc_source_t){ gen_span, gen_read, &gens[i], gen_piece, NULL };
    *size = CDC_SIZE_STREAM;
    return OK;
}

static int s_keep(void *ctx, size_t i) { (void)ctx; return i == 0 || keep_second; }

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "'") != 0 || mkdir(D, 0700) != 0) return -1;
    if (db_open(&db, DB) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO, "t", &repo_id) != OK) return -1;
    if (repo_store_open(REPO, NULL, &store) != OK) return -1;
    return store_index_bind(store, db, repo_id, &idx) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx);
    store_close(store);
    db_close(db);
    return 0;
}

static void test_stream_and_drop(void **state)
{
    (void)state;
    backup_synth_t synth[2] = {
        { "/.cbackup-unit/hooks/pg.dump", 0600, 0, 0, 0, 0 },
        { "/.cbackup-unit/hooks/bad.dump", 0600, 0, 0, 0, 0 },
    };
    backup_synth_ops_t ops = { s_open, NULL, NULL, s_keep, NULL };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.unit_snapshot = 1;
    o.synth = synth;
    o.n_synth = 2;
    o.synth_ops = &ops;
    keep_second = 0;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    int64_t snap = r.snapshot_id;
    backup_result_free(&r);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    struct stat sb;
    assert_int_equal(stat(DEST "/.cbackup-unit/hooks/pg.dump", &sb), 0);
    assert_int_equal((uint64_t)sb.st_size, gens[0].len);
    assert_int_not_equal(stat(DEST "/.cbackup-unit/hooks/bad.dump", &sb), 0);
    FILE *f = fopen(DEST "/.cbackup-unit/hooks/pg.dump", "rb");
    assert_non_null(f);
    uint8_t buf[65536];
    uint64_t off = 0;
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        for (size_t i = 0; i < n; i++)
            if (buf[i] != gen_byte(off + i)) fail_msg("byte %llu differs", (unsigned long long)(off + i));
        off += n;
    }
    fclose(f);
    assert_int_equal(off, gens[0].len);
}

static status_t put_mem(void *ctx, uint8_t type, const void *data, size_t len, obj_put_t *out)
{
    (void)ctx; (void)type; (void)data;
    memset(out, 0, sizeof(*out));
    out->is_new = 1;
    out->phys_bytes = len;
    return OK;
}

static void test_cdc_stream_sizes(void **state)
{
    (void)state;
    cdc_params_t p;
    cdc_params_default(&p);
    obj_sink_t sink = { .put = put_mem, .ctx = NULL };
    uint64_t lens[] = { 0, 1, 4096, 2 * 1048576 + 3 };
    for (size_t k = 0; k < sizeof(lens) / sizeof(lens[0]); k++) {
        gen_t *g = calloc(1, sizeof(*g));
        assert_non_null(g);
        g->len = lens[k];
        cdc_source_t src = { gen_span, gen_read, g, gen_piece };
        cdc_result_t cr;
        assert_int_equal(cdc_store_source(&p, &src, CDC_SIZE_STREAM, &sink, NULL, &cr), OK);
        assert_int_equal(cr.size, lens[k]);
        free(g);
    }
}

static void put_file(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static long long count_of(const char *sql, const char *arg)
{
    char r[32];
    snprintf(r, sizeof(r), "%lld", (long long)repo_id);
    db_result_t *res = NULL;
    long long n = -1;
    if (db_execute(db, sql, (const char *[]){ r, arg, NULL }, &res) == 0 && res && res->nrows == 1)
        n = strtoll(res->rows[0][0], NULL, 10);
    db_result_free(res);
    return n;
}

typedef struct { int calls; long long held_rows, other_rows; } hold_seen_t;

static void on_held(void *ctx)
{
    hold_seen_t *h = ctx;
    h->calls++;
    h->held_rows = count_of("SELECT COUNT(*) FROM file_cache WHERE repo_id = ? AND path LIKE ?", D "/held/%");
    h->other_rows = count_of("SELECT COUNT(*) FROM file_cache WHERE repo_id = ? AND path LIKE ?", D "/other/%");
}

/* Pass 1 stores content and cache rows and no snapshot; the held roots'
 * files are all stored before the hold is let go, the others' after. */
static void test_content_only_and_hold(void **state)
{
    (void)state;
    assert_int_equal(mkdir(D "/held", 0700), 0);
    assert_int_equal(mkdir(D "/other", 0700), 0);
    char p[256];
    for (int i = 0; i < 20; i++) {
        snprintf(p, sizeof(p), D "/held/f%d", i);
        put_file(p, "held content");
        snprintf(p, sizeof(p), D "/other/g%d", i);
        put_file(p, "other content");
    }
    const char *held[] = { D "/held" };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = held;
    o.n_paths = 1;
    o.content_only = 1;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    assert_int_equal(r.snapshot_id, 0);
    backup_result_free(&r);
    assert_int_equal(count_of("SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND ? IS NOT NULL", "x"), 0);

    /* The real run: the held root's files come from pass 1's cache, and the
     * hold is released before the other root is stored. */
    const char *both[] = { D "/held", D "/other" };
    hold_seen_t h = { 0, 0, 0 };
    memset(&o, 0, sizeof(o));
    o.paths = both;
    o.n_paths = 2;
    o.held_roots = 1;
    o.held_done = on_held;
    o.held_ctx = &h;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    assert_true(r.snapshot_id > 0);
    assert_int_equal(r.files_unchanged, 20);         /* pass 1 vouched for them */
    backup_result_free(&r);
    assert_int_equal(h.calls, 1);
    assert_int_equal(h.held_rows, 20);
    assert_int_equal(h.other_rows, 0);
}

typedef struct { struct timespec t0; double held_s; long long small_rows, big_rows; const char *big; } held_clock_t;

static void on_held_clock(void *ctx)
{
    held_clock_t *h = ctx;
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    h->held_s = (double)(t.tv_sec - h->t0.tv_sec) + (double)(t.tv_nsec - h->t0.tv_nsec) / 1e9;
    h->small_rows = count_of("SELECT COUNT(*) FROM file_cache WHERE repo_id = ? AND path LIKE ?", D "/small/%");
    char like[200];
    snprintf(like, sizeof(like), "%s/%%", h->big);
    h->big_rows = count_of("SELECT COUNT(*) FROM file_cache WHERE repo_id = ? AND path LIKE ?", like);
}

/* One run: a small held tree and a snapshot tree of n files; how long the
 * hold lasted, from the start of the run to its release. */
static double held_for(const char *big, int n)
{
    char p[256];
    snprintf(p, sizeof(p), "%s", big);
    assert_int_equal(mkdir(p, 0700), 0);
    for (int i = 0; i < n; i++) {
        if (i % 500 == 0) {
            snprintf(p, sizeof(p), "%s/d%d", big, i / 500);
            assert_int_equal(mkdir(p, 0700), 0);
        }
        snprintf(p, sizeof(p), "%s/d%d/f%d", big, i / 500, i);
        put_file(p, "x");
    }
    const char *paths[] = { D "/small", big };
    held_clock_t h;
    memset(&h, 0, sizeof(h));
    h.big = big;
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 2;
    o.held_roots = 1;
    o.held_done = on_held_clock;
    o.held_ctx = &h;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    clock_gettime(CLOCK_MONOTONIC, &h.t0);
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    /* Every file of both roots made it in, the held ones first. */
    assert_true(r.files >= (uint64_t)n + 10);
    backup_result_free(&r);
    /* At the release the held files were in, none of the snapshot tree's. */
    assert_int_equal(h.small_rows, 10);
    assert_int_equal(h.big_rows, 0);
    return h.held_s;
}

/* The hold covers the held roots' scan and store only: the snapshot
 * tree's directory walk comes after it, so ten times the files there
 * leaves the hold about as long. */
static void test_hold_excludes_rest_scan(void **state)
{
    (void)state;
    assert_int_equal(mkdir(D "/small", 0700), 0);
    char p[256];
    for (int i = 0; i < 10; i++) {
        snprintf(p, sizeof(p), D "/small/s%d", i);
        put_file(p, "held");
    }
    double a = held_for(D "/big1", 2000);
    double b = held_for(D "/big2", 20000);
    print_message("held for %.6f s with 2000 snapshot files, %.6f s with 20000\n", a, b);
    assert_true(b < 2 * a + 0.5);
}

/* The suite's scratch goes with it: tests/scratch-guard.sh fails a suite
 * that leaves anything behind (SCRATCH-1). Setup removes it too, so a
 * killed run is no obstacle to the next. */
static int group_cleanup(void **state)
{
    (void)state;
    int rc = system("rm -rf '" D "'");
    (void)rc;
    return 0;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_stream_and_drop, setup, teardown),
        cmocka_unit_test(test_cdc_stream_sizes),
        cmocka_unit_test_setup_teardown(test_content_only_and_hold, setup, teardown),
        cmocka_unit_test_setup_teardown(test_hold_excludes_rest_scan, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
