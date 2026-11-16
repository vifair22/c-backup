#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * Content-defined chunking through the daemon's own paths (format 3):
 * backup, restore, gc, prune, reindex and replication on files that are
 * chunk lists and directories that are fragmented.
 *
 * tests/test_cdc.c holds the chunker and the format to their contracts in
 * isolation; this holds the engine to them end to end, where the failures
 * that matter are quiet ones - a chunk gc decides nobody needs, a list the
 * index forgets, a replica that holds chunks its own chunker cannot cut.
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
#include <sys/stat.h>
#include <stdatomic.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "cdc.h"
#include "chunklist.h"
#include "gc.h"
#include "pack.h"
#include "verify.h"
#include "manifest.h"
#include "object.h"
#include "prune.h"
#include "reindex.h"
#include "replicate.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_cdcb"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define DST_REPO  TEST_DIR "/dst"
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
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
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
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static uint64_t rng;
static uint64_t rnd(void)
{
    rng ^= rng >> 12; rng ^= rng << 25; rng ^= rng >> 27;
    return rng * 0x2545F4914F6CDD1DULL;
}

static uint8_t *rand_buf(size_t n, uint64_t seed)
{
    rng = seed ? seed : 1;
    uint8_t *b = malloc(n ? n : 1);
    assert_non_null(b);
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = rnd();
        memcpy(b + i, &v, n - i < 8 ? n - i : 8);
    }
    return b;
}

static void write_buf(const char *path, const void *data, size_t n)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    if (n) assert_int_equal(fwrite(data, 1, n, f), n);
    fclose(f);
}

static void write_text(const char *path, const char *s)
{
    write_buf(path, s, strlen(s));
}

static backup_result_t backup(unsigned workers)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    return r;
}

static void root_of(int64_t snap, char out[OBJECT_HASH_SIZE * 2 + 1])
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT root_tree_hash FROM snapshots WHERE id = ?",
                                (const char *[]){ id, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(out, OBJECT_HASH_SIZE * 2 + 1, "%s", r->rows[0][0]);
    db_result_free(r);
}

/* The source directory's own tree in a snapshot. The snapshot root also
 * covers the source path's ancestors, whose metadata other processes on
 * the machine change between runs; the source's tree is what the backup
 * made from the files. */
static void src_tree_of(int64_t snap, char out[OBJECT_HASH_SIZE * 2 + 1])
{
    char id[32], repo[32];
    snprintf(id, sizeof(id), "%lld", (long long)snap);
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT subtree_hash FROM nodes WHERE snap_id = ?"
                                " AND repo_id = ? AND name = 'src' AND type = 2",
                                (const char *[]){ id, repo, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(out, OBJECT_HASH_SIZE * 2 + 1, "%s", r->rows[0][0]);
    db_result_free(r);
}

static int scalar(const char *sql)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

static void restore_and_diff(int64_t snap)
{
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
             TEST_SRC, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
}

/* ------------------------------------------------------------------ */

/*
 * The snapshot does not depend on how many workers ran: boundaries are cut
 * by one sequential scan, and only the per-chunk work is parallel. The
 * second run, with the file cache cleared so every file is read again,
 * must produce the same root and store nothing.
 */
static void test_workers_do_not_change_the_snapshot(void **state)
{
    (void)state;
    uint8_t *big = rand_buf(40 * MiB + 777, 3);
    write_buf(TEST_SRC "/big.bin", big, 40 * MiB + 777);
    /* Twins: the same chunks on whatever workers they land, stored once. */
    write_buf(TEST_SRC "/twin.bin", big, 40 * MiB + 777);
    free(big);
    uint8_t *mid = rand_buf(3 * MiB, 4);
    write_buf(TEST_SRC "/mid.bin", mid, 3 * MiB);
    free(mid);
    write_text(TEST_SRC "/small.txt", "small\n");

    backup_result_t a = backup(1);
    assert_int_equal(a.store_workers, 1);
    assert_true(a.objects_new > 30);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM file_cache", NULL, NULL), 0);
    backup_result_t b = backup(4);
    assert_int_equal(b.store_workers, 4);
    assert_int_equal(b.objects_new, 0);

    char ra[OBJECT_HASH_SIZE * 2 + 1], rb[OBJECT_HASH_SIZE * 2 + 1];
    src_tree_of(a.snapshot_id, ra);
    src_tree_of(b.snapshot_id, rb);
    assert_string_equal(ra, rb);

    /* The twins name one list, held once. */
    assert_int_equal(scalar("SELECT COUNT(DISTINCT content_hash) FROM nodes"
                            " WHERE repo_id = ? AND name IN ('big.bin', 'twin.bin')"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects p JOIN nodes n"
                            "  ON n.repo_id = p.repo_id AND n.content_hash = p.hash"
                            " WHERE p.repo_id = ? AND n.name = 'twin.bin'"
                            "   AND n.snap_id = (SELECT MIN(snap_id) FROM nodes)"), 1);

    /* The large file is a list, and every object is in a pack. */
    assert_true(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND type = 6") >= 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                            " AND location IN ('loose', 'staged')"), 0);
    restore_and_diff(b.snapshot_id);
}

/*
 * Small files on workers (small_files.h): a fresh backup of a tree of many
 * small files gives the same snapshot, the same objects with the same rows,
 * the same counters and a cache row per file whether one thread stores them
 * or eight do. The tree has duplicates within and across batches, empty
 * files, a file with a hole, one that is several chunks, and one that
 * cannot be opened.
 */
static void make_small_tree(void)
{
    char path[256];
    assert_int_equal(mkdir(TEST_SRC "/d0", 0700), 0);
    assert_int_equal(mkdir(TEST_SRC "/d1", 0700), 0);
    for (int i = 0; i < 1500; i++) {
        snprintf(path, sizeof(path), TEST_SRC "/d%d/f%04d.dat", i % 2, i);
        size_t n = (size_t)(i * 37) % 9000;
        /* Every tenth repeats an earlier file's content. */
        uint8_t *d = rand_buf(n, (uint64_t)(i % 10 == 0 ? i / 10 + 1 : 1000 + i));
        write_buf(path, d, n);
        free(d);
    }
    uint8_t *d = rand_buf(900 * 1024, 77);
    write_buf(TEST_SRC "/several-chunks.bin", d, 900 * 1024);
    int fd = open(TEST_SRC "/holey.bin", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    assert_int_equal(pwrite(fd, d, 100 * 1024, 0), 100 * 1024);
    assert_int_equal(pwrite(fd, d, 100 * 1024, 800 * 1024), 100 * 1024);
    close(fd);
    free(d);
    write_text(TEST_SRC "/locked.txt", "nobody may read this\n");
    assert_int_equal(chmod(TEST_SRC "/locked.txt", 0), 0);
}

/* Every object but the trees: the trees above the source directory hold
 * the scratch directories' own times, which the second repository's
 * creation moves; the source's tree is compared on its own. */
static char *object_rows_of(int64_t rid)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)rid);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT hash, type, orig_size, phys_size FROM objects"
                                " WHERE repo_id = ? AND type != 5 ORDER BY hash",
                                (const char *[]){ repo, NULL }, &r), 0);
    size_t cap = (size_t)r->nrows * 120 + 1, off = 0;
    char *out = malloc(cap);
    assert_non_null(out);
    out[0] = '\0';
    for (int i = 0; i < r->nrows; i++)
        off += (size_t)snprintf(out + off, cap - off, "%s %s %s %s\n", r->rows[i][0],
                                r->rows[i][1], r->rows[i][2], r->rows[i][3]);
    db_result_free(r);
    return out;
}

static void test_small_files_on_workers_match_serial(void **state)
{
    (void)state;
    make_small_tree();

    /* Eight workers into the setup's repository. */
    backup_result_t a = backup(8);
    assert_int_equal(a.store_workers, 8);

    /* One thread into a second, fresh one. */
    int64_t rid2 = 0;
    store_t *st2 = NULL;
    store_index_t *idx2 = NULL;
    assert_int_equal(repo_register(db, TEST_DIR "/serial", "serial", &rid2), OK);
    assert_int_equal(repo_store_open(TEST_DIR "/serial", NULL, &st2), OK);
    assert_int_equal(store_index_bind(st2, db, rid2, &idx2), OK);
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    backup_result_t b;
    memset(&b, 0, sizeof(b));
    assert_int_equal(backup_run(st2, db, rid2, &o, &b), OK);
    assert_int_equal(b.store_workers, 1);

    assert_int_equal(a.files, b.files);
    assert_int_equal(a.bytes_logical, b.bytes_logical);
    assert_int_equal(a.objects_new, b.objects_new);
    assert_int_equal(a.bytes_stored, b.bytes_stored);

    char ta[OBJECT_HASH_SIZE * 2 + 1], tb[OBJECT_HASH_SIZE * 2 + 1];
    src_tree_of(a.snapshot_id, ta);
    char id[32], r2[32];
    snprintf(id, sizeof(id), "%lld", (long long)b.snapshot_id);
    snprintf(r2, sizeof(r2), "%lld", (long long)rid2);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT subtree_hash FROM nodes WHERE snap_id = ?"
                                " AND repo_id = ? AND name = 'src' AND type = 2",
                                (const char *[]){ id, r2, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(tb, sizeof(tb), "%s", r->rows[0][0]);
    db_result_free(r);
    assert_string_equal(ta, tb);

    char *oa = object_rows_of(repo_id), *ob = object_rows_of(rid2);
    assert_string_equal(oa, ob);
    free(oa);
    free(ob);

    /* A cache row for every file stored, none for the one that could not
     * be opened, and nothing left staged. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM file_cache WHERE repo_id = ?"),
                     (int)a.files);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                            " AND location = 'staged'"), 0);
    /* The holey file is a list with its hole as a gap. */
    assert_true(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND type = 6") >= 1);

    /* Unchanged: the next run reads nothing. */
    backup_result_t c = backup(8);
    assert_int_equal(c.files_unchanged, c.files);
    assert_int_equal(c.objects_new, 0);

    store_index_free(idx2);
    store_close(st2);
    assert_int_equal(chmod(TEST_SRC "/locked.txt", 0600), 0);
    assert_int_equal(unlink(TEST_SRC "/locked.txt"), 0);
    /* Every other file is vouched for by the cache rows the workers' run
     * wrote, so this snapshot names exactly the objects they prepared. */
    backup_result_t d = backup(8);
    assert_int_equal(d.objects_new, 0);
    restore_and_diff(d.snapshot_id);
}

/*
 * Write amplification, from the store's own counters (B086): an append and a
 * local edit write about the chunks they touch, not the file. And a sparse
 * file comes back with its holes.
 */
static void test_append_and_edit_write_little(void **state)
{
    (void)state;
    size_t n = 64 * MiB;
    uint8_t *data = rand_buf(n, 21);
    write_buf(TEST_SRC "/big.bin", data, n);
    backup_result_t first = backup(4);
    printf("first: %llu written for %zu bytes\n",
           (unsigned long long)first.written_total, n);
    assert_true(first.written_total >= n);
    /* Written once, into packs. */
    assert_true(first.written_total < n + n / 8);

    uint8_t *more = rand_buf(MiB, 22);
    FILE *f = fopen(TEST_SRC "/big.bin", "ab");
    assert_non_null(f);
    assert_int_equal(fwrite(more, 1, MiB, f), MiB);
    fclose(f);
    free(more);
    backup_result_t app = backup(4);
    printf("append 1 MiB: %llu written\n", (unsigned long long)app.written_total);
    assert_true(app.written_total < MiB + 3 * (uint64_t)CDC_MAX_DEFAULT);

    uint8_t *junk = rand_buf(n / 50, 23);
    int fd = open(TEST_SRC "/big.bin", O_WRONLY);
    assert_true(fd >= 0);
    assert_int_equal(pwrite(fd, junk, n / 50, (off_t)(n / 2)), (ssize_t)(n / 50));
    close(fd);
    free(junk);
    backup_result_t ed = backup(4);
    printf("2%% edit: %llu written\n", (unsigned long long)ed.written_total);
    assert_true(ed.written_total < n / 50 + 3 * (uint64_t)CDC_MAX_DEFAULT);
    restore_and_diff(ed.snapshot_id);
    free(data);

    /* Sparse: 64 MiB with 2 MiB of data. */
    fd = open(TEST_SRC "/sparse.bin", O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 64 * (off_t)MiB), 0);
    uint8_t *d = rand_buf(2 * MiB, 24);
    assert_int_equal(pwrite(fd, d, 2 * MiB, 30 * (off_t)MiB), (ssize_t)(2 * MiB));
    close(fd);
    free(d);
    struct stat sb;
    assert_int_equal(stat(TEST_SRC "/sparse.bin", &sb), 0);
    backup_result_t sp = backup(4);
    restore_and_diff(sp.snapshot_id);
    if ((uint64_t)sb.st_blocks * 512 < 8 * MiB) {
        char out[PATH_MAX];
        snprintf(out, sizeof(out), "%s%s/sparse.bin", TEST_OUT, TEST_SRC);
        struct stat ob;
        assert_int_equal(stat(out, &ob), 0);
        assert_true((uint64_t)ob.st_blocks * 512 < 8 * MiB);
    }
}

/*
 * gc liveness through lists: after the old snapshot is pruned, chunks only
 * it named are swept, chunks the new list shares are kept, and the new
 * snapshot restores byte for byte after compaction rewrote its packs.
 */
static void test_gc_keeps_chunks_of_live_lists(void **state)
{
    (void)state;
    size_t n = 30 * MiB;
    uint8_t *data = rand_buf(n, 31);
    write_buf(TEST_SRC "/big.bin", data, n);
    backup_result_t v1 = backup(4);

    /* Change the second half: new chunks there, shared ones before. */
    uint8_t *tail = rand_buf(n / 2, 32);
    memcpy(data + n / 2, tail, n / 2);
    free(tail);
    write_buf(TEST_SRC "/big.bin", data, n);
    backup_result_t v2 = backup(4);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);
    assert_int_equal(g.content_missing, 0);

    int before = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    assert_int_equal(prune_snapshot(store, db, repo_id, v1.snapshot_id), OK);
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    int after = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    printf("gc after prune: %llu swept, %d -> %d objects\n",
           (unsigned long long)g.swept, before, after);
    assert_true(g.swept >= 5);
    assert_true(after < before);

    restore_and_diff(v2.snapshot_id);
    free(data);
}

/*
 * Rows created at pack commit are garbage until a snapshot names them.
 *
 * A pack's commit creates the object row of every entry in it (refcount 0,
 * db_pack_commit), so a backup that fails after one of its packs committed
 * leaves rows and pack entries no snapshot reaches. gc must take them: it
 * zeroes every refcount and marks from the snapshots alone (gc_run), so
 * nothing about how a row was created can make it look referenced. The
 * repository verifies clean afterwards and the next backup is whole.
 */
typedef struct {
    atomic_int *cancel;
    uint64_t    at;
} cancel_at_t;

static void cancel_at_bytes(void *ctx, uint64_t done, uint64_t total)
{
    cancel_at_t *c = ctx;
    (void)total;
    if (done >= c->at) atomic_store(c->cancel, 1);
}

static void quiet_progress(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)phase; (void)done; (void)total; (void)ctx;
}

static void count_pack(uint32_t num, void *ctx)
{
    (void)num;
    (*(int *)ctx)++;
}

static void test_failed_backup_leaves_garbage_gc_reclaims(void **state)
{
    (void)state;
    /* Small packs, so the file workers seal several before the cancel. */
    pack_test_set_seal(4 * MiB);
    for (int f = 0; f < 2; f++) {
        uint8_t *d = rand_buf(24 * MiB, 71 + (uint64_t)f);
        char path[256];
        snprintf(path, sizeof(path), TEST_SRC "/big%d.bin", f);
        write_buf(path, d, 24 * MiB);
        free(d);
    }

    static const char *paths[] = { TEST_SRC };
    atomic_int cancel = 0;
    cancel_at_t ca = { &cancel, 24 * MiB };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 8;          /* two file workers: rows only at commit */
    o.cancel = &cancel;
    o.progress_cb = quiet_progress;
    o.bytes_cb = cancel_at_bytes;
    o.progress_ctx = &ca;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_BUSY);
    err_clear();

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
    int committed = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                           " AND location != 'staged'");
    printf("cancelled backup left %d committed object row(s)\n", committed);
    assert_true(committed > 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                            " AND refcount != 0"), 0);
    int packs = 0;
    assert_int_equal(pack_list(store, count_pack, &packs), OK);
    assert_true(packs > 0);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_true(g.swept >= (uint64_t)committed);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), 0);
    packs = 0;
    assert_int_equal(pack_list(store, count_pack, &packs), OK);
    assert_int_equal(packs, 0);

    verify_result_t v;
    memset(&v, 0, sizeof(v));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &v), OK);
    assert_int_equal(v.missing, 0);
    assert_int_equal(v.unrecoverable, 0);
    assert_int_equal(v.ok, v.checked);

    /* And the next backup is whole. */
    backup_result_t r = backup(8);
    restore_and_diff(r.snapshot_id);
    pack_test_set_seal(0);
}

/*
 * Reindex from an empty object table: every list and chunk gets its row
 * back with its real type, so a gc afterwards keeps every chunk and the
 * snapshot still restores.
 */
static void test_reindex_restores_chunk_rows(void **state)
{
    (void)state;
    uint8_t *data = rand_buf(24 * MiB, 41);
    write_buf(TEST_SRC "/big.bin", data, 24 * MiB);
    free(data);
    write_text(TEST_SRC "/small.txt", "small\n");
    backup_result_t b = backup(4);
    int objs = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    int lists = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND type = 6");
    assert_true(lists >= 1);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), objs);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ? AND type = 6"),
                     lists);
    /* Counted the way gc counts them: nothing reachable at zero. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                            " AND refcount = 0"), 0);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);
    restore_and_diff(b.snapshot_id);
}

/*
 * OWN-34: a directory of more than 100,000 entries. Its tree is fragments
 * named by a list, every object bounded, and backup, gc and reindex all
 * succeed; one file added rewrites a fragment, not the directory.
 */
#define HUGE_DIR 110000u

static void test_huge_directory(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
    char path[256];
    for (unsigned i = 0; i < HUGE_DIR; i++) {
        snprintf(path, sizeof(path), TEST_SRC "/many/entry-%06u", i);
        int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        assert_true(fd >= 0);
        close(fd);
    }
    backup_result_t b = backup(1);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT subtree_hash FROM nodes WHERE name = 'many'",
                                NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(hex, sizeof(hex), "%s", r->rows[0][0]);
    db_result_free(r);
    uint8_t h[OBJECT_HASH_SIZE];
    for (int k = 0; k < OBJECT_HASH_SIZE; k++) {
        unsigned v = 0;
        assert_int_equal(sscanf(hex + k * 2, "%2x", &v), 1);
        h[k] = (uint8_t)v;
    }
    uint64_t size = 0;
    uint8_t type = 0;
    assert_int_equal(object_get_info(store, h, &size, &type), OK);
    assert_int_equal(type, OBJECT_TYPE_CHUNKLIST);
    manifest_entry_t *ents = NULL;
    uint32_t cnt = 0;
    assert_int_equal(manifest_read_tree(store, h, &ents, &cnt), OK);
    assert_int_equal(cnt, HUGE_DIR);
    manifest_free_entries(ents, cnt);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);

    /* Every entry, plus the source path's own directories. */
    int nodes = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?");
    assert_true(nodes > (int)HUGE_DIR);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?"), nodes);

    /* One more file: a fragment and the list are new, not the directory. */
    write_text(TEST_SRC "/many/entry-055555x", "x");
    backup_result_t b2 = backup(1);
    printf("one entry added to %u: %llu new objects, %llu new trees\n", HUGE_DIR,
           (unsigned long long)b2.objects_new, (unsigned long long)b2.trees_new);
    assert_true(b2.objects_new <= 8);
    (void)b;
}

/*
 * Replication into a repository under another key: its gear table differs,
 * so file content is re-chunked and trees rebuilt for it, the target's
 * root differs from the source's, and a restore from the target after a
 * reindex is byte for byte the source. A second run adds nothing.
 */
static void test_replicate_across_keys_rechunks(void **state)
{
    (void)state;
    uint8_t *data = rand_buf(20 * MiB, 51);
    write_buf(TEST_SRC "/big.bin", data, 20 * MiB);
    free(data);
    write_text(TEST_SRC "/small.txt", "small\n");
    int fd = open(TEST_SRC "/sparse.bin", O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 40 * (off_t)MiB), 0);
    uint8_t *d = rand_buf(MiB, 52);
    assert_int_equal(pwrite(fd, d, MiB, 10 * (off_t)MiB), (ssize_t)MiB);
    close(fd);
    free(d);
    backup_result_t b = backup(4);
    char src_root[OBJECT_HASH_SIZE * 2 + 1];
    root_of(b.snapshot_id, src_root);

    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = REPO_ENC_SYSTEM;
    enc.key_dir = TEST_KEYS;
    int64_t dst_id = 0;
    assert_int_equal(repo_register_ex(db, DST_REPO, "dst", &enc, &dst_id), OK);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &m, &sink), OK);
    replicate_result_t rr;
    assert_int_equal(replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr), OK);
    sink.v->close(sink.ctx);
    assert_int_equal(rr.snapshots_sent, 1);
    assert_int_equal(rr.rechunked, 1);
    assert_true(rr.files_rechunked >= 2);
    char dst_root[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(rr.target_root, dst_root);
    assert_string_not_equal(src_root, dst_root);

    /* Again: the target has the snapshot, nothing is rebuilt. */
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &m, &sink), OK);
    assert_int_equal(replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr), OK);
    sink.v->close(sink.ctx);
    assert_int_equal(rr.snapshots_sent, 0);
    assert_int_equal(rr.snapshots_skipped, 1);

    store_t *dst = NULL;
    store_index_t *didx = NULL;
    assert_int_equal(repo_store_open(DST_REPO, NULL, &dst), OK);
    assert_int_equal(store_index_bind(dst, db, dst_id, &didx), OK);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(dst, db, dst_id, NULL, &rx), OK);
    rmrf(TEST_OUT);
    restore_result_t res;
    memset(&res, 0, sizeof(res));
    assert_int_equal(restore_run(dst, db, dst_id, b.snapshot_id, TEST_OUT, NULL, &res), OK);
    assert_int_equal(res.failed, 0);
    store_index_free(didx);
    store_close(dst);
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
             TEST_SRC, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
}

/*
 * Replication to a replica of the same repository copies chunks as they
 * are: same root, nothing re-chunked, and the replica reads back.
 */
static void test_replicate_same_key_copies(void **state)
{
    (void)state;
    uint8_t *data = rand_buf(20 * MiB, 61);
    write_buf(TEST_SRC "/big.bin", data, 20 * MiB);
    backup_result_t b = backup(4);
    char src_root[OBJECT_HASH_SIZE * 2 + 1];
    root_of(b.snapshot_id, src_root);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(DST_REPO, &m, &sink), OK);
    replicate_result_t rr;
    assert_int_equal(replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr), OK);
    sink.v->close(sink.ctx);
    assert_int_equal(rr.rechunked, 0);
    char dst_root[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(rr.target_root, dst_root);
    assert_string_equal(src_root, dst_root);

    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(DST_REPO, &rd), OK);
    store_t *rs = replica_reader_store(rd);
    manifest_entry_t *ents = NULL;
    uint32_t cnt = 0;
    assert_int_equal(manifest_read_tree(rs, rr.target_root, &ents, &cnt), OK);
    /* Down to big.bin: the synthetic root holds the source path's dirs. */
    uint8_t cur[OBJECT_HASH_SIZE];
    int found = 0;
    for (int depth = 0; depth < 32 && !found; depth++) {
        int next = -1;
        for (uint32_t i = 0; i < cnt; i++) {
            if (!strcmp(ents[i].name, "big.bin")) {
                memcpy(cur, ents[i].target_hash, OBJECT_HASH_SIZE);
                found = 1;
                break;
            }
            if (ents[i].type == NODE_TYPE_DIR) next = (int)i;
        }
        if (found || next < 0) break;
        memcpy(cur, ents[next].target_hash, OBJECT_HASH_SIZE);
        manifest_free_entries(ents, cnt);
        ents = NULL;
        assert_int_equal(manifest_read_tree(rs, cur, &ents, &cnt), OK);
    }
    manifest_free_entries(ents, cnt);
    assert_true(found);
    rmrf(TEST_OUT);
    assert_int_equal(mkdir(TEST_OUT, 0700), 0);
    int fd = open(TEST_OUT "/big.bin", O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    uint64_t sz = 0;
    assert_int_equal(content_restore_fd(rs, cur, fd, &sz), OK);
    close(fd);
    replica_reader_close(rd);
    assert_true(sz == 20 * MiB);
    FILE *f = fopen(TEST_OUT "/big.bin", "rb");
    assert_non_null(f);
    uint8_t *got = malloc(20 * MiB);
    assert_int_equal(fread(got, 1, 20 * MiB, f), 20 * MiB);
    fclose(f);
    assert_memory_equal(got, data, 20 * MiB);
    free(got);
    free(data);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_workers_do_not_change_the_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_small_files_on_workers_match_serial, setup, teardown),
        cmocka_unit_test_setup_teardown(test_append_and_edit_write_little, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_keeps_chunks_of_live_lists, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_backup_leaves_garbage_gc_reclaims, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_restores_chunk_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_huge_directory, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicate_across_keys_rechunks, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicate_same_key_copies, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
