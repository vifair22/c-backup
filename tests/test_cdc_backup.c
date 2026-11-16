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
#include <lz4.h>
#include <openssl/evp.h>
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
 * Every pack entry in a store: count and payload bytes, and how many names
 * appear in more than one entry.
 */
typedef struct {
    store_t *st;
    uint64_t entries, payload;
    uint8_t (*names)[OBJECT_HASH_SIZE];
    size_t   n, cap;
} pack_census_t;

static void census_entry(const pack_entry_info_t *info, void *ctx)
{
    pack_census_t *c = ctx;
    c->entries++;
    c->payload += info->compressed_size;
    if (c->n == c->cap) {
        c->cap = c->cap ? c->cap * 2 : 1024;
        c->names = realloc(c->names, c->cap * sizeof(*c->names));
        assert_non_null(c->names);
    }
    memcpy(c->names[c->n++], info->hash, OBJECT_HASH_SIZE);
}

static void census_pack(uint32_t num, void *ctx)
{
    pack_census_t *c = ctx;
    char name[64];
    snprintf(name, sizeof(name), "pack-" PACK_NUM_FMT ".dat", num);
    uint32_t ver = 0, cnt = 0;
    assert_int_equal(pack_enumerate_dat(c->st, name, &ver, &cnt, census_entry, c), OK);
}

static int hash_cmp(const void *a, const void *b)
{
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

static void census(store_t *st, pack_census_t *c, size_t *dup_names)
{
    memset(c, 0, sizeof(*c));
    c->st = st;
    assert_int_equal(object_store_flush(st), OK);
    assert_int_equal(pack_list(st, census_pack, c), OK);
    qsort(c->names, c->n, sizeof(*c->names), hash_cmp);
    size_t d = 0;
    for (size_t i = 1; i < c->n; i++)
        if (memcmp(c->names[i], c->names[i - 1], OBJECT_HASH_SIZE) == 0 &&
            (i < 2 || memcmp(c->names[i - 1], c->names[i - 2], OBJECT_HASH_SIZE) != 0))
            d++;
    *dup_names = d;
    free(c->names);
}

/*
 * OWN-46: several large files at once, each on a file worker with a pack
 * writer of its own, against one at a time. Twins - identical large files -
 * land on different workers, which used to store their chunks once each:
 * the same name in two packs, the bytes twice, one index row pointing at
 * either. Now a chunk one writer is storing is not stored again by another,
 * so the parallel run writes exactly what the serial one does.
 */
static void test_file_workers_do_not_store_twins_twice(void **state)
{
    (void)state;
    uint8_t *big = rand_buf(24 * MiB + 333, 461);
    write_buf(TEST_SRC "/twin-a.bin", big, 24 * MiB + 333);
    write_buf(TEST_SRC "/twin-b.bin", big, 24 * MiB + 333);
    write_buf(TEST_SRC "/twin-c.bin", big, 24 * MiB + 333);
    free(big);
    uint8_t *other = rand_buf(20 * MiB, 462);
    write_buf(TEST_SRC "/other.bin", other, 20 * MiB);
    free(other);

    /* Sixteen chunk workers: a quarter of them, four, are file workers,
     * capped at one per large file. */
    backup_result_t a = backup(16);
    assert_int_equal(a.store_workers, 16);

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

    pack_census_t ca, cb;
    size_t dup_a = 0, dup_b = 0;
    census(store, &ca, &dup_a);
    census(st2, &cb, &dup_b);
    printf("parallel: %llu objects new, %llu entries, %llu payload bytes, %zu names "
           "stored twice; serial: %llu, %llu, %llu, %zu\n",
           (unsigned long long)a.objects_new, (unsigned long long)ca.entries,
           (unsigned long long)ca.payload, dup_a,
           (unsigned long long)b.objects_new, (unsigned long long)cb.entries,
           (unsigned long long)cb.payload, dup_b);
    assert_int_equal(dup_b, 0);
    assert_int_equal(dup_a, 0);
    assert_int_equal(a.objects_new, b.objects_new);
    assert_int_equal(a.bytes_stored, b.bytes_stored);
    assert_int_equal(ca.entries, cb.entries);
    assert_int_equal(ca.payload, cb.payload);

    char *ra = NULL, *rb = NULL;
    {
        char repo[2][32];
        snprintf(repo[0], 32, "%lld", (long long)repo_id);
        snprintf(repo[1], 32, "%lld", (long long)rid2);
        for (int k = 0; k < 2; k++) {
            db_result_t *r = NULL;
            assert_int_equal(db_execute(db, "SELECT hash, type, orig_size, phys_size"
                                        " FROM objects WHERE repo_id = ? AND type != 5"
                                        " ORDER BY hash",
                                        (const char *[]){ repo[k], NULL }, &r), 0);
            size_t cap = (size_t)r->nrows * 120 + 1, off = 0;
            char *out = malloc(cap);
            out[0] = '\0';
            for (int i = 0; i < r->nrows; i++)
                off += (size_t)snprintf(out + off, cap - off, "%s %s %s %s\n",
                                        r->rows[i][0], r->rows[i][1],
                                        r->rows[i][2], r->rows[i][3]);
            db_result_free(r);
            if (k == 0) ra = out; else rb = out;
        }
    }
    assert_string_equal(ra, rb);
    free(ra);
    free(rb);

    store_index_free(idx2);
    store_close(st2);
    restore_and_diff(a.snapshot_id);
}

/*
 * OWN-22: gc reclaims a duplicate copy of an object - the same name in two
 * pack entries - keeping the one the index names, once that one has been
 * read and confirmed. A copy is planted by writing the object's stored
 * bytes into a second pack, as a dedup race used to.
 */
static uint64_t packs_bytes(void)
{
    char cmd[600];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' -printf '%%s\\n'"
             " | awk '{s+=$1} END {print s+0}'", TEST_REPO);
    FILE *f = popen(cmd, "r");
    assert_non_null(f);
    unsigned long long v = 0;
    assert_int_equal(fscanf(f, "%llu", &v), 1);
    pclose(f);
    return (uint64_t)v;
}

/* The one object a 300 KiB file is stored as, and a second copy of it in a
 * pack of its own. Returns the index's location for it after the plant. */
static void plant_duplicate(uint8_t hash[OBJECT_HASH_SIZE], uint32_t *pnum, uint64_t *off)
{
    uint8_t *d = rand_buf(300 * 1024, 4610);
    write_buf(TEST_SRC "/one.bin", d, 300 * 1024);
    free(d);
    backup_result_t r = backup(1);
    (void)r;
    db_result_t *q = NULL;
    assert_int_equal(db_execute(db, "SELECT content_hash FROM nodes WHERE name = 'one.bin'",
                                NULL, &q), 0);
    assert_int_equal(q->nrows, 1);
    for (int k = 0; k < OBJECT_HASH_SIZE; k++) {
        unsigned v = 0;
        assert_int_equal(sscanf(q->rows[0][0] + k * 2, "%2x", &v), 1);
        hash[k] = (uint8_t)v;
    }
    db_result_free(q);

    object_stored_info_t info;
    void *payload = NULL;
    size_t len = 0;
    int damaged = 0;
    assert_int_equal(object_read_stored(store, hash, &info, &payload, &len, &damaged), OK);
    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, hash, info.type, info.compression, info.encryption,
                                     info.uncompressed_size, payload, len), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);
    free(payload);

    size_t dup = 0;
    pack_census_t c;
    census(store, &c, &dup);
    assert_int_equal(dup, 1);
    assert_int_equal(pack_resolve_location(store, hash, pnum, off), OK);
}

static void test_gc_reclaims_a_duplicate_copy(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    uint32_t pnum = 0;
    uint64_t off = 0;
    plant_duplicate(hash, &pnum, &off);
    uint64_t before = packs_bytes();

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);

    size_t dup = 0;
    pack_census_t c;
    census(store, &c, &dup);
    assert_int_equal(dup, 0);
    uint64_t after = packs_bytes();
    printf("duplicate reclaimed: packs %llu -> %llu bytes\n",
           (unsigned long long)before, (unsigned long long)after);
    assert_true(after + 300 * 1024 / 2 < before);
    uint32_t p2 = 0;
    uint64_t o2 = 0;
    assert_int_equal(pack_resolve_location(store, hash, &p2, &o2), OK);

    verify_result_t v;
    memset(&v, 0, sizeof(v));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &v), OK);
    assert_int_equal(v.ok, v.checked);
    db_result_t *q = NULL;
    assert_int_equal(db_execute(db, "SELECT MAX(id) FROM snapshots", NULL, &q), 0);
    int64_t snap = strtoll(q->rows[0][0], NULL, 10);
    db_result_free(q);
    restore_and_diff(snap);
}

static int64_t latest_snapshot(void)
{
    db_result_t *q = NULL;
    assert_int_equal(db_execute(db, "SELECT MAX(id) FROM snapshots", NULL, &q), 0);
    int64_t snap = strtoll(q->rows[0][0], NULL, 10);
    db_result_free(q);
    return snap;
}

/* Overwrite n bytes at off in pack pnum with a fixed fill. */
static void ruin(uint32_t pnum, uint64_t off, size_t n)
{
    char path[600];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t *junk = malloc(n);
    assert_non_null(junk);
    memset(junk, 0xA5, n);
    assert_int_equal(pwrite(fd, junk, n, (off_t)off), (ssize_t)n);
    free(junk);
    close(fd);
    pack_cache_invalidate(store);
}

/*
 * The copy the index names is no longer that object - its header's name is
 * ruined past its parity - so gc must keep the other copy rather than drop the only good
 * one, and having proven it (header, name, payload against the name) moves
 * the index to it. The ruined entry, which no name claims now, goes with
 * its pack, and the restore needs nothing of it.
 */
static void test_gc_moves_the_index_off_a_ruined_header(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    uint32_t pnum = 0;
    uint64_t off = 0;
    plant_duplicate(hash, &pnum, &off);
    /* The name alone is within the stripe's parity (a header is ordinary
     * body, B119), which would put it back; 600 more bytes in the same
     * group take that group past what its parity carries. */
    ruin(pnum, off + offsetof(pack_dat_entry_hdr_t, hash), OBJECT_HASH_SIZE);
    ruin(pnum, off + sizeof(pack_dat_entry_hdr_t) + 100, 600);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);

    uint32_t p2 = 0;
    uint64_t o2 = 0;
    assert_int_equal(pack_resolve_location(store, hash, &p2, &o2), OK);
    assert_true(p2 != pnum);
    size_t dup = 0;
    pack_census_t c;
    census(store, &c, &dup);
    assert_int_equal(dup, 0);
    char path[600];
    struct stat sb;
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);
    assert_int_not_equal(stat(path, &sb), 0);    /* it held only that copy */
    restore_and_diff(latest_snapshot());
}

/*
 * The named copy's header is sound but its payload is damaged past its
 * parity. Confirmation reads the payload against its CRC, so it fails, and
 * the good copy is not dropped as a duplicate of a bad one: it is proven
 * and indexed, the restore reads it, and the next gc - finding the damaged
 * copy unnamed and the named one confirmed - drops the damaged one.
 */
static void test_gc_moves_the_index_off_a_damaged_payload(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    uint32_t pnum = 0;
    uint64_t off = 0;
    plant_duplicate(hash, &pnum, &off);
    ruin(pnum, off + sizeof(pack_dat_entry_hdr_t) + 1024, 64 * 1024);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    /* The move is counted and reported (B138). */
    assert_int_equal(g.index_relocated, 1);
    assert_int_equal(g.dups_dropped, 0);
    uint32_t p2 = 0;
    uint64_t o2 = 0;
    assert_int_equal(pack_resolve_location(store, hash, &p2, &o2), OK);
    assert_true(p2 != pnum);
    restore_and_diff(latest_snapshot());

    uint64_t before = packs_bytes();
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    size_t dup = 0;
    pack_census_t c;
    census(store, &c, &dup);
    assert_int_equal(dup, 0);
    assert_true(packs_bytes() < before);
    verify_result_t v;
    memset(&v, 0, sizeof(v));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &v), OK);
    assert_int_equal(v.ok, v.checked);
    restore_and_diff(latest_snapshot());
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
    /* B118: the file that could not be opened is left out and named, the
     * same way on the workers as on the backup thread. Root reads through
     * mode 000, as CI's runner does, and then neither path skips it. */
    if (geteuid() != 0) {
        assert_int_equal(a.files_skipped, 1);
        assert_int_equal(b.files_skipped, 1);
        assert_string_equal(a.skipped[0].path, TEST_SRC "/locked.txt");
        assert_string_equal(b.skipped[0].path, TEST_SRC "/locked.txt");
        assert_string_equal(a.skipped[0].reason, b.skipped[0].reason);
    } else {
        assert_int_equal(a.files_skipped, 0);
        assert_int_equal(b.files_skipped, 0);
    }

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
    assert_int_equal(d.files_skipped, 0);
    restore_and_diff(d.snapshot_id);
    backup_result_free(&a);
    backup_result_free(&b);
    backup_result_free(&c);
    backup_result_free(&d);
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

static status_t sha_emit(void *ctx, const void *data, size_t len)
{
    EVP_DigestUpdate((EVP_MD_CTX *)ctx, data, len);
    return OK;
}

/* ------------------------------------------------------------------ */
/* B124: compress-or-store is decided per chunk                        */
/* ------------------------------------------------------------------ */

/*
 * On 1.0.0 an object that failed the compression probe 8 MiB in was
 * skip-marked for good, so a disk image whose first 8 MiB compress well,
 * whose next 56 MiB do not and whose long tail is zeros was stored whole:
 * 13.3 GB across 36 such objects where lz4 gives 6.3 GB. In format 3 every
 * chunk is its own object with its own 64 KiB probe (pb_compress, called
 * from pack_backup_prepare_plain for each chunk), and nothing about one
 * chunk's verdict reaches the next or the next backup.
 *
 * The files are shaped like that image but kept small on disk: a written
 * head and middle, a written zero run (holes are list gaps, so only written
 * zeros exercise dedup) and a long hole for the tail.
 */
#define B124_SHAPED  1   /* compressible head, incompressible middle */
#define B124_REVERSE 2   /* incompressible head, compressible body */

/* Text that compresses about 3:1 with lz4, distinct per seed. */
static void fill_text(uint8_t *b, size_t n, uint64_t seed)
{
    static const char *words[] = { "sector", "cluster", "fat", "boot", "volume",
                                   "driver", "kernel", "config", "setup", "win" };
    rng = seed;
    size_t i = 0;
    while (i < n) {
        const char *w = words[rnd() % 10];
        char tmp[32];
        int k = snprintf(tmp, sizeof(tmp), "%s%u ", w, (unsigned)(rnd() % 1000));
        for (int j = 0; j < k && i < n; j++) b[i++] = (uint8_t)tmp[j];
    }
}

static int write_shaped(const char *path, int shape, uint64_t *out_size,
                        uint64_t *out_zero_off, uint64_t *out_zero_len)
{
    const size_t head = shape == B124_SHAPED ? 4 * MiB : 16 * MiB;
    const size_t body = shape == B124_SHAPED ? 16 * MiB : 8 * MiB;
    const size_t zeros = 24 * MiB;
    const uint64_t total = shape == B124_SHAPED ? (uint64_t)300 * MiB : (uint64_t)120 * MiB;

    uint8_t *a = malloc(head), *m = malloc(body);
    assert_non_null(a);
    assert_non_null(m);
    if (shape == B124_SHAPED) {
        fill_text(a, head, 1241);
        uint8_t *r = rand_buf(body, 1242);
        memcpy(m, r, body);
        free(r);
    } else {
        uint8_t *r = rand_buf(head, 1243);
        memcpy(a, r, head);
        free(r);
        fill_text(m, body, 1244);
    }
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, a, head), (ssize_t)head);
    assert_int_equal(write(fd, m, body), (ssize_t)body);
    uint8_t *z = calloc(1, MiB);
    assert_non_null(z);
    for (size_t k = 0; k < zeros / MiB; k++)
        assert_int_equal(write(fd, z, MiB), (ssize_t)MiB);
    free(z);
    assert_int_equal(ftruncate(fd, (off_t)total), 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    close(fd);
    free(a);
    free(m);
    *out_size = total;
    *out_zero_off = head + body;
    *out_zero_len = zeros;
    /* The tail must be a hole, or this is a 300 MiB file on disk. */
    return (uint64_t)sb.st_blocks * 512 < (uint64_t)(head + body + zeros) + 8 * MiB;
}

/* What lz4 makes of one chunk on its own, or the chunk if that is smaller:
 * the per-chunk ideal, computed without the store. */
static uint64_t lz4_or_raw(const uint8_t *b, size_t n)
{
    int bound = LZ4_compressBound((int)n);
    char *out = malloc((size_t)bound);
    assert_non_null(out);
    int c = LZ4_compress_default((const char *)b, out, (int)n, bound);
    free(out);
    return (c > 0 && (uint64_t)c < n) ? (uint64_t)c : (uint64_t)n;
}

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t off, len;
} leaf_t;

static void check_per_chunk(const char *name, int shape)
{
    char path[256];
    snprintf(path, sizeof(path), TEST_SRC "/%s", name);
    uint64_t size = 0, zoff = 0, zlen = 0;
    if (!write_shaped(path, shape, &size, &zoff, &zlen)) {
        unlink(path);
        skip();   /* no holes here: the tail would be written in full */
    }
    backup_result_t r = backup(4);
    (void)r;

    /* The file's content: a list of chunks with the tail as a gap. */
    db_result_t *q = NULL;
    assert_int_equal(db_execute(db, "SELECT content_hash FROM nodes WHERE name = ?",
                                (const char *[]){ name, NULL }, &q), 0);
    assert_int_equal(q->nrows, 1);
    uint8_t root[OBJECT_HASH_SIZE];
    for (int k = 0; k < OBJECT_HASH_SIZE; k++) {
        unsigned v = 0;
        assert_int_equal(sscanf(q->rows[0][0] + k * 2, "%2x", &v), 1);
        root[k] = (uint8_t)v;
    }
    db_result_free(q);

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t h;
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 0, &h, &it), OK);
    assert_int_equal(h.total_len, size);
    leaf_t *leaves = NULL;
    size_t n = 0, cap = 0;
    chunklist_item_t x;
    int done = 0;
    while (chunklist_iter_next(it, &x, &done) == OK && !done) {
        if (n == cap) {
            cap = cap ? cap * 2 : 64;
            leaves = realloc(leaves, cap * sizeof(*leaves));
            assert_non_null(leaves);
        }
        memcpy(leaves[n].hash, x.hash, OBJECT_HASH_SIZE);
        leaves[n].off = x.offset;
        leaves[n].len = x.length;
        n++;
    }
    chunklist_iter_close(it);
    assert_true(n > 0);
    /* Nothing past the written part: the hole is a gap, not chunks. */
    assert_true(leaves[n - 1].off + leaves[n - 1].len <= zoff + zlen);

    /* Distinct chunks: stored bytes against the per-chunk lz4 ideal, and
     * the zero run's share. */
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    uint64_t ideal = 0, stored = 0, raw = 0;
    size_t distinct = 0, zero_distinct = 0;
    uint64_t zero_covered = 0;
    for (size_t i = 0; i < n; i++) {
        int seen = 0;
        for (size_t j = 0; j < i && !seen; j++)
            seen = memcmp(leaves[j].hash, leaves[i].hash, OBJECT_HASH_SIZE) == 0;
        uint8_t *b = malloc((size_t)leaves[i].len);
        assert_non_null(b);
        assert_int_equal(pread(fd, b, (size_t)leaves[i].len, (off_t)leaves[i].off),
                         (ssize_t)leaves[i].len);
        int all_zero = 1;
        for (uint64_t k = 0; k < leaves[i].len && all_zero; k++) all_zero = b[k] == 0;
        if (all_zero) zero_covered += leaves[i].len;
        if (!seen) {
            distinct++;
            if (all_zero) zero_distinct++;
            ideal += lz4_or_raw(b, (size_t)leaves[i].len);
            raw += leaves[i].len;
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(leaves[i].hash, hex);
            db_result_t *ps = NULL;
            assert_int_equal(db_execute(db, "SELECT comp_size FROM pack_objects WHERE hash = ?",
                                        (const char *[]){ hex, NULL }, &ps), 0);
            assert_int_equal(ps->nrows, 1);
            stored += strtoull(ps->rows[0][0], NULL, 10);
            db_result_free(ps);
        }
        free(b);
    }
    close(fd);
    printf("%s: %zu chunks, %zu distinct (%zu all-zero, covering %llu MiB of "
           "zeros); stored %llu, per-chunk lz4 %llu, raw %llu\n",
           name, n, distinct, zero_distinct,
           (unsigned long long)(zero_covered / MiB), (unsigned long long)stored,
           (unsigned long long)ideal, (unsigned long long)raw);

    /* Within 3% of compressing every chunk on its own merits. */
    assert_true(stored <= ideal + ideal * 3 / 100);
    assert_true(stored + stored * 3 / 100 >= ideal);
    /* And well under storing the written bytes as they are. */
    assert_true(stored < raw * 85 / 100);
    /* The zero run is a handful of distinct chunks, however long. */
    assert_true(zero_distinct >= 1 && zero_distinct <= 3);
    assert_true(zero_covered >= zlen / 2);

    /* Byte for byte, without writing a restored copy to disk. */
    uint8_t want[32], got[32];
    unsigned hl = 0;
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    fd = open(path, O_RDONLY);
    uint8_t *buf = malloc(MiB);
    ssize_t rd;
    while ((rd = read(fd, buf, MiB)) > 0) EVP_DigestUpdate(md, buf, (size_t)rd);
    close(fd);
    EVP_DigestFinal_ex(md, want, &hl);
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    assert_int_equal(content_stream(store, root, sha_emit, md, NULL), OK);
    EVP_DigestFinal_ex(md, got, &hl);
    EVP_MD_CTX_free(md);
    free(buf);
    assert_memory_equal(want, got, 32);
    free(leaves);
    unlink(path);
}

static void test_b124_image_shaped_file_compresses_per_chunk(void **state)
{
    (void)state;
    check_per_chunk("win95.img", B124_SHAPED);
}

/* A first-probe-decides design stores this one whole: the head fails the
 * probe, and the compressible body behind it never gets asked. */
static void test_b124_incompressible_head_does_not_decide_the_rest(void **state)
{
    (void)state;
    check_per_chunk("reverse.img", B124_REVERSE);
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
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &m, 0, &sink), OK);
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
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &m, 0, &sink), OK);
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
/* ------------------------------------------------------------------ */
/* B133: staging a killed writer left in tmp/ goes at the next open      */
/* ------------------------------------------------------------------ */

/* A staging pack as a writer killed mid-copy leaves it: fresh, so the hour
 * rule store_open applies would keep it. */
static void plant_stale(const char *repo, const char *name)
{
    char path[600];
    snprintf(path, sizeof(path), "%s/tmp/%s", repo, name);
    uint8_t *d = rand_buf(256 * 1024, 133);
    write_buf(path, d, 256 * 1024);
    free(d);
}

static int exists_in_tmp(const char *repo, const char *name)
{
    char path[600];
    snprintf(path, sizeof(path), "%s/tmp/%s", repo, name);
    struct stat sb;
    return stat(path, &sb) == 0;
}

static int count_pack_files(const char *repo)
{
    char cmd[700];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' | wc -l", repo);
    FILE *f = popen(cmd, "r");
    assert_non_null(f);
    int n = -1;
    assert_int_equal(fscanf(f, "%d", &n), 1);
    pclose(f);
    return n;
}

static void replicate_into(const char *dst)
{
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(dst, &m, &sink), OK);
    replicate_result_t rr;
    assert_int_equal(replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr), OK);
    sink.v->close(sink.ctx);
}

static void test_killed_writers_staging_is_swept_on_every_open(void **state)
{
    (void)state;
    uint8_t *d = rand_buf(3 * MiB, 1331);
    write_buf(TEST_SRC "/f.bin", d, 3 * MiB);
    free(d);
    backup(1);
    replicate_into(DST_REPO);
    int packs = count_pack_files(DST_REPO);
    assert_true(packs > 0);

    /* A rerun of the replicate. */
    plant_stale(DST_REPO, "pack-dat.Kill01");
    replicate_into(DST_REPO);
    assert_false(exists_in_tmp(DST_REPO, "pack-dat.Kill01"));
    assert_int_equal(count_pack_files(DST_REPO), packs);

    /* Registering the replica in another daemon (its own state.db). */
    cutils_db_t *db2 = NULL;
    assert_int_equal(db_open(&db2, TEST_DIR "/other.db"), 0);
    assert_int_equal(db_run_lib_migrations(db2), 0);
    assert_int_equal(db_run_compiled_migrations(db2, app_migrations), 0);
    plant_stale(DST_REPO, "pack-dat.Kill02");
    int64_t rid2 = 0;
    assert_int_equal(repo_register(db2, DST_REPO, "replica", &rid2), OK);
    assert_false(exists_in_tmp(DST_REPO, "pack-dat.Kill02"));

    /* Reindex there. */
    store_t *st2 = NULL;
    store_index_t *idx2 = NULL;
    assert_int_equal(repo_store_open(DST_REPO, NULL, &st2), OK);
    assert_int_equal(store_index_bind(st2, db2, rid2, &idx2), OK);
    plant_stale(DST_REPO, "pack-dat.Kill03");
    reindex_result_t ri;
    memset(&ri, 0, sizeof(ri));
    assert_int_equal(reindex_repo(st2, db2, rid2, NULL, &ri), OK);
    assert_false(exists_in_tmp(DST_REPO, "pack-dat.Kill03"));

    /* gc on the registered replica, with a fresh handle as a gc job has. */
    store_index_free(idx2);
    store_close(st2);
    assert_int_equal(repo_store_open(DST_REPO, NULL, &st2), OK);
    assert_int_equal(store_index_bind(st2, db2, rid2, &idx2), OK);
    plant_stale(DST_REPO, "pack-dat.Kill04");
    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(st2, db2, rid2, 0, NULL, &g), OK);
    assert_false(exists_in_tmp(DST_REPO, "pack-dat.Kill04"));
    assert_int_equal(count_pack_files(DST_REPO), packs);
    store_index_free(idx2);
    store_close(st2);
    db_close(db2);

    /* And gc on the source repository. */
    store_index_free(idx);
    store_close(store);
    assert_int_equal(repo_store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
    plant_stale(TEST_REPO, "pack-dat.Kill05");
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_false(exists_in_tmp(TEST_REPO, "pack-dat.Kill05"));
}

/*
 * A writer that is alive keeps its staging: it holds tmp/ (store_claim_tmp)
 * from the moment it opened a pack, so a sweep by another handle - a
 * replicate's open here - cannot claim it, falls back to the hour rule, and
 * leaves the young file alone. Under the daemon's locks this cannot happen
 * within one daemon (a replica is written by one job at a time, and a
 * registered repository's jobs serialise); it can from a second process.
 */
static void test_a_live_writers_staging_is_left_alone(void **state)
{
    (void)state;
    uint8_t *d = rand_buf(2 * MiB, 1332);
    write_buf(TEST_SRC "/f.bin", d, 2 * MiB);
    backup(1);
    replicate_into(DST_REPO);

    store_t *w = NULL;
    assert_int_equal(store_open(DST_REPO, NULL, &w), OK);
    pack_backup_writer_t *pw = NULL;
    assert_int_equal(pack_backup_open(w, &pw), OK);
    uint8_t h[OBJECT_HASH_SIZE];
    int is_new = 0;
    assert_int_equal(pack_backup_add(pw, OBJECT_TYPE_FILE, d, 9000, h, &is_new, NULL, NULL), OK);
    free(d);
    char cmd[700];
    snprintf(cmd, sizeof(cmd), "ls '%s/tmp' | grep -c '^pack-dat\\.'", DST_REPO);
    FILE *f = popen(cmd, "r");
    int n = 0;
    assert_int_equal(fscanf(f, "%d", &n), 1);
    pclose(f);
    assert_int_equal(n, 1);

    replicate_into(DST_REPO);
    f = popen(cmd, "r");
    assert_int_equal(fscanf(f, "%d", &n), 1);
    pclose(f);
    assert_int_equal(n, 1);

    pack_backup_abort(pw);
    store_close(w);
}

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


/* Where a copy of hash sits other than in pack `not`: its pack and index. */
typedef struct {
    const uint8_t *hash;
    uint32_t       not_pack, cur, found_pack, found_index;
    int            found;
} copy_find_t;

static void copy_find_entry(const pack_entry_info_t *info, void *ctx)
{
    copy_find_t *f = ctx;
    if (f->cur == f->not_pack || memcmp(info->hash, f->hash, OBJECT_HASH_SIZE) != 0) return;
    f->found = 1;
    f->found_pack = f->cur;
    f->found_index = info->entry_index;
}

static void copy_find_pack(uint32_t num, void *ctx)
{
    copy_find_t *f = ctx;
    char name[64];
    snprintf(name, sizeof(name), "pack-%010u.dat", num);
    f->cur = num;
    (void)pack_enumerate_dat(store, name, NULL, NULL, copy_find_entry, f);
}

/*
 * The same repoint with the good copy deep in its pack rather than its
 * first entry: proving it reads its own CRCs, at its own position. The
 * walk once handed gc every entry as position 0, so a copy anywhere else
 * was never proven, the row stayed on the damaged copy until a rewrite
 * happened to carry the good one, and the move was neither made nor
 * counted (B138).
 */
static void test_gc_repoints_to_a_copy_deep_in_its_pack(void **state)
{
    (void)state;
    for (int i = 0; i < 6; i++) {
        char p[600];
        snprintf(p, sizeof(p), TEST_SRC "/a%d.bin", i);
        uint8_t *d = rand_buf(40 * 1024, 900 + (uint64_t)i);
        write_buf(p, d, 40 * 1024);
        free(d);
    }
    uint8_t hash[OBJECT_HASH_SIZE];
    uint32_t pnum = 0;
    uint64_t off = 0;
    plant_duplicate(hash, &pnum, &off);
    copy_find_t f = { hash, pnum, 0, 0, 0, 0 };
    assert_int_equal(pack_list(store, copy_find_pack, &f), OK);
    assert_true(f.found);
    assert_true(f.found_index > 0);
    ruin(pnum, off + sizeof(pack_dat_entry_hdr_t) + 1024, 64 * 1024);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.index_relocated, 1);
    uint32_t p2 = 0;
    uint64_t o2 = 0;
    assert_int_equal(pack_resolve_location(store, hash, &p2, &o2), OK);
    assert_true(p2 != pnum);
    restore_and_diff(latest_snapshot());
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_workers_do_not_change_the_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_workers_do_not_store_twins_twice, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_reclaims_a_duplicate_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_moves_the_index_off_a_ruined_header, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_moves_the_index_off_a_damaged_payload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_repoints_to_a_copy_deep_in_its_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_small_files_on_workers_match_serial, setup, teardown),
        cmocka_unit_test_setup_teardown(test_append_and_edit_write_little, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_keeps_chunks_of_live_lists, setup, teardown),
        cmocka_unit_test_setup_teardown(test_b124_image_shaped_file_compresses_per_chunk, setup, teardown),
        cmocka_unit_test_setup_teardown(test_b124_incompressible_head_does_not_decide_the_rest, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_backup_leaves_garbage_gc_reclaims, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_restores_chunk_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_huge_directory, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicate_across_keys_rechunks, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicate_same_key_copies, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_writers_staging_is_swept_on_every_open, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_live_writers_staging_is_left_alone, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
