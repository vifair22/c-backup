/*
 * Manifest-tree and DB-sizing measurements.
 *
 * Two questions the daemonization plan currently answers by reasoning rather
 * than by measurement:
 *
 *   1. Does a per-directory content-addressed manifest tree actually dedup
 *      across snapshots, and by how much?  The design assumes an unchanged
 *      directory serialises byte-identically and therefore costs nothing on
 *      the next backup.  If serialisation is not deterministic, dedup silently
 *      degrades to a full rewrite per snapshot and nothing fails loudly.
 *
 *   2. How large does the derived node table actually get?  Rows scale as
 *      snapshots x files regardless of tree sharing, because the DB is kept
 *      flat and denormalised so browsing stays an index seek.
 *
 * Usage:
 *   build/bench_manifest [nfiles] [nsnaps] [nchanged]
 *
 * Defaults: 50000 files, 6 snapshots, 50 files changed between snapshots.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <sqlite3.h>

#include "backup.h"
#include "repo.h"
#include "snapshot.h"

#define BENCH_REPO "/tmp/c_backup_manifest_repo"
#define BENCH_SRC  "/tmp/c_backup_manifest_src"
#define BENCH_DB   "/tmp/c_backup_manifest.db"
#define HASH_MIX   0x9E3779B97F4A7C15ULL
#define HASHLEN    32

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void rmrf(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    int rc = system(cmd); (void)rc;
}

static int64_t file_size_of(const char *path)
{
    struct stat st;
    return stat(path, &st) == 0 ? (int64_t)st.st_size : -1;
}

static int64_t dir_size_of(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "du -sb '%s' 2>/dev/null | cut -f1", path);
    FILE *f = popen(cmd, "r");
    if (!f) return -1;
    long long v = -1;
    if (fscanf(f, "%lld", &v) != 1) v = -1;
    pclose(f);
    return (int64_t)v;
}

static uint64_t g_seed;

static void create_file(const char *path, size_t size)
{
    FILE *f = fopen(path, "wb");
    if (!f) return;
    uint64_t s = 0xdeadbeefcafebabeULL ^ (uint64_t)size ^ (++g_seed * 2654435761ULL);
    uint8_t buf[4096];
    size_t rem = size;
    while (rem > 0) {
        size_t chunk = rem < sizeof(buf) ? rem : sizeof(buf);
        for (size_t i = 0; i < chunk; i++) {
            if ((i & 7) == 0) { s ^= s << 13; s ^= s >> 7; s ^= s << 17; }
            buf[i] = (uint8_t)(s >> 32);
        }
        fwrite(buf, 1, chunk, f);
        rem -= chunk;
    }
    fclose(f);
}

/* Realistic-ish shape: nested directories rather than one flat level, so the
 * "changed file touches its ancestors" cost is actually exercised. */
static void create_tree(const char *root, int nfiles, size_t fsz)
{
    char path[1024];
    rmrf(root);
    mkdir(root, 0755);
    int per_leaf = 20;
    int leaves = (nfiles + per_leaf - 1) / per_leaf;
    if (leaves < 1) leaves = 1;
    int mid = (leaves + 31) / 32;
    if (mid < 1) mid = 1;

    for (int m = 0; m < mid; m++) {
        snprintf(path, sizeof(path), "%s/m%03d", root, m);
        mkdir(path, 0755);
    }
    for (int l = 0; l < leaves; l++) {
        snprintf(path, sizeof(path), "%s/m%03d/l%04d", root, l % mid, l);
        mkdir(path, 0755);
    }
    for (int i = 0; i < nfiles; i++) {
        int l = i / per_leaf;
        snprintf(path, sizeof(path), "%s/m%03d/l%04d/f%06d.dat", root, l % mid, l, i);
        create_file(path, fsz);
    }
}

static void touch_files(const char *root, int nfiles, int nchanged, int gen, size_t fsz)
{
    char path[1024];
    int per_leaf = 20;
    for (int i = 0; i < nchanged; i++) {
        int idx = (gen * 7919 + i * 104729) % nfiles;
        int l = idx / per_leaf;
        int leaves = (nfiles + per_leaf - 1) / per_leaf;
        int mid = (leaves + 31) / 32;
        if (mid < 1) mid = 1;
        snprintf(path, sizeof(path), "%s/m%03d/l%04d/f%06d.dat", root, l % mid, l, idx);
        create_file(path, fsz);
    }
}

/* ------------------------------------------------------------------ */
/* node map + child index over a loaded snapshot                       */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t       *keys;
    const node_t  **vals;
    int            *occ;
    size_t          cap;
} nodemap_t;

static void nodemap_build(nodemap_t *m, const snapshot_t *snap)
{
    size_t cap = 32, need = snap->node_count < 16 ? 32 : (size_t)snap->node_count * 2;
    while (cap < need) cap <<= 1;
    m->keys = calloc(cap, sizeof(uint64_t));
    m->vals = calloc(cap, sizeof(const node_t *));
    m->occ  = calloc(cap, sizeof(int));
    m->cap  = cap;
    size_t mask = cap - 1;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint64_t nid = snap->nodes[i].node_id;
        size_t sl = (size_t)(nid * HASH_MIX >> 32) & mask;
        while (m->occ[sl] && m->keys[sl] != nid) sl = (sl + 1) & mask;
        m->keys[sl] = nid; m->vals[sl] = &snap->nodes[i]; m->occ[sl] = 1;
    }
}
static void nodemap_free(nodemap_t *m)
{
    free(m->keys); free(m->vals); free(m->occ);
    memset(m, 0, sizeof(*m));
}
static const node_t *nodemap_get(const nodemap_t *m, uint64_t nid)
{
    size_t mask = m->cap - 1, sl = (size_t)(nid * HASH_MIX >> 32) & mask;
    while (m->occ[sl]) {
        if (m->keys[sl] == nid) return m->vals[sl];
        sl = (sl + 1) & mask;
    }
    return NULL;
}

typedef struct {
    uint64_t    parent;
    uint64_t    node_id;
    const char *name;
    uint16_t    name_len;
} child_t;

static int child_cmp(const void *a, const void *b)
{
    const child_t *x = a, *y = b;
    if (x->parent != y->parent) return x->parent < y->parent ? -1 : 1;
    size_t n = x->name_len < y->name_len ? x->name_len : y->name_len;
    int c = memcmp(x->name, y->name, n);
    if (c) return c;
    return (int)x->name_len - (int)y->name_len;
}

/* Flatten dirent_data into an array sorted by (parent, name). */
static child_t *build_children(const snapshot_t *snap, size_t *out_n)
{
    size_t cap = snap->dirent_count ? snap->dirent_count : 1, n = 0;
    child_t *arr = malloc(cap * sizeof(child_t));
    const uint8_t *p = snap->dirent_data, *end = p + snap->dirent_data_len;
    while (p + sizeof(dirent_rec_t) <= end && n < cap) {
        const dirent_rec_t *rec = (const dirent_rec_t *)p;
        p += sizeof(dirent_rec_t);
        if (p + rec->name_len > end) break;
        arr[n].parent = rec->parent_node;
        arr[n].node_id = rec->node_id;
        arr[n].name = (const char *)p;
        arr[n].name_len = rec->name_len;
        n++;
        p += rec->name_len;
    }
    qsort(arr, n, sizeof(child_t), child_cmp);
    *out_n = n;
    return arr;
}

/* ------------------------------------------------------------------ */
/* Deterministic tree serialisation                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t *buf;
    size_t   len, cap;
} sbuf_t;

static void sb_put(sbuf_t *b, const void *data, size_t n)
{
    if (b->len + n > b->cap) {
        b->cap = (b->len + n) * 2 + 256;
        b->buf = realloc(b->buf, b->cap);
    }
    memcpy(b->buf + b->len, data, n);
    b->len += n;
}
static void sb_u16(sbuf_t *b, uint16_t v) { sb_put(b, &v, 2); }
static void sb_u32(sbuf_t *b, uint32_t v) { sb_put(b, &v, 4); }
static void sb_u64(sbuf_t *b, uint64_t v) { sb_put(b, &v, 8); }

static void sha256(const void *data, size_t len, uint8_t out[HASHLEN])
{
    unsigned int n = 0;
    EVP_Digest(data, len, out, &n, EVP_sha256(), NULL);
}

/* Collected tree objects for one snapshot. */
typedef struct {
    uint8_t (*hashes)[HASHLEN];
    size_t  *sizes;
    size_t   n, cap;
} treeset_t;

static void ts_add(treeset_t *t, const uint8_t h[HASHLEN], size_t sz)
{
    if (t->n == t->cap) {
        t->cap = t->cap ? t->cap * 2 : 256;
        t->hashes = realloc(t->hashes, t->cap * HASHLEN);
        t->sizes  = realloc(t->sizes, t->cap * sizeof(size_t));
    }
    memcpy(t->hashes[t->n], h, HASHLEN);
    t->sizes[t->n] = sz;
    t->n++;
}
static void ts_free(treeset_t *t) { free(t->hashes); free(t->sizes); memset(t, 0, sizeof(*t)); }

/*
 * Serialise one directory and recurse depth-first.  Entries are already in
 * (parent, name) order from the qsort, so no sorting happens here.
 *
 * Deliberately absent from the serialisation: node_id, parent_node, and any
 * backup-run state.  Those are per-snapshot values; including them would give
 * every directory a fresh hash on every backup and destroy dedup entirely.
 * Children are referenced by content hash only.
 */
static void build_tree(uint64_t dir_id, const child_t *kids, size_t nkids,
                       const nodemap_t *nm, treeset_t *out, uint8_t hash_out[HASHLEN])
{
    /* binary search for the first child of dir_id */
    size_t lo = 0, hi = nkids;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (kids[mid].parent < dir_id) lo = mid + 1; else hi = mid;
    }

    sbuf_t sb = {0};
    for (size_t i = lo; i < nkids && kids[i].parent == dir_id; i++) {
        const node_t *nd = nodemap_get(nm, kids[i].node_id);
        uint8_t child_hash[HASHLEN];
        memset(child_hash, 0, HASHLEN);

        if (nd && nd->type == NODE_TYPE_DIR) {
            build_tree(kids[i].node_id, kids, nkids, nm, out, child_hash);
        } else if (nd) {
            memcpy(child_hash, nd->content_hash, HASHLEN);
        }

        sb_u16(&sb, kids[i].name_len);
        sb_u32(&sb, nd ? (uint32_t)nd->type : 0);
        sb_u32(&sb, nd ? nd->mode : 0);
        sb_u32(&sb, nd ? nd->uid : 0);
        sb_u32(&sb, nd ? nd->gid : 0);
        sb_u64(&sb, nd ? nd->size : 0);
        sb_u64(&sb, nd ? nd->mtime_sec : 0);
        sb_u64(&sb, nd ? nd->mtime_nsec : 0);
        sb_u64(&sb, nd ? nd->inode_identity : 0);
        sb_put(&sb, child_hash, HASHLEN);
        sb_put(&sb, nd ? nd->xattr_hash : child_hash, HASHLEN);
        sb_put(&sb, nd ? nd->acl_hash : child_hash, HASHLEN);
        sb_put(&sb, kids[i].name, kids[i].name_len);
    }

    sha256(sb.buf ? sb.buf : (const uint8_t *)"", sb.len, hash_out);
    ts_add(out, hash_out, sb.len);
    free(sb.buf);
}

/* Hash set for dedup accounting across snapshots. */
typedef struct { uint8_t (*h)[HASHLEN]; int *occ; size_t cap, n; } hset_t;

static void hs_init(hset_t *s, size_t need)
{
    size_t cap = 1024;
    while (cap < need * 2) cap <<= 1;
    s->h = calloc(cap, HASHLEN);
    s->occ = calloc(cap, sizeof(int));
    s->cap = cap; s->n = 0;
}
static int hs_add(hset_t *s, const uint8_t hash[HASHLEN])
{
    uint64_t k;
    memcpy(&k, hash, sizeof(k));
    size_t mask = s->cap - 1, sl = (size_t)(k * HASH_MIX >> 32) & mask;
    while (s->occ[sl]) {
        if (memcmp(s->h[sl], hash, HASHLEN) == 0) return 0;   /* already present */
        sl = (sl + 1) & mask;
    }
    memcpy(s->h[sl], hash, HASHLEN);
    s->occ[sl] = 1; s->n++;
    return 1;   /* newly inserted */
}
static void hs_free(hset_t *s) { free(s->h); free(s->occ); memset(s, 0, sizeof(*s)); }

int main(int argc, char **argv)
{
    int nfiles   = argc > 1 ? atoi(argv[1]) : 50000;
    int nsnaps   = argc > 2 ? atoi(argv[2]) : 6;
    int nchanged = argc > 3 ? atoi(argv[3]) : 50;
    size_t fsz = 128;

    printf("manifest trees + DB sizing\n");
    printf("  files=%d snapshots=%d changed-between-snapshots=%d\n\n",
           nfiles, nsnaps, nchanged);

    rmrf(BENCH_REPO); rmrf(BENCH_SRC); rmrf(BENCH_DB);
    unlink(BENCH_DB "-wal"); unlink(BENCH_DB "-shm");

    create_tree(BENCH_SRC, nfiles, fsz);
    repo_init(BENCH_REPO);
    repo_t *repo = NULL;
    if (repo_open(BENCH_REPO, &repo) != OK) { fprintf(stderr, "repo_open failed\n"); return 1; }
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    for (int s = 0; s < nsnaps; s++) {
        backup_run_opts(repo, paths, 1, &opts);
        if (s < nsnaps - 1) touch_files(BENCH_SRC, nfiles, nchanged, s + 1, fsz);
    }

    /* ---------------- part 1: manifest trees ---------------- */
    printf("  == manifest trees ==\n");
    printf("  %-6s %10s %10s %10s %12s %12s %10s\n",
           "snap", "dirs", "new", "reused", "new bytes", "snap bytes", "build ms");
    printf("  ---------------------------------------------------------------------------------\n");

    hset_t seen;
    hs_init(&seen, (size_t)nfiles / 4 + 4096);

    uint8_t first_root[HASHLEN];
    int have_first = 0;
    int64_t total_new_bytes = 0, total_snap_bytes = 0;

    for (int s = 1; s <= nsnaps; s++) {
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, (uint32_t)s, &snap) != OK || !snap) continue;

        nodemap_t nm;
        nodemap_build(&nm, snap);
        size_t nkids = 0;
        child_t *kids = build_children(snap, &nkids);

        treeset_t ts = {0};
        uint8_t root[HASHLEN];
        double t0 = now_sec();
        build_tree(0, kids, nkids, &nm, &ts, root);
        double build_ms = (now_sec() - t0) * 1000.0;

        size_t new_trees = 0, new_bytes = 0;
        for (size_t i = 0; i < ts.n; i++) {
            if (hs_add(&seen, ts.hashes[i])) { new_trees++; new_bytes += ts.sizes[i]; }
        }

        char sp[512];
        snprintf(sp, sizeof(sp), "%s/snapshots", BENCH_REPO);
        int64_t snapdir = dir_size_of(sp);
        int64_t this_snap_bytes = s == 1 ? snapdir : 0;
        (void)this_snap_bytes;

        printf("  %-6d %10zu %10zu %10zu %12zu %12s %10.1f\n",
               s, ts.n, new_trees, ts.n - new_trees, new_bytes, "-", build_ms);

        total_new_bytes += (int64_t)new_bytes;

        if (!have_first) { memcpy(first_root, root, HASHLEN); have_first = 1; }

        /* determinism: rebuild the same snapshot and compare the root hash */
        if (s == 1) {
            treeset_t ts2 = {0};
            uint8_t root2[HASHLEN];
            build_tree(0, kids, nkids, &nm, &ts2, root2);
            printf("  determinism (snap 1 rebuilt): %s\n",
                   memcmp(root, root2, HASHLEN) == 0 ? "identical root hash OK"
                                                     : "ROOT HASH DIFFERS - dedup would be broken");
            ts_free(&ts2);
        }

        ts_free(&ts);
        free(kids);
        nodemap_free(&nm);
        snapshot_free(snap);
    }

    {
        char sp[512];
        snprintf(sp, sizeof(sp), "%s/snapshots", BENCH_REPO);
        total_snap_bytes = dir_size_of(sp);
    }
    printf("\n  manifest write volume over %d snapshots:\n", nsnaps);
    printf("    tree objects (unique, uncompressed) : %10.2f MiB\n",
           (double)total_new_bytes / 1048576.0);
    printf("    .snap files as they exist today     : %10.2f MiB\n",
           (double)total_snap_bytes / 1048576.0);
    if (total_new_bytes > 0)
        printf("    ratio                               : %10.2fx\n",
               (double)total_snap_bytes / (double)total_new_bytes);

    /* ---------------- part 2: DB sizing ---------------- */
    printf("\n  == derived node table sizing ==\n");

    sqlite3 *db = NULL;
    sqlite3_open(BENCH_DB, &db);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    sqlite3_exec(db,
        "CREATE TABLE nodes (repo_id INTEGER NOT NULL, id INTEGER NOT NULL,"
        " snap_id INTEGER NOT NULL, parent_node_id INTEGER, name TEXT NOT NULL,"
        " mode INTEGER NOT NULL, uid INTEGER NOT NULL, gid INTEGER NOT NULL,"
        " mtime_ns INTEGER NOT NULL, size INTEGER NOT NULL, content_hash BLOB,"
        " xattr_ref INTEGER, acl_ref INTEGER, sparse_ref INTEGER,"
        " hardlink_group_id INTEGER, PRIMARY KEY (repo_id, id))", NULL, NULL, NULL);

    sqlite3_stmt *ins = NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO nodes (repo_id,id,snap_id,parent_node_id,name,mode,uid,gid,"
        "mtime_ns,size,content_hash,xattr_ref,acl_ref,sparse_ref,hardlink_group_id)"
        " VALUES (1,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL)", -1, &ins, NULL);

    int64_t gid_counter = 1, rows = 0;
    double insert_total = 0.0;
    for (int s = 1; s <= nsnaps; s++) {
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, (uint32_t)s, &snap) != OK || !snap) continue;
        nodemap_t nm; nodemap_build(&nm, snap);
        size_t nkids = 0;
        child_t *kids = build_children(snap, &nkids);

        double t0 = now_sec();
        sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
        for (size_t i = 0; i < nkids; i++) {
            const node_t *nd = nodemap_get(&nm, kids[i].node_id);
            sqlite3_reset(ins);
            sqlite3_bind_int64(ins, 1, gid_counter++);
            sqlite3_bind_int(ins, 2, s);
            sqlite3_bind_int64(ins, 3, (int64_t)kids[i].parent);
            sqlite3_bind_text(ins, 4, kids[i].name, kids[i].name_len, SQLITE_STATIC);
            sqlite3_bind_int(ins, 5, nd ? (int)nd->mode : 0);
            sqlite3_bind_int(ins, 6, nd ? (int)nd->uid : 0);
            sqlite3_bind_int(ins, 7, nd ? (int)nd->gid : 0);
            sqlite3_bind_int64(ins, 8, nd ? (int64_t)nd->mtime_sec * 1000000000 : 0);
            sqlite3_bind_int64(ins, 9, nd ? (int64_t)nd->size : 0);
            if (nd) sqlite3_bind_blob(ins, 10, nd->content_hash, HASHLEN, SQLITE_STATIC);
            else sqlite3_bind_null(ins, 10);
            sqlite3_step(ins);
            rows++;
        }
        sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
        insert_total += now_sec() - t0;

        free(kids); nodemap_free(&nm); snapshot_free(snap);
    }
    sqlite3_finalize(ins);
    sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL);
    int64_t sz_bare = file_size_of(BENCH_DB);

    double t0 = now_sec();
    sqlite3_exec(db, "CREATE INDEX nodes_by_snap_parent"
                     " ON nodes (repo_id, snap_id, parent_node_id, name)", NULL, NULL, NULL);
    sqlite3_exec(db, "CREATE INDEX nodes_by_hash ON nodes (repo_id, content_hash)",
                 NULL, NULL, NULL);
    double idx_s = now_sec() - t0;
    sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL);
    int64_t sz_idx = file_size_of(BENCH_DB);

    t0 = now_sec();
    sqlite3_exec(db, "CREATE VIRTUAL TABLE node_fts USING fts5(name, node_id UNINDEXED,"
                     " snap_id UNINDEXED, repo_id UNINDEXED, tokenize='trigram')",
                 NULL, NULL, NULL);
    sqlite3_exec(db, "INSERT INTO node_fts(name,node_id,snap_id,repo_id)"
                     " SELECT name,id,snap_id,repo_id FROM nodes", NULL, NULL, NULL);
    double fts_s = now_sec() - t0;
    sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL);
    int64_t sz_fts = file_size_of(BENCH_DB);

    int64_t repo_total = dir_size_of(BENCH_REPO);

    printf("    node rows                    : %12" PRId64 "\n", rows);
    printf("    bytes/row (table only)       : %12.1f\n",
           rows ? (double)sz_bare / (double)rows : 0.0);
    printf("    db, table only               : %9.2f MiB\n", (double)sz_bare / 1048576.0);
    printf("    db, + both indexes           : %9.2f MiB   (build %.2fs)\n",
           (double)sz_idx / 1048576.0, idx_s);
    printf("    db, + FTS5 trigram           : %9.2f MiB   (build %.2fs)\n",
           (double)sz_fts / 1048576.0, fts_s);
    printf("    bytes/row (fully indexed)    : %12.1f\n",
           rows ? (double)sz_fts / (double)rows : 0.0);
    printf("    insert time, all snapshots   : %9.2f s     (%.0f rows/s)\n",
           insert_total, insert_total > 0 ? (double)rows / insert_total : 0.0);
    printf("    repo on disk (content+meta)  : %9.2f MiB\n", (double)repo_total / 1048576.0);

    printf("\n  extrapolation at these bytes/row:\n");
    double bpr = rows ? (double)sz_fts / (double)rows : 0.0;
    long long cases[][2] = { {1000000, 30}, {1000000, 50}, {5000000, 30} };
    for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        double r = (double)cases[i][0] * (double)cases[i][1];
        printf("    %lldM files x %lld snapshots = %6.0fM rows -> %8.1f GiB\n",
               cases[i][0] / 1000000, cases[i][1], r / 1e6,
               r * bpr / (1024.0*1024.0*1024.0));
    }

    hs_free(&seen);
    sqlite3_close(db);
    repo_close(repo);
    rmrf(BENCH_REPO); rmrf(BENCH_SRC); rmrf(BENCH_DB);
    unlink(BENCH_DB "-wal"); unlink(BENCH_DB "-shm");
    return 0;
}
