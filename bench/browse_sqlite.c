/*
 * Head-to-head: current .snap browse path vs a SQLite-backed node table.
 *
 * Builds one repo, measures the browse operations a GUI performs against
 * the existing in-memory representation, then loads the same snapshots into
 * SQLite using the schema from the daemonization spec (section 6.2) and
 * measures the equivalent queries.  Prints both side by side.
 *
 * The point is to answer one question before the daemon rewrite locks the
 * metadata schema: does an indexed node table actually beat the current
 * hand-tuned cache for interactive browsing, and by how much?
 *
 * Usage:
 *   build/bench_browse_sqlite [nfiles] [nsnaps]
 *
 * Defaults: 100000 files, 4 snapshots.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <sqlite3.h>

#include "backup.h"
#include "ls.h"
#include "repo.h"
#include "snapshot.h"

#define BENCH_REPO "/tmp/c_backup_sql_repo"
#define BENCH_SRC  "/tmp/c_backup_sql_src"
#define BENCH_DB   "/tmp/c_backup_sql_meta.db"

#define HASH_MIX      0x9E3779B97F4A7C15ULL
#define SMALL_ENTRIES 40
#define WIDE_ENTRIES  10000
#define DEEP_DEPTH    24

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

static uint64_t g_seed;

static void create_file(const char *path, size_t size)
{
    FILE *f = fopen(path, "wb");
    if (!f) return;
    uint64_t s = 0xdeadbeefcafebabeULL ^ (uint64_t)size ^ (++g_seed * 2654435761ULL);
    uint8_t buf[4096];
    size_t remaining = size;
    while (remaining > 0) {
        size_t chunk = remaining < sizeof(buf) ? remaining : sizeof(buf);
        for (size_t i = 0; i < chunk; i++) {
            if ((i & 7) == 0) { s ^= s << 13; s ^= s >> 7; s ^= s << 17; }
            buf[i] = (uint8_t)(s >> 32);
        }
        fwrite(buf, 1, chunk, f);
        remaining -= chunk;
    }
    fclose(f);
}

static void create_browse_tree(const char *root, int nfiles, int fanout, size_t fsz)
{
    char path[1024];
    rmrf(root);
    mkdir(root, 0755);

    snprintf(path, sizeof(path), "%s/bulk", root);
    mkdir(path, 0755);
    int ndirs = (nfiles + fanout - 1) / fanout;
    if (ndirs < 1) ndirs = 1;
    for (int d = 0; d < ndirs; d++) {
        snprintf(path, sizeof(path), "%s/bulk/%04d", root, d);
        mkdir(path, 0755);
    }
    for (int i = 0; i < nfiles; i++) {
        snprintf(path, sizeof(path), "%s/bulk/%04d/file_%07d.dat", root, i / fanout, i);
        create_file(path, fsz);
    }

    snprintf(path, sizeof(path), "%s/small", root);
    mkdir(path, 0755);
    for (int i = 0; i < SMALL_ENTRIES; i++) {
        snprintf(path, sizeof(path), "%s/small/probe_%03d.dat", root, i);
        create_file(path, fsz);
    }

    snprintf(path, sizeof(path), "%s/wide", root);
    mkdir(path, 0755);
    for (int i = 0; i < WIDE_ENTRIES; i++) {
        snprintf(path, sizeof(path), "%s/wide/w_%06d.dat", root, i);
        create_file(path, fsz);
    }

    size_t off = (size_t)snprintf(path, sizeof(path), "%s/deep", root);
    mkdir(path, 0755);
    for (int d = 0; d < DEEP_DEPTH; d++) {
        off += (size_t)snprintf(path + off, sizeof(path) - off, "/d%d", d);
        mkdir(path, 0755);
    }
    snprintf(path + off, sizeof(path) - off, "/leaf.dat");
    create_file(path, fsz);
}

/* ---------------- current-path replica (see bench/browse.c) ------------- */

typedef struct {
    uint64_t      *nm_keys;
    const node_t **nm_vals;
    int           *nm_occ;
    size_t         nm_cap;
    uint64_t      *pset;
    int           *pset_occ;
    size_t         pset_cap;
} tables_t;

static void tables_free(tables_t *t)
{
    free(t->nm_keys); free(t->nm_vals); free(t->nm_occ);
    free(t->pset); free(t->pset_occ);
    memset(t, 0, sizeof(*t));
}

static int tables_build(tables_t *t, const snapshot_t *snap)
{
    memset(t, 0, sizeof(*t));
    size_t pcap = 32, need = snap->dirent_count < 16 ? 32 : (size_t)snap->dirent_count * 2;
    while (pcap < need) pcap <<= 1;
    t->pset = calloc(pcap, sizeof(uint64_t));
    t->pset_occ = calloc(pcap, sizeof(int));
    if (!t->pset || !t->pset_occ) { tables_free(t); return -1; }
    t->pset_cap = pcap;
    size_t pmask = pcap - 1;
    if (snap->dirent_data) {
        const uint8_t *p = snap->dirent_data, *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;
            uint64_t pn = rec->parent_node;
            size_t slot = (size_t)(pn * HASH_MIX >> 32) & pmask;
            while (t->pset_occ[slot] && t->pset[slot] != pn) slot = (slot + 1) & pmask;
            t->pset[slot] = pn; t->pset_occ[slot] = 1;
            p += rec->name_len;
        }
    }
    size_t ncap = 32;
    need = snap->node_count < 16 ? 32 : (size_t)snap->node_count * 2;
    while (ncap < need) ncap <<= 1;
    t->nm_keys = calloc(ncap, sizeof(uint64_t));
    t->nm_vals = calloc(ncap, sizeof(const node_t *));
    t->nm_occ  = calloc(ncap, sizeof(int));
    if (!t->nm_keys || !t->nm_vals || !t->nm_occ) { tables_free(t); return -1; }
    t->nm_cap = ncap;
    size_t nmask = ncap - 1;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint64_t nid = snap->nodes[i].node_id;
        size_t slot = (size_t)(nid * HASH_MIX >> 32) & nmask;
        while (t->nm_occ[slot] && t->nm_keys[slot] != nid) slot = (slot + 1) & nmask;
        t->nm_keys[slot] = nid; t->nm_vals[slot] = &snap->nodes[i]; t->nm_occ[slot] = 1;
    }
    return 0;
}

static const node_t *nm_lookup(const tables_t *t, uint64_t nid)
{
    size_t mask = t->nm_cap - 1, slot = (size_t)(nid * HASH_MIX >> 32) & mask;
    while (t->nm_occ[slot]) {
        if (t->nm_keys[slot] == nid) return t->nm_vals[slot];
        slot = (slot + 1) & mask;
    }
    return NULL;
}

static int pset_has(const tables_t *t, uint64_t nid)
{
    size_t mask = t->pset_cap - 1, slot = (size_t)(nid * HASH_MIX >> 32) & mask;
    while (t->pset_occ[slot]) {
        if (t->pset[slot] == nid) return 1;
        slot = (slot + 1) & mask;
    }
    return 0;
}

static uint32_t expand_dir(const snapshot_t *snap, const tables_t *t,
                           uint64_t parent, uint64_t *sink)
{
    uint32_t n = 0; uint64_t acc = 0;
    if (!snap->dirent_data) return 0;
    const uint8_t *p = snap->dirent_data, *end = p + snap->dirent_data_len;
    while (p + sizeof(dirent_rec_t) <= end) {
        const dirent_rec_t *rec = (const dirent_rec_t *)p;
        p += sizeof(dirent_rec_t);
        if (p + rec->name_len > end) break;
        if ((uint64_t)rec->parent_node == parent) {
            const node_t *nd = nm_lookup(t, rec->node_id);
            if (nd) acc += nd->size + nd->mode;
            acc += (uint64_t)pset_has(t, rec->node_id) + rec->name_len;
            n++;
        }
        p += rec->name_len;
    }
    *sink += acc;
    return n;
}

static uint64_t find_node_by_path(const snapshot_t *snap, const char *path)
{
    uint64_t cur = 0;
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s", path);
    char *save = NULL;
    for (char *tok = strtok_r(buf, "/", &save); tok; tok = strtok_r(NULL, "/", &save)) {
        size_t tl = strlen(tok);
        uint64_t next = UINT64_MAX;
        const uint8_t *p = snap->dirent_data, *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;
            if ((uint64_t)rec->parent_node == cur && rec->name_len == tl &&
                memcmp(p, tok, tl) == 0) { next = rec->node_id; p += rec->name_len; break; }
            p += rec->name_len;
        }
        if (next == UINT64_MAX) return UINT64_MAX;
        cur = next;
    }
    return cur;
}

/* ---------------- SQLite side ------------------------------------------ */

static void db_exec(sqlite3 *db, const char *sql)
{
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "sql error: %s\n  (%s)\n", err ? err : "?", sql);
        sqlite3_free(err);
    }
}

/*
 * Schema from daemonization-spec.md section 6.2.  Note the spec declares
 * PRIMARY KEY (repo_id, id) on nodes, which only works if node ids are
 * unique per repo rather than per snapshot; ids are assigned globally at
 * load time here to match that reading.
 */
static const char *SCHEMA =
    "PRAGMA journal_mode=WAL;"
    "PRAGMA synchronous=NORMAL;"
    "CREATE TABLE nodes ("
    " repo_id INTEGER NOT NULL, id INTEGER NOT NULL, snap_id INTEGER NOT NULL,"
    " parent_node_id INTEGER, name TEXT NOT NULL, mode INTEGER NOT NULL,"
    " uid INTEGER NOT NULL, gid INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,"
    " size INTEGER NOT NULL, content_hash BLOB, xattr_ref INTEGER,"
    " acl_ref INTEGER, sparse_ref INTEGER, hardlink_group_id INTEGER,"
    " PRIMARY KEY (repo_id, id));"
    "CREATE INDEX nodes_by_snap_parent ON nodes (repo_id, snap_id, parent_node_id);"
    "CREATE INDEX nodes_by_hash ON nodes (repo_id, content_hash);";

/* Map (snap_id, per-snapshot node_id) -> global row id. */
typedef struct { uint64_t key; int64_t val; int occ; } idmap_ent_t;
typedef struct { idmap_ent_t *e; size_t cap; } idmap_t;

static void idmap_init(idmap_t *m, size_t need)
{
    size_t cap = 32;
    while (cap < need * 2) cap <<= 1;
    m->e = calloc(cap, sizeof(idmap_ent_t));
    m->cap = cap;
}
static void idmap_put(idmap_t *m, uint64_t k, int64_t v)
{
    size_t mask = m->cap - 1, s = (size_t)(k * HASH_MIX >> 32) & mask;
    while (m->e[s].occ && m->e[s].key != k) s = (s + 1) & mask;
    m->e[s].key = k; m->e[s].val = v; m->e[s].occ = 1;
}
static int64_t idmap_get(const idmap_t *m, uint64_t k)
{
    size_t mask = m->cap - 1, s = (size_t)(k * HASH_MIX >> 32) & mask;
    while (m->e[s].occ) {
        if (m->e[s].key == k) return m->e[s].val;
        s = (s + 1) & mask;
    }
    return -1;
}

/*
 * Load one snapshot's dirent tree into the nodes table.  Two passes: first
 * assign global ids to every dirent, then insert with resolved parent ids.
 */
static int64_t load_snapshot(sqlite3 *db, const snapshot_t *snap, uint32_t snap_id,
                             int64_t *next_id, idmap_t *map)
{
    int64_t inserted = 0;

    const uint8_t *p = snap->dirent_data, *end = p + snap->dirent_data_len;
    while (p + sizeof(dirent_rec_t) <= end) {
        const dirent_rec_t *rec = (const dirent_rec_t *)p;
        p += sizeof(dirent_rec_t);
        if (p + rec->name_len > end) break;
        idmap_put(map, ((uint64_t)snap_id << 48) ^ rec->node_id, (*next_id)++);
        p += rec->name_len;
    }

    tables_t t;
    tables_build(&t, snap);

    sqlite3_stmt *ins = NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO nodes (repo_id,id,snap_id,parent_node_id,name,mode,uid,gid,"
        "mtime_ns,size,content_hash,xattr_ref,acl_ref,sparse_ref,hardlink_group_id)"
        " VALUES (1,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL)", -1, &ins, NULL);

    db_exec(db, "BEGIN");
    p = snap->dirent_data; end = p + snap->dirent_data_len;
    while (p + sizeof(dirent_rec_t) <= end) {
        const dirent_rec_t *rec = (const dirent_rec_t *)p;
        p += sizeof(dirent_rec_t);
        if (p + rec->name_len > end) break;

        int64_t gid_ = idmap_get(map, ((uint64_t)snap_id << 48) ^ rec->node_id);
        int64_t pid = rec->parent_node == 0 ? 0
                    : idmap_get(map, ((uint64_t)snap_id << 48) ^ (uint64_t)rec->parent_node);
        const node_t *nd = nm_lookup(&t, rec->node_id);

        sqlite3_reset(ins);
        sqlite3_bind_int64(ins, 1, gid_);
        sqlite3_bind_int(ins, 2, (int)snap_id);
        sqlite3_bind_int64(ins, 3, pid);
        sqlite3_bind_text(ins, 4, (const char *)p, rec->name_len, SQLITE_STATIC);
        sqlite3_bind_int(ins, 5, nd ? (int)nd->mode : 0);
        sqlite3_bind_int(ins, 6, nd ? (int)nd->uid : 0);
        sqlite3_bind_int(ins, 7, nd ? (int)nd->gid : 0);
        sqlite3_bind_int64(ins, 8, nd ? (int64_t)nd->mtime_sec * 1000000000 : 0);
        sqlite3_bind_int64(ins, 9, nd ? (int64_t)nd->size : 0);
        if (nd) sqlite3_bind_blob(ins, 10, nd->content_hash, OBJECT_HASH_SIZE, SQLITE_STATIC);
        else    sqlite3_bind_null(ins, 10);
        sqlite3_step(ins);
        inserted++;
        p += rec->name_len;
    }
    db_exec(db, "COMMIT");
    sqlite3_finalize(ins);
    tables_free(&t);
    return inserted;
}

static void row(const char *label, double cur_us, double sql_us, const char *note)
{
    double ratio = (sql_us > 0.0) ? cur_us / sql_us : 0.0;
    printf("  %-34s %12.3f %12.3f  %8.1fx  %s\n", label, cur_us, sql_us, ratio,
           note ? note : "");
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

int main(int argc, char **argv)
{
    int nfiles = argc > 1 ? atoi(argv[1]) : 100000;
    int nsnaps = argc > 2 ? atoi(argv[2]) : 4;
    size_t fsz = 128;

    printf("browse: current .snap path vs SQLite node table\n");
    printf("  files=%d snapshots=%d\n\n", nfiles, nsnaps);

    rmrf(BENCH_REPO); rmrf(BENCH_SRC); rmrf(BENCH_DB);
    unlink(BENCH_DB "-wal"); unlink(BENCH_DB "-shm");

    create_browse_tree(BENCH_SRC, nfiles, 200, fsz);
    repo_init(BENCH_REPO);
    repo_t *repo = NULL;
    if (repo_open(BENCH_REPO, &repo) != OK) { fprintf(stderr, "repo_open failed\n"); return 1; }
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    for (int s = 0; s < nsnaps; s++) {
        backup_run_opts(repo, paths, 1, &opts);
        if (s < nsnaps - 1) {
            char pth[1024];
            snprintf(pth, sizeof(pth), "%s/small/probe_%03d.dat", BENCH_SRC, s % SMALL_ENTRIES);
            create_file(pth, fsz);
        }
    }

    /* ---- load every snapshot into SQLite ---- */
    sqlite3 *db = NULL;
    sqlite3_open(BENCH_DB, &db);
    db_exec(db, SCHEMA);

    int64_t next_id = 1, total_rows = 0;
    idmap_t map;
    idmap_init(&map, (size_t)(nfiles + WIDE_ENTRIES + 4096) * (size_t)nsnaps * 2);

    double t_load0 = now_sec();
    for (int s = 1; s <= nsnaps; s++) {
        snapshot_t *sn = NULL;
        if (snapshot_load(repo, (uint32_t)s, &sn) != OK || !sn) continue;
        total_rows += load_snapshot(db, sn, (uint32_t)s, &next_id, &map);
        snapshot_free(sn);
    }
    double load_elapsed = now_sec() - t_load0;
    db_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)");

    printf("  loaded %" PRId64 " node rows in %.2fs\n", total_rows, load_elapsed);
    int64_t snapdir = dir_size_of(BENCH_REPO "/snapshots");
    int64_t dbsz = file_size_of(BENCH_DB);
    printf("  snapshots/ on disk: %8.2f MiB    sqlite db: %8.2f MiB   (%.1fx)\n\n",
           (double)snapdir / 1048576.0, (double)dbsz / 1048576.0,
           snapdir > 0 ? (double)dbsz / (double)snapdir : 0.0);

    printf("  %-34s %12s %12s  %9s\n", "operation", "current(us)", "sqlite(us)", "speedup");
    printf("  ---------------------------------------------------------------------------\n");

    /* ---- prepare the current-path side ---- */
    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);
    tables_t t;
    tables_build(&t, snap);

    char probe[1024];
    snprintf(probe, sizeof(probe), "%s/small", BENCH_SRC + 1);
    uint64_t small_nid = find_node_by_path(snap, probe);
    snprintf(probe, sizeof(probe), "%s/wide", BENCH_SRC + 1);
    uint64_t wide_nid = find_node_by_path(snap, probe);

    int64_t small_gid = idmap_get(&map, ((uint64_t)1 << 48) ^ small_nid);
    int64_t wide_gid  = idmap_get(&map, ((uint64_t)1 << 48) ^ wide_nid);

    sqlite3_stmt *q_children = NULL;
    sqlite3_prepare_v2(db,
        "SELECT id,name,mode,uid,gid,size,mtime_ns,content_hash FROM nodes"
        " WHERE repo_id=1 AND snap_id=? AND parent_node_id=?", -1, &q_children, NULL);

    uint64_t sink = 0;

    /* --- expand small directory --- */
    {
        int iters = 300;
        uint32_t kids = expand_dir(snap, &t, small_nid, &sink);
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) expand_dir(snap, &t, small_nid, &sink);
        double cur = (now_sec() - t0) / iters * 1e6;

        int rows = 0;
        int sq_iters = 20000;
        t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            sqlite3_reset(q_children);
            sqlite3_bind_int(q_children, 1, 1);
            sqlite3_bind_int64(q_children, 2, small_gid);
            rows = 0;
            while (sqlite3_step(q_children) == SQLITE_ROW) {
                sink += (uint64_t)sqlite3_column_int64(q_children, 5);
                rows++;
            }
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        char note[64];
        snprintf(note, sizeof(note), "%u vs %d rows", kids, rows);
        row("expand dir (40 children)", cur, sq, note);
    }

    /* --- expand wide directory --- */
    {
        int iters = 200;
        uint32_t kids = expand_dir(snap, &t, wide_nid, &sink);
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) expand_dir(snap, &t, wide_nid, &sink);
        double cur = (now_sec() - t0) / iters * 1e6;

        int sq_iters = 200, rows = 0;
        t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            sqlite3_reset(q_children);
            sqlite3_bind_int(q_children, 1, 1);
            sqlite3_bind_int64(q_children, 2, wide_gid);
            rows = 0;
            while (sqlite3_step(q_children) == SQLITE_ROW) {
                sink += (uint64_t)sqlite3_column_int64(q_children, 5);
                rows++;
            }
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        char note[64];
        snprintf(note, sizeof(note), "%u vs %d rows", kids, rows);
        row("expand dir (10k children)", cur, sq, note);
    }

    /* --- cold open: load snapshot + build tables, vs first query on a snap --- */
    {
        int iters = 10;
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) {
            snapshot_t *s2 = NULL;
            snapshot_load(repo, 1, &s2);
            tables_t t2;
            tables_build(&t2, s2);
            tables_free(&t2);
            snapshot_free(s2);
        }
        double cur = (now_sec() - t0) / iters * 1e6;

        /* SQLite equivalent: open the DB fresh and answer one directory query */
        int sq_iters = 200;
        t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            sqlite3 *d2 = NULL;
            sqlite3_open(BENCH_DB, &d2);
            sqlite3_stmt *q = NULL;
            sqlite3_prepare_v2(d2,
                "SELECT id,name,size FROM nodes WHERE repo_id=1 AND snap_id=1"
                " AND parent_node_id=?", -1, &q, NULL);
            sqlite3_bind_int64(q, 1, small_gid);
            while (sqlite3_step(q) == SQLITE_ROW) sink++;
            sqlite3_finalize(q);
            sqlite3_close(d2);
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        row("cold open + first listing", cur, sq, "snap load+tables vs db open+query");
    }

    /* --- snapshot switch: current pays full reload, sqlite changes a bind --- */
    {
        int iters = 10;
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) {
            uint32_t sid = (uint32_t)(i % nsnaps) + 1;
            snapshot_t *s2 = NULL;
            if (snapshot_load(repo, sid, &s2) != OK || !s2) continue;
            tables_t t2;
            tables_build(&t2, s2);
            uint64_t nid = find_node_by_path(s2, probe);
            if (nid != UINT64_MAX) expand_dir(s2, &t2, nid, &sink);
            tables_free(&t2);
            snapshot_free(s2);
        }
        double cur = (now_sec() - t0) / iters * 1e6;

        int sq_iters = 20000;
        t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            int sid = (i % nsnaps) + 1;
            sqlite3_reset(q_children);
            sqlite3_bind_int(q_children, 1, sid);
            sqlite3_bind_int64(q_children, 2, small_gid);
            while (sqlite3_step(q_children) == SQLITE_ROW) sink++;
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        row("switch snapshot + list dir", cur, sq, "single-slot evict vs rebind");
    }

    /* --- substring search across all snapshots --- */
    {
        int iters = 10;
        double t0 = now_sec();
        for (int i = 0; i < iters; i++) {
            search_result_t *r = NULL;
            snapshot_search(repo, 1, "probe_0", 500, &r);
            if (r) { sink += r->count; search_result_free(r); }
        }
        double cur = (now_sec() - t0) / iters * 1e6;

        sqlite3_stmt *q = NULL;
        sqlite3_prepare_v2(db,
            "SELECT id,name FROM nodes WHERE repo_id=1 AND snap_id=1"
            " AND name LIKE '%probe_0%' LIMIT 500", -1, &q, NULL);
        int sq_iters = 50, rows = 0;
        t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            sqlite3_reset(q);
            rows = 0;
            while (sqlite3_step(q) == SQLITE_ROW) rows++;
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        sqlite3_finalize(q);
        char note[80];
        snprintf(note, sizeof(note), "%d rows, LIKE has no index", rows);
        row("search one snapshot", cur, sq, note);
    }

    /* --- paginated wide directory: what a GUI should actually do --- */
    {
        sqlite3_stmt *q = NULL;
        sqlite3_prepare_v2(db,
            "SELECT id,name,mode,size FROM nodes WHERE repo_id=1 AND snap_id=1"
            " AND parent_node_id=? ORDER BY name LIMIT 500", -1, &q, NULL);
        int sq_iters = 2000, rows = 0;
        double t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            sqlite3_reset(q);
            sqlite3_bind_int64(q, 1, wide_gid);
            rows = 0;
            while (sqlite3_step(q) == SQLITE_ROW) { sink++; rows++; }
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        sqlite3_finalize(q);

        /* The current path cannot paginate: it always scans every dirent. */
        int iters = 200;
        t0 = now_sec();
        for (int i = 0; i < iters; i++) expand_dir(snap, &t, wide_nid, &sink);
        double cur = (now_sec() - t0) / iters * 1e6;

        char note[80];
        snprintf(note, sizeof(note), "first %d of 10k (current cannot page)", rows);
        row("expand wide dir, paginated", cur, sq, note);
    }

    /* --- same paginated query, but with name folded into the index ---
     *
     * The spec's index is (repo_id, snap_id, parent_node_id).  ORDER BY name
     * cannot be served from it, so SQLite must materialise and sort every
     * child before LIMIT applies.  Extending the index to include name lets
     * the scan stop after the first page.  Every file browser lists
     * directories in name order, so this is the normal case, not an edge one.
     */
    {
        db_exec(db, "CREATE INDEX nodes_by_snap_parent_name"
                    " ON nodes (repo_id, snap_id, parent_node_id, name)");

        sqlite3_stmt *q = NULL;
        sqlite3_prepare_v2(db,
            "SELECT id,name,mode,size FROM nodes WHERE repo_id=1 AND snap_id=1"
            " AND parent_node_id=? ORDER BY name LIMIT 500", -1, &q, NULL);
        int sq_iters = 5000, rows = 0;
        double t0 = now_sec();
        for (int i = 0; i < sq_iters; i++) {
            sqlite3_reset(q);
            sqlite3_bind_int64(q, 1, wide_gid);
            rows = 0;
            while (sqlite3_step(q) == SQLITE_ROW) { sink++; rows++; }
        }
        double sq = (now_sec() - t0) / sq_iters * 1e6;
        sqlite3_finalize(q);

        int iters = 200;
        t0 = now_sec();
        for (int i = 0; i < iters; i++) expand_dir(snap, &t, wide_nid, &sink);
        double cur = (now_sec() - t0) / iters * 1e6;

        char note[96];
        snprintf(note, sizeof(note), "first %d of 10k, name in index", rows);
        row("expand wide dir, paginated+idx", cur, sq, note);
    }

    /* --- search with an FTS5 index instead of LIKE --- */
    {
        double t_idx0 = now_sec();
        db_exec(db, "CREATE VIRTUAL TABLE node_fts USING fts5("
                    "name, node_id UNINDEXED, snap_id UNINDEXED, tokenize=\'trigram\')");
        db_exec(db, "INSERT INTO node_fts(name,node_id,snap_id)"
                    " SELECT name,id,snap_id FROM nodes WHERE repo_id=1");
        double idx_build = now_sec() - t_idx0;

        sqlite3_stmt *q = NULL;
        int rc = sqlite3_prepare_v2(db,
            "SELECT node_id,name FROM node_fts WHERE name MATCH \'probe_0\'"
            " AND snap_id=1 LIMIT 500", -1, &q, NULL);
        if (rc != SQLITE_OK) {
            printf("  %-34s FTS5 unavailable: %s\n", "search via FTS5", sqlite3_errmsg(db));
        } else {
            int sq_iters = 2000, rows = 0;
            double t0 = now_sec();
            for (int i = 0; i < sq_iters; i++) {
                sqlite3_reset(q);
                rows = 0;
                while (sqlite3_step(q) == SQLITE_ROW) { sink++; rows++; }
            }
            double sq = (now_sec() - t0) / sq_iters * 1e6;
            sqlite3_finalize(q);

            int iters = 10;
            t0 = now_sec();
            for (int i = 0; i < iters; i++) {
                search_result_t *r = NULL;
                snapshot_search(repo, 1, "probe_0", 500, &r);
                if (r) { sink += r->count; search_result_free(r); }
            }
            double cur = (now_sec() - t0) / iters * 1e6;

            char note[96];
            snprintf(note, sizeof(note), "%d rows, trigram idx built in %.2fs", rows, idx_build);
            row("search one snapshot (FTS5)", cur, sq, note);
        }
        int64_t withfts = file_size_of(BENCH_DB);
        printf("  %-34s db grows to %.2f MiB with the trigram index\n", "",
               (double)withfts / 1048576.0);
    }

    sqlite3_finalize(q_children);
    tables_free(&t);
    snapshot_free(snap);

    printf("\n  (sink=%" PRIu64 ")\n", sink);

    repo_close(repo);
    sqlite3_close(db);
    rmrf(BENCH_REPO); rmrf(BENCH_SRC); rmrf(BENCH_DB);
    unlink(BENCH_DB "-wal"); unlink(BENCH_DB "-shm");
    return 0;
}
