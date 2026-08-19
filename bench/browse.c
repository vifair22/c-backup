/*
 * Browse-path benchmarks for c-backup.
 *
 * These measure the operations a GUI performs while a user navigates a
 * snapshot, as opposed to the storage-engine hot paths covered by
 * bench_micro and bench_phases.  They exist to establish a baseline the
 * daemon rewrite has to beat before the DB-backed metadata schema is
 * locked in.
 *
 * The interesting question is how expansion cost scales.  json_api.c's
 * snap_dir_children (:598-606) answers "list the children of node P" by
 * linearly scanning every dirent record in the snapshot and comparing
 * parent_node.  The cached hash tables (:69-130) accelerate the per-child
 * node_id -> node_t* lookup, but not the search for children itself.  So
 * expanding a 40-entry directory in a 1M-file snapshot should cost the
 * same as expanding the root: O(total dirents), not O(children).
 *
 * The _same_ and _scaling_ benchmarks below are built to expose that.
 *
 * Usage:
 *   build/bench_browse              - run all benchmarks
 *   build/bench_browse expand       - run only benchmarks matching "expand"
 *   build/bench_browse -l           - list available benchmarks
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "backup.h"
#include "ls.h"
#include "repo.h"
#include "scan.h"
#include "snapshot.h"

#define BENCH_REPO "/tmp/c_backup_browse_repo"
#define BENCH_SRC  "/tmp/c_backup_browse_src"

/* Mixer used by json_api.c's cached tables; replicated so the baseline
 * measures what the GUI actually pays, not an idealised version. */
#define HASH_MIX 0x9E3779B97F4A7C15ULL

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
    int rc = system(cmd);
    (void)rc;
}

static void bench_cleanup(void)
{
    rmrf(BENCH_REPO);
    rmrf(BENCH_SRC);
}

static void report_us(const char *name, double usec, double elapsed)
{
    printf("  %-44s %12.3f %-12s (%.2fs)\n", name, usec, "us/op", elapsed);
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

/*
 * Build a tree with a shape that separates "how big is the snapshot" from
 * "how many children does this directory have":
 *
 *   <root>/bulk/<0000..>/file_*.dat   nfiles files spread over dirs of
 *                                     `fanout` entries each -> snapshot size
 *   <root>/small/                     exactly SMALL_ENTRIES files
 *   <root>/wide/                      exactly WIDE_ENTRIES files
 *   <root>/deep/d0/d1/.../d<DEPTH-1>/ one file at the bottom
 *
 * `small` is the probe directory: its child count is fixed, so any change
 * in expansion time across tree sizes is scaling with the snapshot, not
 * with the answer.
 */
#define SMALL_ENTRIES 40
#define WIDE_ENTRIES  10000
#define DEEP_DEPTH    24

static void create_browse_tree(const char *root, int nfiles, int fanout,
                               size_t file_size, int with_wide)
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
        snprintf(path, sizeof(path), "%s/bulk/%04d/file_%07d.dat",
                 root, i / fanout, i);
        create_file(path, file_size);
    }

    snprintf(path, sizeof(path), "%s/small", root);
    mkdir(path, 0755);
    for (int i = 0; i < SMALL_ENTRIES; i++) {
        snprintf(path, sizeof(path), "%s/small/probe_%03d.dat", root, i);
        create_file(path, file_size);
    }

    if (with_wide) {
        snprintf(path, sizeof(path), "%s/wide", root);
        mkdir(path, 0755);
        for (int i = 0; i < WIDE_ENTRIES; i++) {
            snprintf(path, sizeof(path), "%s/wide/w_%06d.dat", root, i);
            create_file(path, file_size);
        }
    }

    size_t off = (size_t)snprintf(path, sizeof(path), "%s/deep", root);
    mkdir(path, 0755);
    for (int d = 0; d < DEEP_DEPTH; d++) {
        off += (size_t)snprintf(path + off, sizeof(path) - off, "/d%d", d);
        mkdir(path, 0755);
    }
    snprintf(path + off, sizeof(path) - off, "/leaf.dat");
    create_file(path, file_size);
}

static repo_t *build_repo(int nfiles, int fanout, size_t file_size,
                          int with_wide, int n_snaps)
{
    bench_cleanup();
    create_browse_tree(BENCH_SRC, nfiles, fanout, file_size, with_wide);
    repo_init(BENCH_REPO);
    repo_t *repo = NULL;
    if (repo_open(BENCH_REPO, &repo) != OK) return NULL;
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    for (int s = 0; s < n_snaps; s++) {
        backup_run_opts(repo, paths, 1, &opts);
        if (s < n_snaps - 1) {
            char p[1024];
            snprintf(p, sizeof(p), "%s/small/probe_%03d.dat", BENCH_SRC, s % SMALL_ENTRIES);
            create_file(p, file_size);
        }
    }
    return repo;
}

/* ------------------------------------------------------------------ */
/* Replica of json_api.c's cached browse tables                        */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t      *nm_keys;
    const node_t **nm_vals;
    int           *nm_occ;
    size_t         nm_cap;
    uint64_t      *pset;
    int           *pset_occ;
    size_t         pset_cap;
} browse_tables_t;

static void tables_free(browse_tables_t *t)
{
    free(t->nm_keys); free(t->nm_vals); free(t->nm_occ);
    free(t->pset);    free(t->pset_occ);
    memset(t, 0, sizeof(*t));
}

static int tables_build(browse_tables_t *t, const snapshot_t *snap)
{
    memset(t, 0, sizeof(*t));

    size_t pcap = 32;
    size_t need = snap->dirent_count < 16 ? 32 : (size_t)snap->dirent_count * 2;
    while (pcap < need) pcap <<= 1;
    t->pset     = calloc(pcap, sizeof(uint64_t));
    t->pset_occ = calloc(pcap, sizeof(int));
    if (!t->pset || !t->pset_occ) { tables_free(t); return -1; }
    t->pset_cap = pcap;
    size_t pmask = pcap - 1;

    if (snap->dirent_data && snap->dirent_data_len > 0) {
        const uint8_t *p = snap->dirent_data;
        const uint8_t *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;
            uint64_t pn = rec->parent_node;
            size_t slot = (size_t)(pn * HASH_MIX >> 32) & pmask;
            while (t->pset_occ[slot] && t->pset[slot] != pn)
                slot = (slot + 1) & pmask;
            t->pset[slot] = pn;
            t->pset_occ[slot] = 1;
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
        while (t->nm_occ[slot] && t->nm_keys[slot] != nid)
            slot = (slot + 1) & nmask;
        t->nm_keys[slot] = nid;
        t->nm_vals[slot] = &snap->nodes[i];
        t->nm_occ[slot]  = 1;
    }
    return 0;
}

static const node_t *nm_lookup(const browse_tables_t *t, uint64_t nid)
{
    size_t mask = t->nm_cap - 1;
    size_t slot = (size_t)(nid * HASH_MIX >> 32) & mask;
    while (t->nm_occ[slot]) {
        if (t->nm_keys[slot] == nid) return t->nm_vals[slot];
        slot = (slot + 1) & mask;
    }
    return NULL;
}

static int pset_has(const browse_tables_t *t, uint64_t nid)
{
    size_t mask = t->pset_cap - 1;
    size_t slot = (size_t)(nid * HASH_MIX >> 32) & mask;
    while (t->pset_occ[slot]) {
        if (t->pset[slot] == nid) return 1;
        slot = (slot + 1) & mask;
    }
    return 0;
}

/*
 * The snap_dir_children inner loop, minus cJSON construction: full linear
 * scan of dirent_data for records whose parent_node matches, with an O(1)
 * node lookup and a has-children probe per hit.  Returns the child count
 * (and consumes the metadata so the compiler cannot elide the work).
 */
static uint32_t expand_dir(const snapshot_t *snap, const browse_tables_t *t,
                           uint64_t parent_node, uint64_t *sink)
{
    uint32_t n = 0;
    uint64_t acc = 0;
    if (!snap->dirent_data || snap->dirent_data_len == 0) return 0;
    const uint8_t *p = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;
    while (p + sizeof(dirent_rec_t) <= end) {
        const dirent_rec_t *rec = (const dirent_rec_t *)p;
        p += sizeof(dirent_rec_t);
        if (p + rec->name_len > end) break;
        if ((uint64_t)rec->parent_node == parent_node) {
            const node_t *nd = nm_lookup(t, rec->node_id);
            if (nd) acc += nd->size + nd->mode;
            acc += (uint64_t)pset_has(t, rec->node_id);
            acc += rec->name_len;
            n++;
        }
        p += rec->name_len;
    }
    *sink += acc;
    return n;
}

/* Resolve a repo-relative directory path to its node_id by walking dirents. */
static uint64_t find_node_by_path(const snapshot_t *snap, const char *path)
{
    uint64_t cur = 0;   /* root */
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s", path);
    char *save = NULL;
    for (char *tok = strtok_r(buf, "/", &save); tok; tok = strtok_r(NULL, "/", &save)) {
        size_t toklen = strlen(tok);
        uint64_t next = UINT64_MAX;
        const uint8_t *p = snap->dirent_data;
        const uint8_t *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;
            if ((uint64_t)rec->parent_node == cur &&
                rec->name_len == toklen &&
                memcmp(p, tok, toklen) == 0) {
                next = rec->node_id;
                p += rec->name_len;
                break;
            }
            p += rec->name_len;
        }
        if (next == UINT64_MAX) return UINT64_MAX;
        cur = next;
    }
    return cur;
}

/* ------------------------------------------------------------------ */
/* Registry                                                            */
/* ------------------------------------------------------------------ */

typedef void (*bench_fn_t)(void);
typedef struct { const char *name; bench_fn_t fn; } browse_bench_t;

static browse_bench_t g_benches[64];
static int g_nbench;

#define REGISTER(nm, func) \
    __attribute__((constructor)) static void reg_##func(void) { \
        g_benches[g_nbench].name = nm; \
        g_benches[g_nbench].fn = func; \
        g_nbench++; \
    }

/* ------------------------------------------------------------------ */
/* 1. Cold snapshot open                                               */
/* ------------------------------------------------------------------ */

static void cold_open(int nfiles, const char *label, int iters)
{
    repo_t *repo = build_repo(nfiles, 200, 128, 0, 1);
    if (!repo) { printf("  %-44s SKIP (repo build failed)\n", label); return; }

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);
    uint32_t nodes = snap ? snap->node_count : 0;
    snapshot_free(snap);

    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snap = NULL;
        snapshot_load(repo, 1, &snap);
        snapshot_free(snap);
    }
    double el = now_sec() - t0;
    report_us(label, el / iters * 1e6, el);
    printf("      (%u nodes)\n", nodes);

    repo_close(repo);
    bench_cleanup();
}

static void bench_cold_open_10k(void)  { cold_open(10000,  "cold_open_10k",  20); }
static void bench_cold_open_100k(void) { cold_open(100000, "cold_open_100k", 10); }
REGISTER("cold_open_10k",  bench_cold_open_10k);
REGISTER("cold_open_100k", bench_cold_open_100k);

/* ------------------------------------------------------------------ */
/* 2. Cold-start table build (paid on every snapshot switch)           */
/* ------------------------------------------------------------------ */

static void build_tables_bench(int nfiles, const char *label, int iters)
{
    repo_t *repo = build_repo(nfiles, 200, 128, 0, 1);
    if (!repo) { printf("  %-44s SKIP\n", label); return; }

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);

    browse_tables_t t;
    tables_build(&t, snap);
    tables_free(&t);

    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        tables_build(&t, snap);
        tables_free(&t);
    }
    double el = now_sec() - t0;
    report_us(label, el / iters * 1e6, el);

    snapshot_free(snap);
    repo_close(repo);
    bench_cleanup();
}

static void bench_build_tables_10k(void)  { build_tables_bench(10000,  "build_tables_10k",  50); }
static void bench_build_tables_100k(void) { build_tables_bench(100000, "build_tables_100k", 20); }
REGISTER("build_tables_10k",  bench_build_tables_10k);
REGISTER("build_tables_100k", bench_build_tables_100k);

/* ------------------------------------------------------------------ */
/* 3. Directory expansion - the scaling question                       */
/*                                                                     */
/* Expands /small, which always has exactly SMALL_ENTRIES children,    */
/* inside snapshots of very different sizes.  If cost tracks snapshot  */
/* size rather than child count, expansion is O(total dirents).        */
/* ------------------------------------------------------------------ */

static void expand_small(int nfiles, const char *label, int iters)
{
    repo_t *repo = build_repo(nfiles, 200, 128, 0, 1);
    if (!repo) { printf("  %-44s SKIP\n", label); return; }

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);

    browse_tables_t t;
    tables_build(&t, snap);

    char probe[1024];
    snprintf(probe, sizeof(probe), "%s/small", BENCH_SRC + 1);  /* strip leading '/' */
    uint64_t nid = find_node_by_path(snap, probe);
    if (nid == UINT64_MAX) {
        printf("  %-44s SKIP (probe dir not found)\n", label);
        tables_free(&t); snapshot_free(snap); repo_close(repo); bench_cleanup();
        return;
    }

    uint64_t sink = 0;
    uint32_t kids = expand_dir(snap, &t, nid, &sink);

    double t0 = now_sec();
    for (int i = 0; i < iters; i++)
        expand_dir(snap, &t, nid, &sink);
    double el = now_sec() - t0;

    report_us(label, el / iters * 1e6, el);
    printf("      (%u children returned, %u dirents scanned, sink=%" PRIu64 ")\n",
           kids, snap->dirent_count, sink);

    tables_free(&t);
    snapshot_free(snap);
    repo_close(repo);
    bench_cleanup();
}

static void bench_expand_small_in_10k(void)  { expand_small(10000,  "expand_40child_dir_in_10k_snap",  2000); }
static void bench_expand_small_in_100k(void) { expand_small(100000, "expand_40child_dir_in_100k_snap", 300); }
REGISTER("expand_40child_dir_in_10k_snap",  bench_expand_small_in_10k);
REGISTER("expand_40child_dir_in_100k_snap", bench_expand_small_in_100k);

/* ------------------------------------------------------------------ */
/* 4. Wide directory expansion (10k children)                          */
/* ------------------------------------------------------------------ */

static void bench_expand_wide(void)
{
    repo_t *repo = build_repo(20000, 200, 128, 1, 1);
    if (!repo) { printf("  expand_wide_10k_children SKIP\n"); return; }

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);
    browse_tables_t t;
    tables_build(&t, snap);

    char probe[1024];
    snprintf(probe, sizeof(probe), "%s/wide", BENCH_SRC + 1);
    uint64_t nid = find_node_by_path(snap, probe);
    if (nid == UINT64_MAX) {
        printf("  expand_wide_10k_children SKIP (not found)\n");
        tables_free(&t); snapshot_free(snap); repo_close(repo); bench_cleanup();
        return;
    }

    uint64_t sink = 0;
    uint32_t kids = expand_dir(snap, &t, nid, &sink);
    int iters = 200;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++)
        expand_dir(snap, &t, nid, &sink);
    double el = now_sec() - t0;

    report_us("expand_wide_10k_children", el / iters * 1e6, el);
    printf("      (%u children returned, %u dirents scanned)\n", kids, snap->dirent_count);

    tables_free(&t);
    snapshot_free(snap);
    repo_close(repo);
    bench_cleanup();
}
REGISTER("expand_wide_10k_children", bench_expand_wide);

/* ------------------------------------------------------------------ */
/* 5. Deep-path walk: expanding every level from root to depth 24      */
/*    This is what clicking down a deep tree in the GUI costs.         */
/* ------------------------------------------------------------------ */

static void bench_expand_deep_path(void)
{
    repo_t *repo = build_repo(50000, 200, 128, 0, 1);
    if (!repo) { printf("  expand_deep_path_24_levels SKIP\n"); return; }

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);
    browse_tables_t t;
    tables_build(&t, snap);

    /* Collect node_ids along the deep chain. */
    uint64_t chain[DEEP_DEPTH + 2];
    int chain_len = 0;
    char probe[1024];
    size_t off = (size_t)snprintf(probe, sizeof(probe), "%s/deep", BENCH_SRC + 1);
    chain[chain_len++] = find_node_by_path(snap, probe);
    for (int d = 0; d < DEEP_DEPTH && chain[chain_len - 1] != UINT64_MAX; d++) {
        off += (size_t)snprintf(probe + off, sizeof(probe) - off, "/d%d", d);
        chain[chain_len++] = find_node_by_path(snap, probe);
    }
    if (chain[chain_len - 1] == UINT64_MAX) {
        printf("  expand_deep_path_24_levels SKIP (chain not found)\n");
        tables_free(&t); snapshot_free(snap); repo_close(repo); bench_cleanup();
        return;
    }

    uint64_t sink = 0;
    int iters = 100;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++)
        for (int c = 0; c < chain_len; c++)
            expand_dir(snap, &t, chain[c], &sink);
    double el = now_sec() - t0;

    report_us("expand_deep_path_24_levels", el / iters * 1e6, el);
    printf("      (%d expansions per walk, %u dirents scanned each)\n",
           chain_len, snap->dirent_count);

    tables_free(&t);
    snapshot_free(snap);
    repo_close(repo);
    bench_cleanup();
}
REGISTER("expand_deep_path_24_levels", bench_expand_deep_path);

/* ------------------------------------------------------------------ */
/* 6. Snapshot switch - the single-slot cache eviction                 */
/* ------------------------------------------------------------------ */

static void bench_snapshot_switch(void)
{
    repo_t *repo = build_repo(50000, 200, 128, 0, 4);
    if (!repo) { printf("  snapshot_switch_50k SKIP\n"); return; }

    int iters = 20;
    uint64_t sink = 0;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        uint32_t sid = (uint32_t)(i % 4) + 1;
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, sid, &snap) != OK || !snap) continue;
        browse_tables_t t;
        tables_build(&t, snap);
        char probe[1024];
        snprintf(probe, sizeof(probe), "%s/small", BENCH_SRC + 1);
        uint64_t nid = find_node_by_path(snap, probe);
        if (nid != UINT64_MAX) expand_dir(snap, &t, nid, &sink);
        tables_free(&t);
        snapshot_free(snap);
    }
    double el = now_sec() - t0;

    report_us("snapshot_switch_50k(load+tables+expand)", el / iters * 1e6, el);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("snapshot_switch_50k", bench_snapshot_switch);

/* ------------------------------------------------------------------ */
/* 7. Existing path index (.pidx)                                      */
/* ------------------------------------------------------------------ */

static void bench_pidx_lookup(void)
{
    repo_t *repo = build_repo(100000, 200, 128, 0, 1);
    if (!repo) { printf("  pidx_lookup_100k SKIP\n"); return; }

    char probe[1024];
    snprintf(probe, sizeof(probe), "%s/small/probe_017.dat", BENCH_SRC + 1);

    node_t nd;
    if (snap_pidx_lookup(repo, 1, probe, &nd) != OK) {
        printf("  %-44s SKIP (pidx miss)\n", "pidx_lookup_100k");
        repo_close(repo); bench_cleanup();
        return;
    }

    int iters = 20000;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++)
        snap_pidx_lookup(repo, 1, probe, &nd);
    double el = now_sec() - t0;

    report_us("pidx_lookup_100k", el / iters * 1e6, el);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("pidx_lookup_100k", bench_pidx_lookup);

/* ------------------------------------------------------------------ */
/* 8. snapshot_ls_collect - the CLI/API listing path                   */
/* ------------------------------------------------------------------ */

static void bench_ls_collect(void)
{
    repo_t *repo = build_repo(100000, 200, 128, 0, 1);
    if (!repo) { printf("  ls_collect_small_dir_in_100k SKIP\n"); return; }

    char probe[1024];
    snprintf(probe, sizeof(probe), "%s/small", BENCH_SRC + 1);

    ls_result_t *r = NULL;
    if (snapshot_ls_collect(repo, 1, probe, 0, 0, NULL, &r) != OK) {
        printf("  %-44s SKIP (ls failed)\n", "ls_collect_small_dir_in_100k");
        repo_close(repo); bench_cleanup();
        return;
    }
    uint32_t got = r->count;
    ls_result_free(r);

    int iters = 200;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        r = NULL;
        snapshot_ls_collect(repo, 1, probe, 0, 0, NULL, &r);
        ls_result_free(r);
    }
    double el = now_sec() - t0;

    report_us("ls_collect_small_dir_in_100k", el / iters * 1e6, el);
    printf("      (%u entries returned)\n", got);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("ls_collect_small_dir_in_100k", bench_ls_collect);

/* ------------------------------------------------------------------ */
/* 9. Search                                                           */
/* ------------------------------------------------------------------ */

static void bench_search_single(void)
{
    repo_t *repo = build_repo(100000, 200, 128, 0, 1);
    if (!repo) { printf("  search_single_snap_100k SKIP\n"); return; }

    search_result_t *r = NULL;
    if (snapshot_search(repo, 1, "probe_0", 500, &r) != OK) {
        printf("  %-44s SKIP\n", "search_single_snap_100k");
        repo_close(repo); bench_cleanup();
        return;
    }
    uint32_t hits = r->total;
    search_result_free(r);

    int iters = 100;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        r = NULL;
        snapshot_search(repo, 1, "probe_0", 500, &r);
        search_result_free(r);
    }
    double el = now_sec() - t0;

    report_us("search_single_snap_100k", el / iters * 1e6, el);
    printf("      (%u matches)\n", hits);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("search_single_snap_100k", bench_search_single);

static uint32_t g_hits;
static void count_hit(uint32_t snap_id, const search_hit_t *hit, void *ctx)
{
    (void)snap_id; (void)hit; (void)ctx;
    g_hits++;
}

static void bench_search_multi(void)
{
    int nsnaps = 8;
    repo_t *repo = build_repo(50000, 200, 128, 0, nsnaps);
    if (!repo) { printf("  search_multi_8snaps_50k SKIP\n"); return; }

    uint32_t ids[8];
    for (int i = 0; i < nsnaps; i++) ids[i] = (uint32_t)i + 1;

    g_hits = 0;
    snapshot_search_multi(repo, ids, (uint32_t)nsnaps, "probe_0", 500, count_hit, NULL);
    uint32_t first = g_hits;

    int iters = 20;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        g_hits = 0;
        snapshot_search_multi(repo, ids, (uint32_t)nsnaps, "probe_0", 500, count_hit, NULL);
    }
    double el = now_sec() - t0;

    report_us("search_multi_8snaps_50k", el / iters * 1e6, el);
    printf("      (%u hits across %d snapshots)\n", first, nsnaps);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("search_multi_8snaps_50k", bench_search_multi);

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    if (argc > 1 && strcmp(argv[1], "-l") == 0) {
        for (int i = 0; i < g_nbench; i++)
            printf("%s\n", g_benches[i].name);
        return 0;
    }

    printf("c-backup browse benchmarks\n");
    printf("  %-44s %12s %-12s\n", "benchmark", "value", "unit");
    printf("  %s\n", "--------------------------------------------------------------------------");

    int ran = 0;
    for (int i = 0; i < g_nbench; i++) {
        int match = (argc == 1);
        for (int a = 1; a < argc && !match; a++)
            if (strstr(g_benches[i].name, argv[a])) match = 1;
        if (!match) continue;
        fflush(stdout);
        g_benches[i].fn();
        ran++;
    }
    if (!ran) {
        fprintf(stderr, "no benchmarks matched\n");
        return 1;
    }
    bench_cleanup();
    return 0;
}
