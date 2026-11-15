/*
 * Interactive browse timings on the daemon's node index.
 *
 * Measures what a file browser costs the daemon while a user navigates a
 * snapshot: listing one directory level a page at a time, resolving a path
 * name by name, switching snapshots, and filename search. All of it is
 * answered from the nodes table and the node_fts trigram index, and none of
 * it reads the repository.
 *
 * The queries are the routes' own SQL, copied from src/api/routes/snapshots.c
 * (list_tree, subpath_exists, search_nodes) and run through c-utils'
 * db_execute exactly as the routes run them. What is left out is the JSON
 * serialisation and the HTTP hop, and the ancestor walk search_nodes does to
 * turn each hit into a path. If those queries change, change them here too:
 * a benchmark of SQL the daemon no longer issues measures nothing.
 *
 * The snapshot is a real one: a generated tree backed up with backup_run,
 * then backed up again unchanged so several snapshots are resident, the way
 * a repository with history looks. The tree separates "how big is the
 * snapshot" from "how many children does this directory have":
 *
 *   src/bulk/<0000..>/file_*.dat   --files files, FANOUT to a directory
 *   src/small/probe_*.dat          exactly SMALL_ENTRIES files
 *   src/wide/w_*.dat               --wide files in one directory
 *   src/deep/d0/.../d23/leaf.dat   DEEP_DEPTH levels
 *
 * Usage:
 *   bench_browse [options] [filter...]
 *     -d, --dir DIR     parent of the working directory    (BENCH_DIR, /tmp)
 *     -n, --files N     files under bulk/                  (BENCH_BROWSE_FILES, 100000)
 *     -w, --wide N      children of wide/                  (BENCH_BROWSE_WIDE, 10000)
 *     -S, --snaps N     resident snapshots                 (BENCH_BROWSE_SNAPS, 4)
 *     -k, --keep        leave the working directory behind
 *     -l, --list        list the benchmarks and exit
 *   A filter runs only benchmarks whose name contains it.
 */

#define _GNU_SOURCE
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>

#include "bench_common.h"

#include <cutils/log.h>

#include "backup.h"

#define FANOUT        40
#define SMALL_ENTRIES 40
#define DEEP_DEPTH    24
#define PAGE          200   /* PAGE_DEFAULT in snapshots.c */
#define SEARCH_SCAN_FACTOR 8
#define MIN_SECS      0.3
#define MIN_ITERS     20

typedef struct {
    char     parent[3072];
    char     work[3200];
    char     src[3300];
    uint64_t files;
    uint64_t wide;
    uint64_t snaps;
    int      keep;
} opts_t;

static opts_t      g_o;
static bench_env_t g_e;
static int64_t     g_snap_first, g_snap_last;
static uint64_t    g_sink;

/* ---------- the routes' queries ---------- */

/* list_tree in snapshots.c. Returns the row count, or -1. */
static int q_list(cutils_db_t *db, int64_t snap, int64_t parent, int limit, int offset)
{
    char snap_s[32], par_s[32], lim[16], off[16];
    snprintf(snap_s, sizeof(snap_s), "%lld", (long long)snap);
    snprintf(par_s,  sizeof(par_s),  "%lld", (long long)parent);
    snprintf(lim,    sizeof(lim),    "%d", limit);
    snprintf(off,    sizeof(off),    "%d", offset);
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, name, type, mode, size, mtime_ns / 1000000000,"
            "       mtime_ns % 1000000000, content_hash,"
            "       EXISTS (SELECT 1 FROM nodes c"
            "                WHERE c.repo_id = n.repo_id AND c.snap_id = n.snap_id"
            "                  AND c.parent_node_id = n.id)"
            "  FROM nodes n"
            " WHERE n.repo_id = ? AND n.snap_id = ? AND n.parent_node_id = ?"
            " ORDER BY n.name LIMIT ? OFFSET ?",
            (const char *[]){ g_e.repo_id_s, snap_s, par_s, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return -1;
    }
    int n = r ? r->nrows : 0;
    if (n > 0) g_sink += (uint64_t)strlen(r->rows[n - 1][1]);
    db_result_free(r);
    return n;
}

/*
 * subpath_exists in snapshots.c, returning the node it reaches: one indexed
 * lookup per path component, from the snapshot root (parent 0) down. 0 when
 * the path is not in the snapshot.
 */
static int64_t q_resolve(int64_t snap, const char *path)
{
    char snap_s[32], parent[32] = "0";
    snprintf(snap_s, sizeof(snap_s), "%lld", (long long)snap);
    const char *p = path;
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != '/') p++;
        char name[NAME_MAX + 1];
        size_t n = (size_t)(p - s);
        if (n > NAME_MAX) return 0;
        memcpy(name, s, n);
        name[n] = '\0';

        db_result_t *r = NULL;
        if (db_execute(g_e.db,
                "SELECT id FROM nodes"
                " WHERE repo_id = ? AND snap_id = ? AND parent_node_id = ?"
                "   AND name = ?",
                (const char *[]){ g_e.repo_id_s, snap_s, parent, name, NULL }, &r) != 0 ||
            !r || r->nrows < 1) {
            db_result_free(r);
            return 0;
        }
        snprintf(parent, sizeof(parent), "%s", r->rows[0][0]);
        db_result_free(r);
    }
    return strtoll(parent, NULL, 10);
}

/* search_nodes in snapshots.c: the query, quoted as fts_quote does, with its
 * scan limit. snap 0 searches every snapshot. Returns rows read, or -1. */
static int q_search(int64_t snap, const char *needle)
{
    char mq[256], lim[16], snap_s[32];
    snprintf(mq, sizeof(mq), "\"%s\"", needle);
    snprintf(lim, sizeof(lim), "%d", snap ? PAGE : PAGE * SEARCH_SCAN_FACTOR);
    snprintf(snap_s, sizeof(snap_s), "%lld", (long long)snap);
    const char *repo = g_e.repo_id_s;
    /* snapshot_in_older_half, as the route decides it. */
    int oldest_first = 0;
    if (snap) {
        db_result_t *h = NULL;
        if (db_execute(g_e.db,
                "SELECT (SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id > ?)"
                "     > (SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id < ?)",
                (const char *[]){ repo, snap_s, repo, snap_s, NULL }, &h) == 0 &&
            h && h->nrows == 1)
            oldest_first = strcmp(h->rows[0][0], "1") == 0;
        db_result_free(h);
    }
    db_result_t *r = NULL;
    int rc = snap
        ? db_execute(g_e.db, oldest_first
            ? "SELECT f.node_id, f.snap_id, f.name, n.type, n.size,"
              "       COALESCE(n.content_hash, '')"
              "  FROM node_fts f"
              "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id"
              " WHERE f.repo_id = ? AND f.name MATCH ? AND n.snap_id = ?"
              " ORDER BY f.rowid ASC LIMIT ?"
            : "SELECT f.node_id, f.snap_id, f.name, n.type, n.size,"
              "       COALESCE(n.content_hash, '')"
              "  FROM node_fts f"
              "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id"
              " WHERE f.repo_id = ? AND f.name MATCH ? AND n.snap_id = ?"
              " ORDER BY f.rowid DESC LIMIT ?",
            (const char *[]){ repo, repo, mq, snap_s, lim, NULL }, &r)
        : db_execute(g_e.db,
            "SELECT f.node_id, f.snap_id, f.name, n.type, n.size,"
            "       COALESCE(n.content_hash, '')"
            "  FROM node_fts f"
            "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id"
            " WHERE f.repo_id = ? AND f.name MATCH ?"
            " ORDER BY f.rowid DESC LIMIT ?",
            (const char *[]){ repo, repo, mq, lim, NULL }, &r);
    if (rc != 0) { db_result_free(r); return -1; }
    int n = r ? r->nrows : 0;
    db_result_free(r);
    return n;
}

/* ---------- tree ---------- */

static int touch(const char *path)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "bench: create %s: %s\n", path, strerror(errno));
        return -1;
    }
    /* A little unique content, so every file is its own object as in a
     * real tree, without the generation costing more than the backup. */
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "%s", strrchr(path, '/') + 1);
    ssize_t w = write(fd, buf, (size_t)n);
    close(fd);
    return w == n ? 0 : -1;
}

static int generate(void)
{
    char path[4096];
    const char *src = g_o.src;
    if (bench_mkdir(src) != 0) return -1;

    snprintf(path, sizeof(path), "%s/bulk", src);
    if (bench_mkdir(path) != 0) return -1;
    for (uint64_t i = 0; i < g_o.files; i++) {
        if (i % FANOUT == 0) {
            snprintf(path, sizeof(path), "%s/bulk/%04" PRIu64, src, i / FANOUT);
            if (bench_mkdir(path) != 0) return -1;
        }
        snprintf(path, sizeof(path), "%s/bulk/%04" PRIu64 "/file_%07" PRIu64 ".dat",
                 src, i / FANOUT, i);
        if (touch(path) != 0) return -1;
    }

    snprintf(path, sizeof(path), "%s/small", src);
    if (bench_mkdir(path) != 0) return -1;
    for (int i = 0; i < SMALL_ENTRIES; i++) {
        snprintf(path, sizeof(path), "%s/small/probe_%03d.dat", src, i);
        if (touch(path) != 0) return -1;
    }

    snprintf(path, sizeof(path), "%s/wide", src);
    if (bench_mkdir(path) != 0) return -1;
    for (uint64_t i = 0; i < g_o.wide; i++) {
        snprintf(path, sizeof(path), "%s/wide/w_%06" PRIu64 ".dat", src, i);
        if (touch(path) != 0) return -1;
    }

    size_t off = (size_t)snprintf(path, sizeof(path), "%s/deep", src);
    if (bench_mkdir(path) != 0) return -1;
    for (int d = 0; d < DEEP_DEPTH; d++) {
        off += (size_t)snprintf(path + off, sizeof(path) - off, "/d%d", d);
        if (bench_mkdir(path) != 0) return -1;
    }
    snprintf(path + off, sizeof(path) - off, "/leaf.dat");
    return touch(path);
}

/* The repo id is always the first parameter; p1 may be NULL, which then
 * terminates the list after it. */
static int64_t scalar(const char *sql, const char *p1)
{
    db_result_t *r = NULL;
    int64_t v = -1;
    if (db_execute(g_e.db, sql, (const char *[]){ g_e.repo_id_s, p1, NULL }, &r) == 0 &&
        r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int build(void)
{
    double t0 = bench_now();
    if (generate() != 0) return -1;
    printf("  generated %" PRIu64 " files in %.2fs\n",
           g_o.files + SMALL_ENTRIES + g_o.wide + 1, bench_now() - t0);

    static const char *paths[1];
    paths[0] = g_o.src;
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.paths = paths;
    bo.n_paths = 1;

    /* backup_run's profile is bench_phases' business; here it is noise. */
    log_level_t lvl = log_get_level();
    log_set_level(LOG_WARNING);
    t0 = bench_now();
    for (uint64_t s = 0; s < g_o.snaps; s++) {
        backup_result_t res;
        if (backup_run(g_e.store, g_e.db, g_e.repo_id, &bo, &res) != OK) {
            log_set_level(lvl);
            fprintf(stderr, "bench: backup %" PRIu64 " failed: %s\n", s + 1, err_msg());
            return -1;
        }
        if (s == 0) g_snap_first = res.snapshot_id;
        g_snap_last = res.snapshot_id;
    }
    log_set_level(lvl);

    char last[32];
    snprintf(last, sizeof(last), "%lld", (long long)g_snap_last);
    int64_t rows  = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?", NULL);
    int64_t per   = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = ?", last);
    char dbp[4096];
    snprintf(dbp, sizeof(dbp), "%s/state.db", g_o.work);
    struct stat sb;
    double db_mib = stat(dbp, &sb) == 0 ? (double)sb.st_size / (1024.0 * 1024.0) : 0.0;
    printf("  %" PRIu64 " snapshot(s) in %.2fs: %lld node rows, %lld per snapshot, "
           "state.db %.1f MiB (excluding WAL)\n",
           g_o.snaps, bench_now() - t0, (long long)rows, (long long)per, db_mib);
    return 0;
}

/* ---------- harness ---------- */

typedef struct {
    const char *name;
    void (*fn)(void);
} bench_t;

static void report_us(const char *name, double usec, double elapsed)
{
    printf("  %-44s %12.3f %-12s (%.2fs)\n", name, usec, "us/op", elapsed);
}

/* Run op until both floors are met; returns microseconds per call. */
#define TIME_OP(name, op)                                                    \
    do {                                                                     \
        uint64_t iters_ = 0;                                                 \
        double t0_ = bench_now(), el_;                                       \
        do { op; iters_++; el_ = bench_now() - t0_; }                        \
        while (el_ < MIN_SECS || iters_ < MIN_ITERS);                        \
        report_us(name, el_ / (double)iters_ * 1e6, el_);                    \
    } while (0)

static char g_path_small[4096], g_path_wide[4096], g_path_deep[4096], g_path_bulk[4096];
static int64_t g_small_id, g_wide_id;

static int locate(void)
{
    snprintf(g_path_small, sizeof(g_path_small), "%s/small", g_o.src);
    snprintf(g_path_wide,  sizeof(g_path_wide),  "%s/wide",  g_o.src);
    size_t off = (size_t)snprintf(g_path_deep, sizeof(g_path_deep), "%s/deep", g_o.src);
    for (int d = 0; d < DEEP_DEPTH; d++)
        off += (size_t)snprintf(g_path_deep + off, sizeof(g_path_deep) - off, "/d%d", d);
    snprintf(g_path_deep + off, sizeof(g_path_deep) - off, "/leaf.dat");
    uint64_t mid = g_o.files / 2;
    snprintf(g_path_bulk, sizeof(g_path_bulk),
             "%s/bulk/%04" PRIu64 "/file_%07" PRIu64 ".dat", g_o.src, mid / FANOUT, mid);

    g_small_id = q_resolve(g_snap_last, g_path_small);
    g_wide_id  = q_resolve(g_snap_last, g_path_wide);
    if (!g_small_id || !g_wide_id || !q_resolve(g_snap_last, g_path_deep) ||
        (g_o.files && !q_resolve(g_snap_last, g_path_bulk))) {
        fprintf(stderr, "bench: generated paths not found in the snapshot\n");
        return -1;
    }
    return 0;
}

static int depth_of(const char *p)
{
    int d = 0;
    for (; *p; p++) if (*p == '/') d++;
    return d;
}

static void b_cold_open(void)
{
    char dbp[4096];
    snprintf(dbp, sizeof(dbp), "%s/state.db", g_o.work);
    int n = 0;
    TIME_OP("cold_open_first_listing", {
        cutils_db_t *db = NULL;
        if (db_open(&db, dbp) == 0) { n = q_list(db, g_snap_last, 0, PAGE, 0); db_close(db); }
    });
    printf("      (new connection, then the snapshot root; OS page cache warm; %d rows)\n", n);
}

static void b_list_root(void)
{
    int n = 0;
    TIME_OP("list_snapshot_root", n = q_list(g_e.db, g_snap_last, 0, PAGE, 0));
    printf("      (%d rows)\n", n);
}

static void b_expand_small(void)
{
    int n = 0;
    TIME_OP("expand_40child_dir", n = q_list(g_e.db, g_snap_last, g_small_id, PAGE, 0));
    printf("      (%d children returned)\n", n);
}

static void b_expand_wide_first(void)
{
    int n = 0;
    TIME_OP("expand_wide_first_page", n = q_list(g_e.db, g_snap_last, g_wide_id, PAGE, 0));
    printf("      (%d of %" PRIu64 " children)\n", n, g_o.wide);
}

static void b_expand_wide_last(void)
{
    int off = g_o.wide > PAGE ? (int)(g_o.wide - PAGE) : 0;
    int n = 0;
    TIME_OP("expand_wide_last_page", n = q_list(g_e.db, g_snap_last, g_wide_id, PAGE, off));
    printf("      (%d children at offset %d: OFFSET walks the index past every row before it)\n",
           n, off);
}

static void b_expand_wide_all(void)
{
    uint64_t total = 0;
    TIME_OP("expand_wide_all_pages", {
        total = 0;
        for (int off = 0;; off += PAGE) {
            int n = q_list(g_e.db, g_snap_last, g_wide_id, PAGE, off);
            if (n <= 0) break;
            total += (uint64_t)n;
            if (n < PAGE) break;
        }
    });
    printf("      (%" PRIu64 " children in pages of %d, per full walk)\n", total, PAGE);
}

static void b_resolve_deep(void)
{
    int64_t id = 0;
    TIME_OP("resolve_path_deep", id = q_resolve(g_snap_last, g_path_deep));
    printf("      (%d components, node %lld)\n", depth_of(g_path_deep), (long long)id);
}

static void b_resolve_bulk(void)
{
    int64_t id = 0;
    TIME_OP("resolve_path_bulk_file", id = q_resolve(g_snap_last, g_path_bulk));
    printf("      (%d components, node %lld)\n", depth_of(g_path_bulk), (long long)id);
}

static void b_switch(void)
{
    uint64_t span = (uint64_t)(g_snap_last - g_snap_first + 1);
    uint64_t k = 0;
    int n = 0;
    TIME_OP("snapshot_switch(resolve+list)", {
        int64_t snap = g_snap_first + (int64_t)(k++ % span);
        int64_t id = q_resolve(snap, g_path_small);
        n = q_list(g_e.db, snap, id, PAGE, 0);
    });
    printf("      (rotating over %" PRIu64 " snapshots, %d children)\n", span, n);
}

static void b_search(const char *name, int64_t snap, const char *needle)
{
    int n = 0;
    TIME_OP(name, n = q_search(snap, needle));
    printf("      (\"%s\", %d rows read)\n", needle, n);
}

static void b_search_rare_one(void)   { b_search("search_one_snap_rare",   g_snap_last, "probe_017"); }
static void b_search_common_one(void) { b_search("search_one_snap_common", g_snap_last, "file_"); }
/* The same query against the oldest snapshot. The snapshot filter is on the
 * joined nodes row, not in the index, so FTS hands back matches from every
 * snapshot in rowid order and the newest is reached last. */
static void b_search_common_old(void) { b_search("search_one_snap_common_oldest", g_snap_first, "file_"); }
static void b_search_rare_all(void)   { b_search("search_all_snaps_rare",  0, "probe_017"); }
static void b_search_common_all(void) { b_search("search_all_snaps_common", 0, "file_"); }

static const bench_t g_benches[] = {
    { "cold_open_first_listing",       b_cold_open },
    { "list_snapshot_root",            b_list_root },
    { "expand_40child_dir",            b_expand_small },
    { "expand_wide_first_page",        b_expand_wide_first },
    { "expand_wide_last_page",         b_expand_wide_last },
    { "expand_wide_all_pages",         b_expand_wide_all },
    { "resolve_path_deep",             b_resolve_deep },
    { "resolve_path_bulk_file",        b_resolve_bulk },
    { "snapshot_switch(resolve+list)", b_switch },
    { "search_one_snap_rare",          b_search_rare_one },
    { "search_one_snap_common",        b_search_common_one },
    { "search_one_snap_common_oldest", b_search_common_old },
    { "search_all_snaps_rare",         b_search_rare_all },
    { "search_all_snaps_common",       b_search_common_all },
};
#define N_BENCHES (sizeof(g_benches) / sizeof(g_benches[0]))

static void usage(FILE *f)
{
    fprintf(f,
        "usage: bench_browse [-d DIR] [-n FILES] [-w WIDE] [-S SNAPS] [-k] [-l] [filter...]\n"
        "  -d, --dir DIR    parent of the working directory (BENCH_DIR, /tmp)\n"
        "  -n, --files N    files under bulk/ (BENCH_BROWSE_FILES, 100000)\n"
        "  -w, --wide N     children of wide/ (BENCH_BROWSE_WIDE, 10000)\n"
        "  -S, --snaps N    resident snapshots (BENCH_BROWSE_SNAPS, 4)\n"
        "  -k, --keep       keep the working directory\n"
        "  -l, --list       list the benchmarks and exit\n");
}

static int matches(const char *name, int argc, char **argv)
{
    if (optind >= argc) return 1;
    for (int i = optind; i < argc; i++)
        if (strstr(name, argv[i])) return 1;
    return 0;
}

int main(int argc, char **argv)
{
    const char *dir = getenv("BENCH_DIR");
    snprintf(g_o.parent, sizeof(g_o.parent), "%s", (dir && *dir) ? dir : "/tmp");
    g_o.files = bench_env_u64("BENCH_BROWSE_FILES", 100000);
    g_o.wide  = bench_env_u64("BENCH_BROWSE_WIDE", 10000);
    g_o.snaps = bench_env_u64("BENCH_BROWSE_SNAPS", 4);

    static const struct option lo[] = {
        { "dir",   required_argument, NULL, 'd' },
        { "files", required_argument, NULL, 'n' },
        { "wide",  required_argument, NULL, 'w' },
        { "snaps", required_argument, NULL, 'S' },
        { "keep",  no_argument,       NULL, 'k' },
        { "list",  no_argument,       NULL, 'l' },
        { "help",  no_argument,       NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:n:w:S:klh", lo, NULL)) != -1) {
        switch (c) {
        case 'd': snprintf(g_o.parent, sizeof(g_o.parent), "%s", optarg); break;
        case 'n': g_o.files = bench_parse_u64("--files", optarg); break;
        case 'w': g_o.wide  = bench_parse_u64("--wide", optarg); break;
        case 'S': g_o.snaps = bench_parse_u64("--snaps", optarg); break;
        case 'k': g_o.keep = 1; break;
        case 'l':
            for (size_t b = 0; b < N_BENCHES; b++) printf("  %s\n", g_benches[b].name);
            return 0;
        case 'h': usage(stdout); return 0;
        default:  usage(stderr); return 2;
        }
    }
    if (g_o.snaps < 1) g_o.snaps = 1;
    if (g_o.wide > INT_MAX - PAGE) {
        fprintf(stderr, "bench: --wide too large\n");
        return 2;
    }
    snprintf(g_o.work, sizeof(g_o.work), "%s/c_backup_bench_browse", g_o.parent);
    snprintf(g_o.src, sizeof(g_o.src), "%s/src", g_o.work);

    if (bench_rm_tree(g_o.work) != 0 || bench_mkdir(g_o.work) != 0) return 1;

    printf("bench_browse: %s\n", g_o.work);
    printf("  %" PRIu64 " bulk files, %d-entry probe dir, %" PRIu64 "-entry wide dir, "
           "%d-level chain, %" PRIu64 " snapshot(s)\n",
           g_o.files, SMALL_ENTRIES, g_o.wide, DEEP_DEPTH, g_o.snaps);

    int rc = 1;
    if (bench_env_open(&g_e, g_o.work) != 0) goto out;
    if (build() != 0 || locate() != 0) goto out;

    printf("  %-44s %12s %-12s\n", "NAME", "RESULT", "UNIT");
    int ran = 0;
    for (size_t b = 0; b < N_BENCHES; b++) {
        if (!matches(g_benches[b].name, argc, argv)) continue;
        g_benches[b].fn();
        ran++;
    }
    if (ran == 0) printf("  (no benchmarks matched filter)\n");
    if (g_sink == 0xFFFFFFFFFFFFFFFFull) printf("  sink\n");
    rc = 0;

out:
    bench_env_close(&g_e);
    if (g_o.keep) printf("kept %s\n", g_o.work);
    else if (bench_rm_tree(g_o.work) != 0) rc = 1;
    return rc;
}
