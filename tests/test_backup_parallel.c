#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

/*
 * Large files stored on a pool of workers (B086).
 *
 * The property everything else rests on: the repository does not depend on
 * how many workers stored it, or in what order they finished. The same
 * source backed up serially and on several workers has to give the same
 * root tree, the same counters, and the same bytes under objects/ and
 * packs/ - including when two large files are identical, which is the case
 * where two workers race to install one object.
 *
 * Then the ways a run ends early: a file that fails to store is skipped
 * with a warning exactly as it is serially, and a cancel stops the workers
 * within a read, leaves no staging files, and does not leave the thread's
 * phase timer aimed at a dead stack frame (OWN-12).
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <limits.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <openssl/evp.h>

#include "backup.h"
#include "object.h"
#include "parity.h"
#include "phase_timer.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "store_pool.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_parallel"
#define TEST_SRC  TEST_DIR "/src"
/* Repositories live apart from the source, so creating one does not touch
 * a directory the next backup scans. */
#define TEST_ENVS TEST_TMP "/c_backup_test_parallel_envs"
#define MIB (1u << 20)

extern const db_migration_t app_migrations[];

typedef struct {
    char           dir[PATH_MAX];
    cutils_db_t   *db;
    store_t       *store;
    store_index_t *idx;
    int64_t        repo_id;
} env_t;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void env_open(env_t *e, const char *name)
{
    memset(e, 0, sizeof(*e));
    snprintf(e->dir, sizeof(e->dir), TEST_ENVS "/%s", name);
    assert_int_equal(mkdir(e->dir, 0700), 0);
    char dbp[PATH_MAX + 16], repo[PATH_MAX + 16];
    snprintf(dbp, sizeof(dbp), "%s/state.db", e->dir);
    snprintf(repo, sizeof(repo), "%s/repo", e->dir);
    assert_int_equal(db_open(&e->db, dbp), 0);
    assert_int_equal(db_run_lib_migrations(e->db), 0);
    assert_int_equal(db_run_compiled_migrations(e->db, app_migrations), 0);
    assert_int_equal(repo_register(e->db, repo, "t", &e->repo_id), OK);
    assert_int_equal(store_open(repo, NULL, &e->store), OK);
    assert_int_equal(store_index_bind(e->store, e->db, e->repo_id, &e->idx), OK);
}

static void env_close(env_t *e)
{
    store_index_free(e->idx);
    store_close(e->store);
    db_close(e->db);
    memset(e, 0, sizeof(*e));
}

static uint64_t rng_state;
static uint64_t xorshift(void)
{
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

/* Incompressible content from a seed, so two calls with one seed are twins. */
static void write_random(const char *path, unsigned mib, uint64_t seed)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    uint64_t *buf = malloc(MIB);
    assert_non_null(buf);
    rng_state = seed * 0x9E3779B97F4A7C15ULL + 1;
    for (unsigned i = 0; i < mib; i++) {
        for (size_t k = 0; k < MIB / sizeof(*buf); k++) buf[k] = xorshift();
        assert_int_equal(write(fd, buf, MIB), (ssize_t)MIB);
    }
    free(buf);
    close(fd);
}

static void write_text(const char *path, const char *s)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(s, f);
    fclose(f);
}

/* A large file with holes, which stores through the sparse stream. */
static void write_sparse(const char *path, unsigned mib)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)mib * MIB), 0);
    char *buf = malloc(MIB);
    assert_non_null(buf);
    memset(buf, 'S', MIB);
    assert_int_equal(pwrite(fd, buf, MIB, 2 * (off_t)MIB), (ssize_t)MIB);
    assert_int_equal(pwrite(fd, buf, MIB, (off_t)(mib - 3) * MIB), (ssize_t)MIB);
    free(buf);
    close(fd);
}

/*
 * Small files for the pack, three distinct large files, two identical
 * ones and three more copies of them, and a sparse one: ten candidates for
 * the pool, five of which race for one object.
 */
static void make_source(void)
{
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    assert_int_equal(mkdir(TEST_SRC "/sub", 0700), 0);
    char p[PATH_MAX];
    for (int i = 0; i < 40; i++) {
        char body[64];
        snprintf(p, sizeof(p), TEST_SRC "/sub/small%02d.txt", i);
        snprintf(body, sizeof(body), "small file %d\n", i);
        write_text(p, body);
    }
    write_random(TEST_SRC "/a.bin", 20, 1);
    write_random(TEST_SRC "/b.bin", 24, 2);
    write_random(TEST_SRC "/sub/c.bin", 18, 3);
    write_random(TEST_SRC "/twin1.bin", 20, 7);
    write_random(TEST_SRC "/sub/twin2.bin", 20, 7);
    /* Three more copies side by side, which the pool picks up together and
     * finishes at nearly the same moment, so more than one of them finds
     * the object absent and installs it. */
    assert_int_equal(mkdir(TEST_SRC "/twins", 0700), 0);
    for (int i = 0; i < 3; i++) {
        snprintf(p, sizeof(p), TEST_SRC "/twins/copy%d.bin", i);
        write_random(p, 20, 7);
    }
    write_sparse(TEST_SRC "/sparse.img", 32);
    write_random(TEST_SRC "/d.bin", 17, 4);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    rmrf(TEST_ENVS);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_ENVS, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    rmrf(TEST_ENVS);
    return 0;
}

static backup_opts_t opts_for(unsigned workers)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    return o;
}

/*
 * One snapshot, as "path|columns" lines sorted by path: every node row the
 * backup indexed, with its full path rebuilt from the parent chain.
 */
#define NODE_COLS 16
typedef struct {
    char  **line;      /* path|type|mode|...|subtree, one per node */
    char  **path;
    char  **mtime;
    char  **nsize;     /* node size, compared apart like mtime */
    char  **subtree;
    int     n;
    char    root[80];
    char    files[32], size[32];
} snap_rows_t;

static void snap_rows_load(env_t *e, int64_t snap, snap_rows_t *out)
{
    memset(out, 0, sizeof(*out));
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)snap);

    db_result_t *s = NULL;
    assert_int_equal(db_execute(e->db, "SELECT hex(root_tree_hash), file_count,"
                                " total_size FROM snapshots WHERE id = ?",
                                (const char *[]){ id, NULL }, &s), 0);
    assert_int_equal(s->nrows, 1);
    snprintf(out->root, sizeof(out->root), "%s", s->rows[0][0]);
    snprintf(out->files, sizeof(out->files), "%s", s->rows[0][1]);
    snprintf(out->size, sizeof(out->size), "%s", s->rows[0][2]);
    db_result_free(s);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(e->db,
        "SELECT id, parent_node_id, name, type, mode, uid, gid, mtime_ns, size,"
        "       hex(content_hash), hex(subtree_hash), link_target,"
        "       hardlink_group_id, hex(xattr_hash), hex(acl_hash), dev_major,"
        "       dev_minor"
        "  FROM nodes WHERE snap_id = ?",
        (const char *[]){ id, NULL }, &r), 0);
    int n = r->nrows;
    out->n = n;
    out->line    = calloc((size_t)n, sizeof(char *));
    out->path    = calloc((size_t)n, sizeof(char *));
    out->mtime   = calloc((size_t)n, sizeof(char *));
    out->nsize   = calloc((size_t)n, sizeof(char *));
    out->subtree = calloc((size_t)n, sizeof(char *));
    for (int i = 0; i < n; i++) {
        /* Walk to the root; trees here are a handful of levels deep. */
        char path[PATH_MAX] = "";
        int cur = i, depth = 0;
        while (cur >= 0 && depth++ < 64) {
            char tmp[PATH_MAX];
            snprintf(tmp, sizeof(tmp), "/%s%s", r->rows[cur][2], path);
            snprintf(path, sizeof(path), "%s", tmp);
            const char *parent = r->rows[cur][1];
            cur = -1;
            for (int k = 0; k < n; k++)
                if (strcmp(r->rows[k][0], parent) == 0) { cur = k; break; }
        }
        char line[PATH_MAX + 1024];
        int off = snprintf(line, sizeof(line), "%s", path);
        /* Every column but the ids, which are the database's own. mtime,
         * size and subtree are compared separately, since an ancestor
         * outside the test's control may differ in exactly those: other
         * processes add and remove entries in /tmp while the test runs,
         * which moves its directory size as well as its mtime. */
        for (int c = 3; c < 17; c++) {
            if (c == 7 || c == 8 || c == 10) continue;
            off += snprintf(line + off, sizeof(line) - (size_t)off, "|%s", r->rows[i][c]);
        }
        out->line[i]    = strdup(line);
        out->path[i]    = strdup(path);
        out->mtime[i]   = strdup(r->rows[i][7]);
        out->nsize[i]   = strdup(r->rows[i][8]);
        out->subtree[i] = strdup(r->rows[i][10]);
    }
    db_result_free(r);
}

static void snap_rows_free(snap_rows_t *s)
{
    for (int i = 0; i < s->n; i++) {
        free(s->line[i]); free(s->path[i]); free(s->mtime[i]); free(s->subtree[i]);
        free(s->nsize[i]);
    }
    free(s->line); free(s->path); free(s->mtime); free(s->subtree); free(s->nsize);
}

static int find_path(const snap_rows_t *s, const char *path)
{
    for (int i = 0; i < s->n; i++)
        if (strcmp(s->path[i], path) == 0) return i;
    return -1;
}

/* A proper ancestor of `of` (both absolute, no trailing slash). */
static int is_ancestor(const char *path, const char *of)
{
    size_t n = strlen(path);
    return n < strlen(of) && strncmp(path, of, n) == 0 && of[n] == '/';
}

/*
 * Two snapshots of one source, compared whole: every node, the snapshot's
 * counts, and the root tree.
 *
 * scan_tree records the source's ancestors too (CLAUDE.md), so the snapshot
 * holds rows for /tmp and the test directory, and /tmp's mtime moves whenever
 * any other process creates a file there. Those ancestors of `owned` - the
 * deepest directory this test controls - are the only rows allowed to differ,
 * and only in mtime and in the tree hashes that mtime feeds: the subtree of
 * each ancestor and the snapshot's root. When their mtimes happen to agree,
 * those hashes must agree as well, so the comparison is exact whenever
 * nothing outside the test moved.
 */
static void assert_same_snapshot(env_t *a, int64_t sa, env_t *b, int64_t sb,
                                 const char *owned)
{
    snap_rows_t x, y;
    snap_rows_load(a, sa, &x);
    snap_rows_load(b, sb, &y);
    assert_int_equal(x.n, y.n);
    assert_string_equal(x.files, y.files);
    assert_string_equal(x.size, y.size);

    int outside_moved = 0;
    for (int i = 0; i < x.n; i++) {
        int j = find_path(&y, x.path[i]);
        if (j < 0) fail_msg("node %s missing from the second snapshot", x.path[i]);
        assert_string_equal(x.line[i], y.line[j]);
        if (is_ancestor(x.path[i], owned)) {
            if (strcmp(x.mtime[i], y.mtime[j]) != 0 ||
                strcmp(x.nsize[i], y.nsize[j]) != 0) outside_moved = 1;
        } else {
            assert_string_equal(x.mtime[i], y.mtime[j]);
            assert_string_equal(x.nsize[i], y.nsize[j]);
            assert_string_equal(x.subtree[i], y.subtree[j]);
        }
    }
    if (!outside_moved) {
        for (int i = 0; i < x.n; i++)
            if (is_ancestor(x.path[i], owned))
                assert_string_equal(x.subtree[i], y.subtree[find_path(&y, x.path[i])]);
        assert_string_equal(x.root, y.root);
    } else {
        print_message("note: an ancestor outside the test changed between the "
                      "runs; its mtime and the root tree were not compared\n");
    }
    snap_rows_free(&x);
    snap_rows_free(&y);
}

/*
 * Every object row of every type, as "hash|type|phys|orig" lines. The
 * location is left out: it names a pack by number, and which numbers a
 * repository hands out is not part of what a backup means.
 */
static char *object_rows(env_t *e, const char *skip1, const char *skip2)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(e->db, "SELECT hex(hash), type, phys_size,"
                                " orig_size FROM objects WHERE type != 5 ORDER BY hash",
                                NULL, &r), 0);
    size_t cap = (size_t)r->nrows * 200 + 1, off = 0;
    char *out = malloc(cap);
    out[0] = '\0';
    for (int i = 0; i < r->nrows; i++) {
        if ((skip1 && strcmp(r->rows[i][0], skip1) == 0) ||
            (skip2 && strcmp(r->rows[i][0], skip2) == 0))
            continue;
        off += (size_t)snprintf(out + off, cap - off, "%s|%s|%s|%s\n",
                                r->rows[i][0], r->rows[i][1],
                                r->rows[i][2], r->rows[i][3]);
    }
    db_result_free(r);
    return out;
}

static int dir_entries(const char *path)
{
    DIR *d = opendir(path);
    if (!d) return -1;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        /* tmp/.lock is the writers' claim on tmp/ (store_claim_tmp), not
         * staging: it stays. */
        if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0 &&
            strcmp(de->d_name, ".lock") != 0)
            n++;
    closedir(d);
    return n;
}

/* ------------------------------------------------------------------ */

static void test_plan(void **state)
{
    (void)state;
    unsigned w;
    size_t cap;

    store_pool_plan(1, 100, &w, &cap);
    assert_int_equal(w, 1);
    store_pool_plan(3, 100, &w, &cap);
    assert_int_equal(w, 3);
    /* More workers than files would only idle. */
    store_pool_plan(8, 2, &w, &cap);
    assert_int_equal(w, 2);
    store_pool_plan(0, 1, &w, &cap);
    assert_int_equal(w, 1);
    store_pool_plan(0, 0, &w, &cap);
    assert_int_equal(w, 1);
    /* The ceiling the config key's range matches. */
    store_pool_plan(1000, 1000, &w, &cap);
    assert_int_equal(w, 64);

    /* Automatic: two thirds of the CPUs, within the memory budget. */
    store_pool_plan(0, 1000, &w, &cap);
    unsigned want = store_pool_cpus() * 2 / 3;
    if (want < 1) want = 1;
    uint64_t by_mem = store_pool_mem_budget() / (40u * MIB);
    if (want > by_mem) want = (unsigned)by_mem;
    if (want > 64) want = 64;
    assert_int_equal(w, want);
    assert_true(cap >= 16u * MIB && cap <= 256u * MIB);
    /* No worker gets more than its share of the budget. */
    assert_true((uint64_t)w * (cap + 8u * MIB) <= store_pool_mem_budget() ||
                cap == 16u * MIB);
}

static void put_file(const char *path, const char *body)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s", path);
    for (char *s = dir + 1; *s; s++) {
        if (*s != '/') continue;
        *s = '\0';
        mkdir(dir, 0700);
        *s = '/';
    }
    write_text(path, body);
}

/*
 * The budget reads the cgroup's memory.max, not only physical RAM, and the
 * tightest limit between the process's cgroup and the root wins. Fixture
 * files stand in for /proc/self/cgroup and /sys/fs/cgroup.
 */
static void test_memory_budget_reads_cgroup(void **state)
{
    (void)state;
    const uint64_t G = 1024ull * MIB;
    const char *root = TEST_DIR "/cg";
    const char *proc = TEST_DIR "/proc_cgroup";

    /* One memory.max file. */
    put_file(TEST_DIR "/mm/a", "max\n");
    assert_int_equal(store_pool_read_memory_max(TEST_DIR "/mm/a"), 0);
    put_file(TEST_DIR "/mm/b", "1073741824\n");
    assert_int_equal(store_pool_read_memory_max(TEST_DIR "/mm/b"), G);
    put_file(TEST_DIR "/mm/c", "12x\n");
    assert_int_equal(store_pool_read_memory_max(TEST_DIR "/mm/c"), 0);
    put_file(TEST_DIR "/mm/d", "");
    assert_int_equal(store_pool_read_memory_max(TEST_DIR "/mm/d"), 0);
    assert_int_equal(store_pool_read_memory_max(TEST_DIR "/mm/none"), 0);

    /* A scope that says max, under a slice that does not: the slice's. A v1
     * line ahead of the unified one is ignored. */
    put_file(proc, "12:memory:/ignored\n0::/user.slice/app.slice/x.scope\n");
    put_file(TEST_DIR "/cg/user.slice/app.slice/x.scope/memory.max", "max\n");
    put_file(TEST_DIR "/cg/user.slice/app.slice/memory.max", "2147483648\n");
    put_file(TEST_DIR "/cg/user.slice/memory.max", "4294967296\n");
    assert_int_equal(store_pool_cgroup_limit(proc, root), 2 * G);

    /* A tighter limit deeper down wins over the slice's. */
    put_file(TEST_DIR "/cg/user.slice/app.slice/x.scope/memory.max", "536870912\n");
    assert_int_equal(store_pool_cgroup_limit(proc, root), G / 2);

    /* A container: the namespace root, read at the mount root. */
    put_file(proc, "0::/\n");
    put_file(TEST_DIR "/cg/memory.max", "805306368\n");
    assert_int_equal(store_pool_cgroup_limit(proc, root), 768 * (uint64_t)MIB);

    /* No limit anywhere, cgroup v1 only, or nothing to read. */
    put_file(TEST_DIR "/cg/memory.max", "max\n");
    assert_int_equal(store_pool_cgroup_limit(proc, root), 0);
    put_file(proc, "12:memory:/docker/abc\n");
    assert_int_equal(store_pool_cgroup_limit(proc, root), 0);
    assert_int_equal(store_pool_cgroup_limit(TEST_DIR "/nope", root), 0);

    /* And what the budget makes of it. */
    assert_int_equal(store_pool_budget_for(64 * G, 0), 2048 * (uint64_t)MIB);
    assert_int_equal(store_pool_budget_for(16 * G, 0), 2048 * (uint64_t)MIB);
    assert_int_equal(store_pool_budget_for(8 * G, 0), 1024 * (uint64_t)MIB);
    assert_int_equal(store_pool_budget_for(64 * G, 4 * G), 512 * (uint64_t)MIB);
    /* A container's limit is the figure even on a large host... */
    assert_int_equal(store_pool_budget_for(64 * G, G), 256 * (uint64_t)MIB);
    /* ...and the 256 MiB floor never takes more than a quarter of it. */
    assert_int_equal(store_pool_budget_for(64 * G, G / 2), 128 * (uint64_t)MIB);
    /* A limit above RAM changes nothing. */
    assert_int_equal(store_pool_budget_for(4 * G, 64 * G), 512 * (uint64_t)MIB);
    /* RAM unknown: planned as 2 GiB. */
    assert_int_equal(store_pool_budget_for(0, 0), 256 * (uint64_t)MIB);
}

/* The heart of it: one worker or four, the repository comes out the same. */
static void test_same_repository_on_one_or_many_workers(void **state)
{
    (void)state;
    make_source();

    env_t a, b;
    env_open(&a, "serial");
    backup_opts_t oa = opts_for(1);
    backup_result_t ra;
    assert_int_equal(backup_run(a.store, a.db, a.repo_id, &oa, &ra), OK);

    env_open(&b, "pooled");
    backup_opts_t ob = opts_for(4);
    backup_result_t rb;
    assert_int_equal(backup_run(b.store, b.db, b.repo_id, &ob, &rb), OK);

    assert_int_equal(ra.store_workers, 1);
    assert_int_equal(rb.store_workers, 4);

    /* The whole snapshot, not only the source directory's tree. */
    assert_same_snapshot(&a, ra.snapshot_id, &b, rb.snapshot_id, TEST_DIR);

    /* And every content object the index knows of, chunks, lists, small
     * files and metadata. Trees are left to assert_same_snapshot, which
     * compares the source subtree: the trees above it hold the scratch
     * directory's own entries, whose times move between the two runs (more
     * so under a sanitizer), and say nothing about the workers. */
    {
        snap_rows_t x, y;
        snap_rows_load(&a, ra.snapshot_id, &x);
        snap_rows_load(&b, rb.snapshot_id, &y);
        int same_root = strcmp(x.root, y.root) == 0;
        char *oa = object_rows(&a, same_root ? NULL : x.root, NULL);
        char *ob = object_rows(&b, same_root ? NULL : y.root, NULL);
        assert_string_equal(oa, ob);
        free(oa);
        free(ob);
        snap_rows_free(&x);
        snap_rows_free(&y);
    }

    assert_int_equal(ra.files, rb.files);
    assert_int_equal(ra.files, 50);
    assert_int_equal(ra.bytes_logical, rb.bytes_logical);
    /* The twins are one object, however the race between them went. */
    assert_int_equal(ra.objects_new, rb.objects_new);
    assert_int_equal(ra.bytes_stored, rb.bytes_stored);
    assert_int_equal(ra.trees_new, rb.trees_new);

    /* The index agrees with the counters: one row per distinct object. */
    db_result_t *r1 = NULL, *r2 = NULL;
    char q[160];
    snprintf(q, sizeof(q), "SELECT hex(hash), phys_size, orig_size, location"
             " FROM objects WHERE type = %d ORDER BY hash", OBJECT_TYPE_FILE);
    assert_int_equal(db_execute(a.db, q, NULL, &r1), 0);
    assert_int_equal(db_execute(b.db, q, NULL, &r2), 0);
    assert_true(r1->nrows >= 46);
    assert_int_equal(r1->nrows, r2->nrows);
    for (int i = 0; i < r1->nrows; i++) {
        assert_string_equal(r1->rows[i][0], r2->rows[i][0]);
        assert_string_equal(r1->rows[i][1], r2->rows[i][1]);
        assert_string_equal(r1->rows[i][2], r2->rows[i][2]);
        assert_string_equal(r1->rows[i][3], r2->rows[i][3]);
    }
    db_result_free(r1);
    db_result_free(r2);

    /* And it is the store's existence check, not luck: an incremental on
     * the pool vouches for every file from the cache and stores nothing. */
    backup_result_t rc;
    assert_int_equal(backup_run(b.store, b.db, b.repo_id, &ob, &rc), OK);
    assert_int_equal(rc.files_unchanged, rc.files);
    assert_int_equal(rc.objects_new, 0);

    env_close(&a);
    env_close(&b);
}

/*
 * Puts a directory where a large file was, as the store phase begins and
 * after the scan has recorded the file. The open succeeds, every read fails
 * with EISDIR, and the store gives up on it: a failure inside the object
 * writer, on a worker when there is a pool. The file is renamed aside rather
 * than removed, so it keeps its inode for the next round.
 */
static void swap_on_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    int *fired = ctx;
    if (*fired || strcmp(phase, "store") != 0 || done != 0) return;
    *fired = 1;
    assert_int_equal(rename(TEST_SRC "/b.bin", TEST_DIR "/b.keep"), 0);
    assert_int_equal(mkdir(TEST_SRC "/b.bin", 0700), 0);
}

static void test_failing_file_is_skipped_as_it_is_serially(void **state)
{
    (void)state;
    make_source();
    struct stat sb;
    assert_int_equal(stat(TEST_SRC, &sb), 0);
    const struct timespec src_times[2] = { sb.st_atim, sb.st_mtim };

    backup_result_t r[2];
    env_t e[2];
    const unsigned workers[2] = { 1, 4 };
    for (int k = 0; k < 2; k++) {
        env_open(&e[k], k ? "pooled" : "serial");
        int fired = 0;
        backup_opts_t o = opts_for(workers[k]);
        o.progress_cb  = swap_on_store;
        o.progress_ctx = &fired;
        assert_int_equal(backup_run(e[k].store, e[k].db, e[k].repo_id, &o, &r[k]), OK);
        assert_true(fired);
        char tmp[PATH_MAX + 16];
        snprintf(tmp, sizeof(tmp), "%s/repo/tmp", e[k].dir);
        assert_int_equal(dir_entries(tmp), 0);

        assert_int_equal(rmdir(TEST_SRC "/b.bin"), 0);
        assert_int_equal(rename(TEST_DIR "/b.keep", TEST_SRC "/b.bin"), 0);
        assert_int_equal(utimensat(AT_FDCWD, TEST_SRC, src_times, 0), 0);
    }
    assert_int_equal(r[1].store_workers, 4);
    /* Skipped, not fatal: one file fewer, and the same snapshot either way.
     * The swap renames into TEST_DIR, so its mtime is not the test's to
     * hold still; from TEST_SRC down it is restored. */
    assert_int_equal(r[0].files, 49);
    assert_int_equal(r[1].files, r[0].files);
    assert_int_equal(r[1].objects_new, r[0].objects_new);
    assert_int_equal(r[1].bytes_logical, r[0].bytes_logical);
    assert_same_snapshot(&e[0], r[0].snapshot_id, &e[1], r[1].snapshot_id, TEST_SRC);
    env_close(&e[0]);
    env_close(&e[1]);
}

typedef struct {
    atomic_int   *cancel;
    const char   *tmp;
    int           done;
} canceller_t;

/* Cancels as soon as a staging file appears, so the cancel lands with the
 * workers inside their files rather than between them. */
static void *canceller(void *arg)
{
    canceller_t *c = arg;
    for (int i = 0; i < 20000 && !__atomic_load_n(&c->done, __ATOMIC_ACQUIRE); i++) {
        if (dir_entries(c->tmp) > 0) break;
        struct timespec ts = { 0, 100000 };
        nanosleep(&ts, NULL);
    }
    atomic_store(c->cancel, 1);
    return NULL;
}

static void test_cancel_stops_the_workers(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    char p[PATH_MAX];
    for (int i = 0; i < 6; i++) {
        snprintf(p, sizeof(p), TEST_SRC "/big%d.bin", i);
        write_random(p, 64, (uint64_t)i + 100);
    }

    env_t e;
    env_open(&e, "cancel");
    char tmp[PATH_MAX + 16];
    snprintf(tmp, sizeof(tmp), "%s/repo/tmp", e.dir);
    atomic_int cancel = 0;
    canceller_t c = { &cancel, tmp, 0 };
    backup_opts_t o = opts_for(4);
    o.cancel = &cancel;

    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, canceller, &c), 0);
    assert_null(phase_current);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    status_t st = backup_run(e.store, e.db, e.repo_id, &o, NULL);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    __atomic_store_n(&c.done, 1, __ATOMIC_RELEASE);
    pthread_join(th, NULL);
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;

    assert_int_equal(st, ERR_BUSY);
    /* Within a read of each file, not at the end of 384 MiB; generous for
     * a sanitizer build on a loaded runner. */
    assert_true(secs < 10.0);
    /* Workers abandoned what they were staging: nothing left in tmp/. */
    assert_int_equal(dir_entries(tmp), 0);
    /* OWN-12: a failed run used to leave this aimed at its stack frame. */
    assert_null(phase_current);
    /* Nothing staged survives a cancelled run. */
    db_result_t *r = NULL;
    assert_int_equal(db_execute(e.db, "SELECT COUNT(*) FROM snapshots", NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);
    env_close(&e);
}

/* Compressible content of a given size, distinct per seed. */
static void write_compressible(const char *path, unsigned mib, unsigned seed)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    char line[80];
    for (unsigned long i = 0; i < (unsigned long)mib * MIB / 64; i++) {
        snprintf(line, sizeof(line), "line %010lu of a compressible file, seed %05u", i, seed);
        fprintf(f, "%-63.63s\n", line);   /* 64 bytes a line, so mib MiB */
    }
    fclose(f);
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal((uint64_t)sb.st_size, (uint64_t)mib * MIB);
}

static uint64_t g_tree_bytes;
static int sum_one(const char *path, const struct stat *sb, int type, struct FTW *ftw)
{
    (void)path; (void)ftw;
    if (type == FTW_F) g_tree_bytes += (uint64_t)sb->st_size;
    return 0;
}

static uint64_t tree_bytes(const env_t *e, const char *sub)
{
    char p[PATH_MAX + 32];
    snprintf(p, sizeof(p), "%s/repo/%s", e->dir, sub);
    g_tree_bytes = 0;
    (void)nftw(p, sum_one, 16, FTW_PHYS);
    return g_tree_bytes;
}

/*
 * The write counters (B086) account for every byte the backup put in the
 * repository, by what it was: checked against the files on disk, not
 * against the counters' own arithmetic.
 *
 * Format 3: small files, an incompressible large file, a compressible one
 * and two identical large files all go into packs as they are read, each
 * chunk compressed or not by its own probe, and the second twin's chunks
 * are found present before anything of them is written. Nothing is loose,
 * nothing is staged and dropped, and the pack pass finds nothing to do.
 */
static void test_write_counters_match_the_disk(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    char p[PATH_MAX];
    for (int i = 0; i < 20; i++) {
        char body[64];
        snprintf(p, sizeof(p), TEST_SRC "/small%02d.txt", i);
        snprintf(body, sizeof(body), "small file %d\n", i);
        write_text(p, body);
    }
    write_random(TEST_SRC "/random.bin", 20, 11);
    write_compressible(TEST_SRC "/text.txt", 20, 1);
    write_random(TEST_SRC "/twin1.bin", 18, 12);
    write_random(TEST_SRC "/twin2.bin", 18, 12);

    env_t e;
    env_open(&e, "counters");
    backup_opts_t o = opts_for(1);
    backup_result_t r;
    assert_int_equal(backup_run(e.store, e.db, e.repo_id, &o, &r), OK);

    const uint64_t *w = r.written.bytes;
    uint64_t sum = 0;
    for (int i = 0; i < STORE_WR__COUNT; i++) sum += w[i];
    assert_int_equal(r.written_total, sum);

    uint64_t p20 = 20ull * MIB, p18 = 18ull * MIB;

    /* Every pack is still on disk after a first backup, so the pack
     * counters are exactly the size of packs/. */
    assert_int_equal(w[STORE_WR_PACK] + w[STORE_WR_PACK_PARITY], tree_bytes(&e, "packs"));
    /* Written once: the random file and one twin at about their size with
     * parity, the text compressed, the second twin not at all. */
    uint64_t packed = w[STORE_WR_PACK] + w[STORE_WR_PACK_PARITY];
    assert_true(packed > p20 + p18);
    assert_true(packed < (p20 + p18) * 107 / 100 + p20 / 4);

    /* A second backup of the same tree writes nothing at all. */
    backup_result_t r2;
    assert_int_equal(backup_run(e.store, e.db, e.repo_id, &o, &r2), OK);
    assert_int_equal(r2.files_unchanged, r2.files);
    assert_int_equal(r2.written_total, 0);
    env_close(&e);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_plan),
        cmocka_unit_test_setup_teardown(test_memory_budget_reads_cgroup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_same_repository_on_one_or_many_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failing_file_is_skipped_as_it_is_serially, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_stops_the_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_write_counters_match_the_disk, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
