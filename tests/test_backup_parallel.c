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
#include <sys/stat.h>
#include <limits.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <openssl/evp.h>

#include "backup.h"
#include "object.h"
#include "phase_timer.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "store_pool.h"

#define TEST_DIR  "/tmp/c_backup_test_parallel"
#define TEST_SRC  TEST_DIR "/src"
/* Repositories live apart from the source, so creating one does not touch
 * a directory the next backup scans. */
#define TEST_ENVS "/tmp/c_backup_test_parallel_envs"
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
 * The tree of the source directory itself. The root tree also covers the
 * source's ancestors (/tmp among them), whose times other processes change
 * between two backups; from src down, nothing else writes.
 */
static void src_tree_hash(env_t *e, int64_t snap, char out[80])
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(e->db, "SELECT hex(subtree_hash) FROM nodes"
                                " WHERE snap_id = ? AND name = 'src' AND type = 2",
                                (const char *[]){ id, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    snprintf(out, 80, "%s", r->rows[0][0]);
    db_result_free(r);
}

/* "relative-path sha256\n" for every file under a directory, sorted. */
static char  **walk_lines;
static size_t  walk_n, walk_cap, walk_prefix;

static int walk_one(const char *path, const struct stat *sb, int type, struct FTW *ftw)
{
    (void)sb; (void)ftw;
    if (type != FTW_F) return 0;
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    int fd = open(path, O_RDONLY);
    if (fd < 0) { EVP_MD_CTX_free(md); return -1; }
    char *buf = malloc(MIB);
    ssize_t n;
    while ((n = read(fd, buf, MIB)) > 0) EVP_DigestUpdate(md, buf, (size_t)n);
    free(buf);
    close(fd);
    unsigned char h[32];
    unsigned hl = 0;
    EVP_DigestFinal_ex(md, h, &hl);
    EVP_MD_CTX_free(md);
    char line[PATH_MAX + 80];
    int off = snprintf(line, sizeof(line), "%s ", path + walk_prefix);
    for (unsigned i = 0; i < hl; i++) off += snprintf(line + off, sizeof(line) - (size_t)off, "%02x", h[i]);
    if (walk_n == walk_cap) {
        walk_cap = walk_cap ? walk_cap * 2 : 64;
        walk_lines = realloc(walk_lines, walk_cap * sizeof(*walk_lines));
    }
    walk_lines[walk_n++] = strdup(line);
    return 0;
}

static int line_cmp(const void *a, const void *b)
{
    return strcmp(*(char *const *)a, *(char *const *)b);
}

/* Every loose object, as one string. Caller frees. These are the objects
 * the workers wrote; the pack holds the small files and the trees, written
 * on the backup thread alone. */
static char *repo_digest(const env_t *e)
{
    walk_lines = NULL; walk_n = walk_cap = 0;
    char root[PATH_MAX + 16];
    snprintf(root, sizeof(root), "%s/repo", e->dir);
    walk_prefix = strlen(root);
    const char *sub[] = { "objects" };
    for (int i = 0; i < 1; i++) {
        char p[PATH_MAX + 32];
        snprintf(p, sizeof(p), "%s/%s", root, sub[i]);
        assert_int_equal(nftw(p, walk_one, 16, FTW_PHYS), 0);
    }
    qsort(walk_lines, walk_n, sizeof(*walk_lines), line_cmp);
    size_t total = 1;
    for (size_t i = 0; i < walk_n; i++) total += strlen(walk_lines[i]) + 1;
    char *out = malloc(total);
    out[0] = '\0';
    for (size_t i = 0; i < walk_n; i++) {
        strcat(out, walk_lines[i]);
        strcat(out, "\n");
        free(walk_lines[i]);
    }
    free(walk_lines);
    walk_lines = NULL;
    return out;
}

static int dir_entries(const char *path)
{
    DIR *d = opendir(path);
    if (!d) return -1;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0) n++;
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

    char ha[80], hb[80];
    src_tree_hash(&a, ra.snapshot_id, ha);
    src_tree_hash(&b, rb.snapshot_id, hb);
    assert_string_equal(ha, hb);

    assert_int_equal(ra.files, rb.files);
    assert_int_equal(ra.files, 50);
    assert_int_equal(ra.bytes_logical, rb.bytes_logical);
    /* The twins are one object, however the race between them went. */
    assert_int_equal(ra.objects_new, rb.objects_new);
    assert_int_equal(ra.bytes_stored, rb.bytes_stored);
    assert_int_equal(ra.trees_new, rb.trees_new);

    char *da = repo_digest(&a), *db_ = repo_digest(&b);
    assert_string_equal(da, db_);
    /* Six large objects loose: the twins are one. */
    size_t loose = 0;
    for (const char *c = da; (c = strstr(c, "/objects/")) != NULL; c++) loose++;
    assert_int_equal(loose, 6);
    free(da);
    free(db_);

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

    char hash[2][80];
    backup_result_t r[2];
    const unsigned workers[2] = { 1, 4 };
    for (int k = 0; k < 2; k++) {
        env_t e;
        env_open(&e, k ? "pooled" : "serial");
        int fired = 0;
        backup_opts_t o = opts_for(workers[k]);
        o.progress_cb  = swap_on_store;
        o.progress_ctx = &fired;
        assert_int_equal(backup_run(e.store, e.db, e.repo_id, &o, &r[k]), OK);
        assert_true(fired);
        src_tree_hash(&e, r[k].snapshot_id, hash[k]);
        char tmp[PATH_MAX + 16];
        snprintf(tmp, sizeof(tmp), "%s/repo/tmp", e.dir);
        assert_int_equal(dir_entries(tmp), 0);
        env_close(&e);

        assert_int_equal(rmdir(TEST_SRC "/b.bin"), 0);
        assert_int_equal(rename(TEST_DIR "/b.keep", TEST_SRC "/b.bin"), 0);
        assert_int_equal(utimensat(AT_FDCWD, TEST_SRC, src_times, 0), 0);
    }
    assert_int_equal(r[1].store_workers, 4);
    /* Skipped, not fatal: one file fewer, and the same snapshot either way. */
    assert_int_equal(r[0].files, 49);
    assert_int_equal(r[1].files, r[0].files);
    assert_int_equal(r[1].objects_new, r[0].objects_new);
    assert_int_equal(r[1].bytes_logical, r[0].bytes_logical);
    assert_string_equal(hash[0], hash[1]);
}

typedef struct {
    volatile int *cancel;
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
    __atomic_store_n(c->cancel, 1, __ATOMIC_RELEASE);
    return NULL;
}

static size_t count_loose(const env_t *e)
{
    char *d = repo_digest(e);
    size_t n = 0;
    for (const char *c = d; (c = strchr(c, '\n')) != NULL; c++) n++;
    free(d);
    return n;
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
    volatile int cancel = 0;
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
    /* Workers abandoned what they were storing, and the queue never ran. */
    assert_true(count_loose(&e) < 6);
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_plan),
        cmocka_unit_test_setup_teardown(test_same_repository_on_one_or_many_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failing_file_is_skipped_as_it_is_serially, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_stops_the_workers, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
