/*
 * Phase-level benchmarks for c-backup.
 *
 * Exercises real pipeline operations against temporary repositories with
 * synthetic data.  Each benchmark creates and tears down its own repo.
 *
 * Usage:
 *   build/bench_phases              — run all benchmarks
 *   build/bench_phases scan         — run only benchmarks matching "scan"
 *   build/bench_phases -l           — list available benchmarks
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <inttypes.h>

#include "repo.h"
#include "backup.h"
#include "scan.h"
#include "object.h"
#include "snapshot.h"
#include "restore.h"
#include "pack.h"
#include "gc.h"

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

#define BENCH_REPO "/tmp/c_backup_bench_repo"
#define BENCH_SRC  "/tmp/c_backup_bench_src"
#define BENCH_DST  "/tmp/c_backup_bench_dst"

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
    rmrf(BENCH_DST);
}

static uint64_t g_file_seed;

/* Create a file with compressible but unique content (unique per call).
 * Every 8th byte comes from the PRNG; the other 7 repeat it.  This gives
 * ~50 % LZ4 compression — well under the 90 % skip threshold — while still
 * producing distinct hashes so dedup doesn't collapse multiple files. */
static void create_file(const char *path, size_t size)
{
    FILE *f = fopen(path, "wb");
    if (!f) return;
    uint64_t s = 0xdeadbeefcafebabeULL ^ (uint64_t)size ^ (++g_file_seed * 2654435761ULL);
    uint8_t buf[8192];
    size_t remaining = size;
    while (remaining > 0) {
        size_t chunk = remaining < sizeof(buf) ? remaining : sizeof(buf);
        for (size_t i = 0; i < chunk; i++) {
            if ((i & 7) == 0) {
                s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            }
            buf[i] = (uint8_t)(s >> 32);
        }
        fwrite(buf, 1, chunk, f);
        remaining -= chunk;
    }
    fclose(f);
}

/* Create a directory tree with N files of given size */
static uint64_t create_tree(const char *root, int nfiles, size_t file_size)
{
    rmrf(root);
    mkdir(root, 0755);

    /* Create subdirectories: root/00/ through root/ff/ */
    for (int d = 0; d < 256; d++) {
        char dir[512];
        snprintf(dir, sizeof(dir), "%s/%02x", root, d);
        mkdir(dir, 0755);
    }

    uint64_t total_bytes = 0;
    for (int i = 0; i < nfiles; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%02x/file_%06d.dat",
                 root, i % 256, i);
        create_file(path, file_size);
        total_bytes += (uint64_t)file_size;
    }
    return total_bytes;
}

static void report(const char *name, double value, const char *unit, double elapsed)
{
    printf("  %-40s %10.1f %-10s (%.2fs)\n", name, value, unit, elapsed);
}

/* ------------------------------------------------------------------ */
/* Benchmark registry                                                  */
/* ------------------------------------------------------------------ */

typedef void (*bench_fn_t)(void);

typedef struct {
    const char *name;
    bench_fn_t fn;
} phase_bench_t;

static phase_bench_t g_benches[64];
static int g_nbench;

#define REGISTER(nm, func) \
    __attribute__((constructor)) static void reg_##func(void) { \
        g_benches[g_nbench].name = nm; \
        g_benches[g_nbench].fn   = func; \
        g_nbench++; \
    }

/* ------------------------------------------------------------------ */
/* Phase 1: Scan                                                       */
/* ------------------------------------------------------------------ */

static void bench_scan_1k_small(void)
{
    int nfiles = 1000;
    create_tree(BENCH_SRC, nfiles, 64);

    /* Warm the dcache with a dummy scan */
    scan_imap_t *imap = scan_imap_new();
    scan_result_t *res = NULL;
    scan_tree(BENCH_SRC, imap, NULL, &res);
    scan_result_free(res); res = NULL;
    scan_imap_free(imap);

    double t0 = now_sec();
    imap = scan_imap_new();
    scan_tree(BENCH_SRC, imap, NULL, &res);
    double elapsed = now_sec() - t0;
    uint32_t entries = res ? res->count : 0;
    scan_result_free(res);
    scan_imap_free(imap);

    report("scan_1k_64B", (double)entries / elapsed, "entries/s", elapsed);
    rmrf(BENCH_SRC);
}
REGISTER("scan_1k_64B", bench_scan_1k_small);

static void bench_scan_10k_small(void)
{
    int nfiles = 10000;
    create_tree(BENCH_SRC, nfiles, 64);

    scan_imap_t *imap = scan_imap_new();
    scan_result_t *res = NULL;
    scan_tree(BENCH_SRC, imap, NULL, &res);
    scan_result_free(res); res = NULL;
    scan_imap_free(imap);

    double t0 = now_sec();
    imap = scan_imap_new();
    scan_tree(BENCH_SRC, imap, NULL, &res);
    double elapsed = now_sec() - t0;
    uint32_t entries = res ? res->count : 0;
    scan_result_free(res);
    scan_imap_free(imap);

    report("scan_10k_64B", (double)entries / elapsed, "entries/s", elapsed);
    rmrf(BENCH_SRC);
}
REGISTER("scan_10k_64B", bench_scan_10k_small);

/* ------------------------------------------------------------------ */
/* Phase 3: Object store (single-threaded loose)                       */
/* ------------------------------------------------------------------ */

static void bench_store_small_objects(void)
{
    bench_cleanup();
    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    int count = 1000;
    size_t obj_sz = 4096;
    uint8_t *data = malloc(obj_sz);
    uint64_t s = 0x1234567890abcdefULL;
    uint64_t total = 0;

    double t0 = now_sec();
    for (int i = 0; i < count; i++) {
        /* Vary content so each object is unique */
        for (size_t j = 0; j < obj_sz; j++) {
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            data[j] = (uint8_t)(s >> 32);
        }
        uint8_t hash[OBJECT_HASH_SIZE];
        int is_new = 0;
        uint64_t phys = 0;
        object_store_ex(repo, 1, data, obj_sz, hash, &is_new, &phys);
        total += obj_sz;
    }
    double elapsed = now_sec() - t0;

    double mib = (double)total / (1024.0 * 1024.0) / elapsed;
    double ops = (double)count / elapsed;

    report("store_1k_4K_loose", mib, "MiB/s", elapsed);
    report("store_1k_4K_loose", ops, "objects/s", elapsed);

    free(data);
    repo_close(repo);
    bench_cleanup();
}
REGISTER("store_1k_4K_loose", bench_store_small_objects);

static void bench_store_file_16m(void)
{
    bench_cleanup();
    mkdir(BENCH_SRC, 0755);
    create_file(BENCH_SRC "/bigfile.dat", 16u * 1024 * 1024);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    int fd = open(BENCH_SRC "/bigfile.dat", O_RDONLY);
    uint8_t hash[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0;

    double t0 = now_sec();
    object_store_file_ex(repo, fd, 16u * 1024 * 1024, hash, &is_new, &phys);
    double elapsed = now_sec() - t0;

    close(fd);

    double mib = 16.0 / elapsed;
    report("store_file_16M_loose", mib, "MiB/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("store_file_16M_loose", bench_store_file_16m);

/* ------------------------------------------------------------------ */
/* Full backup cycle                                                   */
/* ------------------------------------------------------------------ */

static void bench_backup_1k_4k(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 1000, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;

    const char *paths[] = { BENCH_SRC };

    double t0 = now_sec();
    backup_run_opts(repo, paths, 1, &opts);
    double elapsed = now_sec() - t0;

    double mib = (1000.0 * 4096.0) / (1024.0 * 1024.0) / elapsed;
    report("backup_1k_4K (full)", mib, "MiB/s", elapsed);
    report("backup_1k_4K (full)", 1000.0 / elapsed, "files/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("backup_1k_4K", bench_backup_1k_4k);

static void bench_backup_incremental_1k_4k(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 1000, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;

    const char *paths[] = { BENCH_SRC };

    /* First backup (full) */
    backup_run_opts(repo, paths, 1, &opts);

    /* Modify 10 files */
    for (int i = 0; i < 10; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%02x/file_%06d.dat",
                 BENCH_SRC, i % 256, i);
        create_file(path, 4096);
    }

    /* Incremental backup */
    double t0 = now_sec();
    backup_run_opts(repo, paths, 1, &opts);
    double elapsed = now_sec() - t0;

    report("backup_1k_4K (incr, 10 changed)", 1000.0 / elapsed, "entries/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("backup_1k_4K_incr", bench_backup_incremental_1k_4k);

/* ------------------------------------------------------------------ */
/* Pack                                                                */
/* ------------------------------------------------------------------ */

/* Helper: backup source tree into repo, then time repo_pack(). */
static void pack_bench_run(const char *label, int nfiles, size_t file_size)
{
    bench_cleanup();
    uint64_t total_bytes = create_tree(BENCH_SRC, nfiles, file_size);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);

    uint32_t packed = 0;
    double t0 = now_sec();
    repo_pack(repo, &packed);
    double elapsed = now_sec() - t0;

    char name_ops[64], name_mib[64];
    snprintf(name_ops, sizeof(name_ops), "%s", label);
    snprintf(name_mib, sizeof(name_mib), "%s", label);
    report(name_ops, (double)packed / elapsed, "objects/s", elapsed);
    double mib = (double)total_bytes / (1024.0 * 1024.0) / elapsed;
    report(name_mib, mib, "MiB/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}

/* Small objects: per-object overhead (index, RS per-entry, fsync) */
static void bench_pack_small(void)
{
    pack_bench_run("pack_1k_4K", 1000, 4096);
}
REGISTER("pack_1k_4K", bench_pack_small);

/* Medium objects: 64K, typical source code / configs */
static void bench_pack_medium(void)
{
    pack_bench_run("pack_200_64K", 200, 64 * 1024);
}
REGISTER("pack_200_64K", bench_pack_medium);

/* Large objects: 32M, exercises streaming pack path (>16M threshold) */
static void bench_pack_large(void)
{
    pack_bench_run("pack_8_32M", 8, 32u * 1024 * 1024);
}
REGISTER("pack_8_32M", bench_pack_large);

/* Mixed: small + large together, realistic workload */
static void bench_pack_mixed(void)
{
    bench_cleanup();

    /* Create a mix: 500 × 4K + 4 × 32M */
    mkdir(BENCH_SRC, 0755);
    for (int d = 0; d < 256; d++) {
        char dir[512];
        snprintf(dir, sizeof(dir), "%s/%02x", BENCH_SRC, d);
        mkdir(dir, 0755);
    }

    uint64_t total_bytes = 0;
    for (int i = 0; i < 500; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%02x/small_%06d.dat",
                 BENCH_SRC, i % 256, i);
        create_file(path, 4096);
        total_bytes += 4096;
    }
    for (int i = 0; i < 4; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%02x/large_%06d.dat",
                 BENCH_SRC, i % 256, i);
        create_file(path, 32u * 1024 * 1024);
        total_bytes += 32u * 1024 * 1024;
    }

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);

    uint32_t packed = 0;
    double t0 = now_sec();
    repo_pack(repo, &packed);
    double elapsed = now_sec() - t0;

    report("pack_mixed (500×4K + 4×32M)", (double)packed / elapsed, "objects/s", elapsed);
    double mib = (double)total_bytes / (1024.0 * 1024.0) / elapsed;
    report("pack_mixed (500×4K + 4×32M)", mib, "MiB/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("pack_mixed", bench_pack_mixed);

/* ------------------------------------------------------------------ */
/* Verify (read-back + integrity check)                                */
/* ------------------------------------------------------------------ */

static void bench_verify(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 500, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);

    /* Pack first so verify exercises packed read path */
    uint32_t packed;
    repo_pack(repo, &packed);

    verify_opts_t vopts = {0};
    double t0 = now_sec();
    repo_verify(repo, &vopts);
    double elapsed = now_sec() - t0;

    report("verify_500_4K_packed", (double)vopts.objects_checked / elapsed, "objects/s", elapsed);
    double mib = (double)vopts.bytes_checked / (1024.0 * 1024.0) / elapsed;
    report("verify_500_4K_packed", mib, "MiB/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("verify_500_4K", bench_verify);

/* ------------------------------------------------------------------ */
/* GC                                                                  */
/* ------------------------------------------------------------------ */

static void bench_gc(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 500, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);

    uint32_t kept = 0, deleted = 0;
    double t0 = now_sec();
    repo_gc(repo, &kept, &deleted);
    double elapsed = now_sec() - t0;

    report("gc_500_4K", (double)(kept + deleted) / elapsed, "objects/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("gc_500_4K", bench_gc);

/* ------------------------------------------------------------------ */
/* Snapshot read hot paths                                             */
/* ------------------------------------------------------------------ */

#include "diff.h"
#include "ls.h"

/*
 * Helper: create a repo with N snapshots, each containing `nfiles` files.
 * Between snapshots, modifies `n_changed` files so diffs are non-trivial.
 * Returns the repo (caller must close + cleanup).
 */
static repo_t *create_multi_snap_repo(int nfiles, size_t file_size,
                                       int n_snaps, int n_changed)
{
    bench_cleanup();
    create_tree(BENCH_SRC, nfiles, file_size);
    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };

    for (int s = 0; s < n_snaps; s++) {
        backup_run_opts(repo, paths, 1, &opts);
        /* Modify some files for next snapshot */
        if (s < n_snaps - 1 && n_changed > 0) {
            for (int i = 0; i < n_changed; i++) {
                char path[512];
                snprintf(path, sizeof(path), "%s/%02x/file_%06d.dat",
                         BENCH_SRC, (s * n_changed + i) % 256, (s * n_changed + i) % nfiles);
                create_file(path, file_size);
            }
        }
    }
    return repo;
}

/* --- snapshot_load (full) --- */

static void bench_snap_load_1k(void)
{
    repo_t *repo = create_multi_snap_repo(1000, 4096, 1, 0);

    snapshot_t *snap = NULL;
    /* warmup */
    snapshot_load(repo, 1, &snap);
    snapshot_free(snap); snap = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_load(repo, 1, &snap);
        snapshot_free(snap); snap = NULL;
    }
    double elapsed = now_sec() - t0;

    report("snap_load_1k_files", (double)iters / elapsed, "loads/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("snap_load_1k", bench_snap_load_1k);

static void bench_snap_load_10k(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);
    snapshot_free(snap); snap = NULL;

    int iters = 20;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_load(repo, 1, &snap);
        snapshot_free(snap); snap = NULL;
    }
    double elapsed = now_sec() - t0;

    report("snap_load_10k_files", (double)iters / elapsed, "loads/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("snap_load_10k", bench_snap_load_10k);

/* --- snapshot_load_nodes_only --- */

static void bench_snap_load_nodes_10k(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    snapshot_t *snap = NULL;
    snapshot_load_nodes_only(repo, 1, &snap);
    snapshot_free(snap); snap = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_load_nodes_only(repo, 1, &snap);
        snapshot_free(snap); snap = NULL;
    }
    double elapsed = now_sec() - t0;

    report("snap_load_nodes_only_10k", (double)iters / elapsed, "loads/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("snap_load_nodes_only_10k", bench_snap_load_nodes_10k);

/* --- snapshot_list_all --- */

static void bench_snap_list_all(void)
{
    repo_t *repo = create_multi_snap_repo(500, 4096, 10, 5);

    snap_list_t *sl = NULL;
    snapshot_list_all(repo, &sl);
    snap_list_free(sl); sl = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_list_all(repo, &sl);
        snap_list_free(sl); sl = NULL;
    }
    double elapsed = now_sec() - t0;

    report("snap_list_all (10 snaps)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("snap_list_all", bench_snap_list_all);

/* --- pathmap_build --- */

static void bench_pathmap_1k(void)
{
    repo_t *repo = create_multi_snap_repo(1000, 4096, 1, 0);

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);

    pathmap_t *pm = NULL;
    /* warmup */
    pathmap_build(snap, &pm);
    pathmap_free(pm); pm = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        pathmap_build(snap, &pm);
        pathmap_free(pm); pm = NULL;
    }
    double elapsed = now_sec() - t0;

    report("pathmap_build_1k", (double)iters / elapsed, "builds/s", elapsed);
    report("pathmap_build_1k", (double)iters * 1000.0 / elapsed / 1e6, "M entries/s", elapsed);

    snapshot_free(snap);
    repo_close(repo);
    bench_cleanup();
}
REGISTER("pathmap_1k", bench_pathmap_1k);

static void bench_pathmap_10k(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    snapshot_t *snap = NULL;
    snapshot_load(repo, 1, &snap);

    pathmap_t *pm = NULL;
    pathmap_build(snap, &pm);
    pathmap_free(pm); pm = NULL;

    int iters = 20;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        pathmap_build(snap, &pm);
        pathmap_free(pm); pm = NULL;
    }
    double elapsed = now_sec() - t0;

    report("pathmap_build_10k", (double)iters / elapsed, "builds/s", elapsed);
    report("pathmap_build_10k", (double)iters * 10000.0 / elapsed / 1e6, "M entries/s", elapsed);

    snapshot_free(snap);
    repo_close(repo);
    bench_cleanup();
}
REGISTER("pathmap_10k", bench_pathmap_10k);

/* --- snapshot_ls_collect --- */

static void bench_ls_collect_1k(void)
{
    repo_t *repo = create_multi_snap_repo(1000, 4096, 1, 0);

    ls_result_t *r = NULL;
    /* warmup */
    snapshot_ls_collect(repo, 1, "", 1, 0, NULL, &r);
    ls_result_free(r); r = NULL;

    int iters = 30;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_ls_collect(repo, 1, "", 1, 0, NULL, &r);
        ls_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("ls_collect_1k (recursive)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("ls_collect_1k", bench_ls_collect_1k);

static void bench_ls_collect_10k(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    ls_result_t *r = NULL;
    snapshot_ls_collect(repo, 1, "", 1, 0, NULL, &r);
    ls_result_free(r); r = NULL;

    int iters = 10;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_ls_collect(repo, 1, "", 1, 0, NULL, &r);
        ls_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("ls_collect_10k (recursive)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("ls_collect_10k", bench_ls_collect_10k);

static void bench_ls_collect_10k_nonrec(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    ls_result_t *r = NULL;
    snapshot_ls_collect(repo, 1, "", 0, 0, NULL, &r);
    ls_result_free(r); r = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_ls_collect(repo, 1, "", 0, 0, NULL, &r);
        ls_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("ls_collect_10k (non-recursive)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("ls_collect_10k_nonrec", bench_ls_collect_10k_nonrec);

static void bench_ls_collect_10k_subdir(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    /* List a single subdirectory (e.g. "00") */
    ls_result_t *r = NULL;
    snapshot_ls_collect(repo, 1, "00", 0, 0, NULL, &r);
    ls_result_free(r); r = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_ls_collect(repo, 1, "00", 0, 0, NULL, &r);
        ls_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("ls_collect_10k (subdir non-rec)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("ls_collect_10k_subdir", bench_ls_collect_10k_subdir);

/* --- snapshot_search --- */

static void bench_search_1k(void)
{
    repo_t *repo = create_multi_snap_repo(1000, 4096, 1, 0);

    search_result_t *r = NULL;
    snapshot_search(repo, 1, "file_0001", 5000, &r);
    search_result_free(r); r = NULL;

    int iters = 50;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_search(repo, 1, "file_0001", 5000, &r);
        search_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("search_1k (substring)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("search_1k", bench_search_1k);

static void bench_search_10k(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 1, 0);

    search_result_t *r = NULL;
    snapshot_search(repo, 1, "file_0001", 5000, &r);
    search_result_free(r); r = NULL;

    int iters = 20;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_search(repo, 1, "file_0001", 5000, &r);
        search_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("search_10k (substring)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("search_10k", bench_search_10k);

static void bench_search_multi_snap(void)
{
    /* 5 snapshots with 1K files each, search across all */
    repo_t *repo = create_multi_snap_repo(1000, 4096, 5, 10);

    search_result_t *r = NULL;
    int iters = 30;
    double t0 = now_sec();
    for (int it = 0; it < iters; it++) {
        for (uint32_t sid = 1; sid <= 5; sid++) {
            snapshot_search(repo, sid, "file_0001", 5000, &r);
            search_result_free(r); r = NULL;
        }
    }
    double elapsed = now_sec() - t0;

    report("search_multi (5×1k snaps)", (double)iters / elapsed, "full-scans/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("search_multi", bench_search_multi_snap);

/* --- snapshot_diff_collect --- */

static void bench_diff_1k(void)
{
    repo_t *repo = create_multi_snap_repo(1000, 4096, 2, 50);

    diff_result_t *r = NULL;
    /* warmup */
    snapshot_diff_collect(repo, 1, 2, &r);
    diff_result_free(r); r = NULL;

    int iters = 30;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_diff_collect(repo, 1, 2, &r);
        diff_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("diff_collect_1k (50 changes)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("diff_1k", bench_diff_1k);

static void bench_diff_10k(void)
{
    repo_t *repo = create_multi_snap_repo(10000, 256, 2, 200);

    diff_result_t *r = NULL;
    snapshot_diff_collect(repo, 1, 2, &r);
    diff_result_free(r); r = NULL;

    int iters = 10;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        snapshot_diff_collect(repo, 1, 2, &r);
        diff_result_free(r); r = NULL;
    }
    double elapsed = now_sec() - t0;

    report("diff_collect_10k (200 changes)", (double)iters / elapsed, "calls/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("diff_10k", bench_diff_10k);

/* ------------------------------------------------------------------ */
/* Restore                                                             */
/* ------------------------------------------------------------------ */

static void bench_restore(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 500, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);

    uint32_t packed;
    repo_pack(repo, &packed);

    mkdir(BENCH_DST, 0755);

    double t0 = now_sec();
    restore_snapshot(repo, 1, BENCH_DST);
    double elapsed = now_sec() - t0;

    double mib = (500.0 * 4096.0) / (1024.0 * 1024.0) / elapsed;
    report("restore_500_4K_packed", mib, "MiB/s", elapsed);
    report("restore_500_4K_packed", 500.0 / elapsed, "files/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("restore_500_4K", bench_restore);

/* ------------------------------------------------------------------ */
/* Pack Index (Opt 1)                                                  */
/* ------------------------------------------------------------------ */

#include "pack_index.h"

static void bench_pack_index_rebuild(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 1000, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);
    repo_pack(repo, NULL);

    /* Delete index, then time rebuild */
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index.pidx", BENCH_REPO);
    unlink(idx_path);

    int iters = 20;
    double t0 = now_sec();
    for (int i = 0; i < iters; i++) {
        unlink(idx_path);
        pack_index_rebuild(repo);
    }
    double elapsed = now_sec() - t0;

    report("pack_index_rebuild (1k objs)", (double)iters / elapsed, "rebuilds/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("pack_index_rebuild", bench_pack_index_rebuild);

static void bench_pack_index_lookup(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 1000, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);
    repo_pack(repo, NULL);

    /* Collect content hashes from snapshot */
    snapshot_t *snap = NULL;
    uint32_t head = 0;
    snapshot_read_head(repo, &head);
    snapshot_load(repo, head, &snap);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t hashes[2048][OBJECT_HASH_SIZE];
    int nhashes = 0;
    for (uint32_t i = 0; i < snap->node_count && nhashes < 2048; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0)
            memcpy(hashes[nhashes++], snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
    }
    snapshot_free(snap);

    pack_index_t *idx = pack_index_open(repo);
    if (!idx || nhashes == 0) {
        repo_close(repo);
        bench_cleanup();
        return;
    }

    /* Warmup */
    for (int i = 0; i < nhashes; i++)
        pack_index_lookup(idx, hashes[i]);

    uint64_t iters = 0;
    double t0 = now_sec(), elapsed;
    do {
        for (int i = 0; i < nhashes; i++)
            pack_index_lookup(idx, hashes[i]);
        iters++;
        elapsed = now_sec() - t0;
    } while (elapsed < 1.0);

    double lookups = (double)iters * (double)nhashes;
    report("pack_index_lookup", lookups / elapsed / 1e6, "Mlookups/s", elapsed);

    pack_index_close(idx);
    repo_close(repo);
    bench_cleanup();
}
REGISTER("pack_index_lookup", bench_pack_index_lookup);

/* ------------------------------------------------------------------ */
/* Restore packed (Opt 4: pack-ordered reads)                          */
/* ------------------------------------------------------------------ */

static void bench_restore_packed_1k(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 1000, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);
    repo_pack(repo, NULL);

    mkdir(BENCH_DST, 0755);
    double t0 = now_sec();
    restore_snapshot(repo, 1, BENCH_DST);
    double elapsed = now_sec() - t0;

    double mib = (1000.0 * 4096.0) / (1024.0 * 1024.0) / elapsed;
    report("restore_1k_4K_packed", mib, "MiB/s", elapsed);
    report("restore_1k_4K_packed", 1000.0 / elapsed, "files/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("restore_1k_4K_packed", bench_restore_packed_1k);

/* ------------------------------------------------------------------ */
/* Verify packed multi-snap (Opt 5: dedup + pack-ordered)              */
/* ------------------------------------------------------------------ */

static void bench_verify_multi_snap(void)
{
    repo_t *repo = create_multi_snap_repo(1000, 4096, 5, 10);
    repo_pack(repo, NULL);

    verify_opts_t vopts = {0};
    double t0 = now_sec();
    repo_verify(repo, &vopts);
    double elapsed = now_sec() - t0;

    report("verify_5snap_1k_packed", (double)vopts.objects_checked / elapsed, "objects/s", elapsed);
    double mib = (double)vopts.bytes_checked / (1024.0 * 1024.0) / elapsed;
    report("verify_5snap_1k_packed", mib, "MiB/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("verify_5snap_1k_packed", bench_verify_multi_snap);

/* ------------------------------------------------------------------ */
/* Object load with parity (Opt 6: sequential trailer read)            */
/* ------------------------------------------------------------------ */

static void bench_object_load_parity(void)
{
    bench_cleanup();
    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    /* Store 500 loose objects (4K each, with v2 parity trailers) */
    int count = 500;
    size_t obj_sz = 4096;
    uint8_t *data = malloc(obj_sz);
    uint8_t stored_hashes[500][OBJECT_HASH_SIZE];
    uint64_t s = 0xfeedface12345678ULL;

    for (int i = 0; i < count; i++) {
        for (size_t j = 0; j < obj_sz; j++) {
            s ^= s << 13; s ^= s >> 7; s ^= s << 17;
            data[j] = (uint8_t)(s >> 32);
        }
        int is_new = 0;
        uint64_t phys = 0;
        object_store_ex(repo, 1, data, obj_sz, stored_hashes[i], &is_new, &phys);
    }
    free(data);

    /* Warm cache */
    for (int i = 0; i < count; i++) {
        void *d = NULL; size_t sz = 0;
        object_load(repo, stored_hashes[i], &d, &sz, NULL);
        free(d);
    }

    /* Time object_load (exercises parity trailer read) */
    uint64_t total_bytes = 0;
    double t0 = now_sec();
    for (int i = 0; i < count; i++) {
        void *d = NULL; size_t sz = 0;
        object_load(repo, stored_hashes[i], &d, &sz, NULL);
        total_bytes += sz;
        free(d);
    }
    double elapsed = now_sec() - t0;

    double mib = (double)total_bytes / (1024.0 * 1024.0) / elapsed;
    report("object_load_loose_parity_500", mib, "MiB/s", elapsed);
    report("object_load_loose_parity_500", (double)count / elapsed, "objects/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("object_load_parity", bench_object_load_parity);

/* ------------------------------------------------------------------ */
/* pack_resolve_location (Opt 4/5 foundation)                          */
/* ------------------------------------------------------------------ */

static void bench_pack_resolve(void)
{
    bench_cleanup();
    create_tree(BENCH_SRC, 1000, 4096);

    repo_init(BENCH_REPO);
    repo_t *repo;
    repo_open(BENCH_REPO, &repo);
    repo_lock(repo);

    backup_opts_t opts = {0};
    opts.quiet = 1;
    const char *paths[] = { BENCH_SRC };
    backup_run_opts(repo, paths, 1, &opts);
    repo_pack(repo, NULL);

    /* Collect hashes */
    snapshot_t *snap = NULL;
    uint32_t head = 0;
    snapshot_read_head(repo, &head);
    snapshot_load(repo, head, &snap);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t hashes[2048][OBJECT_HASH_SIZE];
    int nhashes = 0;
    for (uint32_t i = 0; i < snap->node_count && nhashes < 2048; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0)
            memcpy(hashes[nhashes++], snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
    }
    snapshot_free(snap);

    /* Warmup */
    for (int i = 0; i < nhashes; i++) {
        uint32_t pn; uint64_t off;
        pack_resolve_location(repo, hashes[i], &pn, &off);
    }

    uint64_t iters = 0;
    double t0 = now_sec(), elapsed;
    do {
        for (int i = 0; i < nhashes; i++) {
            uint32_t pn; uint64_t off;
            pack_resolve_location(repo, hashes[i], &pn, &off);
        }
        iters++;
        elapsed = now_sec() - t0;
    } while (elapsed < 1.0);

    double resolves = (double)iters * (double)nhashes;
    report("pack_resolve_location", resolves / elapsed / 1e6, "Mresolves/s", elapsed);

    repo_close(repo);
    bench_cleanup();
}
REGISTER("pack_resolve", bench_pack_resolve);

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static int matches_filter(const char *name, int argc, char **argv)
{
    if (argc <= 1) return 1;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0) continue;
        if (strstr(name, argv[i]) != NULL) return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0) {
            printf("Available benchmarks:\n");
            for (int b = 0; b < g_nbench; b++)
                printf("  %s\n", g_benches[b].name);
            return 0;
        }
    }

    printf("bench_phases: %d benchmark(s) registered\n", g_nbench);
    printf("  %-40s %10s %-10s\n", "NAME", "RESULT", "UNIT");
    printf("  %-40s %10s %-10s\n",
           "----------------------------------------",
           "----------", "----------");

    int ran = 0;
    for (int b = 0; b < g_nbench; b++) {
        if (!matches_filter(g_benches[b].name, argc, argv))
            continue;
        g_benches[b].fn();
        ran++;
    }

    if (ran == 0)
        printf("  (no benchmarks matched filter)\n");

    bench_cleanup();
    return 0;
}
