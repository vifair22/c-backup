/*
 * End-to-end backup, incremental and restore timings on the daemon's engine.
 *
 * Generates a source tree of many small files and a few large ones, then
 * times, in order:
 *
 *   1. a first backup into an empty repository, and the pack that follows it
 *   2. a second backup with nothing changed, and its pack (normally a no-op)
 *   3. a restore of the latest snapshot into an empty directory
 *
 * The backup job packs on completion (backup_routes.c), so the pack is part
 * of what a user waits for; it is timed separately because it is a separate
 * cost to attack. backup_run logs its own per-phase profile (phase_timer.h)
 * at info level, and that log is left on here: it is the same breakdown the
 * daemon writes on a deployment, which is the point of printing it.
 *
 * The large files are what the B086 first-backup work needs: random ones are
 * incompressible and exercise the object writer's read -> hash -> write ->
 * parity pipeline and the LZ4 bailout; the text copies are compressible and
 * identical, so the second dedups and the first is re-copied by the pack.
 *
 * Usage:
 *   bench_phases [options]
 *     -d, --dir DIR        parent of the working directory      (BENCH_DIR, /tmp)
 *     -n, --small N        small files                          (BENCH_SMALL_FILES, 10000)
 *     -s, --small-bytes B  mean small-file size in bytes        (BENCH_SMALL_BYTES, 4096)
 *     -l, --large N        incompressible large files           (BENCH_LARGE_FILES, 3)
 *     -t, --text N         identical compressible large files   (BENCH_TEXT_FILES, 2)
 *     -m, --large-mib M    size of each large file, MiB         (BENCH_LARGE_MIB, 64)
 *     -w, --workers N      large-file store workers, 0 automatic, 1 serial
 *                                                          (BENCH_STORE_WORKERS, 0)
 *     -p, --preset NAME    many-large: 8 incompressible files of 2 GiB and
 *                          nothing else, the B086 case; later options still
 *                          override it
 *     -c, --cold           sync and evict the source from the page cache before
 *                          each backup, and include a trailing sync in every
 *                          timing; only meaningful on a real filesystem
 *     -k, --keep           leave the working directory behind
 *
 * The working directory is DIR/c_backup_bench_phases and is removed at the
 * end unless --keep. On tmpfs (the default /tmp here) every number is a CPU
 * number; put DIR on the target filesystem to measure storage.
 */

#define _GNU_SOURCE
#include <getopt.h>
#include <inttypes.h>

#include "bench_common.h"

#include "backup.h"
#include "pack.h"
#include "restore.h"

#define FILES_PER_DIR 100
#define WRITE_CHUNK   (1u << 20)

typedef struct {
    char     parent[3072];
    char     work[3200];
    uint64_t small_files;
    uint64_t small_bytes;
    uint64_t large_files;
    uint64_t text_files;
    uint64_t large_mib;
    unsigned workers;
    int      cold;
    int      keep;
} opts_t;

typedef struct {
    uint64_t files;
    uint64_t bytes;
} tree_stats_t;

/* ---------- source generation ---------- */

static uint64_t g_rng = 0x9E3779B97F4A7C15ull;

static uint64_t xorshift(void)
{
    g_rng ^= g_rng << 13;
    g_rng ^= g_rng >> 7;
    g_rng ^= g_rng << 17;
    return g_rng;
}

static int write_all(int fd, const uint8_t *buf, size_t n)
{
    while (n > 0) {
        ssize_t w = write(fd, buf, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        buf += w;
        n -= (size_t)w;
    }
    return 0;
}

typedef enum { FILL_SMALL, FILL_RANDOM, FILL_TEXT } fill_t;

/*
 * Fill one chunk.
 *
 * FILL_SMALL repeats each random byte eight times: unique per file, so
 * nothing dedups, and about half-compressible, so it is packed compressed
 * rather than bailing out. FILL_RANDOM is incompressible. FILL_TEXT is
 * deterministic from its offset alone, so every copy is byte-identical.
 */
static void fill(uint8_t *buf, size_t n, fill_t kind, uint64_t offset)
{
    switch (kind) {
    case FILL_SMALL: {
        uint64_t r = 0;
        for (size_t i = 0; i < n; i++) {
            if ((i & 7) == 0) r = xorshift();
            buf[i] = (uint8_t)(r >> 24);
        }
        break;
    }
    case FILL_RANDOM:
        for (size_t i = 0; i + 8 <= n; i += 8) {
            uint64_t r = xorshift();
            memcpy(buf + i, &r, 8);
        }
        for (size_t i = n & ~(size_t)7; i < n; i++) buf[i] = (uint8_t)xorshift();
        break;
    case FILL_TEXT: {
        static const char words[] =
            "the quick brown fox jumps over the lazy dog while a backup runs ";
        for (size_t i = 0; i < n; i++) {
            uint64_t pos = offset + i;
            buf[i] = (pos % 81 == 80) ? '\n'
                                      : (uint8_t)words[(pos / 7 + pos) % (sizeof(words) - 1)];
        }
        break;
    }
    }
}

static int make_file(const char *path, uint64_t size, fill_t kind, uint8_t *buf)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "bench: create %s: %s\n", path, strerror(errno));
        return -1;
    }
    uint64_t done = 0;
    while (done < size) {
        size_t n = (size - done) < WRITE_CHUNK ? (size_t)(size - done) : WRITE_CHUNK;
        fill(buf, n, kind, done);
        if (write_all(fd, buf, n) != 0) {
            fprintf(stderr, "bench: write %s: %s\n", path, strerror(errno));
            close(fd);
            return -1;
        }
        done += n;
    }
    return close(fd);
}

static int generate(const opts_t *o, const char *src, tree_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    uint8_t *buf = malloc(WRITE_CHUNK);
    if (!buf) return -1;
    char path[4096];
    int rc = -1;

    if (bench_mkdir(src) != 0) goto out;

    snprintf(path, sizeof(path), "%s/small", src);
    if (bench_mkdir(path) != 0) goto out;
    for (uint64_t i = 0; i < o->small_files; i++) {
        if (i % FILES_PER_DIR == 0) {
            snprintf(path, sizeof(path), "%s/small/%05" PRIu64, src, i / FILES_PER_DIR);
            if (bench_mkdir(path) != 0) goto out;
        }
        /* Uniform in [mean/2, 3*mean/2), so the mean is what was asked for. */
        uint64_t size = o->small_bytes / 2 + (o->small_bytes ? xorshift() % o->small_bytes : 0);
        snprintf(path, sizeof(path), "%s/small/%05" PRIu64 "/f%07" PRIu64 ".dat",
                 src, i / FILES_PER_DIR, i);
        if (make_file(path, size, FILL_SMALL, buf) != 0) goto out;
        st->files++;
        st->bytes += size;
    }

    uint64_t lsize = o->large_mib << 20;
    if (o->large_files + o->text_files > 0) {
        snprintf(path, sizeof(path), "%s/large", src);
        if (bench_mkdir(path) != 0) goto out;
    }
    for (uint64_t i = 0; i < o->large_files; i++) {
        snprintf(path, sizeof(path), "%s/large/random_%02" PRIu64 ".bin", src, i);
        if (make_file(path, lsize, FILL_RANDOM, buf) != 0) goto out;
        st->files++;
        st->bytes += lsize;
    }
    for (uint64_t i = 0; i < o->text_files; i++) {
        snprintf(path, sizeof(path), "%s/large/text_%02" PRIu64 ".txt", src, i);
        if (make_file(path, lsize, FILL_TEXT, buf) != 0) goto out;
        st->files++;
        st->bytes += lsize;
    }
    rc = 0;
out:
    free(buf);
    return rc;
}

/* ---------- cold cache ---------- */

static int evict_one(const char *path, const struct stat *sb, int type, struct FTW *ftw)
{
    (void)sb; (void)ftw;
    if (type != FTW_F) return 0;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return 0;
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);
    return 0;
}

/* Clean pages only go, so sync first. Not as thorough as drop_caches, and
 * needs no root; it covers the source, which is what a first backup reads. */
static void evict(const char *src)
{
    sync();
    (void)nftw(src, evict_one, 32, FTW_PHYS | FTW_MOUNT);
}

/* ---------- reporting ---------- */

static void report(const char *name, double value, const char *unit, double elapsed)
{
    printf("  %-34s %12.3f %-10s (%.2fs)\n", name, value, unit, elapsed);
}

static void section(const char *what)
{
    printf("\n--- %s ---\n", what);
    fflush(stdout);
}

static double finish_timing(const opts_t *o, double t0)
{
    if (o->cold) sync();
    return bench_now() - t0;
}

/* ---------- the run ---------- */

static int timed_backup(const opts_t *o, bench_env_t *e, const char *src,
                        const char *label, backup_result_t *res)
{
    static const char *paths[1];
    paths[0] = src;
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.paths = paths;
    bo.n_paths = 1;
    bo.store_workers = o->workers;

    if (o->cold) evict(src);
    section(label);
    memset(res, 0, sizeof(*res));
    double t0 = bench_now();
    if (backup_run(e->store, e->db, e->repo_id, &bo, res) != OK) {
        fprintf(stderr, "bench: %s failed: %s\n", label, err_msg());
        return -1;
    }
    double tb = finish_timing(o, t0);

    double t1 = bench_now();
    uint32_t packed = 0;
    if (repo_pack(e->store, &packed, NULL, NULL) != OK) {
        fprintf(stderr, "bench: %s pack failed: %s\n", label, err_msg());
        return -1;
    }
    double tp = finish_timing(o, t1);

    double mib = (double)res->bytes_logical / (1024.0 * 1024.0);
    char name[96];
    printf("\n");
    snprintf(name, sizeof(name), "%s", label);
    report(name, tb, "s", tb);
    snprintf(name, sizeof(name), "%s files/s", label);
    report(name, (double)res->files / tb, "files/s", tb);
    snprintf(name, sizeof(name), "%s MiB/s", label);
    report(name, mib / tb, "MiB/s", tb);
    snprintf(name, sizeof(name), "%s pack", label);
    report(name, tp, "s", tp);
    snprintf(name, sizeof(name), "%s + pack MiB/s", label);
    report(name, mib / (tb + tp), "MiB/s", tb + tp);
    printf("      (%" PRIu64 " files, %" PRIu64 " dirs, %" PRIu64 " unchanged, "
           "%" PRIu64 " new objects, %" PRIu64 " new trees, %" PRIu64 " reused, "
           "%u packed, %u store workers)\n",
           res->files, res->dirs, res->files_unchanged, res->objects_new,
           res->trees_new, res->trees_reused, packed, res->store_workers);
    return 0;
}

static int timed_restore(const opts_t *o, bench_env_t *e, int64_t snap_id,
                         const char *dst, const tree_stats_t *want)
{
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));

    section("restore");
    double t0 = bench_now();
    status_t st = restore_run(e->store, e->db, e->repo_id, snap_id, dst, &ro, &rr);
    double t = finish_timing(o, t0);
    if (st != OK || rr.failed != 0 || rr.files != want->files) {
        fprintf(stderr, "bench: restore wrong: status %d, %" PRIu64 " of %" PRIu64
                " files, %" PRIu64 " failed: %s\n",
                (int)st, rr.files, want->files, rr.failed, err_msg());
        return -1;
    }
    double mib = (double)rr.bytes / (1024.0 * 1024.0);
    report("restore", t, "s", t);
    report("restore files/s", (double)rr.files / t, "files/s", t);
    report("restore MiB/s", mib / t, "MiB/s", t);
    printf("      (%" PRIu64 " files, %" PRIu64 " dirs, %.1f MiB)\n", rr.files, rr.dirs, mib);
    return 0;
}

static void usage(FILE *f)
{
    fprintf(f,
        "usage: bench_phases [-d DIR] [-n SMALL] [-s BYTES] [-l LARGE] [-t TEXT]\n"
        "                    [-m MIB] [-w N] [-p PRESET] [-c] [-k]\n"
        "  -d, --dir DIR        parent of the working directory (BENCH_DIR, /tmp)\n"
        "  -n, --small N        small files (BENCH_SMALL_FILES, 10000)\n"
        "  -s, --small-bytes B  mean small-file size (BENCH_SMALL_BYTES, 4096)\n"
        "  -l, --large N        incompressible large files (BENCH_LARGE_FILES, 3)\n"
        "  -t, --text N         identical compressible large files (BENCH_TEXT_FILES, 2)\n"
        "  -m, --large-mib M    MiB per large file (BENCH_LARGE_MIB, 64)\n"
        "  -w, --workers N      large-file store workers, 0 auto, 1 serial (BENCH_STORE_WORKERS, 0)\n"
        "  -p, --preset NAME    many-large: 8 x 2 GiB incompressible, no small files\n"
        "  -c, --cold           evict the source before each backup, sync in timings\n"
        "  -k, --keep           keep the working directory\n");
}

static void parse(int argc, char **argv, opts_t *o)
{
    const char *dir = getenv("BENCH_DIR");
    snprintf(o->parent, sizeof(o->parent), "%s", (dir && *dir) ? dir : "/tmp");
    o->small_files = bench_env_u64("BENCH_SMALL_FILES", 10000);
    o->small_bytes = bench_env_u64("BENCH_SMALL_BYTES", 4096);
    o->large_files = bench_env_u64("BENCH_LARGE_FILES", 3);
    o->text_files  = bench_env_u64("BENCH_TEXT_FILES", 2);
    o->large_mib   = bench_env_u64("BENCH_LARGE_MIB", 64);
    uint64_t w     = bench_env_u64("BENCH_STORE_WORKERS", 0);
    o->workers     = w > 64 ? 64u : (unsigned)w;

    static const struct option lo[] = {
        { "dir",         required_argument, NULL, 'd' },
        { "small",       required_argument, NULL, 'n' },
        { "small-bytes", required_argument, NULL, 's' },
        { "large",       required_argument, NULL, 'l' },
        { "text",        required_argument, NULL, 't' },
        { "large-mib",   required_argument, NULL, 'm' },
        { "workers",     required_argument, NULL, 'w' },
        { "preset",      required_argument, NULL, 'p' },
        { "cold",        no_argument,       NULL, 'c' },
        { "keep",        no_argument,       NULL, 'k' },
        { "help",        no_argument,       NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:n:s:l:t:m:w:p:ckh", lo, NULL)) != -1) {
        switch (c) {
        case 'd': snprintf(o->parent, sizeof(o->parent), "%s", optarg); break;
        case 'n': o->small_files = bench_parse_u64("--small", optarg); break;
        case 's': o->small_bytes = bench_parse_u64("--small-bytes", optarg); break;
        case 'l': o->large_files = bench_parse_u64("--large", optarg); break;
        case 't': o->text_files  = bench_parse_u64("--text", optarg); break;
        case 'm': o->large_mib   = bench_parse_u64("--large-mib", optarg); break;
        case 'w': {
            uint64_t n = bench_parse_u64("--workers", optarg);
            if (n > 64) { fprintf(stderr, "bench: --workers is 0 to 64\n"); exit(2); }
            o->workers = (unsigned)n;
            break;
        }
        case 'p':
            if (strcmp(optarg, "many-large") != 0) {
                fprintf(stderr, "bench: unknown preset '%s'\n", optarg);
                exit(2);
            }
            o->small_files = 0;
            o->large_files = 8;
            o->text_files  = 0;
            o->large_mib   = 2048;
            break;
        case 'c': o->cold = 1; break;
        case 'k': o->keep = 1; break;
        case 'h': usage(stdout); exit(0);
        default:  usage(stderr); exit(2);
        }
    }
    if (optind != argc) { usage(stderr); exit(2); }
    if (o->large_mib > (UINT64_MAX >> 21)) {
        fprintf(stderr, "bench: --large-mib too large\n");
        exit(2);
    }
    if (o->small_files + o->large_files + o->text_files == 0) {
        fprintf(stderr, "bench: nothing to back up\n");
        exit(2);
    }
    snprintf(o->work, sizeof(o->work), "%s/c_backup_bench_phases", o->parent);
}

int main(int argc, char **argv)
{
    opts_t o;
    memset(&o, 0, sizeof(o));
    parse(argc, argv, &o);

    char src[3300], dst[3300];
    snprintf(src, sizeof(src), "%s/src", o.work);
    snprintf(dst, sizeof(dst), "%s/restore", o.work);

    if (bench_rm_tree(o.work) != 0 || bench_mkdir(o.work) != 0) return 1;

    printf("bench_phases: %s\n", o.work);
    printf("  %" PRIu64 " small files of ~%" PRIu64 " B, %" PRIu64 " random + %" PRIu64
           " identical text files of %" PRIu64 " MiB, store workers %u%s%s\n",
           o.small_files, o.small_bytes, o.large_files, o.text_files, o.large_mib,
           o.workers, o.workers ? "" : " (auto)", o.cold ? ", cold" : "");

    tree_stats_t ts;
    double t0 = bench_now();
    if (generate(&o, src, &ts) != 0) { bench_rm_tree(o.work); return 1; }
    printf("  generated %" PRIu64 " files, %.1f MiB in %.2fs\n",
           ts.files, (double)ts.bytes / (1024.0 * 1024.0), bench_now() - t0);

    int rc = 1;
    bench_env_t e;
    if (bench_env_open(&e, o.work) != 0) goto out;

    backup_result_t first, incr;
    if (timed_backup(&o, &e, src, "first backup", &first) != 0) goto out;
    if (timed_backup(&o, &e, src, "incremental, no change", &incr) != 0) goto out;
    if (incr.files_unchanged != incr.files)
        printf("  warning: %" PRIu64 " of %" PRIu64 " files were re-read by the incremental\n",
               incr.files - incr.files_unchanged, incr.files);
    if (timed_restore(&o, &e, incr.snapshot_id, dst, &ts) != 0) goto out;
    rc = 0;

out:
    bench_env_close(&e);
    if (o.keep) printf("\nkept %s\n", o.work);
    else if (bench_rm_tree(o.work) != 0) rc = 1;
    return rc;
}
