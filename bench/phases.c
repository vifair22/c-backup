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
 * The backup job packs on completion, inside backup_run (pack_after), so the
 * backup's time, profile and write counters all include that pack pass, as
 * the daemon's do. A second, separate pack follows and is timed on its own;
 * after the first it should find nothing to do. backup_run logs its own
 * per-phase profile (phase_timer.h) at info level, and that log is left on
 * here: it is the same breakdown the daemon writes on a deployment, which is
 * the point of printing it.
 *
 * Each backup also prints what it wrote by category (backup_result_t
 * written) and the process's write_bytes from /proc/self/io over the same
 * span, which includes the database and anything the counters miss.
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
 *     -T, --text-unique N  distinct compressible large files    (BENCH_UTEXT_FILES, 0)
 *     -H, --head-text N    large files with a 1 MiB compressible head and a
 *                          random body, as media files often are; the pack
 *                          prober reads only the head  (BENCH_HEAD_FILES, 0)
 *     -m, --large-mib M    size of each large file, MiB         (BENCH_LARGE_MIB, 64)
 *     -w, --workers N      large-file store workers, 0 automatic, 1 serial
 *                                                          (BENCH_STORE_WORKERS, 0)
 *     -p, --preset NAME    many-large: 8 incompressible files of 2 GiB and
 *                          nothing else, the B086 case; later options still
 *                          override it
 *     -D, --per-dir N      small files per directory            (BENCH_PER_DIR, 100)
 *     -j, --job-progress   write progress to a jobs row the way the daemon's
 *                          job manager does, at most every 250 ms
 *                                                          (BENCH_JOB_PROGRESS, 0)
 *     -c, --cold           sync and evict the source from the page cache before
 *                          each backup, and include a trailing sync in every
 *                          timing; only meaningful on a real filesystem
 *     -k, --keep           leave the working directory behind
 *     -S, --source DIR     back up DIR instead of generating a tree; it is
 *                          read, never written or removed. With --keep, a
 *                          first run's DIR/c_backup_bench_phases/src can be
 *                          reused, so runs under different conditions back
 *                          up the same bytes and `cp` can copy them too
 *     -R, --no-restore     skip the restore step
 *
 * The working directory is DIR/c_backup_bench_phases and is removed at the
 * end unless --keep. On tmpfs (the default /tmp here) every number is a CPU
 * number; put DIR on the target filesystem to measure storage.
 */

#define _GNU_SOURCE
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>

#include "bench_common.h"

#include "backup.h"
#include "pack.h"
#include "restore.h"

#define WRITE_CHUNK   (1u << 20)

typedef struct {
    char     parent[3072];
    char     work[3200];
    uint64_t small_files;
    uint64_t small_bytes;
    uint64_t large_files;
    uint64_t text_files;
    uint64_t utext_files;
    uint64_t head_files;
    uint64_t large_mib;
    unsigned workers;
    uint64_t per_dir;
    int      job_progress;
    int      cold;
    int      keep;
    int      no_restore;
    char     source[3072];   /* back up this tree instead of generating one */
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

typedef enum { FILL_SMALL, FILL_RANDOM, FILL_TEXT, FILL_HEAD } fill_t;

/* How much of a FILL_HEAD file is compressible: more than the pack prober's
 * 64 KiB window, so the prober calls the file compressible. */
#define HEAD_TEXT_BYTES ((uint64_t)1 << 20)

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
    case FILL_HEAD:
        /* A media file's shape: a compressible header (container tables,
         * metadata, a thumbnail) in front of an incompressible stream. The
         * first chunk is text, every later one random. */
        fill(buf, n, offset < HEAD_TEXT_BYTES ? FILL_TEXT : FILL_RANDOM, offset);
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

/* base shifts FILL_TEXT's pattern, so files with different bases are
 * distinct objects that compress exactly as well as the identical ones. */
static int make_file_at(const char *path, uint64_t size, fill_t kind, uint8_t *buf,
                        uint64_t base)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "bench: create %s: %s\n", path, strerror(errno));
        return -1;
    }
    uint64_t done = 0;
    while (done < size) {
        size_t n = (size - done) < WRITE_CHUNK ? (size_t)(size - done) : WRITE_CHUNK;
        fill(buf, n, kind, base + done);
        if (write_all(fd, buf, n) != 0) {
            fprintf(stderr, "bench: write %s: %s\n", path, strerror(errno));
            close(fd);
            return -1;
        }
        done += n;
    }
    return close(fd);
}

static int make_file(const char *path, uint64_t size, fill_t kind, uint8_t *buf)
{
    return make_file_at(path, size, kind, buf, 0);
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
        if (i % o->per_dir == 0) {
            snprintf(path, sizeof(path), "%s/small/%05" PRIu64, src, i / o->per_dir);
            if (bench_mkdir(path) != 0) goto out;
        }
        /* Uniform in [mean/2, 3*mean/2), so the mean is what was asked for. */
        uint64_t size = o->small_bytes / 2 + (o->small_bytes ? xorshift() % o->small_bytes : 0);
        snprintf(path, sizeof(path), "%s/small/%05" PRIu64 "/f%07" PRIu64 ".dat",
                 src, i / o->per_dir, i);
        if (make_file(path, size, FILL_SMALL, buf) != 0) goto out;
        st->files++;
        st->bytes += size;
    }

    uint64_t lsize = o->large_mib << 20;
    if (o->large_files + o->text_files + o->utext_files + o->head_files > 0) {
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
    for (uint64_t i = 0; i < o->utext_files; i++) {
        snprintf(path, sizeof(path), "%s/large/utext_%02" PRIu64 ".txt", src, i);
        if (make_file_at(path, lsize, FILL_TEXT, buf, (i + 1) * 1000003u) != 0) goto out;
        st->files++;
        st->bytes += lsize;
    }
    for (uint64_t i = 0; i < o->head_files; i++) {
        snprintf(path, sizeof(path), "%s/large/media_%02" PRIu64 ".bin", src, i);
        if (make_file(path, lsize, FILL_HEAD, buf) != 0) goto out;
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

/* Bytes this process caused to be sent to storage, or 0 where the kernel
 * does not say (no task I/O accounting). Includes state.db and its WAL. */
static uint64_t proc_write_bytes(void)
{
    FILE *f = fopen("/proc/self/io", "r");
    if (!f) return 0;
    char line[128];
    uint64_t v = 0;
    while (fgets(line, sizeof(line), f))
        if (sscanf(line, "write_bytes: %" SCNu64, &v) == 1) break;
    fclose(f);
    return v;
}

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

/* ---------- job progress ---------- */

/*
 * What the daemon's job manager does with a backup's progress (on_progress
 * in job_manager.c): an UPDATE of the job's row at most every 250 ms and on
 * every phase change, on the backup's own thread, through the shared handle.
 * Off by default so earlier PERF.md entries stay comparable; on, it puts the
 * daemon's progress writes back into the measurement. A write issued while
 * the backup has no batch open is a commit of its own, and on slow storage
 * a sync of its own (OWN-16).
 */
typedef struct {
    cutils_db_t *db;
    char         job_id[32];
    int64_t      last_ms;
    char         last_phase[32];
    uint64_t     bytes_done, bytes_total;
    uint64_t     writes;
} job_progress_t;

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void job_progress_cb(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    job_progress_t *jp = ctx;
    const char *ph = phase ? phase : "";
    int64_t now = mono_ms();
    int changed  = strncmp(jp->last_phase, ph, sizeof(jp->last_phase) - 1) != 0;
    int finished = total > 0 && done >= total;
    if (!changed && !finished && now - jp->last_ms < 250) return;
    jp->last_ms = now;
    snprintf(jp->last_phase, sizeof(jp->last_phase), "%s", ph);

    char d[32], t[32], bd[32], bt[32];
    snprintf(d,  sizeof(d),  "%" PRIu64, done);
    snprintf(t,  sizeof(t),  "%" PRIu64, total);
    snprintf(bd, sizeof(bd), "%" PRIu64, jp->bytes_done);
    snprintf(bt, sizeof(bt), "%" PRIu64, jp->bytes_total);
    /* Best effort, as in the daemon: a lost tick never fails the backup. */
    if (db_execute_non_query(jp->db,
        "UPDATE jobs SET progress_phase = ?, progress_current = ?,"
        " progress_total = ?, bytes_done = ?, bytes_total = ? WHERE id = ?",
        (const char *[]){ ph, d, t, bd, bt, jp->job_id, NULL }, NULL) == 0)
        jp->writes++;
}

static void job_bytes_cb(void *ctx, uint64_t done, uint64_t total)
{
    job_progress_t *jp = ctx;
    jp->bytes_done  = done;
    jp->bytes_total = total;
}

static int job_progress_open(job_progress_t *jp, bench_env_t *e)
{
    memset(jp, 0, sizeof(*jp));
    jp->db = e->db;
    db_result_t *r = NULL;
    int rc = db_execute(e->db,
        "INSERT INTO jobs (repo_id, action, params, state)"
        " VALUES (?, 'backup', '{}', 'running') RETURNING id",
        (const char *[]){ e->repo_id_s, NULL }, &r);
    if (rc == 0 && r && r->nrows == 1)
        snprintf(jp->job_id, sizeof(jp->job_id), "%s", r->rows[0][0]);
    db_result_free(r);
    if (!jp->job_id[0]) {
        fprintf(stderr, "bench: cannot create a jobs row\n");
        return -1;
    }
    return 0;
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
    bo.pack_after = 1;
    job_progress_t jp;
    memset(&jp, 0, sizeof(jp));
    if (o->job_progress) {
        if (job_progress_open(&jp, e) != 0) return -1;
        bo.progress_cb  = job_progress_cb;
        bo.bytes_cb     = job_bytes_cb;
        bo.progress_ctx = &jp;
    }

    if (o->cold) evict(src);
    section(label);
    memset(res, 0, sizeof(*res));
    uint64_t wb0 = proc_write_bytes();
    double t0 = bench_now();
    if (backup_run(e->store, e->db, e->repo_id, &bo, res) != OK) {
        fprintf(stderr, "bench: %s failed: %s\n", label, err_msg());
        return -1;
    }
    double tb = finish_timing(o, t0);
    uint64_t wb1 = proc_write_bytes();

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
    if (o->job_progress)
        printf("      (%" PRIu64 " job progress writes)\n", jp.writes);

    const uint64_t *w = res->written.bytes;
    double lb = res->bytes_logical ? (double)res->bytes_logical : 1.0;
    printf("      written %.1f MiB = %.3f x content: loose %.1f + parity %.1f, "
           "packs %.1f + parity %.1f, spill %.1f, discarded %.1f, abandoned %.1f; "
           "of which pack pass %.1f (%" PRIu64 " packed)\n",
           (double)res->written_total / 1048576.0, (double)res->written_total / lb,
           (double)w[STORE_WR_LOOSE] / 1048576.0, (double)w[STORE_WR_LOOSE_PARITY] / 1048576.0,
           (double)w[STORE_WR_PACK] / 1048576.0, (double)w[STORE_WR_PACK_PARITY] / 1048576.0,
           (double)w[STORE_WR_PARITY_SPILL] / 1048576.0,
           (double)w[STORE_WR_LOOSE_DISCARD] / 1048576.0,
           (double)w[STORE_WR_PACK_ABANDONED] / 1048576.0,
           (double)res->written_sweep / 1048576.0, res->sweep_packed);
    if (wb1 >= wb0 && wb1)
        printf("      /proc/self/io write_bytes %.1f MiB = %.3f x content "
               "(database and everything else included)\n",
               (double)(wb1 - wb0) / 1048576.0, (double)(wb1 - wb0) / lb);
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
        "                    [-T TEXT] [-H N] [-m MIB] [-w N] [-p PRESET] [-D N] [-j] [-c] [-k]\n"
        "  -d, --dir DIR        parent of the working directory (BENCH_DIR, /tmp)\n"
        "  -n, --small N        small files (BENCH_SMALL_FILES, 10000)\n"
        "  -s, --small-bytes B  mean small-file size (BENCH_SMALL_BYTES, 4096)\n"
        "  -l, --large N        incompressible large files (BENCH_LARGE_FILES, 3)\n"
        "  -t, --text N         identical compressible large files (BENCH_TEXT_FILES, 2)\n"
        "  -T, --text-unique N  distinct compressible large files (BENCH_UTEXT_FILES, 0)\n"
        "  -H, --head-text N    large files, compressible 1 MiB head, random body (BENCH_HEAD_FILES, 0)\n"
        "  -m, --large-mib M    MiB per large file (BENCH_LARGE_MIB, 64)\n"
        "  -w, --workers N      large-file store workers, 0 auto, 1 serial (BENCH_STORE_WORKERS, 0)\n"
        "  -p, --preset NAME    many-large: 8 x 2 GiB incompressible, no small files\n"
        "  -D, --per-dir N      small files per directory (BENCH_PER_DIR, 100)\n"
        "  -j, --job-progress   write job progress like the daemon (BENCH_JOB_PROGRESS, 0)\n"
        "  -c, --cold           evict the source before each backup, sync in timings\n"
        "  -k, --keep           keep the working directory\n"
        "  -S, --source DIR     back up DIR instead of generating a tree\n"
        "  -R, --no-restore     skip the restore step\n");
}

static void parse(int argc, char **argv, opts_t *o)
{
    const char *dir = getenv("BENCH_DIR");
    snprintf(o->parent, sizeof(o->parent), "%s", (dir && *dir) ? dir : "/tmp");
    o->small_files = bench_env_u64("BENCH_SMALL_FILES", 10000);
    o->small_bytes = bench_env_u64("BENCH_SMALL_BYTES", 4096);
    o->large_files = bench_env_u64("BENCH_LARGE_FILES", 3);
    o->text_files  = bench_env_u64("BENCH_TEXT_FILES", 2);
    o->utext_files = bench_env_u64("BENCH_UTEXT_FILES", 0);
    o->head_files  = bench_env_u64("BENCH_HEAD_FILES", 0);
    o->large_mib   = bench_env_u64("BENCH_LARGE_MIB", 64);
    uint64_t w     = bench_env_u64("BENCH_STORE_WORKERS", 0);
    o->workers     = w > 64 ? 64u : (unsigned)w;
    o->per_dir     = bench_env_u64("BENCH_PER_DIR", 100);
    o->job_progress = bench_env_u64("BENCH_JOB_PROGRESS", 0) != 0;

    static const struct option lo[] = {
        { "dir",         required_argument, NULL, 'd' },
        { "small",       required_argument, NULL, 'n' },
        { "small-bytes", required_argument, NULL, 's' },
        { "large",       required_argument, NULL, 'l' },
        { "text",        required_argument, NULL, 't' },
        { "text-unique", required_argument, NULL, 'T' },
        { "head-text",   required_argument, NULL, 'H' },
        { "large-mib",   required_argument, NULL, 'm' },
        { "workers",     required_argument, NULL, 'w' },
        { "preset",      required_argument, NULL, 'p' },
        { "per-dir",     required_argument, NULL, 'D' },
        { "job-progress", no_argument,      NULL, 'j' },
        { "cold",        no_argument,       NULL, 'c' },
        { "keep",        no_argument,       NULL, 'k' },
        { "source",      required_argument, NULL, 'S' },
        { "no-restore",  no_argument,       NULL, 'R' },
        { "help",        no_argument,       NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:n:s:l:t:T:H:m:w:p:D:jckS:Rh", lo, NULL)) != -1) {
        switch (c) {
        case 'd': snprintf(o->parent, sizeof(o->parent), "%s", optarg); break;
        case 'n': o->small_files = bench_parse_u64("--small", optarg); break;
        case 's': o->small_bytes = bench_parse_u64("--small-bytes", optarg); break;
        case 'l': o->large_files = bench_parse_u64("--large", optarg); break;
        case 't': o->text_files  = bench_parse_u64("--text", optarg); break;
        case 'T': o->utext_files = bench_parse_u64("--text-unique", optarg); break;
        case 'H': o->head_files  = bench_parse_u64("--head-text", optarg); break;
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
            o->utext_files = 0;
            o->head_files  = 0;
            o->large_mib   = 2048;
            break;
        case 'D': o->per_dir = bench_parse_u64("--per-dir", optarg); break;
        case 'j': o->job_progress = 1; break;
        case 'c': o->cold = 1; break;
        case 'k': o->keep = 1; break;
        case 'S': snprintf(o->source, sizeof(o->source), "%s", optarg); break;
        case 'R': o->no_restore = 1; break;
        case 'h': usage(stdout); exit(0);
        default:  usage(stderr); exit(2);
        }
    }
    if (optind != argc) { usage(stderr); exit(2); }
    if (o->per_dir == 0) {
        fprintf(stderr, "bench: --per-dir must be at least 1\n");
        exit(2);
    }
    if (o->large_mib > (UINT64_MAX >> 21)) {
        fprintf(stderr, "bench: --large-mib too large\n");
        exit(2);
    }
    if (!o->source[0] && o->small_files + o->large_files + o->text_files
        + o->utext_files + o->head_files == 0) {
        fprintf(stderr, "bench: nothing to back up\n");
        exit(2);
    }
    snprintf(o->work, sizeof(o->work), "%s/c_backup_bench_phases", o->parent);
}

static tree_stats_t g_walk;
static int count_one(const char *path, const struct stat *sb, int type, struct FTW *ftw)
{
    (void)path; (void)ftw;
    if (type == FTW_F && S_ISREG(sb->st_mode)) {
        g_walk.files++;
        g_walk.bytes += (uint64_t)sb->st_size;
    }
    return 0;
}

int main(int argc, char **argv)
{
    opts_t o;
    memset(&o, 0, sizeof(o));
    parse(argc, argv, &o);

    char src[3300], dst[3300];
    if (o.source[0]) snprintf(src, sizeof(src), "%s", o.source);
    else snprintf(src, sizeof(src), "%s/src", o.work);
    snprintf(dst, sizeof(dst), "%s/restore", o.work);

    /* The working directory is emptied first, so a source inside it would
     * be deleted before it was read. */
    if (o.source[0]) {
        char rs[PATH_MAX], rp[PATH_MAX];
        if (!realpath(o.source, rs)) {
            fprintf(stderr, "bench: --source %s: %s\n", o.source, strerror(errno));
            return 2;
        }
        size_t n = strlen(o.work);
        if (realpath(o.parent, rp)) {
            snprintf(o.work, sizeof(o.work), "%.3000s/c_backup_bench_phases", rp);
            n = strlen(o.work);
        }
        if (strncmp(rs, o.work, n) == 0 && (rs[n] == '/' || rs[n] == '\0')) {
            fprintf(stderr, "bench: --source must not be inside %s, which is "
                    "removed before the run\n", o.work);
            return 2;
        }
    }
    if (bench_rm_tree(o.work) != 0 || bench_mkdir(o.work) != 0) return 1;

    printf("bench_phases: %s\n", o.work);
    if (o.source[0]) printf("  source %s (not generated)\n", o.source);
    else printf("  %" PRIu64 " small files of ~%" PRIu64 " B, %" PRIu64 " random + %" PRIu64
           " identical text + %" PRIu64 " distinct text + %" PRIu64
           " head-text files of %" PRIu64 " MiB, store workers %u%s%s\n",
           o.small_files, o.small_bytes, o.large_files, o.text_files, o.utext_files,
           o.head_files, o.large_mib,
           o.workers, o.workers ? "" : " (auto)", o.cold ? ", cold" : "");

    tree_stats_t ts;
    double t0 = bench_now();
    if (o.source[0]) {
        memset(&g_walk, 0, sizeof(g_walk));
        if (nftw(src, count_one, 32, FTW_PHYS) != 0) {
            fprintf(stderr, "bench: cannot walk %s: %s\n", src, strerror(errno));
            bench_rm_tree(o.work);
            return 1;
        }
        ts = g_walk;
        printf("  source holds %" PRIu64 " files, %.1f MiB\n",
               ts.files, (double)ts.bytes / (1024.0 * 1024.0));
    } else {
        if (generate(&o, src, &ts) != 0) { bench_rm_tree(o.work); return 1; }
        printf("  generated %" PRIu64 " files, %.1f MiB in %.2fs\n",
               ts.files, (double)ts.bytes / (1024.0 * 1024.0), bench_now() - t0);
    }

    int rc = 1;
    bench_env_t e;
    if (bench_env_open(&e, o.work) != 0) goto out;

    backup_result_t first, incr;
    if (timed_backup(&o, &e, src, "first backup", &first) != 0) goto out;
    if (timed_backup(&o, &e, src, "incremental, no change", &incr) != 0) goto out;
    if (incr.files_unchanged != incr.files)
        printf("  warning: %" PRIu64 " of %" PRIu64 " files were re-read by the incremental\n",
               incr.files - incr.files_unchanged, incr.files);
    if (!o.no_restore && timed_restore(&o, &e, incr.snapshot_id, dst, &ts) != 0) goto out;
    rc = 0;

out:
    bench_env_close(&e);
    if (o.keep) printf("\nkept %s\n", o.work);
    else if (bench_rm_tree(o.work) != 0) rc = 1;
    return rc;
}
