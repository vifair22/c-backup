/*
 * LD_PRELOAD shim: bytes and syncs per destination, for the first-backup
 * investigation (PERF.md, 2026-11-19). Counts the bytes given to
 * write/pwrite/writev/pwritev/fwrite and every fsync, fdatasync,
 * sync_file_range, syncfs, open, rename, unlink and ftruncate, by the
 * class of the file the descriptor names (state.db, its -wal and -shm,
 * packs/.staging, packs, the repository's tmp/ and snapshots/, the
 * source). fsync and fdatasync also keep a short backtrace per call site
 * (bench/io_sites.py names them). Every WAL frame header written is
 * decoded, so the dump has the WAL frames per database page; joined with
 * dbstat (bench/first_backup_model.sh does) that is the WAL per b-tree.
 *
 * Optionally a read device model: reads from the source are paced as if
 * each stream could do IOC_STREAM_MBS and the device IOC_DEV_MBS in all,
 * so a tmpfs source behaves like a slower disk without loading one.
 *
 *   IOC_DIR          directory; touch IOC_DIR/dump to write counters.N
 *                    and reset (required, nothing is dumped without it)
 *   IOC_SRC          path prefix of the source (default /usr/include)
 *   IOC_STREAM_MBS   per-stream read rate in MB/s (unset: no pacing)
 *   IOC_DEV_MBS      device read rate in MB/s (unset: no device limit)
 *
 * Only calls through the PLT are seen; that includes SQLite's. A
 * descriptor opened some other way (openat2, dup) is classified from
 * /proc/self/fd on first use. Repository paths are recognised by a
 * directory named repo, as bench/first_backup_model.sh lays them out.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

enum { K_OTHER, K_DB, K_WAL, K_SHM, K_STAGING, K_PACK, K_REPO_TMP, K_SNAP, K_REPO_OTHER, K_SRC, K_NCLS };
static const char *kname[K_NCLS] = { "other", "state.db", "state.db-wal", "state.db-shm", "packs/.staging", "packs", "repo/tmp", "snapshots", "repo-other", "source" };

enum { P_WR, P_WRB, P_RD, P_RDB, P_FSYNC, P_FDSYNC, P_SFR, P_SYNCFS, P_OPEN, P_RENAME, P_UNLINK, P_FTRUNC, P_NOPS };
static const char *oname[P_NOPS] = { "write_calls", "write_bytes", "read_calls", "read_bytes", "fsync", "fdatasync", "sync_file_range", "syncfs", "open", "rename", "unlink", "ftruncate" };

static _Atomic uint64_t ctr[K_NCLS][P_NOPS];
static _Atomic uint64_t sync_ns[K_NCLS];
#define MAXFD 65536
static unsigned char fdk[MAXFD];

/* backtrace sites for fsync/fdatasync */
#define NSITE 256
#define DEPTH 8
static struct { void *f[DEPTH]; int n; int k; int op; uint64_t count; uint64_t ns; } site[NSITE];
static int nsite;
static pthread_mutex_t site_mu = PTHREAD_MUTEX_INITIALIZER;
static char ioc_dir[512];
#define MAXPG (1u<<22)
static _Atomic uint32_t walpg[MAXPG];
static _Atomic uint64_t walcommits;

static int dumpno;

static int classify(const char *p)
{
    if (!p) return K_OTHER;
    size_t n = strlen(p);
    if (n >= 8 && strstr(p, "state.db")) {
        if (n >= 4 && strcmp(p + n - 4, "-wal") == 0) return K_WAL;
        if (n >= 4 && strcmp(p + n - 4, "-shm") == 0) return K_SHM;
        return K_DB;
    }
    if (strstr(p, "/packs/.staging")) return K_STAGING;
    if (strstr(p, "/packs/")) return K_PACK;
    if (strstr(p, "/repo/tmp")) return K_REPO_TMP;
    if (strstr(p, "/repo/snapshots")) return K_SNAP;
    if (strstr(p, "/repo/")) return K_REPO_OTHER;
    { static const char *sp; static size_t spn; if (!sp) { sp = getenv("IOC_SRC"); if (!sp) sp = "/usr/include"; spn = strlen(sp); }
      if (strncmp(p, sp, spn) == 0) return K_SRC; }
    return K_OTHER;
}

static int kfd(int fd)
{
    if (fd < 0 || fd >= MAXFD) return K_OTHER;
    if (fdk[fd] == 255) {
        char lp[64], dp[4096]; snprintf(lp, sizeof lp, "/proc/self/fd/%d", fd);
        ssize_t r = readlink(lp, dp, sizeof dp - 1);
        if (r > 0) { dp[r] = 0; fdk[fd] = (unsigned char)classify(dp); } else fdk[fd] = K_OTHER;
    }
    return fdk[fd];
}
static inline void setfd(int fd, const char *p, int dfd)
{
    if (fd < 0 || fd >= MAXFD) return;
    if (p && p[0] != '/' && dfd >= 0) {
        char lp[64], dp[4096]; snprintf(lp, sizeof lp, "/proc/self/fd/%d", dfd);
        ssize_t r = readlink(lp, dp, sizeof dp - 1);
        if (r > 0) { dp[r] = 0; char full[8192]; snprintf(full, sizeof full, "%s/%s", dp, p); fdk[fd] = (unsigned char)classify(full); return; }
    }
    fdk[fd] = (unsigned char)classify(p);
}
static inline uint64_t now(void);
static double g_stream_bps, g_dev_bps; static _Atomic uint64_t g_dev_free_ns;
static void pace(size_t n);
static inline uint64_t now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec; }
#define ADD(k, o, v) atomic_fetch_add_explicit(&ctr[k][o], (uint64_t)(v), memory_order_relaxed)

static void record_site(int k, int op, uint64_t ns)
{
    void *f[DEPTH + 1]; int n = backtrace(f, DEPTH + 1);
    pthread_mutex_lock(&site_mu);
    for (int i = 0; i < nsite; i++) {
        if (site[i].k == k && site[i].op == op && site[i].n == n - 1 && memcmp(site[i].f, f + 1, sizeof(void *) * (size_t)(n - 1)) == 0) {
            site[i].count++; site[i].ns += ns; pthread_mutex_unlock(&site_mu); return; }
    }
    if (nsite < NSITE) { memcpy(site[nsite].f, f + 1, sizeof(void *) * (size_t)(n - 1)); site[nsite].n = n - 1; site[nsite].k = k; site[nsite].op = op; site[nsite].count = 1; site[nsite].ns = ns; nsite++; }
    pthread_mutex_unlock(&site_mu);
}

static void dump(void)
{
    char p[600]; snprintf(p, sizeof p, "%s/counters.%d", ioc_dir, dumpno++);
    static int (*ro)(const char *, int, ...);
    if (!ro) *(void **)(&ro) = dlsym(RTLD_NEXT, "open");
    int fd = ro(p, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    FILE *f = fdopen(fd, "w");
    for (int k = 0; k < K_NCLS; k++) {
        int any = 0; for (int o = 0; o < P_NOPS; o++) if (ctr[k][o]) any = 1;
        if (!any) continue;
        fprintf(f, "class %s", kname[k]);
        for (int o = 0; o < P_NOPS; o++) { fprintf(f, " %s=%lu", oname[o], (unsigned long)atomic_exchange(&ctr[k][o], 0)); }
        fprintf(f, " sync_ms=%.1f\n", (double)atomic_exchange(&sync_ns[k], 0) / 1e6);
    }
    pthread_mutex_lock(&site_mu);
    for (int i = 0; i < nsite; i++) {
        fprintf(f, "site %s %s count=%lu ms=%.1f :", kname[site[i].k], oname[site[i].op], (unsigned long)site[i].count, (double)site[i].ns / 1e6);
        for (int j = 0; j < site[i].n; j++) fprintf(f, " %p", site[i].f[j]);
        fprintf(f, "\n");
    }
    nsite = 0;
    pthread_mutex_unlock(&site_mu);
    fprintf(f, "walcommits %lu\n", (unsigned long)atomic_exchange(&walcommits, 0));
    for (uint32_t i = 0; i < MAXPG; i++) { uint32_t c = atomic_exchange(&walpg[i], 0); if (c) fprintf(f, "walpg %u %u\n", i, c); }
    /* maps for symbolising */
    FILE *m = fopen("/proc/self/maps", "r");
    if (m) { char line[1024]; while (fgets(line, sizeof line, m)) if (strstr(line, "r-xp") || strstr(line, " r--p ")) fprintf(f, "map %s", line); fclose(m); }
    fclose(f);
}

static void *poller(void *a)
{
    (void)a;
    char rq[600]; snprintf(rq, sizeof rq, "%s/dump", ioc_dir);
    for (;;) {
        struct timespec ts = { 0, 200000000 }; nanosleep(&ts, NULL);
        if (access(rq, F_OK) == 0) { unlink(rq); dump(); }
    }
    return NULL;
}

static void pace(size_t n)
{
    if (g_stream_bps <= 0 || n == 0) return;
    uint64_t t = now();
    /* the device: one timeline at g_dev_bps shared by every reader */
    uint64_t cost_dev = (uint64_t)((double)n / g_dev_bps * 1e9);
    uint64_t cur = atomic_load(&g_dev_free_ns), start, end;
    do { start = cur > t ? cur : t; end = start + cost_dev; } while (!atomic_compare_exchange_weak(&g_dev_free_ns, &cur, end));
    /* a single stream cannot go faster than g_stream_bps */
    uint64_t done = t + (uint64_t)((double)n / g_stream_bps * 1e9);
    if (end > done) done = end;
    uint64_t now2 = now();
    if (done > now2) { struct timespec ts = { (time_t)((done - now2) / 1000000000ull), (long)((done - now2) % 1000000000ull) }; nanosleep(&ts, NULL); }
}

__attribute__((constructor)) static void init(void)
{
    { const char *a = getenv("IOC_STREAM_MBS"), *b = getenv("IOC_DEV_MBS"); if (a) g_stream_bps = atof(a) * 1e6; if (b) g_dev_bps = atof(b) * 1e6; if (g_dev_bps <= 0) g_dev_bps = 1e12; }
    memset(fdk, 255, sizeof fdk);
    const char *d = getenv("IOC_DIR");
    if (!d) return;
    snprintf(ioc_dir, sizeof ioc_dir, "%s", d);
    pthread_t t; pthread_create(&t, NULL, poller, NULL); pthread_detach(t);
}

#define REAL(name) static __typeof__(name) *real_##name; if (!real_##name) *(void **)(&real_##name) = dlsym(RTLD_NEXT, #name)

int open(const char *p, int fl, ...)
{
    static int (*r)(const char *, int, ...); if (!r) *(void **)(&r) = dlsym(RTLD_NEXT, "open");
    mode_t m = 0; if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    int fd = r(p, fl, m); setfd(fd, p, -1); if (fd >= 0) ADD(kfd(fd), P_OPEN, 1); return fd;
}
int open64(const char *p, int fl, ...)
{
    static int (*r)(const char *, int, ...); if (!r) *(void **)(&r) = dlsym(RTLD_NEXT, "open64");
    mode_t m = 0; if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    int fd = r(p, fl, m); setfd(fd, p, -1); if (fd >= 0) ADD(kfd(fd), P_OPEN, 1); return fd;
}
int openat(int d, const char *p, int fl, ...)
{
    static int (*r)(int, const char *, int, ...); if (!r) *(void **)(&r) = dlsym(RTLD_NEXT, "openat");
    mode_t m = 0; if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    int fd = r(d, p, fl, m); setfd(fd, p, d); if (fd >= 0) ADD(kfd(fd), P_OPEN, 1); return fd;
}
int openat64(int d, const char *p, int fl, ...)
{
    static int (*r)(int, const char *, int, ...); if (!r) *(void **)(&r) = dlsym(RTLD_NEXT, "openat64");
    mode_t m = 0; if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    int fd = r(d, p, fl, m); setfd(fd, p, d); if (fd >= 0) ADD(kfd(fd), P_OPEN, 1); return fd;
}
int mkstemp(char *t) { REAL(mkstemp); int fd = real_mkstemp(t); setfd(fd, t, -1); if (fd >= 0) ADD(kfd(fd), P_OPEN, 1); return fd; }
int mkstemp64(char *t) { REAL(mkstemp64); int fd = real_mkstemp64(t); setfd(fd, t, -1); if (fd >= 0) ADD(kfd(fd), P_OPEN, 1); return fd; }
FILE *fopen(const char *p, const char *m) { REAL(fopen); FILE *f = real_fopen(p, m); if (f) { setfd(fileno(f), p, -1); ADD(kfd(fileno(f)), P_OPEN, 1); } return f; }
FILE *fopen64(const char *p, const char *m) { REAL(fopen64); FILE *f = real_fopen64(p, m); if (f) { setfd(fileno(f), p, -1); ADD(kfd(fileno(f)), P_OPEN, 1); } return f; }

ssize_t write(int fd, const void *b, size_t n) { REAL(write); ssize_t r = real_write(fd, b, n); int k = kfd(fd); ADD(k, P_WR, 1); if (r > 0) ADD(k, P_WRB, r); return r; }
ssize_t pwrite(int fd, const void *b, size_t n, off_t o) { REAL(pwrite); ssize_t r = real_pwrite(fd, b, n, o); int k = kfd(fd); if (k == K_WAL && n == 24) { const unsigned char *u = b; uint32_t pg = ((uint32_t)u[0]<<24)|((uint32_t)u[1]<<16)|((uint32_t)u[2]<<8)|u[3]; uint32_t c = ((uint32_t)u[4]<<24)|((uint32_t)u[5]<<16)|((uint32_t)u[6]<<8)|u[7]; if (pg < MAXPG) atomic_fetch_add(&walpg[pg], 1); if (c) atomic_fetch_add(&walcommits, 1); } ADD(k, P_WR, 1); if (r > 0) ADD(k, P_WRB, r); return r; }
ssize_t pwrite64(int fd, const void *b, size_t n, off_t o) { REAL(pwrite64); ssize_t r = real_pwrite64(fd, b, n, o); int k = kfd(fd); if (k == K_WAL && n == 24) { const unsigned char *u = b; uint32_t pg = ((uint32_t)u[0]<<24)|((uint32_t)u[1]<<16)|((uint32_t)u[2]<<8)|u[3]; uint32_t c = ((uint32_t)u[4]<<24)|((uint32_t)u[5]<<16)|((uint32_t)u[6]<<8)|u[7]; if (pg < MAXPG) atomic_fetch_add(&walpg[pg], 1); if (c) atomic_fetch_add(&walcommits, 1); } ADD(k, P_WR, 1); if (r > 0) ADD(k, P_WRB, r); return r; }
ssize_t writev(int fd, const struct iovec *v, int c) { REAL(writev); ssize_t r = real_writev(fd, v, c); int k = kfd(fd); ADD(k, P_WR, 1); if (r > 0) ADD(k, P_WRB, r); return r; }
ssize_t pwritev(int fd, const struct iovec *v, int c, off_t o) { REAL(pwritev); ssize_t r = real_pwritev(fd, v, c, o); int k = kfd(fd); ADD(k, P_WR, 1); if (r > 0) ADD(k, P_WRB, r); return r; }
size_t fwrite(const void *b, size_t s, size_t n, FILE *f) { REAL(fwrite); size_t r = real_fwrite(b, s, n, f); int k = kfd(fileno(f)); ADD(k, P_WR, 1); ADD(k, P_WRB, r * s); return r; }
ssize_t read(int fd, void *b, size_t n) { REAL(read); ssize_t r = real_read(fd, b, n); int k = kfd(fd); ADD(k, P_RD, 1); if (r > 0) { ADD(k, P_RDB, r); if (k == K_SRC) pace((size_t)r); } return r; }
ssize_t pread(int fd, void *b, size_t n, off_t o) { REAL(pread); ssize_t r = real_pread(fd, b, n, o); int k = kfd(fd); ADD(k, P_RD, 1); if (r > 0) { ADD(k, P_RDB, r); if (k == K_SRC) pace((size_t)r); } return r; }
ssize_t pread64(int fd, void *b, size_t n, off_t o) { REAL(pread64); ssize_t r = real_pread64(fd, b, n, o); int k = kfd(fd); ADD(k, P_RD, 1); if (r > 0) { ADD(k, P_RDB, r); if (k == K_SRC) pace((size_t)r); } return r; }

int fsync(int fd) { REAL(fsync); uint64_t t = now(); int r = real_fsync(fd); uint64_t d = now() - t; int k = kfd(fd); ADD(k, P_FSYNC, 1); atomic_fetch_add(&sync_ns[k], d); record_site(k, P_FSYNC, d); return r; }
int fdatasync(int fd) { REAL(fdatasync); uint64_t t = now(); int r = real_fdatasync(fd); uint64_t d = now() - t; int k = kfd(fd); ADD(k, P_FDSYNC, 1); atomic_fetch_add(&sync_ns[k], d); record_site(k, P_FDSYNC, d); return r; }
int sync_file_range(int fd, off_t o, off_t n, unsigned int fl) { REAL(sync_file_range); int r = real_sync_file_range(fd, o, n, fl); ADD(kfd(fd), P_SFR, 1); return r; }
int syncfs(int fd) { REAL(syncfs); int r = real_syncfs(fd); ADD(kfd(fd), P_SYNCFS, 1); return r; }
int ftruncate(int fd, off_t n) { REAL(ftruncate); int r = real_ftruncate(fd, n); ADD(kfd(fd), P_FTRUNC, 1); return r; }
int ftruncate64(int fd, off_t n) { REAL(ftruncate64); int r = real_ftruncate64(fd, n); ADD(kfd(fd), P_FTRUNC, 1); return r; }
int rename(const char *a, const char *b) { REAL(rename); int r = real_rename(a, b); ADD(classify(b), P_RENAME, 1); return r; }
int renameat(int da, const char *a, int db, const char *b) { REAL(renameat); int r = real_renameat(da, a, db, b); char lp[64], dp[4096], full[8192]; snprintf(lp, sizeof lp, "/proc/self/fd/%d", db); ssize_t n = readlink(lp, dp, sizeof dp - 1); if (n > 0 && b[0] != '/') { dp[n] = 0; snprintf(full, sizeof full, "%s/%s", dp, b); ADD(classify(full), P_RENAME, 1); } else ADD(classify(b), P_RENAME, 1); return r; }
int unlink(const char *p) { REAL(unlink); int r = real_unlink(p); ADD(classify(p), P_UNLINK, 1); return r; }
int close(int fd) { REAL(close); if (fd >= 0 && fd < MAXFD) fdk[fd] = 255; return real_close(fd); }
