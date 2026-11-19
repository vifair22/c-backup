/*
 * LD_PRELOAD shim: count the libc calls a run makes, time them, and sample
 * the main thread's stack, without perf, strace or root.
 *
 * Written for the many-small-files profile (PERF.md, 2026-11-16), on a
 * machine where perf and strace are not installed and perf_event_paranoid
 * is 2. A small-file backup runs on the calling thread end to end, so one
 * thread's stack, sampled on a wall clock, shows where its time goes
 * whether it is computing or blocked in the kernel: a sample landing in
 * fdatasync is a sample of waiting for the disk.
 *
 * Two independent parts:
 *
 *   Counters. Every wrapped call is counted and timed (CLOCK_MONOTONIC
 *   around the real call), across all threads. Only calls that go through
 *   the PLT are seen: glibc's internal calls (stdio's write under fwrite,
 *   opendir's openat) are not, so stdio is counted at the fwrite/fflush
 *   level instead. SQLite reaches the kernel through its own table of libc
 *   pointers, which resolve through the PLT, so its pread/pwrite/fcntl/
 *   fdatasync are seen.
 *
 *   Sampler. A POSIX timer on the main thread (SIGEV_THREAD_ID), wall
 *   clock or that thread's CPU clock, records a backtrace() per tick. The
 *   signal interrupts the thread wherever it is, including inside a system
 *   call, so the innermost frame of a wall sample is where it was waiting.
 *   SA_RESTART keeps interrupted calls from failing; calls the kernel does
 *   not restart (nanosleep, a few others) can see EINTR, which nothing on
 *   the backup path depends on.
 *
 * Segments: a program that wants per-step numbers looks up prof_mark with
 * dlsym(RTLD_DEFAULT) and calls it with a label at each step; counters are
 * reported per segment and every sample carries its segment number.
 * bench_phases does this when the shim is loaded and is unchanged when not.
 *
 *   PROF_OUT     directory for samples.txt, maps.txt and counters.txt
 *                (unset: counters to stderr only, no sampling)
 *   PROF_CLOCK   wall (default) or cpu
 *   PROF_HZ      samples per second (default 1000)
 *
 * bench/prof_report.py turns samples.txt into a profile.
 *
 * SQLite's prepare, step and exec are counted too, which is how a run says
 * how many statements it executed per file.
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <dirent.h>
#include <time.h>
#include <unistd.h>

#include <sqlite3.h>

/* ---------------------------------------------------------------- */
/* Counters                                                          */
/* ---------------------------------------------------------------- */

#define CALLS(X) \
    X(open) X(openat) X(close) X(read) X(pread) X(write) X(pwrite) \
    X(fstat) X(stat) X(lstat) X(fstatat) X(statx) X(fstatfs) X(lseek) \
    X(fsync) X(fdatasync) X(syncfs) X(sync_file_range) X(fadvise) \
    X(rename) X(renameat) X(unlink) X(mkdir) X(ftruncate) X(fcntl) \
    X(opendir) X(readdir) X(closedir) X(lgetxattr) X(llistxattr) \
    X(getxattr) X(listxattr) X(fgetxattr) X(flistxattr) X(fsetxattr) \
    X(lsetxattr) X(fchmod) X(fchown) X(lchown) X(utimensat) X(futimens) \
    X(fwrite) X(fflush) X(fopen) X(fclose) X(mmap) X(munmap) \
    X(sqlite3_prepare_v2) X(sqlite3_prepare_v3) X(sqlite3_step) \
    X(sqlite3_reset) X(sqlite3_exec)

enum {
#define X(n) C_##n,
    CALLS(X)
#undef X
    C__COUNT
};

static const char *const call_names[C__COUNT] = {
#define X(n) #n,
    CALLS(X)
#undef X
};

static _Atomic uint64_t g_calls[C__COUNT];
static _Atomic uint64_t g_ns[C__COUNT];
/* Bytes moved, for the calls that move them (read, pread, write, pwrite). */
static _Atomic uint64_t g_bytes[C__COUNT];

static inline uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static inline void account(int c, uint64_t t0)
{
    atomic_fetch_add_explicit(&g_calls[c], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_ns[c], now_ns() - t0, memory_order_relaxed);
}

/* dlsym hands back an object pointer; copying it is the conversion POSIX
 * promises works, and -Wpedantic rejects the cast. */
#define RESOLVE(var, name) do { \
        if (!(var)) { void *p_ = dlsym(RTLD_NEXT, name); memcpy(&(var), &p_, sizeof(p_)); } \
    } while (0)

/* ---------------------------------------------------------------- */
/* Sampler                                                           */
/* ---------------------------------------------------------------- */

#define MAX_FRAMES  48
#define MAX_SAMPLES 400000
#define MAX_MARKS   16

typedef struct {
    uint16_t seg;
    uint16_t n;
    uint32_t weight;    /* 1 + timer overruns: ticks this sample stands for */
    void    *pc[MAX_FRAMES];
} sample_t;

static sample_t        *g_samples;
static _Atomic uint32_t g_nsamples;
static _Atomic uint32_t g_dropped;
static _Atomic uint16_t g_seg;
static timer_t          g_timer;
static int              g_sampling;
static char             g_out[1024];
static pid_t            g_main_tid;

static char     g_mark_label[MAX_MARKS][64];
static uint64_t g_mark_ns[MAX_MARKS];
static uint64_t g_mark_calls[MAX_MARKS][C__COUNT];
static uint64_t g_mark_cns[MAX_MARKS][C__COUNT];
static uint64_t g_mark_bytes[MAX_MARKS][C__COUNT];
static unsigned g_nmarks;

static void on_tick(int sig, siginfo_t *si, void *uc)
{
    (void)sig; (void)si; (void)uc;
    int saved = errno;
    uint32_t i = atomic_fetch_add_explicit(&g_nsamples, 1, memory_order_relaxed);
    if (i < MAX_SAMPLES) {
        sample_t *s = &g_samples[i];
        s->seg = atomic_load_explicit(&g_seg, memory_order_relaxed);
        /* A thread in uninterruptible sleep (fdatasync, mostly) takes the
         * signal only when it wakes, and the ticks it slept through are
         * folded into one: the overrun count gives them back. */
        int ov = timer_getoverrun(g_timer);
        s->weight = 1u + (uint32_t)(ov > 0 ? ov : 0);
        int n = backtrace(s->pc, MAX_FRAMES);
        s->n = (uint16_t)(n > 0 ? n : 0);
    } else {
        atomic_fetch_add_explicit(&g_dropped, 1, memory_order_relaxed);
    }
    errno = saved;
}

static void snapshot_mark(const char *label)
{
    if (g_nmarks >= MAX_MARKS) return;
    unsigned m = g_nmarks++;
    snprintf(g_mark_label[m], sizeof(g_mark_label[m]), "%s", label);
    g_mark_ns[m] = now_ns();
    for (int c = 0; c < C__COUNT; c++) {
        g_mark_calls[m][c] = atomic_load(&g_calls[c]);
        g_mark_cns[m][c]   = atomic_load(&g_ns[c]);
        g_mark_bytes[m][c] = atomic_load(&g_bytes[c]);
    }
}

/* Called by the program under test at the start of each step. */
void prof_mark(const char *label);
void prof_mark(const char *label)
{
    snapshot_mark(label ? label : "?");
    atomic_store(&g_seg, (uint16_t)(g_nmarks - 1));
}

__attribute__((constructor))
static void prof_init(void)
{
    g_main_tid = (pid_t)syscall(SYS_gettid);
    snapshot_mark("start");

    const char *out = getenv("PROF_OUT");
    if (!out || !*out) return;
    snprintf(g_out, sizeof(g_out), "%s", out);
    mkdir(g_out, 0755);

    g_samples = calloc(MAX_SAMPLES, sizeof(*g_samples));
    if (!g_samples) return;

    /* backtrace() loads libgcc_s on first use, which is not something to do
     * inside a signal handler. */
    void *warm[4];
    (void)backtrace(warm, 4);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = on_tick;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPROF, &sa, NULL);

    const char *ck = getenv("PROF_CLOCK");
    clockid_t clk = CLOCK_MONOTONIC;
    if (ck && strcmp(ck, "cpu") == 0) pthread_getcpuclockid(pthread_self(), &clk);

    struct sigevent sev;
    memset(&sev, 0, sizeof(sev));
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev.sigev_signo = SIGPROF;
    sev._sigev_un._tid = g_main_tid;
    if (timer_create(clk, &sev, &g_timer) != 0) return;

    long hz = 1000;
    const char *h = getenv("PROF_HZ");
    if (h && atol(h) > 0) hz = atol(h);
    struct itimerspec its;
    memset(&its, 0, sizeof(its));
    its.it_interval.tv_nsec = 1000000000L / hz;
    its.it_value = its.it_interval;
    if (timer_settime(g_timer, 0, &its, NULL) == 0) g_sampling = 1;
}

static void write_sql(FILE *f);

static void write_counters(FILE *f)
{
    snapshot_mark("end");
    for (unsigned m = 0; m + 1 < g_nmarks; m++) {
        double secs = (double)(g_mark_ns[m + 1] - g_mark_ns[m]) / 1e9;
        fprintf(f, "== segment %u: %s (%.3f s)\n", m, g_mark_label[m], secs);
        fprintf(f, "   %-16s %12s %12s %10s %14s\n", "call", "count", "total ms", "us/call",
                "bytes");
        for (int c = 0; c < C__COUNT; c++) {
            uint64_t n  = g_mark_calls[m + 1][c] - g_mark_calls[m][c];
            uint64_t ns = g_mark_cns[m + 1][c] - g_mark_cns[m][c];
            if (!n) continue;
            uint64_t by = g_mark_bytes[m + 1][c] - g_mark_bytes[m][c];
            fprintf(f, "   %-16s %12llu %12.1f %10.2f %14llu\n", call_names[c],
                    (unsigned long long)n, (double)ns / 1e6,
                    (double)ns / 1e3 / (double)n, (unsigned long long)by);
        }
    }
    write_sql(f);
}

__attribute__((destructor))
static void prof_fini(void)
{
    if (g_sampling) {
        struct itimerspec its;
        memset(&its, 0, sizeof(its));
        timer_settime(g_timer, 0, &its, NULL);
        g_sampling = 0;
    }
    if (!g_out[0]) {
        write_counters(stderr);
        return;
    }
    char path[1200];
    snprintf(path, sizeof(path), "%s/counters.txt", g_out);
    FILE *f = fopen(path, "w");
    if (f) { write_counters(f); fclose(f); }

    if (!g_samples) return;
    snprintf(path, sizeof(path), "%s/samples.txt", g_out);
    f = fopen(path, "w");
    if (f) {
        uint32_t n = atomic_load(&g_nsamples);
        if (n > MAX_SAMPLES) n = MAX_SAMPLES;
        fprintf(f, "# clock %s hz %s dropped %u\n",
                getenv("PROF_CLOCK") ? getenv("PROF_CLOCK") : "wall",
                getenv("PROF_HZ") ? getenv("PROF_HZ") : "1000",
                atomic_load(&g_dropped));
        for (unsigned m = 0; m < g_nmarks; m++)
            fprintf(f, "# seg %u %s\n", m, g_mark_label[m]);
        for (uint32_t i = 0; i < n; i++) {
            const sample_t *s = &g_samples[i];
            fprintf(f, "%u:%u", s->seg, s->weight);
            for (unsigned k = 0; k < s->n; k++) fprintf(f, " %p", s->pc[k]);
            fputc('\n', f);
        }
        fclose(f);
    }
    snprintf(path, sizeof(path), "%s/maps.txt", g_out);
    FILE *in = fopen("/proc/self/maps", "r");
    FILE *outm = fopen(path, "w");
    if (in && outm) {
        char line[4096];
        while (fgets(line, sizeof(line), in)) fputs(line, outm);
    }
    if (in) fclose(in);
    if (outm) fclose(outm);
}

/* ---------------------------------------------------------------- */
/* Wrappers                                                          */
/* ---------------------------------------------------------------- */

/* WRAP, also counting the bytes a successful call moved. */
#define WRAPB(ret, name, C, params, args) \
    ret name params { \
        static ret (*real) params; \
        RESOLVE(real, #name); \
        uint64_t t0 = now_ns(); \
        ret r = real args; \
        account(C, t0); \
        if (r > 0) atomic_fetch_add_explicit(&g_bytes[C], (uint64_t)r, memory_order_relaxed); \
        return r; \
    }

#define WRAP(ret, name, C, params, args) \
    ret name params { \
        static ret (*real) params; \
        RESOLVE(real, #name); \
        uint64_t t0 = now_ns(); \
        ret r = real args; \
        account(C, t0); \
        return r; \
    }

/* open and openat take a mode only with O_CREAT or O_TMPFILE. */
static int (*real_open)(const char *, int, ...);
static int (*real_openat)(int, const char *, int, ...);

int open(const char *p, int fl, ...)
{
    RESOLVE(real_open, "open");
    mode_t m = 0;
    if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    uint64_t t0 = now_ns();
    int r = real_open(p, fl, m);
    account(C_open, t0);
    return r;
}
int open64(const char *p, int fl, ...)
{
    RESOLVE(real_open, "open");
    mode_t m = 0;
    if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    uint64_t t0 = now_ns();
    int r = real_open(p, fl, m);
    account(C_open, t0);
    return r;
}
int openat(int d, const char *p, int fl, ...)
{
    RESOLVE(real_openat, "openat");
    mode_t m = 0;
    if (fl & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, fl); m = (mode_t)va_arg(ap, int); va_end(ap); }
    uint64_t t0 = now_ns();
    int r = real_openat(d, p, fl, m);
    account(C_openat, t0);
    return r;
}

/* fcntl's third argument is an int or a pointer; passing it on as a pointer
 * carries either through the x86-64 calling convention unchanged. */
static int (*real_fcntl)(int, int, ...);
int fcntl(int fd, int cmd, ...)
{
    RESOLVE(real_fcntl, "fcntl");
    va_list ap; va_start(ap, cmd); void *a = va_arg(ap, void *); va_end(ap);
    uint64_t t0 = now_ns();
    int r = real_fcntl(fd, cmd, a);
    account(C_fcntl, t0);
    return r;
}
int fcntl64(int fd, int cmd, ...)
{
    static int (*real)(int, int, ...);
    RESOLVE(real, "fcntl64");
    va_list ap; va_start(ap, cmd); void *a = va_arg(ap, void *); va_end(ap);
    uint64_t t0 = now_ns();
    int r = real(fd, cmd, a);
    account(C_fcntl, t0);
    return r;
}

WRAP(int, close, C_close, (int fd), (fd))
WRAPB(ssize_t, read, C_read, (int fd, void *b, size_t n), (fd, b, n))
WRAPB(ssize_t, pread, C_pread, (int fd, void *b, size_t n, off_t o), (fd, b, n, o))
WRAPB(ssize_t, pread64, C_pread, (int fd, void *b, size_t n, off_t o), (fd, b, n, o))
WRAPB(ssize_t, write, C_write, (int fd, const void *b, size_t n), (fd, b, n))
WRAPB(ssize_t, pwrite, C_pwrite, (int fd, const void *b, size_t n, off_t o), (fd, b, n, o))
WRAPB(ssize_t, pwrite64, C_pwrite, (int fd, const void *b, size_t n, off_t o), (fd, b, n, o))
WRAP(int, fstat, C_fstat, (int fd, struct stat *s), (fd, s))
WRAP(int, fstat64, C_fstat, (int fd, struct stat64 *s), (fd, s))
WRAP(int, stat, C_stat, (const char *p, struct stat *s), (p, s))
WRAP(int, stat64, C_stat, (const char *p, struct stat64 *s), (p, s))
WRAP(int, lstat, C_lstat, (const char *p, struct stat *s), (p, s))
WRAP(int, lstat64, C_lstat, (const char *p, struct stat64 *s), (p, s))
WRAP(int, fstatat, C_fstatat, (int d, const char *p, struct stat *s, int f), (d, p, s, f))
WRAP(int, fstatat64, C_fstatat, (int d, const char *p, struct stat64 *s, int f), (d, p, s, f))
WRAP(int, statx, C_statx, (int d, const char *p, int f, unsigned m, struct statx *s), (d, p, f, m, s))
WRAP(int, fstatfs, C_fstatfs, (int fd, struct statfs *s), (fd, s))
WRAP(int, fstatfs64, C_fstatfs, (int fd, struct statfs64 *s), (fd, s))
WRAP(off_t, lseek, C_lseek, (int fd, off_t o, int w), (fd, o, w))
WRAP(off_t, lseek64, C_lseek, (int fd, off_t o, int w), (fd, o, w))
WRAP(int, fsync, C_fsync, (int fd), (fd))
WRAP(int, fdatasync, C_fdatasync, (int fd), (fd))
WRAP(int, syncfs, C_syncfs, (int fd), (fd))
WRAP(int, sync_file_range, C_sync_file_range, (int fd, off_t o, off_t n, unsigned f), (fd, o, n, f))
WRAP(int, posix_fadvise, C_fadvise, (int fd, off_t o, off_t n, int a), (fd, o, n, a))
WRAP(int, posix_fadvise64, C_fadvise, (int fd, off_t o, off_t n, int a), (fd, o, n, a))
WRAP(int, rename, C_rename, (const char *a, const char *b), (a, b))
WRAP(int, renameat, C_renameat, (int da, const char *a, int db, const char *b), (da, a, db, b))
WRAP(int, unlink, C_unlink, (const char *p), (p))
WRAP(int, mkdir, C_mkdir, (const char *p, mode_t m), (p, m))
WRAP(int, ftruncate, C_ftruncate, (int fd, off_t n), (fd, n))
WRAP(int, ftruncate64, C_ftruncate, (int fd, off_t n), (fd, n))
WRAP(DIR *, opendir, C_opendir, (const char *p), (p))
WRAP(struct dirent *, readdir, C_readdir, (DIR *d), (d))
WRAP(struct dirent64 *, readdir64, C_readdir, (DIR *d), (d))
WRAP(int, closedir, C_closedir, (DIR *d), (d))
WRAP(ssize_t, lgetxattr, C_lgetxattr, (const char *p, const char *n, void *v, size_t s), (p, n, v, s))
WRAP(ssize_t, llistxattr, C_llistxattr, (const char *p, char *l, size_t s), (p, l, s))
WRAP(ssize_t, getxattr, C_getxattr, (const char *p, const char *n, void *v, size_t s), (p, n, v, s))
WRAP(ssize_t, listxattr, C_listxattr, (const char *p, char *l, size_t s), (p, l, s))
WRAP(ssize_t, fgetxattr, C_fgetxattr, (int fd, const char *n, void *v, size_t s), (fd, n, v, s))
WRAP(ssize_t, flistxattr, C_flistxattr, (int fd, char *l, size_t s), (fd, l, s))
WRAP(int, fsetxattr, C_fsetxattr, (int fd, const char *n, const void *v, size_t s, int f), (fd, n, v, s, f))
WRAP(int, lsetxattr, C_lsetxattr, (const char *p, const char *n, const void *v, size_t s, int f), (p, n, v, s, f))
WRAP(int, fchmod, C_fchmod, (int fd, mode_t m), (fd, m))
WRAP(int, fchown, C_fchown, (int fd, uid_t u, gid_t g), (fd, u, g))
WRAP(int, lchown, C_lchown, (const char *p, uid_t u, gid_t g), (p, u, g))
WRAP(int, utimensat, C_utimensat, (int d, const char *p, const struct timespec t[2], int f), (d, p, t, f))
WRAP(int, futimens, C_futimens, (int fd, const struct timespec t[2]), (fd, t))
WRAP(size_t, fwrite, C_fwrite, (const void *b, size_t s, size_t n, FILE *f), (b, s, n, f))
WRAP(int, fflush, C_fflush, (FILE *f), (f))
WRAP(FILE *, fopen, C_fopen, (const char *p, const char *m), (p, m))
WRAP(FILE *, fopen64, C_fopen, (const char *p, const char *m), (p, m))
WRAP(int, fclose, C_fclose, (FILE *f), (f))
WRAP(void *, mmap, C_mmap, (void *a, size_t l, int p, int f, int fd, off_t o), (a, l, p, f, fd, o))
WRAP(void *, mmap64, C_mmap, (void *a, size_t l, int p, int f, int fd, off_t o), (a, l, p, f, fd, o))
WRAP(int, munmap, C_munmap, (void *a, size_t l), (a, l))

/*
 * Per statement: prepares and steps are charged to the statement's SQL, so a
 * run says which query costs what, prepare and execution apart. Statements
 * are mapped by handle to their text (first 120 bytes, whitespace runs
 * collapsed); a handle reused after a finalize is remapped at its next
 * prepare.
 */
#define SQL_SLOTS   256
#define SQL_HANDLES 8192

typedef struct {
    char     text[121];
    uint64_t prepares, prep_ns, steps, step_ns;
    /* sqlite3_stmt_status at finalize, summed: what the statement did. */
    uint64_t vm_steps, fullscan, sorts, autoidx, reprepares;
} sql_stat_t;

static sql_stat_t      g_sql[SQL_SLOTS];
static unsigned        g_nsql;
static pthread_mutex_t g_sql_mu = PTHREAD_MUTEX_INITIALIZER;
static struct { const void *h; int idx; } g_hmap[SQL_HANDLES];

static int sql_slot(const char *sql)
{
    char key[121];
    size_t k = 0;
    for (const char *c = sql; *c && k < 120; c++) {
        int ws = (*c == ' ' || *c == '\n' || *c == '\t');
        if (ws && (k == 0 || key[k - 1] == ' ')) continue;
        key[k++] = ws ? ' ' : *c;
    }
    key[k] = '\0';
    for (unsigned i = 0; i < g_nsql; i++)
        if (strcmp(g_sql[i].text, key) == 0) return (int)i;
    if (g_nsql >= SQL_SLOTS) return SQL_SLOTS - 1;
    memcpy(g_sql[g_nsql].text, key, k + 1);
    return (int)g_nsql++;
}

static unsigned hslot(const void *h)
{
    return (unsigned)(((uintptr_t)h >> 4) * 2654435761u) & (SQL_HANDLES - 1);
}

static void sql_prepared(const char *sql, sqlite3_stmt *st, uint64_t ns)
{
    if (!sql || !st) return;
    pthread_mutex_lock(&g_sql_mu);
    int idx = sql_slot(sql);
    g_sql[idx].prepares++;
    g_sql[idx].prep_ns += ns;
    unsigned h = hslot(st);
    for (unsigned n = 0; n < SQL_HANDLES; n++, h = (h + 1) & (SQL_HANDLES - 1))
        if (!g_hmap[h].h || g_hmap[h].h == st) { g_hmap[h].h = st; g_hmap[h].idx = idx; break; }
    pthread_mutex_unlock(&g_sql_mu);
}

static void sql_stepped(sqlite3_stmt *st, uint64_t ns)
{
    pthread_mutex_lock(&g_sql_mu);
    unsigned h = hslot(st);
    for (unsigned n = 0; n < SQL_HANDLES; n++, h = (h + 1) & (SQL_HANDLES - 1)) {
        if (!g_hmap[h].h) break;
        if (g_hmap[h].h == st) {
            g_sql[g_hmap[h].idx].steps++;
            g_sql[g_hmap[h].idx].step_ns += ns;
            break;
        }
    }
    pthread_mutex_unlock(&g_sql_mu);
}

static void write_sql(FILE *f)
{
    fprintf(f, "== statements, whole run (count, ms)\n");
    for (unsigned i = 0; i < g_nsql; i++) {
        const sql_stat_t *q = &g_sql[i];
        if (q->prepares < 100 && q->steps < 100) continue;
        fprintf(f, "   prep %9llu %9.1f  step %9llu %9.1f  vm/run %7.1f scan %llu sort %llu"
                " autoidx %llu reprep %llu  %s\n",
                (unsigned long long)q->prepares, (double)q->prep_ns / 1e6,
                (unsigned long long)q->steps, (double)q->step_ns / 1e6,
                q->steps ? (double)q->vm_steps / (double)q->steps : 0.0,
                (unsigned long long)q->fullscan, (unsigned long long)q->sorts,
                (unsigned long long)q->autoidx, (unsigned long long)q->reprepares,
                q->text);
    }
}

int sqlite3_prepare_v2(sqlite3 *db, const char *sql, int n, sqlite3_stmt **st,
                       const char **tail)
{
    static int (*real)(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
    RESOLVE(real, "sqlite3_prepare_v2");
    uint64_t t0 = now_ns();
    int r = real(db, sql, n, st, tail);
    account(C_sqlite3_prepare_v2, t0);
    if (r == SQLITE_OK && st) sql_prepared(sql, *st, now_ns() - t0);
    return r;
}

int sqlite3_prepare_v3(sqlite3 *db, const char *sql, int n, unsigned fl,
                       sqlite3_stmt **st, const char **tail)
{
    static int (*real)(sqlite3 *, const char *, int, unsigned, sqlite3_stmt **, const char **);
    RESOLVE(real, "sqlite3_prepare_v3");
    uint64_t t0 = now_ns();
    int r = real(db, sql, n, fl, st, tail);
    account(C_sqlite3_prepare_v3, t0);
    if (r == SQLITE_OK && st) sql_prepared(sql, *st, now_ns() - t0);
    return r;
}

int sqlite3_step(sqlite3_stmt *st)
{
    static int (*real)(sqlite3_stmt *);
    RESOLVE(real, "sqlite3_step");
    uint64_t t0 = now_ns();
    int r = real(st);
    account(C_sqlite3_step, t0);
    sql_stepped(st, now_ns() - t0);
    return r;
}

int sqlite3_finalize(sqlite3_stmt *st)
{
    static int (*real)(sqlite3_stmt *);
    RESOLVE(real, "sqlite3_finalize");
    if (st) {
        uint64_t vm = (uint64_t)sqlite3_stmt_status(st, SQLITE_STMTSTATUS_VM_STEP, 0);
        uint64_t fs = (uint64_t)sqlite3_stmt_status(st, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
        uint64_t so = (uint64_t)sqlite3_stmt_status(st, SQLITE_STMTSTATUS_SORT, 0);
        uint64_t ai = (uint64_t)sqlite3_stmt_status(st, SQLITE_STMTSTATUS_AUTOINDEX, 0);
        uint64_t rp = (uint64_t)sqlite3_stmt_status(st, SQLITE_STMTSTATUS_REPREPARE, 0);
        pthread_mutex_lock(&g_sql_mu);
        unsigned h = hslot(st);
        for (unsigned n = 0; n < SQL_HANDLES; n++, h = (h + 1) & (SQL_HANDLES - 1)) {
            if (!g_hmap[h].h) break;
            if (g_hmap[h].h == st) {
                sql_stat_t *q = &g_sql[g_hmap[h].idx];
                q->vm_steps += vm; q->fullscan += fs; q->sorts += so;
                q->autoidx += ai; q->reprepares += rp;
                break;
            }
        }
        pthread_mutex_unlock(&g_sql_mu);
    }
    return real(st);
}

WRAP(int, sqlite3_reset, C_sqlite3_reset, (sqlite3_stmt *st), (st))
WRAP(int, sqlite3_exec, C_sqlite3_exec,
     (sqlite3 *db, const char *sql, int (*cb)(void *, int, char **, char **), void *a, char **e),
     (db, sql, cb, a, e))
