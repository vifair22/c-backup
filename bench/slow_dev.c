/*
 * LD_PRELOAD shim: a slow, shared block device, modelled, with no real sync.
 *
 * slow_sync.c delays a sync and then makes it, which is right for timing a
 * backup end to end and wrong for an edit-latency run on a developer's
 * desktop: the real syncs are disk load the user feels. Here no fsync,
 * fdatasync, syncfs or sync_file_range on a modelled file reaches the disk.
 * Each is charged to one FIFO device timeline instead, so the calls that
 * would have queued behind each other on a real device still do:
 *
 *   fsync / fdatasync    latency + the file's dirty bytes / bandwidth
 *   sync_file_range      the range's bytes / bandwidth: on a busy device the
 *                        submitter waits in the block layer's writeback
 *                        throttle (wbt_wait) for the device to take them,
 *                        which is what FS2 measured under the backup's pack
 *                        writeback (B232)
 *
 * A file's dirty bytes are what pwrite/write put into it since its last
 * modelled sync. stdio writes (the pack writer's fwrite) do not pass through
 * an interposable write, so for a file that never saw one the dirty bytes
 * are its size less what earlier syncs and sync_file_range calls covered.
 *
 *   SLOWDEV_DIRS         colon-separated directory prefixes to model
 *                        (required; nothing is modelled without it)
 *   SLOWDEV_BW_MBS       device bandwidth in MiB/s (default 20)
 *   SLOWDEV_LAT_MS       latency of each sync in ms (default 20)
 *   SLOWDEV_FLOCK        1: a write to a file waits while a sync of that file
 *                        runs (btrfs holds the inode lock across fsync; xfs
 *                        and ext4 do not). Default 0
 *   SLOWDEV_SRC_DIR      source tree whose regular-file opens are paced, so
 *   SLOWDEV_SRC_OPEN_MS  a small tree takes as long to back up as a large
 *                        one would, without reading more bytes
 *   SLOWDEV_REPORT       file the per-call summary is written to at exit
 *
 * The device is a model: one queue, cost linear in bytes, no dirty-page
 * throttling of plain writes. It reproduces who waits for whom inside the
 * daemon exactly, because that code runs unmodified; the device-level
 * numbers are parameters, not measurements. See bench/edit_latency.sh.
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAX_DIRS 8
#define MAX_INO  4096

enum { K_WAL, K_DB, K_REPO, K_N };
enum { OP_SYNC, OP_SFR, OP_WRITE_WAIT, OP_N };
static const char *const kind_name[K_N] = { "wal", "db", "repo" };
static const char *const op_name[OP_N]  = { "sync", "sync_file_range", "write-blocked" };

typedef struct {
    dev_t    dev;
    ino_t    ino;
    int      kind;        /* K_*, or -1 for a file outside every prefix */
    int      pwritten;    /* saw an interposable write: dirty is tracked */
    uint64_t dirty;
    uint64_t flushed;     /* size covered by syncs, for stdio-written files */
    int      syncing;
} ino_t_entry;

static ino_t_entry     g_ino[MAX_INO];
static int             g_nino;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv = PTHREAD_COND_INITIALIZER;
static uint64_t        g_next_free;
static char            g_dirs[MAX_DIRS][PATH_MAX];
static size_t          g_ndirs;
static char            g_src[PATH_MAX];
static uint64_t        g_src_ns;
static double          g_ns_per_byte;
static uint64_t        g_lat_ns;
static int             g_flock;
static atomic_ullong   g_n[OP_N][K_N], g_ns[OP_N][K_N], g_max[OP_N][K_N], g_bytes[OP_N][K_N];

static uint64_t now_ns(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + (uint64_t)t.tv_nsec;
}

static void sleep_until(uint64_t t)
{
    struct timespec ts = { (time_t)(t / 1000000000ull), (long)(t % 1000000000ull) };
    while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL) == EINTR) {}
}

static void sleep_ns(uint64_t ns)
{
    struct timespec ts = { (time_t)(ns / 1000000000ull), (long)(ns % 1000000000ull) };
    while (nanosleep(&ts, &ts) == EINTR) {}
}

static void note(int op, int kind, uint64_t ns, uint64_t bytes)
{
    atomic_fetch_add(&g_n[op][kind], 1);
    atomic_fetch_add(&g_ns[op][kind], ns);
    atomic_fetch_add(&g_bytes[op][kind], bytes);
    unsigned long long m = atomic_load(&g_max[op][kind]);
    while (ns > m && !atomic_compare_exchange_weak(&g_max[op][kind], &m, ns)) {}
}

/* dlsym hands back an object pointer; copying it is the conversion POSIX
 * promises works, and -Wpedantic rejects the cast. */
#define REAL(name, ret, ...)                                              \
    static ret (*real_##name)(__VA_ARGS__);                                \
    if (!real_##name) {                                                    \
        void *p_ = dlsym(RTLD_NEXT, #name);                                \
        memcpy(&real_##name, &p_, sizeof(p_));                             \
    }

__attribute__((constructor)) static void slow_dev_init(void)
{
    const char *e;
    double bw = (e = getenv("SLOWDEV_BW_MBS")) ? atof(e) : 20.0;
    if (bw <= 0) bw = 20.0;
    g_ns_per_byte = 1e9 / (bw * 1048576.0);
    g_lat_ns = (uint64_t)(((e = getenv("SLOWDEV_LAT_MS")) ? atof(e) : 20.0) * 1e6);
    g_flock = (e = getenv("SLOWDEV_FLOCK")) && strcmp(e, "1") == 0;
    if ((e = getenv("SLOWDEV_SRC_DIR")) && *e) snprintf(g_src, sizeof(g_src), "%s", e);
    g_src_ns = (uint64_t)(((e = getenv("SLOWDEV_SRC_OPEN_MS")) ? atof(e) : 0.0) * 1e6);
    const char *list = getenv("SLOWDEV_DIRS");
    while (list && *list && g_ndirs < MAX_DIRS) {
        const char *c = strchr(list, ':');
        size_t n = c ? (size_t)(c - list) : strlen(list);
        if (n > 0 && n < PATH_MAX) {
            memcpy(g_dirs[g_ndirs], list, n);
            g_dirs[g_ndirs][n] = '\0';
            g_ndirs++;
        }
        list = c ? c + 1 : NULL;
    }
}

static int under(const char *path, const char *pre)
{
    size_t n = strlen(pre);
    return n && strncmp(path, pre, n) == 0 && (path[n] == '/' || path[n] == '\0');
}

static int fd_path(int fd, char *out, size_t outsz)
{
    char l[64];
    snprintf(l, sizeof(l), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(l, out, outsz - 1);
    if (n <= 0) return -1;
    out[n] = '\0';
    return 0;
}

static int classify(const char *path)
{
    int in = 0;
    for (size_t i = 0; i < g_ndirs && !in; i++) in = under(path, g_dirs[i]);
    if (!in) return -1;
    size_t n = strlen(path);
    if (n > 4 && strcmp(path + n - 4, "-wal") == 0) return K_WAL;
    if (n > 4 && strcmp(path + n - 4, "-shm") == 0) return -1;
    if (n > 3 && strcmp(path + n - 3, ".db") == 0) return K_DB;
    return K_REPO;
}

/* The fd's inode slot, or NULL for a file not modelled. Under g_mu. */
static ino_t_entry *slot_locked(int fd, struct stat *st)
{
    if (g_ndirs == 0 || fstat(fd, st) != 0 || !S_ISREG(st->st_mode)) return NULL;
    for (int i = 0; i < g_nino; i++)
        if (g_ino[i].dev == st->st_dev && g_ino[i].ino == st->st_ino)
            return g_ino[i].kind >= 0 ? &g_ino[i] : NULL;
    char path[PATH_MAX];
    if (fd_path(fd, path, sizeof(path)) != 0) return NULL;
    int kind = classify(path);
    if (g_nino == MAX_INO) {
        /* Forget the oldest repository file: packs come and go. */
        memmove(&g_ino[0], &g_ino[1], sizeof(g_ino[0]) * (MAX_INO - 1));
        g_nino--;
    }
    ino_t_entry *s = &g_ino[g_nino++];
    memset(s, 0, sizeof(*s));
    s->dev = st->st_dev;
    s->ino = st->st_ino;
    s->kind = kind;
    return kind >= 0 ? s : NULL;
}

static void model_write(int fd, size_t n)
{
    if (g_ndirs == 0) return;
    struct stat st;
    uint64_t t0 = now_ns();
    pthread_mutex_lock(&g_mu);
    ino_t_entry *s = slot_locked(fd, &st);
    if (!s) { pthread_mutex_unlock(&g_mu); return; }
    int kind = s->kind;
    while (g_flock && s->syncing) pthread_cond_wait(&g_cv, &g_mu);
    s->pwritten = 1;
    s->dirty += n;
    pthread_mutex_unlock(&g_mu);
    uint64_t took = now_ns() - t0;
    if (took > 100000ull) note(OP_WRITE_WAIT, kind, took, n);
}

/* 0 when modelled (and charged), -1 when the real call should run. */
static int model_sync(int fd, int op, off64_t off, off64_t len)
{
    if (g_ndirs == 0) return -1;
    struct stat st;
    uint64_t t0 = now_ns();
    pthread_mutex_lock(&g_mu);
    ino_t_entry *s = slot_locked(fd, &st);
    if (!s) { pthread_mutex_unlock(&g_mu); return -1; }
    uint64_t size = (uint64_t)st.st_size, bytes;
    if (op == OP_SFR) {
        uint64_t end = len == 0 ? size : (uint64_t)off + (uint64_t)len;
        if (end > size) end = size;
        bytes = end > (uint64_t)off ? end - (uint64_t)off : 0;
        if (!s->pwritten && end > s->flushed) s->flushed = end;
        if (s->pwritten) s->dirty = s->dirty > bytes ? s->dirty - bytes : 0;
    } else if (s->pwritten) {
        bytes = s->dirty;
        s->dirty = 0;
    } else {
        bytes = size > s->flushed ? size - s->flushed : 0;
        s->flushed = size;
    }
    uint64_t start = g_next_free > t0 ? g_next_free : t0;
    g_next_free = start + (op == OP_SFR ? 0 : g_lat_ns) + (uint64_t)((double)bytes * g_ns_per_byte);
    uint64_t until = g_next_free;
    int kind = s->kind;
    s->syncing++;
    pthread_mutex_unlock(&g_mu);
    sleep_until(until);
    pthread_mutex_lock(&g_mu);
    s->syncing--;
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
    note(op, kind, now_ns() - t0, bytes);
    return 0;
}

ssize_t pwrite(int fd, const void *b, size_t n, off_t o);
ssize_t pwrite(int fd, const void *b, size_t n, off_t o)
{
    REAL(pwrite, ssize_t, int, const void *, size_t, off_t);
    model_write(fd, n);
    return real_pwrite(fd, b, n, o);
}

ssize_t pwrite64(int fd, const void *b, size_t n, off64_t o);
ssize_t pwrite64(int fd, const void *b, size_t n, off64_t o)
{
    REAL(pwrite64, ssize_t, int, const void *, size_t, off64_t);
    model_write(fd, n);
    return real_pwrite64(fd, b, n, o);
}

ssize_t write(int fd, const void *b, size_t n);
ssize_t write(int fd, const void *b, size_t n)
{
    REAL(write, ssize_t, int, const void *, size_t);
    model_write(fd, n);
    return real_write(fd, b, n);
}

int fsync(int fd);
int fsync(int fd)
{
    REAL(fsync, int, int);
    if (model_sync(fd, OP_SYNC, 0, 0) == 0) return 0;
    return real_fsync(fd);
}

int fdatasync(int fd);
int fdatasync(int fd)
{
    REAL(fdatasync, int, int);
    if (model_sync(fd, OP_SYNC, 0, 0) == 0) return 0;
    return real_fdatasync(fd);
}

int syncfs(int fd);
int syncfs(int fd)
{
    REAL(syncfs, int, int);
    char path[PATH_MAX];
    if (g_ndirs && fd_path(fd, path, sizeof(path)) == 0)
        for (size_t i = 0; i < g_ndirs; i++)
            if (under(path, g_dirs[i])) return 0;
    return real_syncfs(fd);
}

int sync_file_range(int fd, off64_t off, off64_t len, unsigned int flags);
int sync_file_range(int fd, off64_t off, off64_t len, unsigned int flags)
{
    REAL(sync_file_range, int, int, off64_t, off64_t, unsigned int);
    if (model_sync(fd, OP_SFR, off, len) == 0) return 0;
    return real_sync_file_range(fd, off, len, flags);
}

/* open_scanned opens sources with openat2 through syscall(2): pace there. */
long syscall(long n, ...);
long syscall(long n, ...)
{
    REAL(syscall, long, long, ...);
    va_list ap;
    va_start(ap, n);
    long a1 = va_arg(ap, long), a2 = va_arg(ap, long), a3 = va_arg(ap, long);
    long a4 = va_arg(ap, long), a5 = va_arg(ap, long), a6 = va_arg(ap, long);
    va_end(ap);
    long r = real_syscall(n, a1, a2, a3, a4, a5, a6);
#ifdef SYS_openat2
    if (n == SYS_openat2 && r >= 0 && g_src_ns && g_src[0]) {
        int saved = errno;
        char path[PATH_MAX];
        struct stat st;
        if (fd_path((int)r, path, sizeof(path)) == 0 && under(path, g_src) &&
            fstat((int)r, &st) == 0 && S_ISREG(st.st_mode))
            sleep_ns(g_src_ns);
        errno = saved;
    }
#endif
    return r;
}

__attribute__((destructor)) static void slow_dev_report(void)
{
    const char *p = getenv("SLOWDEV_REPORT");
    if (!p || !*p) return;
    FILE *f = fopen(p, "w");
    if (!f) return;
    fprintf(f, "# op kind calls total_s max_ms MiB\n");
    for (int op = 0; op < OP_N; op++)
        for (int k = 0; k < K_N; k++) {
            unsigned long long n = atomic_load(&g_n[op][k]);
            if (!n) continue;
            fprintf(f, "%s %s %llu %.3f %.1f %.1f\n", op_name[op], kind_name[k], n,
                    (double)atomic_load(&g_ns[op][k]) / 1e9,
                    (double)atomic_load(&g_max[op][k]) / 1e6,
                    (double)atomic_load(&g_bytes[op][k]) / 1048576.0);
        }
    fclose(f);
}
