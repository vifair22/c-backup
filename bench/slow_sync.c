/*
 * LD_PRELOAD shim: make fsync and fdatasync slow under chosen directories.
 *
 * The UAT host keeps state.db and the repositories on btrfs RAID10 over LUKS
 * on DRAM-less SATA SSDs, where one sync costs tens of milliseconds and
 * varies run to run. A developer's tmpfs or NVMe answers the same call in
 * microseconds, so a change that multiplies the number of syncs - committing
 * a batch every 200 ms instead of every 2,000 entries (OWN-16) - is free on
 * the machine it was written on and costs a minute where it runs. This puts
 * the deployment's sync cost back in front of bench_phases.
 *
 * Only syncs of files under one of the listed directories are delayed, so a
 * benchmark can slow the state database and the repository without slowing
 * anything else the process touches. The delay comes before the real call,
 * and syncs are serialised through one mutex by default: one device answers
 * them one at a time, and parallel sleeps would model a disk with as many
 * heads as there are threads.
 *
 *   SLOW_SYNC_MS      delay per call, milliseconds (0 or unset: no delay)
 *   SLOW_SYNC_DIRS    colon-separated directory prefixes (unset: every file)
 *   SLOW_SYNC_SERIAL  0 to let delayed syncs overlap (default 1)
 *
 * On exit it prints how many calls it delayed and for how long in total, so
 * a run reports its own sync count rather than leaving it to be inferred.
 *
 *   LD_PRELOAD=build/bins/slow_sync.so SLOW_SYNC_MS=20 \
 *   SLOW_SYNC_DIRS=/home/me/bench bench_phases -d /home/me/bench
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX_DIRS 8

typedef int (*sync_fn)(int);

static sync_fn          real_fsync;
static sync_fn          real_fdatasync;
static char             dirs[MAX_DIRS][PATH_MAX];
static size_t           n_dirs;
static int              all_files;
static long             delay_ms;
static int              serial = 1;
static pthread_mutex_t  dev_mu = PTHREAD_MUTEX_INITIALIZER;
static atomic_ulong     n_fsync, n_fdatasync, n_passed;
static atomic_ullong    slept_ns;

/* dlsym hands back an object pointer; copying it is the conversion POSIX
 * promises works, and -Wpedantic rejects the cast. */
static sync_fn lookup(const char *name)
{
    void *p = dlsym(RTLD_NEXT, name);
    sync_fn f = NULL;
    if (p) memcpy(&f, &p, sizeof(f));
    return f;
}

__attribute__((constructor)) static void slow_sync_init(void)
{
    real_fsync     = lookup("fsync");
    real_fdatasync = lookup("fdatasync");

    const char *ms = getenv("SLOW_SYNC_MS");
    delay_ms = ms ? strtol(ms, NULL, 10) : 0;
    if (delay_ms < 0) delay_ms = 0;

    const char *ser = getenv("SLOW_SYNC_SERIAL");
    if (ser && strcmp(ser, "0") == 0) serial = 0;

    const char *list = getenv("SLOW_SYNC_DIRS");
    if (!list || !*list) { all_files = 1; return; }
    const char *p = list;
    while (*p && n_dirs < MAX_DIRS) {
        const char *end = strchr(p, ':');
        size_t len = end ? (size_t)(end - p) : strlen(p);
        if (len > 0 && len < PATH_MAX) {
            char tmp[PATH_MAX];
            memcpy(tmp, p, len);
            tmp[len] = '\0';
            /* Compared against /proc/self/fd links, which are resolved. */
            if (!realpath(tmp, dirs[n_dirs]))
                snprintf(dirs[n_dirs], sizeof(dirs[n_dirs]), "%s", tmp);
            n_dirs++;
        }
        if (!end) break;
        p = end + 1;
    }
}

__attribute__((destructor)) static void slow_sync_report(void)
{
    if (delay_ms == 0) return;
    fprintf(stderr, "slow_sync: %lu fsync + %lu fdatasync delayed by %ld ms "
            "(%.2fs slept), %lu elsewhere not delayed\n",
            atomic_load(&n_fsync), atomic_load(&n_fdatasync), delay_ms,
            (double)atomic_load(&slept_ns) / 1e9, atomic_load(&n_passed));
}

static int is_target(int fd)
{
    if (all_files) return 1;
    char link[64], path[PATH_MAX];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, sizeof(path) - 1);
    if (n <= 0) return 0;
    path[n] = '\0';
    for (size_t i = 0; i < n_dirs; i++) {
        size_t len = strlen(dirs[i]);
        if (strncmp(path, dirs[i], len) == 0 && (path[len] == '/' || path[len] == '\0'))
            return 1;
    }
    return 0;
}

static void delay(void)
{
    struct timespec ts = { delay_ms / 1000, (delay_ms % 1000) * 1000000L };
    if (serial) pthread_mutex_lock(&dev_mu);
    while (nanosleep(&ts, &ts) != 0) { /* EINTR: sleep the rest */ }
    if (serial) pthread_mutex_unlock(&dev_mu);
    atomic_fetch_add(&slept_ns, (unsigned long long)delay_ms * 1000000ULL);
}

int fsync(int fd)
{
    if (delay_ms > 0 && is_target(fd)) {
        atomic_fetch_add(&n_fsync, 1);
        delay();
    } else {
        atomic_fetch_add(&n_passed, 1);
    }
    return real_fsync ? real_fsync(fd) : -1;
}

int fdatasync(int fd)
{
    if (delay_ms > 0 && is_target(fd)) {
        atomic_fetch_add(&n_fdatasync, 1);
        delay();
    } else {
        atomic_fetch_add(&n_passed, 1);
    }
    return real_fdatasync ? real_fdatasync(fd) : -1;
}
