#define _POSIX_C_SOURCE 200809L

#include "wal_sync.h"
#include "sync_latch.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static char            g_path[PATH_MAX];
static int             g_fd = -1;
static dev_t           g_dev;
static ino_t           g_ino;
static int             g_dir_synced;     /* for the file g_fd names */
static atomic_ulong    g_dir_syncs;
static void (*volatile g_before_sync)(void);

static void forget_locked(void)
{
    if (g_fd >= 0) close(g_fd);
    g_fd = -1;
    g_dir_synced = 0;
    g_path[0] = '\0';
}

void wal_sync_reset(void)
{
    pthread_mutex_lock(&g_mu);
    forget_locked();
    pthread_mutex_unlock(&g_mu);
}

unsigned long wal_sync_dir_syncs(void)
{
    return atomic_load(&g_dir_syncs);
}

void wal_sync_set_test_hook(void (*before_sync)(void))
{
    g_before_sync = before_sync;
}

static status_t sync_dir_of(const char *path)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash == dir) dir[1] = '\0';
    else if (slash) *slash = '\0';
    else snprintf(dir, sizeof(dir), ".");
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) return set_error_errno(ERR_IO, "state.db: cannot open %s to sync it", dir);
    int rc = sync_latch_fsync(dfd, dir);
    int saved = errno;
    close(dfd);
    if (rc != 0) {
        errno = saved;
        return set_error_errno(ERR_IO, "state.db: cannot sync %s, which holds the WAL", dir);
    }
    atomic_fetch_add(&g_dir_syncs, 1);
    return OK;
}

status_t wal_sync(const char *wal_path)
{
    if (!wal_path || !*wal_path) return set_error(ERR_INVALID, "wal_sync: no path");
    if (strlen(wal_path) >= sizeof(g_path))
        return set_error(ERR_INVALID, "wal_sync: path too long: %s", wal_path);

    pthread_mutex_lock(&g_mu);
    if (strcmp(g_path, wal_path) != 0) forget_locked();

    struct stat ps;
    if (stat(wal_path, &ps) != 0) {
        int saved = errno;
        /* No WAL: nothing uncheckpointed is on disk. The kept descriptor
         * named a file that is gone; a new one will need its entry synced. */
        forget_locked();
        pthread_mutex_unlock(&g_mu);
        if (saved == ENOENT) return OK;
        errno = saved;
        return set_error_errno(ERR_IO, "state.db: cannot stat %s", wal_path);
    }
    if (g_fd >= 0 && (ps.st_dev != g_dev || ps.st_ino != g_ino)) forget_locked();
    if (g_fd < 0) {
        int fd = open(wal_path, O_RDONLY | O_CLOEXEC);
        struct stat fs;
        if (fd < 0 || fstat(fd, &fs) != 0) {
            int saved = errno;
            if (fd >= 0) close(fd);
            pthread_mutex_unlock(&g_mu);
            if (saved == ENOENT) return OK;
            errno = saved;
            return set_error_errno(ERR_IO, "state.db: cannot open %s to sync it", wal_path);
        }
        if (fs.st_dev != ps.st_dev || fs.st_ino != ps.st_ino) {
            /* Replaced between the stat and the open: SQLite never does
             * that to an open database, so say so instead of guessing. */
            close(fd);
            pthread_mutex_unlock(&g_mu);
            return set_error(ERR_IO, "state.db: %s changed while it was being opened "
                                     "to sync it", wal_path);
        }
        snprintf(g_path, sizeof(g_path), "%s", wal_path);
        g_fd = fd;
        g_dev = fs.st_dev;
        g_ino = fs.st_ino;
        g_dir_synced = 0;
    }
    int fd = dup(g_fd);
    dev_t dev = g_dev;
    ino_t ino = g_ino;
    int need_dir = !g_dir_synced;
    pthread_mutex_unlock(&g_mu);
    if (fd < 0) return set_error_errno(ERR_IO, "state.db: cannot duplicate the WAL descriptor");

    void (*hook)(void) = g_before_sync;
    if (hook) hook();

    status_t st = OK;
    if (sync_latch_fdatasync(fd, wal_path) != 0)
        st = set_error_errno(ERR_IO, "state.db: cannot sync %s; the index is not "
                                     "durable yet", wal_path);
    close(fd);
    if (st != OK) return st;

    /* Still the file just synced? If the name moved on, the sync covered a
     * file that no longer holds the commits. */
    if (stat(wal_path, &ps) != 0 || ps.st_dev != dev || ps.st_ino != ino)
        return set_error(ERR_IO, "state.db: %s was replaced while it was being synced; "
                                 "the index is not durable yet", wal_path);

    if (need_dir) {
        st = sync_dir_of(wal_path);
        if (st != OK) return st;
        pthread_mutex_lock(&g_mu);
        if (g_fd >= 0 && g_dev == dev && g_ino == ino) g_dir_synced = 1;
        pthread_mutex_unlock(&g_mu);
    }
    return OK;
}
