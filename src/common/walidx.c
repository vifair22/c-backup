#define _POSIX_C_SOURCE 200809L

#include "walidx.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define WALIDX_VERSION   3007000u
#define WALIDX_HDR_SIZE  48
#define WALIDX_ISINIT    12
#define WALIDX_MXFRAME   16
#define WALIDX_SALT      32
#define WALIDX_BACKFILL  96
#define WALIDX_READ      (WALIDX_BACKFILL + 4)

/* One kept descriptor per -shm this process reads; a daemon has one. */
#define SLOTS 64
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static struct { char path[PATH_MAX]; int fd; dev_t dev; ino_t ino; } g_slot[SLOTS];
static int g_nslots;

static uint32_t u32_at(const unsigned char *b, size_t off)
{
    uint32_t v;
    memcpy(&v, b + off, sizeof(v));
    return v;
}

/* The kept descriptor for shm, opened or replaced as needed. Under g_mu.
 * -1 with *err set to the errno of a stat or open that failed, or 0 when
 * there is just nothing to read yet. */
static int shm_fd(const char *shm, int *err)
{
    struct stat ps;
    *err = 0;
    if (stat(shm, &ps) != 0) { *err = errno; return -1; }
    int i;
    for (i = 0; i < g_nslots; i++) if (strcmp(g_slot[i].path, shm) == 0) break;
    if (i < g_nslots && g_slot[i].fd >= 0 &&
        g_slot[i].dev == ps.st_dev && g_slot[i].ino == ps.st_ino)
        return g_slot[i].fd;
    int fd = open(shm, O_RDONLY | O_CLOEXEC);
    struct stat fs;
    if (fd < 0) { *err = errno; return -1; }
    if (fstat(fd, &fs) != 0 || fs.st_dev != ps.st_dev || fs.st_ino != ps.st_ino) {
        /* Never this file's descriptor to drop locks with: a fresh open
         * of a file that changed under us; leave it open and unused
         * rather than close it while it may be live. */
        return -1;
    }
    if (i == g_nslots) {
        if (g_nslots == SLOTS) {
            /* Full: reuse a slot whose file is gone. Closing its
             * descriptor drops locks only on that deleted file. */
            for (i = 0; i < SLOTS; i++) {
                struct stat os;
                if (stat(g_slot[i].path, &os) != 0 || os.st_dev != g_slot[i].dev ||
                    os.st_ino != g_slot[i].ino) break;
            }
            if (i == SLOTS) return fd;   /* all live: read once, never closed */
        } else {
            g_nslots++;
        }
        if (i < g_nslots && g_slot[i].fd > 0 && g_slot[i].path[0]) close(g_slot[i].fd);
        snprintf(g_slot[i].path, sizeof(g_slot[i].path), "%s", shm);
    } else if (g_slot[i].fd >= 0) {
        /* The path names another file now: the old one is deleted or
         * replaced, and nobody's locks on it matter any more. */
        close(g_slot[i].fd);
    }
    g_slot[i].fd = fd;
    g_slot[i].dev = fs.st_dev;
    g_slot[i].ino = fs.st_ino;
    return fd;
}

int walidx_read(const char *db_path, walidx_t *out)
{
    char shm[PATH_MAX];
    if (!db_path || snprintf(shm, sizeof(shm), "%s-shm", db_path) >= (int)sizeof(shm))
        return -1;
    unsigned char b[WALIDX_READ];
    int err = 0;
    pthread_mutex_lock(&g_mu);
    int fd = shm_fd(shm, &err);
    ssize_t n = fd >= 0 ? pread(fd, b, sizeof(b), 0) : -1;
    if (fd >= 0 && n < 0) err = errno;
    pthread_mutex_unlock(&g_mu);
    /* No -shm yet is normal (nothing written); anything else is a reader
     * that cannot see the WAL at all (B215). */
    if (err && err != ENOENT) { errno = err; return -2; }
    if (n != (ssize_t)sizeof(b)) return -1;
    if (u32_at(b, 0) != WALIDX_VERSION || b[WALIDX_ISINIT] != 1) return -1;
    if (memcmp(b, b + WALIDX_HDR_SIZE, WALIDX_HDR_SIZE) != 0) return -1;
    uint32_t mx = u32_at(b, WALIDX_MXFRAME);
    uint32_t bf = u32_at(b, WALIDX_BACKFILL);
    if (bf > mx) bf = mx;     /* between a restart and its backfill reset */
    out->frames = mx;
    out->backfilled = bf;
    out->salt = u32_at(b, WALIDX_SALT);
    return 0;
}

void walidx_close_all(void)
{
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < g_nslots; i++) {
        if (g_slot[i].fd >= 0) close(g_slot[i].fd);
        g_slot[i].fd = -1;
        g_slot[i].path[0] = '\0';
    }
    g_nslots = 0;
    pthread_mutex_unlock(&g_mu);
}
