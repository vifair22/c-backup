#define _GNU_SOURCE

#include "disk_stream.h"
#include "nbd_client.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define NBD_WINDOW  ((uint64_t)1 << 30)
#define NBD_EXT_MAX 512

/* ---- a raw file ---- */

typedef struct { int fd; uint64_t size; } fstream_t;

static status_t f_extent(void *ctx, uint64_t from, sp_ext_type_t *type, uint64_t *len, int *done)
{
    fstream_t *f = ctx;
    *done = from >= f->size;
    if (*done) return OK;
    off_t d = lseek(f->fd, (off_t)from, SEEK_DATA);
    if (d < 0 && errno == ENXIO) {                     /* a hole to the end */
        *type = SP_EXT_ZERO;
        *len = f->size - from;
        return OK;
    }
    if (d < 0) return set_error_errno(ERR_IO, "disk: seek data");
    if ((uint64_t)d > from) {
        *type = SP_EXT_ZERO;
        *len = ((uint64_t)d < f->size ? (uint64_t)d : f->size) - from;
        return OK;
    }
    off_t h = lseek(f->fd, (off_t)from, SEEK_HOLE);
    if (h < 0) return set_error_errno(ERR_IO, "disk: seek hole");
    uint64_t end = (uint64_t)h < f->size ? (uint64_t)h : f->size;
    *type = SP_EXT_DATA;
    *len = end > from ? end - from : f->size - from;
    return OK;
}

static status_t f_read(void *ctx, void *buf, size_t n, uint64_t off)
{
    fstream_t *f = ctx;
    uint8_t *p = buf;
    size_t done = 0;
    while (done < n) {
        ssize_t r = pread(f->fd, p + done, n - done, (off_t)(off + done));
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) return set_error_errno(ERR_IO, "disk: read");
        if (r == 0) {
            /* Past the file's end inside the virtual size reads as zeroes. */
            memset(p + done, 0, n - done);
            break;
        }
        done += (size_t)r;
    }
    return OK;
}

static void f_close(void *ctx)
{
    fstream_t *f = ctx;
    close(f->fd);
    free(f);
}

static status_t no_changes(void *ctx, uint64_t from, uint64_t *s, uint64_t *e, int *done)
{
    (void)ctx; (void)from; (void)s; (void)e;
    *done = 1;
    return set_error(ERR_INVALID, "disk: no changed ranges in this build");
}

status_t disk_stream_file(const char *path, uint64_t size, sp_block_stream_t *out)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0 && errno == ELOOP) fd = open(path, O_RDONLY | O_CLOEXEC);   /* /mnt/user/domains is a symlink */
    if (fd < 0) return set_error_errno(ERR_IO, "disk: open %s", path);
    struct stat sb;
    if (fstat(fd, &sb) != 0 || !S_ISREG(sb.st_mode)) {
        close(fd);
        return set_error(ERR_INVALID, "disk: %s is not a regular file", path);
    }
    fstream_t *f = calloc(1, sizeof(*f));
    if (!f) { close(fd); return set_error(ERR_NOMEM, "disk: stream"); }
    f->fd = fd;
    f->size = size ? size : (uint64_t)sb.st_size;
    *out = (sp_block_stream_t){ f_extent, no_changes, f_read, f_close, f };
    /* The stream owns f; its close frees it. */
    return OK; // cppcheck-suppress memleak
}

/* ---- an NBD export ---- */

typedef struct {
    nbd_client_t   *c;
    pthread_mutex_t mu;
    uint64_t        size;
    nbd_extent_t    ext[NBD_EXT_MAX];
    size_t          n, at;          /* extents of the current window, the next to hand out */
    uint64_t        win_off;        /* where ext[at] starts */
} nstream_t;

static status_t n_extent(void *ctx, uint64_t from, sp_ext_type_t *type, uint64_t *len, int *done)
{
    nstream_t *s = ctx;
    *done = from >= s->size;
    if (*done) return OK;
    pthread_mutex_lock(&s->mu);
    status_t st = OK;
    /* Forward only: skip what lies before from, refill past the window. */
    while (st == OK) {
        while (s->at < s->n && s->win_off + s->ext[s->at].len <= from) {
            s->win_off += s->ext[s->at].len;
            s->at++;
        }
        if (s->at < s->n) break;
        uint64_t w = s->size - from < NBD_WINDOW ? s->size - from : NBD_WINDOW;
        st = nbd_block_status(s->c, from, w, 0, s->ext, NBD_EXT_MAX, &s->n);
        s->at = 0;
        s->win_off = from;
    }
    if (st == OK) {
        uint64_t end = s->win_off + s->ext[s->at].len;
        if (end > s->size) end = s->size;
        *type = (s->ext[s->at].flags & NBD_STATE_ZERO) ? SP_EXT_ZERO : SP_EXT_DATA;
        *len = end - from;
    }
    pthread_mutex_unlock(&s->mu);
    return st;
}

static status_t n_read(void *ctx, void *buf, size_t n, uint64_t off)
{
    nstream_t *s = ctx;
    pthread_mutex_lock(&s->mu);
    status_t st = nbd_read(s->c, off, buf, n);
    pthread_mutex_unlock(&s->mu);
    return st;
}

static void n_close(void *ctx)
{
    nstream_t *s = ctx;
    nbd_close(s->c);
    pthread_mutex_destroy(&s->mu);
    free(s);
}

static unsigned g_depth, g_request_kb;

void disk_stream_nbd_tuning(unsigned depth, unsigned request_kb)
{
    g_depth = depth > 64 ? 64 : depth;
    g_request_kb = request_kb > 32768 ? 32768 : request_kb;
}

status_t disk_stream_nbd(const char *socket, const char *export_name, uint64_t size,
                         sp_block_stream_t *out)
{
    nbd_open_opts_t o = { socket, export_name ? export_name : "", NULL, 0, g_depth, g_request_kb };
    nbd_client_t *c = NULL;
    status_t st = nbd_open(&o, &c);
    if (st != OK) return st;
    if (size && nbd_size(c) != size) {
        st = set_error(ERR_IO, "disk: the export is %llu bytes, the disk %llu",
                       (unsigned long long)nbd_size(c), (unsigned long long)size);
        nbd_close(c);
        return st;
    }
    nstream_t *s = calloc(1, sizeof(*s));
    if (!s) { nbd_close(c); return set_error(ERR_NOMEM, "disk: stream"); }
    s->c = c;
    s->size = nbd_size(c);
    pthread_mutex_init(&s->mu, NULL);
    *out = (sp_block_stream_t){ n_extent, no_changes, n_read, n_close, s };
    return OK; // cppcheck-suppress memleak
}
