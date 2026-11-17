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

/*
 * A shut-off VM's image read offline (vm-backup.md 2.4) is consistent only
 * if nothing wrote it while it was read: the VM started, or something else
 * opened the image. Nothing prevents that, so it is detected: the image's
 * size, mtime and ctime when the stream opened against the same once the
 * last byte has been read. block_source reads a disk forward and
 * synchronously, so "the last byte" is a read or a zero extent reaching the
 * disk's end, and at that point every earlier byte has been read. A change
 * fails the read, which fails the disk and its unit: a torn image is never
 * stored as consistent.
 */
typedef struct {
    int         fd;            /* -1: nothing watched */
    struct stat before;
    char        path[512];
} watch_t;

static status_t watch_open(watch_t *w, const char *path)
{
    w->fd = -1;
    if (!path || !path[0]) return OK;
    snprintf(w->path, sizeof(w->path), "%s", path);
    w->fd = open(path, O_RDONLY | O_CLOEXEC);
    if (w->fd < 0) return set_error_errno(ERR_IO, "disk: open %s to watch it", path);
    if (fstat(w->fd, &w->before) != 0) {
        close(w->fd);
        w->fd = -1;
        return set_error_errno(ERR_IO, "disk: stat %s", path);
    }
    return OK;
}

static status_t watch_check(const watch_t *w)
{
    if (w->fd < 0) return OK;
    struct stat now;
    if (fstat(w->fd, &now) != 0) return set_error_errno(ERR_IO, "disk: stat %s", w->path);
    if (now.st_size != w->before.st_size ||
        now.st_mtim.tv_sec != w->before.st_mtim.tv_sec || now.st_mtim.tv_nsec != w->before.st_mtim.tv_nsec ||
        now.st_ctim.tv_sec != w->before.st_ctim.tv_sec || now.st_ctim.tv_nsec != w->before.st_ctim.tv_nsec)
        return set_error(ERR_IO, "disk: %s changed while it was read offline (was the VM started, "
                         "or the image written?); not stored as a consistent image", w->path);
    return OK;
}

static void watch_close(watch_t *w)
{
    if (w->fd >= 0) close(w->fd);
    w->fd = -1;
}

/* ---- a raw file ---- */

typedef struct { int fd; uint64_t size; watch_t w; } fstream_t;

static status_t f_extent(void *ctx, uint64_t from, sp_ext_type_t *type, uint64_t *len, int *done)
{
    fstream_t *f = ctx;
    *done = from >= f->size;
    if (*done) return OK;
    off_t d = lseek(f->fd, (off_t)from, SEEK_DATA);
    if (d < 0 && errno == ENXIO) {                     /* a hole to the end */
        *type = SP_EXT_ZERO;
        *len = f->size - from;
        return watch_check(&f->w);
    }
    if (d < 0) return set_error_errno(ERR_IO, "disk: seek data");
    if ((uint64_t)d > from) {
        *type = SP_EXT_ZERO;
        *len = ((uint64_t)d < f->size ? (uint64_t)d : f->size) - from;
        return from + *len >= f->size ? watch_check(&f->w) : OK;
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
    return off + n >= f->size ? watch_check(&f->w) : OK;
}

static void f_close(void *ctx)
{
    fstream_t *f = ctx;
    watch_close(&f->w);
    close(f->fd);
    free(f);
}

static status_t no_changes(void *ctx, uint64_t from, uint64_t *s, uint64_t *e, int *done)
{
    (void)ctx; (void)from; (void)s; (void)e;
    *done = 1;
    return set_error(ERR_INVALID, "disk: no changed ranges: this disk has no dirty bitmap");
}

status_t disk_stream_file(const char *path, uint64_t size, int offline, sp_block_stream_t *out)
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
    status_t wst = watch_open(&f->w, offline ? path : NULL);
    if (wst != OK) { close(fd); free(f); return wst; }
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
    /* The dirty bitmap, the same way, in its own window: changed() may
     * start again from 0 (the estimate, then the delta, then the audit). */
    nbd_extent_t    bext[NBD_EXT_MAX];
    size_t          bn, bat;
    uint64_t        bwin_off;
    watch_t         w;              /* the image behind a qemu-nbd, offline */
} nstream_t;

static status_t n_extent(void *ctx, uint64_t from, sp_ext_type_t *type, uint64_t *len, int *done)
{
    nstream_t *s = ctx;
    *done = from >= s->size;
    if (*done) return OK;
    pthread_mutex_lock(&s->mu);
    status_t st = OK;
    /* Forward: skip what lies before from, refill past the window, and
     * start over at from if it lies before the window (a seek back). */
    if (s->at < s->n && from < s->win_off) s->n = s->at = 0;
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
        if (*type == SP_EXT_ZERO && end >= s->size) st = watch_check(&s->w);
    }
    pthread_mutex_unlock(&s->mu);
    return st;
}

static status_t n_read(void *ctx, void *buf, size_t n, uint64_t off)
{
    nstream_t *s = ctx;
    pthread_mutex_lock(&s->mu);
    status_t st = nbd_read(s->c, off, buf, n);
    if (st == OK && off + n >= s->size) st = watch_check(&s->w);
    pthread_mutex_unlock(&s->mu);
    return st;
}

/* The next dirty range at or after from, merged across the window's
 * consecutive dirty extents. */
static status_t n_changed(void *ctx, uint64_t from, uint64_t *rs, uint64_t *re, int *done)
{
    nstream_t *s = ctx;
    *done = 0;
    if (!nbd_has_bitmap(s->c)) return set_error(ERR_INVALID, "disk: the export has no dirty bitmap");
    pthread_mutex_lock(&s->mu);
    status_t st = OK;
    int found = 0;
    uint64_t pos = from;
    if (s->bat < s->bn && pos < s->bwin_off) s->bn = s->bat = 0;
    while (st == OK && pos < s->size) {
        while (s->bat < s->bn && s->bwin_off + s->bext[s->bat].len <= pos) {
            s->bwin_off += s->bext[s->bat].len;
            s->bat++;
        }
        if (s->bat >= s->bn) {
            if (found) break;       /* a range ending at the window's end: done with it */
            uint64_t w = s->size - pos < NBD_WINDOW ? s->size - pos : NBD_WINDOW;
            st = nbd_block_status(s->c, pos, w, 1, s->bext, NBD_EXT_MAX, &s->bn);
            s->bat = 0;
            s->bwin_off = pos;
            if (st == OK && s->bn == 0) st = set_error(ERR_IO, "disk: the dirty bitmap described nothing at %llu",
                                                       (unsigned long long)pos);
            continue;
        }
        const nbd_extent_t *x = &s->bext[s->bat];
        uint64_t xs = s->bwin_off > pos ? s->bwin_off : pos;
        uint64_t xe = s->bwin_off + x->len;
        if (xe > s->size) xe = s->size;
        int dirty = (x->flags & NBD_STATE_DIRTY) != 0;
        if (dirty) {
            if (!found) { *rs = xs; found = 1; }
            *re = xe;
        } else if (found) {
            break;
        }
        pos = xe;
    }
    pthread_mutex_unlock(&s->mu);
    if (st == OK && !found) *done = 1;
    return st;
}

static void n_close(void *ctx)
{
    nstream_t *s = ctx;
    watch_close(&s->w);
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
                         const char *watch, const char *bitmap, int *has_bitmap,
                         sp_block_stream_t *out)
{
    if (has_bitmap) *has_bitmap = 0;
    nbd_open_opts_t o = { socket, export_name ? export_name : "", bitmap && *bitmap ? bitmap : NULL,
                          0, g_depth, g_request_kb };
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
    st = watch_open(&s->w, watch);
    if (st != OK) { nbd_close(c); free(s); return st; }
    pthread_mutex_init(&s->mu, NULL);
    int hb = nbd_has_bitmap(c);
    if (has_bitmap) *has_bitmap = hb;
    *out = (sp_block_stream_t){ n_extent, hb ? n_changed : no_changes, n_read, n_close, s };
    return OK; // cppcheck-suppress memleak
}
