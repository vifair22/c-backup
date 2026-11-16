#define _GNU_SOURCE

#include "open_scanned.h"

#include <linux/openat2.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

/*
 * Why beneath the root and not from a pinned fd per directory: the scan
 * walks with one fd per level and lets go of it, and holding one per
 * directory through the store phase would be one fd per directory of the
 * tree, tens of thousands on the trees this runs on. One fd per source and
 * one openat2 per file costs a path walk the kernel does anyway.
 */
typedef struct {
    const char *path;     /* the root entry's path; not owned */
    uint32_t    len;
    dev_t       dev;
    ino_t       ino;
    int         fd;       /* -1 until opened */
    int         failed;   /* opened and found wrong, or could not open */
    char        why[160];
} source_root_t;

struct scan_roots {
    pthread_mutex_t mu;   /* the lazy open; workers open files at once */
    source_root_t  *r;
    unsigned        n;
};

status_t scan_roots_new(const scan_result_t *scan, scan_roots_t **out)
{
    *out = NULL;
    scan_roots_t *t = calloc(1, sizeof(*t));
    if (!t) return set_error(ERR_NOMEM, "backup: source root table");
    pthread_mutex_init(&t->mu, NULL);
    unsigned n = 0;
    for (uint32_t i = 0; i < scan->count; i++) {
        const scan_entry_t *e = &scan->entries[i];
        if (e->root_len && strlen(e->path) == e->root_len) n++;
    }
    if (n) {
        t->r = calloc(n, sizeof(*t->r));
        if (!t->r) {
            pthread_mutex_destroy(&t->mu);
            free(t);
            return set_error(ERR_NOMEM, "backup: source root table");
        }
    }
    for (uint32_t i = 0; i < scan->count; i++) {
        const scan_entry_t *e = &scan->entries[i];
        if (!e->root_len || strlen(e->path) != e->root_len) continue;
        source_root_t *r = &t->r[t->n++];
        r->path = e->path;
        r->len  = e->root_len;
        r->dev  = e->st.st_dev;
        r->ino  = e->st.st_ino;
        r->fd   = -1;
    }
    *out = t;
    return OK;
}

void scan_roots_free(scan_roots_t *t)
{
    if (!t) return;
    for (unsigned k = 0; k < t->n; k++)
        if (t->r[k].fd != -1) close(t->r[k].fd);
    free(t->r);
    pthread_mutex_destroy(&t->mu);
    free(t);
}

/* The open root an entry was scanned under, or NULL with why filled. The
 * fd, once open, is never closed before scan_roots_free, so a caller may
 * use it after the lock is let go. */
static source_root_t *root_of(scan_roots_t *t, const scan_entry_t *e,
                              char *why, size_t why_sz)
{
    source_root_t *r = NULL;
    for (unsigned k = 0; k < t->n && !r; k++)
        if (t->r[k].len == e->root_len &&
            memcmp(t->r[k].path, e->path, e->root_len) == 0)
            r = &t->r[k];
    if (!r) {
        snprintf(why, why_sz, "cannot open: its source root is not in the scan");
        return NULL;
    }
    pthread_mutex_lock(&t->mu);
    if (r->fd == -1 && !r->failed) {
        int fd = open(r->path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        struct stat st;
        if (fd == -1) {
            snprintf(r->why, sizeof(r->why), "cannot open its source root: %s",
                     strerror(errno));
            r->failed = 1;
        } else if (fstat(fd, &st) != 0 || st.st_dev != r->dev || st.st_ino != r->ino) {
            snprintf(r->why, sizeof(r->why), "its source root %s was replaced "
                     "since the scan", r->path);
            close(fd);
            r->failed = 1;
        } else {
            r->fd = fd;
        }
    }
    int failed = r->failed;
    if (failed) snprintf(why, why_sz, "%s", r->why);
    pthread_mutex_unlock(&t->mu);
    return failed ? NULL : r;
}

/* Whether openat2 is there; known after the first call that needed it. */
static atomic_int openat2_missing = 0;

/*
 * Open `rel` beneath dirfd as the store must: no symlink, no escape, no
 * mount crossing when one_fs (root_dev is the root's device). -1 with
 * errno: ELOOP for a symlink on the way or at the end, EXDEV for a mount
 * crossed or an escape.
 */
static int open_beneath(int dirfd, const char *rel, int one_fs, dev_t root_dev)
{
    const int flags = O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW;
    if (!atomic_load(&openat2_missing)) {
        struct open_how how;
        memset(&how, 0, sizeof(how));
        how.flags   = (uint64_t)flags;
        how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS |
                      (one_fs ? RESOLVE_NO_XDEV : 0);
        long fd = syscall(SYS_openat2, dirfd, rel, &how, sizeof(how));
        if (fd >= 0) return (int)fd;
        if (errno != ENOSYS && errno != EPERM) return -1;
        atomic_store(&openat2_missing, 1);
    }
    char buf[PATH_MAX];
    if (snprintf(buf, sizeof(buf), "%s", rel) >= (int)sizeof(buf)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int cur = dirfd;
    char *save = NULL;
    char *c = strtok_r(buf, "/", &save);
    while (c) {
        char *next = strtok_r(NULL, "/", &save);
        if (strcmp(c, ".") == 0 || strcmp(c, "..") == 0) {
            if (cur != dirfd) close(cur);
            errno = EXDEV;
            return -1;
        }
        int fd = openat(cur, c, next ? (O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
                                     : flags);
        int err = errno;
        /* O_DIRECTORY | O_NOFOLLOW on a symlink says ENOTDIR; call it what
         * it is. */
        struct stat ls;
        if (fd == -1 && next && err == ENOTDIR &&
            fstatat(cur, c, &ls, AT_SYMLINK_NOFOLLOW) == 0 && S_ISLNK(ls.st_mode))
            err = ELOOP;
        if (cur != dirfd) close(cur);
        if (fd == -1) { errno = err; return -1; }
        struct stat st;
        if (one_fs && (fstat(fd, &st) != 0 || st.st_dev != root_dev)) {
            close(fd);
            errno = EXDEV;
            return -1;
        }
        cur = fd;
        c = next;
    }
    if (cur == dirfd) { errno = EINVAL; return -1; }
    return cur;
}

/* What a path names now, for a skip reason. */
static const char *file_kind(mode_t m)
{
    if (S_ISLNK(m))  return "a symlink";
    if (S_ISDIR(m))  return "a directory";
    if (S_ISFIFO(m)) return "a FIFO";
    if (S_ISSOCK(m)) return "a socket";
    if (S_ISCHR(m))  return "a character device";
    if (S_ISBLK(m))  return "a block device";
    return "something other than a regular file";
}

int open_scanned(scan_roots_t *roots, const scan_entry_t *e, int one_fs,
                 struct stat *fst, char *why, size_t why_sz)
{
    int fd;
    if (roots && e->root_len && e->path[e->root_len] == '/') {
        source_root_t *r = root_of(roots, e, why, why_sz);
        if (!r) return -1;
        const char *rel = e->path + e->root_len + 1;
        fd = open_beneath(r->fd, rel, one_fs, r->dev);
        if (fd == -1) {
            int err = errno;
            struct stat lst;
            if (err == ELOOP && fstatat(r->fd, rel, &lst, AT_SYMLINK_NOFOLLOW) == 0 &&
                S_ISLNK(lst.st_mode))
                snprintf(why, why_sz, "no longer a regular file (now a symlink)");
            else if (err == ELOOP)
                snprintf(why, why_sz, "a directory on its path is now a symlink; "
                         "not followed");
            else if (err == EXDEV)
                snprintf(why, why_sz, "its path now crosses a mount point or leaves "
                         "the source; not followed");
            else
                /* A file that vanished between scan and store is normal on a
                 * live filesystem; it is a warning, not a failed backup. */
                snprintf(why, why_sz, "cannot open: %s",
                         err == ENOENT ? "it no longer exists" : strerror(err));
            return -1;
        }
    } else {
        /* The source root itself is this file: its path is the operator's,
         * and only its last component must not be a symlink now. */
        fd = open(e->path, O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW);
        if (fd == -1) {
            int err = errno;
            if (err == ELOOP)
                snprintf(why, why_sz, "no longer a regular file (now a symlink)");
            else
                snprintf(why, why_sz, "cannot open: %s",
                         err == ENOENT ? "it no longer exists" : strerror(err));
            return -1;
        }
    }
    /* A directory opens read-only without complaint, and on btrfs SEEK_DATA
     * on it answers ENXIO, "no data from here on", which once stored the
     * scan's size in zeros (OWN-41). */
    if (fstat(fd, fst) != 0) {
        snprintf(why, why_sz, "cannot open: fstat: %s", strerror(errno));
        close(fd);
        return -1;
    }
    if (!S_ISREG(fst->st_mode)) {
        snprintf(why, why_sz, "no longer a regular file (now %s)", file_kind(fst->st_mode));
        close(fd);
        return -1;
    }
    return fd;
}
