#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "restore.h"
#include "metablob.h"

#include "manifest.h"
#include "object.h"
#include "chunklist.h"
#include "read_budget.h"
#include "util.h"

#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <stdarg.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <linux/openat2.h>
#include <stdatomic.h>
#include <stdint.h>
#include <sys/acl.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/sysmacros.h>
#include <sys/time.h>
#include <sys/xattr.h>
#include <unistd.h>

/*
 * Paths are rebuilt by walking parent links rather than being stored, because
 * nodes hold only a name and a parent id. Depth is bounded by the filesystem
 * that produced them, and the loop guard below is a corruption check rather
 * than a real limit.
 */
#define MAX_PATH_DEPTH 256

/*
 * Is this node inside the requested subpath?
 *
 * Node paths are stored with the leading slash stripped, because scan_tree
 * records absolute paths and the restore rebuilds them under dest. A caller
 * naturally passes the absolute path they see in the browser, so both forms
 * are accepted rather than silently matching nothing.
 *
 * Matching stops at a path boundary: a plain prefix test would let a subpath
 * of "src/doc" pull in "src/documents" too.
 */
/*
 * With strip_subpath, the subpath's contents go straight into dest: rel is
 * cut to what lies below the subpath, in place, so every write beneath the
 * destination's directory fds (dest_parent) lands there. Returns 1 when
 * rel is the subpath itself, which is dest and is not written.
 */
static int strip_rel(char *rel, const restore_opts_t *o)
{
    if (!o || !o->strip_subpath || !o->subpath) return 0;
    const char *sp = o->subpath;
    while (*sp == '/') sp++;
    char *r = rel;
    while (*r == '/') r++;
    size_t n = strlen(sp);
    while (n > 0 && sp[n - 1] == '/') n--;
    if (strncmp(r, sp, n) == 0 && (r[n] == '\0' || r[n] == '/')) r += n;
    while (*r == '/') r++;
    memmove(rel, r, strlen(r) + 1);
    return rel[0] == '\0';
}

/* Where rel lands under dest. -1 if too long. */
static int dest_path(char *full, size_t sz, const char *dest, const char *rel, const restore_opts_t *o)
{
    (void)o;
    return snprintf(full, sz, "%s/%s", dest, rel) >= (int)sz ? -1 : 0;
}

static int path_within(const char *rel, const char *subpath)
{
    while (*subpath == '/') subpath++;
    while (*rel == '/') rel++;

    size_t n = strlen(subpath);
    while (n > 0 && subpath[n - 1] == '/') n--;
    if (n == 0) return 1;   /* the whole snapshot */

    if (strncmp(rel, subpath, n) != 0) return 0;
    return rel[n] == '\0' || rel[n] == '/';
}

/*
 * First path restored for each inode identity, so later members of the group
 * can be linked to it rather than copied. Open addressing, sized from the
 * node count: a tree with no hardlinks pays one allocation and nothing else.
 */
typedef struct {
    uint64_t *keys;    /* inode identity, 0 means empty */
    char    **paths;
    uint32_t  mask;
} hardlink_map_t;

static void hardlink_map_free(hardlink_map_t *m)
{
    if (m->paths)
        for (uint32_t i = 0; i <= m->mask; i++) free(m->paths[i]);
    free(m->keys);
    free(m->paths);
    m->keys = NULL;
    m->paths = NULL;
}

static int hardlink_map_init(hardlink_map_t *m, uint32_t n)
{
    uint32_t cap = 16;
    while (cap < n * 2) cap <<= 1;
    m->keys  = calloc(cap, sizeof(*m->keys));
    m->paths = calloc(cap, sizeof(*m->paths));
    if (!m->keys || !m->paths) { hardlink_map_free(m); return -1; }
    m->mask = cap - 1;
    return 0;
}

static uint32_t hardlink_slot(const hardlink_map_t *m, uint64_t key)
{
    uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
    while (m->keys[h] != 0 && m->keys[h] != key) h = (h + 1) & m->mask;
    return h;
}

static const char *hardlink_lookup(const hardlink_map_t *m, uint64_t key)
{
    if (!m->keys) return NULL;
    uint32_t h = hardlink_slot(m, key);
    return m->keys[h] == key ? m->paths[h] : NULL;
}

static void hardlink_remember(hardlink_map_t *m, uint64_t key, const char *path)
{
    if (!m->keys) return;
    uint32_t h = hardlink_slot(m, key);
    if (m->keys[h] == key) return;
    char *dup = strdup(path);
    if (!dup) return;
    m->keys[h]  = key;
    m->paths[h] = dup;
}

/*
 * How many snapshot members each inode group has. Every regular file
 * carries an inode identity, so size-one groups are the overwhelming
 * majority and the map exists to find the few that are not.
 */
typedef struct {
    uint64_t *keys;
    uint32_t *counts;
    uint32_t  mask;
} groupcount_t;

static int groupcount_init(groupcount_t *m, uint32_t n)
{
    uint32_t cap = 16;
    while (cap < n * 2) cap <<= 1;
    m->keys   = calloc(cap, sizeof(*m->keys));
    m->counts = calloc(cap, sizeof(*m->counts));
    if (!m->keys || !m->counts) {
        free(m->keys); free(m->counts);
        m->keys = NULL; m->counts = NULL;
        return -1;
    }
    m->mask = cap - 1;
    return 0;
}

static void groupcount_free(groupcount_t *m)
{
    free(m->keys);
    free(m->counts);
    m->keys = NULL;
    m->counts = NULL;
}

static uint32_t groupcount_slot(const groupcount_t *m, uint64_t key)
{
    uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
    while (m->keys[h] != 0 && m->keys[h] != key) h = (h + 1) & m->mask;
    return h;
}

static void groupcount_add(groupcount_t *m, uint64_t key)
{
    if (!m->keys || key == 0) return;
    uint32_t h = groupcount_slot(m, key);
    m->keys[h] = key;
    m->counts[h]++;
}

static uint32_t groupcount_get(const groupcount_t *m, uint64_t key)
{
    if (!m->keys || key == 0) return 0;
    uint32_t h = groupcount_slot(m, key);
    return m->keys[h] == key ? m->counts[h] : 0;
}

static int hash_is_zero_hex(const char *hex)
{
    for (const char *c = hex; *c; c++) if (*c != '0') return 0;
    return 1;
}

/*
 * The destination is written relative to directory fds, never by path.
 *
 * By path, anything already in the destination was followed: a directory
 * entry whose name held a symlink got mkdir's EEXIST and then chown/chmod
 * on the link's target, and every file under it was created wherever the
 * link pointed - with --overwrite, renamed over what was there. The daemon
 * runs as root, so a symlink planted in a restore target could have the
 * restore rewrite /etc (B128's other half). Now every entry's parent is
 * opened beneath the destination with no symlink anywhere on the way
 * (dest_parent), and the entry itself is made and changed through that fd
 * with *at calls that do not follow its last component either.
 *
 * The destination path itself is the operator's and may run through
 * symlinks; nothing below it is followed.
 */

/* Whether openat2 is there: ENOSYS on an old kernel, and in a container
 * whose seccomp profile predates it. Known after the first call. */
static _Atomic int openat2_missing = 0;

/* Open the directory `rel` beneath dirfd with no symlink on the way, and no
 * ".." out of it. */
static int open_beneath(int dirfd, const char *rel)
{
    if (!openat2_missing) {
        struct open_how how;
        memset(&how, 0, sizeof(how));
        how.flags   = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
        how.resolve = RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS;
        long fd = syscall(SYS_openat2, dirfd, rel, &how, sizeof(how));
        if (fd >= 0) return (int)fd;
        if (errno != ENOSYS && errno != EPERM) return -1;
        openat2_missing = 1;
    }
    /* One component at a time, each O_NOFOLLOW. "." and ".." are refused
     * rather than resolved: no snapshot path contains them. */
    char buf[PATH_MAX];
    if (snprintf(buf, sizeof(buf), "%s", rel) >= (int)sizeof(buf)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int cur = dup(dirfd);
    if (cur == -1) return -1;
    char *save = NULL;
    for (char *c = strtok_r(buf, "/", &save); c; c = strtok_r(NULL, "/", &save)) {
        if (strcmp(c, ".") == 0 || strcmp(c, "..") == 0) {
            close(cur);
            errno = EXDEV;
            return -1;
        }
        int next = openat(cur, c, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        int err = errno;
        close(cur);
        if (next == -1) { errno = err; return -1; }
        cur = next;
    }
    return cur;
}

/* mkdir -p beneath dirfd, never through a symlink. */
static int make_beneath(int dirfd, const char *rel)
{
    char buf[PATH_MAX];
    if (snprintf(buf, sizeof(buf), "%s", rel) >= (int)sizeof(buf)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int cur = dup(dirfd);
    if (cur == -1) return -1;
    char *save = NULL;
    for (char *c = strtok_r(buf, "/", &save); c; c = strtok_r(NULL, "/", &save)) {
        if (strcmp(c, ".") == 0 || strcmp(c, "..") == 0) {
            close(cur);
            errno = EXDEV;
            return -1;
        }
        if (mkdirat(cur, c, 0755) != 0 && errno != EEXIST) {
            int err = errno;
            close(cur);
            errno = err;
            return -1;
        }
        int next = openat(cur, c, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        int err = errno;
        close(cur);
        if (next == -1) { errno = err; return -1; }
        cur = next;
    }
    close(cur);
    return 0;
}

/*
 * The parent directory of one entry, as an fd. Entries come in scan order,
 * so siblings follow each other and the last parent is kept open. Not
 * owned by the caller; valid until the next call on the same cache.
 */
typedef struct {
    int  dest_fd;
    int  fd;
    char rel[PATH_MAX];
} dir_cache_t;

static void dir_cache_init(dir_cache_t *c, int dest_fd)
{
    c->dest_fd = dest_fd;
    c->fd      = -1;
    c->rel[0]  = '\0';
}

static void dir_cache_drop(dir_cache_t *c)
{
    if (c->fd != -1) close(c->fd);
    c->fd = -1;
    c->rel[0] = '\0';
}

/* *leaf is set to the last component of rel. With create, missing parents
 * are made. -1 with errno: ELOOP or ENOTDIR when something in the
 * destination that is not a directory stands on the way. */
static int dest_parent(dir_cache_t *c, const char *rel, const char **leaf, int create)
{
    const char *slash = strrchr(rel, '/');
    *leaf = slash ? slash + 1 : rel;
    if (!**leaf || strcmp(*leaf, ".") == 0 || strcmp(*leaf, "..") == 0) {
        errno = EINVAL;
        return -1;
    }
    if (!slash) return c->dest_fd;

    size_t n = (size_t)(slash - rel);
    if (n >= sizeof(c->rel)) { errno = ENAMETOOLONG; return -1; }
    if (c->fd != -1 && strlen(c->rel) == n && memcmp(c->rel, rel, n) == 0)
        return c->fd;
    dir_cache_drop(c);
    memcpy(c->rel, rel, n);
    c->rel[n] = '\0';
    int fd = open_beneath(c->dest_fd, c->rel);
    if (fd == -1 && errno == ENOENT && create) {
        if (make_beneath(c->dest_fd, c->rel) == 0)
            fd = open_beneath(c->dest_fd, c->rel);
    }
    if (fd == -1) {
        c->rel[0] = '\0';
        return -1;
    }
    c->fd = fd;
    return fd;
}

/* Why an entry's directory could not be reached, for its failure line. */
static const char *parent_reason(int err)
{
    if (err == ELOOP || err == ENOTDIR || err == EXDEV)
        return "a symlink or non-directory in the destination stands where a "
               "parent directory belongs; it is not followed";
    return strerror(err);
}

static void set_times_at(int pfd, const char *leaf, const char *shown, int64_t mtime_ns)
{
    struct timespec times[2];
    times[0] = ns_to_timespec(mtime_ns);
    times[1] = times[0];
    if (utimensat(pfd, leaf, times, AT_SYMLINK_NOFOLLOW) != 0)
        log_warn("restore: utimensat %s: %s", shown, strerror(errno));
}

/* Ownership, mode and times of a thing the restore holds open. */
static void apply_metadata_fd(int fd, const char *shown, uint32_t mode,
                              uint32_t uid, uint32_t gid, int64_t mtime_ns)
{
    /* Ownership only succeeds as root; a non-root restore keeps the invoking
     * user's ownership rather than failing the whole operation. chown
     * first: it clears set-id bits, which the chmod puts back. */
    if (fchown(fd, (uid_t)uid, (gid_t)gid) != 0 && errno != EPERM)
        log_warn("restore: chown %s: %s", shown, strerror(errno));
    if (fchmod(fd, (mode_t)(mode & 07777)) != 0)
        log_warn("restore: chmod %s: %s", shown, strerror(errno));
    struct timespec times[2];
    times[0] = ns_to_timespec(mtime_ns);
    times[1] = times[0];
    if (futimens(fd, times) != 0)
        log_warn("restore: utimensat %s: %s", shown, strerror(errno));
}

/*
 * The same for an entry that cannot be opened for it - a symlink, FIFO or
 * device node - by name in its parent, never following the name. with_mode
 * is off for a symlink, whose own permissions the kernel ignores.
 */
static void apply_metadata_at(int pfd, const char *leaf, const char *shown,
                              int with_mode, uint32_t mode, uint32_t uid,
                              uint32_t gid, int64_t mtime_ns)
{
    if (fchownat(pfd, leaf, (uid_t)uid, (gid_t)gid, AT_SYMLINK_NOFOLLOW) != 0 &&
        errno != EPERM)
        log_warn("restore: chown %s: %s", shown, strerror(errno));
    if (with_mode &&
        fchmodat(pfd, leaf, (mode_t)(mode & 07777), AT_SYMLINK_NOFOLLOW) != 0)
        log_warn("restore: chmod %s: %s", shown, strerror(errno));
    set_times_at(pfd, leaf, shown, mtime_ns);
}

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

/*
 * Reapply a node's stored xattr set. Best effort, like ownership: a restore
 * running unprivileged cannot set security.* or trusted.* attributes, and
 * that must not fail the file whose content just restored correctly.
 *
 * Through an open fd where there is one. Otherwise lsetxattr on a path,
 * never setxattr: the entry may be a symlink, and the recorded attributes
 * belong to the link itself, not to whatever it points at - except on a
 * /proc/self/fd/N path, which names the pinned inode only when followed.
 *
 * Blob format is defined in types.h and written by scan.c. A malformed blob
 * stops the walk rather than reading past the end; a truncated record can
 * only come from corruption the object layer failed to catch, and quietly
 * applying half of it would be worse than applying none.
 */
typedef struct {
    int         fd;      /* >= 0: an fd open for read or write */
    const char *path;    /* otherwise this */
    int         follow;  /* path is a /proc/self/fd/N magic link */
    const char *shown;   /* the destination path, for messages */
} meta_target_t;

static void apply_one_xattr(const char *name, const void *value, size_t vlen,
                            void *vctx)
{
    const meta_target_t *t = vctx;
    int rc = t->fd >= 0 ? fsetxattr(t->fd, name, value, vlen, 0)
           : t->follow  ? setxattr(t->path, name, value, vlen, 0)
                        : lsetxattr(t->path, name, value, vlen, 0);
    if (rc != 0)
        log_warn("restore: cannot set xattr %s on %s: %s",
                 name, t->shown, strerror(errno));
}

static void apply_xattrs(store_t *store, const meta_target_t *t, const char *xhex)
{
    const char *path = t->shown;
    if (!xhex || !xhex[0] || hash_is_zero_hex(xhex)) return;

    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(xhex, hash);
    void   *data = NULL;
    size_t  len  = 0;
    uint8_t type = 0;
    uint64_t taken = 0;
    if (object_load_budgeted(store, hash, &data, &len, &type, &taken) != OK) {
        log_warn("restore: cannot load xattrs for %s: %s", path, err_msg());
        err_clear();
        return;
    }
    if (type != OBJECT_TYPE_XATTR) {
        log_warn("restore: xattrs for %s name a type %u object, not an xattr "
                 "blob; not applied", path, type);
        free(data);
    read_budget_release(taken);
        return;
    }

    meta_target_t tc = *t;
    if (xattr_blob_foreach(data, len, apply_one_xattr, &tc) != OK) {
        log_warn("restore: malformed xattr blob for %s: %s", path, err_msg());
        err_clear();
    }
    free(data);
    read_budget_release(taken);
}

/*
 * Remove whatever ACLs an entry has before its own are applied: the access
 * ACL, and on a directory the default ACL too.
 *
 * A restored directory gets its default ACL before its children are
 * created, and the kernel hands that default to every child as it is made:
 * as a file's access ACL, and as a directory's access and default ACL. The
 * restore only ever set what the snapshot recorded, so a child backed up
 * with no ACL came back with its parent's (B129), named users and all. So
 * every entry starts from none; apply_acls then sets exactly what was
 * recorded. Runs before the chmod, because while an access ACL is present
 * chmod's group bits set the ACL mask rather than the group class, and the
 * final mode would not be the backed-up one.
 */
static void clear_acls(const meta_target_t *t, int is_dir)
{
    static const char *const names[] = {
        "system.posix_acl_access", "system.posix_acl_default"
    };
    for (int i = 0; i < (is_dir ? 2 : 1); i++) {
        int rc = t->fd >= 0 ? fremovexattr(t->fd, names[i])
               : t->follow  ? removexattr(t->path, names[i])
                            : lremovexattr(t->path, names[i]);
        if (rc != 0 && errno != ENODATA && errno != ENOTSUP)
            log_warn("restore: cannot clear the inherited %s ACL on %s: %s",
                     i ? "default" : "access", t->shown, strerror(errno));
    }
}

/*
 * Reapply a node's stored ACLs. Runs after chmod on purpose: the access
 * ACL's base entries carry the definitive permissions, and applying it last
 * means the ACL wins over whatever chmod left. Default ACLs only make sense
 * on directories; applying one here, before the directory's children are
 * restored, is harmless because every child is cleared (clear_acls) and
 * given its own recorded state over anything it inherits.
 */
struct acl_ctx { meta_target_t t; int is_dir; };

static void apply_one_acl(uint8_t kind, const char *text, void *vctx)
{
    const struct acl_ctx *c = vctx;
    /* A default ACL only means anything on a directory. */
    if (kind == ACL_BLOB_DEFAULT && !c->is_dir) return;

    acl_t acl = acl_from_text(text);
    if (!acl) {
        log_warn("restore: unparseable ACL text for %s", c->t.shown);
        return;
    }
    int rc;
    if (c->t.fd >= 0 && kind != ACL_BLOB_DEFAULT) {
        rc = acl_set_fd(c->t.fd, acl);
    } else if (c->t.fd >= 0) {
        /* libacl has no fd call for a default ACL; the fd's magic link
         * names the same directory and nothing else. */
        char p[64];
        snprintf(p, sizeof(p), "/proc/self/fd/%d", c->t.fd);
        rc = acl_set_file(p, ACL_TYPE_DEFAULT, acl);
    } else {
        rc = acl_set_file(c->t.path,
                          kind == ACL_BLOB_DEFAULT ? ACL_TYPE_DEFAULT : ACL_TYPE_ACCESS,
                          acl);
    }
    if (rc != 0)
        log_warn("restore: cannot set %s ACL on %s: %s",
                 kind == ACL_BLOB_DEFAULT ? "default" : "access",
                 c->t.shown, strerror(errno));
    acl_free(acl);
}

static void apply_acls(store_t *store, const meta_target_t *t, const char *ahex,
                       int is_dir)
{
    const char *path = t->shown;
    if (!ahex || !ahex[0] || hash_is_zero_hex(ahex)) return;

    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(ahex, hash);
    void   *data = NULL;
    size_t  len  = 0;
    uint8_t type = 0;
    uint64_t taken = 0;
    if (object_load_budgeted(store, hash, &data, &len, &type, &taken) != OK) {
        log_warn("restore: cannot load ACL for %s: %s", path, err_msg());
        err_clear();
        return;
    }
    if (type != OBJECT_TYPE_ACL) {
        log_warn("restore: ACLs for %s name a type %u object, not an ACL "
                 "blob; not applied", path, type);
        free(data);
    read_budget_release(taken);
        return;
    }

    struct acl_ctx actx = { *t, is_dir };
    if (acl_blob_foreach(data, len, apply_one_acl, &actx) != OK) {
        log_warn("restore: malformed ACL blob for %s: %s", path, err_msg());
        err_clear();
    }
    free(data);
    read_budget_release(taken);
}

/* Something to say when a failure arrives with no message attached. */
static const char *status_reason(status_t st)
{
    switch (st) {
    case OK:            return "no error";
    case ERR_IO:        return "I/O error (no detail recorded)";
    case ERR_CORRUPT:   return "corrupt beyond repair (no detail recorded)";
    case ERR_NOMEM:     return "out of memory";
    case ERR_INVALID:   return "invalid object (no detail recorded)";
    case ERR_NOT_FOUND: return "not found in the repository";
    case ERR_TOO_LARGE: return "too large to load";
    case ERR_BUSY:      return "busy";
    case ERR_UNAVAILABLE: return "repository unavailable";
    }
    return "unknown error";
}

/*
 * One entry that could not be restored: counted, logged with its path and
 * the reason, and kept as the first failure if it is one.
 *
 * The error at the end of a restore says "see the log for which", and the
 * log did not always say: a regular file with no content hash was counted
 * failed in silence, so a restore of a B118 snapshot failed naming two
 * entries and the log named neither. Every failure goes through here so the
 * promise holds by construction rather than by each site remembering.
 */
static void restore_fail(restore_result_t *res, const char *path,
                         const char *fmt, ...) __attribute__((format(printf, 3, 4)));

static void restore_fail(restore_result_t *res, const char *path,
                         const char *fmt, ...)
{
    char why[768];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(why, sizeof(why), fmt, ap);
    va_end(ap);
    log_warn("restore: %s not restored: %s", path, why);
    /* A long path truncates the copy, never the log line above. */
    if (res->failed == 0 &&
        snprintf(res->first_failure, sizeof(res->first_failure), "%s: %s",
                 path, why) >= (int)sizeof(res->first_failure))
        memcpy(res->first_failure + sizeof(res->first_failure) - 4, "...", 4);
    res->failed++;
}

static int cancelled(const restore_opts_t *o)
{
    return o && cancel_requested(o->cancel);
}

static void report(const restore_opts_t *o, const char *phase,
                   uint64_t done, uint64_t total)
{
    if (o && o->progress_cb) o->progress_cb(phase, done, total, o->progress_ctx);
}

/*
 * Bytes written so far into the file being restored, from inside the store.
 *
 * Without it a restore's byte count moved only between files, and one large
 * file held the job at a single value for as long as it took (B089). Capped
 * at the file's size, so a count that overshoots never reports more than
 * the file holds.
 */
typedef struct {
    const restore_opts_t *opts;
    uint64_t base, total;     /* bytes finished before this file, and all */
    uint64_t size, written;   /* this file */
    uint64_t index, count;    /* entry being restored, for the count */
} restore_io_t;

static void restore_bytes(uint64_t bytes, void *ctx)
{
    restore_io_t *io = ctx;
    io->written += bytes;
    if (io->written > io->size) io->written = io->size;
    if (io->opts->bytes_cb)
        io->opts->bytes_cb(io->opts->progress_ctx, io->base + io->written, io->total);
    report(io->opts, "restore", io->index, io->count);
}

/*
 * Every node's path, built from the rows the restore has already read.
 *
 * This walked parents with one query per path component, per node, and the
 * restore asked up to three times per node. Through the one database handle
 * every thread shares, that made a restore cost entries times depth queries
 * whatever it restored: a one-file --subpath restore of an 82,604-entry
 * snapshot took 5 minutes 41 seconds, almost all of it rebuilding paths of
 * files it then skipped. The rows already carry each node's name and parent,
 * so the table maps node id to row once and memoises directory paths; a
 * file's path is its directory's plus its name. Linear in entries, and the
 * memory is one string per directory beside a result set already in memory.
 */
#define COL_PARENT 15
#define COL_FLAGS  16

typedef struct {
    const db_result_t *r;
    int64_t  *keys;      /* node id, 0 = empty slot */
    int32_t  *rows;
    uint32_t  mask;
    char    **dir_path;  /* per row, set once computed */
} path_table_t;

static uint32_t pt_slot(int64_t id, uint32_t mask)
{
    uint64_t h = (uint64_t)id * 0x9E3779B97F4A7C15ull;
    return (uint32_t)(h >> 32) & mask;
}

static int pt_init(path_table_t *pt, const db_result_t *r)
{
    memset(pt, 0, sizeof(*pt));
    pt->r = r;
    uint32_t cap = 16;
    while (cap < (uint32_t)r->nrows * 2u) cap <<= 1;
    pt->mask = cap - 1;
    pt->keys = calloc(cap, sizeof(*pt->keys));
    pt->rows = calloc(cap, sizeof(*pt->rows));
    pt->dir_path = calloc((size_t)r->nrows + 1, sizeof(*pt->dir_path));
    if (!pt->keys || !pt->rows || !pt->dir_path) return -1;
    for (int i = 0; i < r->nrows; i++) {
        int64_t id = strtoll(r->rows[i][0], NULL, 10);
        uint32_t k = pt_slot(id, pt->mask);
        while (pt->keys[k] != 0) k = (k + 1) & pt->mask;
        pt->keys[k] = id;
        pt->rows[k] = i;
    }
    return 0;
}

static void pt_free(path_table_t *pt)
{
    if (pt->dir_path)
        for (int i = 0; i < (pt->r ? pt->r->nrows : 0); i++) free(pt->dir_path[i]);
    free(pt->dir_path);
    free(pt->keys);
    free(pt->rows);
    memset(pt, 0, sizeof(*pt));
}

static int pt_find(const path_table_t *pt, int64_t id)
{
    uint32_t k = pt_slot(id, pt->mask);
    while (pt->keys[k] != 0) {
        if (pt->keys[k] == id) return pt->rows[k];
        k = (k + 1) & pt->mask;
    }
    return -1;
}

static status_t pt_rel(path_table_t *pt, int i, char *out, size_t outsz, int depth);

/* A directory's path, computed once. */
static const char *pt_dir(path_table_t *pt, int i, int depth)
{
    if (pt->dir_path[i]) return pt->dir_path[i];
    char buf[PATH_MAX];
    if (pt_rel(pt, i, buf, sizeof(buf), depth) != OK) return NULL;
    pt->dir_path[i] = strdup(buf);
    return pt->dir_path[i];
}

static status_t pt_rel(path_table_t *pt, int i, char *out, size_t outsz, int depth)
{
    if (depth > MAX_PATH_DEPTH)
        return set_error(ERR_CORRUPT, "restore: path deeper than %d", MAX_PATH_DEPTH);
    const char *name = pt->r->rows[i][1];
    int64_t parent = strtoll(pt->r->rows[i][COL_PARENT], NULL, 10);
    int w;
    if (parent == 0) {
        w = snprintf(out, outsz, "%s", name);
    } else {
        int p = pt_find(pt, parent);
        if (p < 0) return set_error(ERR_CORRUPT, "restore: node %s has no parent row",
                                    pt->r->rows[i][0]);
        const char *dir = pt_dir(pt, p, depth + 1);
        if (!dir) return set_error(ERR_IO, "restore: cannot rebuild path");
        w = snprintf(out, outsz, "%s/%s", dir, name);
    }
    if (w < 0 || (size_t)w >= outsz) return set_error(ERR_IO, "restore: path too long");
    return OK;
}

/*
 * A regular file is restored under a temporary name in its own directory
 * and renamed into place only once the object has read back authentic and
 * whole (OWN-36).
 *
 * The streaming read emits as it goes and learns whether the bytes were
 * right only at the end, from the tag and the hash. Writing into the final
 * path and deleting on failure covered a failure the restore lived to see,
 * not a daemon killed mid-file: that left a file of the right name holding
 * wrong or partial content, and the resumed restore skipped it as done.
 * Now the final path either does not exist or holds the verified file with
 * its metadata, and anything a crash leaves behind is a temporary file
 * under this prefix, which the next restore into that directory removes.
 *
 * The prefix plus six random characters, and nothing else, is what the
 * sweep matches, so it cannot take a file of the user's that merely starts
 * the same way unless the name has exactly that shape.
 */
#define PARTIAL_PREFIX     ".c-backup-partial."
#define PARTIAL_SUFFIX_LEN 6

/*
 * Create a temporary file in the directory pfd, its name (no directory
 * part) into name. mkostemp's contract, by fd: O_EXCL and mode 0600, so
 * nothing else can have the name and nobody else can read the bytes before
 * they are verified and the file's own mode is applied; and O_NOFOLLOW, so
 * a symlink planted under a guessed name is never opened.
 */
static int make_partial_at(int pfd, const char *shown, char *name, size_t namesz)
{
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    if (namesz < sizeof(PARTIAL_PREFIX) + PARTIAL_SUFFIX_LEN) {
        set_error(ERR_IO, "restore: temporary name buffer too small for %s", shown);
        return -1;
    }
    for (int attempt = 0; attempt < 100; attempt++) {
        unsigned char rnd[PARTIAL_SUFFIX_LEN];
        if (getrandom(rnd, sizeof(rnd), 0) != (ssize_t)sizeof(rnd)) {
            set_error_errno(ERR_IO, "restore: no randomness for a temporary file beside %s",
                            shown);
            return -1;
        }
        memcpy(name, PARTIAL_PREFIX, sizeof(PARTIAL_PREFIX) - 1);
        for (int i = 0; i < PARTIAL_SUFFIX_LEN; i++)
            name[sizeof(PARTIAL_PREFIX) - 1 + (size_t)i] =
                alphabet[rnd[i] % (sizeof(alphabet) - 1)];
        name[sizeof(PARTIAL_PREFIX) - 1 + PARTIAL_SUFFIX_LEN] = '\0';
        int fd = openat(pfd, name, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                        0600);
        if (fd != -1) return fd;
        if (errno != EEXIST) break;
    }
    set_error_errno(ERR_IO, "restore: cannot create a temporary file beside %s", shown);
    return -1;
}

static int is_partial_name(const char *name)
{
    size_t pl = sizeof(PARTIAL_PREFIX) - 1;
    return strncmp(name, PARTIAL_PREFIX, pl) == 0 &&
           strlen(name) == pl + PARTIAL_SUFFIX_LEN;
}

/*
 * Remove the temporary files an interrupted restore left in the directory
 * pfd (shown as `dir`). Called once per directory per run, before the first
 * file is restored into it, so a resumed restore cleans up exactly where it
 * goes back to work. One readdir per directory a run writes into.
 */
static void sweep_partials(int pfd, const char *dir)
{
    int rfd = openat(pfd, ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    DIR *d = rfd == -1 ? NULL : fdopendir(rfd);
    if (!d) {
        if (rfd != -1) close(rfd);
        return;
    }
    unsigned removed = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (!is_partial_name(de->d_name)) continue;
        struct stat sb;
        if (fstatat(pfd, de->d_name, &sb, AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISREG(sb.st_mode))
            continue;
        if (unlinkat(pfd, de->d_name, 0) == 0) removed++;
        else log_warn("restore: cannot remove partial file %s/%s: %s",
                      dir, de->d_name, strerror(errno));
    }
    closedir(d);
    if (removed)
        log_info("restore: removed %u partial file(s) an interrupted restore left in %s",
                 removed, dir);
}

/*
 * The resume rule: an existing regular file whose size matches the
 * snapshot's and whose mtime is within two seconds of it is the restored
 * file, and is skipped.
 *
 * That is sound because of how files now arrive: the final name only ever
 * appears by rename, after the content has verified and the mtime has been
 * set, so a file c-backup put there is complete. The two seconds are for
 * filesystems that keep coarser timestamps than the snapshot recorded (FAT
 * keeps two-second mtimes). Anything else at the path - another size, an
 * mtime from today, a directory or a symlink - is not the snapshot's
 * version, and is left alone and counted as kept when overwrite is off.
 */
static int already_restored(const struct stat *sb, uint64_t size, int64_t mtime_ns)
{
    if (!S_ISREG(sb->st_mode) || (uint64_t)sb->st_size != size) return 0;
    int64_t have = (int64_t)sb->st_mtim.tv_sec * 1000000000LL + sb->st_mtim.tv_nsec;
    int64_t diff = have > mtime_ns ? have - mtime_ns : mtime_ns - have;
    return diff < 2000000000LL;
}

/*
 * What is at a path already_restored turned down, for the log line that
 * says it was kept. The line always said "size or mtime differ", including
 * when the thing there was a directory (NIT-9), which sends the operator
 * comparing sizes of something that is not a file at all.
 */
static const char *kept_reason(const struct stat *sb, uint64_t size)
{
    if (S_ISDIR(sb->st_mode)) return "a directory is there, not the file";
    if (S_ISLNK(sb->st_mode)) return "a symbolic link is there, not the file";
    if (!S_ISREG(sb->st_mode)) return "something other than a regular file is there";
    if ((uint64_t)sb->st_size != size) return "a file of a different size is there";
    return "a file with a different modification time is there";
}

/*
 * Restore one regular file's content and metadata under a temporary name
 * in the directory pfd. On OK the verified file is complete under the name
 * left in tmp (closed), waiting for the batch below to make it durable and
 * rename it into place; on any failure the temporary file is removed and
 * the final name is untouched. *out_sz is the object's payload size. full
 * is the destination path, for messages.
 */
static status_t restore_file(store_t *store, const char *chash, int pfd,
                             const char *full, uint64_t size, uint32_t mode,
                             uint32_t uid, uint32_t gid, int64_t mtime,
                             const char *xhash, const char *ahash,
                             char *tmp, size_t tmpsz, uint64_t *out_sz)
{
    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(chash, hash);
    *out_sz = 0;

    int fd = make_partial_at(pfd, full, tmp, tmpsz);
    if (fd == -1) return ERR_IO;

    /*
     * One FILE object or a chunk list. Every object is bounded, so each is
     * loaded, decrypted and checked against its name whole before a byte of
     * it is written: nothing unverified ever reaches the file. A list's
     * leaves go in at their offsets and the file is extended to its length,
     * so holes stay holes. Any other type is refused: it is not file
     * content, whatever its bytes say (OWN-39).
     */
    (void)size;
    uint64_t sz = 0;
    status_t st = content_restore_fd(store, hash, fd, &sz);

    /* Metadata goes on before the rename, so the file is final the moment
     * it has its name: the resume rule relies on the mtime being right.
     * Through the fd: the file is never looked up by name again. */
    if (st == OK) {
        meta_target_t t = { fd, NULL, 0, full };
        clear_acls(&t, 0);
        apply_metadata_fd(fd, full, mode, uid, gid, mtime);
        apply_xattrs(store, &t, xhash);
        apply_acls(store, &t, ahash, 0);
    }
    if (close(fd) != 0 && st == OK)
        st = set_error_errno(ERR_IO, "restore: cannot close %s", full);
    if (st != OK) {
        if (unlinkat(pfd, tmp, 0) != 0 && errno != ENOENT)
            log_warn("restore: cannot remove partial file %s for %s: %s",
                     tmp, full, strerror(errno));
        return st;
    }
    *out_sz = sz;
    return OK;
}

/*
 * Verified files waiting to be made durable and renamed into place.
 *
 * The final name must not reach disk before the data does, or a power
 * failure leaves a right-named file of zeros - the very thing the
 * temporary file exists to prevent. Syncing each file on its own did that
 * at 175 files/s on btrfs, against 13,400 without: a sync costs a journal
 * commit whatever the file's size. So verified files queue here, and one
 * syncfs makes the whole batch durable before any of them is renamed. A
 * crash before the renames leaves verified temporary files, which the next
 * run sweeps and restores again; that is lost work, never a wrong file.
 *
 * syncfs flushes everything dirty on the destination filesystem, other
 * writers' data included, once per batch. The batch is bounded in files
 * and in bytes so that cost stays proportionate. It was 512 files or
 * 256 MiB; syncfs is a filesystem commit whose cost is mostly per call,
 * and 196 of them were 46% of a 100,000-file restore on btrfs, so it is
 * 4,096 files or 1 GiB now (PERF.md, 2026-11-16). A crash costs at most
 * that much redone work.
 *
 * Each item names its directory relative to the destination, not by fd:
 * 4,096 open directories is a real dent in a daemon's descriptor limit. The
 * batch keeps its own directory cache, so a flush in the middle of the
 * main loop cannot close the fd the loop is holding.
 */
#define BATCH_FILES 4096u
#define BATCH_BYTES ((uint64_t)1024 * 1024 * 1024)

typedef struct {
    char    *rel;      /* the final entry, relative to the destination */
    char    *tmp;      /* the temporary file's name in the same directory */
    char    *full;     /* the destination path, for messages */
    uint64_t bytes;
} pending_file_t;

typedef struct {
    pending_file_t items[BATCH_FILES];
    uint32_t       n;
    uint64_t       bytes;
    dir_cache_t    dirs;
} rename_batch_t;

/* Sync the batch, rename each file into place, and count the outcome. */
static void batch_flush(rename_batch_t *b, restore_result_t *res)
{
    if (b->n == 0) return;
    int synced = 0;
    const char *leaf;
    int pfd = dest_parent(&b->dirs, b->items[0].rel, &leaf, 0);
    if (pfd != -1) {
        synced = syncfs(pfd) == 0;
        if (!synced)
            log_warn("restore: cannot sync %u restored file(s) to disk: %s",
                     b->n, strerror(errno));
    } else {
        log_warn("restore: cannot open the directory of %s to sync it: %s",
                 b->items[0].full, strerror(errno));
    }
    for (uint32_t i = 0; i < b->n; i++) {
        pending_file_t *p = &b->items[i];
        pfd = dest_parent(&b->dirs, p->rel, &leaf, 0);
        if (pfd == -1) {
            /* Its directory was swapped out from under the restore; the
             * temporary file, wherever it is now, is swept next run. */
            restore_fail(res, p->full, "its directory cannot be reached to "
                         "rename it into place: %s", parent_reason(errno));
        } else if (!synced) {
            restore_fail(res, p->full, "its data could not be synced");
            if (unlinkat(pfd, p->tmp, 0) != 0 && errno != ENOENT)
                log_warn("restore: cannot remove partial file %s for %s: %s",
                         p->tmp, p->full, strerror(errno));
        } else if (renameat(pfd, p->tmp, pfd, leaf) != 0) {
            restore_fail(res, p->full, "cannot rename it into place: %s",
                         strerror(errno));
            if (unlinkat(pfd, p->tmp, 0) != 0 && errno != ENOENT)
                log_warn("restore: cannot remove partial file %s for %s: %s",
                         p->tmp, p->full, strerror(errno));
        } else {
            res->files++;
            res->bytes += p->bytes;
        }
        free(p->rel);
        free(p->tmp);
        free(p->full);
    }
    b->n = 0;
    b->bytes = 0;
}

/* Queue a verified file; a full batch is flushed. Returns -1 when the
 * paths cannot be held, and the temporary file is removed. */
static int batch_add(rename_batch_t *b, int pfd, const char *rel, const char *tmp,
                     const char *full, uint64_t bytes, restore_result_t *res)
{
    char *r = strdup(rel), *t = strdup(tmp), *f = strdup(full);
    if (!r || !t || !f) {
        free(r); free(t); free(f);
        if (unlinkat(pfd, tmp, 0) != 0 && errno != ENOENT) { /* swept next run */ }
        return -1;
    }
    b->items[b->n].rel   = r;
    b->items[b->n].tmp   = t;
    b->items[b->n].full  = f;
    b->items[b->n].bytes = bytes;
    b->n++;
    b->bytes += bytes;
    if (b->n == BATCH_FILES || b->bytes >= BATCH_BYTES) batch_flush(b, res);
    return 0;
}

static status_t restore_run_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                  int64_t snap_id, const char *dest, int dest_fd,
                                  const restore_opts_t *opts, restore_result_t *out);

/*
 * The cancel flag goes onto the store for the duration, so the streaming
 * readers can stop inside one large file instead of only between files: a
 * restore of a 2 GiB file was otherwise uncancellable for as long as that
 * file took. Put back afterwards, since the handle may outlive this call.
 */
status_t restore_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                     int64_t snap_id, const char *dest,
                     const restore_opts_t *opts, restore_result_t *out)
{
    if (!store || !db || !dest)
        return set_error(ERR_INVALID, "restore_run: missing store, db or dest");
    if (mkdir(dest, 0755) != 0 && errno != EEXIST)
        return set_error_errno(ERR_IO, "restore: cannot create %s", dest);
    /* Everything below is reached from this fd (dest_parent). */
    int dest_fd = open(dest, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dest_fd == -1)
        return set_error_errno(ERR_IO, "restore: cannot open %s", dest);
    atomic_int *prev_cancel = store_get_cancel(store);
    if (opts && opts->cancel) store_set_cancel(store, opts->cancel);
    status_t st = restore_run_inner(store, db, repo_id, snap_id, dest, dest_fd,
                                    opts, out);
    store_set_cancel(store, prev_cancel);
    close(dest_fd);
    return st;
}

static status_t restore_run_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                  int64_t snap_id, const char *dest, int dest_fd,
                                  const restore_opts_t *opts, restore_result_t *out)
{

    restore_opts_t defaults;
    memset(&defaults, 0, sizeof(defaults));
    if (!opts) opts = &defaults;

    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    /*
     * Directories first, then everything else. A file cannot be written
     * before its directory exists, and ordering by type is cheaper than
     * sorting by depth or creating parents ad hoc per file.
     */
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, name, mode, uid, gid, mtime_ns, size, content_hash, type,"
            "       COALESCE(link_target,''), COALESCE(hardlink_group_id, 0),"
            "       COALESCE(xattr_hash,''), COALESCE(acl_hash,''),"
            "       COALESCE(dev_major, 0), COALESCE(dev_minor, 0),"
            "       COALESCE(parent_node_id, 0), COALESCE(flags, 0)"
            "  FROM nodes WHERE repo_id = ? AND snap_id = ?"
            " ORDER BY CASE WHEN type = 2 THEN 0 ELSE 1 END, id",
            (const char *[]){ repo, snap, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "restore: cannot read snapshot nodes");
    }
    if (r->nrows == 0) {
        db_result_free(r);
        /* Two different situations, and only one of them is fixed by a
         * reindex. */
        db_result_t *s = NULL;
        int exists = db_execute(db,
                "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
                (const char *[]){ repo, snap, NULL }, &s) == 0 && s && s->nrows == 1;
        db_result_free(s);
        if (!exists)
            return set_error(ERR_NOT_FOUND, "restore: no snapshot %lld in this repository",
                             (long long)snap_id);
        return set_error(ERR_NOT_FOUND, "restore: snapshot %lld has no nodes"
                         " (it may have been evicted; reindex it first)",
                         (long long)snap_id);
    }

    restore_result_t res;
    uint64_t matched = 0;     /* entries in scope passed so far: all, or the subpath's */
    memset(&res, 0, sizeof(res));
    status_t st = OK;

    hardlink_map_t hlinks = { NULL, NULL, 0 };
    if (hardlink_map_init(&hlinks, (uint32_t)r->nrows) != 0) {
        db_result_free(r);
        return set_error(ERR_NOMEM, "restore: hardlink map");
    }
    path_table_t paths;
    if (pt_init(&paths, r) != 0) {
        pt_free(&paths);
        hardlink_map_free(&hlinks);
        db_result_free(r);
        return set_error(ERR_NOMEM, "restore: path table");
    }
    /* Holds at most one fd; dropped on every return below. */
    dir_cache_t dirs;
    dir_cache_init(&dirs, dest_fd);

    /*
     * Resume pre-pass. A restore into a partially-populated destination
     * skips what already exists, and skipped files never reach the code
     * that remembers a group's first restored member. Whichever member of
     * a group happens to be missing then gets a copy, because its
     * surviving siblings were invisible to the map. Order does not save
     * this: the missing member can sort before every survivor.
     *
     * So before restoring anything, members of every multi-member group
     * that already exist on disk are remembered. Fresh restores pay a
     * stat per member of a real hardlink group, all ENOENT; single-link
     * files, the overwhelming majority, never reach the stat.
     */
    if (!opts->overwrite) {
        groupcount_t gcount;
        if (groupcount_init(&gcount, (uint32_t)r->nrows) != 0) {
            pt_free(&paths);
            hardlink_map_free(&hlinks);
            db_result_free(r);
            return set_error(ERR_NOMEM, "restore: group counts");
        }
        for (int i = 0; i < r->nrows; i++) {
            uint32_t ntype = (uint32_t)strtoul(r->rows[i][8], NULL, 10);
            if (ntype != NODE_TYPE_REG) continue;
            groupcount_add(&gcount, (uint64_t)strtoll(r->rows[i][10], NULL, 10));
        }
        for (int i = 0; i < r->nrows; i++) {
            uint32_t ntype = (uint32_t)strtoul(r->rows[i][8], NULL, 10);
            if (ntype != NODE_TYPE_REG) continue;
            uint64_t hgroup = (uint64_t)strtoll(r->rows[i][10], NULL, 10);
            if (hgroup == 0 || groupcount_get(&gcount, hgroup) < 2) continue;
            if (hardlink_lookup(&hlinks, hgroup)) continue;

            char rel[PATH_MAX];
            if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) {
                err_clear();
                continue;
            }
            if (opts->subpath && !path_within(rel, opts->subpath)) continue;
            if (strip_rel(rel, opts)) continue;
            /* Only a member the resume rule would skip anchors the group:
             * linking later members to a file that is not the snapshot's
             * would hand them its content. */
            const char *leaf;
            int pfd = dest_parent(&dirs, rel, &leaf, 0);
            struct stat sb;
            if (pfd != -1 && fstatat(pfd, leaf, &sb, AT_SYMLINK_NOFOLLOW) == 0 &&
                already_restored(&sb, strtoull(r->rows[i][6], NULL, 10),
                                 strtoll(r->rows[i][5], NULL, 10)))
                hardlink_remember(&hlinks, hgroup, rel);
        }
        groupcount_free(&gcount);
    }

    /* Bytes to restore: regular files inside the subpath, if there is one.
     * A file's size is counted once the loop has moved past it, written or
     * skipped alike, since both are progress through the snapshot.
     *
     * And the entries to restore, which is what the item count measures.
     * It was the snapshot's entry count, so a one-file restore out of a
     * 104-entry snapshot reported 104 of 104 (B093): neither the work asked
     * for nor the work done. With no subpath every entry is in scope and no
     * pass is needed for the count. */
    uint64_t bytes_total = 0, bytes_done = 0, bytes_pending = 0;
    uint64_t work_total = opts->subpath ? 0 : (uint64_t)r->nrows;
    if (opts->bytes_cb || opts->subpath) {
        for (int i = 0; i < r->nrows; i++) {
            int reg = (uint32_t)strtoul(r->rows[i][8], NULL, 10) == NODE_TYPE_REG;
            if (opts->subpath) {
                char rel[PATH_MAX];
                if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) { err_clear(); continue; }
                if (!path_within(rel, opts->subpath)) continue;
                work_total++;
            }
            if (reg) bytes_total += strtoull(r->rows[i][6], NULL, 10);
        }
    }

    /* Per directory row, plus one for the destination itself: whether this
     * run has swept it for partial files yet. */
    char *swept = calloc((size_t)r->nrows + 1, 1);
    rename_batch_t *batch = calloc(1, sizeof(*batch));
    if (!swept || !batch) {
        free(swept);
        free(batch);
        dir_cache_drop(&dirs);
        pt_free(&paths);
        hardlink_map_free(&hlinks);
        db_result_free(r);
        return set_error(ERR_NOMEM, "restore: directory sweep map");
    }
    dir_cache_init(&batch->dirs, dest_fd);

    for (int i = 0; i < r->nrows && st == OK; i++) {
        if (cancelled(opts)) { st = set_error(ERR_BUSY, "restore cancelled"); break; }
        bytes_done += bytes_pending;
        bytes_pending = 0;
        if (opts->bytes_cb) opts->bytes_cb(opts->progress_ctx, bytes_done, bytes_total);
        /* Entries in scope finished so far. Reported before the subpath
         * test so a long run of out-of-scope entries still reaches the job
         * (and its cancel check) at the throttle's rate. */
        report(opts, "restore", matched, work_total);

        uint32_t    mode    = (uint32_t)strtoul(r->rows[i][2], NULL, 10);
        uint32_t    uid     = (uint32_t)strtoul(r->rows[i][3], NULL, 10);
        uint32_t    gid     = (uint32_t)strtoul(r->rows[i][4], NULL, 10);
        int64_t     mtime   = strtoll(r->rows[i][5], NULL, 10);
        const char *chash   = r->rows[i][7];
        uint32_t    ntype   = (uint32_t)strtoul(r->rows[i][8], NULL, 10);
        const char *ltarget = r->rows[i][9];
        /* Written signed; the bit pattern is what identifies the inode. */
        uint64_t    hgroup  = (uint64_t)strtoll(r->rows[i][10], NULL, 10);
        const char *xhash   = r->rows[i][11];
        const char *ahash   = r->rows[i][12];
        uint32_t    dmajor  = (uint32_t)strtoul(r->rows[i][13], NULL, 10);
        uint32_t    dminor  = (uint32_t)strtoul(r->rows[i][14], NULL, 10);

        char rel[PATH_MAX];
        if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) {
            char what[600];
            snprintf(what, sizeof(what), "node %s (named '%s')",
                     r->rows[i][0], r->rows[i][1]);
            restore_fail(&res, what, "its path cannot be reconstructed: %s",
                         err_msg()[0] ? err_msg() : "a parent is missing");
            err_clear();
            /* Without a subpath it is in work_total, and it is finished. */
            if (!opts->subpath) matched++;
            continue;
        }

        if (opts->subpath && !path_within(rel, opts->subpath))
            continue;
        if (ntype == NODE_TYPE_REG) bytes_pending = strtoull(r->rows[i][6], NULL, 10);
        matched++;
        if (strip_rel(rel, opts)) {
            /* The subpath itself is dest, which exists; a file cannot be
             * stripped onto it. */
            if (ntype != NODE_TYPE_DIR) {
                restore_fail(&res, rel[0] ? rel : opts->subpath,
                             "a single file cannot be restored as the destination itself");
                continue;
            }
            /* A directory can, and dest takes its metadata. dest is the
             * staging directory an in-place unit restore swaps in as the
             * mount (B168): left alone it kept the helper's 0700, root's
             * group and the time of the restore, and the swap made that the
             * live mount's. The mtime comes in the pass below, as for every
             * directory. */
            meta_target_t t = { dest_fd, NULL, 0, dest };
            clear_acls(&t, 1);
            apply_metadata_fd(dest_fd, dest, mode, uid, gid, mtime);
            apply_xattrs(store, &t, xhash);
            apply_acls(store, &t, ahash, 1);
            res.dirs++;
            continue;
        }

        char full[PATH_MAX];
        if (dest_path(full, sizeof(full), dest, rel, opts) != 0) {
            restore_fail(&res, rel, "the destination path is too long");
            continue;
        }

        /* The entry's directory, reached with no symlink on the way and
         * made if missing. Something not a directory in the way fails this
         * entry; anything else (no space, read-only) fails the restore,
         * as it always did. */
        const char *leaf;
        int pfd = dest_parent(&dirs, rel, &leaf, 1);
        if (pfd == -1) {
            int err = errno;
            if (err == ELOOP || err == ENOTDIR || err == EXDEV || err == EINVAL) {
                restore_fail(&res, full, "cannot reach its directory: %s",
                             parent_reason(err));
                continue;
            }
            st = set_error(ERR_IO, "restore: cannot create the directories for %s: %s",
                           full, strerror(err));
            break;
        }

        if (ntype == NODE_TYPE_DIR) {
            if (mkdirat(pfd, leaf, 0755) != 0 && errno != EEXIST) {
                st = set_error_errno(ERR_IO, "restore: mkdir %s", full);
                break;
            }
            /* What is there is opened, not assumed: a symlink or file
             * already at the name is never followed, never chmod'd, and
             * nothing is written beneath it. */
            int dfd = openat(pfd, leaf, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
            if (dfd == -1) {
                struct stat sb;
                int is_link = fstatat(pfd, leaf, &sb, AT_SYMLINK_NOFOLLOW) == 0 &&
                              S_ISLNK(sb.st_mode);
                restore_fail(&res, full, "%s is there where the directory belongs; "
                             "not followed or replaced",
                             is_link ? "a symlink" : "something that is not a directory");
                continue;
            }
            /* Mode and ownership are final here; the mtime is not. Writing
             * a file into a directory updates that directory's mtime, so
             * whatever is set now is overwritten by the children that follow
             * and the restored directory ends up stamped with the time of
             * the restore. It is reapplied in the pass below, after
             * everything has been written. */
            meta_target_t t = { dfd, NULL, 0, full };
            clear_acls(&t, 1);
            apply_metadata_fd(dfd, full, mode, uid, gid, mtime);
            apply_xattrs(store, &t, xhash);
            apply_acls(store, &t, ahash, 1);
            close(dfd);
            res.dirs++;
            continue;
        }

        /*
         * Symlinks carry no content object; their target is the payload.
         * Without this they fell through to the content path, matched a
         * zero hash, and were recreated as empty regular files: a restored
         * tree that looked complete and had lost every link in it.
         */
        if (ntype == NODE_TYPE_SYMLINK) {
            if (!ltarget || !ltarget[0]) {
                restore_fail(&res, rel, "symlink has no recorded target");
                continue;
            }
            if (opts->overwrite) unlinkat(pfd, leaf, 0);
            if (symlinkat(ltarget, pfd, leaf) != 0) {
                struct stat sb;
                if (errno != EEXIST) {
                    restore_fail(&res, full, "cannot create symlink: %s",
                                 strerror(errno));
                    continue;
                }
                /* A symlink already there is this one from an earlier run;
                 * anything else is the operator's and is not touched. */
                if (fstatat(pfd, leaf, &sb, AT_SYMLINK_NOFOLLOW) != 0 ||
                    !S_ISLNK(sb.st_mode)) {
                    log_warn("restore: %s already exists and is not the "
                             "snapshot's symlink; kept, since overwrite is off", full);
                    res.kept++;
                    continue;
                }
            }
            /* Mode is not applied: a symlink's own permissions are ignored by
             * the kernel, and chmod would follow the link and alter its
             * target instead. */
            apply_metadata_at(pfd, leaf, full, 0, mode, uid, gid, mtime);
            /* No ACL branch: symlinks have none (scan does not collect them,
             * and acl_set_file would follow the link anyway). The xattrs go
             * on through the pinned parent, lsetxattr on the link itself. */
            char xp[PATH_MAX + 64];
            int via_proc = pfd != AT_FDCWD &&
                snprintf(xp, sizeof(xp), "/proc/self/fd/%d/%s", pfd, leaf) < (int)sizeof(xp);
            meta_target_t t = { -1, via_proc ? xp : full, 0, full };
            apply_xattrs(store, &t, xhash);
            res.symlinks++;
            continue;
        }

        /*
         * FIFOs and device nodes have no content object: the mode, and for a
         * device the major/minor pair, are the entire file. They were
         * scanned and indexed all along and then silently skipped here, so a
         * restored tree came back missing them.
         *
         * A device node needs CAP_MKNOD, which an unprivileged restore does
         * not have. That is a warning rather than a failure, the same
         * treatment ownership gets: the rest of the tree is still correct
         * and re-running as root fills these in.
         */
        if (ntype == NODE_TYPE_FIFO || ntype == NODE_TYPE_CHR ||
            ntype == NODE_TYPE_BLK) {
            if (opts->overwrite) unlinkat(pfd, leaf, 0);

            int made;
            mode_t kind = ntype == NODE_TYPE_FIFO ? S_IFIFO
                        : ntype == NODE_TYPE_CHR  ? S_IFCHR : S_IFBLK;
            if (ntype == NODE_TYPE_FIFO) {
                made = mkfifoat(pfd, leaf, (mode_t)(mode & 07777));
            } else {
                /*
                 * Device 0:0 is a device, not a missing one.
                 *
                 * This used to treat a zero pair as "version 1 tree, the
                 * numbers were never recorded" and skip the node. Two things
                 * were wrong with that. A v1 tree cannot be read at all -
                 * manifest_parse refuses any version but 2, so the case it
                 * guarded cannot occur - and 0:0 is exactly what an overlayfs
                 * whiteout is. Every whiteout in a Docker overlay2 tree was
                 * therefore dropped, counted as skipped, and reported as a
                 * restore with nothing lost.
                 *
                 * It is not nothing: a whiteout records that a file was
                 * deleted in an upper layer. Restore the layers without them
                 * and the deleted files come back. Found by restoring 561,132
                 * files of a real runner cache, where 60 of them were
                 * whiteouts and the diff against the source was the only thing
                 * that said so.
                 */
                made = mknodat(pfd, leaf, kind | (mode_t)(mode & 07777),
                               makedev(dmajor, dminor));
            }

            if (made != 0 && errno != EEXIST) {
                log_warn("restore: cannot create %s: %s", full, strerror(errno));
                res.skipped++;
                continue;
            }
            /* Pinned, and checked to be the kind of node it should be: an
             * existing entry of another kind (a symlink above all) is the
             * operator's, and its metadata is not changed. */
            int ofd = openat(pfd, leaf, O_PATH | O_NOFOLLOW | O_CLOEXEC);
            struct stat sb;
            if (ofd == -1 || fstat(ofd, &sb) != 0 || (sb.st_mode & S_IFMT) != kind ||
                (kind != S_IFIFO && sb.st_rdev != makedev(dmajor, dminor))) {
                if (ofd != -1) close(ofd);
                log_warn("restore: %s already exists and is not the snapshot's "
                         "version; kept, since overwrite is off", full);
                res.kept++;
                continue;
            }
            /* An O_PATH fd takes no fsetxattr; its magic link, followed,
             * reaches exactly the node it holds. */
            char xp[64];
            snprintf(xp, sizeof(xp), "/proc/self/fd/%d", ofd);
            meta_target_t t = { -1, xp, 1, full };
            clear_acls(&t, 0);
            apply_metadata_at(pfd, leaf, full, 1, mode, uid, gid, mtime);
            apply_xattrs(store, &t, xhash);
            apply_acls(store, &t, ahash, 0);
            close(ofd);
            res.specials++;
            continue;
        }

        /*
         * Anything sharing an inode with something already restored is
         * relinked rather than copied. Without this, 30,000 hardlinked files
         * in a source tree came back as 30,000 independent copies: the
         * content was right and the tree was several gigabytes larger than
         * what was backed up.
         */
        if (hgroup != 0) {
            const char *first = hardlink_lookup(&hlinks, hgroup);
            if (first) {
                /* The first member may still be waiting in the batch
                 * under its temporary name. */
                batch_flush(batch, &res);
                if (opts->overwrite) unlinkat(pfd, leaf, 0);
                const char *fslash = strrchr(first, '/');
                const char *fleaf = fslash ? fslash + 1 : first;
                int fpfd = dest_fd;
                if (fslash) {
                    char fdir[PATH_MAX];
                    snprintf(fdir, sizeof(fdir), "%.*s", (int)(fslash - first), first);
                    fpfd = open_beneath(dest_fd, fdir);
                }
                int linked = fpfd != -1 && linkat(fpfd, fleaf, pfd, leaf, 0) == 0;
                int err = errno;
                if (fpfd != -1 && fpfd != dest_fd) close(fpfd);
                if (linked) { res.hardlinks++; continue; }
                /* Cross-device or unsupported: fall through and copy. */
                log_warn("restore: cannot hardlink %s to %s/%s, copying: %s",
                         full, dest, first, strerror(err));
            }
        }

        if (!chash || !chash[0] || hash_is_zero_hex(chash)) {
            /*
             * A node the scanner could not classify has no content and never
             * did, so there is nothing here for the repository to have lost.
             * In practice this is a unix socket: stat_to_node_type has no case
             * for one and returns 0, and backup stores no object for it.
             *
             * That belongs in skipped rather than failed. Counting it as a
             * failure made restore_run return ERR_CORRUPT and the job report
             * "restore incomplete", every time, for any tree containing a live
             * socket - /run, appdata, anything with a database or a container
             * runtime in it. Five sockets in one appdata share were enough to
             * make a restore that wrote every recoverable byte indistinguish-
             * able from one that had genuinely lost data, which is the more
             * expensive failure: an operator who sees "incomplete" on every
             * successful restore stops reading it.
             *
             * A regular file with no content hash is a different matter and
             * stays a failure: that one the repository should have had.
             */
            if (ntype == 0) {
                log_info("restore: %s has no restorable content "
                         "(unclassified node type, most likely a socket)", rel);
                res.skipped++;
                continue;
            }
            if (ntype != NODE_TYPE_REG)
                restore_fail(&res, rel, "unsupported node type %u", ntype);
            else
                /* B118: a build before 1.0.0 kept files it skipped. */
                restore_fail(&res, rel, "regular file has no content in the "
                             "snapshot (written by a c-backup older than "
                             "1.0.0, which recorded a file it could not read)");
            continue;
        }

        uint64_t nsize = strtoull(r->rows[i][6], NULL, 10);
        if (!opts->overwrite) {
            struct stat sb;
            if (fstatat(pfd, leaf, &sb, AT_SYMLINK_NOFOLLOW) == 0) {
                if (already_restored(&sb, nsize, mtime)) {
                    /* Skipped, but still this inode group's anchor. A
                     * resumed restore skips everything an earlier attempt
                     * wrote, and without remembering those paths every
                     * group whose first member landed in the earlier run
                     * came back as copies: 2,262 links flattened on the
                     * first interrupted 45G run, silently, because the
                     * content was never wrong. */
                    if (hgroup != 0) hardlink_remember(&hlinks, hgroup, rel);
                    res.skipped++;
                } else {
                    log_warn("restore: %s already exists and is not the "
                             "snapshot's version (%s); kept, since overwrite "
                             "is off", full, kept_reason(&sb, nsize));
                    res.kept++;
                }
                continue;
            }
        }

        /* Leftovers of an interrupted run in this directory, once per run. */
        int64_t parent = strtoll(r->rows[i][COL_PARENT], NULL, 10);
        int     slot   = parent == 0 ? r->nrows : pt_find(&paths, parent);
        if (slot >= 0 && !swept[slot]) {
            swept[slot] = 1;
            char shown_dir[PATH_MAX];
            snprintf(shown_dir, sizeof(shown_dir), "%.*s",
                     (int)(strlen(full) - strlen(leaf) - 1), full);
            sweep_partials(pfd, shown_dir);
        }

        uint64_t sz = 0;
        char     tmp[NAME_MAX + 1];
        err_clear();    /* so a reason below cannot be left over from before */
        restore_io_t io = { opts, bytes_done, bytes_total, nsize, 0,
                            matched - 1, work_total };
        store_io_fn prev_io;
        void *prev_io_ctx;
        store_get_io_progress(store, &prev_io, &prev_io_ctx);
        if (opts->bytes_cb || opts->progress_cb)
            store_set_io_progress(store, restore_bytes, &io);
        status_t lst = restore_file(store, chash, pfd, full, nsize, mode, uid, gid,
                                    mtime, xhash, ahash, tmp, sizeof(tmp), &sz);
        store_set_io_progress(store, prev_io, prev_io_ctx);

        if (lst != OK && cancelled(opts)) {
            /* Stopped inside the file. Its temporary file is gone and the
             * final path was never touched. */
            st = set_error(ERR_BUSY, "restore cancelled");
            break;
        }
        if (lst != OK) {
            /* The fault is the repository's, not the destination's: name
             * the object, and never end on an empty reason. Several store
             * paths return a bare status, and this line used to read
             * "cannot write <path>: " - pointing an operator at a disk that
             * was fine. */
            restore_fail(&res, full, "object %s could not be read or repaired: %s",
                         chash, err_msg()[0] ? err_msg() : status_reason(lst));
            err_clear();
            continue;
        }

        /* First member of its inode group: everything later links here.
         * Later members share this inode, so the attributes restore_file
         * set are already theirs; the link() sites need no reapply. */
        if (batch_add(batch, pfd, rel, tmp, full, sz, &res) != 0) {
            restore_fail(&res, full, "out of memory");
            continue;
        }
        /* Restored as stored, and said: the file never settled while the
         * backup read it, so its bytes may mix two versions (OWN-45-LIVE). */
        if (strtoul(r->rows[i][COL_FLAGS], NULL, 10) & MANIFEST_FLAG_UNSETTLED)
            log_warn("restore: %s was still changing when it was backed up; "
                     "restored as stored, and it may not be internally "
                     "consistent", full);
        if (hgroup != 0) hardlink_remember(&hlinks, hgroup, rel);
    }
    /* Whatever verified before a stop is still worth keeping. */
    batch_flush(batch, &res);
    dir_cache_drop(&batch->dirs);
    free(batch);
    free(swept);

    hardlink_map_free(&hlinks);

    /*
     * Directory mtimes, now that nothing else will be written.
     *
     * A directory's mtime tracks changes to its entries, so every file
     * restored into it moved it forward again. Restoring 5722 files left 575
     * of 689 directories stamped with the moment of the restore rather than
     * the moment they were backed up - modes and ownership were right, so it
     * looked like metadata was being preserved, and only the timestamps were
     * silently today's date.
     *
     * The same rows, walked again: directories sort first in this result set
     * and it is already in memory, so this costs a second scan of a prefix
     * and no allocation. Order does not matter here - setting a timestamp on
     * a child does not touch its parent, only creating and removing entries
     * does, and all of that is finished.
     */
    for (int i = 0; i < r->nrows; i++) {
        if ((uint32_t)strtoul(r->rows[i][8], NULL, 10) != NODE_TYPE_DIR) break;
        if (cancelled(opts)) break;

        char rel[PATH_MAX], full[PATH_MAX];
        if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) {
            err_clear();
            continue;
        }
        if (opts->subpath && !path_within(rel, opts->subpath)) continue;
        if (strip_rel(rel, opts)) {
            struct timespec times[2];
            times[0] = ns_to_timespec(strtoll(r->rows[i][5], NULL, 10));
            times[1] = times[0];
            if (futimens(dest_fd, times) != 0)
                log_warn("restore: utimensat %s: %s", dest, strerror(errno));
            continue;
        }
        if (dest_path(full, sizeof(full), dest, rel, opts) != 0)
            continue;

        /* Just the timestamp: nothing else about a directory changes after
         * it is made. Through its pinned parent, never following it. */
        const char *leaf;
        int pfd = dest_parent(&dirs, rel, &leaf, 0);
        if (pfd != -1) set_times_at(pfd, leaf, full, strtoll(r->rows[i][5], NULL, 10));
    }
    dir_cache_drop(&dirs);

    if (opts->bytes_cb && st == OK)
        opts->bytes_cb(opts->progress_ctx, bytes_done + bytes_pending, bytes_total);
    /* The loop reports what was finished before each entry, so the last
     * entry is counted here: matched equals work_total on a full run. */
    if (st == OK) report(opts, "restore", matched, work_total);
    pt_free(&paths);
    db_result_free(r);
    if (st != OK) return st;

    /* The route refuses this up front when the index can answer; this is
     * the backstop for every other caller. A restore that wrote nothing
     * because nothing matched is not a success. */
    if (opts->subpath && matched == 0 && res.failed == 0)
        return set_error(ERR_NOT_FOUND,
                         "subpath %s matches nothing in snapshot %lld",
                         opts->subpath, (long long)snap_id);

    log_info("restore: snapshot %lld -> %s, %llu files, %llu dirs, "
             "%llu symlinks, %llu hardlinks, %llu specials, %llu skipped, "
             "%llu kept, %llu failed",
             (long long)snap_id, dest, (unsigned long long)res.files,
             (unsigned long long)res.dirs, (unsigned long long)res.symlinks,
             (unsigned long long)res.hardlinks,
             (unsigned long long)res.specials, (unsigned long long)res.skipped,
             (unsigned long long)res.kept, (unsigned long long)res.failed);
    if (out) *out = res;

    /* Everything that could be written has been. Say what could not, rather
     * than reporting a restore that produced nothing as a success. */
    if (res.failed)
        return set_error(ERR_CORRUPT,
                         "restore incomplete: %llu entr%s could not be restored "
                         "(%llu written); see the log for %s; first: %s",
                         (unsigned long long)res.failed,
                         res.failed == 1 ? "y" : "ies",
                         (unsigned long long)(res.files + res.dirs + res.symlinks
                                              + res.hardlinks + res.specials),
                         res.failed == 1 ? "it" : "each",
                         res.first_failure);
    return OK;
}
