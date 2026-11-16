#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "scan.h"
#include "reserved.h"
#include "util.h"
#include <cutils/log.h>

#include <dirent.h>
#include <fnmatch.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <acl/libacl.h>

static uint64_t inode_identity(dev_t dev, ino_t ino) {
    uint64_t h = 1469598103934665603ULL;
    uint64_t d = (uint64_t)dev;
    uint64_t i = (uint64_t)ino;
    for (int b = 0; b < 8; b++) {
        h ^= (d >> (b * 8)) & 0xffu;
        h *= 1099511628211ULL;
    }
    for (int b = 0; b < 8; b++) {
        h ^= (i >> (b * 8)) & 0xffu;
        h *= 1099511628211ULL;
    }
    if (h == 0) h = 1;
    return h;
}

/* ------------------------------------------------------------------ */
/* Inode map: inode_identity (uint64) → first node_id (uint64)        */
/* Exposed as scan_imap_t so callers can share it across scan_tree()  */
/* calls to deduplicate hard links spanning multiple source roots.     */
/* ------------------------------------------------------------------ */

typedef struct { uint64_t key; uint64_t val; } imap_slot_t;
struct scan_imap {
    imap_slot_t *s;
    size_t cap;
    size_t cnt;

    /*
     * Node ids for this backup run.
     *
     * This was a file-static that started at 1 and was never reset, so it
     * counted across every scan the process ever did and two repositories
     * backing up at once shared it. The imap is already the thing that spans
     * one run's scan_tree calls and nothing else, which is exactly the scope
     * a node id needs: unique within a run, restarting each run, private to
     * it.
     *
     * These are not the ids that reach the database. index_nodes offsets them
     * past whatever the repository already holds, because reindex allocates
     * from the table itself and the two have to agree.
     */
    uint64_t next_node_id;

    char     **dir_keys;  /* relative path (no leading slash) */
    uint64_t  *dir_vals;  /* node_id */
    size_t     dir_count;
    size_t     dir_cap;
};
typedef struct scan_imap imap_t;  /* local alias */

scan_imap_t *scan_imap_new(void) {
    imap_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;
    m->s = calloc(256, sizeof(imap_slot_t));
    if (!m->s) { free(m); return NULL; }
    m->cap = 256;
    m->next_node_id = 1;
    return m;
}
void scan_imap_free(scan_imap_t *m) {
    if (!m) return;
    for (size_t i = 0; i < m->dir_count; i++) free(m->dir_keys[i]);
    free(m->dir_keys);
    free(m->dir_vals);
    free(m->s);
    free(m);
}

static uint64_t imap_get(const imap_t *m, uint64_t key) {
    if (!key) return 0;
    size_t h = (size_t)((key * 11400714819323198485ULL) >> 32) & (m->cap - 1);
    for (size_t i = 0; i < m->cap; i++) {
        size_t idx = (h + i) & (m->cap - 1);
        if (!m->s[idx].key) return 0;
        if (m->s[idx].key == key) return m->s[idx].val;
    }
    return 0;
}

static int imap_set(imap_t *m, uint64_t key, uint64_t val) {
    if (!key) return 0;
    if (m->cnt * 10 >= m->cap * 7) {
        size_t nc = m->cap * 2;
        imap_slot_t *ns = calloc(nc, sizeof(imap_slot_t));
        if (!ns) return -1;
        for (size_t i = 0; i < m->cap; i++) {
            if (!m->s[i].key) continue;
            size_t h = (size_t)((m->s[i].key * 11400714819323198485ULL) >> 32) & (nc - 1);
            while (ns[h].key) h = (h + 1) & (nc - 1);
            ns[h] = m->s[i];
        }
        free(m->s); m->s = ns; m->cap = nc;
    }
    size_t h = (size_t)((key * 11400714819323198485ULL) >> 32) & (m->cap - 1);
    while (m->s[h].key && m->s[h].key != key) h = (h + 1) & (m->cap - 1);
    if (!m->s[h].key) m->cnt++;
    m->s[h].key = key; m->s[h].val = val;
    return 0;
}

static uint64_t dirmap_get(const imap_t *m, const char *rel) {
    for (size_t i = 0; i < m->dir_count; i++) {
        if (strcmp(m->dir_keys[i], rel) == 0) return m->dir_vals[i];
    }
    return 0;
}

static int dirmap_set(imap_t *m, const char *rel, uint64_t node_id) {
    if (m->dir_count == m->dir_cap) {
        size_t nc = m->dir_cap ? m->dir_cap * 2 : 64;
        char **nk = malloc(nc * sizeof(char *));
        uint64_t *nv = malloc(nc * sizeof(uint64_t));
        if (!nk || !nv) {
            free(nk);
            free(nv);
            return -1;
        }
        if (m->dir_count) {
            memcpy(nk, m->dir_keys, m->dir_count * sizeof(char *));
            memcpy(nv, m->dir_vals, m->dir_count * sizeof(uint64_t));
        }
        free(m->dir_keys);
        free(m->dir_vals);
        m->dir_keys = nk;
        m->dir_vals = nv;
        m->dir_cap = nc;
    }
    m->dir_keys[m->dir_count] = strdup(rel);
    if (!m->dir_keys[m->dir_count]) return -1;
    m->dir_vals[m->dir_count] = node_id;
    m->dir_count++;
    return 0;
}

static void scan_warn(scan_result_t *res, const char *msg) {
    if (!res) return;
    if (res->warn_count == res->warn_cap) {
        uint32_t nc = res->warn_cap ? res->warn_cap * 2 : 16;
        char **tmp = realloc(res->warnings, nc * sizeof(char *));
        if (!tmp) return; /* drop warning on OOM — non-fatal */
        res->warnings = tmp;
        res->warn_cap = nc;
    }
    char *dup = strdup(msg);
    if (!dup) return;
    res->warnings[res->warn_count++] = dup;
}

static status_t result_append(scan_result_t *res, const scan_entry_t *e,
                              const scan_opts_t *opts) {
    if (res->count == res->capacity) {
        uint32_t newcap = res->capacity ? res->capacity * 2 : 64;
        scan_entry_t *tmp = realloc(res->entries, newcap * sizeof(*tmp));
        if (!tmp) return set_error(ERR_NOMEM, "scan: realloc entries failed (%u)", newcap);
        res->entries  = tmp;
        res->capacity = newcap;
    }
    res->entries[res->count++] = *e;
    if (opts && opts->progress_cb) {
        uint32_t every = opts->progress_every ? opts->progress_every : 5000;
        if (res->count == 1 || (res->count % every) == 0)
            opts->progress_cb(res->count, opts->progress_ctx);
    }
    return OK;
}

static uint8_t stat_to_node_type(const struct stat *st) {
    if (S_ISREG(st->st_mode))  return NODE_TYPE_REG;
    if (S_ISDIR(st->st_mode))  return NODE_TYPE_DIR;
    if (S_ISLNK(st->st_mode))  return NODE_TYPE_SYMLINK;
    if (S_ISFIFO(st->st_mode)) return NODE_TYPE_FIFO;
    if (S_ISCHR(st->st_mode))  return NODE_TYPE_CHR;
    if (S_ISBLK(st->st_mode))  return NODE_TYPE_BLK;
    return 0;
}

/*
 * The blob these produce is stored as an object and its hash lands in
 * manifest trees, so its bytes are covered by the determinism contract:
 * attributes sorted by name (llistxattr order is filesystem-dependent) and
 * lengths written explicitly little-endian, never by struct copy.
 */

static int xattr_name_cmp(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/*
 * POSIX ACLs surface in listxattr as system.posix_acl_* on most filesystems.
 * They are excluded here because ACLs travel on their own channel
 * (collect_acl, as acl_to_text), and carrying them twice would apply them
 * twice on restore, in an undefined order.
 */
static int xattr_name_excluded(const char *n) {
    return strcmp(n, "system.posix_acl_access") == 0 ||
           strcmp(n, "system.posix_acl_default") == 0;
}

static void put_le16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)((v >> 8) & 0xFF);
}

static void put_le32(uint8_t *p, uint32_t v) {
    for (int i = 0; i < 4; i++) p[i] = (uint8_t)((v >> (8 * i)) & 0xFF);
}

/*
 * follow is set only for a /proc/self/fd/N path, where following lands on
 * the inode that fd holds and nothing else (see meta_path); every other
 * path is read with the l-variants, which never follow the last component.
 */
static ssize_t xlist(const char *path, char *buf, size_t sz, int follow) {
    return follow ? listxattr(path, buf, sz) : llistxattr(path, buf, sz);
}

static ssize_t xget(const char *path, const char *name, void *buf, size_t sz,
                    int follow) {
    return follow ? getxattr(path, name, buf, sz) : lgetxattr(path, name, buf, sz);
}

/* shown is the path for messages; path may be a /proc/self/fd one. */
static status_t collect_xattrs(const char *path, int follow, const char *shown,
                              uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;
    ssize_t list_sz_s = xlist(path, NULL, 0, follow);
    if (list_sz_s <= 0) return OK;
    size_t list_sz = (size_t)list_sz_s;

    char *names = malloc(list_sz);
    if (!names) return set_error(ERR_NOMEM, "collect_xattrs: alloc failed");
    if (xlist(path, names, list_sz, follow) < 0) { free(names); return set_error_errno(ERR_IO, "listxattr(%s)", shown); }

    /* Collect name pointers, dropping the excluded ones, then sort. */
    size_t attr_count = 0;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1)
        if (!xattr_name_excluded(n)) attr_count++;
    if (attr_count == 0) { free(names); return OK; }

    const char **attrs = malloc(attr_count * sizeof(*attrs));
    ssize_t *sizes = malloc(attr_count * sizeof(*sizes));
    if (!attrs || !sizes) {
        free(attrs); free(sizes); free(names);
        return set_error(ERR_NOMEM, "collect_xattrs: alloc names failed");
    }
    size_t idx = 0;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1)
        if (!xattr_name_excluded(n)) attrs[idx++] = n;
    qsort(attrs, attr_count, sizeof(*attrs), xattr_name_cmp);

    const size_t XATTR_MAX_TOTAL = 64u * 1024u * 1024u; /* 64 MiB cap */
    size_t total = 0;
    for (idx = 0; idx < attr_count; idx++) {
        ssize_t vsz = xget(path, attrs[idx], NULL, 0, follow);
        sizes[idx] = vsz;
        if (vsz >= 0) {
            size_t inc = sizeof(uint16_t) + strlen(attrs[idx]) + 1 + sizeof(uint32_t) + (size_t)vsz;
            if (inc > XATTR_MAX_TOTAL - total) {
                free(attrs); free(sizes); free(names);
                return set_error(ERR_CORRUPT, "collect_xattrs: total xattr size exceeds limit for '%s'", shown);
            }
            total += inc;
        }
    }
    if (total == 0) { free(attrs); free(sizes); free(names); return OK; }

    /* Second pass: read values directly into the output buffer using cached sizes.
     * The value may have shrunk between the probe and this read (TOCTOU), so we
     * capture the actual returned length and patch the vlen field afterward.
     * On error or disappearance, emit the attribute with vlen=0 rather than
     * hashing uninitialized buffer contents. */
    uint8_t *buf = malloc(total);
    if (!buf) { free(attrs); free(sizes); free(names); return set_error(ERR_NOMEM, "collect_xattrs: alloc xattr buf failed"); }
    uint8_t *p = buf;
    for (idx = 0; idx < attr_count; idx++) {
        if (sizes[idx] < 0) continue;
        const char *n = attrs[idx];
        uint16_t nlen = (uint16_t)strlen(n) + 1;
        put_le16(p, nlen); p += sizeof(uint16_t);
        memcpy(p, n, nlen); p += nlen;
        uint8_t *vlen_pos = p;
        p += sizeof(uint32_t);
        uint32_t actual_vlen = 0;
        if (sizes[idx] > 0) {
            ssize_t got = xget(path, n, p, (size_t)sizes[idx], follow);
            if (got < 0) {
                actual_vlen = 0;
            } else if ((size_t)got > (size_t)sizes[idx]) {
                /* Shouldn't happen (kernel would truncate); clamp defensively */
                actual_vlen = (uint32_t)sizes[idx];
            } else {
                actual_vlen = (uint32_t)got;
            }
        }
        put_le32(vlen_pos, actual_vlen);
        p += actual_vlen;
    }
    free(attrs);
    free(sizes);
    free(names);
    *out     = buf;
    *out_len = (size_t)(p - buf);
    return OK;
}

/*
 * One ACL as text, or NULL when there is nothing worth storing.
 *
 * A trivial access ACL (owner/group/other only) restates the mode bits the
 * manifest already carries, and acl_get_file synthesises one for every file
 * on the system. Storing those would give every node an ACL object whose
 * restore is a slower chmod; acl_equiv_mode is the standard test for "this
 * ACL is only a mode".
 */
static char *acl_fetch_text(const char *path, acl_type_t type) {
    acl_t acl = acl_get_file(path, type);
    if (!acl) return NULL;
    if (type == ACL_TYPE_ACCESS && acl_equiv_mode(acl, NULL) == 0) {
        acl_free(acl);
        return NULL;
    }
    /* A default ACL with no entries is how "none" reads back on some
     * filesystems; it says nothing, so store nothing. */
    if (type == ACL_TYPE_DEFAULT) {
        acl_entry_t e;
        if (acl_get_entry(acl, ACL_FIRST_ENTRY, &e) != 1) {
            acl_free(acl);
            return NULL;
        }
    }
    char *txt = acl_to_text(acl, NULL);
    acl_free(acl);
    return txt;  /* caller acl_free()s */
}

/*
 * Blob format: see types.h. Access before default, so the bytes are
 * deterministic. Directories are the only place a default ACL exists;
 * everything else stores at most the access record.
 */
static status_t collect_acl(const char *path, int is_dir,
                            uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;

    char *access_txt = acl_fetch_text(path, ACL_TYPE_ACCESS);
    char *dflt_txt   = is_dir ? acl_fetch_text(path, ACL_TYPE_DEFAULT) : NULL;
    if (!access_txt && !dflt_txt) return OK;

    /* Text lengths include the trailing NUL so restore can hand each record
     * straight to acl_from_text() without a copy. */
    size_t alen = access_txt ? strlen(access_txt) + 1 : 0;
    size_t dlen = dflt_txt   ? strlen(dflt_txt)   + 1 : 0;
    size_t rec  = 1 + sizeof(uint32_t);
    size_t total = (access_txt ? rec + alen : 0) + (dflt_txt ? rec + dlen : 0);

    uint8_t *buf = malloc(total);
    if (!buf) {
        if (access_txt) acl_free(access_txt);
        if (dflt_txt)   acl_free(dflt_txt);
        return set_error(ERR_NOMEM, "collect_acl: alloc failed");
    }
    uint8_t *p = buf;
    if (access_txt) {
        *p++ = ACL_BLOB_ACCESS;
        put_le32(p, (uint32_t)alen); p += sizeof(uint32_t);
        memcpy(p, access_txt, alen); p += alen;
        acl_free(access_txt);
    }
    if (dflt_txt) {
        *p++ = ACL_BLOB_DEFAULT;
        put_le32(p, (uint32_t)dlen); p += sizeof(uint32_t);
        memcpy(p, dflt_txt, dlen); p += dlen;
        acl_free(dflt_txt);
    }
    *out     = buf;
    *out_len = total;
    return OK;
}

/* Whether /proc/self/fd can stand in for a path (meta_at). Checked once. */
static int proc_fd_usable(void) {
    static _Atomic int known = -1;
    int v = known;
    if (v < 0) {
        v = access("/proc/self/fd", X_OK) == 0;
        known = v;
    }
    return v;
}

/*
 * Extended attributes and ACLs of the entry `name` in directory dirfd
 * (AT_FDCWD with a full path for a source root), which the scan has just
 * lstat'd as *st.
 *
 * By path, an entry swapped for a symlink between the lstat and these reads
 * was followed: acl_get_file always follows, so the scan recorded the
 * target's ACL under the entry's name, and a parent swapped for a symlink
 * redirected the l-variants too (B128). So a non-symlink is pinned first,
 * with an O_PATH | O_NOFOLLOW open checked against the lstat, and read
 * through /proc/self/fd/N: following that magic link reaches the pinned
 * inode and nothing else. An entry that is no longer what the lstat saw
 * gets no metadata; if it is a file the store refuses it on the same
 * inode test. A symlink, which has no ACL, is read with the l-variants
 * through its pinned parent.
 *
 * Without /proc the path is used as before, which is the old exposure for
 * metadata only; content never depends on this.
 */
static status_t meta_at(int dirfd, const char *name, const char *path,
                        const struct stat *st, uint32_t type,
                        uint8_t **xd, size_t *xl, uint8_t **ad, size_t *al) {
    char xpath[PATH_MAX + 64];
    int follow = 0, ofd = -1;
    const char *use = path;

    if (proc_fd_usable()) {
        if (type == NODE_TYPE_SYMLINK) {
            if (dirfd != AT_FDCWD &&
                snprintf(xpath, sizeof(xpath), "/proc/self/fd/%d/%s", dirfd, name)
                    < (int)sizeof(xpath))
                use = xpath;
        } else {
            ofd = openat(dirfd, name, O_PATH | O_NOFOLLOW | O_CLOEXEC);
            struct stat now;
            if (ofd == -1 || fstat(ofd, &now) != 0 ||
                (now.st_mode & S_IFMT) != (st->st_mode & S_IFMT) ||
                now.st_dev != st->st_dev || now.st_ino != st->st_ino) {
                if (ofd != -1) close(ofd);
                return OK;   /* changed under the scan: no metadata */
            }
            snprintf(xpath, sizeof(xpath), "/proc/self/fd/%d", ofd);
            use = xpath;
            follow = 1;
        }
    }

    status_t rc = collect_xattrs(use, follow, path, xd, xl);
    /* No ACL for symlinks: acl_get_file follows the link, so what it returns
     * is the target's ACL wearing the symlink's name. */
    if (rc == OK && type != NODE_TYPE_SYMLINK) {
        rc = collect_acl(use, type == NODE_TYPE_DIR, ad, al);
        if (rc != OK) {
            free(*xd);
            *xd = NULL;
            *xl = 0;
        }
    }
    if (ofd != -1) close(ofd);
    return rc;
}

status_t scan_entry_collect_metadata(scan_entry_t *e) {
    if (!e || !e->path) return set_error(ERR_INVALID, "scan_entry_collect_metadata: null entry");
    if (e->xattr_data || e->xattr_len || e->acl_data || e->acl_len) return OK;
    return meta_at(AT_FDCWD, e->path, e->path, &e->st, e->node.type,
                   &e->xattr_data, &e->xattr_len, &e->acl_data, &e->acl_len);
}

/*
 * root_dev is threaded through the recursion rather than stored in
 * scan_opts_t. It used to live there, which meant scan_tree had to cast away
 * the const on its own options argument to record it: an output smuggled into
 * an input. Callers never set it and never read it, so it was never really
 * part of the options.
 */

/*
 * The directory being walked whose recorded path is `/`, if any: `/` itself
 * for an ordinary scan, the read root for scan_tree_as onto `/`. Children of
 * it with a reserved name (reserved.h) are dropped. Thread-local and set
 * around one walk, rather than threaded through the recursion or stored in
 * scan_opts_t, which is input only (CLAUDE.md, the root_dev trap).
 */
static _Thread_local const char *tl_top_read;

/*
 * The walk is relative to directory fds, never by path below a source root.
 *
 * By path, a directory swapped for a symlink between its lstat and its
 * opendir was followed, and so was one swapped while its children were
 * being lstat'd: the scan walked /etc and recorded what it found there
 * under the source's names, regular files the store then read as listed
 * (B128). Each directory is now opened O_NOFOLLOW from its parent's fd and
 * checked against the lstat that classified it, and every child is
 * fstatat'd relative to that fd, so nothing below a root is reached through
 * a symlink. `path` is still built alongside: it is the entry's name in the
 * snapshot, and what the store opens later (with its own checks).
 *
 * dirfd is AT_FDCWD for a source root, whose own path is the operator's to
 * choose and may run through symlinks.
 */
static status_t scan_dir(int dirfd, const char *name, const char *path,
                         const struct stat *dst,
                         uint64_t dir_node_id,
                         size_t strip_prefix_len, imap_t *imap,
                         const scan_opts_t *opts, dev_t root_dev,
                         scan_result_t *res);

static status_t scan_entry_at(int dirfd, const char *name, const char *path,
                                uint64_t parent_node_id,
                                size_t strip_prefix_len, imap_t *imap,
                                const scan_opts_t *opts, dev_t root_dev,
                                scan_result_t *res) {
    struct stat st;
    if (fstatat(dirfd, name, &st, AT_SYMLINK_NOFOLLOW) == -1) {
        if (opts && opts->progress_clear_cb)
            opts->progress_clear_cb(opts->progress_ctx);
        char msg[PATH_MAX + 96];
        snprintf(msg, sizeof(msg), "lstat failed, skipping: %s (%s)", path, strerror(errno));
        scan_warn(res, msg);
        log_warn("%s", msg);
        return OK;
    }

    /* Skip entries on different filesystems (mount point crossing) */
    if (opts && opts->one_filesystem && root_dev != 0 &&
        st.st_dev != root_dev) {
        char msg[PATH_MAX + 96];
        snprintf(msg, sizeof(msg), "skipping mount point: %s (different device)", path);
        scan_warn(res, msg);
        log_warn("%s", msg);
        return OK;
    }

    scan_entry_t e = {0};
    e.path             = strdup(path);
    if (!e.path) return set_error(ERR_NOMEM, "scan: strdup failed for '%s'", path);
    e.parent_node_id   = parent_node_id;
    e.strip_prefix_len = strip_prefix_len;
    e.st               = st;

    node_t *nd = &e.node;
    nd->node_id        = imap->next_node_id++;
    nd->inode_identity = inode_identity(st.st_dev, st.st_ino);
    nd->type           = stat_to_node_type(&st);
    nd->mode           = (uint32_t)st.st_mode;
    nd->uid            = (uint32_t)st.st_uid;
    nd->gid            = (uint32_t)st.st_gid;
    nd->size           = (uint64_t)st.st_size;
    nd->mtime_sec      = (int64_t)st.st_mtim.tv_sec;
    nd->mtime_nsec     = (uint64_t)st.st_mtim.tv_nsec;
    nd->link_count     = (uint32_t)st.st_nlink;

    /* Hard link detection: if we've seen this inode before, record the primary */
    if (nd->link_count > 1 && nd->type == NODE_TYPE_REG) {
        uint64_t primary = imap_get(imap, nd->inode_identity);
        if (primary) {
            e.hardlink_to_node_id = primary;
        } else {
            imap_set(imap, nd->inode_identity, nd->node_id);
        }
    }

    if (nd->type == NODE_TYPE_CHR || nd->type == NODE_TYPE_BLK) {
        nd->device.major = (uint32_t)major(st.st_rdev);
        nd->device.minor = (uint32_t)minor(st.st_rdev);
    } else if (nd->type == NODE_TYPE_SYMLINK) {
        char target[4096];
        ssize_t tlen = readlinkat(dirfd, name, target, sizeof(target) - 1);
        if (tlen > 0) {
            target[tlen] = '\0';
            nd->symlink.target_len = (uint32_t)tlen + 1;
            e.symlink_target = strdup(target);
        }
    }

    status_t st2;
    int collect_meta = (!opts || opts->collect_meta);
    if (collect_meta &&
        (st2 = meta_at(dirfd, name, path, &st, nd->type, &e.xattr_data,
                       &e.xattr_len, &e.acl_data, &e.acl_len)) != OK) {
        free(e.path); free(e.symlink_target); return st2;
    }

    uint64_t this_node_id = nd->node_id;

    if ((st2 = result_append(res, &e, opts)) != OK) {
        free(e.path); free(e.xattr_data); free(e.acl_data); free(e.symlink_target); return st2;
    }

    if (nd->type == NODE_TYPE_DIR && e.path && e.path[0] == '/' &&
        e.strip_prefix_len == 1 && e.path[1] != '\0') {
        const char *rel = e.path + 1;
        if (!dirmap_get(imap, rel)) (void)dirmap_set(imap, rel, nd->node_id);
    }

    if (S_ISDIR(st.st_mode))
        return scan_dir(dirfd, name, path, &st, this_node_id, strip_prefix_len,
                        imap, opts, root_dev, res);
    return OK;
}

static int path_has_prefix(const char *path, const char *prefix) {
    if (!path || !prefix) return 0;
    size_t plen = strlen(prefix);
    while (plen > 1 && prefix[plen - 1] == '/') plen--;
    if (strncmp(path, prefix, plen) != 0) return 0;
    return path[plen] == '\0' || path[plen] == '/';
}

static int pattern_has_glob(const char *s) {
    return strpbrk(s, "*?[") != NULL;
}

/*
 * Match a pattern against every component-aligned tail of the path, so a
 * fragment like "src/tmp" excludes /anywhere/src/tmp without the caller
 * having to know where the tree is rooted.
 */
static int match_path_tail(const char *pat, const char *full, int cross) {
    int flags = cross ? 0 : FNM_PATHNAME;
    const char *seg = full;
    for (;;) {
        if (fnmatch(pat, seg, flags) == 0) return 1;
        const char *slash = strchr(seg, '/');
        if (!slash) return 0;
        seg = slash + 1;
    }
}

/*
 * One exclude pattern against one absolute path.
 *
 * Four forms, decided by what the pattern contains rather than by a flag:
 *
 *   /var/cache        absolute and literal - prefix match at a component
 *                     boundary, so it excludes the directory and its subtree
 *                     but not /var/cache-manager
 *   /home/x/D*        absolute with wildcards - matched against the whole
 *                     path, wildcards stopping at '/'
 *   build/tmp         contains a slash - matched against any component
 *                     aligned tail of the path
 *   node_modules      no slash - matched against the basename, at any depth
 *
 * A pattern containing ** lets its wildcards cross '/' as well.
 *
 * Directories are tested before the walk descends into them, so excluding a
 * directory prunes its whole subtree rather than filtering entries one by
 * one on the way out.
 */
static int pattern_matches(const char *pat, const char *full) {
    if (!pat || !*pat) return 0;

    /* "node_modules/" and "node_modules" mean the same thing to everyone
     * who has written a .gitignore, so accept both. */
    char trimmed[PATH_MAX];
    size_t plen = strlen(pat);
    while (plen > 1 && pat[plen - 1] == '/') plen--;
    if (plen >= sizeof(trimmed)) return 0;
    memcpy(trimmed, pat, plen);
    trimmed[plen] = '\0';

    /* An absolute pattern is a path, and the paths it is compared with are
     * already clean: "/a/../a/b" or "/a//b" matched nothing at all, silently,
     * which is the same failure as an exclude the matcher ignored. Resolved
     * lexically, since the pattern may name something that does not exist
     * here. Relative patterns are fragments matched at any depth, where ".."
     * has nothing to resolve against, so they are left as written. */
    if (trimmed[0] == '/') {
        char norm[PATH_MAX];
        if (path_normalize(trimmed, norm, sizeof(norm)) != 0) return 0;
        memcpy(trimmed, norm, strlen(norm) + 1);
    }

    int cross = strstr(trimmed, "**") != NULL;

    if (trimmed[0] == '/') {
        if (!pattern_has_glob(trimmed)) return path_has_prefix(full, trimmed);
        return fnmatch(trimmed, full, cross ? 0 : FNM_PATHNAME) == 0;
    }
    if (strchr(trimmed, '/')) return match_path_tail(trimmed, full, cross);

    const char *slash = strrchr(full, '/');
    return fnmatch(trimmed, slash ? slash + 1 : full, 0) == 0;
}

/*
 * This used to accept absolute prefixes only, and skip anything else without
 * a word. Passing --exclude '*.iso' or 'node_modules' therefore excluded
 * nothing, reported nothing, and backed the lot up: the operator believed
 * they had excluded something and the only evidence otherwise was the size
 * of the repository months later.
 */
static int is_excluded(const char *full_path, const scan_opts_t *opts) {
    if (!opts || !opts->exclude || opts->n_exclude == 0) return 0;
    for (int i = 0; i < opts->n_exclude; i++) {
        if (pattern_matches(opts->exclude[i], full_path)) return 1;
    }
    return 0;
}

/* Dirent batch entry for inode-sorted readdir.
 * On HDD, sorting by d_ino before lstat() groups inodes sequentially
 * on disk, reducing seek time from random to near-sequential. */
typedef struct {
    char   name[NAME_MAX + 1];
    ino_t  d_ino;
} scan_dirent_t;

static int scan_dirent_cmp(const void *a, const void *b) {
    const scan_dirent_t *da = a, *db = b;
    if (da->d_ino < db->d_ino) return -1;
    if (da->d_ino > db->d_ino) return  1;
    return 0;
}

static status_t scan_dir(int dirfd, const char *name, const char *path,
                         const struct stat *dst,
                         uint64_t dir_node_id,
                         size_t strip_prefix_len, imap_t *imap,
                         const scan_opts_t *opts, dev_t root_dev,
                         scan_result_t *res) {
    /* Held while the children are walked, so each is reached from this
     * directory and not by its path: one fd per level of depth. */
    int dfd = openat(dirfd, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    const char *why = NULL;
    char errbuf[128] = "";
    struct stat now;
    if (dfd == -1) {
        /* O_DIRECTORY | O_NOFOLLOW on a symlink fails ENOTDIR on Linux, not
         * ELOOP, so look at what is there to say which. */
        int err = errno;
        struct stat lst;
        if (err == ENOTDIR || err == ELOOP)
            why = fstatat(dirfd, name, &lst, AT_SYMLINK_NOFOLLOW) == 0 && S_ISLNK(lst.st_mode)
                ? "it is now a symlink, not followed" : "it is no longer a directory";
        else { snprintf(errbuf, sizeof(errbuf), "%s", strerror(err)); why = errbuf; }
    } else if (fstat(dfd, &now) != 0 || now.st_dev != dst->st_dev ||
               now.st_ino != dst->st_ino) {
        why = "it was replaced during the scan";
    }
    DIR *d = NULL;
    if (!why) {
        int rfd = dup(dfd);
        d = rfd == -1 ? NULL : fdopendir(rfd);
        if (!d) {
            snprintf(errbuf, sizeof(errbuf), "%s", strerror(errno));
            why = errbuf;
            if (rfd != -1) close(rfd);
        }
    }
    if (why) {
        if (dfd != -1) close(dfd);
        if (opts && opts->progress_clear_cb)
            opts->progress_clear_cb(opts->progress_ctx);
        char msg[PATH_MAX + 160];
        snprintf(msg, sizeof(msg), "cannot open directory: %s (%s)", path, why);
        scan_warn(res, msg);
        log_warn("%s", msg);
        return OK;
    }

    /* Phase 1: batch all directory entries */
    size_t cap = 256, cnt = 0;
    scan_dirent_t *batch = malloc(cap * sizeof(*batch));
    if (!batch) { closedir(d); close(dfd); return set_error(ERR_NOMEM, "scan_dir: dirent batch alloc"); }

    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' ||
             (de->d_name[1] == '.' && de->d_name[2] == '\0')))
            continue;
        if (cnt == cap) {
            size_t nc = cap * 2;
            scan_dirent_t *tmp = realloc(batch, nc * sizeof(*tmp));
            if (!tmp) { free(batch); closedir(d); close(dfd); return set_error(ERR_NOMEM, "scan_dir: dirent batch realloc"); }
            batch = tmp;
            cap = nc;
        }
        size_t nlen = strlen(de->d_name);
        if (nlen > NAME_MAX) nlen = NAME_MAX;
        memcpy(batch[cnt].name, de->d_name, nlen);
        batch[cnt].name[nlen] = '\0';
        batch[cnt].d_ino = de->d_ino;
        cnt++;
    }
    closedir(d);

    /* Phase 2: sort by inode number */
    if (cnt > 1)
        qsort(batch, cnt, sizeof(*batch), scan_dirent_cmp);

    /* Phase 3: process in inode order */
    size_t plen = strlen(path);
    status_t st = OK;
    for (size_t i = 0; i < cnt && st == OK; i++) {
        size_t nlen = strlen(batch[i].name);
        char child[PATH_MAX];
        if (plen + 1 + nlen >= sizeof(child)) {
            if (opts && opts->progress_clear_cb)
                opts->progress_clear_cb(opts->progress_ctx);
            scan_warn(res, "path too long, skipping entry");
            log_warn("path too long, skipping entry");
            continue;
        }
        memcpy(child, path, plen);
        child[plen] = '/';
        memcpy(child + plen + 1, batch[i].name, nlen + 1);
        /* A source of `/` walks into every top-level name, including the
         * ones reserved for the daemon (reserved.h). A file snapshot must
         * never carry them, or data could pass for a unit's metadata. */
        if (tl_top_read && strcmp(path, tl_top_read) == 0 &&
            reserved_top_name(batch[i].name)) {
            char msg[PATH_MAX + 128];
            snprintf(msg, sizeof(msg), "refusing /%s: the name is reserved for the "
                     "daemon in every snapshot root; it is not backed up",
                     batch[i].name);
            scan_warn(res, msg);
            log_error("%s", msg);
            continue;
        }
        if (is_excluded(child, opts)) continue;
        st = scan_entry_at(dfd, batch[i].name, child, dir_node_id,
                           strip_prefix_len, imap, opts,
                           root_dev, res);
    }
    free(batch);
    close(dfd);
    return st;
}

/* record: whose ancestors are synthesised above the root; root itself for
 * an ordinary scan. The entries are rewritten to it by scan_tree_as. */
static status_t scan_tree_impl(const char *root, const char *record,
                               scan_imap_t *imap, const scan_opts_t *opts,
                               scan_result_t **out) {
    int as = strcmp(root, record) != 0;
    scan_result_t *res = calloc(1, sizeof(*res));
    if (!res) return set_error(ERR_NOMEM, "scan_tree: alloc result failed");

    /*
     * Tar-like relative-absolute layout: keep full absolute source path,
     * but drop the leading '/'.
     *
     *   /home/alice/file.txt -> home/alice/file.txt
     *   /etc/hosts           -> etc/hosts
     */
    size_t strip_prefix_len = (root[0] == '/') ? 1 : 0;

    struct stat root_st;
    if (lstat(root, &root_st) != 0) {
        if (opts && opts->progress_clear_cb)
            opts->progress_clear_cb(opts->progress_ctx);
        char msg[PATH_MAX + 96];
        snprintf(msg, sizeof(msg), "lstat failed, skipping: %s (%s)", root, strerror(errno));
        scan_warn(res, msg);
        log_warn("%s", msg);
        *out = res;
        return OK;
    }

    /* Root device for mount-crossing detection, fixed for this whole scan and
     * compared against every entry's st_dev by scan_entry_at. */
    dev_t root_dev = (opts && opts->one_filesystem) ? root_st.st_dev : 0;

    uint64_t root_parent_id = 0;
    if (record[0] == '/' && strip_prefix_len == 1) {
        const char *rel = record + 1; /* no leading slash */
        size_t len = strlen(rel);
        while (len > 1 && rel[len - 1] == '/') len--;

        for (size_t i = 0; i < len; i++) {
            if (rel[i] != '/') continue;
            size_t plen = i;
            if (plen == 0) continue;

            char parent_rel[PATH_MAX];
            if (plen >= sizeof(parent_rel)) break;
            memcpy(parent_rel, rel, plen);
            parent_rel[plen] = '\0';

            uint64_t existing = dirmap_get(imap, parent_rel);
            if (existing) {
                root_parent_id = existing;
                continue;
            }

            char abs_path[PATH_MAX];
            if (snprintf(abs_path, sizeof(abs_path), "/%s", parent_rel) >= (int)sizeof(abs_path))
                break;

            struct stat st;
            if (lstat(abs_path, &st) != 0 || !S_ISDIR(st.st_mode)) {
                if (!as) continue;
                /* A recorded root's ancestors need not exist here: the
                 * data was read from somewhere else. Made up, the same
                 * every time, so the trees above it serialise alike. */
                memset(&st, 0, sizeof(st));
                st.st_mode = S_IFDIR | 0755;
                st.st_nlink = 2;
            }

            scan_entry_t e = {0};
            e.path = strdup(abs_path);
            if (!e.path) { scan_result_free(res); return set_error(ERR_NOMEM, "scan_tree: strdup failed"); }
            e.parent_node_id = root_parent_id;
            e.strip_prefix_len = 1;
            e.st = st;

            node_t *nd = &e.node;
            nd->node_id        = imap->next_node_id++;
            nd->inode_identity = inode_identity(st.st_dev, st.st_ino);
            nd->type           = NODE_TYPE_DIR;
            nd->mode           = (uint32_t)st.st_mode;
            nd->uid            = (uint32_t)st.st_uid;
            nd->gid            = (uint32_t)st.st_gid;
            nd->size           = (uint64_t)st.st_size;
            nd->mtime_sec      = (int64_t)st.st_mtim.tv_sec;
            nd->mtime_nsec     = (uint64_t)st.st_mtim.tv_nsec;
            nd->link_count     = (uint32_t)st.st_nlink;

            status_t ast = result_append(res, &e, opts);
            if (ast != OK) {
                free(e.path);
                scan_result_free(res);
                return ast;
            }
            if (dirmap_set(imap, parent_rel, nd->node_id) != 0) {
                scan_result_free(res);
                return set_error(ERR_NOMEM, "scan_tree: dirmap_set failed");
            }
            root_parent_id = nd->node_id;
        }
    }

    if (is_excluded(root, opts)) {
        *out = res;
        return OK;
    }

    /* Root may disappear between prefix synthesis and scan entry walk. */
    if (lstat(root, &root_st) != 0) {
        if (opts && opts->progress_clear_cb)
            opts->progress_clear_cb(opts->progress_ctx);
        char msg[PATH_MAX + 96];
        snprintf(msg, sizeof(msg), "lstat failed, skipping: %s (%s)", root, strerror(errno));
        log_warn("%s", msg);
        scan_result_free(res);
        res = calloc(1, sizeof(*res));
        if (!res) return set_error(ERR_NOMEM, "scan_tree: realloc result failed");
        scan_warn(res, msg);
        *out = res;
        return OK;
    }

    uint32_t before = res->count;
    const char *prev_top = tl_top_read;
    tl_top_read = strcmp(record, "/") == 0 ? root : NULL;
    status_t st = scan_entry_at(AT_FDCWD, root, root, root_parent_id,
                                strip_prefix_len, imap, opts,
                                root_dev, res);
    tl_top_read = prev_top;
    if (st != OK) { scan_result_free(res); return st; }
    res->root_entries = res->count - before;
    /* Every path below was built as root + "/" + name..., so the root is
     * exactly this many bytes at the head of each. */
    size_t rlen = strlen(root);
    if (rlen > UINT32_MAX) rlen = 0;   /* cannot happen: PATH_MAX */
    for (uint32_t i = before; i < res->count; i++)
        res->entries[i].root_len = (uint32_t)rlen;

    /* Every entry the walk produced moves to the record root; the path it
     * was found at is kept for reading. */
    if (as) {
        size_t rl = strlen(root), clen = strlen(record);
        while (rl > 1 && root[rl - 1] == '/') rl--;
        while (clen > 1 && record[clen - 1] == '/') clen--;
        for (uint32_t i = before; i < res->count; i++) {
            scan_entry_t *e = &res->entries[i];
            if (strncmp(e->path, root, rl) != 0) continue;
            const char *tail = e->path + rl;
            size_t tl = strlen(tail);
            /* Onto `/`: the tail already starts with the separator. */
            size_t cl = (clen == 1 && tl > 0) ? 0 : clen;
            char *np = malloc(cl + tl + 1);
            if (!np) { scan_result_free(res); return set_error(ERR_NOMEM, "scan_tree_as: path"); }
            memcpy(np, record, cl);
            memcpy(np + cl, tail, tl + 1);
            e->read_path = e->path;
            e->path = np;
        }
    }
    *out = res;
    return OK;
}

status_t scan_tree(const char *root, scan_imap_t *imap,
                   const scan_opts_t *opts, scan_result_t **out) {
    return scan_tree_impl(root, root, imap, opts, out);
}

status_t scan_tree_as(const char *read_root, const char *record_root,
                      scan_imap_t *imap, const scan_opts_t *opts,
                      scan_result_t **out) {
    if (!read_root || !record_root || read_root[0] != '/' || record_root[0] != '/')
        return set_error(ERR_INVALID, "scan_tree_as: roots must be absolute");
    return scan_tree_impl(read_root, record_root, imap, opts, out);
}

status_t scan_add_synthetic(scan_result_t **resp, scan_imap_t *imap,
                            const char *path, const node_t *node,
                            uint32_t *out_index) {
    if (!path || path[0] != '/' || !path[1] || !node)
        return set_error(ERR_INVALID, "scan_add_synthetic: bad path");
    scan_result_t *res = *resp;
    if (!res) {
        if (!(res = calloc(1, sizeof(*res))))
            return set_error(ERR_NOMEM, "scan_add_synthetic: result");
        *resp = res;
    }

    /* Ancestors, through the same map ordinary roots use, so a synthetic
     * directory is created once however many entries live under it. */
    uint64_t parent = 0;
    const char *rel = path + 1;
    size_t len = strlen(rel);
    for (size_t i = 0; i < len; i++) {
        if (rel[i] != '/') continue;
        char prel[PATH_MAX];
        if (i == 0 || i >= sizeof(prel)) return set_error(ERR_INVALID, "scan_add_synthetic: bad path");
        memcpy(prel, rel, i);
        prel[i] = '\0';
        uint64_t have = dirmap_get(imap, prel);
        if (have) { parent = have; continue; }
        scan_entry_t e = {0};
        char abs_path[PATH_MAX + 2];
        if (snprintf(abs_path, sizeof(abs_path), "/%s", prel) >= (int)sizeof(abs_path))
            return set_error(ERR_INVALID, "scan_add_synthetic: path too long");
        if (!(e.path = strdup(abs_path))) return set_error(ERR_NOMEM, "scan_add_synthetic");
        e.parent_node_id = parent;
        e.strip_prefix_len = 1;
        e.synthetic = 1;
        e.st.st_mode = S_IFDIR | 0700;
        e.st.st_nlink = 2;
        e.node.node_id = imap->next_node_id++;
        e.node.type = NODE_TYPE_DIR;
        e.node.mode = S_IFDIR | 0700;
        e.node.link_count = 2;
        status_t st = result_append(res, &e, NULL);
        if (st != OK) { free(e.path); return st; }
        if (dirmap_set(imap, prel, e.node.node_id) != 0)
            return set_error(ERR_NOMEM, "scan_add_synthetic: dirmap");
        parent = e.node.node_id;
    }

    scan_entry_t e = {0};
    if (!(e.path = strdup(path))) return set_error(ERR_NOMEM, "scan_add_synthetic");
    e.parent_node_id = parent;
    e.strip_prefix_len = 1;
    e.synthetic = 1;
    e.node = *node;
    e.node.node_id = imap->next_node_id++;
    e.node.inode_identity = 0;
    e.node.link_count = 1;
    e.st.st_mode = (mode_t)node->mode;
    e.st.st_uid = (uid_t)node->uid;
    e.st.st_gid = (gid_t)node->gid;
    e.st.st_size = (off_t)node->size;
    e.st.st_nlink = 1;
    e.st.st_mtim.tv_sec = (time_t)node->mtime_sec;
    e.st.st_mtim.tv_nsec = (long)node->mtime_nsec;
    status_t st = result_append(res, &e, NULL);
    if (st != OK) { free(e.path); return st; }
    if (node->type == NODE_TYPE_DIR && dirmap_set(imap, rel, e.node.node_id) != 0)
        return set_error(ERR_NOMEM, "scan_add_synthetic: dirmap");
    if (out_index) *out_index = res->count - 1;
    return OK;
}

void scan_result_free(scan_result_t *res) {
    if (!res) return;
    for (uint32_t i = 0; i < res->count; i++) {
        free(res->entries[i].path);
        free(res->entries[i].symlink_target);
        free(res->entries[i].xattr_data);
        free(res->entries[i].acl_data);
        free(res->entries[i].read_path);
    }
    free(res->entries);
    for (uint32_t i = 0; i < res->warn_count; i++) free(res->warnings[i]);
    free(res->warnings);
    free(res);
}
