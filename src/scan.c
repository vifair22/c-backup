#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "scan.h"
#include "../vendor/log.h"

#include <dirent.h>
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

static uint64_t next_node_id = 1;

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

static status_t collect_xattrs(const char *path, uint8_t **out, size_t *out_len) {
    ssize_t list_sz_s = llistxattr(path, NULL, 0);
    if (list_sz_s <= 0) { *out = NULL; *out_len = 0; return OK; }
    size_t list_sz = (size_t)list_sz_s;

    char *names = malloc(list_sz);
    if (!names) return set_error(ERR_NOMEM, "collect_xattrs: alloc failed");
    if (llistxattr(path, names, list_sz) < 0) { free(names); return set_error_errno(ERR_IO, "llistxattr(%s)", path); }

    /* Count attributes and probe sizes in a single pass */
    size_t attr_count = 0;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1)
        attr_count++;

    ssize_t *sizes = malloc(attr_count * sizeof(ssize_t));
    if (!sizes) { free(names); return set_error(ERR_NOMEM, "collect_xattrs: alloc sizes failed"); }

    size_t total = 0, idx = 0;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1, idx++) {
        ssize_t vsz = lgetxattr(path, n, NULL, 0);
        sizes[idx] = vsz;
        if (vsz >= 0)
            total += sizeof(uint16_t) + strlen(n) + 1 + sizeof(uint32_t) + (size_t)vsz;
    }
    if (total == 0) { free(sizes); free(names); *out = NULL; *out_len = 0; return OK; }

    /* Second pass: read values directly into the output buffer using cached sizes */
    uint8_t *buf = malloc(total);
    if (!buf) { free(sizes); free(names); return set_error(ERR_NOMEM, "collect_xattrs: alloc xattr buf failed"); }
    uint8_t *p = buf;
    idx = 0;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1, idx++) {
        if (sizes[idx] < 0) continue;
        uint16_t nlen = (uint16_t)strlen(n) + 1;
        memcpy(p, &nlen, sizeof(nlen)); p += sizeof(nlen);
        memcpy(p, n, nlen); p += nlen;
        uint32_t v32 = (uint32_t)sizes[idx];
        memcpy(p, &v32, sizeof(v32)); p += sizeof(v32);
        if (sizes[idx] > 0)
            lgetxattr(path, n, p, (size_t)sizes[idx]);
        p += (size_t)sizes[idx];
    }
    free(sizes);
    free(names);
    *out     = buf;
    *out_len = (size_t)(p - buf);
    return OK;
}

static status_t collect_acl(const char *path, uint8_t **out, size_t *out_len) {
    acl_t acl = acl_get_file(path, ACL_TYPE_ACCESS);
    if (!acl) { *out = NULL; *out_len = 0; return OK; }
    ssize_t sz_s;
    char *txt = acl_to_text(acl, &sz_s);
    acl_free(acl);
    if (!txt) { *out = NULL; *out_len = 0; return OK; }
    size_t sz = (size_t)sz_s;
    uint8_t *buf = malloc(sz);
    if (!buf) { acl_free(txt); return set_error(ERR_NOMEM, "collect_acl: alloc failed"); }
    memcpy(buf, txt, sz);
    acl_free(txt);
    *out     = buf;
    *out_len = (size_t)sz;
    return OK;
}

status_t scan_entry_collect_metadata(scan_entry_t *e) {
    if (!e || !e->path) return set_error(ERR_INVALID, "scan_entry_collect_metadata: null entry");
    if (e->xattr_data || e->xattr_len || e->acl_data || e->acl_len) return OK;

    status_t st = collect_xattrs(e->path, &e->xattr_data, &e->xattr_len);
    if (st != OK) return st;
    st = collect_acl(e->path, &e->acl_data, &e->acl_len);
    if (st != OK) {
        free(e->xattr_data);
        e->xattr_data = NULL;
        e->xattr_len = 0;
        return st;
    }
    return OK;
}

/* Forward declaration */
static status_t scan_dir(const char *path, uint64_t dir_node_id,
                         size_t strip_prefix_len, imap_t *imap,
                         const scan_opts_t *opts, scan_result_t *res);

static status_t scan_entry_at(const char *path, uint64_t parent_node_id,
                                size_t strip_prefix_len, imap_t *imap,
                                const scan_opts_t *opts, scan_result_t *res) {
    struct stat st;
    if (lstat(path, &st) == -1) {
        if (opts && opts->progress_clear_cb)
            opts->progress_clear_cb(opts->progress_ctx);
        char msg[PATH_MAX + 96];
        snprintf(msg, sizeof(msg), "lstat failed, skipping: %s (%s)", path, strerror(errno));
        scan_warn(res, msg);
        log_msg("WARN", msg);
        return OK;
    }

    scan_entry_t e = {0};
    e.path             = strdup(path);
    if (!e.path) return set_error(ERR_NOMEM, "scan: strdup failed for '%s'", path);
    e.parent_node_id   = parent_node_id;
    e.strip_prefix_len = strip_prefix_len;
    e.st               = st;

    node_t *nd = &e.node;
    nd->node_id        = next_node_id++;
    nd->inode_identity = inode_identity(st.st_dev, st.st_ino);
    nd->type           = stat_to_node_type(&st);
    nd->mode           = (uint32_t)st.st_mode;
    nd->uid            = (uint32_t)st.st_uid;
    nd->gid            = (uint32_t)st.st_gid;
    nd->size           = (uint64_t)st.st_size;
    nd->mtime_sec      = (uint64_t)st.st_mtim.tv_sec;
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
        ssize_t tlen = readlink(path, target, sizeof(target) - 1);
        if (tlen > 0) {
            target[tlen] = '\0';
            nd->symlink.target_len = (uint32_t)tlen + 1;
            e.symlink_target = strdup(target);
        }
    }

    status_t st2;
    int collect_meta = (!opts || opts->collect_meta);
    if (collect_meta) {
        if ((st2 = collect_xattrs(path, &e.xattr_data, &e.xattr_len)) != OK) {
            free(e.path); return st2;
        }
        if ((st2 = collect_acl(path, &e.acl_data, &e.acl_len)) != OK) {
            free(e.path); free(e.xattr_data); return st2;
        }
    }

    uint64_t this_node_id = nd->node_id;

    if ((st2 = result_append(res, &e, opts)) != OK) {
        free(e.path); free(e.xattr_data); free(e.acl_data); return st2;
    }

    if (nd->type == NODE_TYPE_DIR && e.path && e.path[0] == '/' &&
        e.strip_prefix_len == 1 && e.path[1] != '\0') {
        const char *rel = e.path + 1;
        if (!dirmap_get(imap, rel)) (void)dirmap_set(imap, rel, nd->node_id);
    }

    if (S_ISDIR(st.st_mode))
        return scan_dir(path, this_node_id, strip_prefix_len, imap, opts, res);
    return OK;
}

static int path_has_prefix(const char *path, const char *prefix) {
    if (!path || !prefix) return 0;
    size_t plen = strlen(prefix);
    while (plen > 1 && prefix[plen - 1] == '/') plen--;
    if (strncmp(path, prefix, plen) != 0) return 0;
    return path[plen] == '\0' || path[plen] == '/';
}

static int is_excluded(const char *full_path, const scan_opts_t *opts) {
    if (!opts || !opts->exclude || opts->n_exclude == 0) return 0;
    for (int i = 0; i < opts->n_exclude; i++) {
        const char *ex = opts->exclude[i];
        if (!ex || ex[0] != '/') continue;
        if (path_has_prefix(full_path, ex)) return 1;
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

static status_t scan_dir(const char *path, uint64_t dir_node_id,
                         size_t strip_prefix_len, imap_t *imap,
                         const scan_opts_t *opts, scan_result_t *res) {
    DIR *d = opendir(path);
    if (!d) {
        if (opts && opts->progress_clear_cb)
            opts->progress_clear_cb(opts->progress_ctx);
        char msg[PATH_MAX + 96];
        snprintf(msg, sizeof(msg), "cannot open directory: %s (%s)", path, strerror(errno));
        scan_warn(res, msg);
        log_msg("WARN", msg);
        return OK;
    }

    /* Phase 1: batch all directory entries */
    size_t cap = 256, cnt = 0;
    scan_dirent_t *batch = malloc(cap * sizeof(*batch));
    if (!batch) { closedir(d); return set_error(ERR_NOMEM, "scan_dir: dirent batch alloc"); }

    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.' &&
            (de->d_name[1] == '\0' ||
             (de->d_name[1] == '.' && de->d_name[2] == '\0')))
            continue;
        if (cnt == cap) {
            size_t nc = cap * 2;
            scan_dirent_t *tmp = realloc(batch, nc * sizeof(*tmp));
            if (!tmp) { free(batch); closedir(d); return set_error(ERR_NOMEM, "scan_dir: dirent batch realloc"); }
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
            log_msg("WARN", "path too long, skipping entry");
            continue;
        }
        memcpy(child, path, plen);
        child[plen] = '/';
        memcpy(child + plen + 1, batch[i].name, nlen + 1);
        if (is_excluded(child, opts)) continue;
        st = scan_entry_at(child, dir_node_id, strip_prefix_len, imap, opts, res);
    }
    free(batch);
    return st;
}

status_t scan_tree(const char *root, scan_imap_t *imap,
                   const scan_opts_t *opts, scan_result_t **out) {
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
        log_msg("WARN", msg);
        *out = res;
        return OK;
    }

    uint64_t root_parent_id = 0;
    if (root[0] == '/' && strip_prefix_len == 1) {
        const char *rel = root + 1; /* no leading slash */
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
            if (lstat(abs_path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

            scan_entry_t e = {0};
            e.path = strdup(abs_path);
            if (!e.path) { scan_result_free(res); return set_error(ERR_NOMEM, "scan_tree: strdup failed"); }
            e.parent_node_id = root_parent_id;
            e.strip_prefix_len = 1;
            e.st = st;

            node_t *nd = &e.node;
            nd->node_id        = next_node_id++;
            nd->inode_identity = inode_identity(st.st_dev, st.st_ino);
            nd->type           = NODE_TYPE_DIR;
            nd->mode           = (uint32_t)st.st_mode;
            nd->uid            = (uint32_t)st.st_uid;
            nd->gid            = (uint32_t)st.st_gid;
            nd->size           = (uint64_t)st.st_size;
            nd->mtime_sec      = (uint64_t)st.st_mtim.tv_sec;
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
        log_msg("WARN", msg);
        scan_result_free(res);
        res = calloc(1, sizeof(*res));
        if (!res) return set_error(ERR_NOMEM, "scan_tree: realloc result failed");
        scan_warn(res, msg);
        *out = res;
        return OK;
    }

    status_t st = scan_entry_at(root, root_parent_id, strip_prefix_len, imap, opts, res);
    if (st != OK) { scan_result_free(res); return st; }
    *out = res;
    return OK;
}

void scan_result_free(scan_result_t *res) {
    if (!res) return;
    for (uint32_t i = 0; i < res->count; i++) {
        free(res->entries[i].path);
        free(res->entries[i].symlink_target);
        free(res->entries[i].xattr_data);
        free(res->entries[i].acl_data);
    }
    free(res->entries);
    for (uint32_t i = 0; i < res->warn_count; i++) free(res->warnings[i]);
    free(res->warnings);
    free(res);
}
