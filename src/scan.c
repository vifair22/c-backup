#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "scan.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
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

/* ------------------------------------------------------------------ */
/* Inode map: inode_identity (uint64) → first node_id (uint64)        */
/* Exposed as scan_imap_t so callers can share it across scan_tree()  */
/* calls to deduplicate hard links spanning multiple source roots.     */
/* ------------------------------------------------------------------ */

typedef struct { uint64_t key; uint64_t val; } imap_slot_t;
struct scan_imap { imap_slot_t *s; size_t cap; size_t cnt; };
typedef struct scan_imap imap_t;  /* local alias */

scan_imap_t *scan_imap_new(void) {
    imap_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;
    m->s = calloc(256, sizeof(imap_slot_t));
    if (!m->s) { free(m); return NULL; }
    m->cap = 256;
    return m;
}
void scan_imap_free(scan_imap_t *m) { if (m) { free(m->s); free(m); } }

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

static status_t result_append(scan_result_t *res, const scan_entry_t *e) {
    if (res->count == res->capacity) {
        uint32_t newcap = res->capacity ? res->capacity * 2 : 64;
        scan_entry_t *tmp = realloc(res->entries, newcap * sizeof(*tmp));
        if (!tmp) return ERR_NOMEM;
        res->entries  = tmp;
        res->capacity = newcap;
    }
    res->entries[res->count++] = *e;
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
    ssize_t list_sz = llistxattr(path, NULL, 0);
    if (list_sz <= 0) { *out = NULL; *out_len = 0; return OK; }

    char *names = malloc(list_sz);
    if (!names) return ERR_NOMEM;
    if (llistxattr(path, names, list_sz) < 0) { free(names); return ERR_IO; }

    size_t total = 0;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1) {
        ssize_t vsz = lgetxattr(path, n, NULL, 0);
        if (vsz < 0) continue;
        total += sizeof(uint16_t) + strlen(n) + 1 + sizeof(uint32_t) + (size_t)vsz;
    }
    if (total == 0) { free(names); *out = NULL; *out_len = 0; return OK; }

    uint8_t *buf = malloc(total);
    if (!buf) { free(names); return ERR_NOMEM; }
    uint8_t *p = buf;
    for (char *n = names; n < names + list_sz; n += strlen(n) + 1) {
        ssize_t vsz = lgetxattr(path, n, NULL, 0);
        if (vsz < 0) continue;
        uint16_t nlen = (uint16_t)strlen(n) + 1;
        memcpy(p, &nlen, sizeof(nlen)); p += sizeof(nlen);
        memcpy(p, n, nlen); p += nlen;
        char *vbuf = malloc(vsz);
        if (!vbuf) { free(buf); free(names); return ERR_NOMEM; }
        lgetxattr(path, n, vbuf, vsz);
        uint32_t v32 = (uint32_t)vsz;
        memcpy(p, &v32, sizeof(v32)); p += sizeof(v32);
        memcpy(p, vbuf, vsz); p += vsz;
        free(vbuf);
    }
    free(names);
    *out     = buf;
    *out_len = (size_t)(p - buf);
    return OK;
}

static status_t collect_acl(const char *path, uint8_t **out, size_t *out_len) {
    acl_t acl = acl_get_file(path, ACL_TYPE_ACCESS);
    if (!acl) { *out = NULL; *out_len = 0; return OK; }
    ssize_t sz;
    char *txt = acl_to_text(acl, &sz);
    acl_free(acl);
    if (!txt) { *out = NULL; *out_len = 0; return OK; }
    uint8_t *buf = malloc(sz);
    if (!buf) { acl_free(txt); return ERR_NOMEM; }
    memcpy(buf, txt, sz);
    acl_free(txt);
    *out     = buf;
    *out_len = (size_t)sz;
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
        log_msg("WARN", "lstat failed, skipping entry");
        return OK;
    }

    scan_entry_t e = {0};
    e.path             = strdup(path);
    if (!e.path) return ERR_NOMEM;
    e.parent_node_id   = parent_node_id;
    e.strip_prefix_len = strip_prefix_len;
    e.st               = st;

    node_t *nd = &e.node;
    nd->node_id        = next_node_id++;
    nd->inode_identity = (uint64_t)st.st_dev ^ ((uint64_t)st.st_ino << 32);
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
    if ((st2 = collect_xattrs(path, &e.xattr_data, &e.xattr_len)) != OK) {
        free(e.path); return st2;
    }
    if ((st2 = collect_acl(path, &e.acl_data, &e.acl_len)) != OK) {
        free(e.path); free(e.xattr_data); return st2;
    }

    uint64_t this_node_id = nd->node_id;

    if ((st2 = result_append(res, &e)) != OK) {
        free(e.path); free(e.xattr_data); free(e.acl_data); return st2;
    }

    if (S_ISDIR(st.st_mode))
        return scan_dir(path, this_node_id, strip_prefix_len, imap, opts, res);
    return OK;
}

static int is_excluded(const char *name, const scan_opts_t *opts) {
    if (!opts || !opts->exclude || opts->n_exclude == 0) return 0;
    for (int i = 0; i < opts->n_exclude; i++) {
        if (fnmatch(opts->exclude[i], name, FNM_PATHNAME) == 0 ||
            fnmatch(opts->exclude[i], name, 0) == 0)
            return 1;
    }
    return 0;
}

static status_t scan_dir(const char *path, uint64_t dir_node_id,
                         size_t strip_prefix_len, imap_t *imap,
                         const scan_opts_t *opts, scan_result_t *res) {
    DIR *d = opendir(path);
    if (!d) { log_msg("WARN", "cannot open directory"); return OK; }
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        if (is_excluded(de->d_name, opts)) continue;
        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", path, de->d_name);
        status_t st = scan_entry_at(child, dir_node_id, strip_prefix_len, imap, opts, res);
        if (st != OK) { closedir(d); return st; }
    }
    closedir(d);
    return OK;
}

status_t scan_tree(const char *root, scan_imap_t *imap,
                   const scan_opts_t *opts, scan_result_t **out) {
    scan_result_t *res = calloc(1, sizeof(*res));
    if (!res) return ERR_NOMEM;

    /*
     * strip_prefix_len: strip everything up to and including the last '/'
     * before the source root basename, so the stored relative path starts
     * with basename(root).
     *
     * e.g. root="/home/alice" → strip_prefix_len = strlen("/home/") = 6
     *      "/home/alice/file.txt" → rel = "alice/file.txt"
     * e.g. root="/etc" → strip_prefix_len = 1 (just the leading '/')
     *      "/etc/hosts" → rel = "etc/hosts"
     */
    const char *last_slash = strrchr(root, '/');
    size_t strip_prefix_len = last_slash ? (size_t)(last_slash - root + 1) : 0;

    status_t st = scan_entry_at(root, 0, strip_prefix_len, imap, opts, res);
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
    free(res);
}
