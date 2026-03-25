#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "restore.h"
#include "snapshot.h"
#include "object.h"
#include "util.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>
#include <acl/libacl.h>
#include <sys/xattr.h>
#include <openssl/evp.h>

static void restore_fmt_bytes(uint64_t n, char *buf, size_t sz) {
    if      (n >= (uint64_t)1024*1024*1024)
        snprintf(buf, sz, "%.1f GB", (double)n / (1024.0*1024*1024));
    else if (n >= 1024*1024)
        snprintf(buf, sz, "%.1f MB", (double)n / (1024.0*1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1f KB", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%" PRIu64 " B", n);
}

/* ------------------------------------------------------------------ */
/* Path reconstruction from a snapshot's dirent tree                  */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t     node_id;
    const node_t *node;      /* pointer into snap->nodes */
    char        *path;       /* repo-relative path (heap-allocated) */
} snap_pe_t;                 /* path entry */

typedef struct {
    snap_pe_t *entries;
    uint32_t   count;
} snap_paths_t;

/* Temporary flat record used during reconstruction */
typedef struct {
    uint64_t parent_node_id;
    uint64_t node_id;
    char    *name;
    char    *full_path;   /* built on demand */
} dr_entry_t;

static dr_entry_t *find_dr_by_id(dr_entry_t *arr, uint32_t n, uint64_t id) {
    for (uint32_t i = 0; i < n; i++)
        if (arr[i].node_id == id) return &arr[i];
    return NULL;
}

static char *build_dr_path(dr_entry_t *arr, uint32_t n, dr_entry_t *e) {
    if (e->full_path) return e->full_path;
    if (e->parent_node_id == 0) {
        e->full_path = strdup(e->name);
        return e->full_path;
    }
    dr_entry_t *parent = find_dr_by_id(arr, n, e->parent_node_id);
    if (!parent) { e->full_path = strdup(e->name); return e->full_path; }
    char *pp = build_dr_path(arr, n, parent);
    if (!pp) return NULL;
    size_t plen = strlen(pp), nlen = strlen(e->name);
    char *fp = malloc(plen + 1 + nlen + 1);
    if (!fp) return NULL;
    memcpy(fp, pp, plen);
    fp[plen] = '/';
    memcpy(fp + plen + 1, e->name, nlen + 1);
    e->full_path = fp;
    return fp;
}

static status_t snap_paths_build(const snapshot_t *snap, snap_paths_t *out) {
    out->entries = NULL;
    out->count   = 0;
    if (snap->dirent_count == 0) return OK;

    dr_entry_t *flat = calloc(snap->dirent_count, sizeof(dr_entry_t));
    if (!flat) return set_error(ERR_NOMEM, "snap_paths_build: alloc failed for dirent flat array");
    uint32_t n_flat = 0;

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;

    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;
        char *name = malloc(dr.name_len + 1);
        if (!name) goto oom;
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;
        flat[n_flat].parent_node_id = dr.parent_node;
        flat[n_flat].node_id        = dr.node_id;
        flat[n_flat].name           = name;
        flat[n_flat].full_path      = NULL;
        n_flat++;
    }

    snap_pe_t *entries = malloc(n_flat * sizeof(snap_pe_t));
    if (!entries) goto oom;

    for (uint32_t i = 0; i < n_flat; i++) {
        char *fp = build_dr_path(flat, n_flat, &flat[i]);
        if (!fp) { free(entries); goto oom; }

        /* find the node */
        const node_t *nd = NULL;
        for (uint32_t j = 0; j < snap->node_count; j++) {
            if (snap->nodes[j].node_id == flat[i].node_id) { nd = &snap->nodes[j]; break; }
        }

        entries[i].node_id = flat[i].node_id;
        entries[i].node    = nd;
        entries[i].path    = strdup(fp);
        if (!entries[i].path) { free(entries); goto oom; }
    }

    for (uint32_t i = 0; i < n_flat; i++) { free(flat[i].name); free(flat[i].full_path); }
    free(flat);
    out->entries = entries;
    out->count   = n_flat;
    return OK;

oom:
    for (uint32_t i = 0; i < n_flat; i++) { free(flat[i].name); free(flat[i].full_path); }
    free(flat);
    return set_error(ERR_NOMEM, "snap_paths_build: alloc failed for path entries");
}

static void snap_paths_free(snap_paths_t *sp) {
    if (!sp->entries) return;
    for (uint32_t i = 0; i < sp->count; i++) free(sp->entries[i].path);
    free(sp->entries);
    sp->entries = NULL;
    sp->count   = 0;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE]) {
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

/* Path traversal guard */
static int path_is_safe(const char *rel) {
    if (!rel || rel[0] == '/') return 0;
    /* reject any ".." component */
    const char *p = rel;
    while (*p) {
        if (p[0] == '.' && p[1] == '.' && (p[2] == '/' || p[2] == '\0')) return 0;
        while (*p && *p != '/') p++;
        if (*p == '/') p++;
    }
    return 1;
}

/* write_all_fd: uses io_write_full from util.h */
#define write_all_fd io_write_full

static int write_hex_fd(int fd, const uint8_t *buf, size_t len, size_t *ioff) {
    static const char hexdig[] = "0123456789abcdef";
    char line[80];
    size_t off = 0;
    size_t base = ioff ? *ioff : 0;
    while (off < len) {
        size_t n = (len - off > 16) ? 16 : (len - off);
        int pos = snprintf(line, sizeof(line), "%08zx: ", base + off);
        if (pos < 0) return -1;
        for (size_t i = 0; i < n; i++) {
            unsigned b = buf[off + i];
            line[pos++] = hexdig[(b >> 4) & 0xF];
            line[pos++] = hexdig[b & 0xF];
            line[pos++] = (i == 7) ? ' ' : ' ';
        }
        line[pos++] = '\n';
        if (write_all_fd(fd, line, (size_t)pos) != 0) return -1;
        off += n;
    }
    if (ioff) *ioff += len;
    return 0;
}

static int normalize_snapshot_path(const char *in, char *out, size_t out_sz) {
    if (!in || !*in || !out || out_sz == 0) return -1;
    const char *s = in;
    while (*s == '/') s++;
    size_t len = strlen(s);
    while (len > 0 && s[len - 1] == '/') len--;
    if (len == 0 || len >= out_sz) return -1;
    memcpy(out, s, len);
    out[len] = '\0';
    return 0;
}

static int join_dest_path(char *out, size_t out_sz,
                          const char *dest_path, const char *rel_path) {
    int n = snprintf(out, out_sz, "%s/%s", dest_path, rel_path);
    return (n >= 0 && (size_t)n < out_sz) ? 0 : -1;
}

/* Returns count of non-fatal metadata failures (permissions, ACL, xattr).
 * These are expected when not running as root and do not abort the restore. */
static int apply_metadata(const char *full, const node_t *nd,
                          repo_t *repo, int is_symlink) {
    int warn = 0;
    if (lchown(full, (uid_t)nd->uid, (gid_t)nd->gid) == -1)
        warn++;
    if (!is_symlink && chmod(full, (mode_t)nd->mode) == -1)
        warn++;

    /* xattrs */
    if (!hash_is_zero(nd->xattr_hash)) {
        void *xd = NULL; size_t xl = 0;
        if (object_load(repo, nd->xattr_hash, &xd, &xl, NULL) == OK) {
            uint8_t *pp = xd, *ep = pp + xl;
            while (pp + 2 <= ep) {
                uint16_t nlen; memcpy(&nlen, pp, 2); pp += 2;
                if (pp + nlen > ep) break;
                char *xname = (char *)pp; pp += nlen;
                if (pp + 4 > ep) break;
                uint32_t vlen; memcpy(&vlen, pp, 4); pp += 4;
                if (pp + vlen > ep) break;
                if (lsetxattr(full, xname, pp, vlen, 0) == -1) warn++;
                pp += vlen;
            }
            free(xd);
        }
    }

    /* ACL */
    if (!hash_is_zero(nd->acl_hash)) {
        void *ad = NULL; size_t al = 0;
        if (object_load(repo, nd->acl_hash, &ad, &al, NULL) == OK) {
            acl_t acl = acl_from_text((char *)ad);
            if (acl) {
                if (acl_set_file(full, ACL_TYPE_ACCESS, acl) == -1) warn++;
                acl_free(acl);
            }
            free(ad);
        }
    }

    /* timestamps – applied last so dir mtimes aren't clobbered by child creation */
    struct timespec times[2] = {
        { .tv_sec = (time_t)nd->mtime_sec, .tv_nsec = (long)nd->mtime_nsec },
        { .tv_sec = (time_t)nd->mtime_sec, .tv_nsec = (long)nd->mtime_nsec },
    };
    utimensat(AT_FDCWD, full,
              times, is_symlink ? AT_SYMLINK_NOFOLLOW : 0);
    return warn;
}

/* ------------------------------------------------------------------ */
/* Core restore                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *path;
    const node_t *node;
    uint64_t node_id;
} restore_entry_t;

typedef struct {
    uint32_t files;
    uint64_t bytes;
    uint32_t warns;
} restore_stats_t;

static status_t restore_make_dirs(const restore_entry_t *entries, uint32_t count,
                                  const char *dest_path) {
    int progress = 1;
    while (progress) {
        progress = 0;
        for (uint32_t i = 0; i < count; i++) {
            const restore_entry_t *e = &entries[i];
            if (!e->node || e->node->type != NODE_TYPE_DIR) continue;
            if (!path_is_safe(e->path)) {
                set_error(ERR_INVALID, "unsafe path in snapshot: %s", e->path);
                log_msg("ERROR", err_msg());
                continue;
            }
            char full[PATH_MAX];
            if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0)
                return set_error(ERR_IO, "restore_make_dirs: path too long: %s", e->path);
            if (mkdir(full, 0700) == 0) progress = 1;
            else if (errno != EEXIST && errno != ENOENT) return set_error_errno(ERR_IO, "restore_make_dirs: mkdir(%s)", full);
        }
    }
    return OK;
}

static status_t restore_materialize_nodes(repo_t *repo,
                                          const restore_entry_t *entries,
                                          uint32_t count,
                                          const char *dest_path,
                                          restore_stats_t *stats) {
    typedef struct { uint64_t node_id; char path[PATH_MAX]; } nl_entry_t;
    nl_entry_t *nl_map = calloc(count, sizeof(nl_entry_t));
    uint32_t nl_cnt = 0;
    if (!nl_map) return set_error(ERR_NOMEM, "restore_materialize_nodes: alloc failed for hardlink map");

    for (uint32_t i = 0; i < count; i++) {
        const restore_entry_t *e = &entries[i];
        if (!e->node || e->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(e->path)) continue;

        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0) {
            free(nl_map);
            return set_error(ERR_IO, "restore: path too long: %s", e->path);
        }
        const node_t *nd = e->node;

        const char *hl_src = NULL;
        for (uint32_t k = 0; k < nl_cnt; k++) {
            if (nl_map[k].node_id == e->node_id) { hl_src = nl_map[k].path; break; }
        }
        if (hl_src) {
            if (link(hl_src, full) == -1 && errno != EEXIST) {
                {
                    char emsg[128];
                    snprintf(emsg, sizeof(emsg), "hard link failed: %s; falling through to copy", strerror(errno));
                    log_msg("WARN", emsg);
                }
            } else {
                nl_map[nl_cnt].node_id = e->node_id;
                snprintf(nl_map[nl_cnt].path, PATH_MAX, "%s", full);
                nl_cnt++;
                continue;
            }
        }

        switch (nd->type) {
        case NODE_TYPE_REG: {
            int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (fd == -1) { free(nl_map); return set_error_errno(ERR_IO, "restore: open(%s)", full); }
            if (!hash_is_zero(nd->content_hash)) {
                void *data = NULL; size_t len = 0; uint8_t obj_type = 0;
                status_t load_st = object_load(repo, nd->content_hash, &data, &len, &obj_type);
                if (load_st == ERR_TOO_LARGE) {
                    /* Large regular file: stream directly to the destination fd.
                     * Pass &obj_type so sparse type detection still works. */
                    uint64_t stream_sz = 0;
                    load_st = object_load_stream(repo, nd->content_hash, fd,
                                                 &stream_sz, &obj_type);
                }
                if (load_st != OK) { close(fd); free(nl_map); return load_st; }
                if (obj_type == OBJECT_TYPE_SPARSE && len >= sizeof(sparse_hdr_t)) {
                    const uint8_t *sp_p = (const uint8_t *)data;
                    sparse_hdr_t shdr;
                    memcpy(&shdr, sp_p, sizeof(shdr));
                    sp_p += sizeof(shdr);
                    if (shdr.magic != SPARSE_MAGIC ||
                        len < sizeof(sparse_hdr_t) + shdr.region_count * sizeof(sparse_region_t)) {
                        free(data); close(fd); free(nl_map); return set_error(ERR_CORRUPT, "restore: invalid sparse header for %s", full);
                    }
                    if (ftruncate(fd, (off_t)nd->size) == -1 && nd->size > 0) {
                        free(data); close(fd); free(nl_map); return set_error_errno(ERR_IO, "restore: ftruncate(%s)", full);
                    }
                    const sparse_region_t *rgns = (const sparse_region_t *)sp_p;
                    sp_p += shdr.region_count * sizeof(sparse_region_t);
                    const uint8_t *dptr = sp_p;
                    const uint8_t *dend = (const uint8_t *)data + len;
                    uint64_t pos = 0;
                    for (uint32_t r = 0; r < shdr.region_count; r++) {
                        if (rgns[r].offset < pos ||
                            rgns[r].offset > nd->size ||
                            rgns[r].length > nd->size - rgns[r].offset) {
                            free(data); close(fd); free(nl_map); return set_error(ERR_CORRUPT, "restore: sparse region %u out of bounds for %s", r, full);
                        }
                        if ((size_t)(dend - dptr) < (size_t)rgns[r].length) {
                            free(data); close(fd); free(nl_map); return set_error(ERR_CORRUPT, "restore: sparse region %u data truncated for %s", r, full);
                        }
                        if (lseek(fd, (off_t)rgns[r].offset, SEEK_SET) == (off_t)-1) {
                            free(data); close(fd); free(nl_map); return set_error_errno(ERR_IO, "restore: lseek(%s)", full);
                        }
                        size_t rem = (size_t)rgns[r].length;
                        while (rem > 0) {
                            ssize_t w = write(fd, dptr, rem);
                            if (w < 0) {
                                if (errno == EINTR) continue;
                                free(data); close(fd); free(nl_map); return set_error_errno(ERR_IO, "restore: write sparse region to %s", full);
                            }
                            if (w == 0) {
                                free(data); close(fd); free(nl_map); return set_error(ERR_IO, "restore: zero-length write to %s", full);
                            }
                            dptr += w;
                            rem -= (size_t)w;
                        }
                        pos = rgns[r].offset + rgns[r].length;
                    }
                } else {
                    if (len > 0 && write(fd, data, len) != (ssize_t)len) {
                        free(data); close(fd); free(nl_map); return set_error_errno(ERR_IO, "restore: write(%s)", full);
                    }
                }
                free(data);
            }
            if (ftruncate(fd, (off_t)nd->size) == -1 && nd->size > 0) {
                close(fd); free(nl_map); return set_error_errno(ERR_IO, "restore: ftruncate final(%s)", full);
            }
            fsync(fd);
            close(fd);
            stats->files++;
            stats->bytes += nd->size;
            nl_map[nl_cnt].node_id = e->node_id;
            snprintf(nl_map[nl_cnt].path, PATH_MAX, "%s", full);
            nl_cnt++;
            break;
        }
        case NODE_TYPE_SYMLINK: {
            if (hash_is_zero(nd->content_hash)) break;
            void *tdata = NULL; size_t tlen = 0;
            if (object_load(repo, nd->content_hash, &tdata, &tlen, NULL) != OK) break;
            unlink(full);
            if (symlink((char *)tdata, full) == -1 && errno != EEXIST) {
                fprintf(stderr, "warn: symlink failed: %s: %s\n", full, strerror(errno));
                stats->warns++;
            } else {
                stats->files++;
            }
            free(tdata);
            break;
        }
        case NODE_TYPE_FIFO:
            if (mkfifo(full, (mode_t)nd->mode) == -1 && errno != EEXIST) {
                fprintf(stderr, "warn: mkfifo failed: %s: %s\n", full, strerror(errno));
                stats->warns++;
            } else {
                stats->files++;
            }
            break;
        case NODE_TYPE_CHR:
        case NODE_TYPE_BLK: {
            dev_t dev = makedev(nd->device.major, nd->device.minor);
            mode_t m  = (nd->type == NODE_TYPE_CHR ? S_IFCHR : S_IFBLK) | (mode_t)nd->mode;
            if (mknod(full, m, dev) == -1 && errno != EEXIST) {
                fprintf(stderr, "warn: mknod failed: %s: %s\n", full, strerror(errno));
                stats->warns++;
            } else {
                stats->files++;
            }
            break;
        }
        default: break;
        }
    }

    free(nl_map);
    return OK;
}

static status_t restore_apply_metadata_pass(repo_t *repo,
                                            const restore_entry_t *entries,
                                            uint32_t count,
                                            const char *dest_path,
                                            const node_t *root_dir_meta,
                                            restore_stats_t *stats) {
    for (uint32_t i = 0; i < count; i++) {
        const restore_entry_t *e = &entries[i];
        if (!e->node || e->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(e->path)) continue;
        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0)
            return set_error(ERR_IO, "restore_apply_metadata: path too long: %s", e->path);
        stats->warns += (uint32_t)apply_metadata(full, e->node, repo,
                                                 e->node->type == NODE_TYPE_SYMLINK);
    }
    for (uint32_t i = count; i-- > 0;) {
        const restore_entry_t *e = &entries[i];
        if (!e->node || e->node->type != NODE_TYPE_DIR) continue;
        if (!path_is_safe(e->path)) continue;
        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0)
            return set_error(ERR_IO, "restore_apply_metadata: path too long: %s", e->path);
        stats->warns += (uint32_t)apply_metadata(full, e->node, repo, 0);
    }
    if (root_dir_meta)
        stats->warns += (uint32_t)apply_metadata(dest_path, root_dir_meta, repo, 0);
    return OK;
}

static status_t restore_entries(repo_t *repo,
                                const restore_entry_t *entries,
                                uint32_t count,
                                const char *dest_path,
                                const node_t *root_dir_meta) {
    if (mkdir(dest_path, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "restore: mkdir(%s)", dest_path);

    restore_stats_t stats = {0};
    status_t st = restore_make_dirs(entries, count, dest_path);
    if (st != OK) return st;

    st = restore_materialize_nodes(repo, entries, count, dest_path, &stats);
    if (st != OK) return st;

    {
        char sz[32];
        restore_fmt_bytes(stats.bytes, sz, sizeof(sz));
        fprintf(stderr, "restored: %u file(s)  %s\n", stats.files, sz);
    }

    st = restore_apply_metadata_pass(repo, entries, count, dest_path, root_dir_meta, &stats);
    if (st != OK) return st;

    if (stats.warns > 0)
        fprintf(stderr, "warn: %u metadata/special-file error(s) — "
                "ownership, permissions, xattrs, and/or special files may be incomplete "
                "(try running as root)\n", stats.warns);
    return OK;
}

static status_t do_restore(repo_t *repo, const snapshot_t *snap,
                           const char *dest_path) {
    snap_paths_t sp = {0};
    status_t st = snap_paths_build(snap, &sp);
    if (st != OK) return st;

    restore_entry_t *entries = calloc(sp.count ? sp.count : 1, sizeof(*entries));
    if (!entries) {
        snap_paths_free(&sp);
        return set_error(ERR_NOMEM, "do_restore: alloc failed for restore entries");
    }

    const node_t *root_dir_meta = NULL;
    for (uint32_t i = 0; i < sp.count; i++) {
        entries[i].path = sp.entries[i].path;
        entries[i].node = sp.entries[i].node;
        entries[i].node_id = sp.entries[i].node_id;
        if (!root_dir_meta && sp.entries[i].node &&
            sp.entries[i].node->type == NODE_TYPE_DIR &&
            strchr(sp.entries[i].path, '/') == NULL) {
            root_dir_meta = sp.entries[i].node;
        }
    }

    st = restore_entries(repo, entries, sp.count, dest_path, root_dir_meta);
    free(entries);
    snap_paths_free(&sp);
    return st;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

status_t restore_latest(repo_t *repo, const char *dest_path) {
    uint32_t head_id = 0;
    if (snapshot_read_head(repo, &head_id) != OK || head_id == 0) {
        return set_error(ERR_NOT_FOUND, "no snapshots in repository");
    }
    return restore_snapshot(repo, head_id, dest_path);
}

status_t restore_snapshot(repo_t *repo, uint32_t snap_id, const char *dest_path) {
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    fprintf(stderr, "restoring snapshot %u -> %s\n", snap_id, dest_path);
    st = do_restore(repo, snap, dest_path);
    snapshot_free(snap);
    return st;
}

typedef struct {
    char  *path;
    node_t node;
} verify_ws_entry_t;

status_t restore_verify_dest(repo_t *repo, uint32_t snap_id,
                               const char *dest_path) {
    /* Load snapshot working set */
    verify_ws_entry_t *ws  = NULL;
    uint32_t    cnt = 0;

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    pathmap_t *pm = NULL;
    if (pathmap_build(snap, &pm) != OK) {
        snapshot_free(snap);
        return set_error(ERR_IO, "restore_verify: pathmap_build failed for snap %u", snap_id);
    }
    snapshot_free(snap);

    ws = calloc(pm->capacity, sizeof(*ws));
    if (!ws) {
        pathmap_free(pm);
        return set_error(ERR_NOMEM, "restore_verify: alloc failed for working set");
    }
    for (size_t i = 0; i < pm->capacity; i++) {
        if (!pm->slots[i].key) continue;
        ws[cnt].path = strdup(pm->slots[i].key);
        if (!ws[cnt].path) {
            for (uint32_t j = 0; j < cnt; j++) free(ws[j].path);
            free(ws);
            pathmap_free(pm);
            return set_error(ERR_NOMEM, "restore_verify: alloc failed for path entry");
        }
        ws[cnt].node = pm->slots[i].value;
        cnt++;
    }
    pathmap_free(pm);

    int errors = 0;
    uint8_t read_hash[OBJECT_HASH_SIZE];
    uint8_t expected_hash[OBJECT_HASH_SIZE];
    static const uint8_t zero_buf[65536];   /* BSS zeroes, no stack cost */

    for (uint32_t i = 0; i < cnt; i++) {
        if (!ws[i].path) continue;
        if (ws[i].node.type != NODE_TYPE_REG) continue;
        if (hash_is_zero(ws[i].node.content_hash)) continue;

        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, ws[i].path) != 0) {
            fprintf(stderr, "verify: path too long: %s\n", ws[i].path);
            errors++; continue;
        }

        /* Hash the restored file on disk */
        int fd = open(full, O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "verify: cannot open %s\n", full);
            errors++; continue;
        }
        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (!ctx || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
            if (ctx) EVP_MD_CTX_free(ctx);
            fprintf(stderr, "verify: cannot init digest for %s\n", ws[i].path);
            errors++; close(fd); continue;
        }
        uint8_t buf[65536];
        ssize_t nr;
        while ((nr = read(fd, buf, sizeof(buf))) > 0)
            EVP_DigestUpdate(ctx, buf, (size_t)nr);
        close(fd);
        if (nr < 0 || EVP_DigestFinal_ex(ctx, read_hash, NULL) != 1) {
            fprintf(stderr, "verify: cannot hash %s\n", ws[i].path);
            EVP_MD_CTX_free(ctx);
            errors++; continue;
        }
        EVP_MD_CTX_free(ctx);

        /* Compute the expected hash from the stored object.
         * For OBJECT_TYPE_FILE the object payload IS the raw bytes, so
         * SHA256(object_data) == content_hash and we can shortcut.
         * For OBJECT_TYPE_SPARSE the content_hash covers the sparse
         * payload (header+regions+data), not the expanded file bytes,
         * so we reconstruct the expected hash by streaming through
         * the regions and filling gaps with zeros. */
        void *obj_data = NULL; size_t obj_len = 0; uint8_t obj_type = 0;
        {
            status_t oload_st = object_load(repo, ws[i].node.content_hash,
                                            &obj_data, &obj_len, &obj_type);
            if (oload_st == ERR_TOO_LARGE) {
                /* Large regular file: expected hash == content_hash by definition. */
                /* obj_data=NULL, obj_len=0 — SPARSE branch will be skipped below. */
            } else if (oload_st != OK) {
                fprintf(stderr, "verify: cannot load object for %s\n", ws[i].path);
                errors++; continue;
            }
        }

        if (obj_type != OBJECT_TYPE_SPARSE ||
            obj_len < sizeof(sparse_hdr_t)) {
            /* Non-sparse: expected hash == content_hash */
            memcpy(expected_hash, ws[i].node.content_hash, OBJECT_HASH_SIZE);
        } else {
            /* Sparse: stream expected bytes = zeros + region data */
            sparse_hdr_t shdr;
            memcpy(&shdr, obj_data, sizeof(shdr));
            if (shdr.magic != SPARSE_MAGIC ||
                obj_len < sizeof(sparse_hdr_t) +
                          shdr.region_count * sizeof(sparse_region_t)) {
                memcpy(expected_hash, ws[i].node.content_hash, OBJECT_HASH_SIZE);
            } else {
                const sparse_region_t *rgns =
                    (const sparse_region_t *)((uint8_t *)obj_data + sizeof(shdr));
                const uint8_t *rdata =
                    (const uint8_t *)obj_data + sizeof(shdr)
                    + shdr.region_count * sizeof(sparse_region_t);

                EVP_MD_CTX *ectx = EVP_MD_CTX_new();
                if (!ectx || EVP_DigestInit_ex(ectx, EVP_sha256(), NULL) != 1) {
                    if (ectx) EVP_MD_CTX_free(ectx);
                    free(obj_data);
                    fprintf(stderr, "verify: cannot init expected digest for %s\n", ws[i].path);
                    errors++;
                    continue;
                }
                uint64_t pos = 0;
                for (uint32_t r = 0; r < shdr.region_count; r++) {
                    /* zeros covering the hole before this region */
                    uint64_t gap = rgns[r].offset - pos;
                    while (gap > 0) {
                        uint64_t chunk = gap < sizeof(zero_buf)
                                         ? gap : sizeof(zero_buf);
                        EVP_DigestUpdate(ectx, zero_buf, (size_t)chunk);
                        gap -= chunk; pos += chunk;
                    }
                    EVP_DigestUpdate(ectx, rdata, (size_t)rgns[r].length);
                    rdata += rgns[r].length;
                    pos   += rgns[r].length;
                }
                /* trailing zeros to reach file size */
                uint64_t tail = ws[i].node.size - pos;
                while (tail > 0) {
                    uint64_t chunk = tail < sizeof(zero_buf)
                                     ? tail : sizeof(zero_buf);
                    EVP_DigestUpdate(ectx, zero_buf, (size_t)chunk);
                    tail -= chunk;
                }
                if (EVP_DigestFinal_ex(ectx, expected_hash, NULL) != 1) {
                    EVP_MD_CTX_free(ectx);
                    free(obj_data);
                    fprintf(stderr, "verify: cannot finalize expected digest for %s\n", ws[i].path);
                    errors++;
                    continue;
                }
                EVP_MD_CTX_free(ectx);
            }
        }
        free(obj_data);

        if (memcmp(read_hash, expected_hash, OBJECT_HASH_SIZE) != 0) {
            fprintf(stderr, "verify: hash mismatch: %s\n", ws[i].path);
            errors++;
        }
    }

    for (uint32_t i = 0; i < cnt; i++) free(ws[i].path);
    free(ws);

    if (errors == 0) {
        fprintf(stderr, "verify: all files OK\n");
        return OK;
    }
    fprintf(stderr, "verify: %d error(s) found\n", errors);
    return set_error(ERR_CORRUPT, "verify: %d file(s) failed hash check in snap %u", errors, snap_id);
}

/* Recursively create parent directories for dest_path/rel_path. */
static void makedirs_for(const char *dest_path, const char *rel_path) {
    char buf[PATH_MAX];
    if (join_dest_path(buf, sizeof(buf), dest_path, rel_path) != 0) return;
    char *last = strrchr(buf, '/');
    if (!last) return;
    *last = '\0';
    for (char *p = buf + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(buf, 0755);
            *p = '/';
        }
    }
    mkdir(buf, 0755);
}

status_t restore_file(repo_t *repo, uint32_t snap_id,
                      const char *file_path, const char *dest_path) {
    char norm[PATH_MAX];
    if (normalize_snapshot_path(file_path, norm, sizeof(norm)) != 0)
        return set_error(ERR_INVALID, "restore_file: invalid path: %s", file_path);
    if (!path_is_safe(norm)) return set_error(ERR_INVALID, "restore_file: unsafe path: %s", norm);

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) { snapshot_free(snap); return st; }

    st = ERR_NOT_FOUND;
    for (uint32_t i = 0; i < sp.count; i++) {
        if (strcmp(sp.entries[i].path, norm) != 0) continue;
        if (!sp.entries[i].node) break;
        /* Directories are not "files" — signal caller to try restore_subtree */
        if (sp.entries[i].node->type == NODE_TYPE_DIR) { st = ERR_NOT_FOUND; break; }

        /* snapshot_t.nodes is node_t* but this node is not modified — cast is safe. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
        node_t *node_nc = (node_t *)sp.entries[i].node;
#pragma GCC diagnostic pop
        /* Create a temporary snapshot containing just this one node + dirent */
        snapshot_t single = {
            .snap_id         = snap_id,
            .node_count      = 1,
            .nodes           = node_nc,
            .dirent_data_len = sizeof(dirent_rec_t) + strlen(norm),
        };
        /* Build a minimal dirent blob: parent=0, node_id, name=path */
        size_t blobsz = sizeof(dirent_rec_t) + strlen(norm);
        single.dirent_data = malloc(blobsz);
        if (single.dirent_data) {
            dirent_rec_t dr = {
                .parent_node = 0,
                .node_id     = sp.entries[i].node_id,
                .name_len    = (uint16_t)strlen(norm),
            };
            uint8_t *dp = single.dirent_data;
            memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
            memcpy(dp, norm, strlen(norm));
            single.dirent_count = 1;
            /* Pre-create intermediate parent directories */
            makedirs_for(dest_path, norm);
            st = do_restore(repo, &single, dest_path);
            free(single.dirent_data);
        } else {
            st = set_error(ERR_NOMEM, "restore_file: alloc failed for single-file dirent blob");
        }
        break;
    }

    snap_paths_free(&sp);
    snapshot_free(snap);
    return st;
}

/* ------------------------------------------------------------------ */

status_t restore_subtree(repo_t *repo, uint32_t snap_id,
                         const char *subtree_path, const char *dest_path) {
    char norm[PATH_MAX];
    if (normalize_snapshot_path(subtree_path, norm, sizeof(norm)) != 0)
        return set_error(ERR_INVALID, "restore_subtree: invalid path: %s", subtree_path);
    if (!path_is_safe(norm)) return set_error(ERR_INVALID, "restore_subtree: unsafe path: %s", norm);

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) { snapshot_free(snap); return st; }

    size_t pfx_len = strlen(norm);

#define MATCHES(path) \
    (strcmp((path), norm) == 0 || \
     (strncmp((path), norm, pfx_len) == 0 && (path)[pfx_len] == '/'))

    /* Count matching entries and total dirent blob size */
    uint32_t nmatch = 0;
    size_t   dirent_sz = 0;
    for (uint32_t i = 0; i < sp.count; i++) {
        if (!MATCHES(sp.entries[i].path)) continue;
        nmatch++;
        dirent_sz += sizeof(dirent_rec_t) + strlen(sp.entries[i].path);
    }

    if (nmatch == 0) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return set_error(ERR_NOT_FOUND, "restore_subtree: no entries match '%s' in snap %u", norm, snap_id);
    }

    /* Collect unique nodes for matching entries */
    uint64_t *node_ids = malloc(nmatch * sizeof(uint64_t));
    node_t   *nodes    = malloc(nmatch * sizeof(node_t));
    uint8_t  *dirent_data = malloc(dirent_sz ? dirent_sz : 1);
    if (!node_ids || !nodes || !dirent_data) {
        free(node_ids); free(nodes); free(dirent_data);
        snap_paths_free(&sp); snapshot_free(snap);
        return set_error(ERR_NOMEM, "restore_subtree: alloc failed for subtree working set");
    }

    uint32_t n_nodes = 0;
    uint8_t *dp = dirent_data;
    uint32_t dcount = 0;

    for (uint32_t i = 0; i < sp.count; i++) {
        const char *p = sp.entries[i].path;
        if (!MATCHES(p)) continue;

        /* Collect unique node */
        uint64_t nid = sp.entries[i].node_id;
        int found = 0;
        for (uint32_t j = 0; j < n_nodes; j++)
            if (node_ids[j] == nid) { found = 1; break; }
        if (!found && sp.entries[i].node) {
            node_ids[n_nodes] = nid;
            nodes[n_nodes]    = *sp.entries[i].node;
            n_nodes++;
        }

        /* Write dirent: parent_node=0, name=full_path */
        uint16_t nlen = (uint16_t)strlen(p);
        dirent_rec_t dr = { .parent_node = 0, .node_id = nid, .name_len = nlen };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, p, nlen);         dp += nlen;
        dcount++;

        /* Pre-create intermediate parent directories */
        makedirs_for(dest_path, p);
    }
#undef MATCHES

    snapshot_t subset = {
        .snap_id         = snap_id,
        .node_count      = n_nodes,
        .nodes           = nodes,
        .dirent_count    = dcount,
        .dirent_data     = dirent_data,
        .dirent_data_len = dirent_sz,
    };

    st = do_restore(repo, &subset, dest_path);

    free(dirent_data);
    free(node_ids);
    free(nodes);
    snap_paths_free(&sp);
    snapshot_free(snap);
    return st;
}

status_t restore_cat_file_ex(repo_t *repo, uint32_t snap_id,
                             const char *file_path, int out_fd, int hex_mode) {
    char norm[PATH_MAX];
    if (normalize_snapshot_path(file_path, norm, sizeof(norm)) != 0)
        return set_error(ERR_INVALID, "restore_cat: invalid path: %s", file_path);
    if (!path_is_safe(norm)) return set_error(ERR_INVALID, "restore_cat: unsafe path: %s", norm);

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) {
        snapshot_free(snap);
        return st;
    }

    const node_t *target = NULL;
    for (uint32_t i = 0; i < sp.count; i++) {
        if (strcmp(sp.entries[i].path, norm) != 0) continue;
        target = sp.entries[i].node;
        break;
    }

    if (!target) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return set_error(ERR_NOT_FOUND, "restore_cat: '%s' not found in snap %u", norm, snap_id);
    }
    if (target->type != NODE_TYPE_REG) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return set_error(ERR_INVALID, "restore_cat: '%s' is not a regular file", norm);
    }
    if (hash_is_zero(target->content_hash)) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return OK;
    }

    void *obj = NULL;
    size_t obj_len = 0;
    uint8_t obj_type = 0;
    st = object_load(repo, target->content_hash, &obj, &obj_len, &obj_type);
    if (st == ERR_TOO_LARGE) {
        /* Large regular file: stream directly to the output fd. */
        st = object_load_stream(repo, target->content_hash, out_fd, NULL, NULL);
        snap_paths_free(&sp);
        snapshot_free(snap);
        return st;
    }
    if (st != OK) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return st;
    }

    if (obj_type == OBJECT_TYPE_SPARSE && obj_len >= sizeof(sparse_hdr_t)) {
        sparse_hdr_t shdr;
        memcpy(&shdr, obj, sizeof(shdr));
        if (shdr.magic != SPARSE_MAGIC ||
            obj_len < sizeof(sparse_hdr_t) + shdr.region_count * sizeof(sparse_region_t)) {
            free(obj);
            snap_paths_free(&sp);
            snapshot_free(snap);
            return set_error(ERR_CORRUPT, "restore_cat: invalid sparse header for '%s'", norm);
        }

        const sparse_region_t *rgns = (const sparse_region_t *)((uint8_t *)obj + sizeof(shdr));
        const uint8_t *rdata = (const uint8_t *)obj + sizeof(shdr) +
                               shdr.region_count * sizeof(sparse_region_t);
        size_t remain = obj_len - (sizeof(shdr) + shdr.region_count * sizeof(sparse_region_t));
        uint64_t pos = 0;
        size_t hex_off = 0;
        static const uint8_t zbuf[65536];

        for (uint32_t i = 0; i < shdr.region_count; i++) {
            if (rgns[i].offset < pos) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error(ERR_CORRUPT, "restore_cat: sparse region %u out of order for '%s'", i, norm);
            }
            uint64_t gap = rgns[i].offset - pos;
            while (gap > 0) {
                size_t chunk = (size_t)(gap > sizeof(zbuf) ? sizeof(zbuf) : gap);
                if (hex_mode) {
                    if (write_hex_fd(out_fd, zbuf, chunk, &hex_off) != 0) {
                        free(obj);
                        snap_paths_free(&sp);
                        snapshot_free(snap);
                        return set_error_errno(ERR_IO, "restore_cat: write sparse gap for '%s'", norm);
                    }
                } else if (write_all_fd(out_fd, zbuf, chunk) != 0) {
                    free(obj);
                    snap_paths_free(&sp);
                    snapshot_free(snap);
                    return set_error_errno(ERR_IO, "restore_cat: write sparse gap for '%s'", norm);
                }
                gap -= chunk;
                pos += chunk;
            }

            if (rgns[i].length > remain) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error(ERR_CORRUPT, "restore_cat: sparse region %u data truncated for '%s'", i, norm);
            }
            if (hex_mode) {
                if (write_hex_fd(out_fd, rdata, (size_t)rgns[i].length, &hex_off) != 0) {
                    free(obj);
                    snap_paths_free(&sp);
                    snapshot_free(snap);
                    return set_error_errno(ERR_IO, "restore_cat: write sparse region %u for '%s'", i, norm);
                }
            } else if (write_all_fd(out_fd, rdata, (size_t)rgns[i].length) != 0) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error_errno(ERR_IO, "restore_cat: write sparse region %u for '%s'", i, norm);
            }
            rdata += rgns[i].length;
            remain -= (size_t)rgns[i].length;
            pos += rgns[i].length;
        }

        if (target->size < pos) {
            free(obj);
            snap_paths_free(&sp);
            snapshot_free(snap);
            return set_error(ERR_CORRUPT, "restore_cat: sparse regions exceed file size for '%s'", norm);
        }
        uint64_t tail = target->size - pos;
        while (tail > 0) {
            size_t chunk = (size_t)(tail > sizeof(zbuf) ? sizeof(zbuf) : tail);
            if (hex_mode) {
                if (write_hex_fd(out_fd, zbuf, chunk, &hex_off) != 0) {
                    free(obj);
                    snap_paths_free(&sp);
                    snapshot_free(snap);
                    return set_error_errno(ERR_IO, "restore_cat: write trailing zeros for '%s'", norm);
                }
            } else if (write_all_fd(out_fd, zbuf, chunk) != 0) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error_errno(ERR_IO, "restore_cat: write trailing zeros for '%s'", norm);
            }
            tail -= chunk;
        }
    } else {
        if (hex_mode) {
            if (write_hex_fd(out_fd, obj, obj_len, NULL) != 0) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error_errno(ERR_IO, "restore_cat: write output for '%s'", norm);
            }
        } else if (write_all_fd(out_fd, obj, obj_len) != 0) {
            free(obj);
            snap_paths_free(&sp);
            snapshot_free(snap);
            return set_error_errno(ERR_IO, "restore_cat: write output for '%s'", norm);
        }
    }

    free(obj);
    snap_paths_free(&sp);
    snapshot_free(snap);
    return OK;
}

status_t restore_cat_file(repo_t *repo, uint32_t snap_id,
                          const char *file_path) {
    return restore_cat_file_ex(repo, snap_id, file_path, STDOUT_FILENO, 0);
}
