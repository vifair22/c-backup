#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "restore.h"
#include "snapshot.h"
#include "reverse.h"
#include "object.h"
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
#include <openssl/sha.h>

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
    if (!flat) return ERR_NOMEM;
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
    return ERR_NOMEM;
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

static void apply_metadata(const char *full, const node_t *nd,
                           repo_t *repo, int is_symlink) {
    (void)lchown(full, (uid_t)nd->uid, (gid_t)nd->gid);
    if (!is_symlink) (void)chmod(full, (mode_t)nd->mode);

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
                lsetxattr(full, xname, pp, vlen, 0);
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
            if (acl) { acl_set_file(full, ACL_TYPE_ACCESS, acl); acl_free(acl); }
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
}

/* ------------------------------------------------------------------ */
/* Core restore                                                        */
/* ------------------------------------------------------------------ */

static status_t do_restore(repo_t *repo, const snapshot_t *snap,
                           const char *dest_path) {
    snap_paths_t sp = {0};
    status_t st = snap_paths_build(snap, &sp);
    if (st != OK) return st;

    if (mkdir(dest_path, 0755) == -1 && errno != EEXIST) {
        snap_paths_free(&sp); return ERR_IO;
    }

    /* ---- Pass 1: directories — multi-pass so parents always exist first ---- */
    {
        int progress = 1;
        while (progress) {
            progress = 0;
            for (uint32_t i = 0; i < sp.count; i++) {
                const snap_pe_t *pe = &sp.entries[i];
                if (!pe->node || pe->node->type != NODE_TYPE_DIR) continue;
                if (!path_is_safe(pe->path)) {
                    log_msg("ERROR", "unsafe path in snapshot - skipping");
                    continue;
                }
                char full[PATH_MAX];
                snprintf(full, sizeof(full), "%s/%s", dest_path, pe->path);
                if (mkdir(full, 0700) == 0) progress = 1;
                else if (errno != EEXIST && errno != ENOENT) {
                    st = ERR_IO; goto done;
                }
            }
        }
    }

    /* ---- Pass 2: regular files, symlinks, special files ---- */
    /* Track node_id -> first created absolute path for hard link detection */
    typedef struct { uint64_t node_id; char path[PATH_MAX]; } nl_entry_t;
    nl_entry_t *nl_map = calloc(sp.count, sizeof(nl_entry_t));
    uint32_t    nl_cnt  = 0;
    if (!nl_map) { st = ERR_NOMEM; goto done; }
    uint32_t restore_files = 0;
    uint64_t restore_bytes = 0;

    for (uint32_t i = 0; i < sp.count; i++) {
        const snap_pe_t *pe = &sp.entries[i];
        if (!pe->node || pe->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(pe->path)) continue;

        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, pe->path);
        const node_t *nd = pe->node;

        /* Hard link: same node_id seen before — use link() */
        const char *hl_src = NULL;
        for (uint32_t k = 0; k < nl_cnt; k++) {
            if (nl_map[k].node_id == pe->node_id) { hl_src = nl_map[k].path; break; }
        }
        if (hl_src) {
            if (link(hl_src, full) == -1 && errno != EEXIST) {
                log_msg("WARN", "hard link failed; falling through to copy");
                /* fall through to normal creation below */
            } else {
                /* record and move on */
                nl_map[nl_cnt].node_id = pe->node_id;
                snprintf(nl_map[nl_cnt].path, PATH_MAX, "%s", full);
                nl_cnt++;
                continue;
            }
        }

        switch (nd->type) {
        case NODE_TYPE_REG: {
            int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (fd == -1) { free(nl_map); st = ERR_IO; goto done; }
            if (!hash_is_zero(nd->content_hash)) {
                void *data = NULL; size_t len = 0;
                uint8_t obj_type = 0;
                if (object_load(repo, nd->content_hash, &data, &len, &obj_type) != OK) {
                    close(fd); free(nl_map); st = ERR_IO; goto done;
                }
                if (obj_type == OBJECT_TYPE_SPARSE &&
                    len >= sizeof(sparse_hdr_t)) {
                    /* sparse payload: [sparse_hdr][regions][data bytes] */
                    const uint8_t *sp_p = (const uint8_t *)data;
                    sparse_hdr_t shdr;
                    memcpy(&shdr, sp_p, sizeof(shdr));
                    sp_p += sizeof(shdr);
                    if (shdr.magic == SPARSE_MAGIC &&
                        len >= sizeof(sparse_hdr_t) +
                               shdr.region_count * sizeof(sparse_region_t)) {
                        /* ftruncate creates the holes */
                        (void)ftruncate(fd, (off_t)nd->size);
                        const sparse_region_t *rgns =
                            (const sparse_region_t *)sp_p;
                        sp_p += shdr.region_count * sizeof(sparse_region_t);
                        const uint8_t *dptr = sp_p;
                        for (uint32_t r = 0; r < shdr.region_count; r++) {
                            lseek(fd, (off_t)rgns[r].offset, SEEK_SET);
                            size_t rem = (size_t)rgns[r].length;
                            while (rem > 0) {
                                ssize_t w = write(fd, dptr, rem);
                                if (w <= 0) break;
                                dptr += w; rem -= (size_t)w;
                            }
                        }
                    }
                } else {
                    if (len > 0 && write(fd, data, len) != (ssize_t)len) {
                        free(data); close(fd); free(nl_map); st = ERR_IO; goto done;
                    }
                }
                free(data);
            }
            if (ftruncate(fd, (off_t)nd->size) == -1 && nd->size > 0) {
                close(fd); free(nl_map); st = ERR_IO; goto done;
            }
            fsync(fd);
            close(fd);
            restore_files++;
            restore_bytes += nd->size;
            /* record this node_id -> path */
            nl_map[nl_cnt].node_id = pe->node_id;
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
                free(tdata); free(nl_map); st = ERR_IO; goto done;
            }
            free(tdata);
            restore_files++;
            break;
        }
        case NODE_TYPE_FIFO:
            if (mkfifo(full, (mode_t)nd->mode) == -1 && errno != EEXIST) {
                free(nl_map); st = ERR_IO; goto done;
            }
            restore_files++;
            break;
        case NODE_TYPE_CHR:
        case NODE_TYPE_BLK: {
            dev_t dev = makedev(nd->device.major, nd->device.minor);
            mode_t m  = (nd->type == NODE_TYPE_CHR ? S_IFCHR : S_IFBLK) | (mode_t)nd->mode;
            if (mknod(full, m, dev) == -1 && errno != EEXIST) {
                free(nl_map); st = ERR_IO; goto done;
            }
            restore_files++;
            break;
        }
        default: break;
        }
    }
    free(nl_map);
    {
        char sz[32];
        restore_fmt_bytes(restore_bytes, sz, sizeof(sz));
        fprintf(stderr, "restored: %u file(s)  %s\n", restore_files, sz);
    }

    /* ---- Pass 3: metadata (ownership, perms, xattrs, ACLs, timestamps)
     *              Apply to non-dirs first, then dirs in reverse order
     *              so parent dir timestamps are set after their children.  ---- */
    for (uint32_t i = 0; i < sp.count; i++) {
        const snap_pe_t *pe = &sp.entries[i];
        if (!pe->node || pe->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(pe->path)) continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, pe->path);
        apply_metadata(full, pe->node, repo, pe->node->type == NODE_TYPE_SYMLINK);
    }
    /* dirs in reverse DFS order (leaves before roots) */
    for (uint32_t i = sp.count; i-- > 0;) {
        const snap_pe_t *pe = &sp.entries[i];
        if (!pe->node || pe->node->type != NODE_TYPE_DIR) continue;
        if (!path_is_safe(pe->path)) continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, pe->path);
        apply_metadata(full, pe->node, repo, 0);
    }
    /* set metadata on dest_path root if the snapshot has a root-level dir node */
    for (uint32_t i = 0; i < sp.count; i++) {
        const snap_pe_t *pe = &sp.entries[i];
        if (pe->node && pe->node->type == NODE_TYPE_DIR &&
            strchr(pe->path, '/') == NULL) {
            /* top-level entry — apply metadata to dest_path */
            apply_metadata(dest_path, pe->node, repo, 0);
            break;
        }
    }

done:
    snap_paths_free(&sp);
    return st;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

status_t restore_latest(repo_t *repo, const char *dest_path) {
    uint32_t head_id = 0;
    if (snapshot_read_head(repo, &head_id) != OK || head_id == 0) {
        log_msg("ERROR", "no snapshots in repository");
        return ERR_IO;
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

/*
 * Historical restore: reconstruct a past snapshot by walking backwards
 * through reverse records from HEAD down to target+1, then restore the
 * resulting state.
 *
 * Each reverse record for snapshot N describes how to undo N:
 *   REV_OP_REMOVE  — path was CREATED in N     → delete from working set
 *   REV_OP_RESTORE — path was MODIFIED in N    → replace with prev_node
 *   REV_OP_META    — metadata changed in N      → replace metadata fields
 */

static int ws_find(ws_entry_t *ws, uint32_t n, const char *path) {
    for (uint32_t i = 0; i < n; i++)
        if (ws[i].path && strcmp(ws[i].path, path) == 0) return (int)i;
    return -1;
}

/* Restore from a flat working-set rather than a snapshot_t.
 * We need to derive a path → node mapping and restore in the same
 * three-pass order (dirs → files → metadata in reverse).              */
static status_t do_restore_ws(repo_t *repo,
                               ws_entry_t *ws, uint32_t ws_cnt,
                               const char *dest_path) {
    if (mkdir(dest_path, 0755) == -1 && errno != EEXIST)
        return ERR_IO;

    status_t st = OK;

    /* Pass 1: directories (sort by path depth so parents come first) */
    /* Simple approach: iterate repeatedly until no new dirs created.
     * For typical tree depths this converges in O(depth) passes.      */
    int progress = 1;
    while (progress) {
        progress = 0;
        for (uint32_t i = 0; i < ws_cnt; i++) {
            if (!ws[i].path || ws[i].node.type != NODE_TYPE_DIR) continue;
            if (!path_is_safe(ws[i].path)) continue;
            char full[PATH_MAX];
            snprintf(full, sizeof(full), "%s/%s", dest_path, ws[i].path);
            if (mkdir(full, 0700) == 0) progress = 1;
            else if (errno != EEXIST && errno != ENOENT) { st = ERR_IO; return st; }
        }
    }

    /* Pass 2: non-directory files.  Reuse snap_pe_t logic via inline. */
    typedef struct { uint64_t node_id; char path[PATH_MAX]; } nl_t;
    nl_t *nl_map = calloc(ws_cnt, sizeof(nl_t));
    uint32_t nl_cnt = 0;
    if (!nl_map) return ERR_NOMEM;
    uint32_t ws_restore_files = 0;
    uint64_t ws_restore_bytes = 0;

    for (uint32_t i = 0; i < ws_cnt; i++) {
        if (!ws[i].path || ws[i].node.type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(ws[i].path)) continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, ws[i].path);
        const node_t *nd = &ws[i].node;

        /* Hard link? */
        const char *hl_src = NULL;
        for (uint32_t k = 0; k < nl_cnt; k++) {
            if (nl_map[k].node_id == nd->node_id) { hl_src = nl_map[k].path; break; }
        }
        if (hl_src) {
            if (link(hl_src, full) == -1 && errno != EEXIST)
                log_msg("WARN", "hard link failed in historical restore");
            else {
                nl_map[nl_cnt].node_id = nd->node_id;
                snprintf(nl_map[nl_cnt].path, PATH_MAX, "%s", full);
                nl_cnt++;
            }
            continue;
        }

        switch (nd->type) {
        case NODE_TYPE_REG: {
            int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (fd == -1) { free(nl_map); return ERR_IO; }
            if (!hash_is_zero(nd->content_hash)) {
                void *data = NULL; size_t len = 0; uint8_t obj_type = 0;
                if (object_load(repo, nd->content_hash, &data, &len, &obj_type) != OK) {
                    close(fd); free(nl_map); return ERR_IO;
                }
                if (obj_type == OBJECT_TYPE_SPARSE && len >= sizeof(sparse_hdr_t)) {
                    const uint8_t *sp_p = (const uint8_t *)data;
                    sparse_hdr_t shdr; memcpy(&shdr, sp_p, sizeof(shdr)); sp_p += sizeof(shdr);
                    if (shdr.magic == SPARSE_MAGIC &&
                        len >= sizeof(sparse_hdr_t) + shdr.region_count * sizeof(sparse_region_t)) {
                        (void)ftruncate(fd, (off_t)nd->size);
                        const sparse_region_t *rgns = (const sparse_region_t *)sp_p;
                        sp_p += shdr.region_count * sizeof(sparse_region_t);
                        const uint8_t *dptr = sp_p;
                        for (uint32_t r = 0; r < shdr.region_count; r++) {
                            lseek(fd, (off_t)rgns[r].offset, SEEK_SET);
                            size_t rem = (size_t)rgns[r].length;
                            while (rem > 0) {
                                ssize_t w = write(fd, dptr, rem);
                                if (w <= 0) break;
                                dptr += w; rem -= (size_t)w;
                            }
                        }
                    }
                } else {
                    if (len > 0) (void)write(fd, data, len);
                }
                free(data);
            }
            if (nd->size > 0) (void)ftruncate(fd, (off_t)nd->size);
            fsync(fd); close(fd);
            ws_restore_files++;
            ws_restore_bytes += nd->size;
            nl_map[nl_cnt].node_id = nd->node_id;
            snprintf(nl_map[nl_cnt].path, PATH_MAX, "%s", full); nl_cnt++;
            break;
        }
        case NODE_TYPE_SYMLINK: {
            if (hash_is_zero(nd->content_hash)) break;
            void *tdata = NULL; size_t tlen = 0;
            if (object_load(repo, nd->content_hash, &tdata, &tlen, NULL) != OK) break;
            unlink(full);
            (void)symlink((char *)tdata, full);
            free(tdata); break;
        }
        case NODE_TYPE_FIFO:
            (void)mkfifo(full, (mode_t)nd->mode); break;
        case NODE_TYPE_CHR:
        case NODE_TYPE_BLK: {
            dev_t dev = makedev(nd->device.major, nd->device.minor);
            mode_t m = (nd->type == NODE_TYPE_CHR ? S_IFCHR : S_IFBLK) | (mode_t)nd->mode;
            (void)mknod(full, m, dev); break;
        }
        default: break;
        }
    }
    free(nl_map);
    {
        char sz[32];
        restore_fmt_bytes(ws_restore_bytes, sz, sizeof(sz));
        fprintf(stderr, "restored: %u file(s)  %s\n", ws_restore_files, sz);
    }

    /* Pass 3: metadata */
    for (uint32_t i = 0; i < ws_cnt; i++) {
        if (!ws[i].path || ws[i].node.type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(ws[i].path)) continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, ws[i].path);
        apply_metadata(full, &ws[i].node, repo, ws[i].node.type == NODE_TYPE_SYMLINK);
    }
    /* dirs in reverse order */
    for (uint32_t i = ws_cnt; i-- > 0;) {
        if (!ws[i].path || ws[i].node.type != NODE_TYPE_DIR) continue;
        if (!path_is_safe(ws[i].path)) continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, ws[i].path);
        apply_metadata(full, &ws[i].node, repo, 0);
    }

    return st;
}

/*
 * Build the working set for target_id.
 * Finds the nearest existing snapshot anchor > target_id to minimize
 * the reverse chain walk, falls back to HEAD if none closer exists.
 */
status_t restore_build_ws(repo_t *repo, uint32_t target_id,
                           ws_entry_t **out_ws, uint32_t *out_cnt,
                           uint32_t *out_anchor_id) {
    uint32_t head_id = 0;
    if (snapshot_read_head(repo, &head_id) != OK || head_id == 0) {
        log_msg("ERROR", "cannot read HEAD"); return ERR_IO;
    }
    if (target_id >= head_id) {
        log_msg("ERROR", "target snapshot not found or is HEAD"); return ERR_INVALID;
    }

    /* Find nearest existing .snap anchor > target_id (minimises chain walk) */
    uint32_t anchor_id = head_id;
    for (uint32_t id = target_id + 1; id < head_id; id++) {
        snapshot_t *probe = NULL;
        if (snapshot_load(repo, id, &probe) == OK) {
            snapshot_free(probe);
            anchor_id = id;
            break;
        }
    }

    /* Load anchor snapshot and convert to mutable working set */
    snapshot_t *anchor_snap = NULL;
    if (snapshot_load(repo, anchor_id, &anchor_snap) != OK) {
        log_msg("ERROR", "cannot load anchor snapshot"); return ERR_IO;
    }

    pathmap_t *pm = NULL;
    status_t st = pathmap_build(anchor_snap, &pm);
    snapshot_free(anchor_snap);
    if (st != OK) return st;

    uint32_t ws_cap = (uint32_t)(pm->capacity);
    ws_entry_t *ws = calloc(ws_cap, sizeof(ws_entry_t));
    if (!ws) { pathmap_free(pm); return ERR_NOMEM; }
    uint32_t ws_cnt = 0;
    for (size_t i = 0; i < pm->capacity; i++) {
        if (!pm->slots[i].key) continue;
        ws[ws_cnt].path = strdup(pm->slots[i].key);
        if (!ws[ws_cnt].path) {
            for (uint32_t k = 0; k < ws_cnt; k++) free(ws[k].path);
            free(ws); pathmap_free(pm); return ERR_NOMEM;
        }
        ws[ws_cnt].node = pm->slots[i].value;
        ws_cnt++;
    }
    pathmap_free(pm);

    /* Apply reverse records from anchor_id down to target_id+1 */
    for (uint32_t id = anchor_id; id > target_id; id--) {
        rev_record_t *rev = NULL;
        if (reverse_load(repo, id, &rev) != OK) continue;

        for (uint32_t j = 0; j < rev->entry_count; j++) {
            const rev_entry_t *re = &rev->entries[j];
            int idx = ws_find(ws, ws_cnt, re->path);

            switch (re->op_type) {
            case REV_OP_REMOVE:
                if (idx >= 0) { free(ws[idx].path); ws[idx].path = NULL; }
                break;
            case REV_OP_RESTORE:
                if (idx >= 0) {
                    ws[idx].node = re->prev_node;
                } else {
                    if (ws_cnt == ws_cap) {
                        uint32_t nc = ws_cap * 2;
                        ws_entry_t *tmp = realloc(ws, nc * sizeof(*ws));
                        if (!tmp) { reverse_free(rev); st = ERR_NOMEM; goto fail; }
                        ws = tmp; ws_cap = nc;
                    }
                    ws[ws_cnt].path = strdup(re->path);
                    if (!ws[ws_cnt].path) { reverse_free(rev); st = ERR_NOMEM; goto fail; }
                    ws[ws_cnt].node = re->prev_node;
                    ws_cnt++;
                }
                break;
            case REV_OP_META:
                if (idx >= 0) {
                    ws[idx].node.mode       = re->prev_node.mode;
                    ws[idx].node.uid        = re->prev_node.uid;
                    ws[idx].node.gid        = re->prev_node.gid;
                    ws[idx].node.mtime_sec  = re->prev_node.mtime_sec;
                    ws[idx].node.mtime_nsec = re->prev_node.mtime_nsec;
                    memcpy(ws[idx].node.xattr_hash, re->prev_node.xattr_hash, OBJECT_HASH_SIZE);
                    memcpy(ws[idx].node.acl_hash,   re->prev_node.acl_hash,   OBJECT_HASH_SIZE);
                }
                break;
            default: break;
            }
        }
        reverse_free(rev);
    }

    *out_ws       = ws;
    *out_cnt      = ws_cnt;
    *out_anchor_id = anchor_id;
    return OK;

fail:
    for (uint32_t i = 0; i < ws_cnt; i++) free(ws[i].path);
    free(ws);
    return st;
}

status_t restore_snapshot_at(repo_t *repo, uint32_t target_id, const char *dest_path) {
    /* Fast path: target snapshot file still exists */
    snapshot_t *snap = NULL;
    if (snapshot_load(repo, target_id, &snap) == OK) {
        status_t st = do_restore(repo, snap, dest_path);
        snapshot_free(snap);
        return st;
    }

    /* Slow path: build working set via reverse chain */
    ws_entry_t *ws = NULL;
    uint32_t ws_cnt = 0, anchor_id = 0;
    status_t st = restore_build_ws(repo, target_id, &ws, &ws_cnt, &anchor_id);
    if (st != OK) return st;

    fprintf(stderr, "restoring snapshot %u (via reverse chain from %u) -> %s\n",
            target_id, anchor_id, dest_path);

    st = do_restore_ws(repo, ws, ws_cnt, dest_path);

    for (uint32_t i = 0; i < ws_cnt; i++) free(ws[i].path);
    free(ws);
    return st;
}

status_t restore_verify_dest(repo_t *repo, uint32_t snap_id,
                              const char *dest_path) {
    /* Load or reconstruct working set for this snapshot */
    ws_entry_t *ws  = NULL;
    uint32_t    cnt = 0;

    snapshot_t *snap = NULL;
    if (snapshot_load(repo, snap_id, &snap) == OK) {
        pathmap_t *pm = NULL;
        if (pathmap_build(snap, &pm) != OK) { snapshot_free(snap); return ERR_IO; }
        snapshot_free(snap);
        ws = calloc(pm->capacity, sizeof(ws_entry_t));
        if (!ws) { pathmap_free(pm); return ERR_NOMEM; }
        for (size_t i = 0; i < pm->capacity; i++) {
            if (!pm->slots[i].key) continue;
            ws[cnt].path = strdup(pm->slots[i].key);
            ws[cnt].node = pm->slots[i].value;
            cnt++;
        }
        pathmap_free(pm);
    } else {
        uint32_t anchor = 0;
        status_t st = restore_build_ws(repo, snap_id, &ws, &cnt, &anchor);
        if (st != OK) return st;
    }

    int errors = 0;
    uint8_t read_hash[OBJECT_HASH_SIZE];
    uint8_t expected_hash[OBJECT_HASH_SIZE];
    static const uint8_t zero_buf[65536];   /* BSS zeroes, no stack cost */

    for (uint32_t i = 0; i < cnt; i++) {
        if (!ws[i].path) continue;
        if (ws[i].node.type != NODE_TYPE_REG) continue;
        if (hash_is_zero(ws[i].node.content_hash)) continue;

        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, ws[i].path);

        /* Hash the restored file on disk */
        int fd = open(full, O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "verify: cannot open %s\n", full);
            errors++; continue;
        }
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        uint8_t buf[65536];
        ssize_t nr;
        while ((nr = read(fd, buf, sizeof(buf))) > 0)
            SHA256_Update(&ctx, buf, (size_t)nr);
        close(fd);
        SHA256_Final(read_hash, &ctx);

        /* Compute the expected hash from the stored object.
         * For OBJECT_TYPE_FILE the object payload IS the raw bytes, so
         * SHA256(object_data) == content_hash and we can shortcut.
         * For OBJECT_TYPE_SPARSE the content_hash covers the sparse
         * payload (header+regions+data), not the expanded file bytes,
         * so we reconstruct the expected hash by streaming through
         * the regions and filling gaps with zeros. */
        void *obj_data = NULL; size_t obj_len = 0; uint8_t obj_type = 0;
        if (object_load(repo, ws[i].node.content_hash,
                        &obj_data, &obj_len, &obj_type) != OK) {
            fprintf(stderr, "verify: cannot load object for %s\n", ws[i].path);
            errors++; continue;
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

                SHA256_CTX ectx;
                SHA256_Init(&ectx);
                uint64_t pos = 0;
                for (uint32_t r = 0; r < shdr.region_count; r++) {
                    /* zeros covering the hole before this region */
                    uint64_t gap = rgns[r].offset - pos;
                    while (gap > 0) {
                        uint64_t chunk = gap < sizeof(zero_buf)
                                         ? gap : sizeof(zero_buf);
                        SHA256_Update(&ectx, zero_buf, (size_t)chunk);
                        gap -= chunk; pos += chunk;
                    }
                    SHA256_Update(&ectx, rdata, (size_t)rgns[r].length);
                    rdata += rgns[r].length;
                    pos   += rgns[r].length;
                }
                /* trailing zeros to reach file size */
                uint64_t tail = ws[i].node.size - pos;
                while (tail > 0) {
                    uint64_t chunk = tail < sizeof(zero_buf)
                                     ? tail : sizeof(zero_buf);
                    SHA256_Update(&ectx, zero_buf, (size_t)chunk);
                    tail -= chunk;
                }
                SHA256_Final(expected_hash, &ectx);
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
    return ERR_CORRUPT;
}

/* Recursively create parent directories for dest_path/rel_path. */
static void makedirs_for(const char *dest_path, const char *rel_path) {
    char buf[PATH_MAX];
    snprintf(buf, sizeof(buf), "%s/%s", dest_path, rel_path);
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
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) { snapshot_free(snap); return st; }

    st = ERR_INVALID;
    for (uint32_t i = 0; i < sp.count; i++) {
        if (strcmp(sp.entries[i].path, file_path) != 0) continue;
        if (!sp.entries[i].node) break;
        /* Directories are not "files" — signal caller to try restore_subtree */
        if (sp.entries[i].node->type == NODE_TYPE_DIR) { st = ERR_NOT_FOUND; break; }
        if (!path_is_safe(file_path)) { st = ERR_INVALID; break; }

        /* Create a temporary snapshot containing just this one node + dirent */
        snapshot_t single = {
            .snap_id         = snap_id,
            .node_count      = 1,
            .nodes           = (node_t *)sp.entries[i].node,
            .dirent_data_len = sizeof(dirent_rec_t) + strlen(file_path),
        };
        /* Build a minimal dirent blob: parent=0, node_id, name=file_path */
        size_t blobsz = sizeof(dirent_rec_t) + strlen(file_path);
        single.dirent_data = malloc(blobsz);
        if (single.dirent_data) {
            dirent_rec_t dr = {
                .parent_node = 0,
                .node_id     = sp.entries[i].node_id,
                .name_len    = (uint16_t)strlen(file_path),
            };
            uint8_t *dp = single.dirent_data;
            memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
            memcpy(dp, file_path, strlen(file_path));
            single.dirent_count = 1;
            /* Pre-create intermediate parent directories */
            makedirs_for(dest_path, file_path);
            st = do_restore(repo, &single, dest_path);
            free(single.dirent_data);
        } else {
            st = ERR_NOMEM;
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
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) { snapshot_free(snap); return st; }

    size_t pfx_len = strlen(subtree_path);

#define MATCHES(path) \
    (strcmp((path), subtree_path) == 0 || \
     (strncmp((path), subtree_path, pfx_len) == 0 && (path)[pfx_len] == '/'))

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
        return ERR_NOT_FOUND;
    }

    /* Collect unique nodes for matching entries */
    uint64_t *node_ids = malloc(nmatch * sizeof(uint64_t));
    node_t   *nodes    = malloc(nmatch * sizeof(node_t));
    uint8_t  *dirent_data = malloc(dirent_sz ? dirent_sz : 1);
    if (!node_ids || !nodes || !dirent_data) {
        free(node_ids); free(nodes); free(dirent_data);
        snap_paths_free(&sp); snapshot_free(snap);
        return ERR_NOMEM;
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

    snapshot_t synth = {
        .snap_id         = snap_id,
        .node_count      = n_nodes,
        .nodes           = nodes,
        .dirent_count    = dcount,
        .dirent_data     = dirent_data,
        .dirent_data_len = dirent_sz,
    };

    st = do_restore(repo, &synth, dest_path);

    free(dirent_data);
    free(node_ids);
    free(nodes);
    snap_paths_free(&sp);
    snapshot_free(snap);
    return st;
}
