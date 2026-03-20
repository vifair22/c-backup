#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "restore.h"
#include "snapshot.h"
#include "reverse.h"
#include "object.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>
#include <acl/libacl.h>
#include <sys/xattr.h>

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
        if (object_load(repo, nd->xattr_hash, &xd, &xl) == OK) {
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
        if (object_load(repo, nd->acl_hash, &ad, &al) == OK) {
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

    /* ---- Pass 1: directories (DFS order = parents before children) ---- */
    for (uint32_t i = 0; i < sp.count; i++) {
        const snap_pe_t *pe = &sp.entries[i];
        if (!pe->node || pe->node->type != NODE_TYPE_DIR) continue;
        if (!path_is_safe(pe->path)) {
            log_msg("ERROR", "unsafe path in snapshot - skipping");
            continue;
        }
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, pe->path);
        if (mkdir(full, 0700) == -1 && errno != EEXIST) {
            st = ERR_IO; goto done;
        }
    }

    /* ---- Pass 2: regular files, symlinks, special files ---- */
    for (uint32_t i = 0; i < sp.count; i++) {
        const snap_pe_t *pe = &sp.entries[i];
        if (!pe->node || pe->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(pe->path)) continue;

        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", dest_path, pe->path);
        const node_t *nd = pe->node;

        switch (nd->type) {
        case NODE_TYPE_REG: {
            int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (fd == -1) { st = ERR_IO; goto done; }
            if (!hash_is_zero(nd->content_hash)) {
                void *data = NULL; size_t len = 0;
                if (object_load(repo, nd->content_hash, &data, &len) != OK) {
                    close(fd); st = ERR_IO; goto done;
                }
                if (len > 0 && write(fd, data, len) != (ssize_t)len) {
                    free(data); close(fd); st = ERR_IO; goto done;
                }
                free(data);
            }
            if (ftruncate(fd, (off_t)nd->size) == -1) {
                close(fd); st = ERR_IO; goto done;
            }
            fsync(fd);
            close(fd);
            break;
        }
        case NODE_TYPE_SYMLINK: {
            if (hash_is_zero(nd->content_hash)) break;
            void *tdata = NULL; size_t tlen = 0;
            if (object_load(repo, nd->content_hash, &tdata, &tlen) != OK) break;
            /* target is stored as a NUL-terminated string */
            unlink(full);   /* remove stale if any */
            if (symlink((char *)tdata, full) == -1 && errno != EEXIST) {
                free(tdata); st = ERR_IO; goto done;
            }
            free(tdata);
            break;
        }
        case NODE_TYPE_FIFO:
            if (mkfifo(full, (mode_t)nd->mode) == -1 && errno != EEXIST) {
                st = ERR_IO; goto done;
            }
            break;
        case NODE_TYPE_CHR:
        case NODE_TYPE_BLK: {
            dev_t dev = makedev(nd->device.major, nd->device.minor);
            mode_t m  = (nd->type == NODE_TYPE_CHR ? S_IFCHR : S_IFBLK) | (mode_t)nd->mode;
            if (mknod(full, m, dev) == -1 && errno != EEXIST) {
                st = ERR_IO; goto done;
            }
            break;
        }
        default: break;
        }
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

    char msg[64];
    snprintf(msg, sizeof(msg), "restoring snapshot %u", snap_id);
    log_msg("INFO", msg);

    st = do_restore(repo, snap, dest_path);
    snapshot_free(snap);
    return st;
}

/*
 * Historical restore: apply reverse records backward from HEAD to target.
 *
 * We walk backward: load rev record HEAD, HEAD-1, ... target+1 and
 * build a merged "what did the tree look like at target?" by starting
 * with the HEAD snapshot and applying undo operations.
 *
 * For MVP we load the target snapshot directly (reverse records are stored
 * but the walk is not yet implemented).  Full reverse-chain application
 * is a follow-up.
 */
status_t restore_snapshot_at(repo_t *repo, uint32_t target_id, const char *dest_path) {
    /* If the target snapshot file still exists, restore from it directly. */
    snapshot_t *snap = NULL;
    if (snapshot_load(repo, target_id, &snap) == OK) {
        status_t st = do_restore(repo, snap, dest_path);
        snapshot_free(snap);
        return st;
    }

    log_msg("ERROR", "historical restore via reverse chain not yet implemented");
    return ERR_INVALID;
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
