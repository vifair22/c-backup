#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "restore.h"
#include "snapshot.h"
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

/* Helper – zero hash check */
static int hash_is_zero_local(const uint8_t h[OBJECT_HASH_SIZE]) {
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

/* Validate that path does not escape dest_root (path traversal guard). */
static int path_is_safe(const char *dest_root, const char *path) {
    if (strstr(path, "..")) return 0;
    if (path[0] == '/') return 0;
    (void)dest_root;
    return 1;
}

/* Restore one node's content + metadata into dest_root/rel_path. */
static status_t restore_node(repo_t *repo, const node_t *nd,
                              const char *rel_path, const char *dest_root) {
    if (!path_is_safe(dest_root, rel_path)) {
        log_msg("ERROR", "unsafe path in snapshot - rejecting");
        return ERR_INVALID;
    }

    char full[PATH_MAX];
    snprintf(full, sizeof(full), "%s/%s", dest_root, rel_path);

    switch (nd->type) {
    case NODE_TYPE_DIR:
        if (mkdir(full, (mode_t)nd->mode) == -1 && errno != EEXIST) return ERR_IO;
        break;

    case NODE_TYPE_REG: {
        void *data = NULL; size_t len = 0;
        if (!hash_is_zero_local(nd->content_hash) &&
            object_load(repo, nd->content_hash, &data, &len) != OK) return ERR_IO;
        int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, (mode_t)nd->mode);
        if (fd == -1) { free(data); return ERR_IO; }
        if (data && len > 0) {
            if (write(fd, data, len) != (ssize_t)len) {
                free(data); close(fd); return ERR_IO;
            }
        }
        free(data);
        if (ftruncate(fd, (off_t)nd->size) == -1) { close(fd); return ERR_IO; }
        fsync(fd);
        close(fd);
        break;
    }

    case NODE_TYPE_SYMLINK:
        /* target not yet stored separately; skip */
        break;

    case NODE_TYPE_FIFO:
        if (mkfifo(full, (mode_t)nd->mode) == -1 && errno != EEXIST) return ERR_IO;
        break;

    case NODE_TYPE_CHR:
    case NODE_TYPE_BLK: {
        dev_t dev = makedev(nd->device.major, nd->device.minor);
        mode_t m  = (nd->type == NODE_TYPE_CHR ? S_IFCHR : S_IFBLK) | (mode_t)nd->mode;
        if (mknod(full, m, dev) == -1 && errno != EEXIST) return ERR_IO;
        break;
    }

    default:
        break;
    }

    (void)lchown(full, (uid_t)nd->uid, (gid_t)nd->gid);
    if (nd->type != NODE_TYPE_SYMLINK) chmod(full, (mode_t)nd->mode);

    /* xattrs */
    if (!hash_is_zero_local(nd->xattr_hash)) {
        void *xd = NULL; size_t xl = 0;
        if (object_load(repo, nd->xattr_hash, &xd, &xl) == OK) {
            uint8_t *p = xd, *end = p + xl;
            while (p < end) {
                if (p + 2 > end) break;
                uint16_t nlen; memcpy(&nlen, p, 2); p += 2;
                if (p + nlen > end) break;
                char *name = (char *)p; p += nlen;
                if (p + 4 > end) break;
                uint32_t vlen; memcpy(&vlen, p, 4); p += 4;
                if (p + vlen > end) break;
                lsetxattr(full, name, p, vlen, 0);
                p += vlen;
            }
            free(xd);
        }
    }

    /* ACL */
    if (!hash_is_zero_local(nd->acl_hash)) {
        void *ad = NULL; size_t al = 0;
        if (object_load(repo, nd->acl_hash, &ad, &al) == OK) {
            acl_t acl = acl_from_text((char *)ad);
            if (acl) { acl_set_file(full, ACL_TYPE_ACCESS, acl); acl_free(acl); }
            free(ad);
        }
    }

    /* timestamps */
    struct timespec times[2] = {
        { .tv_sec = (time_t)nd->mtime_sec, .tv_nsec = (long)nd->mtime_nsec },
        { .tv_sec = (time_t)nd->mtime_sec, .tv_nsec = (long)nd->mtime_nsec },
    };
    if (nd->type != NODE_TYPE_SYMLINK) {
        utimensat(AT_FDCWD, full, times, 0);
    } else {
        utimensat(AT_FDCWD, full, times, AT_SYMLINK_NOFOLLOW);
    }

    return OK;
}

status_t restore_latest(repo_t *repo, const char *dest_path) {
    uint32_t head_id = 0;
    status_t st = snapshot_read_head(repo, &head_id);
    if (st != OK || head_id == 0) { log_msg("ERROR", "no snapshots in repo"); return ERR_IO; }
    return restore_snapshot(repo, head_id, dest_path);
}

status_t restore_snapshot(repo_t *repo, uint32_t snap_id, const char *dest_path) {
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    log_msg("INFO", "restoring snapshot");

    /* directories first */
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type != NODE_TYPE_DIR) continue;
        char rel[64];
        snprintf(rel, sizeof(rel), "node_%llu", (unsigned long long)snap->nodes[i].node_id);
        restore_node(repo, &snap->nodes[i], rel, dest_path);
    }
    /* then everything else */
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_DIR) continue;
        char rel[64];
        snprintf(rel, sizeof(rel), "node_%llu", (unsigned long long)snap->nodes[i].node_id);
        st = restore_node(repo, &snap->nodes[i], rel, dest_path);
        if (st != OK) break;
    }

    snapshot_free(snap);
    return st;
}

status_t restore_file(repo_t *repo, uint32_t snap_id,
                      const char *file_path, const char *dest_path) {
    (void)file_path;
    return restore_snapshot(repo, snap_id, dest_path);
}
