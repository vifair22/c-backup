#define _POSIX_C_SOURCE 200809L
#include "backup.h"
#include "scan.h"
#include "object.h"
#include "snapshot.h"
#include "reverse.h"
#include "../vendor/log.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/* Change classification */
#define CHANGE_UNCHANGED     0
#define CHANGE_CREATED       1
#define CHANGE_MODIFIED      2
#define CHANGE_METADATA_ONLY 3

/* Read a regular file; caller frees *buf. */
static status_t read_file(const char *path, uint8_t **buf, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return ERR_IO;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    rewind(f);
    if (sz < 0) { fclose(f); return ERR_IO; }
    *buf = malloc((size_t)sz + 1);
    if (!*buf) { fclose(f); return ERR_NOMEM; }
    *len = fread(*buf, 1, (size_t)sz, f);
    fclose(f);
    return OK;
}


/* Append a reverse entry; grows the array as needed. */
static status_t rev_append(rev_record_t *rev, uint32_t *cap,
                            uint8_t op, const char *path, const node_t *prev) {
    if (rev->entry_count == *cap) {
        uint32_t nc = *cap ? *cap * 2 : 16;
        rev_entry_t *tmp = realloc(rev->entries, nc * sizeof(*tmp));
        if (!tmp) return ERR_NOMEM;
        rev->entries = tmp;
        *cap = nc;
    }
    rev_entry_t *e = &rev->entries[rev->entry_count++];
    e->op_type = op;
    e->path    = strdup(path);
    if (!e->path) { rev->entry_count--; return ERR_NOMEM; }
    if (prev)
        e->prev_node = *prev;
    else
        memset(&e->prev_node, 0, sizeof(e->prev_node));
    return OK;
}

/* Context passed to pathmap_foreach_unseen callback */
typedef struct {
    rev_record_t *rev;
    uint32_t     *cap;
    status_t      st;
} deleted_ctx_t;

static void deleted_cb(const char *path, const node_t *node, void *ctx_) {
    deleted_ctx_t *ctx = ctx_;
    if (ctx->st != OK) return;
    ctx->st = rev_append(ctx->rev, ctx->cap, REV_OP_RESTORE, path, node);
}

status_t backup_run(repo_t *repo, const char **source_paths, int path_count) {
    /* ----------------------------------------------------------------
     * Phase 1: Scan source trees
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 1: scanning");

    scan_result_t *scan = NULL;
    for (int i = 0; i < path_count; i++) {
        scan_result_t *partial = NULL;
        status_t st = scan_tree(source_paths[i], &partial);
        if (st != OK) { scan_result_free(scan); return st; }
        if (!scan) {
            scan = partial;
        } else {
            for (uint32_t j = 0; j < partial->count; j++) {
                if (scan->count == scan->capacity) {
                    uint32_t nc = scan->capacity * 2;
                    scan_entry_t *tmp = realloc(scan->entries, nc * sizeof(*tmp));
                    if (!tmp) {
                        scan_result_free(scan); scan_result_free(partial);
                        return ERR_NOMEM;
                    }
                    scan->entries  = tmp;
                    scan->capacity = nc;
                }
                scan->entries[scan->count++] = partial->entries[j];
                memset(&partial->entries[j], 0, sizeof(partial->entries[j]));
            }
            partial->count = 0;
            scan_result_free(partial);
        }
    }
    if (!scan) return ERR_INVALID;

    /* ----------------------------------------------------------------
     * Phase 2: Load previous snapshot + build path map
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 2: loading previous snapshot");

    uint32_t  prev_id   = 0;
    snapshot_t *prev_snap = NULL;
    pathmap_t  *prev_map  = NULL;

    if (snapshot_read_head(repo, &prev_id) == OK && prev_id > 0) {
        if (snapshot_load(repo, prev_id, &prev_snap) == OK) {
            if (pathmap_build(prev_snap, &prev_map) != OK) {
                log_msg("WARN", "could not build previous path map; treating all as new");
                prev_map = NULL;
            }
        }
    }

    /* ----------------------------------------------------------------
     * Phase 3: Compare, hash, store objects, build reverse records
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 3: compare and store");

    rev_record_t rev  = { .snap_id = prev_id + 1 };
    uint32_t     rev_cap = 0;
    status_t     st   = OK;

    for (uint32_t i = 0; i < scan->count; i++) {
        scan_entry_t *e = &scan->entries[i];

        /* Repo-relative path: strip everything before basename(source root) */
        const char *rel = e->path + e->strip_prefix_len;

        /* --- Step A: store xattr/acl objects and get their hashes --- */
        if (e->xattr_len > 0) {
            st = object_store(repo, OBJECT_TYPE_XATTR,
                              e->xattr_data, e->xattr_len, e->node.xattr_hash);
            if (st != OK) goto done;
        }
        if (e->acl_len > 0) {
            st = object_store(repo, OBJECT_TYPE_ACL,
                              e->acl_data, e->acl_len, e->node.acl_hash);
            if (st != OK) goto done;
        }

        /* --- Step B: look up in previous snapshot --- */
        const node_t *prev = prev_map ? pathmap_lookup(prev_map, rel) : NULL;
        if (prev) pathmap_mark_seen(prev_map, rel);

        /* --- Step C: classify change --- */
        int change;
        if (!prev) {
            change = CHANGE_CREATED;
        } else {
            int content_same;
            if (e->node.type == NODE_TYPE_REG) {
                /* Use mtime + size as a fast proxy for content equality */
                content_same = (e->node.size       == prev->size       &&
                                e->node.mtime_sec  == prev->mtime_sec  &&
                                e->node.mtime_nsec == prev->mtime_nsec);
            } else {
                content_same = 1;
            }

            int meta_same = (e->node.mode == prev->mode &&
                             e->node.uid  == prev->uid  &&
                             e->node.gid  == prev->gid  &&
                             memcmp(e->node.xattr_hash, prev->xattr_hash,
                                    OBJECT_HASH_SIZE) == 0 &&
                             memcmp(e->node.acl_hash, prev->acl_hash,
                                    OBJECT_HASH_SIZE) == 0);

            if (content_same && meta_same)
                change = CHANGE_UNCHANGED;
            else if (content_same)
                change = CHANGE_METADATA_ONLY;
            else
                change = CHANGE_MODIFIED;
        }

        /* --- Step D: handle content --- */
        if (e->node.type == NODE_TYPE_REG) {
            if (change == CHANGE_UNCHANGED || change == CHANGE_METADATA_ONLY) {
                /* Inherit content hash; no need to re-read the file */
                memcpy(e->node.content_hash, prev->content_hash, OBJECT_HASH_SIZE);
            } else if (e->node.size > 0) {
                /* CREATED or MODIFIED: read, hash, store */
                uint8_t *fbuf = NULL; size_t flen = 0;
                if (read_file(e->path, &fbuf, &flen) == OK) {
                    st = object_store(repo, OBJECT_TYPE_FILE, fbuf, flen,
                                      e->node.content_hash);
                    free(fbuf);
                    if (st != OK) goto done;
                }
            }
        }

        /* --- Step E: emit reverse entry --- */
        switch (change) {
        case CHANGE_CREATED:
            st = rev_append(&rev, &rev_cap, REV_OP_REMOVE, rel, NULL);
            break;
        case CHANGE_MODIFIED:
            st = rev_append(&rev, &rev_cap, REV_OP_RESTORE, rel, prev);
            break;
        case CHANGE_METADATA_ONLY:
            st = rev_append(&rev, &rev_cap, REV_OP_META, rel, prev);
            break;
        case CHANGE_UNCHANGED:
        default:
            break;
        }
        if (st != OK) goto done;
    }

    /* --- Find deleted entries (in prev but not seen in current scan) --- */
    if (prev_map) {
        deleted_ctx_t dctx = { .rev = &rev, .cap = &rev_cap, .st = OK };
        pathmap_foreach_unseen(prev_map, deleted_cb, &dctx);
        if (dctx.st != OK) { st = dctx.st; goto done; }
    }

    /* ----------------------------------------------------------------
     * Phase 4 & 5: Build new snapshot
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 4/5: building snapshot");

    snapshot_t *new_snap = calloc(1, sizeof(*new_snap));
    if (!new_snap) { st = ERR_NOMEM; goto done; }
    new_snap->snap_id    = prev_id + 1;
    new_snap->node_count = scan->count;
    new_snap->nodes      = malloc(scan->count * sizeof(node_t));
    if (!new_snap->nodes) { free(new_snap); st = ERR_NOMEM; goto done; }
    for (uint32_t i = 0; i < scan->count; i++)
        new_snap->nodes[i] = scan->entries[i].node;

    /* Build dirent table using parent_node_id from scan entries */
    size_t dirent_buf_sz = 0;
    for (uint32_t i = 0; i < scan->count; i++) {
        const char *name = strrchr(scan->entries[i].path, '/');
        name = name ? name + 1 : scan->entries[i].path;
        dirent_buf_sz += sizeof(dirent_rec_t) + strlen(name);
    }
    new_snap->dirent_data     = malloc(dirent_buf_sz ? dirent_buf_sz : 1);
    new_snap->dirent_data_len = dirent_buf_sz;
    new_snap->dirent_count    = scan->count;

    if (!new_snap->dirent_data) {
        snapshot_free(new_snap); st = ERR_NOMEM; goto done;
    }

    uint8_t *dp = new_snap->dirent_data;
    for (uint32_t i = 0; i < scan->count; i++) {
        const char *name = strrchr(scan->entries[i].path, '/');
        name = name ? name + 1 : scan->entries[i].path;
        uint16_t nlen = (uint16_t)strlen(name);
        dirent_rec_t dr = {
            .parent_node = scan->entries[i].parent_node_id,
            .node_id     = scan->entries[i].node.node_id,
            .name_len    = nlen,
        };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, name, nlen);      dp += nlen;
    }

    /* ----------------------------------------------------------------
     * Phase 6: Commit (objects → reverse → snapshot → HEAD)
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 6: committing");

    if (rev.entry_count > 0) {
        st = reverse_write(repo, &rev);
        if (st != OK) { snapshot_free(new_snap); goto done; }
    }

    st = snapshot_write(repo, new_snap);
    if (st != OK) { snapshot_free(new_snap); goto done; }

    st = snapshot_write_head(repo, new_snap->snap_id);
    if (st == OK) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot %u written (%u entries, %u changes)",
                 new_snap->snap_id, scan->count, rev.entry_count);
        log_msg("INFO", msg);
    }

    snapshot_free(new_snap);

done:
    pathmap_free(prev_map);
    snapshot_free(prev_snap);
    scan_result_free(scan);
    for (uint32_t i = 0; i < rev.entry_count; i++) free(rev.entries[i].path);
    free(rev.entries);
    return st;
}
