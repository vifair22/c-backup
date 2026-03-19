#define _POSIX_C_SOURCE 200809L
#include "backup.h"
#include "scan.h"
#include "object.h"
#include "snapshot.h"
#include "reverse.h"
#include "../vendor/log.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>



/* Read a regular file and return its content. Caller frees *buf. */
static status_t read_file(const char *path, uint8_t **buf, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return ERR_IO;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return ERR_IO; }
    *buf = malloc((size_t)sz + 1);
    if (!*buf) { fclose(f); return ERR_NOMEM; }
    *len = fread(*buf, 1, (size_t)sz, f);
    fclose(f);
    return OK;
}

status_t backup_run(repo_t *repo, const char **source_paths, int path_count) {
    /* --- Phase 1: Scan --- */
    log_msg("INFO", "Phase 1: scanning source tree");

    /* Merge multiple source paths into one result */
    scan_result_t *scan = NULL;
    for (int i = 0; i < path_count; i++) {
        scan_result_t *partial = NULL;
        status_t st = scan_tree(source_paths[i], &partial);
        if (st != OK) { scan_result_free(scan); return st; }
        if (!scan) {
            scan = partial;
        } else {
            /* append partial into scan */
            for (uint32_t j = 0; j < partial->count; j++) {
                /* shallow copy – ownership transferred */
                if (scan->count == scan->capacity) {
                    uint32_t nc = scan->capacity * 2;
                    scan_entry_t *tmp = realloc(scan->entries, nc * sizeof(*tmp));
                    if (!tmp) { scan_result_free(scan); scan_result_free(partial); return ERR_NOMEM; }
                    scan->entries  = tmp;
                    scan->capacity = nc;
                }
                scan->entries[scan->count++] = partial->entries[j];
                /* zero out to prevent double-free when partial is freed */
                memset(&partial->entries[j], 0, sizeof(partial->entries[j]));
            }
            partial->count = 0;
            scan_result_free(partial);
        }
    }
    if (!scan) return ERR_INVALID;

    /* --- Phase 2: Load previous snapshot --- */
    log_msg("INFO", "Phase 2: loading previous snapshot");
    uint32_t prev_id = 0;
    snapshot_t *prev_snap = NULL;
    if (snapshot_read_head(repo, &prev_id) == OK && prev_id > 0) {
        snapshot_load(repo, prev_id, &prev_snap);
    }

    /* --- Phase 3: Content handling + reverse records --- */
    log_msg("INFO", "Phase 3: hashing and storing objects");

    rev_record_t rev = {0};
    rev.snap_id = prev_id + 1;
    rev.entries  = NULL;
    rev.entry_count = 0;
    uint32_t rev_cap = 0;

    for (uint32_t i = 0; i < scan->count; i++) {
        scan_entry_t *e = &scan->entries[i];

        /* store xattr object */
        if (e->xattr_len > 0) {
            object_store(repo, OBJECT_TYPE_XATTR,
                         e->xattr_data, e->xattr_len, e->node.xattr_hash);
        }
        /* store acl object */
        if (e->acl_len > 0) {
            object_store(repo, OBJECT_TYPE_ACL,
                         e->acl_data, e->acl_len, e->node.acl_hash);
        }
        /* store file content */
        if (e->node.type == NODE_TYPE_REG && e->node.size > 0) {
            uint8_t *fbuf = NULL; size_t flen = 0;
            if (read_file(e->path, &fbuf, &flen) == OK) {
                object_store(repo, OBJECT_TYPE_FILE, fbuf, flen, e->node.content_hash);
                free(fbuf);
            }
        }

        /* Build reverse entry if this path existed in previous snapshot.
         * (Simplified: compare by path via a linear scan of dirent data.)
         * A real implementation would use a hash map. */
        (void)prev_snap; /* used for comparison – omitted in this skeleton */
    }

    /* --- Phase 4 & 5: Build new snapshot --- */
    log_msg("INFO", "Phase 4/5: building snapshot");
    snapshot_t *new_snap = calloc(1, sizeof(*new_snap));
    if (!new_snap) { scan_result_free(scan); return ERR_NOMEM; }
    new_snap->snap_id    = prev_id + 1;
    new_snap->node_count = scan->count;
    new_snap->nodes      = malloc(scan->count * sizeof(node_t));
    if (!new_snap->nodes) { free(new_snap); scan_result_free(scan); return ERR_NOMEM; }
    for (uint32_t i = 0; i < scan->count; i++) {
        new_snap->nodes[i] = scan->entries[i].node;
    }

    /* Dirent table – each dirent is: parent_node(8) + node_id(8) + name_len(2) + name */
    /* For this initial version root entries have parent_node = 0 */
    size_t dirent_buf_sz = 0;
    for (uint32_t i = 0; i < scan->count; i++) {
        const char *name = strrchr(scan->entries[i].path, '/');
        name = name ? name + 1 : scan->entries[i].path;
        dirent_buf_sz += sizeof(dirent_rec_t) + strlen(name);
    }
    new_snap->dirent_data     = malloc(dirent_buf_sz ? dirent_buf_sz : 1);
    new_snap->dirent_data_len = dirent_buf_sz;
    new_snap->dirent_count    = scan->count;

    uint8_t *dp = new_snap->dirent_data;
    for (uint32_t i = 0; i < scan->count; i++) {
        const char *name = strrchr(scan->entries[i].path, '/');
        name = name ? name + 1 : scan->entries[i].path;
        uint16_t nlen = (uint16_t)strlen(name);
        dirent_rec_t dr = { .parent_node = 0, .node_id = scan->entries[i].node.node_id, .name_len = nlen };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, name, nlen); dp += nlen;
    }

    /* --- Phase 6: Commit --- */
    log_msg("INFO", "Phase 6: committing");
    status_t st = OK;

    /* 1. objects already written above */
    /* 2. write reverse file */
    if (rev.entry_count > 0) {
        st = reverse_write(repo, &rev);
        if (st != OK) goto done;
    }
    /* 3. write snapshot */
    st = snapshot_write(repo, new_snap);
    if (st != OK) goto done;
    /* 4. update HEAD */
    st = snapshot_write_head(repo, new_snap->snap_id);
    if (st == OK) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot %u written", new_snap->snap_id);
        log_msg("INFO", msg);
    }

done:
    snapshot_free(new_snap);
    snapshot_free(prev_snap);
    scan_result_free(scan);
    for (uint32_t i = 0; i < rev.entry_count; i++) free(rev.entries[i].path);
    free(rev.entries);
    (void)rev_cap;
    return st;
}
