#define _POSIX_C_SOURCE 200809L
#include "synth.h"
#include "restore.h"
#include "snapshot.h"
#include "../vendor/log.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Path LUT for parent resolution                                      */
/* ------------------------------------------------------------------ */

typedef struct { const char *path; uint64_t node_id; } path_lut_t;

static int path_lut_cmp(const void *a, const void *b) {
    return strcmp(((const path_lut_t *)a)->path,
                  ((const path_lut_t *)b)->path);
}

/* Return dirname of path into buf (no trailing slash; "" for root entries). */
static void path_dirname(const char *path, char *buf, size_t bufsz) {
    const char *slash = strrchr(path, '/');
    if (!slash || slash == path) {
        buf[0] = '\0';
        return;
    }
    size_t len = (size_t)(slash - path);
    if (len >= bufsz) len = bufsz - 1;
    memcpy(buf, path, len);
    buf[len] = '\0';
}

/* ------------------------------------------------------------------ */
/* Working-set → snapshot_t conversion                                 */
/* ------------------------------------------------------------------ */

static status_t ws_to_snapshot(const ws_entry_t *ws, uint32_t ws_cnt,
                                uint32_t snap_id, snapshot_t **out) {
    /* Count live (non-NULL path) entries */
    uint32_t live = 0;
    for (uint32_t i = 0; i < ws_cnt; i++)
        if (ws[i].path) live++;

    if (live == 0) {
        /* Empty working set — produce an empty snapshot */
        snapshot_t *snap = calloc(1, sizeof(*snap));
        if (!snap) return ERR_NOMEM;
        snap->snap_id     = snap_id;
        snap->created_sec = (uint64_t)time(NULL);
        *out = snap;
        return OK;
    }

    /* Build path LUT (sorted for bsearch parent lookup) */
    path_lut_t *lut = malloc(live * sizeof(path_lut_t));
    if (!lut) return ERR_NOMEM;
    uint32_t lut_cnt = 0;
    for (uint32_t i = 0; i < ws_cnt; i++) {
        if (!ws[i].path) continue;
        lut[lut_cnt].path    = ws[i].path;
        lut[lut_cnt].node_id = ws[i].node.node_id;
        lut_cnt++;
    }
    qsort(lut, lut_cnt, sizeof(path_lut_t), path_lut_cmp);

    /* Collect unique node_ids for the node table */
    uint64_t *unique_ids = malloc(live * sizeof(uint64_t));
    if (!unique_ids) { free(lut); return ERR_NOMEM; }
    uint32_t n_unique = 0;
    for (uint32_t i = 0; i < ws_cnt; i++) {
        if (!ws[i].path) continue;
        uint64_t nid = ws[i].node.node_id;
        int found = 0;
        for (uint32_t j = 0; j < n_unique; j++) {
            if (unique_ids[j] == nid) { found = 1; break; }
        }
        if (!found) unique_ids[n_unique++] = nid;
    }

    /* Build node table: one node_t per unique node_id */
    node_t *nodes = malloc(n_unique * sizeof(node_t));
    if (!nodes) { free(unique_ids); free(lut); return ERR_NOMEM; }
    for (uint32_t u = 0; u < n_unique; u++) {
        uint64_t nid = unique_ids[u];
        /* Find the first ws entry with this node_id */
        for (uint32_t i = 0; i < ws_cnt; i++) {
            if (ws[i].path && ws[i].node.node_id == nid) {
                nodes[u] = ws[i].node;
                break;
            }
        }
    }
    free(unique_ids);

    /* Build dirent blob */
    /* First pass: compute total size */
    size_t dirent_data_sz = 0;
    for (uint32_t i = 0; i < ws_cnt; i++) {
        if (!ws[i].path) continue;
        const char *slash = strrchr(ws[i].path, '/');
        const char *name  = slash ? slash + 1 : ws[i].path;
        dirent_data_sz += sizeof(dirent_rec_t) + strlen(name);
    }

    uint8_t *dirent_data = malloc(dirent_data_sz ? dirent_data_sz : 1);
    if (!dirent_data) { free(nodes); free(lut); return ERR_NOMEM; }

    /* Second pass: write records */
    uint8_t *dp = dirent_data;
    char dir_buf[PATH_MAX];
    for (uint32_t i = 0; i < ws_cnt; i++) {
        if (!ws[i].path) continue;

        const char *slash = strrchr(ws[i].path, '/');
        const char *name  = slash ? slash + 1 : ws[i].path;
        uint16_t    nlen  = (uint16_t)strlen(name);

        /* Resolve parent node_id via bsearch on lut */
        uint64_t parent_node_id = 0;
        path_dirname(ws[i].path, dir_buf, sizeof(dir_buf));
        if (dir_buf[0] != '\0') {
            path_lut_t key = { .path = dir_buf };
            path_lut_t *found = bsearch(&key, lut, lut_cnt,
                                        sizeof(path_lut_t), path_lut_cmp);
            if (found) parent_node_id = found->node_id;
        }

        dirent_rec_t dr = {
            .parent_node = parent_node_id,
            .node_id     = ws[i].node.node_id,
            .name_len    = nlen,
        };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, name, nlen);     dp += nlen;
    }

    free(lut);

    /* Assemble snapshot_t */
    snapshot_t *snap = calloc(1, sizeof(*snap));
    if (!snap) { free(nodes); free(dirent_data); return ERR_NOMEM; }
    snap->snap_id         = snap_id;
    snap->created_sec     = (uint64_t)time(NULL);
    snap->node_count      = n_unique;
    snap->dirent_count    = live;
    snap->nodes           = nodes;
    snap->dirent_data     = dirent_data;
    snap->dirent_data_len = dirent_data_sz;

    *out = snap;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

status_t snapshot_synthesize(repo_t *repo, uint32_t target_id) {
    /* Check if the snapshot file already exists */
    snapshot_t *probe = NULL;
    if (snapshot_load(repo, target_id, &probe) == OK) {
        snapshot_free(probe);
        return OK;   /* already a full checkpoint */
    }

    /* Build working set for this target */
    ws_entry_t *ws = NULL;
    uint32_t ws_cnt = 0, anchor_id = 0;
    status_t st = restore_build_ws(repo, target_id, &ws, &ws_cnt, &anchor_id);
    if (st != OK) return st;

    fprintf(stderr, "synthesizing checkpoint %u (anchor %u, %u entries)\n",
            target_id, anchor_id, ws_cnt);

    /* Convert to snapshot_t and write */
    snapshot_t *snap = NULL;
    st = ws_to_snapshot(ws, ws_cnt, target_id, &snap);

    for (uint32_t i = 0; i < ws_cnt; i++) free(ws[i].path);
    free(ws);

    if (st != OK) return st;

    st = snapshot_write(repo, snap);
    snapshot_free(snap);
    return st;
}

status_t snapshot_synthesize_every(repo_t *repo, uint32_t interval,
                                   uint32_t *out_count) {
    if (interval == 0) return ERR_INVALID;

    uint32_t head_id = 0;
    if (snapshot_read_head(repo, &head_id) != OK || head_id == 0) {
        log_msg("ERROR", "cannot read HEAD"); return ERR_IO;
    }

    /*
     * Iterate high → low so each synthesis can anchor on the checkpoint
     * just written above it, keeping each reverse-chain walk to at most
     * `interval` steps.  Low → high would anchor every synthesis on the
     * nearest surviving full snapshot above (potentially far away).
     */
    uint32_t count = 0;
    uint32_t start = (head_id / interval) * interval;
    /* HEAD itself always has a .snap file; snapshot_synthesize is a no-op for
     * it, but skipping it avoids a pointless probe. */
    if (start >= head_id && start >= interval) start -= interval;
    for (uint32_t id = start; id >= interval; id -= interval) {
        status_t st = snapshot_synthesize(repo, id);
        if (st != OK) return st;
        count++;
    }

    if (out_count) *out_count = count;
    return OK;
}
