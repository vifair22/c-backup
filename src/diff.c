#define _POSIX_C_SOURCE 200809L
#include "diff.h"
#include "snapshot.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Load a snapshot as a flat (path, node) array                        */
/* ------------------------------------------------------------------ */

typedef struct {
    char  *path;
    node_t node;
} diff_entry_t;

static status_t load_as_ws(repo_t *repo, uint32_t id,
                            diff_entry_t **out_ws, uint32_t *out_cnt) {
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, id, &snap);
    if (st != OK) return st;

    pathmap_t *pm = NULL;
    st = pathmap_build(snap, &pm);
    snapshot_free(snap);
    if (st != OK) return st;

    uint32_t cnt = 0;
    diff_entry_t *ws = calloc(pm->capacity, sizeof(*ws));
    if (!ws) { pathmap_free(pm); return ERR_NOMEM; }

    for (size_t i = 0; i < pm->capacity; i++) {
        if (!pm->slots[i].key) continue;
        ws[cnt].path = strdup(pm->slots[i].key);
        if (!ws[cnt].path) {
            for (uint32_t k = 0; k < cnt; k++) free(ws[k].path);
            free(ws);
            pathmap_free(pm);
            return ERR_NOMEM;
        }
        ws[cnt].node = pm->slots[i].value;
        cnt++;
    }
    pathmap_free(pm);
    *out_ws  = ws;
    *out_cnt = cnt;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Sorting                                                             */
/* ------------------------------------------------------------------ */

static int ws_path_cmp(const void *a, const void *b) {
    return strcmp(((const diff_entry_t *)a)->path,
                  ((const diff_entry_t *)b)->path);
}

/* ------------------------------------------------------------------ */
/* Public                                                              */
/* ------------------------------------------------------------------ */

status_t snapshot_diff(repo_t *repo, uint32_t snap_id1, uint32_t snap_id2) {
    diff_entry_t *ws1 = NULL, *ws2 = NULL;
    uint32_t    cnt1 = 0,   cnt2 = 0;

    status_t st = load_as_ws(repo, snap_id1, &ws1, &cnt1);
    if (st != OK) return st;
    st = load_as_ws(repo, snap_id2, &ws2, &cnt2);
    if (st != OK) {
        for (uint32_t i = 0; i < cnt1; i++) free(ws1[i].path);
        free(ws1); return st;
    }

    /* Compact out NULL paths and sort both arrays by path */
    uint32_t n1 = 0;
    for (uint32_t i = 0; i < cnt1; i++) if (ws1[i].path) ws1[n1++] = ws1[i];
    uint32_t n2 = 0;
    for (uint32_t i = 0; i < cnt2; i++) if (ws2[i].path) ws2[n2++] = ws2[i];

    qsort(ws1, n1, sizeof(*ws1), ws_path_cmp);
    qsort(ws2, n2, sizeof(*ws2), ws_path_cmp);

    /* Merge walk */
    uint32_t i = 0, j = 0;
    int any = 0;
    while (i < n1 || j < n2) {
        int cmp;
        if      (i >= n1) cmp =  1;
        else if (j >= n2) cmp = -1;
        else              cmp = strcmp(ws1[i].path, ws2[j].path);

        if (cmp < 0) {
            /* present in snap1 but not snap2 → Deleted */
            printf("D  %s\n", ws1[i].path);
            i++; any = 1;
        } else if (cmp > 0) {
            /* present in snap2 but not snap1 → Added */
            printf("A  %s\n", ws2[j].path);
            j++; any = 1;
        } else {
            /* present in both — compare */
            const node_t *nd1 = &ws1[i].node;
            const node_t *nd2 = &ws2[j].node;

            int content_changed = (memcmp(nd1->content_hash, nd2->content_hash,
                                          OBJECT_HASH_SIZE) != 0);
            int meta_changed    = (!content_changed &&
                                   (nd1->mode != nd2->mode ||
                                    nd1->uid  != nd2->uid  ||
                                    nd1->gid  != nd2->gid  ||
                                    nd1->mtime_sec != nd2->mtime_sec));
            if (content_changed) {
                printf("M  %s\n", ws1[i].path);
                any = 1;
            } else if (meta_changed) {
                printf("m  %s\n", ws1[i].path);
                any = 1;
            }
            i++; j++;
        }
    }

    if (!any) printf("(no differences)\n");

    for (uint32_t k = 0; k < cnt1; k++) free(ws1[k].path);
    for (uint32_t k = 0; k < cnt2; k++) free(ws2[k].path);
    free(ws1); free(ws2);
    return OK;
}
