#define _POSIX_C_SOURCE 200809L
#include "diff.h"
#include "snapshot.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Hash helpers (same as snapshot.c's static versions)                  */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t  key;
    uintptr_t val;
} id_slot_t;

static uint64_t id_hash_u64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static uint64_t pm_fnv1a(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h;
}

/* ------------------------------------------------------------------ */
/* Core: collect structured diff result                                */
/*                                                                     */
/* Strategy: build a pathmap for each snapshot, then iterate+probe     */
/* instead of copy+sort+merge. Only changed entries (typically a tiny  */
/* fraction of the total) produce allocations.                         */
/* ------------------------------------------------------------------ */

static status_t result_push(diff_result_t *r, char change,
                             const char *path,
                             const node_t *old_node,
                             const node_t *new_node)
{
    if (r->count == r->capacity) {
        uint32_t newcap = r->capacity ? r->capacity * 2 : 256;
        diff_change_t *tmp = realloc(r->changes, (size_t)newcap * sizeof(*tmp));
        if (!tmp) return set_error(ERR_NOMEM, "diff: result realloc failed");
        r->changes  = tmp;
        r->capacity = newcap;
    }
    diff_change_t *c = &r->changes[r->count++];
    c->change = change;
    c->path   = strdup(path);
    if (!c->path) return set_error(ERR_NOMEM, "diff: strdup change path failed");

    static const node_t zero_node;
    c->old_node = old_node ? *old_node : zero_node;
    c->new_node = new_node ? *new_node : zero_node;
    return OK;
}

static int change_path_cmp(const void *a, const void *b) {
    return strcmp(((const diff_change_t *)a)->path,
                  ((const diff_change_t *)b)->path);
}

/* Build a pathmap from snap using a shared dirent structure.
 * If ref_snap has identical dirent data, reuse its dirent parsing
 * but look up nodes from snap's node array. */
static status_t build_pathmap_shared(const snapshot_t *snap,
                                      const snapshot_t *ref_snap,
                                      const pathmap_t *ref_pm,
                                      pathmap_t **out)
{
    /* Dirent data identical → paths are the same, only nodes differ.
     * Clone ref_pm's keys, look up each path's node_id in snap. */
    (void)ref_snap;

    /* Build node_id → node pointer map for snap */
    size_t ncap = 16;
    while (ncap < snap->node_count * 2u) ncap <<= 1;
    id_slot_t *nidx = calloc(ncap, sizeof(id_slot_t));
    if (!nidx) return set_error(ERR_NOMEM, "diff: shared nidx alloc");
    size_t nmask = ncap - 1;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint64_t nid = snap->nodes[i].node_id;
        size_t h = (size_t)(id_hash_u64(nid) & (uint64_t)nmask);
        while (nidx[h].key != 0) h = (h + 1) & nmask;
        nidx[h].key = nid;
        nidx[h].val = (uintptr_t)&snap->nodes[i];
    }

    /* Build new pathmap with same capacity as ref */
    pathmap_t *m = calloc(1, sizeof(*m));
    if (!m) { free(nidx); return set_error(ERR_NOMEM, "diff: shared pm alloc"); }
    m->slots = calloc(ref_pm->capacity, sizeof(pm_slot_t));
    m->capacity = ref_pm->capacity;
    if (!m->slots) { free(m); free(nidx); return set_error(ERR_NOMEM, "diff: shared slots alloc"); }

    /* Copy paths from ref, but look up node in snap */
    for (size_t i = 0; i < ref_pm->capacity; i++) {
        if (!ref_pm->slots[i].key) continue;
        /* The node_id for this path must be the same (same dirent data) */
        uint64_t nid = ref_pm->slots[i].value.node_id;
        const node_t *nd = NULL;
        size_t h = (size_t)(id_hash_u64(nid) & (uint64_t)nmask);
        while (nidx[h].key != 0) {
            if (nidx[h].key == nid) { nd = (const node_t *)nidx[h].val; break; }
            h = (h + 1) & nmask;
        }
        /* Insert into new map: reuse the path string via strdup */
        size_t mh = (size_t)(pm_fnv1a(ref_pm->slots[i].key) & (uint64_t)(m->capacity - 1));
        while (m->slots[mh].key) mh = (mh + 1) & (m->capacity - 1);
        m->slots[mh].key = strdup(ref_pm->slots[i].key);
        if (!m->slots[mh].key) { free(nidx); pathmap_free(m); return set_error(ERR_NOMEM, "diff: shared strdup"); }
        m->slots[mh].value = nd ? *nd : ref_pm->slots[i].value;
        m->slots[mh].seen = 0;
        m->count++;
    }

    free(nidx);
    *out = m;
    return OK;
}

status_t snapshot_diff_collect(repo_t *repo, uint32_t snap_id1, uint32_t snap_id2,
                                diff_result_t **out)
{
    /* Load both snapshots */
    snapshot_t *snap1 = NULL;
    status_t st = snapshot_load(repo, snap_id1, &snap1);
    if (st != OK) return st;

    snapshot_t *snap2 = NULL;
    st = snapshot_load(repo, snap_id2, &snap2);
    if (st != OK) { snapshot_free(snap1); return st; }

    /* Build pathmaps — share path construction if dirent data matches */
    pathmap_t *pm1 = NULL;
    st = pathmap_build(snap1, &pm1);
    if (st != OK) { snapshot_free(snap1); snapshot_free(snap2); return st; }

    pathmap_t *pm2 = NULL;
    int shared = (snap1->dirent_data_len == snap2->dirent_data_len &&
                  snap1->dirent_data_len > 0 &&
                  memcmp(snap1->dirent_data, snap2->dirent_data,
                         snap1->dirent_data_len) == 0);
    if (shared) {
        st = build_pathmap_shared(snap2, snap1, pm1, &pm2);
    } else {
        st = pathmap_build(snap2, &pm2);
    }
    snapshot_free(snap1);
    snapshot_free(snap2);
    if (st != OK) { pathmap_free(pm1); return st; }

    /* Allocate result */
    diff_result_t *r = calloc(1, sizeof(*r));
    if (!r) {
        pathmap_free(pm1); pathmap_free(pm2);
        return set_error(ERR_NOMEM, "diff: result alloc failed");
    }

    /* Iterate pm2: find Added and Modified entries */
    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    for (size_t i = 0; i < pm2->capacity; i++) {
        if (!pm2->slots[i].key) continue;
        const char *path = pm2->slots[i].key;
        const node_t *n2 = &pm2->slots[i].value;
        const node_t *n1 = pathmap_lookup(pm1, path);

        if (!n1) {
            st = result_push(r, 'A', path, NULL, n2);
            if (st != OK) goto cleanup;
        } else {
            pathmap_mark_seen(pm1, path);
            int content_changed = (memcmp(n1->content_hash, n2->content_hash,
                                          OBJECT_HASH_SIZE) != 0);
            int meta_changed    = (!content_changed &&
                                   (n1->mode != n2->mode ||
                                    n1->uid  != n2->uid  ||
                                    n1->gid  != n2->gid  ||
                                    n1->mtime_sec != n2->mtime_sec));
            if (content_changed) {
                st = result_push(r, 'M', path, n1, n2);
                if (st != OK) goto cleanup;
            } else if (meta_changed) {
                st = result_push(r, 'm', path, n1, n2);
                if (st != OK) goto cleanup;
            }
        }
    }

    /* Iterate pm1: unseen entries are Deleted */
    for (size_t i = 0; i < pm1->capacity; i++) {
        if (!pm1->slots[i].key || pm1->slots[i].seen) continue;
        st = result_push(r, 'D', pm1->slots[i].key, &pm1->slots[i].value, NULL);
        if (st != OK) goto cleanup;
    }

    /* Sort by path for consistent output */
    if (r->count > 1)
        qsort(r->changes, r->count, sizeof(*r->changes), change_path_cmp);

    *out = r;
    st = OK;

cleanup:
    pathmap_free(pm1);
    pathmap_free(pm2);
    if (st != OK && r) { diff_result_free(r); }
    return st;
}

void diff_result_free(diff_result_t *r)
{
    if (!r) return;
    for (uint32_t i = 0; i < r->count; i++)
        free(r->changes[i].path);
    free(r->changes);
    free(r);
}

/* ------------------------------------------------------------------ */
/* CLI wrapper: prints to stdout (unchanged behavior)                  */
/* ------------------------------------------------------------------ */

status_t snapshot_diff(repo_t *repo, uint32_t snap_id1, uint32_t snap_id2) {
    diff_result_t *r = NULL;
    status_t st = snapshot_diff_collect(repo, snap_id1, snap_id2, &r);
    if (st != OK) return st;

    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    for (uint32_t i = 0; i < r->count; i++)
        printf("%c  %s\n", r->changes[i].change, r->changes[i].path);

    if (r->count == 0) printf("(no differences)\n");

    diff_result_free(r);
    return OK;
}
