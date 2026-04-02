#define _POSIX_C_SOURCE 200809L
#include "ls.h"
#include "object.h"
#include "snapshot.h"
#include "../vendor/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fnmatch.h>

/* ------------------------------------------------------------------ */
/* Internal path table — mirrors snap_paths_build in restore.c        */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t parent_id;
    uint64_t node_id;
    char    *name;
    char    *full_path;   /* lazily built */
} ls_dirent_t;

/* Hash map: node_id → index into flat dirent array (for parent lookup) */
typedef struct {
    uint64_t key;     /* node_id, 0 = empty */
    uint32_t idx;
} dr_slot_t;

static uint32_t dr_lookup(const dr_slot_t *slots, uint32_t mask, uint64_t nid)
{
    uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & mask;
    for (;;) {
        if (slots[h].key == 0)   return UINT32_MAX;
        if (slots[h].key == nid) return slots[h].idx;
        h = (h + 1) & mask;
    }
}

static char *build_path(ls_dirent_t *arr, const dr_slot_t *dr_map,
                         uint32_t dr_mask, ls_dirent_t *e)
{
    if (e->full_path) return e->full_path;
    if (e->parent_id == 0) {
        e->full_path = strdup(e->name);
        return e->full_path;
    }
    uint32_t pi = dr_lookup(dr_map, dr_mask, e->parent_id);
    ls_dirent_t *parent = (pi != UINT32_MAX) ? &arr[pi] : NULL;
    if (!parent) { e->full_path = strdup(e->name); return e->full_path; }
    char *pp = build_path(arr, dr_map, dr_mask, parent);
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

/* ------------------------------------------------------------------ */
/* Comparison for sorting child entries by name                        */
/* ------------------------------------------------------------------ */

static int ls_entry_cmp(const void *a, const void *b) {
    return strcmp(((const ls_entry_t *)a)->name,
                  ((const ls_entry_t *)b)->name);
}

/* ------------------------------------------------------------------ */
/* Core: collect structured ls result                                  */
/* ------------------------------------------------------------------ */

status_t snapshot_ls_collect(repo_t *repo, uint32_t snap_id, const char *dir_path,
                              int recursive, char type_filter, const char *name_glob,
                              ls_result_t **out)
{
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    /* Normalise dir_path: strip leading/trailing slashes, treat "." as "" */
    char norm[4096] = "";
    if (dir_path && strcmp(dir_path, ".") != 0 && strcmp(dir_path, "/") != 0) {
        const char *s = dir_path;
        while (*s == '/') s++;
        size_t len = strlen(s);
        while (len > 0 && s[len - 1] == '/') len--;
        if (len >= sizeof(norm)) { snapshot_free(snap); return set_error(ERR_INVALID, "ls: path too long"); }
        memcpy(norm, s, len);
        norm[len] = '\0';
    }

    /* ── Fast path: non-recursive listing ────────────────────────────
     * Instead of building all N paths, find the target dir's node_id
     * by walking path components, then scan dirents by parent_id.
     * Only touches O(children) entries instead of O(N). */
    if (!recursive) {
        /* Build node_id → node index hash map */
        uint32_t nmap_cap = 1;
        while (nmap_cap < snap->node_count * 2) nmap_cap <<= 1;
        uint32_t nmap_mask = nmap_cap - 1;
        dr_slot_t *nmap = calloc(nmap_cap, sizeof(dr_slot_t));
        if (!nmap) { snapshot_free(snap); return set_error(ERR_NOMEM, "ls: nmap alloc"); }
        for (uint32_t i = 0; i < snap->node_count; i++) {
            uint64_t nid = snap->nodes[i].node_id;
            uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & nmap_mask;
            while (nmap[h].key != 0) h = (h + 1) & nmap_mask;
            nmap[h].key = nid;
            nmap[h].idx = i;
        }

        /* Parse raw dirents (no path building) */
        const uint8_t *p   = snap->dirent_data;
        const uint8_t *end = p + snap->dirent_data_len;
        /* We need: parent_node, node_id, name for each dirent */
        typedef struct { uint64_t parent_id; uint64_t node_id; char *name; } raw_dr_t;
        raw_dr_t *raws = calloc(snap->dirent_count + 1, sizeof(raw_dr_t));
        if (!raws) { free(nmap); snapshot_free(snap); return set_error(ERR_NOMEM, "ls: raws alloc"); }
        uint32_t n_raws = 0;
        while (p < end && n_raws < snap->dirent_count) {
            if (p + sizeof(dirent_rec_t) > end) break;
            dirent_rec_t dr;
            memcpy(&dr, p, sizeof(dr));
            p += sizeof(dr);
            if (p + dr.name_len > end) break;
            char *nm = malloc(dr.name_len + 1);
            if (!nm) { for (uint32_t k = 0; k < n_raws; k++) free(raws[k].name); free(raws); free(nmap); snapshot_free(snap); return set_error(ERR_NOMEM, "ls: name alloc"); }
            memcpy(nm, p, dr.name_len);
            nm[dr.name_len] = '\0';
            p += dr.name_len;
            raws[n_raws].parent_id = dr.parent_node;
            raws[n_raws].node_id   = dr.node_id;
            raws[n_raws].name      = nm;
            n_raws++;
        }

        /* Find target directory's node_id by walking path components */
        uint64_t target_parent = 0;  /* 0 = root */
        if (norm[0] != '\0') {
            char path_copy[4096];
            memcpy(path_copy, norm, strlen(norm) + 1);
            char *saveptr = NULL;
            char *comp = strtok_r(path_copy, "/", &saveptr);
            while (comp) {
                int found = 0;
                for (uint32_t i = 0; i < n_raws; i++) {
                    if (raws[i].parent_id == target_parent &&
                        strcmp(raws[i].name, comp) == 0) {
                        /* Verify it's a directory */
                        uint32_t ni = dr_lookup(nmap, nmap_mask, raws[i].node_id);
                        if (ni != UINT32_MAX && snap->nodes[ni].type == NODE_TYPE_DIR) {
                            target_parent = raws[i].node_id;
                            found = 1;
                            break;
                        }
                    }
                }
                if (!found) {
                    for (uint32_t k = 0; k < n_raws; k++) free(raws[k].name);
                    free(raws); free(nmap); snapshot_free(snap);
                    return set_error(ERR_INVALID, "ls: '%s' is not a directory in snapshot %u",
                                     norm, snap_id);
                }
                comp = strtok_r(NULL, "/", &saveptr);
            }
        }

        /* Collect children with matching parent_id */
        ls_entry_t *children = malloc((n_raws + 1) * sizeof(ls_entry_t));
        if (!children) { for (uint32_t k = 0; k < n_raws; k++) free(raws[k].name); free(raws); free(nmap); snapshot_free(snap); return set_error(ERR_NOMEM, "ls: children alloc"); }
        uint32_t n_children = 0;

        for (uint32_t i = 0; i < n_raws; i++) {
            if (raws[i].parent_id != target_parent) continue;
            uint32_t ni = dr_lookup(nmap, nmap_mask, raws[i].node_id);
            const node_t *nd = (ni != UINT32_MAX) ? &snap->nodes[ni] : NULL;
            if (!nd) continue;

            int type_ok = 1;
            if (type_filter) {
                switch (type_filter) {
                    case 'f': type_ok = (nd->type == NODE_TYPE_REG || nd->type == NODE_TYPE_HARDLINK); break;
                    case 'd': type_ok = (nd->type == NODE_TYPE_DIR); break;
                    case 'l': type_ok = (nd->type == NODE_TYPE_SYMLINK); break;
                    case 'p': type_ok = (nd->type == NODE_TYPE_FIFO); break;
                    case 'c': type_ok = (nd->type == NODE_TYPE_CHR); break;
                    case 'b': type_ok = (nd->type == NODE_TYPE_BLK); break;
                    default: type_ok = 1; break;
                }
            }
            if (!type_ok) continue;
            if (name_glob && *name_glob && fnmatch(name_glob, raws[i].name, 0) != 0) continue;

            ls_entry_t *ce = &children[n_children++];
            ce->name = strdup(raws[i].name);
            ce->node = *nd;
            ce->symlink_target[0] = '\0';

            if (nd->type == NODE_TYPE_SYMLINK) {
                uint8_t zero[OBJECT_HASH_SIZE] = {0};
                if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) {
                    void  *tdata = NULL;
                    size_t tlen  = 0;
                    if (object_load(repo, nd->content_hash, &tdata, &tlen, NULL) == OK) {
                        size_t copy = tlen < sizeof(ce->symlink_target) - 1
                                      ? tlen : sizeof(ce->symlink_target) - 1;
                        memcpy(ce->symlink_target, tdata, copy);
                        ce->symlink_target[copy] = '\0';
                        free(tdata);
                    }
                }
            }
        }

        for (uint32_t k = 0; k < n_raws; k++) free(raws[k].name);
        free(raws);
        free(nmap);

        qsort(children, n_children, sizeof(*children), ls_entry_cmp);
        ls_result_t *r = calloc(1, sizeof(*r));
        if (!r) { free(children); snapshot_free(snap); return set_error(ERR_NOMEM, "ls: result alloc"); }
        r->entries = children;
        r->count   = n_children;
        *out = r;
        snapshot_free(snap);
        return OK;
    }

    /* ── General path: recursive listing ─────────────────────────── */

    /* Build node_id → index hash map for O(1) node lookup */
    uint32_t nmap_cap = 1;
    while (nmap_cap < snap->node_count * 2) nmap_cap <<= 1;
    uint32_t nmap_mask = nmap_cap - 1;
    dr_slot_t *nmap = calloc(nmap_cap, sizeof(dr_slot_t));
    if (!nmap) { snapshot_free(snap); return set_error(ERR_NOMEM, "ls: nmap alloc"); }
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint64_t nid = snap->nodes[i].node_id;
        uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & nmap_mask;
        while (nmap[h].key != 0) h = (h + 1) & nmap_mask;
        nmap[h].key = nid;
        nmap[h].idx = i;
    }

    /* Build flat dirent list from the snapshot's binary dirent_data */
    ls_dirent_t *flat = calloc(snap->dirent_count + 1, sizeof(ls_dirent_t));
    if (!flat) { free(nmap); snapshot_free(snap); return set_error(ERR_NOMEM, "ls: alloc dirent list failed"); }
    uint32_t n_flat = 0;
    dr_slot_t *drmap = NULL;
    uint32_t drmap_mask = 0;

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;
    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;
        char *name = malloc(dr.name_len + 1);
        if (!name) { st = ERR_NOMEM; goto done; }
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;
        flat[n_flat].parent_id  = dr.parent_node;
        flat[n_flat].node_id    = dr.node_id;
        flat[n_flat].name       = name;
        flat[n_flat].full_path  = NULL;
        n_flat++;
    }

    /* Build dirent node_id → index hash map for O(1) parent lookup */
    uint32_t drmap_cap = 1;
    while (drmap_cap < n_flat * 2) drmap_cap <<= 1;
    drmap_mask = drmap_cap - 1;
    drmap = calloc(drmap_cap, sizeof(dr_slot_t));
    if (!drmap) { st = ERR_NOMEM; goto done; }
    for (uint32_t i = 0; i < n_flat; i++) {
        uint64_t nid = flat[i].node_id;
        uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & drmap_mask;
        while (drmap[h].key != 0) h = (h + 1) & drmap_mask;
        drmap[h].key = nid;
        drmap[h].idx = i;
    }

    /* Force full path computation for all entries (O(n) with hash map) */
    for (uint32_t i = 0; i < n_flat; i++)
        build_path(flat, drmap, drmap_mask, &flat[i]);

    /* If dir_path is non-empty, confirm it exists as a directory in the snapshot */
    if (norm[0] != '\0') {
        int found_dir = 0;
        for (uint32_t i = 0; i < n_flat; i++) {
            if (!flat[i].full_path || strcmp(flat[i].full_path, norm) != 0) continue;
            uint32_t ni = dr_lookup(nmap, nmap_mask, flat[i].node_id);
            if (ni != UINT32_MAX && snap->nodes[ni].type == NODE_TYPE_DIR)
                found_dir = 1;
            break;
        }
        if (!found_dir) {
            st = set_error(ERR_INVALID, "ls: '%s' is not a directory in snapshot %u",
                           norm, snap_id);
            goto done;
        }
    }

    /* Collect matching entries */
    ls_entry_t *children = malloc(n_flat * sizeof(ls_entry_t));
    if (!children) { st = ERR_NOMEM; goto done; }
    uint32_t n_children = 0;

    size_t norm_len = strlen(norm);

    for (uint32_t i = 0; i < n_flat; i++) {
        if (!flat[i].full_path) continue;

        const char *fp    = flat[i].full_path;
        const char *disp_name;

        if (norm[0] == '\0') {
            disp_name = fp;
        } else {
            if (strcmp(fp, norm) == 0) continue;
            if (strncmp(fp, norm, norm_len) != 0 || fp[norm_len] != '/') continue;
            disp_name = fp + norm_len + 1;
        }

        /* Find node via hash map — O(1) */
        uint32_t ni = dr_lookup(nmap, nmap_mask, flat[i].node_id);
        const node_t *nd = (ni != UINT32_MAX) ? &snap->nodes[ni] : NULL;
        if (!nd) continue;

        int type_ok = 1;
        if (type_filter) {
            switch (type_filter) {
                case 'f': type_ok = (nd->type == NODE_TYPE_REG || nd->type == NODE_TYPE_HARDLINK); break;
                case 'd': type_ok = (nd->type == NODE_TYPE_DIR); break;
                case 'l': type_ok = (nd->type == NODE_TYPE_SYMLINK); break;
                case 'p': type_ok = (nd->type == NODE_TYPE_FIFO); break;
                case 'c': type_ok = (nd->type == NODE_TYPE_CHR); break;
                case 'b': type_ok = (nd->type == NODE_TYPE_BLK); break;
                default: type_ok = 1; break;
            }
        }
        if (!type_ok) continue;
        if (name_glob && *name_glob && fnmatch(name_glob, disp_name, 0) != 0) continue;

        ls_entry_t *ce = &children[n_children++];
        ce->name = strdup(disp_name);  /* own copy for result lifetime */
        ce->node = *nd;
        ce->symlink_target[0] = '\0';

        /* Load symlink target */
        if (nd->type == NODE_TYPE_SYMLINK) {
            uint8_t zero[OBJECT_HASH_SIZE] = {0};
            if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void  *tdata = NULL;
                size_t tlen  = 0;
                if (object_load(repo, nd->content_hash, &tdata, &tlen, NULL) == OK) {
                    size_t copy = tlen < sizeof(ce->symlink_target) - 1
                                  ? tlen : sizeof(ce->symlink_target) - 1;
                    memcpy(ce->symlink_target, tdata, copy);
                    ce->symlink_target[copy] = '\0';
                    free(tdata);
                }
            }
        }
    }

    qsort(children, n_children, sizeof(*children), ls_entry_cmp);

    /* Build result */
    ls_result_t *r = calloc(1, sizeof(*r));
    if (!r) { free(children); st = ERR_NOMEM; goto done; }
    r->entries = children;
    r->count   = n_children;
    *out = r;
    st = OK;

done:
    for (uint32_t i = 0; i < n_flat; i++) {
        free(flat[i].name);
        free(flat[i].full_path);
    }
    free(flat);
    free(drmap);
    free(nmap);
    snapshot_free(snap);
    return st;
}

void ls_result_free(ls_result_t *r)
{
    if (!r) return;
    for (uint32_t i = 0; i < r->count; i++)
        free(r->entries[i].name);
    free(r->entries);
    free(r);
}

/* ------------------------------------------------------------------ */
/* Fast filename search                                                */
/* ------------------------------------------------------------------ */

/* Reuse dr_slot_t / dr_lookup from above for node_id → index maps */

/* Case-insensitive substring search (ASCII) */
static int ci_contains(const char *haystack, size_t hlen,
                        const char *needle,   size_t nlen)
{
    if (nlen == 0) return 1;
    if (hlen < nlen) return 0;
    for (size_t i = 0; i <= hlen - nlen; i++) {
        size_t j = 0;
        while (j < nlen) {
            char a = haystack[i + j], b = needle[j];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (b >= 'A' && b <= 'Z') b = (char)(b + 32);
            if (a != b) break;
            j++;
        }
        if (j == nlen) return 1;
    }
    return 0;
}

status_t snapshot_search(repo_t *repo, uint32_t snap_id,
                          const char *query, uint32_t max_results,
                          search_result_t **out)
{
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    /* Build node_id → index hash map */
    uint32_t map_cap = 1;
    while (map_cap < snap->node_count * 2) map_cap <<= 1;
    uint32_t mask = map_cap - 1;
    dr_slot_t *nmap = calloc(map_cap, sizeof(dr_slot_t));
    if (!nmap) { snapshot_free(snap); return set_error(ERR_NOMEM, "search: nmap alloc"); }
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint64_t nid = snap->nodes[i].node_id;
        uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & mask;
        while (nmap[h].key != 0) h = (h + 1) & mask;
        nmap[h].key = nid;
        nmap[h].idx = i;
    }

    /* Parse dirents into flat array */
    ls_dirent_t *flat = calloc(snap->dirent_count + 1, sizeof(ls_dirent_t));
    if (!flat) { free(nmap); snapshot_free(snap); return set_error(ERR_NOMEM, "search: flat alloc"); }
    uint32_t n_flat = 0;
    dr_slot_t *drmap = NULL;
    uint32_t drmap_mask = 0;

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;
    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;
        char *name = malloc(dr.name_len + 1);
        if (!name) { st = ERR_NOMEM; goto done; }
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;
        flat[n_flat].parent_id = dr.parent_node;
        flat[n_flat].node_id   = dr.node_id;
        flat[n_flat].name      = name;
        flat[n_flat].full_path = NULL;
        n_flat++;
    }

    /* Build dirent node_id → index map for O(1) parent lookup in build_path */
    uint32_t drmap_cap = 1;
    while (drmap_cap < n_flat * 2) drmap_cap <<= 1;
    drmap_mask = drmap_cap - 1;
    drmap = calloc(drmap_cap, sizeof(dr_slot_t));
    if (!drmap) { st = ERR_NOMEM; goto done; }
    for (uint32_t i = 0; i < n_flat; i++) {
        uint64_t nid = flat[i].node_id;
        uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & drmap_mask;
        while (drmap[h].key != 0) h = (h + 1) & drmap_mask;
        drmap[h].key = nid;
        drmap[h].idx = i;
    }

    /* Scan dirents: substring match on name, only build path for hits */
    size_t qlen = strlen(query);
    search_hit_t *hits = malloc((max_results + 1) * sizeof(search_hit_t));
    if (!hits) { st = ERR_NOMEM; goto done; }
    uint32_t n_hits = 0, n_total = 0;

    for (uint32_t i = 0; i < n_flat; i++) {
        size_t nlen = strlen(flat[i].name);
        if (!ci_contains(flat[i].name, nlen, query, qlen))
            continue;

        /* Match — look up node */
        uint32_t ni = dr_lookup(nmap, mask, flat[i].node_id);
        if (ni == UINT32_MAX) continue;

        n_total++;
        if (n_hits >= max_results) continue;

        /* Build path only for this hit */
        char *fp = build_path(flat, drmap, drmap_mask, &flat[i]);
        if (!fp) continue;

        hits[n_hits].path = strdup(fp);
        hits[n_hits].node = snap->nodes[ni];
        n_hits++;
    }

    {
        search_result_t *r = calloc(1, sizeof(*r));
        if (!r) { st = ERR_NOMEM; goto done_hits; }
        r->hits  = hits;
        r->count = n_hits;
        r->total = n_total;
        *out = r;
        hits = NULL;  /* ownership transferred */
        st = OK;
    }

done_hits:
    if (hits) {
        for (uint32_t i = 0; i < n_hits; i++) free(hits[i].path);
        free(hits);
    }
done:
    for (uint32_t i = 0; i < n_flat; i++) {
        free(flat[i].name);
        free(flat[i].full_path);
    }
    free(flat);
    free(drmap);
    free(nmap);
    snapshot_free(snap);
    return st;
}

void search_result_free(search_result_t *r)
{
    if (!r) return;
    for (uint32_t i = 0; i < r->count; i++)
        free(r->hits[i].path);
    free(r->hits);
    free(r);
}

/* ------------------------------------------------------------------ */
/* Batch multi-snapshot search                                         */
/*                                                                     */
/* When searching across many snapshots, reuse the dirent parse +      */
/* drmap if consecutive snapshots have identical dirent data.           */
/* ------------------------------------------------------------------ */

status_t snapshot_search_multi(repo_t *repo, const uint32_t *snap_ids,
                                uint32_t n_snaps, const char *query,
                                uint32_t max_results,
                                void (*hit_cb)(uint32_t snap_id,
                                               const search_hit_t *hit,
                                               void *ctx),
                                void *ctx)
{
    size_t qlen = strlen(query);
    uint32_t total_hits = 0;

    /* Cached state from previous snapshot */
    ls_dirent_t *flat = NULL;
    uint32_t     n_flat = 0;
    dr_slot_t   *drmap = NULL;
    uint32_t     drmap_mask = 0;
    uint8_t     *prev_dirent_data = NULL;
    size_t       prev_dirent_len  = 0;

    for (uint32_t si = 0; si < n_snaps && total_hits < max_results; si++) {
        snapshot_t *snap = NULL;
        status_t st = snapshot_load(repo, snap_ids[si], &snap);
        if (st != OK) { err_clear(); continue; }

        /* Check if we can reuse the cached dirent parse */
        int reuse = (flat != NULL &&
                     snap->dirent_data_len == prev_dirent_len &&
                     snap->dirent_data_len > 0 &&
                     memcmp(snap->dirent_data, prev_dirent_data,
                            snap->dirent_data_len) == 0);

        if (!reuse) {
            /* Free old cached state */
            if (flat) {
                for (uint32_t i = 0; i < n_flat; i++) {
                    free(flat[i].name);
                    free(flat[i].full_path);
                }
                free(flat);
                free(drmap);
            }
            free(prev_dirent_data);
            prev_dirent_data = NULL;
            n_flat = 0;

            /* Parse dirents */
            flat = calloc(snap->dirent_count + 1, sizeof(ls_dirent_t));
            if (!flat) { snapshot_free(snap); return set_error(ERR_NOMEM, "search_multi: flat alloc"); }
            const uint8_t *p   = snap->dirent_data;
            const uint8_t *end = p + snap->dirent_data_len;
            while (p < end && n_flat < snap->dirent_count) {
                if (p + sizeof(dirent_rec_t) > end) break;
                dirent_rec_t dr;
                memcpy(&dr, p, sizeof(dr));
                p += sizeof(dr);
                if (p + dr.name_len > end) break;
                char *name = malloc(dr.name_len + 1);
                if (!name) { snapshot_free(snap); goto cleanup; }
                memcpy(name, p, dr.name_len);
                name[dr.name_len] = '\0';
                p += dr.name_len;
                flat[n_flat].parent_id = dr.parent_node;
                flat[n_flat].node_id   = dr.node_id;
                flat[n_flat].name      = name;
                flat[n_flat].full_path = NULL;
                n_flat++;
            }

            /* Build drmap */
            uint32_t drmap_cap = 1;
            while (drmap_cap < n_flat * 2) drmap_cap <<= 1;
            drmap_mask = drmap_cap - 1;
            drmap = calloc(drmap_cap, sizeof(dr_slot_t));
            if (!drmap) { snapshot_free(snap); goto cleanup; }
            for (uint32_t i = 0; i < n_flat; i++) {
                uint64_t nid = flat[i].node_id;
                uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & drmap_mask;
                while (drmap[h].key != 0) h = (h + 1) & drmap_mask;
                drmap[h].key = nid;
                drmap[h].idx = i;
            }

            /* Save dirent data for comparison */
            if (snap->dirent_data_len > 0) {
                prev_dirent_data = malloc(snap->dirent_data_len);
                if (prev_dirent_data) {
                    memcpy(prev_dirent_data, snap->dirent_data, snap->dirent_data_len);
                    prev_dirent_len = snap->dirent_data_len;
                }
            }
        } else {
            /* Reusing cached flat+drmap — clear full_path cache for fresh builds */
            for (uint32_t i = 0; i < n_flat; i++) {
                free(flat[i].full_path);
                flat[i].full_path = NULL;
            }
        }

        /* Build node_id → index map for this snapshot's nodes */
        uint32_t nmap_cap = 1;
        while (nmap_cap < snap->node_count * 2) nmap_cap <<= 1;
        uint32_t nmap_mask = nmap_cap - 1;
        dr_slot_t *nmap = calloc(nmap_cap, sizeof(dr_slot_t));
        if (!nmap) { snapshot_free(snap); goto cleanup; }
        for (uint32_t i = 0; i < snap->node_count; i++) {
            uint64_t nid = snap->nodes[i].node_id;
            uint32_t h = (uint32_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & nmap_mask;
            while (nmap[h].key != 0) h = (h + 1) & nmap_mask;
            nmap[h].key = nid;
            nmap[h].idx = i;
        }

        /* Scan dirents for matches */
        for (uint32_t i = 0; i < n_flat && total_hits < max_results; i++) {
            size_t nlen = strlen(flat[i].name);
            if (!ci_contains(flat[i].name, nlen, query, qlen))
                continue;
            uint32_t ni = dr_lookup(nmap, nmap_mask, flat[i].node_id);
            if (ni == UINT32_MAX) continue;

            char *fp = build_path(flat, drmap, drmap_mask, &flat[i]);
            if (!fp) continue;

            search_hit_t hit;
            hit.path = fp;   /* borrowed — callback must copy if needed */
            hit.node = snap->nodes[ni];
            hit_cb(snap_ids[si], &hit, ctx);
            total_hits++;
        }

        free(nmap);
        snapshot_free(snap);
    }

cleanup:
    if (flat) {
        for (uint32_t i = 0; i < n_flat; i++) {
            free(flat[i].name);
            free(flat[i].full_path);
        }
        free(flat);
    }
    free(drmap);
    free(prev_dirent_data);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Formatting helpers (CLI presentation only)                          */
/* ------------------------------------------------------------------ */

static char type_char(uint8_t node_type) {
    switch (node_type) {
        case NODE_TYPE_DIR:      return 'd';
        case NODE_TYPE_SYMLINK:  return 'l';
        case NODE_TYPE_FIFO:     return 'p';
        case NODE_TYPE_CHR:      return 'c';
        case NODE_TYPE_BLK:      return 'b';
        default:                 return '-';
    }
}

static void mode_str(uint8_t node_type, uint32_t mode, char out[11]) {
    out[0] = type_char(node_type);
    out[1] = (mode & 0400) ? 'r' : '-';
    out[2] = (mode & 0200) ? 'w' : '-';
    out[3] = (mode & 04000) ? 's' : (mode & 0100) ? 'x' : '-';
    out[4] = (mode & 0040) ? 'r' : '-';
    out[5] = (mode & 0020) ? 'w' : '-';
    out[6] = (mode & 02000) ? 's' : (mode & 0010) ? 'x' : '-';
    out[7] = (mode & 0004) ? 'r' : '-';
    out[8] = (mode & 0002) ? 'w' : '-';
    out[9] = (mode & 01000) ? 't' : (mode & 0001) ? 'x' : '-';
    out[10] = '\0';
}

static void fmt_time(uint64_t sec, char out[20]) {
    time_t t = (time_t)sec;
    struct tm *tm = localtime(&t);
    if (tm) strftime(out, 20, "%Y-%m-%d %H:%M", tm);
    else    snprintf(out, 20, "?");
}

static void fmt_size_human(uint64_t n, char out[16]) {
    if (n >= (uint64_t)1024 * 1024 * 1024 * 1024)
        snprintf(out, 16, "%.1fT", (double)n / (1024.0 * 1024.0 * 1024.0 * 1024.0));
    else if (n >= (uint64_t)1024 * 1024 * 1024)
        snprintf(out, 16, "%.1fG", (double)n / (1024.0 * 1024.0 * 1024.0));
    else if (n >= (uint64_t)1024 * 1024)
        snprintf(out, 16, "%.1fM", (double)n / (1024.0 * 1024.0));
    else if (n >= 1024)
        snprintf(out, 16, "%.1fK", (double)n / 1024.0);
    else
        snprintf(out, 16, "%lluB", (unsigned long long)n);
}

static const char *name_color_for_node(const node_t *nd) {
    if (!nd) return NULL;
    switch (nd->type) {
        case NODE_TYPE_DIR:     return "\x1b[1;34m";
        case NODE_TYPE_SYMLINK: return "\x1b[1;36m";
        case NODE_TYPE_FIFO:    return "\x1b[33m";
        case NODE_TYPE_CHR:
        case NODE_TYPE_BLK:     return "\x1b[1;33m";
        case NODE_TYPE_REG:
        case NODE_TYPE_HARDLINK:
            if (nd->mode & 0111) return "\x1b[1;32m";
            return NULL;
        default:
            return NULL;
    }
}

/* ------------------------------------------------------------------ */
/* CLI wrapper: prints to stdout (unchanged behavior)                  */
/* ------------------------------------------------------------------ */

status_t snapshot_ls(repo_t *repo, uint32_t snap_id, const char *dir_path,
                     int recursive, char type_filter, const char *name_glob)
{
    ls_result_t *r = NULL;
    status_t st = snapshot_ls_collect(repo, snap_id, dir_path, recursive,
                                      type_filter, name_glob, &r);
    if (st != OK) return st;

    /* Normalise for header display */
    char norm[4096] = "";
    if (dir_path && strcmp(dir_path, ".") != 0 && strcmp(dir_path, "/") != 0) {
        const char *s = dir_path;
        while (*s == '/') s++;
        size_t len = strlen(s);
        while (len > 0 && s[len - 1] == '/') len--;
        if (len < sizeof(norm)) { memcpy(norm, s, len); norm[len] = '\0'; }
    }

    int use_color = isatty(STDOUT_FILENO) ? 1 : 0;
    const char *no_color = getenv("NO_COLOR");
    if (no_color && *no_color) use_color = 0;

    if (norm[0] == '\0') printf("snapshot %u  /\n", snap_id);
    else                  printf("snapshot %u  /%s\n", snap_id, norm);
    if (recursive) printf("(recursive)\n");

    if (r->count == 0) {
        printf("(empty)\n");
        ls_result_free(r);
        return OK;
    }

    printf("MODE        UID   GID          SIZE  MTIME             NAME\n");
    printf("----------  ----  ----  ------------  ----------------  ----\n");

    for (uint32_t i = 0; i < r->count; i++) {
        const ls_entry_t *ce = &r->entries[i];
        char mstr[11];
        mode_str(ce->node.type, ce->node.mode, mstr);

        char tstr[20];
        fmt_time(ce->node.mtime_sec, tstr);

        char sizebuf[16];
        fmt_size_human(ce->node.size, sizebuf);

        const char *color_on = "";
        const char *color_off = "";
        if (use_color) {
            const char *c = name_color_for_node(&ce->node);
            if (c) {
                color_on = c;
                color_off = "\x1b[0m";
            }
        }

        if (ce->node.type == NODE_TYPE_SYMLINK) {
            printf("%s  %4u  %4u  %12s  %s  %s%s%s -> %s\n",
                   mstr, ce->node.uid, ce->node.gid,
                   sizebuf, tstr, color_on, ce->name, color_off, ce->symlink_target);
        } else if (ce->node.type == NODE_TYPE_CHR ||
                   ce->node.type == NODE_TYPE_BLK) {
            printf("%s  %4u  %4u  %5u,%5u  %s  %s%s%s\n",
                   mstr, ce->node.uid, ce->node.gid,
                   ce->node.device.major, ce->node.device.minor,
                   tstr, color_on, ce->name, color_off);
        } else {
            printf("%s  %4u  %4u  %12s  %s  %s%s%s\n",
                   mstr, ce->node.uid, ce->node.gid,
                   sizebuf, tstr, color_on, ce->name, color_off);
        }
    }

    ls_result_free(r);
    return OK;
}
