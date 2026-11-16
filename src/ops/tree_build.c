#define _POSIX_C_SOURCE 200809L

#include "tree_build.h"

#include "manifest.h"

#include <stdlib.h>
#include <string.h>

/* The last path component; what the entry is called inside its parent. */
static const char *basename_of(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

/*
 * node_id to scan index.
 *
 * This was a linear scan of every entry. It is called once per step of every
 * parent chain, so on a tree with a quarter of a million entries and
 * twenty-five thousand directories the cost compounded into something that
 * would not have finished this decade. Open addressing, built once per run,
 * keeps it O(1) and the whole ordering pass linear, which is what design
 * constraint 3 asks for.
 */
typedef struct {
    uint64_t *keys;     /* node_id, 0 means empty */
    uint32_t *vals;     /* scan index */
    uint32_t  mask;
} idmap_t;

static void idmap_free(idmap_t *m)
{
    free(m->keys);
    free(m->vals);
    m->keys = NULL;
    m->vals = NULL;
}

static int idmap_build(idmap_t *m, const scan_result_t *scan)
{
    uint32_t cap = 16;
    while (cap < scan->count * 2) cap <<= 1;
    m->keys = calloc(cap, sizeof(*m->keys));
    m->vals = calloc(cap, sizeof(*m->vals));
    if (!m->keys || !m->vals) { idmap_free(m); return -1; }
    m->mask = cap - 1;

    for (uint32_t i = 0; i < scan->count; i++) {
        uint64_t k = scan->entries[i].node.node_id;
        if (k == 0) continue;   /* 0 is the empty marker and never a node id */
        uint32_t h = (uint32_t)((k * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
        while (m->keys[h] != 0 && m->keys[h] != k) h = (h + 1) & m->mask;
        m->keys[h] = k;
        m->vals[h] = i;
    }
    return 0;
}

static uint32_t idmap_get(const idmap_t *m, uint64_t node_id)
{
    if (node_id == 0) return UINT32_MAX;
    uint32_t h = (uint32_t)((node_id * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
    while (m->keys[h] != 0) {
        if (m->keys[h] == node_id) return m->vals[h];
        h = (h + 1) & m->mask;
    }
    return UINT32_MAX;
}

/*
 * Depth of every entry, in one pass.
 *
 * Each chain walk memoises what it learns, so an entry is resolved once and
 * every descendant that passes through it afterwards stops there. Total work
 * is proportional to the number of entries rather than to entries times
 * depth, and nothing recomputes a depth it has already established.
 *
 * The visiting marker guards against a parent cycle. That should not be
 * possible from a filesystem scan, but a cycle here would be an unkillable
 * loop rather than a wrong answer, and this is the pass that just proved how
 * badly that ends.
 */
#define DEPTH_UNKNOWN  UINT32_MAX
#define DEPTH_VISITING (UINT32_MAX - 1)

static status_t compute_depths(const scan_result_t *scan, const idmap_t *map,
                               uint32_t **out_depth)
{
    uint32_t count = scan->count;
    uint32_t *depth = malloc((size_t)count * sizeof(*depth));
    if (!depth) return set_error(ERR_NOMEM, "build_trees: depth alloc");
    for (uint32_t i = 0; i < count; i++) depth[i] = DEPTH_UNKNOWN;

    uint32_t *stack = malloc((size_t)count * sizeof(*stack));
    if (!stack) { free(depth); return set_error(ERR_NOMEM, "build_trees: depth stack"); }

    for (uint32_t i = 0; i < count; i++) {
        if (depth[i] != DEPTH_UNKNOWN) continue;

        uint32_t n_stack = 0;
        uint32_t cur = i;
        uint32_t base = 0;   /* depth to give the topmost entry on the stack */

        /*
         * Walk up until an ancestor whose depth is already known, or a root.
         * An anchored chain hangs one level below that ancestor; an unanchored
         * one is rooted here, at depth zero.
         */
        for (;;) {
            if (depth[cur] == DEPTH_VISITING) { base = 0; break; }  /* cycle */
            if (depth[cur] != DEPTH_UNKNOWN) { base = depth[cur] + 1; break; }

            depth[cur] = DEPTH_VISITING;
            stack[n_stack++] = cur;

            uint64_t parent = scan->entries[cur].parent_node_id;
            if (parent == 0) { base = 0; break; }
            uint32_t pi = idmap_get(map, parent);
            if (pi == UINT32_MAX) { base = 0; break; }
            cur = pi;
        }

        /* Top of the stack is shallowest, so unwind assigning increasing
         * depth. Everything touched is now resolved for later chains. */
        uint32_t d = base;
        while (n_stack > 0) depth[stack[--n_stack]] = d++;
    }

    free(stack);
    *out_depth = depth;
    return OK;
}

/*
 * Directories in build order, deepest first.
 *
 * A counting sort by descending depth, stable in scan order within a depth.
 * This was an insertion sort calling a parent-chain walk on every
 * comparison, which is fine at a few hundred directories and fatal at
 * twenty-five thousand: quadratic in directories, each comparison linear in
 * entries. Depths are small integers, so ordering by them is a pair of
 * linear passes.
 */
static status_t order_dirs(const scan_result_t *scan, const uint32_t *depth,
                           uint32_t **out_dirs, uint32_t *out_ndirs)
{
    *out_dirs = NULL;
    *out_ndirs = 0;

    uint32_t ndirs = 0;
    uint32_t maxd = 0;
    for (uint32_t i = 0; i < scan->count; i++) {
        if (scan->entries[i].node.type != NODE_TYPE_DIR) continue;
        ndirs++;
        if (depth[i] > maxd) maxd = depth[i];
    }
    if (!ndirs) return OK;

    uint32_t *counts = calloc((size_t)maxd + 2, sizeof(*counts));
    uint32_t *sorted = malloc((size_t)ndirs * sizeof(*sorted));
    if (!counts || !sorted) {
        free(counts); free(sorted);
        return set_error(ERR_NOMEM, "build_trees: ordering");
    }
    for (uint32_t i = 0; i < scan->count; i++)
        if (scan->entries[i].node.type == NODE_TYPE_DIR) counts[depth[i]]++;
    /* Deepest first: offsets accumulate from the high end. */
    uint32_t running = 0;
    for (uint32_t d = maxd + 1; d-- > 0; ) {
        uint32_t c = counts[d];
        counts[d] = running;
        running += c;
    }
    for (uint32_t i = 0; i < scan->count; i++)
        if (scan->entries[i].node.type == NODE_TYPE_DIR) sorted[counts[depth[i]]++] = i;

    free(counts);
    *out_dirs = sorted;
    *out_ndirs = ndirs;
    return OK;
}

/*
 * Every entry grouped under its parent, in one pass (OWN-43).
 *
 * Serialising a directory used to scan the whole scan for entries whose
 * parent was that directory, once to count them and once to copy them:
 * O(directories x entries) for the phase. The ordering above had already
 * been made linear after a 223,000-file backup never finished, and this was
 * the same shape one step later; at 25,000 directories by 10 files it is
 * seven billion comparisons.
 *
 * Instead each entry is bucketed by its parent's scan index, compressed
 * sparse row style: kids[off[b] .. off[b + 1]) are bucket b's children, in
 * ascending scan order, which is the order the rescan visited them in.
 * Bucket `count` is the synthetic root, for entries whose parent is 0. An
 * entry whose parent id names nothing in the scan goes in no bucket, which
 * is the old behaviour: no directory had that node id, so no rescan matched
 * it. A bucket under a non-directory is built and never read, as before.
 */
typedef struct {
    uint32_t *off;      /* count + 2 */
    uint32_t *kids;     /* count */
    uint32_t  max_bucket;
} children_t;

static void children_free(children_t *c)
{
    free(c->off);
    free(c->kids);
    c->off = NULL;
    c->kids = NULL;
}

#define NO_BUCKET UINT32_MAX

/* Entry i is to be left out of every tree (tree_build_ops_t.skipped). */
static int is_skipped(const tree_build_ops_t *ops, uint32_t i)
{
    if (!ops || !ops->skipped) return 0;
    return *(const int *)((const uint8_t *)ops->skipped + (size_t)i * ops->stride) != 0;
}

static status_t children_build(const scan_result_t *scan, const idmap_t *map,
                               const tree_build_ops_t *ops, children_t *c)
{
    uint32_t count = scan->count;
    memset(c, 0, sizeof(*c));
    uint32_t *bucket = malloc(((size_t)count + 1) * sizeof(*bucket));
    c->off  = calloc((size_t)count + 2, sizeof(*c->off));
    c->kids = malloc(((size_t)count + 1) * sizeof(*c->kids));
    if (!bucket || !c->off || !c->kids) {
        free(bucket);
        children_free(c);
        return set_error(ERR_NOMEM, "build_trees: child index");
    }

    for (uint32_t i = 0; i < count; i++) {
        uint64_t parent = scan->entries[i].parent_node_id;
        uint32_t b = parent == 0 ? count : idmap_get(map, parent);
        bucket[i] = (b == UINT32_MAX || is_skipped(ops, i)) ? NO_BUCKET : b;
        if (bucket[i] != NO_BUCKET) c->off[bucket[i] + 1]++;
    }
    for (uint32_t b = 0; b <= count; b++) {
        uint32_t n = c->off[b + 1];
        if (n > c->max_bucket) c->max_bucket = n;
        c->off[b + 1] = c->off[b] + n;
    }
    /* off[b] doubles as bucket b's fill cursor, and ends up at off[b + 1].
     * Shifting back afterwards restores the starts; scanning in ascending
     * order keeps each bucket in scan order. */
    for (uint32_t i = 0; i < count; i++)
        if (bucket[i] != NO_BUCKET) c->kids[c->off[bucket[i]]++] = i;
    for (uint32_t b = count + 1; b > 0; b--) c->off[b] = c->off[b - 1];
    c->off[0] = 0;

    free(bucket);
    return OK;
}

/*
 * Serialise one directory from its immediate children. Called only after
 * every child directory already has its hash, which the depth ordering
 * guarantees. entries has room for the largest bucket.
 */
static status_t build_one_tree(const scan_result_t *scan, const tree_build_ops_t *ops,
                               const children_t *c, manifest_entry_t *entries,
                               uint32_t index, uint8_t out_hash[OBJECT_HASH_SIZE],
                               tree_build_stats_t *stats)
{
    uint32_t b = index == TREE_BUILD_ROOT ? scan->count : index;
    uint32_t first = c->off[b];
    uint32_t n = c->off[b + 1] - first;
    if (n) memset(entries, 0, (size_t)n * sizeof(*entries));

    for (uint32_t k = 0; k < n; k++) {
        uint32_t i = c->kids[first + k];
        const scan_entry_t *e = &scan->entries[i];
        manifest_entry_t *m = &entries[k];

        m->name           = basename_of(e->path);
        m->name_len       = (uint16_t)strlen(m->name);
        m->type           = e->node.type;
        m->mode           = e->node.mode;
        m->uid            = e->node.uid;
        m->gid            = e->node.gid;
        m->size           = e->node.size;
        m->mtime_sec      = e->node.mtime_sec;
        m->mtime_nsec     = e->node.mtime_nsec;
        m->inode_identity = e->node.inode_identity;
        /* node_t overlays device and symlink in a union, so this is only a
         * device pair for the two types that have one. Copying it
         * unconditionally would write a symlink's target_len into dev_major
         * and change tree hashes for every symlink in the repository. */
        if (e->node.type == NODE_TYPE_CHR || e->node.type == NODE_TYPE_BLK) {
            m->dev_major = e->node.device.major;
            m->dev_minor = e->node.device.minor;
        }
        memcpy(m->xattr_hash, e->node.xattr_hash, OBJECT_HASH_SIZE);
        memcpy(m->acl_hash,   e->node.acl_hash,   OBJECT_HASH_SIZE);

        if (e->node.type == NODE_TYPE_DIR)
            memcpy(m->target_hash, ops->hashes + (size_t)i * ops->stride,
                   OBJECT_HASH_SIZE);
        else
            memcpy(m->target_hash, e->node.content_hash, OBJECT_HASH_SIZE);

        if (e->symlink_target) {
            m->link_target     = e->symlink_target;
            m->link_target_len = (uint16_t)strlen(e->symlink_target);
        }
    }
    stats->entries_visited += n;
    stats->trees++;

    return ops->put(ops->ctx, index, entries, n, out_hash);
}

status_t tree_build(const scan_result_t *scan, const tree_build_ops_t *ops,
                    uint8_t root_hash[OBJECT_HASH_SIZE],
                    tree_build_stats_t *stats)
{
    if (!scan || !ops || !ops->step || !ops->put || !root_hash ||
        (scan->count && (!scan->entries || !ops->hashes || ops->stride < OBJECT_HASH_SIZE)))
        return set_error(ERR_INVALID, "tree_build: bad arguments");

    tree_build_stats_t local = { 0, 0 };
    if (!stats) stats = &local;
    memset(stats, 0, sizeof(*stats));

    idmap_t map = { NULL, NULL, 0 };
    if (idmap_build(&map, scan) != 0)
        return set_error(ERR_NOMEM, "build_trees: id map");

    uint32_t *depth = NULL;
    children_t c = { NULL, NULL, 0 };
    status_t st = compute_depths(scan, &map, &depth);
    if (st == OK) st = children_build(scan, &map, ops, &c);
    idmap_free(&map);

    uint32_t *dirs = NULL;
    uint32_t ndirs = 0;
    if (st == OK) st = order_dirs(scan, depth, &dirs, &ndirs);
    free(depth);

    manifest_entry_t *entries = NULL;
    if (st == OK && c.max_bucket) {
        entries = malloc((size_t)c.max_bucket * sizeof(*entries));
        if (!entries) st = set_error(ERR_NOMEM, "build_trees: entries");
    }

    for (uint32_t a = 0; st == OK && a < ndirs; a++) {
        st = ops->step(ops->ctx, a, ndirs);
        if (st != OK) break;
        uint32_t i = dirs[a];
        st = build_one_tree(scan, ops, &c, entries, i,
                            ops->hashes + (size_t)i * ops->stride, stats);
    }

    /* The synthetic root ties the source paths together into one tree. */
    if (st == OK) st = ops->step(ops->ctx, ndirs, ndirs);
    if (st == OK)
        st = build_one_tree(scan, ops, &c, entries, TREE_BUILD_ROOT, root_hash, stats);

    free(entries);
    free(dirs);
    children_free(&c);
    return st;
}
