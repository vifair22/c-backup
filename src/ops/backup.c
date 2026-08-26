#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "backup.h"

#include "file_cache.h"
#include "manifest.h"
#include "object.h"
#include "scan.h"
#include "store_index.h"
#include "db_txn.h"
#include "phase_timer.h"

#include <cutils/log.h>

#include <openssl/sha.h>

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/*
 * The pipeline in one paragraph: scan produces a flat list of entries with
 * parent pointers; content is stored first so every file has a hash; trees
 * are then assembled bottom-up, because a directory's hash depends on its
 * children's; finally the root tree hash goes into a snapshot record on disk
 * and the whole thing is indexed into the database.
 *
 * Bottom-up is not a preference. A directory cannot be serialised until every
 * child hash is known, so the only workable order is deepest-first.
 */

typedef struct {
    uint64_t node_id;         /* scan-assigned, never written into a tree */
    uint64_t parent_id;
    uint32_t scan_index;
    uint8_t  tree_hash[OBJECT_HASH_SIZE];   /* directories only */
    int      hash_ready;
} node_link_t;

/*
 * Hashes already ensured present this run. Open addressing over full 32-byte
 * keys; the all-zero hash doubles as the empty marker, which is safe because
 * SHA-256 of real data is never zero and a zero hash means "no blob".
 */
typedef struct {
    uint8_t *keys;      /* capacity * OBJECT_HASH_SIZE */
    uint32_t mask;
    uint32_t cnt;
} hashset_t;

typedef struct {
    store_t       *store;
    cutils_db_t   *db;
    int64_t        repo_id;
    scan_result_t *scan;
    node_link_t   *links;
    uint32_t       count;
    backup_result_t *res;
    const backup_opts_t *opts;
    /* Stamped into every cache row this run touches, so entries the scan did
     * not see can be swept afterwards. */
    int64_t        cache_stamp;
    struct timespec last_report;
    /* Metadata blobs (xattr sets, ACL texts) already stored this run. */
    hashset_t      meta_seen;
    /* Per-component attribution for the whole run. */
    phase_timers_t *timers;
} pipeline_t;

static void hashset_free(hashset_t *s)
{
    free(s->keys);
    s->keys = NULL;
    s->mask = 0;
    s->cnt = 0;
}

static uint32_t hashset_slot(const hashset_t *s, const uint8_t key[OBJECT_HASH_SIZE])
{
    uint32_t h;
    memcpy(&h, key, sizeof(h));   /* SHA-256 output: any 4 bytes are uniform */
    h &= s->mask;
    while (memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE,
                  (uint8_t[OBJECT_HASH_SIZE]){ 0 }, OBJECT_HASH_SIZE) != 0 &&
           memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE) != 0)
        h = (h + 1) & s->mask;
    return h;
}

static int hashset_contains(const hashset_t *s, const uint8_t key[OBJECT_HASH_SIZE])
{
    if (!s->keys) return 0;
    uint32_t h = hashset_slot(s, key);
    return memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE) == 0;
}

static void hashset_add(hashset_t *s, const uint8_t key[OBJECT_HASH_SIZE])
{
    if (!s->keys) {
        s->keys = calloc(64, OBJECT_HASH_SIZE);
        if (!s->keys) return;   /* memo only; a miss costs a lookup, not data */
        s->mask = 63;
    }
    if ((s->cnt + 1) * 10 >= (s->mask + 1) * 7) {
        uint32_t ncap = (s->mask + 1) * 2;
        uint8_t *nk = calloc(ncap, OBJECT_HASH_SIZE);
        if (!nk) return;
        hashset_t ns = { nk, ncap - 1, 0 };
        for (uint32_t i = 0; i <= s->mask; i++) {
            const uint8_t *k = s->keys + (size_t)i * OBJECT_HASH_SIZE;
            if (memcmp(k, (uint8_t[OBJECT_HASH_SIZE]){ 0 }, OBJECT_HASH_SIZE) == 0)
                continue;
            memcpy(ns.keys + (size_t)hashset_slot(&ns, k) * OBJECT_HASH_SIZE,
                   k, OBJECT_HASH_SIZE);
            ns.cnt++;
        }
        free(s->keys);
        *s = ns;
    }
    uint32_t h = hashset_slot(s, key);
    if (memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE) == 0)
        return;
    memcpy(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE);
    s->cnt++;
}

static int cancelled(const pipeline_t *p)
{
    return p->opts->cancel && *p->opts->cancel;
}

/*
 * Progress is throttled, not reported per entry.
 *
 * Every tick is a database write, and a write here costs tens of
 * milliseconds; on a tree of a few thousand files that was more time than the
 * work being reported on. Nobody can read updates faster than a few a second
 * anyway. The first and last of a phase always go through so a phase never
 * appears stuck at its starting count or stops short of its total.
 */
#define PROGRESS_MIN_INTERVAL_MS 250

static void report(pipeline_t *p, const char *phase, uint64_t done, uint64_t total)
{
    if (!p->opts->progress_cb) return;

    if (done > 0 && done + 1 < total) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        int64_t ms = (int64_t)(now.tv_sec - p->last_report.tv_sec) * 1000
                   + (int64_t)(now.tv_nsec - p->last_report.tv_nsec) / 1000000;
        if (ms < PROGRESS_MIN_INTERVAL_MS) return;
        p->last_report = now;
    }

    p->opts->progress_cb(phase, done, total, p->opts->progress_ctx);
}

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

static int idmap_build(idmap_t *m, const node_link_t *links, uint32_t count)
{
    uint32_t cap = 16;
    while (cap < count * 2) cap <<= 1;
    m->keys = calloc(cap, sizeof(*m->keys));
    m->vals = calloc(cap, sizeof(*m->vals));
    if (!m->keys || !m->vals) { idmap_free(m); return -1; }
    m->mask = cap - 1;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t k = links[i].node_id;
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

/* ------------------------------------------------------------------ */
/* Phase 1: content                                                    */
/* ------------------------------------------------------------------ */

static int scan_path_cmp(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/*
 * Store one metadata blob (an xattr set or ACL text) as its own object and
 * return its hash for the manifest entry. No blob means a zero hash, which is
 * how "none" is spelled everywhere else.
 *
 * The memo matters more than it looks. The same blob recurs across most of a
 * tree - one SELinux label set can cover every file on a machine - and once
 * packing has run, each object_store_ex existence check on it is a resolver
 * query. A quarter-million of those per incremental is the tight-loop-on-the-
 * shared-DB-handle pattern that has starved the daemon before; hashing the
 * few bytes locally and remembering the answer makes repeats free.
 */
static status_t store_meta_blob(pipeline_t *p, uint8_t obj_type,
                                const uint8_t *data, size_t len,
                                uint8_t out_hash[OBJECT_HASH_SIZE])
{
    if (!data || len == 0) {
        memset(out_hash, 0, OBJECT_HASH_SIZE);
        return OK;
    }

    SHA256(data, len, out_hash);
    if (hashset_contains(&p->meta_seen, out_hash)) return OK;

    int is_new = 0;
    uint64_t phys = 0;
    status_t st = object_store_ex(p->store, obj_type, data, len, out_hash,
                                  &is_new, &phys);
    if (st != OK) return st;
    if (is_new) {
        p->res->objects_new++;
        st = store_index_put_loose(p->db, p->repo_id, out_hash, obj_type,
                                   phys, len);
        if (st != OK) return st;
    }
    hashset_add(&p->meta_seen, out_hash);
    return OK;
}

static status_t store_content(pipeline_t *p)
{
    /*
     * Cache writes are batched. One transaction per file would put an fsync
     * between every entry, and on a tree of any size that costs more than the
     * hashing it exists to avoid.
     */
    int in_txn = 0;
    uint32_t since_commit = 0;
    #define CACHE_BATCH 2000

    for (uint32_t i = 0; i < p->count; i++) {
        if (cancelled(p)) {
            if (in_txn && db_txn_commit(p->db) != OK)
                log_warn("backup: cache commit failed on cancel");
            return set_error(ERR_BUSY, "backup cancelled");
        }

        scan_entry_t *e = &p->scan->entries[i];
        report(p, "store", i, p->count);

        /* Every node type can carry xattrs and ACLs, so their blobs are
         * stored before the type branches below skip non-regular entries.
         * The hashes land in the node and travel into the manifest. */
        uint64_t _tm = phase_now();
        status_t mst = store_meta_blob(p, OBJECT_TYPE_XATTR,
                                       e->xattr_data, e->xattr_len,
                                       e->node.xattr_hash);
        if (mst == OK)
            mst = store_meta_blob(p, OBJECT_TYPE_ACL,
                                  e->acl_data, e->acl_len,
                                  e->node.acl_hash);
        phase_add(p->timers, PHASE_META_BLOB, _tm);
        if (mst != OK) {
            if (in_txn && db_txn_commit(p->db) != OK)
                log_warn("backup: cache commit failed");
            return mst;
        }

        if (e->node.type == NODE_TYPE_DIR) { p->res->dirs++; continue; }
        if (e->node.type != NODE_TYPE_REG) continue;

        /*
         * A transaction spans a batch of entries whether or not they write.
         *
         * Reads are far cheaper inside one: a standalone SELECT in WAL mode
         * re-establishes a snapshot every time, measured at 30ms a lookup
         * against 0.7ms batched. But it has to be committed periodically
         * rather than held for the whole scan, because an open transaction
         * blocks WAL checkpointing and the lookups then slow down as the WAL
         * grows - 17ms a lookup by the end. Both extremes were measured; this
         * is the middle.
         */
        if (!in_txn) {
            if (db_txn_begin(p->db) == OK) in_txn = 1;
            else err_clear();
            since_commit = 0;
        }
        if (++since_commit >= CACHE_BATCH) {
            if (in_txn && db_txn_commit(p->db) != OK)
                log_warn("backup: cache commit failed");
            in_txn = 0;
        }

        /*
         * The file is only opened when the cache cannot vouch for it. This is
         * the difference between an incremental costing a full read of the
         * tree and costing a directory walk: content addressing has no way to
         * know a file is unchanged without hashing it, so the metadata has to
         * answer instead.
         */
        uint64_t _tc = phase_now();
        int cache_hit = file_cache_lookup(p->db, p->repo_id, e->path, &e->st,
                                          e->node.content_hash);
        phase_add(p->timers, PHASE_CACHE_LOOKUP, _tc);
        if (cache_hit) {
            /* Nothing is written here on purpose. A hit has to be free, or
             * the cache costs more than the reading it avoids. */
            p->res->files++;
            p->res->bytes_logical += e->node.size;
            p->res->files_unchanged++;
            continue;
        }

        int fd = open(e->path, O_RDONLY);
        if (fd == -1) {
            /* A file that vanished between scan and store is normal on a live
             * filesystem; it is a warning, not a failed backup. */
            log_warn("backup: cannot open %s, skipping", e->path);
            continue;
        }

        object_write_t w;
        uint64_t _tr = phase_now();
        status_t st = object_store_file_info(p->store, fd, e->node.size,
                                             e->node.content_hash, &w);
        phase_add(p->timers, PHASE_READ_HASH, _tr);
        int is_new = w.is_new;
        close(fd);
        if (st != OK) {
            log_warn("backup: cannot store %s: %s", e->path, err_msg());
            err_clear();
            continue;
        }

        if (is_new) p->res->objects_new++;
        p->res->files++;
        p->res->bytes_logical += e->node.size;

        /*
         * The object's payload length, not the file's logical size.
         *
         * They differ only for a sparse file, whose payload is its region
         * table plus the bytes that are not holes. Recording the logical size
         * here disagreed with the object header, and a pack rewrites this
         * column from that header, so stats and summary reported one total
         * before a pack and a smaller one after - on a repository that had
         * changed nothing. The logical size is not lost: it is the node's,
         * in nodes.size and snapshots.size, and restore reads it from there.
         * It could not live here anyway, because two files with the same data
         * and different trailing holes share a payload and so share a hash.
         */
        uint64_t _ti = phase_now();
        st = store_index_put_loose(p->db, p->repo_id, e->node.content_hash,
                                   OBJECT_TYPE_FILE, w.phys_bytes,
                                   w.payload_size);
        phase_add(p->timers, PHASE_INDEX_PUT, _ti);
        if (st != OK) {
            if (in_txn && db_txn_commit(p->db) != OK)
                log_warn("backup: cache commit failed");
            return st;
        }

        uint64_t _tp = phase_now();
        file_cache_put(p->db, p->repo_id, e->path, &e->st,
                       e->node.content_hash, p->cache_stamp);
        phase_add(p->timers, PHASE_CACHE_PUT, _tp);
    }

    if (in_txn && db_txn_commit(p->db) != OK)
        log_warn("backup: cache commit failed");
    return OK;
    #undef CACHE_BATCH
}

/* ------------------------------------------------------------------ */
/* Phase 2: trees, deepest first                                       */
/* ------------------------------------------------------------------ */

/*
 * Serialise one directory from its immediate children. Called only after
 * every child directory already has its hash, which the depth ordering
 * below guarantees.
 */
static status_t build_one_tree(pipeline_t *p, uint32_t dir_index,
                               uint8_t out_hash[OBJECT_HASH_SIZE])
{
    uint64_t dir_node = (dir_index == UINT32_MAX)
                        ? 0 : p->links[dir_index].node_id;

    uint32_t n = 0;
    for (uint32_t i = 0; i < p->count; i++)
        if (p->links[i].parent_id == dir_node) n++;

    manifest_entry_t *entries = n ? calloc(n, sizeof(*entries)) : NULL;
    if (n && !entries) return set_error(ERR_NOMEM, "build_one_tree: alloc");

    uint32_t k = 0;
    for (uint32_t i = 0; i < p->count && k < n; i++) {
        if (p->links[i].parent_id != dir_node) continue;
        scan_entry_t *e = &p->scan->entries[i];
        manifest_entry_t *m = &entries[k++];

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
            memcpy(m->target_hash, p->links[i].tree_hash, OBJECT_HASH_SIZE);
        else
            memcpy(m->target_hash, e->node.content_hash, OBJECT_HASH_SIZE);

        if (e->symlink_target) {
            m->link_target     = e->symlink_target;
            m->link_target_len = (uint16_t)strlen(e->symlink_target);
        }
    }

    int is_new = 0;
    uint64_t tree_phys = 0;
    uint8_t *buf = NULL; size_t len = 0;
    status_t st = manifest_serialise(entries, k, &buf, &len);
    if (st == OK) {
        st = object_store_ex(p->store, OBJECT_TYPE_TREE, buf, len, out_hash,
                             &is_new, &tree_phys);
        free(buf);
    }
    free(entries);
    if (st != OK) return st;

    /* The measurement that matters: an unchanged directory serialises to
     * bytes the store has already seen, so is_new stays 0 and nothing is
     * written. A run where this is always 1 means determinism has broken. */
    if (is_new) {
        p->res->trees_new++;
        /* The serialised length is the logical size. The physical size is
         * what the store actually wrote, header and parity trailer included,
         * and only the store knows it. Recording len for both understated
         * every tree by the trailer. */
        st = store_index_put_loose(p->db, p->repo_id, out_hash,
                                   OBJECT_TYPE_TREE, tree_phys, len);
        if (st != OK) return st;
    } else {
        p->res->trees_reused++;
    }
    return OK;
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

static status_t compute_depths(const pipeline_t *p, const idmap_t *map,
                               uint32_t **out_depth)
{
    uint32_t *depth = malloc(p->count * sizeof(*depth));
    if (!depth) return set_error(ERR_NOMEM, "build_trees: depth alloc");
    for (uint32_t i = 0; i < p->count; i++) depth[i] = DEPTH_UNKNOWN;

    uint32_t *stack = malloc(p->count * sizeof(*stack));
    if (!stack) { free(depth); return set_error(ERR_NOMEM, "build_trees: depth stack"); }

    for (uint32_t i = 0; i < p->count; i++) {
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

            uint64_t parent = p->links[cur].parent_id;
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



static status_t build_trees(pipeline_t *p, uint8_t root_hash[OBJECT_HASH_SIZE])
{
    /* Order directories deepest-first so a parent is never serialised before
     * its children have hashes. */
    uint32_t *dirs = NULL;
    uint32_t ndirs = 0;
    for (uint32_t i = 0; i < p->count; i++)
        if (p->scan->entries[i].node.type == NODE_TYPE_DIR) ndirs++;

    if (ndirs) {
        dirs = malloc(ndirs * sizeof(*dirs));
        if (!dirs) return set_error(ERR_NOMEM, "build_trees: alloc");
        uint32_t k = 0;
        for (uint32_t i = 0; i < p->count; i++)
            if (p->scan->entries[i].node.type == NODE_TYPE_DIR) dirs[k++] = i;

        /*
         * Counting sort by descending depth.
         *
         * This was an insertion sort calling a parent-chain walk on every
         * comparison, which is fine at a few hundred directories and fatal at
         * twenty-five thousand: quadratic in directories, each comparison
         * linear in entries. Depths are already computed, and they are small
         * integers, so ordering by them is a pair of linear passes.
         */
        idmap_t map = { NULL, NULL, 0 };
        if (idmap_build(&map, p->links, p->count) != 0) {
            free(dirs);
            return set_error(ERR_NOMEM, "build_trees: id map");
        }
        uint32_t *depth = NULL;
        status_t dst = compute_depths(p, &map, &depth);
        idmap_free(&map);
        if (dst != OK) { free(dirs); return dst; }

        uint32_t maxd = 0;
        for (uint32_t a = 0; a < ndirs; a++)
            if (depth[dirs[a]] > maxd) maxd = depth[dirs[a]];

        uint32_t *counts = calloc((size_t)maxd + 2, sizeof(*counts));
        uint32_t *sorted = malloc(ndirs * sizeof(*sorted));
        if (!counts || !sorted) {
            free(counts); free(sorted); free(depth); free(dirs);
            return set_error(ERR_NOMEM, "build_trees: ordering");
        }
        for (uint32_t a = 0; a < ndirs; a++) counts[depth[dirs[a]]]++;
        /* Deepest first: offsets accumulate from the high end. */
        uint32_t running = 0;
        for (uint32_t d = maxd + 1; d-- > 0; ) {
            uint32_t c = counts[d];
            counts[d] = running;
            running += c;
        }
        for (uint32_t a = 0; a < ndirs; a++)
            sorted[counts[depth[dirs[a]]]++] = dirs[a];

        memcpy(dirs, sorted, ndirs * sizeof(*dirs));
        free(sorted);
        free(counts);
        free(depth);

        for (uint32_t a = 0; a < ndirs; a++) {
            if (cancelled(p)) { free(dirs); return set_error(ERR_BUSY, "backup cancelled"); }
            report(p, "trees", a, ndirs);
            status_t st = build_one_tree(p, dirs[a], p->links[dirs[a]].tree_hash);
            if (st != OK) { free(dirs); return st; }
            p->links[dirs[a]].hash_ready = 1;
        }
        free(dirs);
    }

    /* The synthetic root ties the source paths together into one tree. */
    return build_one_tree(p, UINT32_MAX, root_hash);
}

/* ------------------------------------------------------------------ */
/* Phase 3: snapshot record + index                                    */
/* ------------------------------------------------------------------ */

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

static void hash_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    static const char *d = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        out[i * 2]     = d[(h[i] >> 4) & 0xF];
        out[i * 2 + 1] = d[h[i] & 0xF];
    }
    out[OBJECT_HASH_SIZE * 2] = '\0';
}

/*
 * The snapshot record on disk is what makes the repository self-describing.
 * Written atomically, because a half-written record would make a snapshot
 * unreadable during reindex even though all of its data is present.
 */
static status_t write_snapshot_record(store_t *store, int64_t snap_id,
                                      const uint8_t root[OBJECT_HASH_SIZE],
                                      time_t ts, const char *label)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(root, hex);

    char tmp[PATH_MAX], final[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp/snap.XXXXXX", store_root(store)) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/snapshots/%lld",
                 store_root(store), (long long)snap_id) >= (int)sizeof(final))
        return set_error(ERR_IO, "snapshot record: path too long");

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "snapshot record: mkstemp");

    char body[512];
    int n = snprintf(body, sizeof(body),
                     "{\"id\":%lld,\"ts\":%lld,\"root\":\"%s\",\"label\":\"%s\"}\n",
                     (long long)snap_id, (long long)ts, hex, label ? label : "");
    if (n < 0 || write(fd, body, (size_t)n) != n) {
        close(fd); unlink(tmp);
        return set_error_errno(ERR_IO, "snapshot record: write");
    }
    if (fsync(fd) != 0) { close(fd); unlink(tmp); return set_error_errno(ERR_IO, "snapshot record: fsync"); }
    close(fd);

    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "snapshot record: rename");
    }
    return OK;
}

static status_t index_nodes(pipeline_t *p, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)p->repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    /*
     * Node ids are unique per repository, and there are two things that
     * allocate them: this, from the scan's own numbering, and reindex, from
     * MAX(id) in the table. They disagreed, and the insert below was INSERT
     * OR REPLACE, so a backup after a reindex quietly overwrote an older
     * snapshot's rows with its own - that snapshot kept nodes_materialized=1
     * and had nothing left to browse or restore. The manifest trees were
     * fine, so nothing was lost that a reindex could not rebuild, and nothing
     * said a word.
     *
     * Starting past whatever the repository already holds makes the two
     * agree. They are an index artifact: nothing in the repo refers to them,
     * which is what lets the database be rebuilt without preserving them.
     */
    uint64_t id_base = 0;
    {
        db_result_t *r = NULL;
        if (db_execute(p->db, "SELECT COALESCE(MAX(id), 0) FROM nodes WHERE repo_id = ?",
                       (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
            id_base = strtoull(r->rows[0][0], NULL, 10);
        db_result_free(r);
    }

    /*
     * Committed in batches, for the same reason store_content is.
     *
     * This used to be one transaction spanning every node and its FTS row -
     * 1.2M inserts on a 622,000-file tree, minutes of wall clock. That was
     * survivable while writers raced, because a writer that could not begin
     * failed instead of waiting. Now they queue behind the write lock, and a
     * lock held for minutes would block every tag, note, policy and sources
     * write for the length of a backup: a worse bug than the one it replaced.
     *
     * Splitting it is safe here. The authoritative snapshot record is already
     * in the repository, written by write_snapshot_record before this runs,
     * and the snapshots row that marks nodes_materialized is not inserted
     * until every batch has landed. An interrupted run therefore leaves node
     * rows that no snapshot claims - index garbage a reindex rebuilds, which
     * is the state eviction already produces deliberately.
     */
    #define NODE_BATCH 2000
    uint32_t since_commit = 0;
    status_t bst = db_txn_begin(p->db);
    if (bst != OK) return bst;

    for (uint32_t i = 0; i < p->count; i++) {
        scan_entry_t *e = &p->scan->entries[i];
        const char *name = basename_of(e->path);

        char id[32], parent[32], mode[16], uid[16], gid[16], mtime[32], size[32];
        /* Both offset, and together: parent_node_id refers to one of these
         * ids. A root's parent stays 0, which is not an id. */
        snprintf(id,     sizeof(id),     "%llu",
                 (unsigned long long)(e->node.node_id + id_base));
        snprintf(parent, sizeof(parent), "%llu",
                 (unsigned long long)(p->links[i].parent_id
                                      ? p->links[i].parent_id + id_base : 0));
        snprintf(mode,   sizeof(mode),   "%u",   e->node.mode);
        snprintf(uid,    sizeof(uid),    "%u",   e->node.uid);
        snprintf(gid,    sizeof(gid),    "%u",   e->node.gid);
        snprintf(mtime,  sizeof(mtime),  "%llu",
                 (unsigned long long)(e->node.mtime_sec * 1000000000ULL + e->node.mtime_nsec));
        snprintf(size,   sizeof(size),   "%llu", (unsigned long long)e->node.size);

        char chex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.content_hash, chex);
        char shex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(p->links[i].tree_hash, shex);
        char xhex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.xattr_hash, xhex);
        char ahex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.acl_hash, ahex);
        int is_dir = (e->node.type == NODE_TYPE_DIR);

        char ntype[8];
        snprintf(ntype, sizeof(ntype), "%u", e->node.type);

        /*
         * One statement with every column bound, "" standing for absent and
         * NULLIF turning it back into a real NULL in the row.
         *
         * The arity has to be fixed because the params array is
         * NULL-terminated: a conditional NULL value silently truncates the
         * list and unbinds every column after it. But the stored value has to
         * be NULL rather than "", because queries elsewhere ask
         * "content_hash IS NOT NULL" to mean "this node has content", and an
         * empty string satisfies that while matching no object.
         */
        const char *ltarget = e->symlink_target ? e->symlink_target : "";
        /*
         * Signed, deliberately. inode_identity is a 64-bit hash, so about
         * half of all values exceed INT64_MAX; written as unsigned decimal
         * SQLite cannot fit them in an INTEGER column and falls back to REAL,
         * which loses precision and collapses distinct inodes onto the same
         * value. Restoring then hardlinked unrelated files together. The bit
         * pattern round-trips through int64 exactly, so signed is lossless.
         */
        char hgroup[32];
        snprintf(hgroup, sizeof(hgroup), "%lld",
                 (long long)e->node.inode_identity);

        /* Union again: only a device node has a device pair. */
        int is_dev = (e->node.type == NODE_TYPE_CHR || e->node.type == NODE_TYPE_BLK);
        char dmaj[16], dmin[16];
        snprintf(dmaj, sizeof(dmaj), "%u", is_dev ? e->node.device.major : 0u);
        snprintf(dmin, sizeof(dmin), "%u", is_dev ? e->node.device.minor : 0u);

        int rc = db_execute_non_query(p->db,
                "INSERT INTO nodes"
                " (repo_id, id, snap_id, parent_node_id, name, type, mode, uid,"
                "  gid, mtime_ns, size, content_hash, subtree_hash, link_target,"
                "  hardlink_group_id, xattr_hash, acl_hash, dev_major, dev_minor)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                "         NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?,"
                "         NULLIF(?,''), NULLIF(?,''), ?, ?)",
                (const char *[]){ repo, id, snap, parent, name, ntype, mode, uid,
                                  gid, mtime, size,
                                  /* A symlink or device node has no content
                                   * object, so its hash is zero. Storing that
                                   * rather than NULL leaves a row claiming
                                   * content that matches no object, which is
                                   * exactly what an index consistency check
                                   * is looking for. */
                                  (is_dir || hash_is_zero(e->node.content_hash))
                                      ? "" : chex,
                                  is_dir ? shex : "",
                                  ltarget, hgroup,
                                  hash_is_zero(e->node.xattr_hash) ? "" : xhex,
                                  hash_is_zero(e->node.acl_hash)   ? "" : ahex,
                                  dmaj, dmin, NULL }, NULL);
        if (rc != 0) {
            db_txn_rollback(p->db);
            return set_error(ERR_IO, "index_nodes: insert failed");
        }

        if (db_execute_non_query(p->db,
                "INSERT INTO node_fts (name, node_id, snap_id, repo_id)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ name, id, snap, repo, NULL }, NULL) != 0) {
            db_txn_rollback(p->db);
            return set_error(ERR_IO, "index_nodes: fts insert failed");
        }

        if (++since_commit >= NODE_BATCH) {
            status_t cst = db_txn_commit(p->db);
            if (cst != OK) return cst;
            since_commit = 0;
            cst = db_txn_begin(p->db);
            if (cst != OK) return cst;
        }
    }

    #undef NODE_BATCH
    return db_txn_commit(p->db);
}

/* ------------------------------------------------------------------ */

status_t backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    const backup_opts_t *opts, backup_result_t *out)
{
    if (!store || !db || !opts || opts->n_paths < 1)
        return set_error(ERR_INVALID, "backup_run: missing store, db or paths");

    backup_result_t res;
    memset(&res, 0, sizeof(res));

    scan_imap_t *imap = scan_imap_new();
    if (!imap) return set_error(ERR_NOMEM, "backup_run: scan imap");

    scan_opts_t sopts;
    memset(&sopts, 0, sizeof(sopts));
    sopts.exclude        = opts->exclude;
    sopts.n_exclude      = opts->n_exclude;
    sopts.collect_meta   = 1;
    sopts.one_filesystem = opts->one_filesystem;

    /* Every source root is scanned into one result set so hard links spanning
     * roots are detected once, which is why the inode map is shared. */
    scan_result_t *scan = NULL;
    status_t st = OK;
    phase_timers_t timers;
    memset(&timers, 0, sizeof(timers));

    uint64_t _tscan = phase_now();
    for (int i = 0; i < opts->n_paths; i++) {
        scan_result_t *part = NULL;
        st = scan_tree(opts->paths[i], imap, &sopts, &part);
        if (st != OK) { scan_result_free(part); break; }
        if (!scan) {
            scan = part;
        } else {
            uint32_t need = scan->count + part->count;
            scan_entry_t *grown = realloc(scan->entries, need * sizeof(*grown));
            if (!grown) { scan_result_free(part); st = set_error(ERR_NOMEM, "backup_run: merge"); break; }
            scan->entries = grown;
            memcpy(scan->entries + scan->count, part->entries,
                   part->count * sizeof(*part->entries));
            scan->count = need;
            scan->capacity = need;
            /* Entries were shallow-copied; release the container only. */
            free(part->entries);
            part->entries = NULL;
            part->count = 0;
            scan_result_free(part);
        }
    }
    scan_imap_free(imap);
    if (st != OK) { scan_result_free(scan); return st; }
    if (!scan || scan->count == 0) {
        scan_result_free(scan);
        return set_error(ERR_INVALID, "backup_run: nothing to back up");
    }

    phase_add(&timers, PHASE_SCAN, _tscan);

    node_link_t *links = calloc(scan->count, sizeof(*links));
    if (!links) { scan_result_free(scan); return set_error(ERR_NOMEM, "backup_run: links"); }
    for (uint32_t i = 0; i < scan->count; i++) {
        links[i].node_id    = scan->entries[i].node.node_id;
        links[i].parent_id  = scan->entries[i].parent_node_id;
        links[i].scan_index = i;
    }

    pipeline_t p = { store, db, repo_id, scan, links, scan->count, &res, opts,
                     (int64_t)time(NULL), { 0, 0 }, { NULL, 0, 0 }, &timers };

    uint8_t root_hash[OBJECT_HASH_SIZE];
    struct timespec t0, t1, t2, t3;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    /* Layers below here attribute through the thread-local; see phase_timer.h. */
    phase_current = &timers;
    st = store_content(&p);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    uint64_t _ttr = phase_now();
    if (st == OK) st = build_trees(&p, root_hash);
    phase_add(&timers, PHASE_TREES, _ttr);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    if (st != OK) {
        hashset_free(&p.meta_seen);
        free(links);
        scan_result_free(scan);
        return st;
    }

    /* Snapshot ids are per repo and monotonic. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int64_t snap_id = 1;
    if (db_execute(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM snapshots WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        snap_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);

    time_t now = time(NULL);
    st = write_snapshot_record(store, snap_id, root_hash, now, opts->label);
    uint64_t _tin = phase_now();
    if (st == OK) st = index_nodes(&p, snap_id);
    phase_add(&timers, PHASE_INDEX_NODES, _tin);
    clock_gettime(CLOCK_MONOTONIC, &t3);

    if (st == OK) {
        char snap[32], ts[32], files[32], bytes[32], rhex[OBJECT_HASH_SIZE * 2 + 1];
        snprintf(snap,  sizeof(snap),  "%lld", (long long)snap_id);
        snprintf(ts,    sizeof(ts),    "%lld", (long long)now);
        snprintf(files, sizeof(files), "%llu", (unsigned long long)res.files);
        snprintf(bytes, sizeof(bytes), "%llu", (unsigned long long)res.bytes_logical);
        hash_hex(root_hash, rhex);

        if (db_execute_non_query(db,
                "INSERT INTO snapshots (repo_id, id, ts, label, file_count,"
                " total_size, completed_at, root_tree_hash, nodes_materialized)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (const char *[]){ repo, snap, ts, opts->label ? opts->label : "",
                                  files, bytes, ts, rhex, NULL }, NULL) != 0)
            st = set_error(ERR_IO, "backup_run: cannot record snapshot");
    }

    /* Paths the scan no longer sees keep their cache rows otherwise, and the
     * cache would grow for every file that ever existed under this repo. */
    if (st == OK) {
        const char **seen = malloc(scan->count * sizeof(*seen));
        if (seen) {
            size_t n_seen = 0;
            for (uint32_t i = 0; i < scan->count; i++)
                if (scan->entries[i].node.type == NODE_TYPE_REG)
                    seen[n_seen++] = scan->entries[i].path;
            qsort(seen, n_seen, sizeof(*seen), scan_path_cmp);
            uint64_t _tsw = phase_now();
            file_cache_sweep(db, repo_id, seen, n_seen);
            phase_add(&timers, PHASE_CACHE_SWEEP, _tsw);
            free(seen);
        }
    }

    phase_current = NULL;
    res.snapshot_id = snap_id;
    hashset_free(&p.meta_seen);
    free(links);
    scan_result_free(scan);

    if (st == OK && out) *out = res;
    if (st == OK) {
        /*
         * t0 is taken after the scan has already run, so the old label on
         * this line - "scan+store" - named a phase it did not contain. The
         * scan is measured properly now and appears in the profile below.
         */
        log_info("backup phases: store %.1fs, trees %.1fs, index %.1fs",
                 (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9,
                 (double)(t2.tv_sec - t1.tv_sec) + (double)(t2.tv_nsec - t1.tv_nsec) / 1e9,
                 (double)(t3.tv_sec - t2.tv_sec) + (double)(t3.tv_nsec - t2.tv_nsec) / 1e9);
        phase_report(&timers, "backup", phase_now() - _tscan);
        log_info("backup: snapshot %lld, %llu files (%llu unchanged, not read), "
                 "%llu new objects, %llu new trees, %llu reused",
                 (long long)snap_id, (unsigned long long)res.files,
                 (unsigned long long)res.files_unchanged,
                 (unsigned long long)res.objects_new,
                 (unsigned long long)res.trees_new,
                 (unsigned long long)res.trees_reused);
    }
    return st;
}
