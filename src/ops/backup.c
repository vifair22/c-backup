#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "backup.h"

#include "manifest.h"
#include "object.h"
#include "scan.h"
#include "store_index.h"

#include <cutils/log.h>

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

typedef struct {
    store_t       *store;
    cutils_db_t   *db;
    int64_t        repo_id;
    scan_result_t *scan;
    node_link_t   *links;
    uint32_t       count;
    backup_result_t *res;
    const backup_opts_t *opts;
} pipeline_t;

static int cancelled(const pipeline_t *p)
{
    return p->opts->cancel && *p->opts->cancel;
}

static void report(const pipeline_t *p, const char *phase, uint64_t done, uint64_t total)
{
    if (p->opts->progress_cb)
        p->opts->progress_cb(phase, done, total, p->opts->progress_ctx);
}

/* The last path component; what the entry is called inside its parent. */
static const char *basename_of(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

static uint32_t find_index(const pipeline_t *p, uint64_t node_id)
{
    for (uint32_t i = 0; i < p->count; i++)
        if (p->links[i].node_id == node_id) return i;
    return UINT32_MAX;
}

/* ------------------------------------------------------------------ */
/* Phase 1: content                                                    */
/* ------------------------------------------------------------------ */

static status_t store_content(pipeline_t *p)
{
    for (uint32_t i = 0; i < p->count; i++) {
        if (cancelled(p)) return set_error(ERR_BUSY, "backup cancelled");

        scan_entry_t *e = &p->scan->entries[i];
        report(p, "store", i, p->count);

        if (e->node.type == NODE_TYPE_DIR) { p->res->dirs++; continue; }
        if (e->node.type != NODE_TYPE_REG) continue;

        int fd = open(e->path, O_RDONLY);
        if (fd == -1) {
            /* A file that vanished between scan and store is normal on a live
             * filesystem; it is a warning, not a failed backup. */
            log_warn("backup: cannot open %s, skipping", e->path);
            continue;
        }

        int is_new = 0;
        uint64_t phys = 0;
        status_t st = object_store_file_ex(p->store, fd, e->node.size,
                                           e->node.content_hash, &is_new, &phys);
        close(fd);
        if (st != OK) {
            log_warn("backup: cannot store %s: %s", e->path, err_msg());
            err_clear();
            continue;
        }

        if (is_new) p->res->objects_new++;
        p->res->files++;
        p->res->bytes_logical += e->node.size;

        st = store_index_put_loose(p->db, p->repo_id, e->node.content_hash,
                                   OBJECT_TYPE_FILE, phys, e->node.size);
        if (st != OK) return st;
    }
    return OK;
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
    uint8_t *buf = NULL; size_t len = 0;
    status_t st = manifest_serialise(entries, k, &buf, &len);
    if (st == OK) {
        st = object_store_ex(p->store, OBJECT_TYPE_TREE, buf, len, out_hash,
                             &is_new, NULL);
        free(buf);
    }
    free(entries);
    if (st != OK) return st;

    /* The measurement that matters: an unchanged directory serialises to
     * bytes the store has already seen, so is_new stays 0 and nothing is
     * written. A run where this is always 1 means determinism has broken. */
    if (is_new) {
        p->res->trees_new++;
        st = store_index_put_loose(p->db, p->repo_id, out_hash,
                                   OBJECT_TYPE_TREE, len, len);
        if (st != OK) return st;
    } else {
        p->res->trees_reused++;
    }
    return OK;
}

static uint32_t depth_of(const pipeline_t *p, uint32_t i)
{
    uint32_t d = 0;
    uint64_t parent = p->links[i].parent_id;
    while (parent != 0 && d < p->count) {
        uint32_t pi = find_index(p, parent);
        if (pi == UINT32_MAX) break;
        parent = p->links[pi].parent_id;
        d++;
    }
    return d;
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

        /* Insertion sort by descending depth: directory counts are small
         * relative to file counts and this keeps the ordering obvious. */
        for (uint32_t a = 1; a < ndirs; a++) {
            uint32_t v = dirs[a];
            uint32_t dv = depth_of(p, v);
            uint32_t b = a;
            while (b > 0 && depth_of(p, dirs[b - 1]) < dv) {
                dirs[b] = dirs[b - 1];
                b--;
            }
            dirs[b] = v;
        }

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

    if (db_exec_raw(p->db, "BEGIN") != 0)
        return set_error(ERR_IO, "index_nodes: begin");

    /* Node ids are global per repo and assigned here. They are an index
     * artifact: nothing in the repo refers to them, which is what lets the
     * database be rebuilt without preserving them. */
    for (uint32_t i = 0; i < p->count; i++) {
        scan_entry_t *e = &p->scan->entries[i];
        const char *name = basename_of(e->path);

        char id[32], parent[32], mode[16], uid[16], gid[16], mtime[32], size[32];
        snprintf(id,     sizeof(id),     "%llu", (unsigned long long)e->node.node_id);
        snprintf(parent, sizeof(parent), "%llu", (unsigned long long)p->links[i].parent_id);
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
        int is_dir = (e->node.type == NODE_TYPE_DIR);

        if (db_execute_non_query(p->db,
                "INSERT OR REPLACE INTO nodes"
                " (repo_id, id, snap_id, parent_node_id, name, mode, uid, gid,"
                "  mtime_ns, size, content_hash, subtree_hash)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, id, snap, parent, name, mode, uid, gid,
                                  mtime, size,
                                  is_dir ? NULL : chex,
                                  is_dir ? shex : NULL, NULL }, NULL) != 0) {
            if (db_exec_raw(p->db, "ROLLBACK") != 0)
                log_error("index_nodes: rollback failed");
            return set_error(ERR_IO, "index_nodes: insert failed");
        }

        if (db_execute_non_query(p->db,
                "INSERT INTO node_fts (name, node_id, snap_id, repo_id)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ name, id, snap, repo, NULL }, NULL) != 0) {
            if (db_exec_raw(p->db, "ROLLBACK") != 0)
                log_error("index_nodes: rollback failed");
            return set_error(ERR_IO, "index_nodes: fts insert failed");
        }
    }

    if (db_exec_raw(p->db, "COMMIT") != 0)
        return set_error(ERR_IO, "index_nodes: commit");
    return OK;
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

    node_link_t *links = calloc(scan->count, sizeof(*links));
    if (!links) { scan_result_free(scan); return set_error(ERR_NOMEM, "backup_run: links"); }
    for (uint32_t i = 0; i < scan->count; i++) {
        links[i].node_id    = scan->entries[i].node.node_id;
        links[i].parent_id  = scan->entries[i].parent_node_id;
        links[i].scan_index = i;
    }

    pipeline_t p = { store, db, repo_id, scan, links, scan->count, &res, opts };

    uint8_t root_hash[OBJECT_HASH_SIZE];
    st = store_content(&p);
    if (st == OK) st = build_trees(&p, root_hash);

    if (st != OK) {
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
    if (st == OK) st = index_nodes(&p, snap_id);

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

    res.snapshot_id = snap_id;
    free(links);
    scan_result_free(scan);

    if (st == OK && out) *out = res;
    if (st == OK)
        log_info("backup: snapshot %lld, %llu files, %llu new objects, "
                 "%llu new trees, %llu reused",
                 (long long)snap_id, (unsigned long long)res.files,
                 (unsigned long long)res.objects_new,
                 (unsigned long long)res.trees_new,
                 (unsigned long long)res.trees_reused);
    return st;
}
