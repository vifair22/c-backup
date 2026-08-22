#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "reindex.h"

#include "manifest.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    store_t     *store;
    cutils_db_t *db;
    int64_t      repo_id;
    int64_t      snap_id;
    int64_t      next_node_id;
    reindex_result_t *res;
} reindex_ctx_t;

static void hash_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    static const char *d = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        out[i * 2]     = d[(h[i] >> 4) & 0xF];
        out[i * 2 + 1] = d[h[i] & 0xF];
    }
    out[OBJECT_HASH_SIZE * 2] = '\0';
}

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

/* Is a hash all zeroes? Non-directory entries that never had content carry
 * one, and inserting it as a real reference would create a node pointing at
 * an object that does not exist. */
static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

/*
 * Depth-first walk of one tree, inserting a node row per entry.
 *
 * Node ids are minted fresh here. Nothing in the repository refers to them,
 * which is precisely what allows the index to be thrown away and rebuilt:
 * had they been written into the trees, a rebuild would have to preserve
 * them and the trees would stop being content-addressable.
 */
static status_t walk_tree(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                          int64_t parent_node_id)
{
    manifest_entry_t *entries = NULL;
    uint32_t count = 0;
    status_t st = manifest_read_tree(ctx->store, tree_hash, &entries, &count);
    if (st != OK) return st;
    ctx->res->trees_read++;

    char repo[32], snap[32], parent[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)ctx->repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)ctx->snap_id);
    snprintf(parent, sizeof(parent), "%lld", (long long)parent_node_id);

    for (uint32_t i = 0; i < count && st == OK; i++) {
        manifest_entry_t *e = &entries[i];
        int64_t node_id = ctx->next_node_id++;

        char id[32], mode[16], uid[16], gid[16], mtime[32], size[32];
        snprintf(id,    sizeof(id),    "%lld", (long long)node_id);
        snprintf(mode,  sizeof(mode),  "%u", e->mode);
        snprintf(uid,   sizeof(uid),   "%u", e->uid);
        snprintf(gid,   sizeof(gid),   "%u", e->gid);
        snprintf(mtime, sizeof(mtime), "%llu",
                 (unsigned long long)(e->mtime_sec * 1000000000ULL + e->mtime_nsec));
        snprintf(size,  sizeof(size),  "%llu", (unsigned long long)e->size);

        int is_dir = (e->type == NODE_TYPE_DIR);
        char thex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->target_hash, thex);
        int have_target = !hash_is_zero(e->target_hash);

        char ntype[8];
        snprintf(ntype, sizeof(ntype), "%u", e->type);

        /* Same reason as the backup path: a NULL in the params array is a
         * terminator, not a value. */
        int rc;
        if (!have_target) {
            rc = db_execute_non_query(ctx->db,
                "INSERT OR REPLACE INTO nodes"
                " (repo_id, id, snap_id, parent_node_id, name, type, mode, uid,"
                "  gid, mtime_ns, size)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, id, snap, parent, e->name, ntype, mode,
                                  uid, gid, mtime, size, NULL }, NULL);
        } else {
            rc = db_execute_non_query(ctx->db,
                is_dir
                  ? "INSERT OR REPLACE INTO nodes"
                    " (repo_id, id, snap_id, parent_node_id, name, type, mode,"
                    "  uid, gid, mtime_ns, size, subtree_hash)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                  : "INSERT OR REPLACE INTO nodes"
                    " (repo_id, id, snap_id, parent_node_id, name, type, mode,"
                    "  uid, gid, mtime_ns, size, content_hash)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, id, snap, parent, e->name, ntype, mode,
                                  uid, gid, mtime, size, thex, NULL }, NULL);
        }
        if (rc != 0) {
            st = set_error(ERR_IO, "reindex: cannot insert node");
            break;
        }

        if (db_execute_non_query(ctx->db,
                "INSERT INTO node_fts (name, node_id, snap_id, repo_id)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ e->name, id, snap, repo, NULL }, NULL) != 0) {
            st = set_error(ERR_IO, "reindex: cannot index name");
            break;
        }

        ctx->res->nodes++;

        if (is_dir && have_target)
            st = walk_tree(ctx, e->target_hash, node_id);
    }

    manifest_free_entries(entries, count);
    return st;
}

/* Parse a snapshot record: {"id":N,"ts":N,"root":"hex","label":"..."} */
static status_t read_snapshot_record(store_t *store, int64_t snap_id,
                                     uint8_t root[OBJECT_HASH_SIZE],
                                     int64_t *out_ts, char **out_label)
{
    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/snapshots/%lld",
                 store_root(store), (long long)snap_id) >= (int)sizeof(path))
        return set_error(ERR_IO, "reindex: path too long");

    FILE *f = fopen(path, "rb");
    if (!f) return set_error_errno(ERR_NOT_FOUND, "reindex: cannot open %s", path);
    char buf[1024];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';

    cutils_json_req_t *req = NULL;
    if (json_req_parse(buf, n, &req) != 0 || !req)
        return set_error(ERR_CORRUPT, "reindex: %s is not valid JSON", path);

    char *root_hex = NULL, *label = NULL;
    uint64_t ts = 0;
    int bad = json_req_get_str(req, "root", &root_hex) != 0 || !root_hex ||
              strlen(root_hex) != OBJECT_HASH_SIZE * 2;
    if (json_req_get_u64(req, "ts", &ts, 0, UINT64_MAX) != 0) ts = 0;
    if (json_req_get_str_opt(req, "label", &label) != 0) label = NULL;

    if (bad) {
        free(root_hex); free(label);
        json_req_free(req);
        return set_error(ERR_CORRUPT, "reindex: %s has no usable root hash", path);
    }

    hex_to_hash(root_hex, root);
    if (out_ts) *out_ts = (int64_t)ts;
    if (out_label) *out_label = label; else free(label);
    free(root_hex);
    json_req_free(req);
    return OK;
}

status_t reindex_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                          int64_t snap_id, reindex_result_t *out)
{
    uint8_t root[OBJECT_HASH_SIZE];
    int64_t ts = 0;
    char *label = NULL;
    status_t st = read_snapshot_record(store, snap_id, root, &ts, &label);
    if (st != OK) return st;

    reindex_result_t res;
    memset(&res, 0, sizeof(res));

    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    if (db_exec_raw(db, "BEGIN") != 0) {
        free(label);
        return set_error(ERR_IO, "reindex: begin");
    }

    /* Replacing, not merging: a half-reindexed snapshot mixed with stale rows
     * would be worse than either state on its own. */
    int failed = db_execute_non_query(db,
        "DELETE FROM nodes WHERE repo_id = ? AND snap_id = ?",
        (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM node_fts WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;

    reindex_ctx_t ctx = { store, db, repo_id, snap_id, 0, &res };
    if (!failed) {
        /* Continue the id space rather than restarting it, so ids stay unique
         * per repo across snapshots as the schema's primary key requires. */
        db_result_t *r = NULL;
        if (db_execute(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM nodes WHERE repo_id = ?",
                       (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
            ctx.next_node_id = strtoll(r->rows[0][0], NULL, 10);
        else
            ctx.next_node_id = 1;
        db_result_free(r);

        st = walk_tree(&ctx, root, 0);
        if (st != OK) failed = 1;
    }

    if (!failed) {
        char tsbuf[32], nodes[32], rhex[OBJECT_HASH_SIZE * 2 + 1];
        snprintf(tsbuf, sizeof(tsbuf), "%lld", (long long)ts);
        snprintf(nodes, sizeof(nodes), "%llu", (unsigned long long)res.nodes);
        hash_hex(root, rhex);
        if (db_execute_non_query(db,
                "INSERT OR REPLACE INTO snapshots (repo_id, id, ts, label,"
                " file_count, completed_at, root_tree_hash, nodes_materialized)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                (const char *[]){ repo, snap, tsbuf, label ? label : "",
                                  nodes, tsbuf, rhex, NULL }, NULL) != 0)
            failed = 1;
    }

    free(label);

    if (failed) {
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("reindex: rollback failed");
        return st != OK ? st : set_error(ERR_IO, "reindex: snapshot %lld failed",
                                         (long long)snap_id);
    }
    if (db_exec_raw(db, "COMMIT") != 0)
        return set_error(ERR_IO, "reindex: commit");

    res.snapshots = 1;
    if (out) *out = res;
    return OK;
}

status_t reindex_repo(store_t *store, cutils_db_t *db, int64_t repo_id,
                      volatile int *cancel, reindex_result_t *out)
{
    char dir[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s/snapshots", store_root(store)) >= (int)sizeof(dir))
        return set_error(ERR_IO, "reindex: path too long");

    DIR *d = opendir(dir);
    if (!d) return set_error_errno(ERR_IO, "reindex: cannot open %s", dir);

    reindex_result_t total;
    memset(&total, 0, sizeof(total));
    status_t st = OK;

    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (cancel && *cancel) { st = set_error(ERR_BUSY, "reindex cancelled"); break; }
        char *end = NULL;
        long long id = strtoll(e->d_name, &end, 10);
        if (id <= 0 || !end || *end != '\0') continue;

        reindex_result_t one;
        memset(&one, 0, sizeof(one));
        status_t one_st = reindex_snapshot(store, db, repo_id, id, &one);
        if (one_st != OK) {
            /* One unreadable snapshot must not abandon the rest: partial
             * recovery of a damaged repository is the whole point. */
            log_warn("reindex: snapshot %lld failed: %s", id, err_msg());
            err_clear();
            continue;
        }
        total.snapshots += one.snapshots;
        total.nodes     += one.nodes;
        total.trees_read += one.trees_read;
    }
    closedir(d);
    if (st != OK) return st;

    log_info("reindex: %llu snapshot(s), %llu nodes, %llu trees read",
             (unsigned long long)total.snapshots, (unsigned long long)total.nodes,
             (unsigned long long)total.trees_read);
    if (out) *out = total;
    return OK;
}
