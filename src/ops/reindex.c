#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "reindex.h"

#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "db_txn.h"

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
/*
 * Record an object in the registry.
 *
 * Sizes are deliberately left NULL here and filled in one pass afterwards.
 * A hash referenced by a thousand nodes would otherwise be stat'd a thousand
 * times, and on a repository with real dedup that dominates the rebuild.
 *
 * refcount starts at zero because that is what the backup path records too:
 * GC owns refcounts, zeroing and recomputing them from every live snapshot on
 * each run, so anything written here would be discarded at the next sweep.
 */
static status_t fill_object_sizes(store_t *store, cutils_db_t *db, int64_t repo_id);

static status_t note_object(reindex_ctx_t *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                            uint8_t type)
{
    if (hash_is_zero(hash)) return OK;

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(hash, hex);
    char repo[32], t[8];
    snprintf(repo, sizeof(repo), "%lld", (long long)ctx->repo_id);
    snprintf(t, sizeof(t), "%u", type);

    if (db_execute_non_query(ctx->db,
            "INSERT INTO objects (repo_id, hash, refcount, location, type)"
            " VALUES (?, ?, 0, 'loose', ?)"
            " ON CONFLICT(repo_id, hash) DO NOTHING",
            (const char *[]){ repo, hex, t, NULL }, NULL) != 0)
        return set_error(ERR_IO, "reindex: cannot record object");

    ctx->res->objects++;
    return OK;
}

static status_t walk_tree(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                          int64_t parent_node_id)
{
    manifest_entry_t *entries = NULL;
    uint32_t count = 0;
    status_t st = manifest_read_tree(ctx->store, tree_hash, &entries, &count);
    if (st != OK) return st;
    ctx->res->trees_read++;

    /* The tree is an object too, and nothing else will record it: no entry
     * points at its own directory. Miss it and GC sweeps the manifest. */
    st = note_object(ctx, tree_hash, OBJECT_TYPE_TREE);
    if (st != OK) { manifest_free_entries(entries, count); return st; }

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

        /*
         * One statement with every column bound, "" for absent, matching the
         * backup path. A NULL in the params array is a terminator rather than
         * a value, so a conditional NULL silently unbinds every column after
         * it.
         *
         * NULLIF keeps absent columns genuinely NULL while the parameter
         * list stays a fixed length, since a NULL parameter would terminate
         * the array rather than bind a value.
         *
         * link_target and hardlink_group_id are the reason a repository can
         * outlive its index: the trees have always carried them, so a rebuild
         * restores symlinks and hardlink identity that the index never had.
         */
        /* Signed for the same reason as the backup path: an unsigned
         * 64-bit hash does not fit SQLite's INTEGER and degrades to REAL. */
        char hgroup[32];
        snprintf(hgroup, sizeof(hgroup), "%lld",
                 (long long)e->inode_identity);
        const char *ltarget = e->link_target ? e->link_target : "";

        char xhex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->xattr_hash, xhex);
        char ahex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->acl_hash, ahex);

        char dmaj[16], dmin[16];
        snprintf(dmaj, sizeof(dmaj), "%u", e->dev_major);
        snprintf(dmin, sizeof(dmin), "%u", e->dev_minor);

        int rc = db_execute_non_query(ctx->db,
            "INSERT OR REPLACE INTO nodes"
            " (repo_id, id, snap_id, parent_node_id, name, type, mode, uid,"
            "  gid, mtime_ns, size, content_hash, subtree_hash, link_target,"
            "  hardlink_group_id, xattr_hash, acl_hash, dev_major, dev_minor)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
            "         NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?,"
            "         NULLIF(?,''), NULLIF(?,''), ?, ?)",
            (const char *[]){ repo, id, snap, parent, e->name, ntype, mode,
                              uid, gid, mtime, size,
                              (have_target && !is_dir) ? thex : "",
                              (have_target && is_dir) ? thex : "",
                              ltarget, hgroup,
                              hash_is_zero(e->xattr_hash) ? "" : xhex,
                              hash_is_zero(e->acl_hash)   ? "" : ahex,
                              dmaj, dmin, NULL }, NULL);
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
        if (e->type == NODE_TYPE_REG) ctx->res->files++;

        /* Subtree hashes are recorded by the recursion below, so only content
         * needs noting here. */
        if (have_target && !is_dir) {
            st = note_object(ctx, e->target_hash, OBJECT_TYPE_FILE);
            if (st != OK) break;
        }
        if (!hash_is_zero(e->xattr_hash))
            st = note_object(ctx, e->xattr_hash, OBJECT_TYPE_XATTR);
        if (st == OK && !hash_is_zero(e->acl_hash))
            st = note_object(ctx, e->acl_hash, OBJECT_TYPE_ACL);
        if (st != OK) break;

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

    if (db_txn_begin(db) != OK) {
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
        char tsbuf[32], files[32], rhex[OBJECT_HASH_SIZE * 2 + 1];
        snprintf(tsbuf, sizeof(tsbuf), "%lld", (long long)ts);
        snprintf(files, sizeof(files), "%llu", (unsigned long long)res.files);
        hash_hex(root, rhex);
        if (db_execute_non_query(db,
                "INSERT OR REPLACE INTO snapshots (repo_id, id, ts, label,"
                " file_count, completed_at, root_tree_hash, nodes_materialized)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
                (const char *[]){ repo, snap, tsbuf, label ? label : "",
                                  files, tsbuf, rhex, NULL }, NULL) != 0)
            failed = 1;
    }

    free(label);

    if (failed) {
        db_txn_rollback(db);
        return st != OK ? st : set_error(ERR_IO, "reindex: snapshot %lld failed",
                                         (long long)snap_id);
    }
    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "reindex: commit");

    /* Standalone callers fault a single cold snapshot back in and get a
     * complete registry for it. Called from reindex_repo this is nearly a
     * no-op, because it only touches rows still missing their sizes. */
    status_t fst = fill_object_sizes(store, db, repo_id);
    if (fst != OK) return fst;

    res.snapshots = 1;
    if (out) *out = res;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Pack index rebuild                                                    */
/* ------------------------------------------------------------------ */

typedef struct {
    cutils_db_t *db;
    const char  *repo;
    char         pack[32];
    int          failed;
    uint64_t     entries;
} pack_entry_ctx_t;

static void pack_entry_seen(const pack_entry_info_t *info, void *vctx)
{
    pack_entry_ctx_t *c = vctx;
    if (c->failed) return;

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(info->hash, hex);
    char off[32], comp[32], orig[32], eidx[16];
    snprintf(off,  sizeof(off),  "%llu", (unsigned long long)info->entry_offset);
    snprintf(comp, sizeof(comp), "%llu", (unsigned long long)info->compressed_size);
    snprintf(orig, sizeof(orig), "%llu", (unsigned long long)info->uncompressed_size);
    snprintf(eidx, sizeof(eidx), "%u",   info->entry_index);

    if (db_execute_non_query(c->db,
            "INSERT OR REPLACE INTO pack_objects"
            " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (const char *[]){ c->repo, hex, c->pack, off, comp, orig, eidx, NULL },
            NULL) != 0) {
        c->failed = 1;
        return;
    }
    c->entries++;
}

typedef struct {
    store_t     *store;
    cutils_db_t *db;
    const char  *repo;
    int          failed;
    uint64_t     packs;
    uint64_t     entries;
} pack_scan_ctx_t;

static void pack_num_seen(uint32_t pack_num, void *vctx)
{
    pack_scan_ctx_t *c = vctx;
    if (c->failed) return;

    char name[64];
    snprintf(name, sizeof(name), "pack-%08u.dat", pack_num);

    uint32_t version = 0, count = 0;
    pack_entry_ctx_t ec;
    memset(&ec, 0, sizeof(ec));
    ec.db = c->db;
    ec.repo = c->repo;
    snprintf(ec.pack, sizeof(ec.pack), "%u", pack_num);

    if (pack_enumerate_dat(c->store, name, &version, &count,
                           pack_entry_seen, &ec) != OK) {
        /* One unreadable pack must not abandon the rest, for the same reason
         * one unreadable snapshot does not. */
        log_warn("reindex: cannot read %s: %s", name, err_msg());
        err_clear();
        return;
    }
    if (ec.failed) { c->failed = 1; return; }

    char path[PATH_MAX];
    uint64_t size = 0;
    if (pack_dat_path_resolve(path, sizeof(path), store_root(c->store), pack_num) == 0) {
        struct stat sb;
        if (stat(path, &sb) == 0) size = (uint64_t)sb.st_size;
    }

    char pid[32], szbuf[32], ver[16];
    snprintf(pid,   sizeof(pid),   "%u", pack_num);
    snprintf(szbuf, sizeof(szbuf), "%llu", (unsigned long long)size);
    snprintf(ver,   sizeof(ver),   "%u", version);
    if (db_execute_non_query(c->db,
            "INSERT OR REPLACE INTO packs (repo_id, id, filename, size,"
            " sealed_at, version) VALUES (?, ?, ?, ?, strftime('%s','now'), ?)",
            (const char *[]){ c->repo, pid, name, szbuf, ver, NULL }, NULL) != 0) {
        c->failed = 1;
        return;
    }
    c->packs++;
    c->entries += ec.entries;
}

/*
 * Rebuild packs and pack_objects by reading the pack files themselves.
 *
 * This has to run before the trees are walked. Every packed object resolves
 * through pack_objects, so until these rows exist the store cannot locate,
 * size or read anything that lives in a pack, and the object registry built
 * from the walk would describe packed objects as missing.
 */
static status_t rebuild_pack_index(store_t *store, cutils_db_t *db, int64_t repo_id,
                                   reindex_result_t *res)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /* One transaction for the whole rebuild. Without it every row is its own
     * committed transaction, and a pack of a few thousand entries spends all
     * its time in SQLite's per-statement bookkeeping rather than in the work.
     * It is also the correct scope: a half-rebuilt pack index should never be
     * observable. */
    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "reindex: cannot begin pack index rebuild");

    if (db_execute_non_query(db, "DELETE FROM pack_objects WHERE repo_id = ?",
                             (const char *[]){ repo, NULL }, NULL) != 0 ||
        db_execute_non_query(db, "DELETE FROM packs WHERE repo_id = ?",
                             (const char *[]){ repo, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "reindex: cannot clear pack index");
    }

    pack_scan_ctx_t c;
    memset(&c, 0, sizeof(c));
    c.store = store;
    c.db    = db;
    c.repo  = repo;

    status_t st = pack_list(store, pack_num_seen, &c);
    if (st != OK || c.failed) {
        db_txn_rollback(db);
        return st != OK ? st
                        : set_error(ERR_IO, "reindex: cannot rebuild pack index");
    }
    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "reindex: cannot commit pack index");

    res->packs         = c.packs;
    res->pack_entries  = c.entries;
    return OK;
}

/*
 * Fill in the sizes and locations the tree walk deliberately left blank.
 *
 * One pass over the distinct objects rather than one lookup per reference:
 * on a repository where dedup is doing its job the same hash appears under
 * many names, and sizing it once is the difference between a rebuild that
 * takes seconds and one that takes minutes.
 */
static status_t fill_object_sizes(store_t *store, cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /*
     * Packed objects are settled in one statement.
     *
     * The obvious implementation asks the store for each object's size, but
     * for a packed object that means resolving it through the index and
     * reading its entry header out of the pack: measured at 57ms an object,
     * which turned a rebuild of a 1116-object repository into 94 seconds.
     * pack_objects was just rebuilt from those same headers and already holds
     * the sizes, so the answer is a join rather than a file read.
     */
    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "reindex: cannot begin object sizing");

    if (db_execute_non_query(db,
            "UPDATE objects SET"
            "   phys_size = (SELECT po.comp_size FROM pack_objects po"
            "                 WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash),"
            "   orig_size = (SELECT po.orig_size FROM pack_objects po"
            "                 WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash),"
            "   location  = (SELECT CAST(po.pack_id AS TEXT) FROM pack_objects po"
            "                 WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash)"
            " WHERE repo_id = ? AND phys_size IS NULL"
            "   AND EXISTS (SELECT 1 FROM pack_objects po"
            "                WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash)",
            (const char *[]){ repo, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "reindex: cannot size packed objects");
    }

    /* Whatever is left is loose, and a loose object's size is a stat and a
     * header read: no index lookup, no pack to open. */
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT hash FROM objects WHERE repo_id = ? AND phys_size IS NULL",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        db_txn_rollback(db);
        return set_error(ERR_IO, "reindex: cannot list unsized objects");
    }

    status_t st = OK;
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        const char *hex = r->rows[i][0];
        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(hex, hash);

        uint64_t phys = 0, orig = 0;
        uint8_t  type = 0;
        if (object_physical_size(store, hash, &phys, NULL) != OK ||
            object_get_info(store, hash, &orig, &type) != OK) {
            /* Referenced but absent. Leaving the row unsized rather than
             * inventing a number keeps the gap visible to a verify pass. */
            log_warn("reindex: object %.16s referenced but unreadable: %s",
                     hex, err_msg());
            err_clear();
            continue;
        }

        char pbuf[32], obuf[32], tbuf[8];
        snprintf(pbuf, sizeof(pbuf), "%llu", (unsigned long long)phys);
        snprintf(obuf, sizeof(obuf), "%llu", (unsigned long long)orig);
        snprintf(tbuf, sizeof(tbuf), "%u", type);
        if (db_execute_non_query(db,
                "UPDATE objects SET phys_size = ?, orig_size = ?, type = ?,"
                " location = 'loose' WHERE repo_id = ? AND hash = ?",
                (const char *[]){ pbuf, obuf, tbuf, repo, hex, NULL },
                NULL) != 0) {
            st = set_error(ERR_IO, "reindex: cannot size object");
            break;
        }
    }
    db_result_free(r);

    if (st != OK) {
        db_txn_rollback(db);
        return st;
    }
    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "reindex: cannot commit object sizes");
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

    /* Packs first: the tree walk resolves packed objects through these rows,
     * so rebuilding them afterwards would be too late. */
    status_t pst = rebuild_pack_index(store, db, repo_id, &total);
    if (pst != OK) {
        closedir(d);
        return pst;
    }

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
        total.files     += one.files;
        total.objects   += one.objects;
        total.trees_read += one.trees_read;
    }
    closedir(d);
    if (st != OK) return st;

    /* Every snapshot has contributed its hashes by now, so each distinct
     * object is sized exactly once. */
    st = fill_object_sizes(store, db, repo_id);
    if (st != OK) return st;

    log_info("reindex: %llu snapshot(s), %llu nodes, %llu trees read, "
             "%llu object(s), %llu pack(s) with %llu entries",
             (unsigned long long)total.snapshots, (unsigned long long)total.nodes,
             (unsigned long long)total.trees_read, (unsigned long long)total.objects,
             (unsigned long long)total.packs, (unsigned long long)total.pack_entries);
    if (out) *out = total;
    return OK;
}
