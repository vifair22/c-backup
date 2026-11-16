#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "util.h"
#include "reindex.h"

#include "chunklist.h"
#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "db_txn.h"
#include "repo_tags.h"
#include "snap_record.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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
 * refcount starts at zero and is counted once every snapshot has been
 * walked, by recompute_refcounts: counting here would double on every
 * repeated reindex, since a snapshot's nodes are replaced but its objects
 * are not.
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

/* Every object a directory consists of, with its real type: the root (a
 * TREE, or for a large directory a CHUNKLIST of fragments), its fragments
 * and its tree lists (OWN-34). */
typedef struct {
    reindex_ctx_t *ctx;
    status_t       st;
} dir_note_t;

static void note_dir_object(void *arg, const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type)
{
    dir_note_t *d = arg;
    if (d->st == OK) d->st = note_object(d->ctx, hash, type);
}

static status_t walk_tree(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                          int64_t parent_node_id)
{
    /*
     * The directory's objects are recorded as the iterator loads them. The
     * tree is an object too, and nothing else will record it: no entry
     * points at its own directory. Miss it and GC sweeps the manifest.
     * The entries themselves are read whole: every one becomes a node row
     * in the transaction this walk runs in anyway.
     */
    dir_note_t dn = { ctx, OK };
    obj_loader_t ld = obj_loader_store(ctx->store);
    manifest_iter_t *it = NULL;
    status_t st = manifest_iter_open(&ld, tree_hash, note_dir_object, &dn, &it);
    if (st != OK) return st;
    manifest_entry_t *entries = NULL;
    uint32_t count = 0, cap = 0;
    for (;;) {
        const manifest_entry_t *me = NULL;
        int done = 0;
        st = manifest_iter_next(it, &me, &done);
        if (st != OK || done) break;
        if (dn.st != OK) { st = dn.st; break; }
        if (count == cap) {
            uint32_t nc = cap ? cap * 2 : 64;
            manifest_entry_t *t = realloc(entries, (size_t)nc * sizeof(*t));
            if (!t) { st = set_error(ERR_NOMEM, "reindex: entries"); break; }
            entries = t;
            cap = nc;
        }
        manifest_entry_t *e = &entries[count];
        *e = *me;
        char *nm = strndup(me->name, me->name_len);
        char *lt = me->link_target ? strndup(me->link_target, me->link_target_len) : NULL;
        e->name = nm;
        e->link_target = lt;
        count++;
        if (!nm || (me->link_target && !lt)) { st = set_error(ERR_NOMEM, "reindex: entries"); break; }
    }
    manifest_iter_close(it);
    if (st == OK) st = dn.st;
    if (st != OK) { manifest_free_entries(entries, count); return st; }
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
        snprintf(mtime, sizeof(mtime), "%lld",
                 (long long)time_to_ns(e->mtime_sec, (int64_t)e->mtime_nsec));
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
        if (e->type == NODE_TYPE_REG) {
            ctx->res->files++;
            /* What backup records as total_size: the logical size of every
             * regular file. Left out, a rebuilt snapshot listed "size":"". */
            ctx->res->bytes += e->size;
        }

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

    /*
     * Sized by the file, not a fixed buffer. A 1,024-byte read truncated any
     * record longer than that, and a truncated record is not JSON, so the
     * snapshot dropped out of the rebuilt index. The bound is what the
     * writer will produce at most; anything larger is not a record.
     */
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd == -1) return set_error_errno(ERR_NOT_FOUND, "reindex: cannot open %s", path);
    struct stat sb;
    if (fstat(fd, &sb) != 0) {
        int err = errno;
        close(fd);
        errno = err;
        return set_error_errno(ERR_IO, "reindex: cannot stat %s", path);
    }
    if (!S_ISREG(sb.st_mode) || sb.st_size < 0 || sb.st_size > SNAPSHOT_RECORD_MAX) {
        close(fd);
        return set_error(ERR_CORRUPT, "reindex: %s is not a snapshot record "
                         "(%lld bytes; at most %d expected)", path,
                         (long long)sb.st_size, SNAPSHOT_RECORD_MAX);
    }
    size_t want = (size_t)sb.st_size;
    char *buf = malloc(want + 1);
    if (!buf) { close(fd); return set_error(ERR_NOMEM, "reindex: out of memory"); }
    size_t n = 0;
    while (n < want) {
        ssize_t r = read(fd, buf + n, want - n);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) {
            int err = errno;
            free(buf); close(fd);
            errno = err;
            return set_error_errno(ERR_IO, "reindex: cannot read %s", path);
        }
        if (r == 0) break;
        n += (size_t)r;
    }
    close(fd);
    buf[n] = '\0';

    cutils_json_req_t *req = NULL;
    int parsed = json_req_parse(buf, n, &req);
    free(buf);
    if (parsed != 0 || !req)
        return set_error(ERR_CORRUPT, "reindex: %s is not valid JSON", path);

    char *root_hex = NULL, *label = NULL;
    /* Signed: the writer records time_t as is, and an unsigned read turned
     * any time before 1970 into 0. */
    int64_t ts = 0;
    int bad = json_req_get_str(req, "root", &root_hex) != 0 || !root_hex ||
              strlen(root_hex) != OBJECT_HASH_SIZE * 2;
    if (json_req_get_i64(req, "ts", &ts, INT64_MIN, INT64_MAX) != 0) ts = 0;
    if (json_req_get_str_opt(req, "label", &label) != 0) label = NULL;

    if (bad) {
        free(root_hex); free(label);
        json_req_free(req);
        return set_error(ERR_CORRUPT, "reindex: %s has no usable root hash", path);
    }

    hex_to_hash(root_hex, root);
    if (out_ts) *out_ts = ts;
    if (out_label) *out_label = label; else free(label);
    free(root_hex);
    json_req_free(req);
    return OK;
}

static status_t reindex_lists(store_t *store, cutils_db_t *db, int64_t repo_id,
                              int count);

static status_t reindex_snapshot_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                       int64_t snap_id, int lists,
                                       reindex_result_t *out);

/*
 * Standalone, a snapshot's chunk lists are walked too, so every chunk it
 * names has an object row: compaction keeps exactly the entries with rows,
 * and a chunk without one would be dropped from its pack. reindex_repo does
 * that once for every snapshot at the end instead.
 */
status_t reindex_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                          int64_t snap_id, reindex_result_t *out)
{
    return reindex_snapshot_inner(store, db, repo_id, snap_id, 1, out);
}

static status_t reindex_snapshot_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                       int64_t snap_id, int lists,
                                       reindex_result_t *out)
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
        char tsbuf[32], files[32], bytes[32], rhex[OBJECT_HASH_SIZE * 2 + 1];
        snprintf(tsbuf, sizeof(tsbuf), "%lld", (long long)ts);
        snprintf(files, sizeof(files), "%llu", (unsigned long long)res.files);
        snprintf(bytes, sizeof(bytes), "%llu", (unsigned long long)res.bytes);
        hash_hex(root, rhex);
        /* An upsert, not INSERT OR REPLACE: REPLACE deletes the row and
         * inserts a fresh one, which reset gfs_flags to its default of 0, so
         * a reindex quietly stripped every snapshot of the tiers the last
         * retention pass gave it. Everything the walk derives is
         * overwritten; what it cannot derive is kept. */
        if (db_execute_non_query(db,
                "INSERT INTO snapshots (repo_id, id, ts, label,"
                " file_count, total_size, completed_at, root_tree_hash,"
                " nodes_materialized)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)"
                " ON CONFLICT (repo_id, id) DO UPDATE SET"
                "  ts = excluded.ts, label = excluded.label,"
                "  file_count = excluded.file_count,"
                "  total_size = excluded.total_size,"
                "  completed_at = excluded.completed_at,"
                "  root_tree_hash = excluded.root_tree_hash,"
                "  nodes_materialized = 1",
                (const char *[]){ repo, snap, tsbuf, label ? label : "",
                                  files, bytes, tsbuf, rhex, NULL }, NULL) != 0)
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
    if (fst == OK && lists) {
        fst = reindex_lists(store, db, repo_id, 0);
        if (fst == OK) fst = fill_object_sizes(store, db, repo_id);
    }
    if (fst != OK) return fst;

    res.snapshots = 1;
    if (out) *out = res;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Pack index rebuild                                                    */
/* ------------------------------------------------------------------ */

/*
 * A pack's entries are collected in memory and written only once the whole
 * pack has been read. Writing them as they were enumerated meant a pack that
 * failed halfway had already replaced part of its rows, and the rebuild used
 * to begin by deleting every row for the repository - so one unreadable pack
 * lost its index entirely, while the job carried on and reported success.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t offset, comp, orig;
    uint32_t eidx;
} pack_row_t;

typedef struct {
    pack_row_t *rows;
    size_t      n, cap;
    int         oom;
} pack_entry_ctx_t;

static void pack_entry_seen(const pack_entry_info_t *info, void *vctx)
{
    pack_entry_ctx_t *c = vctx;
    if (c->oom) return;
    if (c->n == c->cap) {
        size_t ncap = c->cap ? c->cap * 2 : 1024;
        pack_row_t *nr = realloc(c->rows, ncap * sizeof(*nr));
        if (!nr) { c->oom = 1; return; }
        c->rows = nr;
        c->cap = ncap;
    }
    pack_row_t *r = &c->rows[c->n++];
    memcpy(r->hash, info->hash, OBJECT_HASH_SIZE);
    r->offset = info->entry_offset;
    r->comp   = info->compressed_size;
    r->orig   = info->uncompressed_size;
    r->eidx   = info->entry_index;
}

typedef struct {
    store_t     *store;
    cutils_db_t *db;
    const char  *repo;
    int          failed;      /* a database write failed: abandon the rebuild */
    uint64_t     packs;
    uint64_t     entries;
    uint64_t     unreadable;  /* packs on disk that could not be read */
    uint32_t    *seen;        /* every pack number on disk, readable or not */
    size_t       nseen, capseen;
} pack_scan_ctx_t;

static void pack_num_seen(uint32_t pack_num, void *vctx)
{
    pack_scan_ctx_t *c = vctx;
    if (c->failed) return;

    if (c->nseen == c->capseen) {
        size_t ncap = c->capseen ? c->capseen * 2 : 64;
        uint32_t *ns = realloc(c->seen, ncap * sizeof(*ns));
        if (!ns) { c->failed = 1; return; }
        c->seen = ns;
        c->capseen = ncap;
    }
    c->seen[c->nseen++] = pack_num;

    char name[64];
    snprintf(name, sizeof(name), "pack-%08u.dat", pack_num);

    uint32_t version = 0, count = 0;
    pack_entry_ctx_t ec;
    memset(&ec, 0, sizeof(ec));
    if (pack_enumerate_dat(c->store, name, &version, &count,
                           pack_entry_seen, &ec) != OK || ec.oom) {
        /* One unreadable pack must not abandon the rest, and must not lose
         * the rows the index already has for it either: they are left as
         * they were, and the reindex reports the pack as a failure. */
        log_warn("reindex: cannot read %s: %s", name,
                 ec.oom ? "out of memory" : err_msg());
        err_clear();
        free(ec.rows);
        c->unreadable++;
        return;
    }

    char pid[32];
    snprintf(pid, sizeof(pid), "%u", pack_num);
    if (db_execute_non_query(c->db,
            "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
            (const char *[]){ c->repo, pid, NULL }, NULL) != 0) {
        free(ec.rows);
        c->failed = 1;
        return;
    }
    for (size_t i = 0; i < ec.n; i++) {
        const pack_row_t *r = &ec.rows[i];
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(r->hash, hex);
        char off[32], comp[32], orig[32], eidx[16];
        snprintf(off,  sizeof(off),  "%llu", (unsigned long long)r->offset);
        snprintf(comp, sizeof(comp), "%llu", (unsigned long long)r->comp);
        snprintf(orig, sizeof(orig), "%llu", (unsigned long long)r->orig);
        snprintf(eidx, sizeof(eidx), "%u",   r->eidx);
        if (db_execute_non_query(c->db,
                "INSERT OR REPLACE INTO pack_objects"
                " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ c->repo, hex, pid, off, comp, orig, eidx, NULL },
                NULL) != 0) {
            free(ec.rows);
            c->failed = 1;
            return;
        }
    }
    size_t nentries = ec.n;
    free(ec.rows);

    char path[PATH_MAX];
    uint64_t size = 0;
    if (pack_dat_path_resolve(path, sizeof(path), store_root(c->store), pack_num) == 0) {
        struct stat sb;
        if (stat(path, &sb) == 0) size = (uint64_t)sb.st_size;
    }

    char szbuf[32], ver[16];
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
    c->entries += nentries;
}

/* Drop the rows of packs that are no longer on disk at all. Rows of a pack
 * that is present but unreadable stay, for the reason in pack_num_seen. */
static int drop_vanished_packs(pack_scan_ctx_t *c)
{
    db_result_t *r = NULL;
    if (db_execute(c->db, "SELECT id FROM packs WHERE repo_id = ?",
                   (const char *[]){ c->repo, NULL }, &r) != 0) {
        db_result_free(r);
        return -1;
    }
    int rc = 0;
    for (int i = 0; r && i < r->nrows && rc == 0; i++) {
        uint32_t id = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        int present = 0;
        for (size_t k = 0; k < c->nseen && !present; k++)
            present = (c->seen[k] == id);
        if (present) continue;
        const char *pid = r->rows[i][0];
        if (db_execute_non_query(c->db,
                "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
                (const char *[]){ c->repo, pid, NULL }, NULL) != 0 ||
            db_execute_non_query(c->db,
                "DELETE FROM packs WHERE repo_id = ? AND id = ?",
                (const char *[]){ c->repo, pid, NULL }, NULL) != 0)
            rc = -1;
    }
    db_result_free(r);
    return rc;
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

    /*
     * Finish any pack removal a crash interrupted before reading the packs.
     *
     * A .deleting marker means the packs it names have been replaced by
     * packs already installed and announced (gc's rewrite, coalescing), and
     * only the unlinks are outstanding. Rebuilding rows from both sets would
     * point some objects at the doomed copies, by whatever order the
     * directory walk hands packs back, and the next gc's replay of the
     * marker would then delete those rows with the files: objects present
     * on disk and missing from the index. Replayed first, there is only one
     * copy to index. Before the transaction below, because the removal
     * callback begins its own.
     */
    pack_resume_removals(store);

    /* One transaction for the whole rebuild. Without it every row is its own
     * committed transaction, and a pack of a few thousand entries spends all
     * its time in SQLite's per-statement bookkeeping rather than in the work.
     * It is also the correct scope: a half-rebuilt pack index should never be
     * observable. */
    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "reindex: cannot begin pack index rebuild");

    pack_scan_ctx_t c;
    memset(&c, 0, sizeof(c));
    c.store = store;
    c.db    = db;
    c.repo  = repo;

    status_t st = pack_list(store, pack_num_seen, &c);
    if (st == OK && !c.failed && drop_vanished_packs(&c) != 0) c.failed = 1;
    free(c.seen);
    if (st != OK || c.failed) {
        db_txn_rollback(db);
        return st != OK ? st
                        : set_error(ERR_IO, "reindex: cannot rebuild pack index");
    }
    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "reindex: cannot commit pack index");

    res->packs            = c.packs;
    res->pack_entries     = c.entries;
    res->packs_unreadable = c.unreadable;
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

/*
 * Chunk lists (format 3), after the walk and the sizing.
 *
 * The walk records a file's content under type FILE, because finding out
 * whether it is a list would mean reading each object's header, measured
 * at 57 ms a packed object. Once sizes are filled the index says which
 * content can be a list: a single object's payload is the file, so its
 * size equals the node's, and a list's payload is 48 bytes an entry. Only
 * content whose sizes disagree is asked, and each list found is recorded
 * with its real type and walked, recording every sub-list and chunk it
 * names. Tree lists were typed by the walk.
 *
 * With count set (a full reindex, after recompute_refcounts), list
 * internals are counted the way gc's mark counts them: once per distinct
 * list naming them, a list's subtree walked only the first time it is
 * reached. A one-snapshot reindex only records them; the next gc counts.
 */
static status_t note_inner(cutils_db_t *db, const char *repo,
                           const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                           int count, int64_t *out_refs)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1], t[8];
    hash_hex(hash, hex);
    snprintf(t, sizeof(t), "%u", type);
    *out_refs = 0;
    if (db_execute_non_query(db,
            "INSERT INTO objects (repo_id, hash, refcount, location, type)"
            " VALUES (?, ?, 0, 'loose', ?)"
            " ON CONFLICT(repo_id, hash) DO UPDATE SET type = excluded.type",
            (const char *[]){ repo, hex, t, NULL }, NULL) != 0)
        return set_error(ERR_IO, "reindex: cannot record object");
    if (!count) return OK;
    db_result_t *r = NULL;
    if (db_execute(db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ? RETURNING refcount",
            (const char *[]){ repo, hex, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "reindex: cannot count object");
    }
    if (r && r->nrows > 0) *out_refs = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return OK;
}

static status_t walk_list_inner(store_t *store, cutils_db_t *db, const char *repo,
                                const uint8_t root[OBJECT_HASH_SIZE], uint8_t leaf_type,
                                int count)
{
    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    status_t st = chunklist_iter_open(&ld, root, leaf_type, 1, NULL, &it);
    for (int done = 0; st == OK && !done; ) {
        chunklist_item_t x;
        st = chunklist_iter_next(it, &x, &done);
        if (st != OK || done) break;
        int64_t refs = 0;
        st = note_inner(db, repo, x.hash,
                        x.is_list ? OBJECT_TYPE_CHUNKLIST : leaf_type, count, &refs);
        if (st == OK && x.is_list && count && refs > 1) chunklist_iter_skip(it);
    }
    chunklist_iter_close(it);
    return st;
}

static status_t reindex_lists(store_t *store, cutils_db_t *db, int64_t repo_id,
                              int count)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT DISTINCT n.content_hash FROM nodes n"
            "  JOIN objects o ON o.repo_id = n.repo_id AND o.hash = n.content_hash"
            " WHERE n.repo_id = ? AND n.type = 1 AND o.type IN (1, 6)"
            "   AND (o.orig_size IS NULL OR o.orig_size != n.size OR o.type = 6)",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "reindex: cannot list chunk lists");
    }
    if (db_txn_begin(db) != OK) {
        db_result_free(r);
        return set_error(ERR_IO, "reindex: cannot begin chunk lists");
    }
    status_t st = OK;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        hex_to_hash(r->rows[i][0], h);
        uint64_t size = 0;
        uint8_t type = 0;
        if (object_get_info(store, h, &size, &type) != OK) {
            /* Absent or unreadable: verify reports it; nothing to walk. */
            err_clear();
            continue;
        }
        if (type != OBJECT_TYPE_CHUNKLIST) continue;
        int64_t refs = 0;
        st = note_inner(db, repo, h, OBJECT_TYPE_CHUNKLIST, 0, &refs);
        if (st == OK) st = walk_list_inner(store, db, repo, h, OBJECT_TYPE_FILE, count);
    }
    db_result_free(r);

    /* Directories split into fragments: typed by the walk, counted here. */
    r = NULL;
    if (st == OK && count &&
        db_execute(db,
            "SELECT DISTINCT o.hash FROM objects o WHERE o.repo_id = ?1 AND o.type = 6"
            "   AND (EXISTS (SELECT 1 FROM nodes n WHERE n.repo_id = ?1"
            "                 AND n.subtree_hash = o.hash)"
            "        OR EXISTS (SELECT 1 FROM snapshots s WHERE s.repo_id = ?1"
            "                    AND s.root_tree_hash = o.hash))",
            (const char *[]){ repo, NULL }, &r) != 0) {
        st = set_error(ERR_IO, "reindex: cannot list tree lists");
    }
    for (int i = 0; st == OK && r && i < r->nrows; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        hex_to_hash(r->rows[i][0], h);
        st = walk_list_inner(store, db, repo, h, OBJECT_TYPE_TREE, count);
    }
    db_result_free(r);

    if (st != OK) { db_txn_rollback(db); return st; }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "reindex: cannot commit chunk lists");
    return OK;
}

/*
 * Count every object's references from the rebuilt index.
 *
 * A reindex left every refcount at zero, so the object listing said nothing
 * referenced an object the refs endpoint showed two nodes using. GC does not
 * trust the column - it zeroes and re-marks from the trees on every run - so
 * this is not what keeps a post-reindex GC from deleting live objects; it is
 * what makes the column true between reindex and the next GC.
 *
 * The count is the one GC's mark produces: one per visit of a tree,
 * reaching it as a snapshot root or as a directory's subtree, plus one per
 * entry for its content, xattr and ACL objects. A tree shared by ten
 * snapshots counts ten. Every node row carries exactly those hashes, so the
 * count is a single grouped pass over the rows just written rather than a
 * second walk of the trees.
 */
static status_t recompute_refcounts(cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "reindex: cannot begin refcount pass");
    int failed =
        db_execute_non_query(db, "UPDATE objects SET refcount = 0 WHERE repo_id = ?",
                             (const char *[]){ repo, NULL }, NULL) != 0 ||
        db_execute_non_query(db,
            "WITH refs(hash) AS ("
            "  SELECT root_tree_hash FROM snapshots"
            "   WHERE repo_id = ?1 AND root_tree_hash IS NOT NULL"
            "  UNION ALL SELECT content_hash FROM nodes"
            "   WHERE repo_id = ?1 AND content_hash IS NOT NULL"
            "  UNION ALL SELECT subtree_hash FROM nodes"
            "   WHERE repo_id = ?1 AND subtree_hash IS NOT NULL"
            "  UNION ALL SELECT xattr_hash FROM nodes"
            "   WHERE repo_id = ?1 AND xattr_hash IS NOT NULL"
            "  UNION ALL SELECT acl_hash FROM nodes"
            "   WHERE repo_id = ?1 AND acl_hash IS NOT NULL),"
            " counts(hash, n) AS (SELECT hash, COUNT(*) FROM refs GROUP BY hash)"
            " UPDATE objects SET refcount = counts.n FROM counts"
            " WHERE objects.repo_id = ?1 AND objects.hash = counts.hash",
            (const char *[]){ repo, NULL }, NULL) != 0;
    if (failed) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "reindex: cannot recompute refcounts");
    }
    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "reindex: cannot commit refcounts");
    return OK;
}

status_t reindex_repo(store_t *store, cutils_db_t *db, int64_t repo_id,
                      atomic_int *cancel, reindex_result_t *out)
{
    char dir[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s/snapshots", store_root(store)) >= (int)sizeof(dir))
        return set_error(ERR_IO, "reindex: path too long");

    /*
     * Without the key no tree can be read, so every snapshot would fail and
     * the reindex would rebuild nothing. Refused before anything is touched;
     * the job layer says which repository and how to supply the key.
     */
    if (store_needs_key(store))
        return set_error(ERR_INVALID,
                         "reindex: repository is encrypted and no key is loaded; "
                         "nothing was changed");

    DIR *d = opendir(dir);
    if (!d) return set_error_errno(ERR_IO, "reindex: cannot open %s", dir);

    reindex_result_t total;
    memset(&total, 0, sizeof(total));
    status_t st = OK;
    long long first_failed = 0;
    char first_err[256] = "";

    /* Packs first: the tree walk resolves packed objects through these rows,
     * so rebuilding them afterwards would be too late. */
    status_t pst = rebuild_pack_index(store, db, repo_id, &total);
    if (pst != OK) {
        closedir(d);
        return pst;
    }

    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (cancel_requested(cancel)) { st = set_error(ERR_BUSY, "reindex cancelled"); break; }
        char *end = NULL;
        long long id = strtoll(e->d_name, &end, 10);
        if (id <= 0 || !end || *end != '\0') continue;

        reindex_result_t one;
        memset(&one, 0, sizeof(one));
        status_t one_st = reindex_snapshot_inner(store, db, repo_id, id, 0, &one);
        if (one_st != OK) {
            /* One unreadable snapshot must not abandon the rest: partial
             * recovery of a damaged repository is the whole point. Its old
             * rows survive, since reindex_snapshot replaces a snapshot only
             * in a transaction that commits on success. But it is a failure,
             * and the run reports it as one below. */
            log_warn("reindex: snapshot %lld failed: %s", id, err_msg());
            if (total.snapshots_failed == 0) {
                first_failed = id;
                snprintf(first_err, sizeof(first_err), "%s", err_msg());
            }
            total.snapshots_failed++;
            err_clear();
            continue;
        }
        total.snapshots += one.snapshots;
        total.nodes     += one.nodes;
        total.files     += one.files;
        total.bytes     += one.bytes;
        total.objects   += one.objects;
        total.trees_read += one.trees_read;
    }
    closedir(d);
    if (st != OK) return st;

    /* Every snapshot has contributed its hashes by now, so each distinct
     * object is sized exactly once. */
    st = fill_object_sizes(store, db, repo_id);
    if (st != OK) return st;
    st = recompute_refcounts(db, repo_id);
    if (st != OK) return st;
    /* Chunk lists need sizes to be found and refcounts to add to; what they
     * name was just recorded, so it is sized in a second, small pass. */
    st = reindex_lists(store, db, repo_id, 1);
    if (st == OK) st = fill_object_sizes(store, db, repo_id);
    if (st != OK) return st;

    /* Tags last: they attach to snapshot rows, which now exist. */
    status_t tst = repo_tags_restore(db, repo_id, store_root(store),
                                     &total.tags_restored, &total.tags_skipped);
    char tags_err[256] = "";
    if (tst != OK) {
        snprintf(tags_err, sizeof(tags_err), "%s", err_msg());
        err_clear();
    }

    log_info("reindex: %llu snapshot(s), %llu nodes, %llu trees read, "
             "%llu object(s), %llu pack(s) with %llu entries, %llu tag(s) restored",
             (unsigned long long)total.snapshots, (unsigned long long)total.nodes,
             (unsigned long long)total.trees_read, (unsigned long long)total.objects,
             (unsigned long long)total.packs, (unsigned long long)total.pack_entries,
             (unsigned long long)total.tags_restored);
    if (out) *out = total;

    /*
     * A reindex that could not read everything has failed, whatever it did
     * manage. It used to report completed after failing every snapshot, so
     * the job list said the index was rebuilt while it held nothing; an
     * operator recovering from a lost database had no reason to look
     * further until a restore found no snapshot.
     */
    if (total.snapshots_failed > 0)
        return set_error(ERR_CORRUPT,
                         "reindex: %llu of %llu snapshot(s) could not be read "
                         "(snapshot %lld: %s); the readable ones were indexed, "
                         "the rest keep their previous index rows",
                         (unsigned long long)total.snapshots_failed,
                         (unsigned long long)(total.snapshots + total.snapshots_failed),
                         first_failed, first_err);
    if (total.packs_unreadable > 0)
        return set_error(ERR_CORRUPT,
                         "reindex: %llu pack(s) could not be read and keep their "
                         "previous index rows; run verify",
                         (unsigned long long)total.packs_unreadable);
    /* Loud, because tags are the retention guard: an index rebuilt without
     * them leaves every tagged snapshot open to the next gfs pass. */
    if (tags_err[0])
        return set_error(ERR_CORRUPT,
                         "reindex: the index was rebuilt but the repository's tag "
                         "list could not be read (%s); tagged snapshots are not "
                         "protected until they are tagged again", tags_err);
    return OK;
}
