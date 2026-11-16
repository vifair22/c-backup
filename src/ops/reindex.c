#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "util.h"
#include "reindex.h"

#include "chunklist.h"
#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "db_txn.h"
#include "db_typed.h"
#include "node_index.h"
#include "repo_tags.h"
#include "snap_record.h"
#include "snapshot_ids.h"

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

/*
 * Trees this run has rebuilt, so each distinct tree is read and indexed once
 * however many snapshots hold it (docs/index-reuse.md). A snapshot that
 * fails rolls its transaction back, and with it the rows of every tree it
 * rebuilt, so those trees leave the memo too: `keys` holds them in insertion
 * order and `committed` marks how many survived the last commit; the table
 * is rebuilt from the survivors.
 */
typedef struct {
    uint8_t  *keys;       /* n * OBJECT_HASH_SIZE, insertion order */
    size_t    n, cap, committed;
    uint32_t *slots;      /* index + 1 into keys, 0 empty */
    uint32_t  mask;
} tree_memo_t;

static uint32_t memo_slot(const tree_memo_t *m, const uint8_t h[OBJECT_HASH_SIZE])
{
    uint32_t v;
    memcpy(&v, h, sizeof(v));
    return v & m->mask;
}

static int memo_has(const tree_memo_t *m, const uint8_t h[OBJECT_HASH_SIZE])
{
    if (!m->slots) return 0;
    for (uint32_t i = memo_slot(m, h); m->slots[i]; i = (i + 1) & m->mask)
        if (memcmp(m->keys + (size_t)(m->slots[i] - 1) * OBJECT_HASH_SIZE, h,
                   OBJECT_HASH_SIZE) == 0)
            return 1;
    return 0;
}

static int memo_rehash(tree_memo_t *m, uint32_t want)
{
    uint32_t cap = 64;
    while (cap < want * 2) cap <<= 1;
    uint32_t *s = calloc(cap, sizeof(*s));
    if (!s) return -1;
    free(m->slots);
    m->slots = s;
    m->mask = cap - 1;
    for (size_t k = 0; k < m->n; k++) {
        uint32_t i = memo_slot(m, m->keys + k * OBJECT_HASH_SIZE);
        while (m->slots[i]) i = (i + 1) & m->mask;
        m->slots[i] = (uint32_t)k + 1;
    }
    return 0;
}

static int memo_add(tree_memo_t *m, const uint8_t h[OBJECT_HASH_SIZE])
{
    if (m->n == m->cap) {
        size_t nc = m->cap ? m->cap * 2 : 256;
        uint8_t *k = realloc(m->keys, nc * OBJECT_HASH_SIZE);
        if (!k) return -1;
        m->keys = k;
        m->cap = nc;
    }
    memcpy(m->keys + m->n * OBJECT_HASH_SIZE, h, OBJECT_HASH_SIZE);
    m->n++;
    if (!m->slots || m->n * 2 > (size_t)m->mask + 1)
        return memo_rehash(m, (uint32_t)m->n);
    uint32_t i = memo_slot(m, h);
    while (m->slots[i]) i = (i + 1) & m->mask;
    m->slots[i] = (uint32_t)m->n;
    return 0;
}

static void memo_commit(tree_memo_t *m) { m->committed = m->n; }

static void memo_rollback(tree_memo_t *m)
{
    if (m->n == m->committed) return;
    m->n = m->committed;
    if (memo_rehash(m, (uint32_t)(m->n ? m->n : 1)) != 0) {
        /* Without a table nothing is remembered, which costs a reread of
         * trees already rebuilt and is never wrong. */
        free(m->slots);
        m->slots = NULL;
        m->n = m->committed = 0;
    }
}

static void memo_free(tree_memo_t *m)
{
    free(m->keys);
    free(m->slots);
    memset(m, 0, sizeof(*m));
}

typedef struct {
    store_t     *store;
    cutils_db_t *db;
    int64_t      repo_id;
    int64_t      snap_id;
    reindex_result_t *res;
    tree_memo_t *memo;
} reindex_ctx_t;

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

    db_param_t p[] = { DB_INT64(ctx->repo_id), DB_HASH(hash), DB_INT64(type) };
    if (db_execute_non_query_typed(ctx->db,
            "INSERT INTO objects (repo_id, hash, refcount, location, type)"
            " VALUES (?, ?, 0, 'absent', ?)"
            " ON CONFLICT(repo_id, hash) DO NOTHING",
            p, DB_NPARAMS(p), NULL) != 0)
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

/* A subdirectory to descend into: its name and tree. */
typedef struct {
    char    *name;
    uint8_t  tree[OBJECT_HASH_SIZE];
} subdir_t;

static void subdirs_free(subdir_t *s, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) free(s[i].name);
    free(s);
}

/*
 * Read one tree from the repository, record every object it names, and
 * rebuild its entry rows. Returns its subdirectories and file totals.
 *
 * Node ids are minted fresh. Nothing in the repository refers to them,
 * which is precisely what allows the index to be thrown away and rebuilt:
 * had they been written into the trees, a rebuild would have to preserve
 * them and the trees would stop being content-addressable.
 */
static status_t rebuild_tree(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                             subdir_t **out_dirs, uint32_t *out_ndirs)
{
    /*
     * The directory's objects are recorded as the iterator loads them. The
     * tree is an object too, and nothing else will record it: no entry
     * points at its own directory. Miss it and GC sweeps the manifest.
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

    /* Rebuilt, not trusted: whatever rows the index held for this tree are
     * replaced by what the repository says. */
    uint64_t rows = 0;
    st = node_index_put_tree(ctx->db, ctx->repo_id, tree_hash, entries, count, 1, &rows);
    ctx->res->nodes += rows;

    subdir_t *dirs = NULL;
    uint32_t ndirs = 0;
    for (uint32_t i = 0; i < count && st == OK; i++) {
        manifest_entry_t *e = &entries[i];
        int is_dir = (e->type == NODE_TYPE_DIR);
        int have_target = !hash_is_zero(e->target_hash);

        /* Subtree objects are recorded when the subtree is read, so only
         * content needs noting here. */
        if (have_target && !is_dir) st = note_object(ctx, e->target_hash, OBJECT_TYPE_FILE);
        if (st == OK && !hash_is_zero(e->xattr_hash))
            st = note_object(ctx, e->xattr_hash, OBJECT_TYPE_XATTR);
        if (st == OK && !hash_is_zero(e->acl_hash))
            st = note_object(ctx, e->acl_hash, OBJECT_TYPE_ACL);
        if (st != OK || !is_dir || !have_target) continue;

        if (!dirs) {
            dirs = calloc(count, sizeof(*dirs));
            if (!dirs) { st = set_error(ERR_NOMEM, "reindex: subdirectories"); break; }
        }
        dirs[ndirs].name = strdup(e->name);
        memcpy(dirs[ndirs].tree, e->target_hash, OBJECT_HASH_SIZE);
        if (!dirs[ndirs++].name) st = set_error(ERR_NOMEM, "reindex: subdirectories");
    }
    manifest_free_entries(entries, count);
    if (st != OK) { subdirs_free(dirs, ndirs); return st; }
    *out_dirs = dirs;
    *out_ndirs = ndirs;
    return OK;
}

/* A tree this run already rebuilt: its subdirectories from its rows. */
static status_t indexed_subdirs(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                                subdir_t **out_dirs, uint32_t *out_ndirs)
{
    db_param_t p[] = { DB_INT64(ctx->repo_id), DB_HASH(tree_hash) };
    db_result_t *r = NULL;
    if (db_execute_typed(ctx->db,
            "SELECT name, lower(hex(subtree_hash)) FROM nodes"
            " WHERE repo_id = ? AND tree_hash = ? AND type = 2"
            "   AND subtree_hash IS NOT NULL",
            p, DB_NPARAMS(p), &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "reindex: cannot read a tree's subdirectories");
    }
    subdir_t *dirs = r->nrows ? calloc((size_t)r->nrows, sizeof(*dirs)) : NULL;
    if (r->nrows && !dirs) { db_result_free(r); return set_error(ERR_NOMEM, "reindex: subdirectories"); }
    uint32_t n = 0;
    status_t st = OK;
    for (int i = 0; i < r->nrows && st == OK; i++) {
        dirs[n].name = strdup(r->rows[i][0]);
        hex_to_hash(r->rows[i][1], dirs[n].tree);
        if (!dirs[n++].name) st = set_error(ERR_NOMEM, "reindex: subdirectories");
    }
    db_result_free(r);
    if (st != OK) { subdirs_free(dirs, n); return st; }
    *out_dirs = dirs;
    *out_ndirs = n;
    return OK;
}

/* The regular files directly in a tree and their logical bytes, as the
 * tree's completion row recorded them. */
static status_t tree_totals(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE])
{
    db_param_t p[] = { DB_INT64(ctx->repo_id), DB_HASH(tree_hash) };
    db_result_t *r = NULL;
    if (db_execute_typed(ctx->db,
            "SELECT files, bytes FROM trees WHERE repo_id = ? AND hash = ?",
            p, DB_NPARAMS(p), &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_IO, "reindex: a rebuilt tree has no totals");
    }
    ctx->res->files += strtoull(r->rows[0][0], NULL, 10);
    ctx->res->bytes += strtoull(r->rows[0][1], NULL, 10);
    db_result_free(r);
    return OK;
}

/*
 * One directory of the snapshot: its tree rebuilt if this run has not
 * rebuilt it yet, then a skeleton row for each subdirectory and the walk
 * into it. dir_id is this directory's own skeleton row.
 */
static status_t walk_dir(reindex_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                         int64_t dir_id, const char *path, int depth)
{
    if (depth > 4096)
        return set_error(ERR_CORRUPT, "reindex: directory chain deeper than 4096 at %s", path);
    subdir_t *dirs = NULL;
    uint32_t ndirs = 0;
    status_t st;
    if (memo_has(ctx->memo, tree_hash)) {
        st = indexed_subdirs(ctx, tree_hash, &dirs, &ndirs);
    } else {
        st = rebuild_tree(ctx, tree_hash, &dirs, &ndirs);
        if (st == OK && memo_add(ctx->memo, tree_hash) != 0)
            st = set_error(ERR_NOMEM, "reindex: tree memo");
    }
    if (st == OK) st = tree_totals(ctx, tree_hash);

    int64_t first = 0;
    if (st == OK) st = node_ids_reserve(ctx->db, ndirs, &first);
    for (uint32_t i = 0; i < ndirs && st == OK; i++) {
        char *sub = NULL;
        if (asprintf(&sub, "%s/%s", path, dirs[i].name) < 0) {
            st = set_error(ERR_NOMEM, "reindex: path");
            break;
        }
        int64_t id = first + (int64_t)i;
        st = node_index_put_dir(ctx->db, ctx->repo_id, ctx->snap_id, id, dir_id,
                                dirs[i].name, sub, dirs[i].tree);
        if (st == OK) st = walk_dir(ctx, dirs[i].tree, id, sub, depth + 1);
        free(sub);
    }
    subdirs_free(dirs, ndirs);
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
                                       int64_t snap_id, int lists, tree_memo_t *memo,
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
    tree_memo_t memo;
    memset(&memo, 0, sizeof(memo));
    status_t st = reindex_snapshot_inner(store, db, repo_id, snap_id, 1, &memo, out);
    memo_free(&memo);
    return st;
}

static status_t reindex_snapshot_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                       int64_t snap_id, int lists, tree_memo_t *memo,
                                       reindex_result_t *out)
{
    uint8_t root[OBJECT_HASH_SIZE];
    int64_t ts = 0;
    char *label = NULL;
    status_t st = read_snapshot_record(store, snap_id, root, &ts, &label);
    if (st != OK) return st;

    reindex_result_t res;
    memset(&res, 0, sizeof(res));

    if (db_txn_begin(db) != OK) {
        free(label);
        return set_error(ERR_IO, "reindex: begin");
    }

    /*
     * Replacing, not merging: the snapshot's skeleton is rebuilt from its
     * record, in one transaction, so a failure leaves the old one. Trees it
     * no longer reaches keep their rows until the sweep at the end of a full
     * reindex (or the next gc), since another snapshot rebuilt later in the
     * run may well need them.
     */
    st = node_index_drop_snapshot(db, repo_id, snap_id, 0);

    reindex_ctx_t ctx = { store, db, repo_id, snap_id, &res, memo };
    int64_t root_id = 0;
    if (st == OK) st = node_ids_reserve(db, 1, &root_id);
    if (st == OK) st = node_index_put_dir(db, repo_id, snap_id, root_id, 0, "", "", root);
    if (st == OK) st = walk_dir(&ctx, root, root_id, "", 0);

    if (st == OK) {
        /* An upsert, not INSERT OR REPLACE: REPLACE deletes the row and
         * inserts a fresh one, which reset gfs_flags to its default of 0, so
         * a reindex quietly stripped every snapshot of the tiers the last
         * retention pass gave it. Everything the walk derives is
         * overwritten; what it cannot derive is kept. */
        db_param_t sp[] = { DB_INT64(repo_id), DB_INT64(snap_id), DB_INT64(ts),
                            DB_TEXT(label ? label : ""), DB_INT64((int64_t)res.files),
                            DB_INT64((int64_t)res.bytes), DB_INT64(ts), DB_HASH(root) };
        if (db_execute_non_query_typed(db,
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
                sp, DB_NPARAMS(sp), NULL) != 0)
            st = set_error(ERR_IO, "reindex: snapshot %lld failed", (long long)snap_id);
    }

    free(label);

    if (st != OK) {
        db_txn_rollback(db);
        memo_rollback(memo);
        return st;
    }
    if (db_txn_commit(db) != OK) {
        memo_rollback(memo);
        return set_error(ERR_IO, "reindex: commit");
    }
    memo_commit(memo);

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
    snprintf(name, sizeof(name), "pack-%010u.dat", pack_num);

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
        db_param_t pp[] = { DB_TEXT(c->repo), DB_HASH(r->hash), DB_INT64(pack_num),
                            DB_INT64((int64_t)r->offset), DB_INT64((int64_t)r->comp),
                            DB_INT64((int64_t)r->orig), DB_INT64(r->eidx) };
        if (db_execute_non_query_typed(c->db,
                "INSERT OR REPLACE INTO pack_objects"
                " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                pp, DB_NPARAMS(pp), NULL) != 0) {
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

    /* Every object lives in a pack, so whatever is still unsized is
     * referenced by a tree and held by no pack: absent. The row stays, so
     * verify reports it missing, and the count is said here. */
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT COUNT(*) FROM objects WHERE repo_id = ? AND phys_size IS NULL",
            (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1) {
        long long absent = strtoll(r->rows[0][0], NULL, 10);
        if (absent > 0)
            log_warn("reindex: %lld referenced object(s) are in no pack; "
                     "verify will report them missing", absent);
    }
    db_result_free(r);
    (void)store;

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
    *out_refs = 0;
    db_param_t p[] = { DB_TEXT(repo), DB_HASH(hash), DB_INT64(type) };
    if (db_execute_non_query_typed(db,
            "INSERT INTO objects (repo_id, hash, refcount, location, type)"
            " VALUES (?, ?, 0, 'loose', ?)"
            " ON CONFLICT(repo_id, hash) DO UPDATE SET type = excluded.type",
            p, DB_NPARAMS(p), NULL) != 0)
        return set_error(ERR_IO, "reindex: cannot record object");
    if (!count) return OK;
    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ? RETURNING refcount",
            p, 2, &r) != 0) {
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
            "SELECT DISTINCT lower(hex(n.content_hash)) FROM nodes n"
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
            "SELECT DISTINCT lower(hex(o.hash)) FROM objects o"
            " WHERE o.repo_id = ?1 AND o.type = 6"
            "   AND EXISTS (SELECT 1 FROM snap_dirs d WHERE d.repo_id = ?1"
            "                AND d.tree_hash = o.hash)",
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
 * entry for its content, xattr and ACL objects, per visit. A tree shared by
 * ten snapshots counts ten. Every visit is a skeleton row (snap_dirs), and a
 * tree's entries are its rows in nodes, so the count is the skeleton's
 * instances per tree times what each tree's entries name: one grouped pass,
 * not a second walk of the trees.
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
            "WITH inst(h, k) AS ("
            "  SELECT tree_hash, COUNT(*) FROM snap_dirs WHERE repo_id = ?1"
            "   GROUP BY tree_hash),"
            " refs(hash, k) AS ("
            "  SELECT h, k FROM inst"
            "  UNION ALL SELECT n.content_hash, inst.k FROM inst"
            "   JOIN nodes n ON n.repo_id = ?1 AND n.tree_hash = inst.h"
            "   WHERE n.content_hash IS NOT NULL"
            "  UNION ALL SELECT n.xattr_hash, inst.k FROM inst"
            "   JOIN nodes n ON n.repo_id = ?1 AND n.tree_hash = inst.h"
            "   WHERE n.xattr_hash IS NOT NULL"
            "  UNION ALL SELECT n.acl_hash, inst.k FROM inst"
            "   JOIN nodes n ON n.repo_id = ?1 AND n.tree_hash = inst.h"
            "   WHERE n.acl_hash IS NOT NULL),"
            " counts(hash, n) AS (SELECT hash, SUM(k) FROM refs GROUP BY hash)"
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

/*
 * Bring the index's snapshot-id mark up to everything the repository says
 * was ever issued (rule 2 of snapshot_ids.h). The file is the record that
 * survives a lost database; the highest record covers a file that is
 * missing or lagging; the index's own value covers a file and records that
 * both lost the top. Whichever is largest wins everywhere, and a file that
 * was missing, damaged or behind is rewritten and the run says so.
 */
static status_t restore_snapshot_mark(store_t *store, cutils_db_t *db, int64_t repo_id,
                                      long long highest_record, reindex_result_t *total)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    int64_t file_mark = 0;
    int file_ok = 1;
    if (snapshot_ids_read_repo(store_root(store), &file_mark) != OK) {
        log_warn("reindex: %s", err_msg());
        err_clear();
        file_mark = 0;
        file_ok = 0;
    }

    int64_t idx_mark = 0;
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT last_id FROM snapshot_id_seq WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        idx_mark = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);

    int64_t mark = file_mark;
    if ((int64_t)highest_record > mark) mark = (int64_t)highest_record;
    if (idx_mark > mark) mark = idx_mark;

    char m[32];
    snprintf(m, sizeof(m), "%lld", (long long)mark);
    if (db_execute_non_query(db,
            "INSERT INTO snapshot_id_seq (repo_id, last_id) VALUES (?, ?)"
            " ON CONFLICT(repo_id) DO UPDATE SET last_id = MAX(last_id, excluded.last_id)",
            (const char *[]){ repo, m, NULL }, NULL) != 0)
        return set_error(ERR_IO, "reindex: cannot record the snapshot-id mark");

    if (!file_ok || file_mark < mark) {
        status_t st = snapshot_ids_raise_repo(store_root(store), mark, NULL, NULL);
        if (st != OK) return st;
        log_warn("reindex: the repository's snapshot-ids file was %s; rewritten at %lld",
                 file_ok ? "behind its records or the index" : "missing or damaged",
                 (long long)mark);
        total->snapshot_ids_repaired = 1;
    }
    total->snapshot_mark = mark;
    return OK;
}

/*
 * A backup that stopped while writing its skeleton leaves a marker naming
 * the snapshot (backup.c, index_skeleton). A reindex has just rebuilt every
 * snapshot whose record exists, so a marker for one of those now names a
 * complete skeleton: left behind, the next backup would drop it. Markers
 * for snapshots with no row are left for that backup, which drops the
 * partial skeleton they name.
 */
static status_t drop_pending_markers(cutils_db_t *db, int64_t repo_id)
{
    db_param_t p[] = { DB_INT64(repo_id) };
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "reindex: begin");
    if (db_execute_non_query_typed(db,
            "DELETE FROM recovery_state WHERE repo_id = ?1"
            "   AND key LIKE 'index-pending-%'"
            "   AND EXISTS (SELECT 1 FROM snapshots s WHERE s.repo_id = ?1"
            "                AND s.id = CAST(recovery_state.value AS INTEGER))",
            p, DB_NPARAMS(p), NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "reindex: cannot clear pending skeleton markers");
    }
    return db_txn_commit(db) == OK ? OK : set_error(ERR_IO, "reindex: commit");
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
    /* Each distinct tree is rebuilt once per run, whichever snapshot meets
     * it first; the rest of the snapshots holding it only add skeleton
     * rows. */
    tree_memo_t memo;
    memset(&memo, 0, sizeof(memo));
    long long first_failed = 0;
    long long highest_record = 0;
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
        if (id > highest_record) highest_record = id;

        reindex_result_t one;
        memset(&one, 0, sizeof(one));
        status_t one_st = reindex_snapshot_inner(store, db, repo_id, id, 0, &memo, &one);
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
    memo_free(&memo);
    if (st != OK) return st;

    /* Trees no snapshot's skeleton reaches any more, and anything a stopped
     * run left half-written. Skeletons of snapshots whose rebuild failed
     * keep theirs, and with them the trees they need. */
    st = node_index_sweep(db, repo_id, NULL);
    if (st != OK) return st;
    st = drop_pending_markers(db, repo_id);
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
    if (st == OK) st = restore_snapshot_mark(store, db, repo_id, highest_record, &total);
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
