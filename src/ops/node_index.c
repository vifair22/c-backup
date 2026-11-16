#define _POSIX_C_SOURCE 200809L

#include "node_index.h"

#include "db_typed.h"
#include "db_txn.h"
#include "util.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

int node_hex_to_hash(const char *s, uint8_t out[OBJECT_HASH_SIZE])
{
    if (!s || strlen(s) != OBJECT_HASH_SIZE * 2) return -1;
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        for (int k = 0; k < 2; k++) {
            char c = s[i * 2 + k];
            unsigned d;
            if (c >= '0' && c <= '9')      d = (unsigned)(c - '0');
            else if (c >= 'a' && c <= 'f') d = (unsigned)(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') d = (unsigned)(c - 'A' + 10);
            else return -1;
            v = v * 16 + d;
        }
        out[i] = (uint8_t)v;
    }
    return 0;
}

status_t node_ids_reserve(cutils_db_t *db, uint64_t n, int64_t *first)
{
    if (n == 0) { *first = 0; return OK; }
    db_param_t p[] = { DB_INT64((int64_t)n) };
    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "UPDATE node_id_seq SET last_id = last_id + ? WHERE one = 1"
            " RETURNING last_id",
            p, DB_NPARAMS(p), &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_IO, "node index: cannot reserve ids");
    }
    int64_t last = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    *first = last - (int64_t)n + 1;
    return OK;
}

int node_index_has_tree(cutils_db_t *db, int64_t repo_id,
                        const uint8_t hash[OBJECT_HASH_SIZE])
{
    db_param_t p[] = { DB_INT64(repo_id), DB_HASH(hash) };
    db_result_t *r = NULL;
    if (db_execute_typed(db, "SELECT 1 FROM trees WHERE repo_id = ? AND hash = ?",
                         p, DB_NPARAMS(p), &r) != 0 || !r) {
        db_result_free(r);
        set_error(ERR_IO, "node index: cannot look up a tree");
        return -1;
    }
    int have = r->nrows > 0;
    db_result_free(r);
    return have;
}

/* A tree's rows, whatever state they are in, and its completion mark. */
static status_t delete_tree_rows(cutils_db_t *db, int64_t repo_id,
                                 const uint8_t hash[OBJECT_HASH_SIZE])
{
    db_param_t p[] = { DB_INT64(repo_id), DB_HASH(hash) };
    if (db_execute_non_query_typed(db,
            "DELETE FROM node_fts WHERE rowid IN"
            " (SELECT id FROM nodes WHERE repo_id = ? AND tree_hash = ?)",
            p, DB_NPARAMS(p), NULL) != 0 ||
        db_execute_non_query_typed(db,
            "DELETE FROM nodes WHERE repo_id = ? AND tree_hash = ?",
            p, DB_NPARAMS(p), NULL) != 0 ||
        db_execute_non_query_typed(db,
            "DELETE FROM trees WHERE repo_id = ? AND hash = ?",
            p, DB_NPARAMS(p), NULL) != 0)
        return set_error(ERR_IO, "node index: cannot remove a tree's rows");
    return OK;
}

status_t node_index_put_tree(cutils_db_t *db, int64_t repo_id,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             const manifest_entry_t *entries, uint32_t count,
                             int replace, uint64_t *rows_out)
{
    if (rows_out) *rows_out = 0;
    if (!replace) {
        int have = node_index_has_tree(db, repo_id, hash);
        if (have < 0) return ERR_IO;
        if (have) return OK;
    }
    /*
     * Partial rows from a run that stopped before the mark, or, replacing,
     * the complete ones. Without this a retried tree would hold each entry
     * twice: the name lookup in a directory would match two rows.
     */
    status_t st = delete_tree_rows(db, repo_id, hash);
    if (st != OK) return st;

    int64_t id = 0;
    st = node_ids_reserve(db, count, &id);
    if (st != OK) return st;

    uint64_t files = 0, bytes = 0;
    for (uint32_t i = 0; i < count; i++, id++) {
        const manifest_entry_t *e = &entries[i];
        /* Manifest names are counted strings; the database wants C strings. */
        char *name = strndup(e->name, e->name_len);
        char *lt = (e->link_target && e->link_target_len)
                   ? strndup(e->link_target, e->link_target_len) : NULL;
        if (!name || (e->link_target && e->link_target_len && !lt)) {
            free(name); free(lt);
            return set_error(ERR_NOMEM, "node index: entry name");
        }
        int is_dir = (e->type == NODE_TYPE_DIR);
        int have_target = !is_zero(e->target_hash);
        if (e->type == NODE_TYPE_REG) { files++; bytes += e->size; }

        /*
         * Absent hashes are NULL, not a zero hash: queries ask
         * "content_hash IS NOT NULL" to mean "this node has content", and a
         * symlink or device node claiming a zero hash would match no object
         * while saying it has one. The hard-link identity is bound signed,
         * because half of all 64-bit values do not fit SQLite's INTEGER as
         * unsigned and would degrade to REAL, collapsing distinct inodes.
         */
        db_param_t p[] = {
            DB_INT64(id), DB_INT64(repo_id), DB_HASH(hash), DB_TEXT(name),
            DB_INT64(e->type), DB_INT64(e->mode), DB_INT64(e->uid), DB_INT64(e->gid),
            DB_INT64(time_to_ns(e->mtime_sec, (int64_t)e->mtime_nsec)),
            DB_INT64((int64_t)e->size),
            DB_HASH_OR_NULL(have_target && !is_dir, e->target_hash),
            DB_INT64((int64_t)e->inode_identity),
            DB_HASH_OR_NULL(have_target && is_dir, e->target_hash),
            DB_TEXT(lt),
            DB_HASH_OR_NULL(!is_zero(e->xattr_hash), e->xattr_hash),
            DB_HASH_OR_NULL(!is_zero(e->acl_hash), e->acl_hash),
            DB_INT64(e->dev_major), DB_INT64(e->dev_minor),
        };
        int rc = db_execute_non_query_typed(db,
                "INSERT INTO nodes (id, repo_id, tree_hash, name, type, mode, uid, gid,"
                "  mtime_ns, size, content_hash, hardlink_group_id, subtree_hash,"
                "  link_target, xattr_hash, acl_hash, dev_major, dev_minor)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                p, DB_NPARAMS(p), NULL);
        if (rc == 0) {
            db_param_t f[] = { DB_INT64(id), DB_TEXT(name), DB_INT64(repo_id) };
            rc = db_execute_non_query_typed(db,
                    "INSERT INTO node_fts (rowid, name, repo_id) VALUES (?, ?, ?)",
                    f, DB_NPARAMS(f), NULL);
        }
        free(name);
        free(lt);
        if (rc != 0) return set_error(ERR_IO, "node index: cannot insert an entry");
    }

    db_param_t m[] = { DB_INT64(repo_id), DB_HASH(hash), DB_INT64((int64_t)files),
                       DB_INT64((int64_t)bytes) };
    if (db_execute_non_query_typed(db,
            "INSERT INTO trees (repo_id, hash, files, bytes) VALUES (?, ?, ?, ?)",
            m, DB_NPARAMS(m), NULL) != 0)
        return set_error(ERR_IO, "node index: cannot mark a tree complete");
    if (rows_out) *rows_out = count;
    return OK;
}

status_t node_index_put_dir(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                            int64_t id, int64_t parent_id, const char *name,
                            const char *path, const uint8_t tree[OBJECT_HASH_SIZE])
{
    db_param_t p[] = { DB_INT64(id), DB_INT64(repo_id), DB_INT64(snap_id),
                       DB_INT64(parent_id), DB_TEXT(name), DB_TEXT(path),
                       DB_HASH(tree) };
    if (db_execute_non_query_typed(db,
            "INSERT INTO snap_dirs (id, repo_id, snap_id, parent_id, name, path, tree_hash)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            p, DB_NPARAMS(p), NULL) != 0)
        return set_error(ERR_IO, "node index: cannot record directory %s", path);
    return OK;
}

static int str_cmp(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* Remove a tree's rows if no skeleton reaches it any more. */
static status_t release_tree(cutils_db_t *db, int64_t repo_id,
                             const uint8_t hash[OBJECT_HASH_SIZE], int *released)
{
    *released = 0;
    db_param_t p[] = { DB_INT64(repo_id), DB_HASH(hash) };
    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "SELECT 1 FROM snap_dirs WHERE repo_id = ? AND tree_hash = ? LIMIT 1",
            p, DB_NPARAMS(p), &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "node index: cannot check a tree's references");
    }
    int used = r->nrows > 0;
    db_result_free(r);
    if (used) return OK;
    status_t st = delete_tree_rows(db, repo_id, hash);
    if (st == OK) *released = 1;
    return st;
}

status_t node_index_drop_snapshot(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                                  int release)
{
    db_param_t p[] = { DB_INT64(repo_id), DB_INT64(snap_id) };
    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "DELETE FROM snap_dirs WHERE repo_id = ? AND snap_id = ?"
            " RETURNING lower(hex(tree_hash))",
            p, DB_NPARAMS(p), &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "node index: cannot drop snapshot %lld's directories",
                         (long long)snap_id);
    }
    if (!release || !r || r->nrows == 0) { db_result_free(r); return OK; }

    /*
     * Every tree this snapshot reached is in the list: each directory
     * instance, the root included, has a skeleton row. Sorted so a tree
     * seen at several paths is checked once.
     */
    const char **h = malloc((size_t)r->nrows * sizeof(*h));
    if (!h) { db_result_free(r); return set_error(ERR_NOMEM, "node index: drop"); }
    for (int i = 0; i < r->nrows; i++) h[i] = r->rows[i][0];
    qsort(h, (size_t)r->nrows, sizeof(*h), str_cmp);

    status_t st = OK;
    for (int i = 0; i < r->nrows && st == OK; i++) {
        if (i > 0 && strcmp(h[i], h[i - 1]) == 0) continue;
        uint8_t hash[OBJECT_HASH_SIZE];
        if (node_hex_to_hash(h[i], hash) != 0) continue;
        int released = 0;
        st = release_tree(db, repo_id, hash, &released);
    }
    free(h);
    db_result_free(r);
    return st;
}

status_t node_index_sweep(cutils_db_t *db, int64_t repo_id, uint64_t *trees_removed)
{
    if (trees_removed) *trees_removed = 0;
    /*
     * Candidates: complete trees nothing reaches, and hashes with entry rows
     * but no completion mark (a tree whose run stopped mid-write). Read
     * first, deleted after, so nothing is iterated while it changes.
     */
    db_param_t p[] = { DB_INT64(repo_id) };
    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "SELECT lower(hex(t.hash)) FROM trees t WHERE t.repo_id = ?1"
            "   AND NOT EXISTS (SELECT 1 FROM snap_dirs d"
            "                    WHERE d.repo_id = ?1 AND d.tree_hash = t.hash)"
            " UNION"
            " SELECT lower(hex(n.tree_hash)) FROM (SELECT DISTINCT tree_hash FROM nodes"
            "                                       WHERE repo_id = ?1) n"
            "  WHERE NOT EXISTS (SELECT 1 FROM trees t"
            "                     WHERE t.repo_id = ?1 AND t.hash = n.tree_hash)"
            "    AND NOT EXISTS (SELECT 1 FROM snap_dirs d"
            "                     WHERE d.repo_id = ?1 AND d.tree_hash = n.tree_hash)",
            p, DB_NPARAMS(p), &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "node index: cannot list unreferenced trees");
    }

    enum { SWEEP_BATCH = 200 };
    status_t st = OK;
    uint64_t removed = 0;
    for (int i = 0; r && i < r->nrows && st == OK; ) {
        if (db_txn_begin(db) != OK) { st = set_error(ERR_IO, "node index: sweep begin"); break; }
        for (int k = 0; k < SWEEP_BATCH && i < r->nrows && st == OK; k++, i++) {
            uint8_t hash[OBJECT_HASH_SIZE];
            if (node_hex_to_hash(r->rows[i][0], hash) != 0) continue;
            int released = 0;
            /* Checked again inside the transaction: the list was read
             * outside one. */
            st = release_tree(db, repo_id, hash, &released);
            if (released) removed++;
        }
        if (st != OK) { db_txn_rollback(db); break; }
        if (db_txn_commit(db) != OK) st = set_error(ERR_IO, "node index: sweep commit");
    }
    db_result_free(r);
    if (removed) log_info("node index: removed %llu tree(s) no snapshot reaches",
                          (unsigned long long)removed);
    if (trees_removed) *trees_removed = removed;
    return st;
}

int node_index_root(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                    int64_t *root_id, uint8_t tree[OBJECT_HASH_SIZE])
{
    db_param_t p[] = { DB_INT64(repo_id), DB_INT64(snap_id) };
    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "SELECT id, lower(hex(tree_hash)) FROM snap_dirs"
            " WHERE repo_id = ? AND snap_id = ? AND parent_id = 0",
            p, DB_NPARAMS(p), &r) != 0 || !r) {
        db_result_free(r);
        return -1;
    }
    int found = r->nrows == 1 &&
                node_hex_to_hash(r->rows[0][1], tree) == 0;
    if (found && root_id) *root_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return found;
}

int node_index_resolve(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                       const char *path, node_ref_t *out)
{
    memset(out, 0, sizeof(*out));
    int64_t dir = 0;
    uint8_t tree[OBJECT_HASH_SIZE];
    int rr = node_index_root(db, repo_id, snap_id, &dir, tree);
    if (rr != 1) return -1;
    out->dir_id = dir;
    out->is_dir = 1;
    memcpy(out->tree, tree, OBJECT_HASH_SIZE);

    const char *p = path ? path : "";
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != '/') p++;
        size_t n = (size_t)(p - s);
        /* A file has no children: a component past one names nothing. */
        if (!out->is_dir) return 0;
        char *name = strndup(s, n);
        if (!name) return -1;

        /* A directory is a skeleton row of its own; anything else is an
         * entry of the directory it sits in. */
        db_param_t dp[] = { DB_INT64(out->dir_id), DB_TEXT(name) };
        db_result_t *r = NULL;
        if (db_execute_typed(db,
                "SELECT id, lower(hex(tree_hash)) FROM snap_dirs"
                " WHERE parent_id = ? AND name = ?",
                dp, DB_NPARAMS(dp), &r) != 0 || !r) {
            db_result_free(r); free(name);
            return -1;
        }
        int is_dir = r->nrows == 1;
        int64_t child_dir = is_dir ? strtoll(r->rows[0][0], NULL, 10) : 0;
        uint8_t child_tree[OBJECT_HASH_SIZE];
        if (is_dir && node_hex_to_hash(r->rows[0][1], child_tree) != 0) is_dir = 0;
        db_result_free(r);
        if (is_dir) {
            /* One lookup per directory: its skeleton row is the answer. */
            free(name);
            out->dir_id = child_dir;
            out->node_id = 0;
            memcpy(out->tree, child_tree, OBJECT_HASH_SIZE);
            continue;
        }

        db_param_t np[] = { DB_INT64(repo_id), DB_HASH(out->tree), DB_TEXT(name) };
        r = NULL;
        int rc = db_execute_typed(db,
                "SELECT id FROM nodes WHERE repo_id = ? AND tree_hash = ? AND name = ?",
                np, DB_NPARAMS(np), &r);
        free(name);
        if (rc != 0 || !r) { db_result_free(r); return -1; }
        if (r->nrows < 1) { db_result_free(r); return 0; }
        out->node_id = strtoll(r->rows[0][0], NULL, 10);
        db_result_free(r);
        out->is_dir = 0;
    }
    return 1;
}
