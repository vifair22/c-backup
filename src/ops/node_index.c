#define _GNU_SOURCE
#include "node_index.h"

#include "db_txn.h"
#include "db_write.h"
#include "node_gen.h"
#include "store.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* A batch of index writes; db_txn.h has the rule for when it ends. */
typedef struct {
    uint32_t       since_commit;
    db_txn_batch_t rule;
} node_batch_t;

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

static void hash_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    static const char *d = "0123456789abcdef";
    for (size_t i = 0; i < OBJECT_HASH_SIZE; i++) {
        out[i * 2]     = d[(h[i] >> 4) & 0xF];
        out[i * 2 + 1] = d[h[i] & 0xF];
    }
    out[(size_t)OBJECT_HASH_SIZE * 2] = '\0';
}

/* The last path component; what the entry is called inside its parent. */
static const char *basename_of(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

/* One file row's entry in node_hashes, sorted before it is written. */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t id;
} node_hash_ent_t;

static int node_hash_cmp(const void *a, const void *b)
{
    const node_hash_ent_t *x = a, *y = b;
    int c = memcmp(x->hash, y->hash, OBJECT_HASH_SIZE);
    if (c) return c;
    return x->id < y->id ? -1 : x->id > y->id;
}

status_t node_index_write(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                          const scan_result_t *scan, const node_link_t *links,
                          uint32_t count, atomic_int *cancel)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
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
        if (db_execute(db, "SELECT COALESCE(MAX(id), 0) FROM nodes WHERE repo_id = ?",
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
     *
     * And it yields to a queued writer by the rule every batch uses
     * (db_txn_batch_should_end), at every row (B144). It only ended at
     * NODE_BATCH rows, so a tag or sources write that queued during
     * indexing waited for up to 2,000 node and FTS inserts: about 80 ms
     * on an idle host, 700 ms with the CPU contended,
     * and past the 2 s test bound under disk pressure, against Manual 6.3's
     * "well under a second". Each extra commit is one more batch boundary of
     * the kind above, so nothing about nodes_materialized changes.
     */
    #define NODE_BATCH 2000
    /* The rows' generation (node_gen.h), drawn before the first batch so a
     * failed run burns it, and set ahead of the first row. */
    int64_t gen = 0;
    status_t gst = node_gen_alloc(db, &gen);
    if (gst != OK) return gst;

    node_batch_t nb;
    memset(&nb, 0, sizeof(nb));
    status_t bst = db_txn_begin_bulk(db);
    if (bst != OK) return bst;
    db_txn_batch_opened(&nb.rule);
    if (node_gen_set(db, repo_id, snap_id, gen) != 0) {
        db_txn_rollback(db);
        return set_error_wrap(ERR_IO, "index_nodes: cannot record the index generation");
    }

    for (uint32_t i = 0; i < count; i++) {
        /*
         * The daemon stopping does not wait for the index (B218): on a large
         * tree it takes minutes, and Docker kills a stop after 10 s. The
         * record is written, so this is the state a power cut here leaves,
         * which the caller reports (B195) and the next start's reconcile
         * indexes. A user's cancel still lets the index finish: the
         * snapshot is done, and cancelling now would only make it slower to
         * browse.
         */
        if (shutdown_requested(cancel)) {
            db_txn_rollback(db);
            return set_error(ERR_BUSY, "backup cancelled: the daemon is stopping");
        }
        /* In no tree, so in no index row either: the index is derived from
         * the trees, and a reindex would not bring it back. */
        if (links[i].skipped) continue;
        const scan_entry_t *e = &scan->entries[i];
        const char *name = basename_of(e->path);

        char id[32], parent[32], mode[16], uid[16], gid[16], mtime[32], size[32];
        /* Both offset, and together: parent_node_id refers to one of these
         * ids. A root's parent stays 0, which is not an id. */
        snprintf(id,     sizeof(id),     "%llu",
                 (unsigned long long)e->node.node_id + (unsigned long long)id_base);
        snprintf(parent, sizeof(parent), "%llu",
                 (unsigned long long)(links[i].parent_id
                                      ? links[i].parent_id + id_base : 0));
        snprintf(mode,   sizeof(mode),   "%u",   e->node.mode);
        snprintf(uid,    sizeof(uid),    "%u",   e->node.uid);
        snprintf(gid,    sizeof(gid),    "%u",   e->node.gid);
        snprintf(mtime,  sizeof(mtime),  "%lld",
                 (long long)time_to_ns(e->node.mtime_sec,
                                       (int64_t)e->node.mtime_nsec));
        snprintf(size,   sizeof(size),   "%llu", (unsigned long long)e->node.size);

        char chex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.content_hash, chex);
        char shex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(links[i].tree_hash, shex);
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
        char dmaj[16], dmin[16], nflags[8];
        snprintf(nflags, sizeof(nflags), "%u", (unsigned)e->node.flags);
        snprintf(dmaj, sizeof(dmaj), "%u", is_dev ? e->node.device.major : 0u);
        snprintf(dmin, sizeof(dmin), "%u", is_dev ? e->node.device.minor : 0u);

        int rc = db_write(db,
                "INSERT INTO nodes"
                " (repo_id, id, snap_id, parent_node_id, name, type, mode, uid,"
                "  gid, mtime_ns, size, content_hash, subtree_hash, link_target,"
                "  hardlink_group_id, xattr_hash, acl_hash, dev_major, dev_minor,"
                "  flags)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                "         NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?,"
                "         NULLIF(?,''), NULLIF(?,''), ?, ?, ?)",
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
                                  dmaj, dmin, nflags, NULL }, NULL);
        if (rc != 0) {
            db_txn_rollback(db);
            return set_error_wrap(ERR_IO, "index_nodes: insert failed");
        }

        if (db_write(db,
                "INSERT INTO node_fts (name, node_id, snap_id, repo_id)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ name, id, snap, repo, NULL }, NULL) != 0) {
            db_txn_rollback(db);
            return set_error_wrap(ERR_IO, "index_nodes: fts insert failed");
        }

        if (++nb.since_commit >= NODE_BATCH || db_txn_batch_should_end(&nb.rule)) {
            status_t cst = db_txn_commit(db);
            if (cst != OK) return cst;
            nb.since_commit = 0;
            cst = db_txn_begin_bulk(db);
            if (cst != OK) return cst;
            db_txn_batch_opened(&nb.rule);
        }
    }

    /*
     * The snapshot's content hashes, in hash order (node_hashes, migration
     * 016). Sorted first so the rows arrive in key order: they all sort
     * after every earlier snapshot's, so each commit appends to the end of
     * one b-tree instead of touching a page per row across a whole-repo
     * hash index (perf-d2-nochange).
     *
     * Each distinct hash also goes into node_content, the repository's set
     * of hashes any node names, which a lookup by hash asks first. That set
     * is ordered by hash across snapshots, but a hash already in it changes
     * nothing, so an unchanged snapshot writes no page of it.
     */
    node_hash_ent_t *hv = NULL;
    size_t hn = 0;
    if (count) {
        hv = malloc((size_t)count * sizeof(*hv));
        if (!hv) { db_txn_rollback(db); return set_error(ERR_NOMEM, "index_nodes: out of memory"); }
    }
    for (uint32_t i = 0; i < count; i++) {
        if (links[i].skipped) continue;
        const scan_entry_t *e = &scan->entries[i];
        if (e->node.type == NODE_TYPE_DIR || hash_is_zero(e->node.content_hash)) continue;
        memcpy(hv[hn].hash, e->node.content_hash, OBJECT_HASH_SIZE);
        hv[hn].id = e->node.node_id + id_base;
        hn++;
    }
    if (hn > 1) qsort(hv, hn, sizeof(*hv), node_hash_cmp);
    for (size_t k = 0; k < hn; k++) {
        char hid[32], hhex[OBJECT_HASH_SIZE * 2 + 1];
        snprintf(hid, sizeof(hid), "%llu", (unsigned long long)hv[k].id);
        hash_hex(hv[k].hash, hhex);
        if (db_write(db,
                "INSERT INTO node_hashes (repo_id, snap_id, content_hash, node_id)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ repo, snap, hhex, hid, NULL }, NULL) != 0) {
            free(hv);
            db_txn_rollback(db);
            return set_error_wrap(ERR_IO, "index_nodes: hash insert failed");
        }
        if ((k == 0 || memcmp(hv[k].hash, hv[k - 1].hash, OBJECT_HASH_SIZE) != 0) &&
            db_write(db,
                "INSERT OR IGNORE INTO node_content (repo_id, content_hash) VALUES (?, ?)",
                (const char *[]){ repo, hhex, NULL }, NULL) != 0) {
            free(hv);
            db_txn_rollback(db);
            return set_error_wrap(ERR_IO, "index_nodes: content set insert failed");
        }
        if (++nb.since_commit >= NODE_BATCH || db_txn_batch_should_end(&nb.rule)) {
            status_t cst = db_txn_commit(db);
            if (cst != OK) { free(hv); return cst; }
            nb.since_commit = 0;
            cst = db_txn_begin_bulk(db);
            if (cst != OK) { free(hv); return cst; }
            db_txn_batch_opened(&nb.rule);
        }
    }
    free(hv);

    #undef NODE_BATCH
    return db_txn_commit(db);
}
