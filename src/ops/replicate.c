#define _POSIX_C_SOURCE 200809L

#include "replicate.h"

#include "manifest.h"
#include "object.h"
#include "types.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Trees nest as deeply as the source filesystem allowed, which is not a
 * number this process gets to choose. The same bound GC uses, for the same
 * reason: a cycle cannot occur in a hash-addressed tree, but a corrupt tree
 * naming itself would otherwise recurse until the stack ran out.
 */
#define REPLICATE_MAX_DEPTH 512

typedef struct {
    store_t            *store;
    replica_sink_t     *sink;
    volatile int       *cancel;
    replicate_result_t *res;

    /*
     * Hashes waiting to be asked about.
     *
     * Batched because a question per file is what makes a slow link slow,
     * and flushed at the end of each directory rather than whenever it
     * happens to fill: everything a tree names has to be stored before the
     * tree itself is, so the batch cannot be allowed to outlive the
     * directory that filled it. See the ordering note in replicate.h.
     */
    uint8_t  pending[REPLICA_BATCH_MAX][OBJECT_HASH_SIZE];
    size_t   pending_n;
} walk_ctx_t;

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
 * Snapshot roots come back from the database as hex text.
 *
 * Failure is reported rather than absorbed. gc.c's decoder substitutes a
 * zero byte for anything unparseable, which is right there - a zero hash
 * marks nothing and the sweep is conservative - but here it would walk an
 * empty tree and commit a snapshot claiming to contain nothing.
 */
static int hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    if (!hex) return -1;
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (!hex[i * 2] || !hex[i * 2 + 1]) return -1;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

/* Read one object as stored and hand it over. */
static status_t send_one(walk_ctx_t *c, const uint8_t hash[OBJECT_HASH_SIZE])
{
    object_stored_info_t info;
    void  *payload = NULL;
    size_t len = 0;

    status_t st = object_read_stored(c->store, hash, &info, &payload, &len);
    if (st != OK) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(hash, hex);
        return set_error(st, "replicate: cannot read object %s: %s", hex, err_msg());
    }

    replica_object_t obj = {
        .type              = info.type,
        .compression       = info.compression,
        .encryption        = info.encryption,
        .uncompressed_size = info.uncompressed_size,
        .payload           = payload,
        .payload_len       = len,
    };
    memcpy(obj.hash, hash, OBJECT_HASH_SIZE);

    st = c->sink->v->put_object(c->sink->ctx, &obj);
    free(payload);
    if (st != OK) return st;

    c->res->objects_sent++;
    c->res->bytes_sent += len;
    return OK;
}

/*
 * Ask about everything queued and send what is missing.
 *
 * Returning early on a sink error rather than continuing is deliberate: a
 * batch that failed halfway leaves objects unsent, and the tree naming them
 * must not be sent regardless of which of them failed.
 */
static status_t flush_pending(walk_ctx_t *c)
{
    if (c->pending_n == 0) return OK;

    uint8_t present[REPLICA_BATCH_MAX];
    status_t st = c->sink->v->has_objects(c->sink->ctx, c->pending[0],
                                          c->pending_n, present);
    if (st != OK) return st;

    c->res->objects_asked += c->pending_n;

    for (size_t i = 0; i < c->pending_n; i++) {
        if (present[i]) continue;
        st = send_one(c, c->pending[i]);
        if (st != OK) { c->pending_n = 0; return st; }
    }
    c->pending_n = 0;
    return OK;
}

static status_t queue_hash(walk_ctx_t *c, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (hash_is_zero(hash)) return OK;
    memcpy(c->pending[c->pending_n++], hash, OBJECT_HASH_SIZE);
    if (c->pending_n == REPLICA_BATCH_MAX) return flush_pending(c);
    return OK;
}

/*
 * Send a subtree, then the tree naming it.
 *
 * The order is the correctness argument for the whole file. A tree is sent
 * only once everything it references has been stored, so a tree that is
 * present at the target promises that its subtree is complete - which is
 * exactly what the next run assumes when it sees that hash and skips the
 * whole branch. Reverse the two and an interrupted transfer leaves a tree
 * whose children never arrived, and every later run skips them on the
 * strength of it: a target that passes every structural check and cannot be
 * restored.
 */
static status_t walk_tree(walk_ctx_t *c, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                          int depth)
{
    if (c->cancel && *c->cancel)
        return set_error(ERR_BUSY, "replicate: cancelled");
    if (depth > REPLICATE_MAX_DEPTH)
        return set_error(ERR_CORRUPT, "replicate: tree nesting too deep");
    if (hash_is_zero(tree_hash)) return OK;

    /* One question dismisses the entire branch. */
    uint8_t present = 0;
    status_t st = c->sink->v->has_objects(c->sink->ctx, tree_hash, 1, &present);
    if (st != OK) return st;
    c->res->objects_asked++;
    if (present) {
        c->res->subtrees_skipped++;
        return OK;
    }

    manifest_entry_t *entries = NULL;
    uint32_t count = 0;
    st = manifest_read_tree(c->store, tree_hash, &entries, &count);
    if (st != OK)
        return set_error(ERR_CORRUPT,
                         "replicate: cannot read tree, refusing to send a partial snapshot");

    for (uint32_t i = 0; i < count && st == OK; i++) {
        manifest_entry_t *e = &entries[i];

        if (e->type == NODE_TYPE_DIR) {
            /* A directory's own metadata blobs belong to the parent's batch;
             * its contents are the subtree's problem. */
            st = queue_hash(c, e->xattr_hash);
            if (st == OK) st = queue_hash(c, e->acl_hash);
            /* Flush before descending: the child tree is about to be sent,
             * and anything queued here is named by *this* tree, which is
             * sent after the child. Keeping them separate keeps the
             * bottom-up promise honest at every level. */
            if (st == OK) st = flush_pending(c);
            if (st == OK) st = walk_tree(c, e->target_hash, depth + 1);
        } else {
            st = queue_hash(c, e->target_hash);
            if (st == OK) st = queue_hash(c, e->xattr_hash);
            if (st == OK) st = queue_hash(c, e->acl_hash);
        }
    }

    manifest_free_entries(entries, count);
    if (st != OK) return st;

    /* Everything this tree names is now stored. Only now may the tree be. */
    st = flush_pending(c);
    if (st != OK) return st;

    return send_one(c, tree_hash);
}

status_t replicate_tree(store_t *store, replica_sink_t *sink,
                        const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                        volatile int *cancel, replicate_result_t *out)
{
    if (!store || !sink || !sink->v || !root_tree_hash)
        return set_error(ERR_INVALID, "replicate_tree: null argument");

    replicate_result_t res;
    memset(&res, 0, sizeof(res));

    walk_ctx_t c = { .store = store, .sink = sink, .cancel = cancel, .res = &res };
    status_t st = walk_tree(&c, root_tree_hash, 0);

    /* Report what was transferred even on failure: a caller deciding whether
     * to retry wants to know how far it got. */
    if (out) *out = res;
    return st;
}

status_t replicate_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                       replica_sink_t *sink, volatile int *cancel,
                       task_progress_fn progress, void *progress_ctx,
                       replicate_result_t *out)
{
    if (!store || !db || !sink || !sink->v)
        return set_error(ERR_INVALID, "replicate_run: null argument");

    replicate_result_t res;
    memset(&res, 0, sizeof(res));
    /* Set now, not only on the way out: the query below can fail, and a
     * caller reading counters after a failure should see zero rather than
     * whatever its stack happened to hold. */
    if (out) *out = res;

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *snaps = NULL;
    if (db_execute(db,
            "SELECT id, ts, label, root_tree_hash, file_count, total_size"
            " FROM snapshots WHERE repo_id = ? AND completed_at IS NOT NULL"
            " ORDER BY id",
            (const char *[]){ repo, NULL }, &snaps) != 0)
        return set_error(ERR_IO, "replicate: cannot list snapshots");

    status_t st = OK;
    size_t nrows = snaps ? (size_t)snaps->nrows : 0;

    for (size_t i = 0; i < nrows && st == OK; i++) {
        if (cancel && *cancel) { st = set_error(ERR_BUSY, "replicate: cancelled"); break; }

        int64_t snap_id = strtoll(snaps->rows[i][0], NULL, 10);
        int64_t ts      = strtoll(snaps->rows[i][1], NULL, 10);
        /* c-utils hands back an empty string for a NULL column, never a NULL
         * pointer, so an unlabelled snapshot has to be recognised by testing
         * the string. */
        const char *label = (snaps->rows[i][2] && snaps->rows[i][2][0])
                            ? snaps->rows[i][2] : NULL;

        uint8_t root[OBJECT_HASH_SIZE];
        if (hex_to_hash(snaps->rows[i][3], root) != 0) {
            st = set_error(ERR_CORRUPT, "replicate: snapshot %lld has an unreadable root hash",
                           (long long)snap_id);
            break;
        }

        int have = 0;
        st = sink->v->has_snapshot(sink->ctx, snap_id, root, &have);
        if (st != OK) break;
        if (have) { res.snapshots_skipped++; continue; }

        replicate_result_t one;
        st = replicate_tree(store, sink, root, cancel, &one);
        res.objects_asked    += one.objects_asked;
        res.objects_sent     += one.objects_sent;
        res.bytes_sent       += one.bytes_sent;
        res.subtrees_skipped += one.subtrees_skipped;
        if (st != OK) break;

        replica_snapshot_t rs = {
            .snapshot_id = snap_id,
            .ts          = ts,
            .label       = label,
            .file_count  = (uint64_t)strtoull(snaps->rows[i][4], NULL, 10),
            .total_size  = (uint64_t)strtoull(snaps->rows[i][5], NULL, 10),
        };
        memcpy(rs.root_tree_hash, root, OBJECT_HASH_SIZE);

        st = sink->v->commit_snapshot(sink->ctx, &rs);
        if (st != OK) break;

        res.snapshots_sent++;
        if (progress) {
            char msg[128];
            snprintf(msg, sizeof(msg), "replicated snapshot %lld", (long long)snap_id);
            progress((uint64_t)(i + 1), (uint64_t)nrows, msg, progress_ctx);
        }
    }

    db_result_free(snaps);
    if (out) *out = res;
    return st;
}
