#define _POSIX_C_SOURCE 200809L

#include "replicate.h"

#include "cdc.h"
#include "chunklist.h"
#include "manifest.h"
#include "pack.h"
#include "read_budget.h"
#include "object.h"
#include "object_crypt.h"
#include "snapshot_ids.h"
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
    atomic_int       *cancel;
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

/*
 * One object on its way across: the source's stored bytes, whole. Every
 * object is bounded (OBJECT_MAX_STORED_BYTES), so it is read in one piece
 * and handed over in one piece.
 *
 * There is no re-encrypting transform. A target under another key has
 * another gear table, so it is always re-chunked (see needs_rechunk), and
 * its objects are written from plaintext under its own key. Copying stored
 * bytes happens only where both repositories cut identically, which means
 * the same key or none, and the object key is derived from the repository
 * key alone, so the stored form is already right for the target.
 */
/* Hand one object over, as stored. */
static status_t send_one(walk_ctx_t *c, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (cancel_requested(c->cancel))
        return set_error(ERR_BUSY, "replicate: cancelled");

    object_stored_info_t info;
    memset(&info, 0, sizeof(info));
    void  *payload = NULL;
    size_t len = 0;
    int    damaged = 0;

    /*
     * Held to the daemon's read budget for as long as the stored bytes are
     * in memory. The stored size is at most the payload plus the AEAD
     * overhead, and the index says the payload's before anything is read.
     */
    uint64_t taken = 0, psz = 0;
    status_t st = object_get_info(c->store, hash, &psz, NULL);
    if (st == OK) {
        taken = read_budget_acquire(psz + OBJECT_CRYPT_OVERHEAD);
        st = object_read_stored(c->store, hash, &info, &payload, &len, &damaged);
    }
    if (st != OK) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(hash, hex);
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        return set_error(st, "replicate: cannot read object %s: %s", hex, why);
    }
    uint8_t *buf = payload;
    if (st == OK) {
        replica_object_t obj = {
            .type              = info.type,
            .compression       = info.compression,
            .encryption        = info.encryption,
            .uncompressed_size = info.uncompressed_size,
            .payload_len       = len,
            .payload           = buf,
        };
        memcpy(obj.hash, hash, OBJECT_HASH_SIZE);
        st = c->sink->v->put_object(c->sink->ctx, &obj);
    }
    free(buf);

    /* Sent with damage parity could not carry: counted, not fatal here. */
    if (st == OK && damaged) {
        replicate_result_t *res = c->res;
        if (res->damaged_listed < REPLICATE_DAMAGED_LIST_MAX) {
            memcpy(res->damaged[res->damaged_listed].hash, hash, OBJECT_HASH_SIZE);
            res->damaged[res->damaged_listed].snapshot_id = 0;
            res->damaged_listed++;
        }
        res->objects_damaged++;
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(hash, hex);
        log_error("replicate: object %s was sent damaged beyond its parity; the "
                  "replica holds it as damaged", hex);
    }
    if (taken) read_budget_release(taken);
    if (st != OK) return st;

    c->res->objects_sent++;
    c->res->bytes_sent += len;
    return OK;
}
/*
 * Everything a chunk list names, leaves first, then the list's sub-lists
 * after their own contents: the same bottom-up promise trees keep. A list
 * present at the target means every chunk beneath it is too, so a later
 * run that finds the list skips the file without asking about its chunks,
 * and an interrupted transfer never leaves a list naming chunks that did
 * not arrive. Children are asked about in batches; only absent ones are
 * opened. The caller sends the list itself once this returns.
 *
 * Depth is bounded by the format (CHUNKLIST_MAX_LEVEL), and one loaded
 * list per level is all that is held.
 */
static status_t send_one(walk_ctx_t *c, const uint8_t hash[OBJECT_HASH_SIZE]);

static status_t send_list_body(walk_ctx_t *c, const uint8_t hash[OBJECT_HASH_SIZE],
                               unsigned depth)
{
    if (depth > CHUNKLIST_MAX_LEVEL)
        return set_error(ERR_CORRUPT, "replicate: chunk list nested too deep");
    /* Loaded under the budget and released at once: what is kept while
     * the children go across is a parsed index, and holding its slot while
     * they are read would be nested acquisition (read_budget.h). */
    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    uint64_t taken = 0;
    status_t st = object_load_budgeted(c->store, hash, &data, &len, &type, &taken);
    if (st != OK) return st;
    read_budget_release(taken);
    chunklist_hdr_t h;
    if (type != OBJECT_TYPE_CHUNKLIST)
        st = set_error(ERR_CORRUPT, "replicate: object type %u is not a chunk list", type);
    if (st == OK) st = chunklist_parse(data, len, &h);

    uint8_t (*batch)[OBJECT_HASH_SIZE] = NULL;
    if (st == OK && !(batch = malloc(REPLICA_BATCH_MAX * OBJECT_HASH_SIZE)))
        st = set_error(ERR_NOMEM, "replicate: batch");
    for (uint32_t i = 0; st == OK && i < h.count; ) {
        if (cancel_requested(c->cancel)) { st = set_error(ERR_BUSY, "replicate: cancelled"); break; }
        size_t n = 0;
        for (; i < h.count && n < REPLICA_BATCH_MAX; i++, n++) {
            chunklist_entry_t e;
            chunklist_entry_at(data, i, &e);
            memcpy(batch[n], e.hash, OBJECT_HASH_SIZE);
        }
        uint8_t present[REPLICA_BATCH_MAX];
        st = c->sink->v->has_objects(c->sink->ctx, batch[0], n, present);
        if (st != OK) break;
        c->res->objects_asked += n;
        for (size_t k = 0; k < n && st == OK; k++) {
            if (present[k]) continue;
            if (h.level > 0) st = send_list_body(c, batch[k], depth + 1);
            if (st == OK) st = send_one(c, batch[k]);
        }
    }
    free(batch);
    free(data);
    return st;
}

/* One object, and first whatever it names when it is a chunk list. */
static status_t send_obj(walk_ctx_t *c, const uint8_t hash[OBJECT_HASH_SIZE])
{
    uint64_t size = 0;
    uint8_t type = 0;
    status_t st = object_get_info(c->store, hash, &size, &type);
    if (st == OK && type == OBJECT_TYPE_CHUNKLIST) st = send_list_body(c, hash, 0);
    if (st == ERR_NOT_FOUND) err_clear();   /* send_one names the object */
    else if (st != OK) return st;
    return send_one(c, hash);
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
        st = send_obj(c, c->pending[i]);
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
    if (cancel_requested(c->cancel))
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

    /* Everything this tree names is now stored. Only now may the tree be:
     * and a directory split into fragments sends them, then its lists. */
    st = flush_pending(c);
    if (st != OK) return st;
    return send_obj(c, tree_hash);
}

/* ------------------------------------------------------------------ */
/* Re-chunking, for a target that cuts differently                     */
/* ------------------------------------------------------------------ */

/*
 * Source tree hash to rebuilt target hash, for the run. Snapshots share most
 * of their trees, so the second snapshot of a run rebuilds only what
 * changed. Bounded: past the cap nothing more is remembered, which costs
 * rebuilding, never correctness.
 */
#define RC_MEMO_MAX ((size_t)1 << 20)

typedef struct {
    uint8_t src[OBJECT_HASH_SIZE];
    uint8_t tgt[OBJECT_HASH_SIZE];
    int     used;
} rc_slot_t;

typedef struct {
    store_t            *store;
    replica_sink_t     *sink;
    const cdc_params_t *tcdc;
    atomic_int         *cancel;
    replicate_result_t *res;
    rc_slot_t          *memo;
    size_t              memo_cap, memo_n;
} rc_ctx_t;

static size_t rc_slot(const rc_ctx_t *r, const uint8_t h[OBJECT_HASH_SIZE])
{
    uint64_t v;
    memcpy(&v, h, sizeof(v));
    return (size_t)v & (r->memo_cap - 1);
}

static int rc_memo_get(const rc_ctx_t *r, const uint8_t src[OBJECT_HASH_SIZE],
                       uint8_t tgt[OBJECT_HASH_SIZE])
{
    if (!r->memo) return 0;
    for (size_t i = rc_slot(r, src); r->memo[i].used; i = (i + 1) & (r->memo_cap - 1))
        if (!memcmp(r->memo[i].src, src, OBJECT_HASH_SIZE)) {
            memcpy(tgt, r->memo[i].tgt, OBJECT_HASH_SIZE);
            return 1;
        }
    return 0;
}

static void rc_memo_put(rc_ctx_t *r, const uint8_t src[OBJECT_HASH_SIZE],
                        const uint8_t tgt[OBJECT_HASH_SIZE])
{
    if (r->memo_n >= RC_MEMO_MAX) return;
    if ((r->memo_n + 1) * 2 > r->memo_cap) {
        size_t ncap = r->memo_cap ? r->memo_cap * 2 : 1024;
        rc_slot_t *nm = calloc(ncap, sizeof(*nm));
        if (!nm) return;
        rc_slot_t *old = r->memo;
        size_t ocap = r->memo_cap;
        r->memo = nm;
        r->memo_cap = ncap;
        r->memo_n = 0;
        for (size_t i = 0; i < ocap; i++)
            if (old[i].used) rc_memo_put(r, old[i].src, old[i].tgt);
        free(old);
    }
    size_t i = rc_slot(r, src);
    while (r->memo[i].used) {
        if (!memcmp(r->memo[i].src, src, OBJECT_HASH_SIZE)) return;
        i = (i + 1) & (r->memo_cap - 1);
    }
    memcpy(r->memo[i].src, src, OBJECT_HASH_SIZE);
    memcpy(r->memo[i].tgt, tgt, OBJECT_HASH_SIZE);
    r->memo[i].used = 1;
    r->memo_n++;
}

/* The target's "put (type, bytes) -> name", as the chunker and the tree
 * writer take it. */
static status_t rc_put(void *ctx, uint8_t type, const void *data, size_t len,
                       obj_put_t *out)
{
    rc_ctx_t *r = ctx;
    int is_new = 0;
    status_t st = r->sink->v->put_plain(r->sink->ctx, type, data, len,
                                        out->hash, &is_new);
    if (st != OK) return st;
    out->is_new = is_new;
    out->phys_bytes = 0;
    if (is_new) r->res->objects_written++;
    return OK;
}

/* An object whose bytes do not depend on chunking (xattrs, ACLs): its
 * plaintext, handed over whole. Its name may still differ at the target. */
static status_t rc_copy_plain(rc_ctx_t *r, const uint8_t src[OBJECT_HASH_SIZE],
                              uint8_t tgt[OBJECT_HASH_SIZE])
{
    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    uint64_t taken = 0;
    status_t st = object_load_budgeted(r->store, src, &data, &len, &type, &taken);
    if (st != OK) return st;
    obj_put_t put;
    memset(&put, 0, sizeof(put));
    st = rc_put(r, type, data, len, &put);
    free(data);
    read_budget_release(taken);
    if (st == OK) memcpy(tgt, put.hash, OBJECT_HASH_SIZE);
    return st;
}

/* A file's content, reassembled from the source and cut with the target's
 * parameters. Holes stay where they were: the source list's gaps are the
 * spans the chunker is given. */
static status_t rc_content(rc_ctx_t *r, const uint8_t src[OBJECT_HASH_SIZE],
                           uint8_t tgt[OBJECT_HASH_SIZE])
{
    content_src_t *cs = NULL;
    uint64_t size = 0;
    status_t st = content_source_open(r->store, src, &cs, &size);
    if (st != OK) return st;
    cdc_source_t iface = content_source_iface(cs);
    obj_sink_t sink = { .put = rc_put, .ctx = r };
    /* The source's leaf holds the budget for what is in flight. */
    cdc_opts_t o = { .cancel = r->cancel, .unbudgeted = 1 };
    cdc_result_t res;
    st = cdc_store_source(r->tcdc, &iface, size, &sink, &o, &res);
    content_source_close(cs);
    if (st != OK) return st;
    memcpy(tgt, res.hash, OBJECT_HASH_SIZE);
    r->res->files_rechunked++;
    return OK;
}

static void rc_free_entries(manifest_entry_t *e, uint32_t n)
{
    manifest_free_entries(e, n);
}

/*
 * Rebuild one directory for the target, children first. Every object the
 * new tree names is stored at the target before the tree is, the same
 * bottom-up rule the copying walk keeps; the tree itself goes through
 * manifest_store_tree with the target's parameters, so a large directory
 * is fragmented where the target's own backups would fragment it.
 */
static status_t rc_tree(rc_ctx_t *r, const uint8_t src[OBJECT_HASH_SIZE],
                        int depth, uint8_t tgt[OBJECT_HASH_SIZE])
{
    if (cancel_requested(r->cancel)) return set_error(ERR_BUSY, "replicate: cancelled");
    if (depth > REPLICATE_MAX_DEPTH)
        return set_error(ERR_CORRUPT, "replicate: tree nesting too deep");
    if (rc_memo_get(r, src, tgt)) return OK;

    manifest_entry_t *ents = NULL;
    uint32_t n = 0;
    status_t st = manifest_read_tree(r->store, src, &ents, &n);
    if (st != OK)
        return set_error(ERR_CORRUPT, "replicate: cannot read tree, refusing to "
                         "send a partial snapshot");

    for (uint32_t i = 0; i < n && st == OK; i++) {
        manifest_entry_t *e = &ents[i];
        if (!hash_is_zero(e->target_hash)) {
            uint8_t h[OBJECT_HASH_SIZE];
            if (e->type == NODE_TYPE_DIR)      st = rc_tree(r, e->target_hash, depth + 1, h);
            else if (e->type == NODE_TYPE_REG) st = rc_content(r, e->target_hash, h);
            else                               st = rc_copy_plain(r, e->target_hash, h);
            if (st == OK) memcpy(e->target_hash, h, OBJECT_HASH_SIZE);
        }
        if (st == OK && !hash_is_zero(e->xattr_hash))
            st = rc_copy_plain(r, e->xattr_hash, e->xattr_hash);
        if (st == OK && !hash_is_zero(e->acl_hash))
            st = rc_copy_plain(r, e->acl_hash, e->acl_hash);
    }
    if (st == OK) {
        obj_sink_t sink = { .put = rc_put, .ctx = r };
        st = manifest_store_tree(ents, n, r->tcdc, &sink, tgt, NULL);
    }
    rc_free_entries(ents, n);
    if (st != OK) return st;
    r->res->trees_rebuilt++;
    rc_memo_put(r, src, tgt);
    return OK;
}

/* Does the sink cut differently from the source? */
static int needs_rechunk(store_t *store, const replica_sink_t *sink)
{
    if (!sink->v->target_cdc || !sink->v->put_plain) return 0;
    const cdc_params_t *t = sink->v->target_cdc(sink->ctx);
    if (!t) return 0;
    const cdc_params_t *sp = store_cdc(store);
    if (!sp) return 1;
    return t->min_size != sp->min_size || t->avg_size != sp->avg_size ||
           t->max_size != sp->max_size ||
           memcmp(t->gear, sp->gear, sizeof(t->gear)) != 0;
}

status_t replicate_tree(store_t *store, replica_sink_t *sink,
                        const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                        atomic_int *cancel, replicate_result_t *out)
{
    if (!store || !sink || !sink->v || !root_tree_hash)
        return set_error(ERR_INVALID, "replicate_tree: null argument");

    replicate_result_t res;
    memset(&res, 0, sizeof(res));
    status_t st = OK;
    /* Objects this handle staged through object_store_ex are sealed first,
     * so the walk can read every one it reaches. */
    st = object_store_flush(store);
    if (st == OK && needs_rechunk(store, sink)) {
        rc_ctx_t r = { .store = store, .sink = sink, .cancel = cancel, .res = &res,
                       .tcdc = sink->v->target_cdc(sink->ctx) };
        res.rechunked = 1;
        st = rc_tree(&r, root_tree_hash, 0, res.target_root);
        free(r.memo);
    } else if (st == OK) {
        walk_ctx_t c = { .store = store, .sink = sink, .cancel = cancel, .res = &res };
        st = walk_tree(&c, root_tree_hash, 0);
        memcpy(res.target_root, root_tree_hash, OBJECT_HASH_SIZE);
    }

    /* Report what was transferred even on failure: a caller deciding whether
     * to retry wants to know how far it got. */
    if (out) *out = res;
    return st;
}

/* replicate_run's per-snapshot transfer, sharing one memo across the run. */
static status_t transfer_one(store_t *store, replica_sink_t *sink, int rechunk,
                             rc_ctx_t *rc, const uint8_t root[OBJECT_HASH_SIZE],
                             atomic_int *cancel, replicate_result_t *one)
{
    if (!rechunk) return replicate_tree(store, sink, root, cancel, one);
    memset(one, 0, sizeof(*one));
    one->rechunked = 1;
    rc->res = one;
    rc->cancel = cancel;
    return rc_tree(rc, root, 0, one->target_root);
}

status_t replicate_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                       replica_sink_t *sink, atomic_int *cancel,
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

    int rechunk = needs_rechunk(store, sink);
    if (rechunk && !sink->v->has_snapshot_at) {
        db_result_free(snaps);
        return set_error(ERR_INVALID, "replicate: the target cuts files "
                         "differently and this sink cannot say which snapshots "
                         "it already holds");
    }
    rc_ctx_t rc = { .store = store, .sink = sink,
                    .tcdc = rechunk ? sink->v->target_cdc(sink->ctx) : NULL };
    if (rechunk)
        log_info("replicate: the target's chunking differs from the source's "
                 "(key or constants); file content is re-chunked and trees are "
                 "rebuilt, so snapshot roots at the target differ");

    for (size_t i = 0; i < nrows && st == OK; i++) {
        if (cancel_requested(cancel)) { st = set_error(ERR_BUSY, "replicate: cancelled"); break; }

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
        st = rechunk ? sink->v->has_snapshot_at(sink->ctx, snap_id, ts, &have)
                     : sink->v->has_snapshot(sink->ctx, snap_id, root, &have);
        if (st != OK) break;
        if (have) { res.snapshots_skipped++; continue; }

        /* Zeroed: replicate_tree returns before writing it on a bad
         * argument, and the counters below are read either way. */
        replicate_result_t one;
        memset(&one, 0, sizeof(one));
        st = transfer_one(store, sink, rechunk, &rc, root, cancel, &one);
        res.rechunked         = one.rechunked;
        res.files_rechunked  += one.files_rechunked;
        res.trees_rebuilt    += one.trees_rebuilt;
        res.objects_written  += one.objects_written;
        memcpy(res.target_root, one.target_root, OBJECT_HASH_SIZE);
        res.objects_asked    += one.objects_asked;
        res.objects_sent     += one.objects_sent;
        res.bytes_sent       += one.bytes_sent;
        res.subtrees_skipped += one.subtrees_skipped;
        res.objects_damaged  += one.objects_damaged;
        for (uint32_t d = 0; d < one.damaged_listed &&
                             res.damaged_listed < REPLICATE_DAMAGED_LIST_MAX; d++) {
            res.damaged[res.damaged_listed] = one.damaged[d];
            res.damaged[res.damaged_listed].snapshot_id = snap_id;
            res.damaged_listed++;
        }
        if (st != OK) {
            /*
             * Named here because the sink cannot: a sink seals packs as they
             * fill, so an install can fail inside put_object, which is not
             * told which snapshot it is serving. Whatever failed, this
             * snapshot gets no record and neither does any after it.
             * Copied first: err_msg() is the buffer set_error writes into.
             */
            char why[ERR_MSG_MAX];
            snprintf(why, sizeof(why), "%s", err_msg());
            st = set_error(st, "replicate: snapshot %lld was not recorded: %s",
                           (long long)snap_id, why);
            break;
        }

        replica_snapshot_t rs = {
            .snapshot_id = snap_id,
            .ts          = ts,
            .label       = label,
            .file_count  = (uint64_t)strtoull(snaps->rows[i][4], NULL, 10),
            .total_size  = (uint64_t)strtoull(snaps->rows[i][5], NULL, 10),
        };
        memcpy(rs.root_tree_hash, one.target_root, OBJECT_HASH_SIZE);

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
    free(rc.memo);
    if (out) *out = res;

    /* The source's mark, carried across once its snapshots are, so the
     * replica never sits below it (rule 3 of snapshot_ids.h). Taken from
     * the source repository's file and its index together: either alone
     * may lag the other after a lost database or a damaged file. */
    if (st == OK && sink->v->raise_snapshot_ids) {
        int64_t mark = 0;
        if (snapshot_ids_read_repo(store_root(store), &mark) != OK) {
            log_warn("replicate: source mark unreadable (%s); using the index", err_msg());
            err_clear();
            mark = 0;
        }
        db_result_t *r = NULL;
        if (db_execute(db,
                "SELECT MAX(COALESCE((SELECT last_id FROM snapshot_id_seq WHERE repo_id = ?), 0),"
                "           COALESCE((SELECT MAX(id) FROM snapshots WHERE repo_id = ?), 0))",
                (const char *[]){ repo, repo, NULL }, &r) == 0 && r && r->nrows == 1) {
            int64_t idx = strtoll(r->rows[0][0], NULL, 10);
            if (idx > mark) mark = idx;
        }
        db_result_free(r);
        st = sink->v->raise_snapshot_ids(sink->ctx, mark);
    }

    /*
     * Everything else went across and the snapshots are recorded, which is
     * what keeps one lost object from blocking replication for ever. The
     * job still fails: the replica holds these as damaged under parity that
     * cannot correct them, and an OK here is a replica silently taking bad
     * data (OWN-11). The failure path raises the alert.
     */
    if (st == OK && res.objects_damaged > 0)
        st = set_error(ERR_CORRUPT, "replicated with %llu object(s) damaged beyond "
                       "parity; the replica holds them as damaged",
                       (unsigned long long)res.objects_damaged);
    return st;
}
