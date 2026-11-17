#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#define _GNU_SOURCE

#include "gc.h"

#include "chunklist.h"
#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "repo.h"
#include "db_txn.h"
#include "store_index.h"
#include "db_write.h"

#include <cutils/log.h>

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

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

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

typedef struct {
    store_t     *store;
    cutils_db_t *db;
    const char  *repo;
    gc_result_t *res;
} mark_ctx_t;

/*
 * Count one reference. *out_count gets the refcount after it and *out_type
 * the type the index records (both 0 when the index has no row for the
 * hash). Marks start from zero, so a count of 1 means this run has just
 * reached the object for the first time, which is what lets chunk lists be
 * walked once per run without holding a visited set.
 */
static status_t mark_object_ex(mark_ctx_t *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                               int64_t *out_count, uint8_t *out_type)
{
    if (out_count) *out_count = 0;
    if (out_type) *out_type = 0;
    if (hash_is_zero(hash)) return OK;
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(hash, hex);
    db_result_t *r = NULL;
    if (db_write_returning(ctx->db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ? RETURNING refcount, type",
            (const char *[]){ ctx->repo, hex, NULL }, &r) != 0)
        return set_error(ERR_IO, "gc: cannot mark object");
    if (r && r->nrows > 0) {
        /* An empty string is c-utils' NULL (CLAUDE.md); strtoll gives 0. */
        if (out_count) *out_count = strtoll(r->rows[0][0], NULL, 10);
        if (out_type) *out_type = (uint8_t)strtoul(r->rows[0][1], NULL, 10);
    }
    db_result_free(r);
    ctx->res->reachable++;
    return OK;
}

static status_t mark_object(mark_ctx_t *ctx, const uint8_t hash[OBJECT_HASH_SIZE])
{
    return mark_object_ex(ctx, hash, NULL, NULL);
}

/*
 * A file's content, which in format 3 may be a chunk list (chunklist.h).
 *
 * The content object itself is counted on every reference, like any other.
 * What a list names - sub-lists and chunks - is counted only the first time
 * this run reaches the list: a list shared by every snapshot of an
 * unchanged file would otherwise have its chunks walked once per snapshot,
 * an index write per chunk per snapshot. A chunk's count is therefore the
 * number of distinct lists naming it, never zero while any live list does,
 * which is the only property the sweep reads.
 *
 * Whether the content is a list comes from the index row, which every
 * writer records with the object's real type (the name includes it). The
 * repository is asked instead when there is no row, and when the row says
 * FILE for content larger than any single object can be: that row is wrong,
 * and believing it would sweep every chunk of the file.
 */
static status_t mark_content(mark_ctx_t *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                             uint64_t node_size, atomic_int *cancel)
{
    if (hash_is_zero(hash)) return OK;
    int64_t n = 0;
    uint8_t type = 0;
    status_t st = mark_object_ex(ctx, hash, &n, &type);
    if (st != OK) return st;
    if (n > 1) return OK;               /* walked earlier in this run */

    if (type == 0 || (type == OBJECT_TYPE_FILE && node_size > OBJECT_MAX_BYTES)) {
        uint64_t size = 0;
        st = object_get_info(ctx->store, hash, &size, &type);
        if (st == ERR_NOT_FOUND) {
            /* Absent from the repository: nothing beneath it can be read
             * or restored either, so there is nothing to keep alive. */
            err_clear();
            ctx->res->content_missing++;
            return OK;
        }
        if (st != OK)
            return set_error(ERR_CORRUPT, "gc: cannot read file content (%s), "
                             "refusing to sweep", err_msg());
    }
    if (type != OBJECT_TYPE_CHUNKLIST) return OK;

    obj_loader_t ld = obj_loader_store(ctx->store);
    chunklist_iter_t *it = NULL;
    st = chunklist_iter_open(&ld, hash, OBJECT_TYPE_FILE, 1, NULL, &it);
    for (int done = 0; st == OK && !done; ) {
        chunklist_item_t x;
        st = chunklist_iter_next(it, &x, &done);
        if (st != OK || done) break;
        if (cancel_requested(cancel)) { st = set_error(ERR_BUSY, "gc cancelled"); break; }
        int64_t c = 0;
        st = mark_object_ex(ctx, x.hash, &c, NULL);
        /* A sub-list reached before (from another file's list) has had its
         * subtree marked already. A missing row (c == 0) is walked: it
         * cannot be told apart from a first visit. */
        if (st == OK && x.is_list && c > 1) chunklist_iter_skip(it);
    }
    chunklist_iter_close(it);
    if (st == ERR_BUSY) return st;
    if (st != OK)
        return set_error(ERR_CORRUPT, "gc: cannot read chunk list (%s), "
                         "refusing to sweep", err_msg());
    return OK;
}

/* manifest_iter's report of every object a directory consists of. The root
 * is marked by mark_tree itself; fragments and tree lists are marked here,
 * once per visit of the directory, as trees are. */
typedef struct {
    mark_ctx_t *ctx;
    const uint8_t *root;
    status_t st;
} dir_objs_t;

static void mark_dir_object(void *arg, const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type)
{
    (void)type;
    dir_objs_t *d = arg;
    if (d->st != OK || !memcmp(hash, d->root, OBJECT_HASH_SIZE)) return;
    d->st = mark_object(d->ctx, hash);
}

/*
 * Walk a tree, marking it and everything it points at.
 *
 * A tree shared by ten snapshots is visited ten times and marked ten times,
 * which is the point: the resulting count is how many references exist, and
 * pruning one snapshot must not collect a tree the other nine still use.
 */
static status_t mark_tree(mark_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                          atomic_int *cancel, int depth)
{
    if (cancel_requested(cancel)) return set_error(ERR_BUSY, "gc cancelled");
    if (depth > 512) return set_error(ERR_CORRUPT, "gc: tree nesting too deep");

    status_t st = mark_object(ctx, tree_hash);
    if (st != OK) return st;

    /*
     * One fragment at a time (OWN-34): a directory of any size is read in
     * bounded pieces, and every fragment and tree list it consists of is
     * marked as it is loaded. An unreadable tree is a damaged repository.
     * Marking stops rather than continuing, because sweeping on an
     * incomplete mark would delete live objects: the one failure mode GC
     * must never have.
     */
    obj_loader_t ld = obj_loader_store(ctx->store);
    dir_objs_t dobj = { ctx, tree_hash, OK };
    manifest_iter_t *it = NULL;
    st = manifest_iter_open(&ld, tree_hash, mark_dir_object, &dobj, &it);
    if (st != OK)
        return set_error(ERR_CORRUPT, "gc: cannot read tree, refusing to sweep");

    for (;;) {
        const manifest_entry_t *e = NULL;
        int done = 0;
        st = manifest_iter_next(it, &e, &done);
        if (st != OK) {
            st = set_error(ERR_CORRUPT, "gc: cannot read tree, refusing to sweep");
            break;
        }
        if (dobj.st != OK) { st = dobj.st; break; }
        if (done) break;
        if (e->type == NODE_TYPE_DIR) {
            if (!hash_is_zero(e->target_hash))
                st = mark_tree(ctx, e->target_hash, cancel, depth + 1);
        } else if (e->type == NODE_TYPE_REG) {
            st = mark_content(ctx, e->target_hash, e->size, cancel);
        } else {
            st = mark_object(ctx, e->target_hash);
        }
        if (st == OK) st = mark_object(ctx, e->xattr_hash);
        if (st == OK) st = mark_object(ctx, e->acl_hash);
        if (st != OK) break;
    }

    manifest_iter_close(it);
    if (st == OK) st = dobj.st;
    return st;
}

status_t gc_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                int dry_run, atomic_int *cancel, gc_result_t *out)
{
    if (!store || !db) return set_error(ERR_INVALID, "gc_run: null argument");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    gc_result_t res;
    memset(&res, 0, sizeof(res));

    /* gc and coalescing move objects between packs, under any reader that
     * has a pack open. An open NBD export is such a reader for hours, so gc
     * waits for the next run instead (vm-backup.md 7.3). The export takes
     * the read lock to record itself, and gc holds the write lock, so one
     * opened after this check cannot exist while gc runs. */
    {
        db_result_t *r = NULL;
        int open = db_execute(db, "SELECT id FROM exports WHERE repo_id = ? AND closed_at IS NULL",
                              (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows > 0;
        char eid[32] = "";
        if (open) snprintf(eid, sizeof(eid), "%s", r->rows[0][0]);
        db_result_free(r);
        if (open)
            return set_error(ERR_BUSY, "gc deferred: export %s is open on this repository; "
                             "gc runs again on its next schedule, or after the export is "
                             "revoked (DELETE /exports/%s)", eid, eid);
    }

    /*
     * Staging files from a killed writer. gc holds the write lock, so no
     * other job is writing this repository and anything already in tmp/
     * belongs to a process that is gone; see store_sweep_tmp_before. A dry
     * run promises to change nothing, and keeps that promise here too.
     */
    if (!dry_run)
        res.tmp_files_removed = store_sweep_tmp_before(store, time(NULL),
                                                       &res.tmp_bytes_removed);

    /* A pack removed just before a power cut can leave its row (B147). */
    if (!dry_run && store_index_drop_vanished_packs(db, repo_id) != OK) {
        log_warn("gc: %s", err_msg());
        err_clear();
    }

    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "gc: begin");

    /* Clear every mark first. Anything still at zero after the walk is
     * unreachable from any snapshot. */
    if (db_write(db, "UPDATE objects SET refcount = 0 WHERE repo_id = ?",
                 (const char *[]){ repo, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "gc: cannot clear marks");
    }

    db_result_t *snaps = NULL;
    if (db_execute(db, "SELECT id, root_tree_hash FROM snapshots WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &snaps) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "gc: cannot list snapshots");
    }

    mark_ctx_t ctx = { store, db, repo, &res };
    status_t st = OK;
    for (int i = 0; i < (snaps ? snaps->nrows : 0) && st == OK; i++) {
        uint8_t root[OBJECT_HASH_SIZE];
        hex_to_hash(snaps->rows[i][1], root);
        st = mark_tree(&ctx, root, cancel, 0);
    }
    db_result_free(snaps);

    if (st != OK) {
        db_txn_rollback(db);
        return st;
    }

    /* Count and size what would go, before deciding whether to act. */
    db_result_t *dead = NULL;
    if (db_execute(db,
            "SELECT hash, COALESCE(phys_size, 0), location FROM objects"
            " WHERE repo_id = ? AND refcount = 0",
            (const char *[]){ repo, NULL }, &dead) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "gc: cannot list unreachable objects");
    }

    for (int i = 0; i < (dead ? dead->nrows : 0); i++) {
        res.swept++;
        res.bytes_freed += strtoull(dead->rows[i][1], NULL, 10);
    }

    if (dry_run) {
        db_result_free(dead);
        /* Marks are real work, but a dry run must leave nothing changed. */
        db_txn_rollback(db);
        if (out) *out = res;
        return OK;
    }

    db_result_free(dead);

    /* Packed objects keep their bytes until compaction, but they must stop
     * being locatable now. Leaving the pack_objects row behind would let a
     * later restore resolve an object this sweep declared dead, and once
     * compaction rewrites the pack the row points into bytes that belong to
     * something else. */
    if (db_write(db,
            "DELETE FROM pack_objects WHERE repo_id = ? AND hash IN"
            " (SELECT hash FROM objects WHERE repo_id = ? AND refcount = 0)",
            (const char *[]){ repo, repo, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "gc: cannot clear pack locations");
    }

    if (db_write(db,
            "DELETE FROM objects WHERE repo_id = ? AND refcount = 0",
            (const char *[]){ repo, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "gc: cannot delete unreachable objects");
    }

    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "gc: commit");

    /*
     * Second half of the sweep: the bytes. A dead packed object lost its
     * rows above, but its bytes sit inside a pack that live objects share,
     * so reclaiming them means rewriting those packs without the dead
     * entries. pack_gc self-limits - a pack with nothing dead is left
     * alone - and runs the coalescing pass afterwards, so small trailing
     * packs merge under the same operation that orphaned them.
     *
     * The reference list is every surviving object row: the sweep just
     * deleted everything unreachable, so surviving is the definition of
     * referenced. Hashes travel as lowercase hex, which sorts in the same
     * order as the raw bytes, so ORDER BY hash hands pack_gc the sorted
     * array its bsearch needs without a second sort here.
     */
    if (cancel_requested(cancel))
        return set_error(ERR_BUSY, "gc cancelled");

    /* With each name, where the index says its copy is: compaction keeps
     * that copy and drops any other of the same name once the indexed one
     * is confirmed (OWN-22). */
    db_result_t *live = NULL;
    if (db_execute(db,
            "SELECT o.hash, COALESCE(p.pack_id, 0), COALESCE(p.offset, 0)"
            " FROM objects o LEFT JOIN pack_objects p"
            "   ON p.repo_id = o.repo_id AND p.hash = o.hash"
            " WHERE o.repo_id = ? ORDER BY o.hash",
            (const char *[]){ repo, NULL }, &live) != 0)
        return set_error(ERR_IO, "gc: cannot list live objects for compaction");

    size_t refs_cnt = live ? (size_t)live->nrows : 0;
    uint8_t *refs = refs_cnt ? malloc(refs_cnt * OBJECT_HASH_SIZE) : NULL;
    pack_gc_loc_t *locs = refs_cnt ? malloc(refs_cnt * sizeof(*locs)) : NULL;
    if (refs_cnt && (!refs || !locs)) {
        free(refs);
        free(locs);
        db_result_free(live);
        return set_error(ERR_NOMEM, "gc: refs alloc");
    }
    for (size_t i = 0; i < refs_cnt; i++) {
        hex_to_hash(live->rows[i][0], refs + i * OBJECT_HASH_SIZE);
        locs[i].pack   = (uint32_t)strtoul(live->rows[i][1], NULL, 10);
        locs[i].offset = strtoull(live->rows[i][2], NULL, 10);
    }
    db_result_free(live);

    uint32_t kept = 0, dropped = 0;
    pack_gc_stats_t pstats;
    status_t pst = pack_gc_stats_loc(store, refs, locs, refs_cnt, &kept, &dropped, &pstats);
    free(refs);
    free(locs);
    res.pack_entries_kept    = kept;
    res.pack_entries_dropped = dropped;
    res.packs_rewritten      = pstats.packs_rewritten;
    res.packs_deleted        = pstats.packs_deleted;
    res.packs_coalesced      = pstats.packs_coalesced;
    res.packs_removed_by_coalesce = pstats.packs_removed_by_coalesce;
    res.pack_entries_damaged = pstats.entries_damaged;
    res.damaged_listed       = pstats.damaged_listed;
    memcpy(res.damaged, pstats.damaged, sizeof(res.damaged));
    res.packs_unwalkable  = pstats.packs_unwalkable;
    res.dups_dropped      = pstats.dups_dropped;
    res.dups_kept         = pstats.dups_kept;
    res.index_relocated   = pstats.index_relocated;
    res.unwalkable_listed = pstats.unwalkable_listed;
    memcpy(res.unwalkable, pstats.unwalkable, sizeof(res.unwalkable));
    /* The sweep is committed whatever compaction did, so a caller reporting
     * a failed or cancelled gc can still say what it removed. */
    if (out) *out = res;
    if (pst != OK) return pst;

    log_info("gc: %llu reachable references, %llu objects swept, %llu bytes, "
             "%u pack entr%s dropped",
             (unsigned long long)res.reachable, (unsigned long long)res.swept,
             (unsigned long long)res.bytes_freed,
             dropped, dropped == 1 ? "y" : "ies");
    if (out) *out = res;

    /* Compaction finished; it copied these as they were rather than stop at
     * one lost object. Their new parity covers the damage, so they cannot
     * be corrected any more, and that is a failure to report (OWN-31). */
    if (res.pack_entries_damaged > 0 && res.packs_unwalkable > 0)
        return set_error(ERR_CORRUPT, "gc: %llu pack entr%s damaged beyond parity "
                         "%s copied as damaged, and %u pack(s) could not be walked "
                         "(first: pack-%010u.dat, %s); run verify with repair",
                         (unsigned long long)res.pack_entries_damaged,
                         res.pack_entries_damaged == 1 ? "y" : "ies",
                         res.pack_entries_damaged == 1 ? "was" : "were",
                         res.packs_unwalkable, res.unwalkable[0].pack,
                         res.unwalkable[0].reason);
    if (res.pack_entries_damaged > 0)
        return set_error(ERR_CORRUPT, "gc: %llu pack entr%s damaged beyond parity "
                         "%s copied as damaged",
                         (unsigned long long)res.pack_entries_damaged,
                         res.pack_entries_damaged == 1 ? "y" : "ies",
                         res.pack_entries_damaged == 1 ? "was" : "were");
    /* A pack left whole because its headers cannot be walked holds space gc
     * could not reclaim and, worse, damage nobody would otherwise hear
     * about until a restore: the job fails and says which, as for damage. */
    if (res.packs_unwalkable > 0 && store_needs_key(store))
        return set_error(ERR_CORRUPT, "gc: %u pack(s) could not be walked (first: "
                         "pack-%010u.dat); " REPO_SALVAGE_NEEDS_KEY,
                         res.packs_unwalkable, res.unwalkable[0].pack);
    if (res.packs_unwalkable > 0)
        return set_error(ERR_CORRUPT, "gc: %u pack(s) could not be walked and were left "
                         "as they are (first: pack-%010u.dat, %s); run verify with repair",
                         res.packs_unwalkable, res.unwalkable[0].pack,
                         res.unwalkable[0].reason);
    return OK;
}
