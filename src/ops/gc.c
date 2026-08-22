#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#define _GNU_SOURCE

#include "gc.h"

#include "manifest.h"
#include "object.h"

#include <cutils/log.h>

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static status_t mark_object(mark_ctx_t *ctx, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (hash_is_zero(hash)) return OK;
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(hash, hex);
    if (db_execute_non_query(ctx->db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ?",
            (const char *[]){ ctx->repo, hex, NULL }, NULL) != 0)
        return set_error(ERR_IO, "gc: cannot mark object");
    ctx->res->reachable++;
    return OK;
}

/*
 * Walk a tree, marking it and everything it points at.
 *
 * A tree shared by ten snapshots is visited ten times and marked ten times,
 * which is the point: the resulting count is how many references exist, and
 * pruning one snapshot must not collect a tree the other nine still use.
 */
static status_t mark_tree(mark_ctx_t *ctx, const uint8_t tree_hash[OBJECT_HASH_SIZE],
                          volatile int *cancel, int depth)
{
    if (cancel && *cancel) return set_error(ERR_BUSY, "gc cancelled");
    if (depth > 512) return set_error(ERR_CORRUPT, "gc: tree nesting too deep");

    status_t st = mark_object(ctx, tree_hash);
    if (st != OK) return st;

    manifest_entry_t *entries = NULL;
    uint32_t count = 0;
    st = manifest_read_tree(ctx->store, tree_hash, &entries, &count);
    if (st != OK) {
        /* An unreadable tree is a damaged repository. Marking stops rather
         * than continuing, because sweeping on an incomplete mark would
         * delete live objects: the one failure mode GC must never have. */
        return set_error(ERR_CORRUPT, "gc: cannot read tree, refusing to sweep");
    }

    for (uint32_t i = 0; i < count && st == OK; i++) {
        manifest_entry_t *e = &entries[i];
        if (e->type == NODE_TYPE_DIR) {
            if (!hash_is_zero(e->target_hash))
                st = mark_tree(ctx, e->target_hash, cancel, depth + 1);
        } else {
            st = mark_object(ctx, e->target_hash);
        }
        if (st == OK) st = mark_object(ctx, e->xattr_hash);
        if (st == OK) st = mark_object(ctx, e->acl_hash);
    }

    manifest_free_entries(entries, count);
    return st;
}

status_t gc_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                int dry_run, volatile int *cancel, gc_result_t *out)
{
    if (!store || !db) return set_error(ERR_INVALID, "gc_run: null argument");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    gc_result_t res;
    memset(&res, 0, sizeof(res));

    if (db_exec_raw(db, "BEGIN") != 0)
        return set_error(ERR_IO, "gc: begin");

    /* Clear every mark first. Anything still at zero after the walk is
     * unreachable from any snapshot. */
    if (db_execute_non_query(db, "UPDATE objects SET refcount = 0 WHERE repo_id = ?",
                             (const char *[]){ repo, NULL }, NULL) != 0) {
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("gc: rollback failed");
        return set_error(ERR_IO, "gc: cannot clear marks");
    }

    db_result_t *snaps = NULL;
    if (db_execute(db, "SELECT id, root_tree_hash FROM snapshots WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &snaps) != 0) {
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("gc: rollback failed");
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
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("gc: rollback failed");
        return st;
    }

    /* Count and size what would go, before deciding whether to act. */
    db_result_t *dead = NULL;
    if (db_execute(db,
            "SELECT hash, COALESCE(comp_size, 0), location FROM objects"
            " WHERE repo_id = ? AND refcount = 0",
            (const char *[]){ repo, NULL }, &dead) != 0) {
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("gc: rollback failed");
        return set_error(ERR_IO, "gc: cannot list unreachable objects");
    }

    for (int i = 0; i < (dead ? dead->nrows : 0); i++) {
        res.swept++;
        res.bytes_freed += strtoull(dead->rows[i][1], NULL, 10);
    }

    if (dry_run) {
        db_result_free(dead);
        /* Marks are real work, but a dry run must leave nothing changed. */
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("gc: rollback failed");
        if (out) *out = res;
        return OK;
    }

    /* Loose objects are unlinked here; packed ones only lose their rows, and
     * their bytes are reclaimed later by pack compaction. Unlinking a pack
     * because one object inside it died would take the live ones with it. */
    for (int i = 0; i < (dead ? dead->nrows : 0); i++) {
        if (strcmp(dead->rows[i][2], "loose") != 0) continue;
        const char *hex = dead->rows[i][0];
        char path[PATH_MAX];
        if (snprintf(path, sizeof(path), "%s/objects/%.2s/%s",
                     store_root(store), hex, hex + 2) >= (int)sizeof(path))
            continue;
        if (unlink(path) != 0 && errno != ENOENT)
            log_warn("gc: cannot unlink %s: %s", path, strerror(errno));
    }
    db_result_free(dead);

    if (db_execute_non_query(db,
            "DELETE FROM objects WHERE repo_id = ? AND refcount = 0",
            (const char *[]){ repo, NULL }, NULL) != 0) {
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("gc: rollback failed");
        return set_error(ERR_IO, "gc: cannot delete unreachable objects");
    }

    if (db_exec_raw(db, "COMMIT") != 0)
        return set_error(ERR_IO, "gc: commit");

    /* The in-memory loose set predates the sweep and now lists files that are
     * gone; dropping it forces a rebuild on next use. */
    store_clear_loose_set(store);

    log_info("gc: %llu reachable references, %llu objects swept, %llu bytes",
             (unsigned long long)res.reachable, (unsigned long long)res.swept,
             (unsigned long long)res.bytes_freed);
    if (out) *out = res;
    return OK;
}
