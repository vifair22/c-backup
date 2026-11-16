#define _POSIX_C_SOURCE 200809L

#include "store_index.h"

#include "pack.h"   /* PACK_VERSION: the format new packs are written in */
#include "db_txn.h"
#include "db_typed.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct store_index {
    cutils_db_t *db;
    int64_t      repo_id;
};

/* Hashes are bound as raw 32-byte BLOBs (db_typed.h): every hash column in
 * the schema holds the raw name, never hex text (NIT-12). */

/* ------------------------------------------------------------------ */
/* locate: the engine's only way to find a packed object                */
/* ------------------------------------------------------------------ */

static status_t db_locate(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                          store_location_t *out)
{
    store_index_t *idx = user;
    db_param_t p[] = { DB_INT64(idx->repo_id), DB_HASH(hash) };
    db_result_t *r = NULL;
    int rc = db_execute_typed(idx->db,
        "SELECT po.pack_id, po.offset, po.comp_size, po.orig_size,"
        "       po.entry_index, p.version"
        "  FROM pack_objects po"
        "  JOIN packs p ON p.repo_id = po.repo_id AND p.id = po.pack_id"
        " WHERE po.repo_id = ? AND po.hash = ?",
        p, DB_NPARAMS(p), &r);

    if (rc != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "object not in any pack");
    }

    memset(out, 0, sizeof(*out));
    out->in_pack      = 1;
    out->pack_id      = strtoull(r->rows[0][0], NULL, 10);
    out->offset       = strtoull(r->rows[0][1], NULL, 10);
    out->comp_size    = strtoull(r->rows[0][2], NULL, 10);
    out->orig_size    = strtoull(r->rows[0][3], NULL, 10);
    out->entry_index  = (uint32_t)strtoul(r->rows[0][4], NULL, 10);
    out->pack_version = (uint32_t)strtoul(r->rows[0][5], NULL, 10);
    db_result_free(r);
    return OK;
}

/* ------------------------------------------------------------------ */
/* pack commit: replaces the .idx sidecar                               */
/* ------------------------------------------------------------------ */

/*
 * Pack writing is multi-threaded, so several workers can finish a pack at the
 * same moment and each try to open a transaction on the one shared database
 * handle. SQLite has no nested transactions, so the second BEGIN fails, the
 * callback bails, and that pack's rows are never written: its objects are on
 * disk and unreachable. Serialising the commits is enough, and they are rare
 * and short compared to the packing itself.
 */
/*
 * The pack commit mutex is gone: db_txn_begin serialises every writer in the
 * daemon, which is a superset of what this guarded. It existed because two
 * pack workers issuing BEGIN on the shared handle collided, the second one's
 * rows were never written, and its objects sat on disk unreachable.
 *
 * Lock ordering note, since this is the site that used to hold two: whatever
 * replaces it must still be taken before the transaction, never after. See
 * db_txn.h.
 */

static void db_pack_commit(void *user, const pack_commit_info_t *info)
{
    store_index_t *idx = user;
    char repo[32], pack[32], size[32], ver[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)idx->repo_id);
    snprintf(pack, sizeof(pack), "%llu", (unsigned long long)info->pack_id);
    snprintf(size, sizeof(size), "%llu", (unsigned long long)info->size);
    snprintf(ver,  sizeof(ver),  "%u",   PACK_VERSION);

    /*
     * One transaction for the pack row and all of its object rows. A partial
     * insert would leave objects that the resolver cannot find even though
     * their bytes are on disk, which reads as data loss.
     *
     * The pack file is already installed by the time this runs; that ordering
     * is deliberate and explained in daemonization-spec.md section 12.
     */
    if (db_txn_begin(idx->db) != OK) {
        log_error("pack commit: cannot begin transaction for pack %s", pack);
        return;
    }

    int failed = 0;
    if (db_execute_non_query(idx->db,
            "INSERT OR REPLACE INTO packs (repo_id, id, filename, size, sealed_at, version)"
            " VALUES (?, ?, ?, ?, strftime('%s','now'), ?)",
            (const char *[]){ repo, pack, info->filename, size, ver, NULL }, NULL) != 0)
        failed = 1;

    for (uint32_t i = 0; !failed && i < info->entry_count; i++) {
        const pack_commit_entry_t *en = &info->entries[i];
        db_param_t pp[] = { DB_INT64(idx->repo_id), DB_HASH(en->hash),
                            DB_INT64((int64_t)info->pack_id), DB_INT64((int64_t)en->offset),
                            DB_INT64((int64_t)en->comp_size), DB_INT64((int64_t)en->orig_size),
                            DB_INT64(en->entry_index) };
        if (db_execute_non_query_typed(idx->db,
                "INSERT OR REPLACE INTO pack_objects"
                " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                pp, DB_NPARAMS(pp), NULL) != 0)
            failed = 1;

        /*
         * The object moved from staged to installed, so its location and
         * footprint become the pack's. phys_size is the bytes it occupies
         * inside the pack; the pack's own headers and parity trailer are
         * charged to packs.size rather than split across its entries, so total
         * bytes on disk is sum(packs.size).
         *
         * Inserted when there is no row yet. A row written after the commit
         * would stay 'staged', be dropped by the next backup, and leave an
         * object compaction reads as dead; creating it here, in the same
         * transaction as the pack's rows, means no writer has to win a race
         * against its own seal (a backup's file workers record no rows of
         * their own). An existing row keeps its type and refcount.
         */
        db_param_t op[] = { DB_INT64(idx->repo_id), DB_HASH(en->hash),
                            DB_INT64((int64_t)en->comp_size), DB_INT64((int64_t)en->orig_size),
                            DB_TEXT(pack), DB_INT64(en->type) };
        if (!failed && db_execute_non_query_typed(idx->db,
                "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
                "                     location, type)"
                " VALUES (?, ?, 0, ?, ?, ?, ?)"
                " ON CONFLICT(repo_id, hash) DO UPDATE SET location = excluded.location,"
                "   phys_size = excluded.phys_size, orig_size = excluded.orig_size",
                op, DB_NPARAMS(op), NULL) != 0)
            failed = 1;
    }

    if (failed) {
        db_txn_rollback(idx->db);
        log_error("pack commit: rolled back pack %s (%u objects)", pack, info->entry_count);
        return;
    }
    if (db_txn_commit(idx->db) != OK)
        log_error("pack commit: commit failed for pack %s", pack);
}

/*
 * A pack was deleted or is about to be replaced. Its rows go with it.
 *
 * Ordering mirrors the commit side: pack_objects first, then the packs row,
 * so nothing ever references a pack row that is already gone.
 */
static void pack_removed(void *user, uint64_t pack_id)
{
    store_index_t *idx = user;
    if (!idx) return;

    /*
     * Its own transaction, where it used to run two bare DELETEs under the
     * pack commit mutex and join whatever transaction happened to be open.
     * They belong together: a pack_objects row outliving its packs row
     * resolves to bytes that now belong to something else.
     */
    char repo[32], pack[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)idx->repo_id);
    snprintf(pack, sizeof(pack), "%llu", (unsigned long long)pack_id);

    if (db_txn_begin(idx->db) != OK) {
        log_error("pack removed: cannot begin transaction for pack %s", pack);
        return;
    }

    /*
     * Either both rows go or neither does. Committing half of this is the
     * stale-row bug in a new shape: a pack_objects row whose packs row is
     * gone still resolves, to bytes that now belong to something else.
     */
    if (db_execute_non_query(idx->db,
            "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0) {
        log_error("pack removed: cannot clear pack_objects for pack %s", pack);
        db_txn_rollback(idx->db);
        return;
    }

    if (db_execute_non_query(idx->db,
            "DELETE FROM packs WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0) {
        log_error("pack removed: cannot delete packs row %s", pack);
        db_txn_rollback(idx->db);
        return;
    }

    if (db_txn_commit(idx->db) != OK)
        log_error("pack removed: commit failed for pack %s", pack);
}

/* ------------------------------------------------------------------ */

status_t store_index_bind(store_t *store, cutils_db_t *db, int64_t repo_id,
                          store_index_t **out)
{
    if (!store || !db || !out)
        return set_error(ERR_INVALID, "store_index_bind: null argument");

    store_index_t *idx = calloc(1, sizeof(*idx));
    if (!idx) return set_error(ERR_NOMEM, "store_index_bind: alloc");
    idx->db = db;
    idx->repo_id = repo_id;

    store_set_locate_cb(store, db_locate, idx);
    store_set_pack_commit_cb(store, db_pack_commit, idx);
    store_set_pack_removed_cb(store, pack_removed, idx);
    *out = idx;
    return OK;
}

void store_index_free(store_index_t *idx)
{
    free(idx);
}

static status_t put_object(cutils_db_t *db, int64_t repo_id,
                           const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                           uint64_t phys_size, uint64_t orig_size,
                           const char *location);

status_t store_index_put_packed(cutils_db_t *db, int64_t repo_id,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint8_t type, uint64_t phys_size, uint64_t orig_size)
{
    /* The location and sizes are the pack's, as the commit that installed
     * it recorded them; the caller's figures describe nothing new. */
    (void)phys_size;
    (void)orig_size;
    db_param_t p[] = { DB_INT64(type), DB_INT64(repo_id), DB_HASH(hash) };
    if (db_execute_non_query_typed(db,
            "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
            "                     location, type)"
            " SELECT repo_id, hash, 0, comp_size, orig_size, CAST(pack_id AS TEXT), ?"
            " FROM pack_objects WHERE repo_id = ? AND hash = ?"
            " ON CONFLICT(repo_id, hash) DO NOTHING",
            p, DB_NPARAMS(p), NULL) != 0)
        return set_error(ERR_IO, "cannot record object");
    return OK;
}

status_t store_index_put_staged(cutils_db_t *db, int64_t repo_id,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                uint8_t type, uint64_t phys_size, uint64_t orig_size)
{
    return put_object(db, repo_id, hash, type, phys_size, orig_size, "staged");
}

status_t store_index_drop_staged(cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int rc = repo_id > 0
        ? db_execute_non_query(db,
              "DELETE FROM objects WHERE repo_id = ? AND location = 'staged'",
              (const char *[]){ repo, NULL }, NULL)
        : db_execute_non_query(db,
              "DELETE FROM objects WHERE location = 'staged'", NULL, NULL);
    return rc == 0 ? OK : set_error(ERR_IO, "cannot drop staged objects");
}

static status_t put_object(cutils_db_t *db, int64_t repo_id,
                           const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                           uint64_t phys_size, uint64_t orig_size,
                           const char *location)
{
    db_param_t p[] = { DB_INT64(repo_id), DB_HASH(hash), DB_INT64((int64_t)phys_size),
                       DB_INT64((int64_t)orig_size), DB_TEXT(location), DB_INT64(type) };

    /*
     * Content-addressed: seeing the same hash twice is dedup working, not an
     * error, so an existing row is left alone rather than overwritten - with
     * one exception. A row recorded as zero bytes for an object at the same
     * location is a dedup against an object whose size was not known when
     * the row was written, and no stored object is zero bytes: every entry
     * has a header. Taking the
     * real size when it is offered repairs those rows on the next backup
     * that meets them, and cannot touch a correct one.
     */
    if (db_execute_non_query_typed(db,
            "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
            "                     location, type)"
            " VALUES (?, ?, 0, ?, ?, ?, ?)"
            " ON CONFLICT(repo_id, hash) DO UPDATE SET phys_size = excluded.phys_size"
            " WHERE objects.phys_size = 0 AND excluded.phys_size > 0"
            "   AND objects.location = excluded.location",
            p, DB_NPARAMS(p), NULL) != 0)
        return set_error(ERR_IO, "cannot record object");
    return OK;
}

status_t store_index_addref(cutils_db_t *db, int64_t repo_id,
                            const uint8_t hash[OBJECT_HASH_SIZE])
{
    db_param_t p[] = { DB_INT64(repo_id), DB_HASH(hash) };
    if (db_execute_non_query_typed(db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ?",
            p, DB_NPARAMS(p), NULL) != 0)
        return set_error(ERR_IO, "cannot addref object");
    return OK;
}
