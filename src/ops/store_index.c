#define _POSIX_C_SOURCE 200809L

#include "store_index.h"

#include "pack.h"   /* PACK_VERSION: the format new packs are written in */

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct store_index {
    cutils_db_t *db;
    int64_t      repo_id;
};

/* SQLite's C API takes blobs, but c-utils' db layer binds string parameters,
 * so hashes travel as lowercase hex and the schema's BLOB columns are written
 * through X'..' literals. Hex costs 64 bytes per hash instead of 32; the
 * alternative is a second binding path through the whole db layer for one
 * column type. */
static void hash_to_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    static const char *d = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        out[i * 2]     = d[(h[i] >> 4) & 0xF];
        out[i * 2 + 1] = d[h[i] & 0xF];
    }
    out[OBJECT_HASH_SIZE * 2] = '\0';
}

/* ------------------------------------------------------------------ */
/* locate: the engine's only way to find a packed object                */
/* ------------------------------------------------------------------ */

static status_t db_locate(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                          store_location_t *out)
{
    store_index_t *idx = user;
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(hash, hex);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)idx->repo_id);

    db_result_t *r = NULL;
    int rc = db_execute(idx->db,
        "SELECT po.pack_id, po.offset, po.comp_size, po.orig_size,"
        "       po.entry_index, p.version"
        "  FROM pack_objects po"
        "  JOIN packs p ON p.repo_id = po.repo_id AND p.id = po.pack_id"
        " WHERE po.repo_id = ? AND po.hash = ?",
        (const char *[]){ repo, hex, NULL }, &r);

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
    if (db_exec_raw(idx->db, "BEGIN") != 0) {
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
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        hash_to_hex(info->entries[i].hash, hex);
        char off[32], comp[32], orig[32], eidx[16];
        snprintf(off,  sizeof(off),  "%llu", (unsigned long long)info->entries[i].offset);
        snprintf(comp, sizeof(comp), "%llu", (unsigned long long)info->entries[i].comp_size);
        snprintf(orig, sizeof(orig), "%llu", (unsigned long long)info->entries[i].orig_size);
        snprintf(eidx, sizeof(eidx), "%u", info->entries[i].entry_index);

        if (db_execute_non_query(idx->db,
                "INSERT OR REPLACE INTO pack_objects"
                " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, hex, pack, off, comp, orig, eidx, NULL },
                NULL) != 0)
            failed = 1;

        /*
         * The object moved from loose to packed, so both its location and its
         * footprint change. phys_size becomes the bytes it occupies inside the
         * pack; the pack's own headers and parity trailer are charged to
         * packs.size rather than split across its entries, so total bytes on
         * disk is sum(packs.size) plus phys_size of everything still loose.
         * Updating location alone left every packed object still claiming the
         * loose footprint it no longer has.
         */
        if (!failed && db_execute_non_query(idx->db,
                "UPDATE objects SET location = ?, phys_size = ?, orig_size = ?"
                " WHERE repo_id = ? AND hash = ?",
                (const char *[]){ pack, comp, orig, repo, hex, NULL }, NULL) != 0)
            failed = 1;
    }

    if (failed) {
        if (db_exec_raw(idx->db, "ROLLBACK") != 0)
            log_error("pack commit: rollback failed for pack %s", pack);
        log_error("pack commit: rolled back pack %s (%u objects)", pack, info->entry_count);
        return;
    }
    if (db_exec_raw(idx->db, "COMMIT") != 0)
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

    char repo[32], pack[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)idx->repo_id);
    snprintf(pack, sizeof(pack), "%llu", (unsigned long long)pack_id);

    if (db_execute_non_query(idx->db,
            "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0)
        log_error("pack removed: cannot clear pack_objects for pack %s", pack);

    if (db_execute_non_query(idx->db,
            "DELETE FROM packs WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0)
        log_error("pack removed: cannot delete packs row %s", pack);
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

status_t store_index_put_loose(cutils_db_t *db, int64_t repo_id,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint8_t type, uint64_t phys_size, uint64_t orig_size)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(hash, hex);
    char repo[32], phys[32], orig[32], t[8];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(phys, sizeof(phys), "%llu", (unsigned long long)phys_size);
    snprintf(orig, sizeof(orig), "%llu", (unsigned long long)orig_size);
    snprintf(t,    sizeof(t),    "%u",   type);

    /* Content-addressed: seeing the same hash twice is dedup working, not an
     * error, so an existing row is left alone rather than overwritten. */
    if (db_execute_non_query(db,
            "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
            "                     location, type)"
            " VALUES (?, ?, 0, ?, ?, 'loose', ?)"
            " ON CONFLICT(repo_id, hash) DO NOTHING",
            (const char *[]){ repo, hex, phys, orig, t, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot record object");
    return OK;
}

status_t store_index_addref(cutils_db_t *db, int64_t repo_id,
                            const uint8_t hash[OBJECT_HASH_SIZE])
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(hash, hex);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    if (db_execute_non_query(db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ?",
            (const char *[]){ repo, hex, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot addref object");
    return OK;
}
