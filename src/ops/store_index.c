#define _POSIX_C_SOURCE 200809L

#include "store_index.h"

#include "pack.h"   /* PACK_VERSION: the format new packs are written in */
#include "db_txn.h"
#include "db_write.h"
#include "db_readers.h"

#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

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

    /*
     * From a read-only connection when this thread holds no transaction
     * (B184): a file worker asking "already stored?" for every chunk used
     * to queue on the shared connection's mutex behind whatever commit
     * held it. A reader sees the last commit and never waits for a writer.
     *
     * That view is enough for dedup, and the claim set is why. A name is
     * only ever stored by the writer holding its claim, the claim is taken
     * before this lookup, and it is dropped only after the commit that put
     * the name's rows in the index (pack_backup_add_prepared, claims_release).
     * So for any name: either its claim is visible to the caller, or the
     * commit that ended it happened before the caller found the claim gone,
     * and a reader started after that commit sees the rows. A row committed
     * between the claim check and this read is seen, and harmless. What a
     * reader cannot see - rows written by a transaction still open on
     * another thread - is exactly what the claim covers, so it is never the
     * only place a staged name is recorded.
     *
     * A thread inside a transaction asks the shared connection instead: it
     * must see its own uncommitted rows (gc, reindex and a pack commit
     * reading what they have just written), and only that connection does.
     */
    if (!db_txn_held() && db_readers_active()) {
        uint64_t c[6];
        int f = db_readers_locate(idx->repo_id, hex, c);
        if (f == 1) {
            memset(out, 0, sizeof(*out));
            out->in_pack      = 1;
            out->pack_id      = c[0];
            out->offset       = c[1];
            out->comp_size    = c[2];
            out->orig_size    = c[3];
            out->entry_index  = (uint32_t)c[4];
            out->pack_version = (uint32_t)c[5];
            return OK;
        }
        if (f == 0) return set_error(ERR_NOT_FOUND, "object not in any pack");
        /* An error: ask the shared connection rather than call it absent. */
    }

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

/*
 * The rows of one pack entry, as strings for the db layer.
 */
typedef struct {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    char off[32], comp[32], orig[32], eidx[16], type[8];
} entry_args_t;

static void entry_args(const pack_commit_entry_t *e, entry_args_t *a)
{
    hash_to_hex(e->hash, a->hex);
    snprintf(a->off,  sizeof(a->off),  "%llu", (unsigned long long)e->offset);
    snprintf(a->comp, sizeof(a->comp), "%llu", (unsigned long long)e->comp_size);
    snprintf(a->orig, sizeof(a->orig), "%llu", (unsigned long long)e->orig_size);
    snprintf(a->eidx, sizeof(a->eidx), "%u", e->entry_index);
    snprintf(a->type, sizeof(a->type), "%u", e->type);
}

/* No size limit: a batch ends only for a queued writer, so a pack nobody
 * waits behind is one transaction, as it always was (OWN-16: the commits a
 * backup adds are bounded by foreign transactions, not by its own size).
 * Tests set a limit to give a small pack several commits. */
#define PACK_COMMIT_BATCH UINT32_MAX
static uint32_t g_pack_batch_rows = PACK_COMMIT_BATCH;
static status_t (*g_commit_gate)(void);

void store_index_set_pack_batch_rows(uint32_t rows)
{
    g_pack_batch_rows = rows ? rows : PACK_COMMIT_BATCH;
}

void store_index_set_commit_gate(status_t (*gate)(void))
{
    g_commit_gate = gate;
}

/* The test's power cut: the commit that would come next never happens, and
 * neither does anything after it, cleanup included. */
static int cut_before_commit(void)
{
    return g_commit_gate && g_commit_gate() != OK;
}

/*
 * Rows a pack commit wrote without reaching its packs row: back to what they
 * were before it began. Object rows go back to 'staged' rather than away,
 * because the only rows the new-row phase writes are new ones and staged
 * ones (see db_pack_commit); 'staged' is what a failed commit always left,
 * and what start-up drops.
 */
static void drop_unannounced(store_index_t *idx, const char *repo, const char *pack)
{
    if (db_txn_begin(idx->db) != OK) {
        log_error("pack commit: cannot begin cleanup of pack %s", pack);
        return;
    }
    int failed =
        db_write(idx->db,
            "UPDATE objects SET location = 'staged' WHERE repo_id = ? AND location = ?"
            " AND NOT EXISTS (SELECT 1 FROM packs WHERE repo_id = ? AND id = ?)",
            (const char *[]){ repo, pack, repo, pack, NULL }, NULL) != 0 ||
        db_write(idx->db,
            "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?"
            " AND NOT EXISTS (SELECT 1 FROM packs WHERE repo_id = ? AND id = ?)",
            (const char *[]){ repo, pack, repo, pack, NULL }, NULL) != 0;
    if (failed) {
        db_txn_rollback(idx->db);
        log_error("pack commit: cannot clean up pack %s; a restart does", pack);
        return;
    }
    if (db_txn_commit(idx->db) != OK)
        log_error("pack commit: cleanup of pack %s did not commit; a restart does", pack);
}

/*
 * A pack's rows land in two phases, so the lock is not held across a whole
 * pack while somebody waits (B144). A pack of 2,000 small objects was one
 * transaction of 6,000 statements: 107 ms on an idle host, 716 ms with the
 * CPU contended, and the commonest thing a tag or sources write queued
 * behind during a backup, against Manual 6.3's "well under a second".
 *
 *   - New rows, in batches that end for a queued writer by the shared batch
 *     rules (db_txn_batch_should_end): a pack_objects row for a hash that
 *     has none, and an objects row that is new or still 'staged'. Neither
 *     can be read yet: the resolver joins packs, and this pack has no packs
 *     row. A staged object is not readable either, so moving it changes
 *     nothing a reader could see.
 *   - The visibility marker, last and in one transaction: the packs row,
 *     together with every row the first phase set aside because it would
 *     have replaced a mapping that a reader can use now - a hash already
 *     indexed in another pack (gc, coalescing, salvage, a duplicate), or an
 *     object recorded at another location. Moving one of those before the
 *     marker would make a readable object unfindable until it lands, and a
 *     cut in between would lose the old mapping for good. gc, coalescing
 *     and salvage replace every row they write, so their commit is this one
 *     transaction, as it always was.
 *
 * Batches commit in order and so does the marker after them, so a power cut
 * keeps a prefix: rows without their packs row, or everything. Start-up
 * (store_index_drop_unannounced) and a failure here put the former back to
 * the state a lost single transaction left: the installed pack file and no
 * rows for it. B142's ordering holds, because the marker is this callback's
 * last commit and index_sync runs after it returns.
 */
static void db_pack_commit(void *user, const pack_commit_info_t *info)
{
    store_index_t *idx = user;
    char repo[32], pack[32], size[32], ver[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)idx->repo_id);
    snprintf(pack, sizeof(pack), "%llu", (unsigned long long)info->pack_id);
    snprintf(size, sizeof(size), "%llu", (unsigned long long)info->size);
    snprintf(ver,  sizeof(ver),  "%u",   PACK_VERSION);

    /* Per entry: bit 0, its pack_objects row is for the marker; bit 1, its
     * objects row is. */
    uint8_t *later = calloc(info->entry_count ? info->entry_count : 1, 1);
    if (!later) {
        log_error("pack commit: out of memory for pack %s", pack);
        return;
    }

    db_txn_batch_t batch;
    memset(&batch, 0, sizeof(batch));
    int in_txn = 0, failed = 0, wrote = 0;
    uint32_t rows = 0;
    for (uint32_t i = 0; !failed && i < info->entry_count; i++) {
        if (!in_txn) {
            if (db_txn_begin_bulk(idx->db) != OK) { failed = 1; break; }
            in_txn = 1;
            rows = 0;
            db_txn_batch_opened(&batch);
        }
        entry_args_t a;
        entry_args(&info->entries[i], &a);

        int n = 0;
        if (db_write(idx->db,
                "INSERT INTO pack_objects"
                " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(repo_id, hash) DO NOTHING",
                (const char *[]){ repo, a.hex, pack, a.off, a.comp, a.orig, a.eidx, NULL },
                &n) != 0) { failed = 1; break; }
        if (n == 0) later[i] |= 1;

        /*
         * The object moved from staged to installed, so its location and
         * footprint become the pack's. phys_size is the bytes it occupies
         * inside the pack; the pack's own headers and parity trailer are
         * charged to packs.size rather than split across its entries, so
         * total bytes on disk is sum(packs.size).
         *
         * Created when there is no row yet: a row written after the commit
         * would stay 'staged', be dropped by the next backup, and leave an
         * object compaction reads as dead. An existing row keeps its type
         * and refcount.
         */
        n = 0;
        if (db_write(idx->db,
                "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
                "                     location, type)"
                " VALUES (?, ?, 0, ?, ?, ?, ?)"
                " ON CONFLICT(repo_id, hash) DO UPDATE SET location = excluded.location,"
                "   phys_size = excluded.phys_size, orig_size = excluded.orig_size"
                " WHERE objects.location = 'staged'",
                (const char *[]){ repo, a.hex, a.comp, a.orig, pack, a.type, NULL },
                &n) != 0) { failed = 1; break; }
        if (n == 0) later[i] |= 2;
        /* An entry with both rows set aside wrote nothing, so it does not
         * count towards a batch; a queued writer still ends one. */
        if (later[i] != 3) { wrote = 1; rows++; }

        if (rows >= g_pack_batch_rows || db_txn_batch_should_end(&batch)) {
            if (cut_before_commit()) { db_txn_rollback(idx->db); free(later); return; }
            in_txn = 0;
            if (db_txn_commit(idx->db) != OK) { failed = 1; break; }
        }
    }

    /* The marker, with the rows that replace what readers use now. */
    if (!failed && !in_txn) {
        if (db_txn_begin_bulk(idx->db) != OK) failed = 1;
        else in_txn = 1;
    }
    if (!failed && db_write(idx->db,
            "INSERT OR REPLACE INTO packs (repo_id, id, filename, size, sealed_at, version)"
            " VALUES (?, ?, ?, ?, strftime('%s','now'), ?)",
            (const char *[]){ repo, pack, info->filename, size, ver, NULL }, NULL) != 0)
        failed = 1;
    for (uint32_t i = 0; !failed && i < info->entry_count; i++) {
        if (!later[i]) continue;
        entry_args_t a;
        entry_args(&info->entries[i], &a);
        if ((later[i] & 1) && db_write(idx->db,
                "INSERT OR REPLACE INTO pack_objects"
                " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, a.hex, pack, a.off, a.comp, a.orig, a.eidx, NULL },
                NULL) != 0)
            failed = 1;
        if (!failed && (later[i] & 2) && db_write(idx->db,
                "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
                "                     location, type)"
                " VALUES (?, ?, 0, ?, ?, ?, ?)"
                " ON CONFLICT(repo_id, hash) DO UPDATE SET location = excluded.location,"
                "   phys_size = excluded.phys_size, orig_size = excluded.orig_size",
                (const char *[]){ repo, a.hex, a.comp, a.orig, pack, a.type, NULL },
                NULL) != 0)
            failed = 1;
    }
    free(later);

    if (!failed) {
        if (cut_before_commit()) { db_txn_rollback(idx->db); return; }
        if (db_txn_commit(idx->db) == OK) return;
        in_txn = 0;
        failed = 1;
    }
    if (in_txn) db_txn_rollback(idx->db);
    log_error("pack commit: pack %s (%u objects) did not commit", pack, info->entry_count);
    if (wrote) drop_unannounced(idx, repo, pack);
}

/*
 * A pack was deleted or is about to be replaced. Its rows go with it.
 *
 * Ordering mirrors the commit side: pack_objects first, then the packs row,
 * so nothing ever references a pack row that is already gone.
 */
/* Before a pack is removed: the commits that moved its contents, durable. */
static status_t index_sync(void *user)
{
    store_index_t *idx = user;
    if (!idx) return OK;
    return db_txn_make_durable(idx->db);
}

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
    if (db_write(idx->db,
            "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0) {
        log_error("pack removed: cannot clear pack_objects for pack %s", pack);
        db_txn_rollback(idx->db);
        return;
    }

    if (db_write(idx->db,
            "DELETE FROM packs WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0) {
        log_error("pack removed: cannot delete packs row %s", pack);
        db_txn_rollback(idx->db);
        return;
    }

    if (db_txn_commit(idx->db) != OK)
        log_error("pack removed: commit failed for pack %s", pack);
}

/*
 * gc proved another copy of an object and the indexed one is not that
 * object any more: the row moves to the proven copy, the object's location
 * and size with it, in one transaction (OWN-22).
 */
static status_t pack_relocate(void *user, uint64_t pack_id, const pack_commit_entry_t *e)
{
    store_index_t *idx = user;
    if (!idx || !e) return set_error(ERR_INVALID, "pack_relocate: null argument");
    char repo[32], pack[32], off[32], comp[32], orig[32], eidx[16];
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(e->hash, hex);
    snprintf(repo, sizeof(repo), "%lld", (long long)idx->repo_id);
    snprintf(pack, sizeof(pack), "%llu", (unsigned long long)pack_id);
    snprintf(off,  sizeof(off),  "%llu", (unsigned long long)e->offset);
    snprintf(comp, sizeof(comp), "%llu", (unsigned long long)e->comp_size);
    snprintf(orig, sizeof(orig), "%llu", (unsigned long long)e->orig_size);
    snprintf(eidx, sizeof(eidx), "%u", e->entry_index);

    if (db_txn_begin(idx->db) != OK)
        return set_error(ERR_IO, "relocate %s: cannot begin", hex);
    int failed =
        db_write(idx->db,
            "UPDATE pack_objects SET pack_id = ?, offset = ?, comp_size = ?,"
            " orig_size = ?, entry_index = ? WHERE repo_id = ? AND hash = ?",
            (const char *[]){ pack, off, comp, orig, eidx, repo, hex, NULL }, NULL) != 0 ||
        db_write(idx->db,
            "UPDATE objects SET location = ?, phys_size = ? WHERE repo_id = ? AND hash = ?",
            (const char *[]){ pack, comp, repo, hex, NULL }, NULL) != 0;
    if (failed) {
        db_txn_rollback(idx->db);
        return set_error(ERR_IO, "relocate %s: cannot update its rows", hex);
    }
    if (db_txn_commit(idx->db) != OK)
        return set_error(ERR_IO, "relocate %s: commit", hex);
    return OK;
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
    store_set_pack_relocate_cb(store, pack_relocate, idx);
    store_set_index_sync_cb(store, index_sync, idx);

    /* A floor under the next pack number from everything the index knows
     * was ever issued, a dropped pack's tombstone included (B156). The
     * repository's pack-ids file is what normally decides; this covers a
     * file lost or damaged while the index lived. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT MAX(COALESCE((SELECT MAX(id) FROM packs WHERE repo_id = ?1), -1),"
            "           COALESCE((SELECT MAX(id) FROM pack_tombstones WHERE repo_id = ?1), -1))",
            (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1 && r->rows[0][0][0])
        store_raise_pack_floor(store, strtoll(r->rows[0][0], NULL, 10) + 1);
    db_result_free(r);
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
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(hash, hex);
    char repo[32], t[8];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(t, sizeof(t), "%u", type);
    if (db_write(db,
            "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
            "                     location, type)"
            " SELECT repo_id, hash, 0, comp_size, orig_size, CAST(pack_id AS TEXT), ?"
            " FROM pack_objects WHERE repo_id = ? AND hash = ?"
            " ON CONFLICT(repo_id, hash) DO NOTHING",
            (const char *[]){ t, repo, hex, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot record object");
    return OK;
}

status_t store_index_put_staged(cutils_db_t *db, int64_t repo_id,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                uint8_t type, uint64_t phys_size, uint64_t orig_size)
{
    return put_object(db, repo_id, hash, type, phys_size, orig_size, "staged");
}

/*
 * Rows of a pack commit that never reached its packs row, from a crash
 * between its batches: put back to what a lost single transaction left
 * (db_pack_commit). Only packs with such pack_objects rows are touched, so
 * an objects row naming a pack for any other reason is left to reindex as
 * before. Runs before store_index_drop_staged, which then drops the rows
 * this returns to 'staged'.
 *
 * The set of unannounced packs is one uncorrelated subquery, matched as a
 * (repo, location) pair (B153). It used to correlate on objects.repo_id,
 * which made SQLite rebuild the DISTINCT b-tree over the repository's whole
 * pack_objects for every objects row: sum(n^2) per repository, 4.5 minutes
 * of start-up at 56k objects with nothing to change, and hours beyond that.
 * Now pack_objects is read once and an objects row is only visited when its
 * repository has an unannounced pack. location holds a pack number as the
 * decimal text db_pack_commit writes, which is what CAST(... AS TEXT) gives;
 * every column in the pair is NOT NULL, so IN cannot meet a NULL.
 * tests/test_startup_plans.c holds the plan to that shape.
 */
status_t store_index_drop_unannounced(cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    const char *only = repo_id > 0 ? " AND po.repo_id = ?" : "";
    char upd[512], del[256];
    snprintf(upd, sizeof(upd),
             "UPDATE objects SET location = 'staged' WHERE (repo_id, location) IN"
             " (SELECT DISTINCT po.repo_id, CAST(po.pack_id AS TEXT) FROM pack_objects po"
             "   WHERE NOT EXISTS"
             "   (SELECT 1 FROM packs p WHERE p.repo_id = po.repo_id AND p.id = po.pack_id)%s)",
             only);
    snprintf(del, sizeof(del),
             "DELETE FROM pack_objects AS po WHERE NOT EXISTS"
             " (SELECT 1 FROM packs p WHERE p.repo_id = po.repo_id AND p.id = po.pack_id)%s",
             only);
    const char *p1[] = { repo, NULL };
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "cannot drop unannounced pack rows");
    if (db_write(db, upd, repo_id > 0 ? p1 : NULL, NULL) != 0 ||
        db_write(db, del, repo_id > 0 ? p1 : NULL, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "cannot drop unannounced pack rows");
    }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "cannot drop unannounced pack rows");
    return OK;
}

/*
 * A packs row whose file is gone and which nothing locates through (B147).
 * Removing a pack unlinks it, syncs the directory, and only then commits
 * the row's removal - the unlink waits for the commits that moved the
 * pack's contents (B142), not the other way round - so a power cut can keep
 * the unlink and lose that last commit. The row names a pack that is not
 * there and nothing but a reindex ever removed it.
 *
 * Only rows with no pack_objects row: one that still locates objects in a
 * missing pack is a loss for verify and reindex to report, not garbage.
 * And only when the repository's packs/ is there, so an offline mount
 * loses nothing.
 */
status_t store_index_drop_vanished_packs(cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT p.repo_id, p.id, r.path FROM packs p JOIN repos r ON r.id = p.repo_id"
             " WHERE NOT EXISTS (SELECT 1 FROM pack_objects po"
             "   WHERE po.repo_id = p.repo_id AND po.pack_id = p.id)%s",
             repo_id > 0 ? " AND p.repo_id = ?" : "");
    const char *p1[] = { repo, NULL };
    db_result_t *r = NULL;
    if (db_execute(db, sql, repo_id > 0 ? p1 : NULL, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "cannot list packs with no entries");
    }
    status_t st = OK;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        const char *path = r->rows[i][2];
        char packs[PATH_MAX], dat[PATH_MAX];
        struct stat sb;
        uint32_t num = (uint32_t)strtoul(r->rows[i][1], NULL, 10);
        if (snprintf(packs, sizeof(packs), "%s/packs", path) >= (int)sizeof(packs) ||
            stat(packs, &sb) != 0 || !S_ISDIR(sb.st_mode))
            continue;
        if (pack_dat_path_resolve(dat, sizeof(dat), path, num) != 0) continue;
        if (stat(dat, &sb) == 0 || errno != ENOENT) continue;
        if (db_write(db, "DELETE FROM packs WHERE repo_id = ? AND id = ?",
                     (const char *[]){ r->rows[i][0], r->rows[i][1], NULL },
                     NULL) != 0)
            st = set_error(ERR_IO, "cannot drop the row of vanished pack %u", num);
        else
            log_info("index: dropped the row of pack %u of repo %s, which is gone",
                     num, r->rows[i][0]);
    }
    db_result_free(r);
    return st;
}

/*
 * Objects marked absent that a pack holds (B149). reindex records every
 * object a tree names as 'absent' while it walks, and sets the location
 * from pack_objects in a later, separate commit: two short transactions,
 * as the shared handle needs. A crash or power cut between them leaves the
 * snapshots indexed - so nothing asks for another reindex - and their
 * objects marked as held by no pack, which verify reports missing.
 */
status_t store_index_fix_absent(cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    char sql[1024];
    snprintf(sql, sizeof(sql),
             "UPDATE objects SET"
             "   phys_size = (SELECT po.comp_size FROM pack_objects po"
             "                 WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash),"
             "   orig_size = (SELECT po.orig_size FROM pack_objects po"
             "                 WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash),"
             "   location  = (SELECT CAST(po.pack_id AS TEXT) FROM pack_objects po"
             "                 WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash)"
             " WHERE location = 'absent'%s"
             "   AND EXISTS (SELECT 1 FROM pack_objects po"
             "                WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash)",
             repo_id > 0 ? " AND repo_id = ?" : "");
    const char *p1[] = { repo, NULL };
    if (db_write(db, sql, repo_id > 0 ? p1 : NULL, NULL) != 0)
        return set_error(ERR_IO, "cannot locate objects marked absent");
    return OK;
}

/*
 * One statement under the write lock (B157). backup_run calls this while
 * other threads run, and as a plain statement it joined whatever db_txn
 * transaction another thread had open: it reported success and was undone
 * by that thread's rollback, leaving rows that name bytes in a discarded
 * pack. It stays one statement in one transaction rather than batches:
 * there is no index on location, so each batch would rescan the
 * repository's objects, and the connection's own mutex already holds every
 * other thread off for the length of one statement either way.
 */
status_t store_index_drop_staged(cutils_db_t *db, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int rc = repo_id > 0
        ? db_write(db,
              "DELETE FROM objects WHERE repo_id = ? AND location = 'staged'",
              (const char *[]){ repo, NULL }, NULL)
        : db_write(db,
              "DELETE FROM objects WHERE location = 'staged'", NULL, NULL);
    return rc == 0 ? OK : set_error(ERR_IO, "cannot drop staged objects");
}

static status_t put_object(cutils_db_t *db, int64_t repo_id,
                           const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                           uint64_t phys_size, uint64_t orig_size,
                           const char *location)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(hash, hex);
    char repo[32], phys[32], orig[32], t[8];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(phys, sizeof(phys), "%llu", (unsigned long long)phys_size);
    snprintf(orig, sizeof(orig), "%llu", (unsigned long long)orig_size);
    snprintf(t,    sizeof(t),    "%u",   type);

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
    if (db_write(db,
            "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size,"
            "                     location, type)"
            " VALUES (?, ?, 0, ?, ?, ?, ?)"
            " ON CONFLICT(repo_id, hash) DO UPDATE SET phys_size = excluded.phys_size"
            " WHERE objects.phys_size = 0 AND excluded.phys_size > 0"
            "   AND objects.location = excluded.location",
            (const char *[]){ repo, hex, phys, orig, location, t, NULL }, NULL) != 0)
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

    if (db_write(db,
            "UPDATE objects SET refcount = refcount + 1"
            " WHERE repo_id = ? AND hash = ?",
            (const char *[]){ repo, hex, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot addref object");
    return OK;
}
