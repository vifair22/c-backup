#define _POSIX_C_SOURCE 200809L

#include "prune.h"
#include "db_txn.h"
#include "repo_tags.h"

#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The snapshot's tag names, comma separated, into out. Returns how many. */
static unsigned collect_tags(cutils_db_t *db, const char *repo, const char *snap,
                             prune_info_t *out)
{
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT name FROM tags WHERE repo_id = ? AND snap_id = ? ORDER BY name",
            (const char *[]){ repo, snap, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return 0;
    }
    unsigned n = (unsigned)r->nrows;
    if (out) {
        size_t len = 0;
        out->tags[0] = '\0';
        for (int i = 0; i < r->nrows; i++) {
            int w = snprintf(out->tags + len, sizeof(out->tags) - len, "%s%s",
                             i ? ", " : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= sizeof(out->tags) - len) {
                /* Mark the cut rather than end mid-name with no sign of it. */
                size_t cut = sizeof(out->tags) - 4;
                memcpy(out->tags + cut, "...", 4);
                break;
            }
            len += (size_t)w;
        }
        out->ntags = n;
    }
    db_result_free(r);
    return n;
}

static status_t prune_core(store_t *store, cutils_db_t *db, int64_t repo_id,
                           int64_t snap_id, int checked, int force,
                           prune_info_t *info)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    if (info) memset(info, 0, sizeof(*info));

    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "prune: begin");

    /*
     * The checks share the deleting transaction, so a tag set between the
     * check and the delete cannot be deleted unseen with its snapshot.
     */
    if (checked) {
        db_result_t *r = NULL;
        int exists = db_execute(db,
                "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
                (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
        db_result_free(r);
        if (!exists) {
            db_txn_rollback(db);
            return set_error(ERR_NOT_FOUND, "no such snapshot: %lld", (long long)snap_id);
        }
    }
    /* An open NBD export pins its snapshot (vm-backup.md 7.3): it is
     * serving those bytes, and force does not override it. */
    {
        db_result_t *r = NULL;
        int pinned = db_execute(db,
                "SELECT id FROM exports WHERE repo_id = ? AND snap_id = ? AND closed_at IS NULL",
                (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows > 0;
        char eid[32] = "";
        if (pinned) snprintf(eid, sizeof(eid), "%s", r->rows[0][0]);
        db_result_free(r);
        if (pinned) {
            db_txn_rollback(db);
            return set_error(ERR_BUSY, "snapshot %lld is exported (export %s); revoke the "
                             "export first (DELETE /exports/%s)", (long long)snap_id, eid, eid);
        }
    }
    unsigned ntags = collect_tags(db, repo, snap, info);
    if (checked && ntags > 0 && !force) {
        db_txn_rollback(db);
        return set_error(ERR_INVALID,
                         "snapshot %lld is tagged (%s); a tag protects a snapshot "
                         "from deletion. Remove the tag%s first, or delete with "
                         "--force (force=1) to remove the snapshot and its tag%s",
                         (long long)snap_id, info ? info->tags : "",
                         ntags == 1 ? "" : "s", ntags == 1 ? "" : "s");
    }

    /*
     * A marker, so an interrupted prune is recognisable on restart. Without
     * it a crash between the index delete and the record unlink leaves a
     * snapshot record with no rows, which looks exactly like a snapshot that
     * simply needs reindexing and would be silently resurrected. Written in
     * the deleting transaction so a refused or failed prune leaves none.
     */
    char marker[64];
    snprintf(marker, sizeof(marker), "prune-pending-%lld", (long long)snap_id);
    int failed = db_execute_non_query(db,
            "INSERT OR REPLACE INTO recovery_state (repo_id, key, value)"
            " VALUES (?, ?, ?)",
            (const char *[]){ repo, marker, snap, NULL }, NULL) != 0;

    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM nodes WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM node_fts WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM snapshots WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM tags WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM notes WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM snapshot_unit_items WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_execute_non_query(db,
            "DELETE FROM snapshot_units WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;

    if (failed) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "prune: cannot remove snapshot %lld rows",
                         (long long)snap_id);
    }

    /* The repository's copy of the tags (repo_tags.h) loses them too, in the
     * same transaction, as every other tag change does. Left behind, a
     * reindex would skip them only because the timestamp no longer matches. */
    if (ntags > 0 && repo_tags_save(db, repo_id, store_root(store)) != OK) {
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        db_txn_rollback(db);
        return set_error(ERR_IO, "prune: cannot record tags in the repository: %s", why);
    }

    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "prune: commit");

    /* The record file is the snapshot's authoritative copy; it goes only
     * once the commit removing the snapshot's rows is durable, or a power
     * cut could bring back rows for a snapshot whose record is gone (B142).
     * A record left behind is what reconcile and reindex expect. */
    if (db_txn_make_durable(db) != OK) {
        log_warn("prune: snapshot %lld removed from the index; its record stays until "
                 "the index is durable: %s", (long long)snap_id, err_msg());
        err_clear();
        return OK;
    }

    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/snapshots/%lld",
                 store_root(store), (long long)snap_id) < (int)sizeof(path)) {
        if (unlink(path) != 0 && errno != ENOENT)
            log_warn("prune: cannot unlink %s: %s", path, strerror(errno));
    }

    if (db_execute_non_query(db,
            "DELETE FROM recovery_state WHERE repo_id = ? AND key = ?",
            (const char *[]){ repo, marker, NULL }, NULL) != 0)
        log_warn("prune: cannot clear intent marker for snapshot %lld",
                 (long long)snap_id);

    log_info("prune: removed snapshot %lld", (long long)snap_id);
    return OK;
}

status_t prune_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                        int64_t snap_id)
{
    return prune_core(store, db, repo_id, snap_id, 0, 1, NULL);
}

status_t prune_snapshot_checked(store_t *store, cutils_db_t *db, int64_t repo_id,
                                int64_t snap_id, int force, prune_info_t *out)
{
    return prune_core(store, db, repo_id, snap_id, 1, force, out);
}
