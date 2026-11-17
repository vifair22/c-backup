#define _POSIX_C_SOURCE 200809L

#include "prune.h"
#include "sync_latch.h"
#include "db_txn.h"
#include "repo.h"
#include "repo_tags.h"
#include "snap_record.h"
#include "db_write.h"

#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/*
 * The record of snapshot snap_id under repo_path, gone for good: unlinked
 * and its directory synced. Without the sync a power cut can bring the
 * record back after its rows went (B148), which is the snapshot's
 * resurrection by the next reindex, after gc swept what only it used.
 * OK when there was no record.
 */
/* removed, when not NULL, says whether there was a record to unlink. */
static status_t unlink_record(const char *repo_path, int64_t snap_id, int *removed)
{
    if (removed) *removed = 0;
    char dir[PATH_MAX], path[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s/snapshots", repo_path) >= (int)sizeof(dir) ||
        snprintf(path, sizeof(path), "%s/%lld", dir, (long long)snap_id) >= (int)sizeof(path))
        return set_error(ERR_INVALID, "prune: path too long");
    if (unlink(path) == 0) {
        if (removed) *removed = 1;
    } else if (errno != ENOENT) {
        return set_error_errno(ERR_IO, "prune: cannot unlink %s", path);
    }
    if (snapshot_fsync_dir(repo_path, dir) != 0)
        return set_error_errno(ERR_IO, "prune: cannot sync %s", dir);
    return OK;
}

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
    /* A prune deletes on the strength of a durable commit, which a latched
     * daemon cannot make (sync_latch.h). */
    status_t lst = sync_latch_repo_check(store_root(store), "prune");
    if (lst != OK) return lst;
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    if (info) memset(info, 0, sizeof(*info));

    if (db_txn_begin_bulk(db) != OK)
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
    int failed = db_write(db,
            "INSERT OR REPLACE INTO recovery_state (repo_id, key, value)"
            " VALUES (?, ?, ?)",
            (const char *[]){ repo, marker, snap, NULL }, NULL) != 0;

    if (!failed)
        failed = db_write(db,
            "DELETE FROM nodes WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_write(db,
            "DELETE FROM node_fts WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_write(db,
            "DELETE FROM snapshots WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_write(db,
            "DELETE FROM tags WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_write(db,
            "DELETE FROM notes WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_write(db,
            "DELETE FROM snapshot_unit_items WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;
    if (!failed)
        failed = db_write(db,
            "DELETE FROM snapshot_units WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, NULL) != 0;

    if (failed) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "prune: cannot remove snapshot %lld rows",
                         (long long)snap_id);
    }

    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "prune: commit");

    /*
     * The repository's copy of the tags (repo_tags.h) loses them too, after
     * the commit and outside the write lock: its two syncs inside the
     * transaction held up every other writer (B144). This direction is safe
     * where a tag edit's is not: a tag left in the file for a snapshot that
     * is gone is never restored, since reindex matches id and timestamp and
     * snapshot ids are never reused. So a failure here is a warning.
     */
    if (ntags > 0) {
        repo_tags_lock();
        if (repo_tags_save(db, repo_id, store_root(store)) != OK) {
            log_warn("prune: snapshot %lld's tags stay in the repository's tag file, "
                     "where they match nothing: %s", (long long)snap_id, err_msg());
            err_clear();
        }
        repo_tags_unlock();
    }

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

    /* The marker goes only once the record is gone for good: until then it
     * is what lets the next start, or a reindex, finish this prune rather
     * than mistake the record for a snapshot the index lost (B148). */
    if (unlink_record(store_root(store), snap_id, NULL) != OK) {
        log_warn("prune: snapshot %lld removed from the index; its record stays until "
                 "the next start finishes the removal: %s", (long long)snap_id, err_msg());
        err_clear();
        return OK;
    }

    if (db_write(db,
            "DELETE FROM recovery_state WHERE repo_id = ? AND key = ?",
            (const char *[]){ repo, marker, NULL }, NULL) != 0)
        log_warn("prune: cannot clear intent marker for snapshot %lld",
                 (long long)snap_id);

    log_info("prune: removed snapshot %lld", (long long)snap_id);
    return OK;
}

status_t prune_finish_pending(cutils_db_t *db, int64_t repo_id, unsigned *out_finished)
{
    if (out_finished) *out_finished = 0;
    status_t lst = sync_latch_check("prune");
    if (lst != OK) return lst;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    const char *p1[] = { repo, NULL };
    db_result_t *r = NULL;
    if (db_execute(db, repo_id > 0
            ? "SELECT m.repo_id, m.key, m.value, r.path, r.uuid FROM recovery_state m"
              " JOIN repos r ON r.id = m.repo_id"
              " WHERE m.key LIKE 'prune-pending-%' AND m.repo_id = ?"
            : "SELECT m.repo_id, m.key, m.value, r.path, r.uuid FROM recovery_state m"
              " JOIN repos r ON r.id = m.repo_id WHERE m.key LIKE 'prune-pending-%'",
            repo_id > 0 ? p1 : NULL, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "prune: cannot list interrupted prunes");
    }
    status_t st = OK;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        const char *rid = r->rows[i][0], *key = r->rows[i][1], *snap = r->rows[i][2];
        const char *path = r->rows[i][3];
        int64_t snap_id = strtoll(snap, NULL, 10);
        char dir[PATH_MAX];
        struct stat sb;
        /* An offline repository keeps its marker for the next start, and
         * so does one whose syncs are latched (sync_latch.h). */
        if (sync_latch_repo_tripped(path)) continue;
        if (snprintf(dir, sizeof(dir), "%s/snapshots", path) >= (int)sizeof(dir) ||
            stat(dir, &sb) != 0 || !S_ISDIR(sb.st_mode))
            continue;
        /* So does one whose path now holds another repository (B164):
         * finishing the prune there would delete that one's record of the
         * same number. */
        if (repo_identity_check(path, r->rows[i][4]) != OK) {
            log_warn("prune: snapshot %s of repo %s not finished: %s", snap, rid, err_msg());
            err_clear();
            continue;
        }

        /* The marker was committed with the deletion of the rows. Rows
         * there now were put back by something that did not know (a
         * reindex from before markers were honoured): that snapshot is
         * listed, so it stays, and only the marker goes. */
        db_result_t *q = NULL;
        int listed = db_execute(db, "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
                                (const char *[]){ rid, snap, NULL }, &q) == 0 &&
                     q && q->nrows > 0;
        db_result_free(q);
        /*
         * No record left means the prune had finished and only the
         * marker's removal was lost: it is not durable until the WAL
         * syncs, and a power cut within that window rolls it back
         * (NIT-35). Nothing was interrupted, so nothing is reported.
         */
        int removed = 0;
        if (listed) {
            log_warn("prune: snapshot %s of repo %s was pruned and is listed again; "
                     "keeping it", snap, rid);
        } else {
            st = unlink_record(path, snap_id, &removed);
            if (st != OK) break;
        }
        if (db_write(db, "DELETE FROM recovery_state WHERE repo_id = ? AND key = ?",
                     (const char *[]){ rid, key, NULL }, NULL) != 0) {
            st = set_error(ERR_IO, "prune: cannot clear the marker of snapshot %s", snap);
            break;
        }
        if (!listed && removed) {
            log_warn("prune: finished the removal of snapshot %s of repo %s, which a crash "
                     "or power cut interrupted", snap, rid);
            if (out_finished) (*out_finished)++;
        } else if (!listed) {
            log_debug("prune: cleared the stale marker of snapshot %s of repo %s, whose "
                      "removal had completed", snap, rid);
        }
    }
    db_result_free(r);
    return st;
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
