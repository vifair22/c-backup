#define _POSIX_C_SOURCE 200809L

#include "prune.h"

#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

status_t prune_snapshot(store_t *store, cutils_db_t *db, int64_t repo_id,
                        int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    /*
     * A marker first, so an interrupted prune is recognisable on restart.
     * Without it a crash between the index delete and the record unlink
     * leaves a snapshot record with no rows, which looks exactly like a
     * snapshot that simply needs reindexing and would be silently resurrected.
     */
    char marker[64];
    snprintf(marker, sizeof(marker), "prune-pending-%lld", (long long)snap_id);
    if (db_execute_non_query(db,
            "INSERT OR REPLACE INTO recovery_state (repo_id, key, value)"
            " VALUES (?, ?, ?)",
            (const char *[]){ repo, marker, snap, NULL }, NULL) != 0)
        return set_error(ERR_IO, "prune: cannot record intent");

    if (db_exec_raw(db, "BEGIN") != 0)
        return set_error(ERR_IO, "prune: begin");

    int failed = db_execute_non_query(db,
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

    if (failed) {
        if (db_exec_raw(db, "ROLLBACK") != 0) log_error("prune: rollback failed");
        return set_error(ERR_IO, "prune: cannot remove snapshot %lld rows",
                         (long long)snap_id);
    }
    if (db_exec_raw(db, "COMMIT") != 0)
        return set_error(ERR_IO, "prune: commit");

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
