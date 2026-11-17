#pragma once

/*
 * Tags, kept in the repository as well as in the database.
 *
 * A tag is the retention guard: gfs keeps a tagged snapshot whatever its
 * tier. It lived only in state.db, so the disaster-recovery path - lose the
 * database, register the repository, reindex - came back with every
 * snapshot unprotected, and the next retention pass was free to delete the
 * ones somebody had tagged precisely so that it would not.
 *
 * So the repository carries a copy: <repo>/tags, rewritten whole and
 * atomically (temp file, fsync, rename) every time a tag changes, and read
 * back by reindex. The database stays the copy every query uses; the file
 * exists to survive losing it.
 *
 *   {"version":1,"tags":[{"name":"keep","snapshot":3,"ts":1731600000}]}
 *
 * ts is the tagged snapshot's timestamp. Snapshot ids are MAX(id)+1, so
 * pruning the newest snapshot frees its id for the next backup; a tag in the
 * file is restored only onto the snapshot with the same id and the same
 * timestamp, never onto whatever reuses the number.
 *
 * A repository written before this file existed simply has none, and
 * reindex restores no tags from it, which is what it did before.
 */

#include "error.h"

#include <cutils/db.h>
#include <stdint.h>

#define REPO_TAGS_FILE "tags"

/*
 * Rewrite <repo_path>/tags from the database's tags for repo_id, and sync
 * the file and the repository directory.
 *
 * Never call this inside a db_txn transaction: the two syncs would hold the
 * one write lock every other writer, a running backup's batches included,
 * queues on. On a busy pool that was seconds per tag edit, and every writer
 * waited out each one (B144). The repository's file is the copy that
 * survives losing the database, so it goes first and the commit after,
 * the safe direction for anything reconcile can rebuild from the
 * repository: repo_tags_save_with writes the file as it will be once the
 * change commits, and the caller commits afterwards.
 *
 * Every caller holds repo_tags_lock from reading the rows through its own
 * commit (if it has one), so two writers cannot each write a file that
 * misses the other's change. Lock order: repo_tags_lock, then db_txn.
 */
void repo_tags_lock(void);
void repo_tags_unlock(void);

status_t repo_tags_save(cutils_db_t *db, int64_t repo_id, const char *repo_path);

/*
 * One tag change not yet committed: name set on snap_id (a decimal string),
 * or name removed. created_at applies only to a name that is new; an
 * existing tag keeps its own, as the upsert does.
 */
typedef struct {
    const char *name;
    const char *snap_id;
    int64_t     created_at;
    int         remove;
} repo_tag_change_t;

/* repo_tags_save, with chg applied over the database's rows. NULL chg is
 * plain repo_tags_save. */
status_t repo_tags_save_with(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                             const repo_tag_change_t *chg);

/*
 * Insert the file's tags into the database, for snapshots whose id and
 * timestamp both match. A tag already in the database wins: it is the newer
 * of the two. A missing file is not an error. *out_restored counts the tags
 * inserted, *out_skipped those whose snapshot is gone.
 */
status_t repo_tags_restore(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                           uint64_t *out_restored, uint64_t *out_skipped);
