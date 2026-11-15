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
 * Rewrite <repo_path>/tags from the database's tags for repo_id. Meant to be
 * called inside the transaction that changed them, so the file and the rows
 * cannot disagree about a change the caller was told succeeded.
 */
status_t repo_tags_save(cutils_db_t *db, int64_t repo_id, const char *repo_path);

/*
 * Insert the file's tags into the database, for snapshots whose id and
 * timestamp both match. A tag already in the database wins: it is the newer
 * of the two. A missing file is not an error. *out_restored counts the tags
 * inserted, *out_skipped those whose snapshot is gone.
 */
status_t repo_tags_restore(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                           uint64_t *out_restored, uint64_t *out_skipped);
