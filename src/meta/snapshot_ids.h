#pragma once

/*
 * The repository's snapshot-id high-water mark: <repo>/snapshot-ids.
 *
 * Snapshot ids are never reissued. While the daemon's database lives it
 * remembers the highest id handed out (snapshot_id_seq), but a database is a
 * derived index and can be lost; after that, a reindex used to start above
 * the highest record still present, so an id pruned from the top came back
 * and anything outside the repository still naming it - a tag, a note, a
 * client, a replica - silently moved to a different tree. The mark lives in
 * the repository so that cannot happen.
 *
 * One line of JSON, written atomically (temp file in tmp/, fsync, rename,
 * fsync of the repository directory):
 *
 *   {"format":"cbackup-snapshot-ids","version":1,"uuid":"...",
 *    "last_issued":4127,"crc32c":"5e1f0c2a"}
 *
 * last_issued is a non-negative int64, 0 meaning none issued, and never
 * decreases. crc32c is lowercase hex over the ASCII bytes
 * "<uuid>:<last_issued>", so a damaged file is detected and one copied from
 * another repository (different uuid) is refused. The file is created at
 * repository creation, so a missing one on a format 3 repository is damage.
 *
 * Rules (docs/repo-format.md, "snapshot-ids"):
 *   1. A backup raises the mark to the id it is about to use, durably,
 *      before writing that snapshot's record.
 *   2. Reindex raises the index's mark to max(file, highest record) and
 *      rewrites a missing, damaged or lagging file.
 *   3. Replication raises the target's mark to each snapshot's id before
 *      writing its record, and to the source's mark at the end of the run.
 *   4. Prune and delete never touch it.
 */

#include "error.h"

#include <stdint.h>

#define SNAPSHOT_IDS_FILE    "snapshot-ids"
#define SNAPSHOT_IDS_FORMAT  "cbackup-snapshot-ids"
#define SNAPSHOT_IDS_VERSION 1

/*
 * Read the mark. ERR_NOT_FOUND when there is no file; ERR_CORRUPT when it
 * does not parse, fails its CRC, or names another repository (uuid). The
 * message says which, for a caller that reports it.
 */
status_t snapshot_ids_read(const char *repo_root, const char *uuid, int64_t *out_last);

/* Replace the mark with `last`, atomically and durably. Does not compare
 * against the current value; snapshot_ids_raise is what callers want. */
status_t snapshot_ids_write(const char *repo_root, const char *uuid, int64_t last);

/*
 * Make the mark at least `at_least`. Reads the current value; a missing or
 * damaged file counts as 0 and is rewritten (with a warning naming why).
 * Writes only when the value moves, or the file needed repair. *out_now
 * (may be NULL) gets the mark afterwards; *out_repaired (may be NULL) is set
 * when the file was missing or damaged.
 */
status_t snapshot_ids_raise(const char *repo_root, const char *uuid,
                            int64_t at_least, int64_t *out_now, int *out_repaired);

/* The same, for a repository identified only by its root: the uuid is read
 * from the marker. ERR_NOT_FOUND when there is no marker. */
status_t snapshot_ids_raise_repo(const char *repo_root, int64_t at_least,
                                 int64_t *out_now, int *out_repaired);
status_t snapshot_ids_read_repo(const char *repo_root, int64_t *out_last);
