#pragma once

/*
 * Per-path change detection.
 *
 * The backup pipeline is content-addressed, so it cannot know a file is
 * unchanged without reading it: the hash is the only identity it has. That
 * makes an incremental over a mostly-unchanged tree cost a full read and a
 * full write of everything, to store almost nothing.
 *
 * This is the shortcut every backup tool ends up with. Remember what a path
 * looked like last time, and if the filesystem metadata is byte for byte the
 * same, reuse the hash without opening the file.
 *
 * It is a cache, not an index: losing it makes the next backup slow, never
 * wrong. The asymmetry that matters is that a false miss costs one re-read
 * while a false hit records the wrong content under a path, so the match is
 * deliberately strict and every doubt resolves towards re-reading.
 */

#include "error.h"
#include "types.h"

#include <cutils/db.h>
#include <stdint.h>
#include <sys/stat.h>

/*
 * Fields a lookup may disregard. Size and mtime are always compared.
 *
 * Device and inode numbers are only stable on local filesystems. An Unraid
 * user share (shfs, a FUSE filesystem) hands out inode numbers that change
 * between lookups, an NFS or SMB mount of it inherits that, and a container
 * volume can come back on another device number every time it is mounted.
 * Comparing them there turned every incremental into a full re-read of the
 * tree while reporting nothing wrong (B087).
 */
#define FILE_CACHE_IGNORE_DEV    0x1u
#define FILE_CACHE_IGNORE_INODE  0x2u
#define FILE_CACHE_IGNORE_CTIME  0x4u

/*
 * How strictly the cache matches, from `backup.file_cache_match`:
 *   strict  - every field, on every filesystem;
 *   auto    - every field on a local filesystem, everything but device and
 *             inode on a network or FUSE one (the default);
 *   relaxed - path, size and mtime only, everywhere.
 */
typedef enum {
    FILE_CACHE_MATCH_AUTO = 0,
    FILE_CACHE_MATCH_STRICT,
    FILE_CACHE_MATCH_RELAXED,
} file_cache_match_t;

/* Parse a `backup.file_cache_match` value; -1 for anything unknown. */
int file_cache_match_parse(const char *s);

/* Whether a filesystem magic (statfs f_type) has unstable dev/inode numbers:
 * NFS, SMB/CIFS, FUSE (shfs, mergerfs, sshfs), 9p, Ceph and the like. */
int file_cache_fs_is_remote(long f_type);

/*
 * Does this path still hold the bytes we recorded for it?
 *
 * Returns 1 and fills out_hash when the cached entry matches the stat (less
 * the fields `ignore` names) and the object is still present in the
 * repository, 0 otherwise. The object check is not optional: GC removes
 * objects that no live snapshot references, and reusing a hash whose object
 * has gone would write a snapshot pointing at nothing.
 *
 * A miss on an existing entry is logged at debug level with the fields that
 * differed, so a cache that never hits says why.
 */
int file_cache_lookup(cutils_db_t *db, int64_t repo_id, const char *path,
                      const struct stat *st, unsigned ignore,
                      uint8_t out_hash[OBJECT_HASH_SIZE]);

/* Record what a path holds now. Errors are logged, not propagated: a cache
 * that fails to update costs speed, and failing the backup over it would
 * trade a real guarantee for an optimisation. */
void file_cache_put(cutils_db_t *db, int64_t repo_id, const char *path,
                    const struct stat *st,
                    const uint8_t hash[OBJECT_HASH_SIZE], int64_t seen_at);

/*
 * Drop entries for paths the scan no longer sees.
 *
 * Takes the scanned paths rather than a timestamp deliberately. Stamping each
 * row as it is used would mean a database write for every unchanged file, and
 * the whole point of a hit is that it costs nothing: measured at 36ms a row,
 * restamping cost more than the hashing it was avoiding. Diffing against the
 * scan instead makes the cost proportional to what was deleted, which is
 * usually nothing.
 *
 * `paths` must be sorted by strcmp.
 */
/*
 * Drop the rows a run wrote (seen_at = its stamp). For a run that must not be
 * trusted after the fact: one whose files referenced chunks another writer
 * was storing and never committed (B184). Their rows would let the next run
 * skip reading those files and reuse content naming the lost chunks. Best
 * effort, logged on failure.
 */
void file_cache_forget_run(cutils_db_t *db, int64_t repo_id, int64_t seen_at);

/*
 * Provisional rows (B293). A run whose writers share chunk claims writes
 * its rows with seen_at = -stamp, which file_cache_lookup never trusts, and
 * confirms them once every writer of the run has committed. A file's
 * content can be a chunk list in one writer's pack naming a chunk another
 * writer is still storing; that pack can commit first, and a power cut
 * there kept the list's row and the file's cache row and lost the chunk.
 * The next run reused the list unread, and with the chunk's own file gone
 * by then nothing stored it again: a snapshot naming bytes the repository
 * did not hold. A crashed run's provisional rows are never confirmed, so
 * its files are read again; the next run's rows replace them. forget_run
 * drops both kinds. Best effort, logged on failure.
 */
void file_cache_confirm_run(cutils_db_t *db, int64_t repo_id, int64_t stamp);

/*
 * The stat the file cache keys an entry on: its own, except that a snapshot
 * read's entry carries the record root's live device in place of the
 * snapshot's (scan.h, rec_dev). The rows a snapshot read writes then match
 * a live read of the same files and the reverse, and the device is still
 * compared, so a row from another filesystem is never trusted (B229). buf
 * holds the copy when one is needed.
 */
struct scan_entry;
const struct stat *file_cache_key_stat(const struct scan_entry *e, struct stat *buf);

void file_cache_sweep(cutils_db_t *db, int64_t repo_id,
                      const char *const *paths, size_t n_paths);
