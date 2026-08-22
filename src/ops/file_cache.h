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
 * Does this path still hold the bytes we recorded for it?
 *
 * Returns 1 and fills out_hash when the cached entry matches the stat and the
 * object is still present in the repository, 0 otherwise. The object check is
 * not optional: GC removes objects that no live snapshot references, and
 * reusing a hash whose object has gone would write a snapshot pointing at
 * nothing.
 */
int file_cache_lookup(cutils_db_t *db, int64_t repo_id, const char *path,
                      const struct stat *st, uint8_t out_hash[OBJECT_HASH_SIZE]);

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
void file_cache_sweep(cutils_db_t *db, int64_t repo_id,
                      const char *const *paths, size_t n_paths);
