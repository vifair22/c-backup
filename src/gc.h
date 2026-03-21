#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/*
 * Remove objects from the repo that are not referenced by any snapshot.
 * *out_deleted (may be NULL) receives the count of deleted objects.
 * *out_kept   (may be NULL) receives the count of retained objects.
 */
status_t repo_gc(repo_t *repo, uint32_t *out_kept, uint32_t *out_deleted);

/*
 * Delete snapshot files older than the most-recent keep_count snapshots.
 * Reverse records are preserved.  Runs GC afterwards to purge unreferenced
 * objects.  *out_pruned (may be NULL) receives the number of .snap files
 * removed.  keep_count must be >= 1.
 * If dry_run is non-zero, print what would be removed but make no changes.
 */
status_t repo_prune(repo_t *repo, uint32_t keep_count, uint32_t *out_pruned,
                    int dry_run);

/*
 * Verify every object referenced by every surviving snapshot: loads and
 * decompresses each object, re-hashing its content to confirm integrity.
 * Returns OK if all objects are present and uncorrupted, ERR_CORRUPT
 * if any are missing or have hash mismatches.
 */
status_t repo_verify(repo_t *repo);

/*
 * Complete any prune that was interrupted before GC could run.
 * Reads repo/prune-pending (if it exists), deletes any listed snap files
 * that have not already been removed, runs GC, then removes the file.
 * Called automatically on every exclusive lock acquisition.
 */
status_t repo_prune_resume_pending(repo_t *repo);

