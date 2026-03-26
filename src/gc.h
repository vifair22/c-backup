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

typedef struct {
    uint64_t objects_checked;
    uint64_t parity_repaired;
    uint64_t parity_corrupt;
    uint64_t bytes_checked;
    int      repair;        /* if true, rewrite corrected files to disk */
} verify_opts_t;

/*
 * Verify every object referenced by every surviving snapshot: loads and
 * decompresses each object, re-hashing its content to confirm integrity.
 * If opts is non-NULL, populates parity stats and honours the repair flag.
 * Returns OK if all objects are present and uncorrupted, ERR_CORRUPT
 * if any are missing or have hash mismatches.
 */
status_t repo_verify(repo_t *repo, verify_opts_t *opts);

/*
 * Complete any prune that was interrupted.
 * Reads repo/prune-pending (if it exists), deletes any listed snap files
 * that have not already been removed, then removes the marker file.
 * Called automatically on every exclusive lock acquisition.
 */
status_t repo_prune_resume_pending(repo_t *repo);

