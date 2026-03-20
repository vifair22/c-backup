#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/* Restore latest snapshot to dest_path. */
status_t restore_latest(repo_t *repo, const char *dest_path);

/* Restore a specific snapshot (by ID) to dest_path. */
status_t restore_snapshot(repo_t *repo, uint32_t snap_id, const char *dest_path);

/*
 * Historical restore: reconstruct the tree as it was at target_id.
 * Uses the target snapshot file if present; otherwise walks the reverse chain.
 */
status_t restore_snapshot_at(repo_t *repo, uint32_t target_id, const char *dest_path);

/* Restore a single file (by repo-relative path) from a snapshot. */
status_t restore_file(repo_t *repo, uint32_t snap_id,
                      const char *file_path, const char *dest_path);
