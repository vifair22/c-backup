#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stdint.h>

/* Restore latest snapshot to dest_path. */
status_t restore_latest(repo_t *repo, const char *dest_path);

/* Restore a specific snapshot (by ID) to dest_path. */
status_t restore_snapshot(repo_t *repo, uint32_t snap_id, const char *dest_path);

/* Restore a single file (by repo-relative path) from a snapshot. */
status_t restore_file(repo_t *repo, uint32_t snap_id,
                      const char *file_path, const char *dest_path);

/*
 * Restore a directory subtree (all entries whose path equals subtree_path or
 * starts with subtree_path + "/") from a snapshot.
 */
status_t restore_subtree(repo_t *repo, uint32_t snap_id,
                         const char *subtree_path, const char *dest_path);

/*
 * After a restore, walk dest_path and verify every regular file's content
 * hash matches the snapshot.  Returns OK if all match, ERR_CORRUPT if any
 * mismatch.
 */
status_t restore_verify_dest(repo_t *repo, uint32_t snap_id,
                               const char *dest_path);
