#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stdint.h>

/* Mutable working-set entry: (path, node) pair. */
typedef struct { char *path; node_t node; } ws_entry_t;

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

/*
 * Restore a directory subtree (all entries whose path equals subtree_path or
 * starts with subtree_path + "/") from a snapshot.
 */
status_t restore_subtree(repo_t *repo, uint32_t snap_id,
                         const char *subtree_path, const char *dest_path);

/*
 * After a restore, walk dest_path and verify every regular file's content
 * hash matches the snapshot.  Returns OK if all match, ERR_CORRUPT if any
 * mismatch.  Works for both live and pruned snapshots.
 */
status_t restore_verify_dest(repo_t *repo, uint32_t snap_id,
                              const char *dest_path);

/*
 * Build the working set for target_id by finding the nearest existing
 * snapshot anchor >= target_id and walking the reverse chain backward.
 * Caller must free: for (i=0; i<*out_cnt; i++) free((*out_ws)[i].path);
 *                   free(*out_ws);
 * out_anchor_id is set to the snapshot ID used as starting point.
 */
status_t restore_build_ws(repo_t *repo, uint32_t target_id,
                           ws_entry_t **out_ws, uint32_t *out_cnt,
                           uint32_t *out_anchor_id);
