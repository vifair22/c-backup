#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/*
 * Synthesize a full snapshot file for target_id by walking the reverse chain.
 * If the snapshot file already exists this is a no-op (returns OK).
 * On success, snapshots/<target_id>.snap is written and can be used by
 * restore_snapshot() directly without touching the reverse chain.
 */
status_t snapshot_synthesize(repo_t *repo, uint32_t target_id);

/*
 * Synthesize checkpoints at every <interval> snapshots that don't already
 * have a full snapshot file.  E.g. interval=5 synthesizes snaps 5,10,15,...
 * *out_count is incremented for each checkpoint written (may be NULL).
 */
status_t snapshot_synthesize_every(repo_t *repo, uint32_t interval,
                                   uint32_t *out_count);

/*
 * Synthesise a snapshot for snap_id (if missing), OR new_flags into its
 * gfs_flags, then verify it loads.  Used by the GFS engine.
 * Declared here to avoid a circular dependency between gfs.h and synth.h.
 */
status_t snapshot_synthesize_gfs(repo_t *repo, uint32_t snap_id, uint32_t new_flags);
