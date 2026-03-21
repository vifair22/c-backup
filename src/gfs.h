#pragma once

#include "error.h"
#include "repo.h"
#include "policy.h"
#include "snapshot.h"
#include <stdint.h>

/*
 * GFS (Grandfather-Father-Son) retention engine.
 *
 * GFS boundaries are fixed to standard calendar days (UTC):
 *   daily   — last backup of each calendar day
 *   weekly  — Sunday's daily is promoted to weekly
 *   monthly — last Sunday of the month's weekly is promoted to monthly
 *   yearly  — December's monthly is promoted to yearly
 *
 * After each backup run, gfs_run():
 *   1. Detects which boundaries were crossed by the new snapshot
 *   2. Flags the appropriate snap(s) with GFS tier bits
 *   3. Prunes non-GFS snaps outside the rev retention window
 *   4. Deletes revs that satisfy BOTH: older than oldest GFS anchor
 *      AND outside the rev retention window
 *   5. Runs GC to reclaim objects no longer referenced
 */

/* Working record for a single snapshot during GFS processing */
typedef struct {
    uint32_t id;
    uint64_t ts;
    uint32_t gfs_flags;
    int      preserved;   /* protected by a preserved tag */
} gfs_snap_info_t;

/*
 * Main entry point.  Call after each successful backup_run_opts.
 * new_snap_id is the ID just committed (== HEAD).
 */
status_t gfs_run(repo_t *repo, const policy_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet);

/*
 * Synthesise a .snap for snap_id (if missing) then OR gfs_flags into it.
 * Verifies the snap loads correctly before returning OK.
 */
status_t snapshot_synthesize_gfs(repo_t *repo, uint32_t snap_id, uint32_t gfs_flags);

/* Human-readable GFS tier string, e.g. "daily+weekly+monthly". */
void gfs_flags_str(uint32_t flags, char *buf, size_t sz);
