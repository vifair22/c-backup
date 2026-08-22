#pragma once

#include "error.h"
#include "repo.h"
/* Retention counters, formerly the relevant slice of policy_t from the
 * deleted policy.h. Phase 6 sources these from policy_values rows in the
 * daemon DB; the field names are kept so the algorithm below is untouched. */
typedef struct {
    int keep_snaps;
    int keep_daily;
    int keep_weekly;
    int keep_monthly;
    int keep_yearly;
} gfs_retention_t;
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
 *   3. Prunes non-GFS snaps outside the snapshot retention window
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
/*
 * full_scan=0: incremental — process only windows closed since the
 *              previous snapshot.
 * full_scan=1: clear all existing GFS flags and recompute from scratch
 *              across the full history.  Use for manual `backup gfs` runs.
 */
status_t gfs_run(repo_t *repo, const gfs_retention_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet,
                 int full_scan, uint32_t *out_pruned);

/* Human-readable GFS tier string, e.g. "daily+weekly+monthly". */
void gfs_flags_str(uint32_t flags, char *buf, size_t sz);
