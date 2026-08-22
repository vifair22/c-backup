#pragma once

#include "error.h"
#include "store.h"
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
#include <cutils/db.h>
#include <stdint.h>

/*
 * Tier membership, stored as a bitfield in snapshots.gfs_flags.
 *
 * A bitfield rather than a single tier because a snapshot can hold several
 * at once: the last Sunday of December is simultaneously that day's daily,
 * that week's weekly, that month's monthly and that year's yearly. Collapsing
 * it to one tier would lose the information that keeps it alive when the
 * daily window rolls past.
 *
 * Lived in snapshot.h when tiers were written into .snap headers.
 */
#define GFS_DAILY    (1u << 0)
#define GFS_WEEKLY   (1u << 1)
#define GFS_MONTHLY  (1u << 2)
#define GFS_YEARLY   (1u << 3)

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
/*
 * Ported to the database in Phase 6. The calendar boundaries and the tier
 * promotion algorithm are unchanged; only where the data comes from and
 * where the results go have moved. Retention counters come from
 * policy_values, tier flags are a column, and pruning is a transactional
 * row delete rather than an unlink.
 */
status_t gfs_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                 const gfs_retention_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet,
                 int full_scan, uint32_t *out_pruned);

/* Human-readable GFS tier string, e.g. "daily+weekly+monthly". */
void gfs_flags_str(uint32_t flags, char *buf, size_t sz);
