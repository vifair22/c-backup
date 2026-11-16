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
#include <time.h>

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
 * Periods are local calendar days on the daemon's host:
 *   daily   - the last snapshot of each day
 *   weekly  - the last daily of each Monday-Sunday week
 *   monthly - the last weekly of each month
 *   yearly  - the last monthly of each year
 * A period still in progress elects its latest snapshot so far, so the
 * newest snapshot holds every tier. Each pass recomputes every snapshot's
 * tiers from nothing, flushes the ones that changed, then prunes what no
 * tier, recency window or tag keeps. Manual section 9.1 is the contract.
 */

/* Working record for a single snapshot during GFS processing */
typedef struct {
    uint32_t id;
    uint64_t ts;
    uint32_t gfs_flags;
    int      preserved;   /* protected by a preserved tag */
} gfs_snap_info_t;

/*
 * What a pass removed, or on a dry run would remove: the snapshot ids, in
 * ascending order, and how many. The caller reports them; a dry run whose
 * answer reached only the daemon log answered nobody who asked for it.
 * Ids only, so the array is bounded by the snapshot count, which the pass
 * already holds in memory. Release with gfs_result_free.
 */
typedef struct {
    uint32_t  pruned;
    uint32_t *ids;
} gfs_result_t;

void gfs_result_free(gfs_result_t *r);

/*
 * new_snap_id is the repository's newest snapshot (the head): it is never
 * pruned and anchors the keep_snaps window. There is no incremental mode and
 * no notion of a closed day any more; see the election in gfs.c for why.
 */
status_t gfs_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                 const gfs_retention_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet,
                 gfs_result_t *out);

/* Human-readable GFS tier string, e.g. "daily+weekly+monthly". */
/*
 * One retention group (docs/source-providers.md 8.1): group NULL is the file
 * snapshots (those with no snapshot_units row), otherwise one unit key's.
 * The election is gfs_run's over that group alone, with the group's newest
 * snapshot as its head, never pruned. gfs_run is this for the file group
 * with the head given. A group named unknown:* is never pruned.
 */
status_t gfs_run_group(store_t *store, cutils_db_t *db, int64_t repo_id,
                       const gfs_retention_t *policy, const char *group,
                       int dry_run, int quiet, gfs_result_t *out);

void gfs_flags_str(uint32_t flags, char *buf, size_t sz);
