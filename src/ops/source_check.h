#pragma once

/*
 * Sources that came back empty, or much smaller than last time (B295).
 *
 * A source path that is a mount point nobody mounted is an empty directory,
 * and a backup of it succeeds: on FS2 a repository backed up a stub NFS
 * mountpoint every day and every job said "completed". Two things follow
 * from that, and they live here.
 *
 * At backup time, each source root of a file backup is counted from the
 * scan and judged: empty (nothing below it), an empty directory that is an
 * unmounted fstab mount point or an empty mount, or a drop of more than
 * backup.source_drop_pct per cent in files or bytes against the previous
 * non-empty snapshot of the same path. Each is a warning naming the source;
 * the job completes with it and the warning alert fires. The counts are
 * kept in snapshot_sources, a derived table that is the drop check's only
 * input: a reindex that loses it costs the next backup its comparison, never
 * a retention decision.
 *
 * At retention time, each configured source keeps its newest snapshot in
 * which that source is not empty, whatever keep_snaps and the tiers say, so
 * a run of empty snapshots cannot push the last good one out. Emptiness is
 * read from the repository (the snapshot's tree at the source's path), not
 * from the index, so it holds after a reindex. At most one extra snapshot
 * per source is kept.
 */

#include "error.h"
#include "scan.h"
#include "store.h"

#include <cutils/db.h>
#include <stddef.h>
#include <stdint.h>

/* What the default drop threshold is, in per cent; 0 turns the check off. */
#define SOURCE_DROP_PCT_DEFAULT 50

typedef enum {
    SOURCE_OK = 0,
    SOURCE_EMPTY,          /* a directory with nothing below it */
    SOURCE_UNMOUNTED,      /* empty, and an fstab mount point with nothing mounted */
    SOURCE_EMPTY_MOUNT,    /* empty, and something is mounted there */
    SOURCE_DROPPED,        /* files or bytes fell past the threshold */
} source_state_t;

typedef struct {
    char          *path;      /* the root as recorded */
    int            is_dir;
    uint64_t       entries;   /* everything below the root; a file root counts itself */
    uint64_t       files;     /* regular files */
    uint64_t       bytes;     /* their sizes */
    source_state_t state;
    /* SOURCE_DROPPED: the snapshot compared against, and its counts. */
    int64_t        ref_snap;
    uint64_t       ref_files, ref_bytes;
} source_report_t;

/* Every source root of one backup, judged, and the warning for those
 * flagged (NULL when none is). Release with source_findings_free. */
typedef struct {
    source_report_t *r;
    int              n;
    char            *warning;
} source_findings_t;

const char *source_state_name(source_state_t s);

/*
 * The whole backup-time check, for backup_run once the snapshot's row is
 * in: count each of the n roots from the scan, judge them, record the
 * counts, and describe what is flagged into *out, logging it. n 0 (a unit
 * snapshot) does nothing. Advisory throughout: a failure is logged, never
 * returned, since the snapshot already holds everything it stored.
 */
void source_check_backup(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                         const scan_result_t *scan, const char *const *paths, int n,
                         int drop_pct, source_findings_t *out);
void source_findings_free(source_findings_t *f);

/*
 * Count each root's entries in a finished scan. paths are the recorded
 * roots in backup order. *out gets n reports, state SOURCE_OK; release
 * with source_reports_free.
 */
status_t source_check_count(const scan_result_t *scan, const char *const *paths, int n,
                            source_report_t **out);

/*
 * Judge each report: an empty directory root is classified by its mount
 * state, and a non-empty one is compared against the newest earlier
 * snapshot of this repository that recorded the same path with something
 * in it, when drop_pct is 1 to 100. snap_id is the snapshot just written.
 */
void source_check_classify(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           source_report_t *r, int n, int drop_pct);

/* Record the counts for snap_id in snapshot_sources. Advisory: a failure
 * only costs the next backup its comparison, and is the caller's to log. */
status_t source_check_record(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                             const source_report_t *r, int n);

/* The warning for every flagged report, "; "-joined, into buf. Empty when
 * nothing is flagged. Returns how many were flagged. */
int source_check_describe(const source_report_t *r, int n, int64_t snap_id, int drop_pct,
                          char *buf, size_t sz);

void source_reports_free(source_report_t *r, int n);

/* An empty directory's mount state: SOURCE_UNMOUNTED, SOURCE_EMPTY_MOUNT
 * or SOURCE_EMPTY. Exposed for its test. */
source_state_t source_mount_state(const char *path);

/* Read these instead of /etc/fstab and /proc/self/mountinfo; NULL puts the
 * real one back. Tests only. */
void source_check_set_mount_files(const char *fstab, const char *mountinfo);

/*
 * Retention's guard. For every source root of the group (configured on the
 * repository, recorded by a backup in snapshot_sources, or implied by the
 * tree of a snapshot with no recorded roots), the newest of the snapshots
 * `ids` (ascending, one retention group) in which
 * the source's path is a directory holding at least one entry, or any
 * non-directory. A snapshot whose tree cannot be read counts as non-empty:
 * the guard keeps what it cannot judge. A source no snapshot holds, or holds
 * only empty, has no guard. *out gets one per guarded source.
 */
typedef struct {
    char    *source;
    uint32_t snap_id;
    /* Set by retention: 1 when nothing but this guard kept the snapshot. */
    int      held;
} source_guard_t;

status_t source_guard_find(store_t *store, cutils_db_t *db, int64_t repo_id,
                           const uint32_t *ids, uint32_t n,
                           source_guard_t **out, uint32_t *n_out);
void source_guards_free(source_guard_t *g, uint32_t n);
