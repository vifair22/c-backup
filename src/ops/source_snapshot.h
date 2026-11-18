#pragma once

/*
 * Source snapshot mode for plain file sources (docs/db-detection.md 9.4).
 *
 * A file source opted into snapshot mode is read from a ZFS or btrfs
 * snapshot the helper takes for the job (capture.begin_paths) and recorded
 * at its own path: backup_run gets read_paths, and the engine remaps the
 * device so the trees and file cache rows are a live read's (B229).
 *
 * Per source, in repo_backup_paths (kind 'snapshot:strict' or
 * 'snapshot:warn', value the source path):
 *
 *   strict  (the default for --snapshot) a source that cannot be read from
 *           a snapshot fails the backup before anything is read, naming
 *           the path and the reason.
 *   warn    it is read live instead, and the job says so: a warning naming
 *           the path and the reason (journalled, alerted at warning level)
 *           and the path listed in the job result's live_sources.
 *
 * A source is never read live without one or the other. The capture is
 * ended after the read whatever the outcome (success, failure, cancel); a
 * daemon that dies holding one has it ended by the helper when the
 * daemon's session goes, and a helper that dies has it swept at its start.
 */

#include "error.h"

#include <cutils/db.h>

#include <stddef.h>

typedef enum {
    SRC_SNAP_OFF    = 0,
    SRC_SNAP_STRICT = 1,
    SRC_SNAP_WARN   = 2,
} src_snap_mode_t;

#define SRC_SNAP_KIND_STRICT "snapshot:strict"
#define SRC_SNAP_KIND_WARN   "snapshot:warn"
#define SRC_SNAP_PATHS_MAX   64      /* capture.begin_paths' bound */

/* "strict", "warn" or "off"; -1 for anything else. */
int         src_snap_mode_parse(const char *s);
const char *src_snap_mode_name(int mode);

/* The mode configured for each of paths[0..n) in this repository, into
 * modes[] (SRC_SNAP_OFF where none). */
status_t src_snap_modes_load(cutils_db_t *db, int64_t repo_id, const char *const *paths,
                             size_t n, int *modes);

typedef struct {
    int   mode;
    char *read_root;      /* NULL: read where it is */
    char  kind[8];        /* "zfs", "btrfs"; "" when read live */
    char *reason;         /* why it is read live; NULL otherwise */
} src_snap_path_t;

typedef struct {
    size_t           n;            /* the job's paths */
    src_snap_path_t *p;
    const char     **read_paths;   /* parallel to the job's paths, for backup_opts_t; NULL if none */
    char            *capture;      /* the helper's token; NULL: none held */
    size_t           n_snapshot;   /* paths read from a snapshot */
    size_t           n_live;       /* opted-in paths read live (warn mode) */
    int              lease_lost;   /* the helper had destroyed the capture by its end */
} src_snap_t;

/*
 * Ask the helper for snapshots of every path whose mode is not OFF and work
 * out where each is read. one_filesystem: the source does not cross mounts,
 * so a mount below it, which the snapshot shows empty, costs nothing; with
 * it off such a path cannot be read from the snapshot.
 *
 * ERR_INVALID when a strict path cannot be read from a snapshot, the error
 * naming each such path and its reason; any capture taken is ended first.
 * Otherwise OK, with *out to pass to backup_run and then to src_snap_end.
 */
status_t src_snap_begin(const char *const *paths, const int *modes, size_t n,
                        int one_filesystem, src_snap_t *out);

/*
 * The read is over: end the capture now (logged, never fatal: the helper's
 * lease and sweep are the backstop). A capture the helper no longer has
 * means its lease ran out during the read, which sets lease_lost.
 */
void src_snap_release(src_snap_t *s);

/*
 * A warning naming every opted-in path read live and why, and a lost lease,
 * for the job and the journal; "" when there is neither.
 */
void src_snap_describe_live(const src_snap_t *s, const char *const *paths, char *buf, size_t sz);

/* The job result's fields, ',"source_snapshots":{...},"live_sources":[...]',
 * malloc'd; NULL when no path was opted in. */
char *src_snap_result_json(const src_snap_t *s, const char *const *paths);

/* src_snap_release if not done, and free everything. Safe on a zeroed
 * struct. */
void src_snap_end(src_snap_t *s);
