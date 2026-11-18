#pragma once

/*
 * Engine copies for plain file sources (docs/db-detection.md 9.2, 9.3).
 *
 * A file backup whose sources hold a SQLite finding with `sqlite-backup`
 * enabled stores, beside the raw file as read, a consistent copy made with
 * SQLite's online backup, at /.cbackup-captures/<original path>, and an
 * index of what it did at /.cbackup-captures/index.json. Both are ordinary
 * files in the snapshot: no format change, and readers that do not know the
 * directory see files. The raw file is always kept (decided, 13), read once
 * and flagged if it moved.
 *
 * Nothing here can fail the backup: a copy that cannot be made is left out
 * and said so, in index.json, the job result and the log.
 */

#include "backup.h"
#include "db_detect.h"
#include "error.h"
#include "reserved.h"
#include "sqlite_copy.h"

#include <cutils/db.h>
#include <stdint.h>
#include <time.h>

#define DB_CAPTURES_INDEX "/" RESERVED_CAPTURES_DIR "/index.json"
#define DB_CAPTURES_FORMAT "cbackup-captures"
#define DB_CAPTURES_VERSION 1

typedef struct db_captures db_captures_t;

/*
 * Plan the copies this backup makes: every SQLite finding enabled with
 * `sqlite-backup` in a scope of one of the job's sources, whose file is
 * there and regular now. tmp_dir is where copies are made (the
 * repository's tmp/), d the backup's detector, which is told which files
 * have a copy (read once, 9.5). *out is NULL when there is nothing to copy
 * or detection is off (d NULL). Errors reading the findings are logged and
 * plan nothing.
 */
void db_captures_plan(cutils_db_t *db, int64_t repo_id, const char *const *paths, size_t n_paths,
                      const char *tmp_dir, db_detector_t *d, db_captures_t **out);

/* Lend the plan's synthetic entries to a backup. */
void db_captures_apply(db_captures_t *c, backup_opts_t *o);

/* The job result's "captures" array, or NULL. */
char *db_captures_json(const db_captures_t *c);

/* What one capture did, for messages: NULL when path has none. */
typedef struct {
    const char           *path;
    const char           *copy_path;      /* in the snapshot */
    int64_t               finding_id;
    sqlite_copy_result_t  res;
    int                   attempted;
} db_capture_info_t;
const db_capture_info_t *db_captures_find(const db_captures_t *c, const char *path);

void db_captures_free(db_captures_t *c);

/*
 * The explanation of an unsettled file that is a detected database (9.5),
 * into buf; 0 and buf empty when path is not one. f and c may be NULL.
 */
int db_unsettled_why(const db_findings_t *f, const int64_t *ids, const db_captures_t *c,
                     const char *path, char *buf, size_t sz);

/* Engine copies made, by outcome, since start (metrics). */
uint64_t db_captures_total(sqlite_copy_outcome_t o);
