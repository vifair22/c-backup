#pragma once

/*
 * Database findings: what a backup's detector found, kept in state.db
 * (db_findings, docs/db-detection.md 7.1) and written into the job result.
 *
 * The detector (db_detect.h) knows nothing of the database; this is the
 * half that does. It never fails a backup: every caller logs a failure here
 * and carries on (principle 5).
 */

#include "db_detect.h"
#include "error.h"

#include <cutils/db.h>
#include <stdint.h>

/* db_detect.* config, read once at start (app_config.h). */
typedef struct {
    int      enabled;            /* db_detect.enabled */
    unsigned sqlite_min_kb;      /* db_detect.sqlite_min_kb */
    int      notify_uncovered;   /* db_detect.notify_uncovered */
} db_detect_settings_t;

void db_detect_settings_set(const db_detect_settings_t *s);
/* Defaults (enabled, 1024, notify) until set. */
db_detect_settings_t db_detect_settings(void);
/* A detector configured from the settings, or NULL when detection is off
 * or cannot start (logged). */
db_detector_t *db_detect_start(void);

/* Something a caller journals or alerts about, after a record. */
typedef enum {
    DB_EVENT_NEW_LIVE = 1,       /* a live finding seen for the first time */
    DB_EVENT_COVERAGE,           /* a live finding's coverage changed */
    DB_EVENT_ALERT,              /* uncovered, first of its spell: alert once */
} db_event_kind_t;

typedef struct {
    db_event_kind_t kind;
    int64_t         finding_id;
    char            text[1024];
} db_event_t;

typedef struct db_record_result {
    int64_t    *ids;             /* parallel to the findings; 0 when not stored */
    db_event_t *events;
    size_t      n_events;
} db_record_result_t;

void db_record_result_free(db_record_result_t *r);

/*
 * Record a run's findings. Each finding's scope is root_scopes[f->root]
 * when root_scopes is given and the root is in range, else scope. Rows of
 * every scope this run covered (all of root_scopes, or scope) that are new
 * or suggested and were not seen this run are dropped: the database is
 * gone. Enabled and dismissed rows are the user's, and stay.
 *
 * snap_id is the snapshot the findings came from. Writes go through
 * db_txn. On failure nothing is recorded and the error is set.
 */
status_t db_findings_record(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                            const db_findings_t *f,
                            const char *const *root_scopes, size_t n_root_scopes,
                            const char *scope, db_record_result_t *out);

/*
 * The same for a detection run that made no snapshot
 * (POST /repos/<id>/units/<p>/<u>/detect): last_snap_id is left as it was,
 * the run's own rows are told apart by last_seen, and nothing alerts,
 * since the coverage is a prediction; the journal events still come back.
 */
status_t db_findings_record_detect(cutils_db_t *db, int64_t repo_id, const db_findings_t *f,
                                   const char *scope, db_record_result_t *out);

/*
 * Resolve each Nextcloud finding's dbhost that its own unit could not
 * (section 3) against the repository's other units: the unit whose
 * recorded database findings answer to that name. Two units are one
 * capture only when they share capture.group; otherwise the finding is
 * suggested "group-units". Rewrites the findings' evidence; reads only.
 */
void db_findings_link_nextcloud(cutils_db_t *db, int64_t repo_id, const char *scope,
                                db_findings_t *f);

/*
 * The findings as a JSON object for a job result:
 * {"findings":[...],"listed":N,"total":N,"sqlite_small":N}, each finding
 * with its id when ids is given. At most DB_FINDINGS_LISTED_MAX listed.
 * Returns a malloc'd string, or NULL.
 */
#define DB_FINDINGS_LISTED_MAX 200u
char *db_findings_json(const db_findings_t *f, const int64_t *ids);

/* Metrics (docs/db-detection.md 12). */
uint64_t db_detect_uncovered_reads_total(void);
void     db_detect_count_uncovered(const db_findings_t *f);
