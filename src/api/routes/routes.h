#pragma once

#include "server.h"
#include <cutils/db.h>
#include <cutils/config.h>
#include <stdatomic.h>
#include <stdint.h>
#include "error.h"
#include "backup.h"

/*
 * Route registration.
 *
 * Each group registers its own paths so main.c does not accumulate a list of
 * every endpoint in the daemon.
 */

/* healthz and readyz. Public by design: a health check that needs a
 * credential cannot tell you the daemon is wedged before login works. */
int routes_register_health(api_server_t *srv, cutils_db_t *db);

/* Auth endpoints: setup, login, logout, password. Lifted from airies-ups. */
int routes_register_auth(api_server_t *srv, cutils_db_t *db);

/* Repo lifecycle endpoints. */
int routes_register_repos(api_server_t *srv, cutils_db_t *db);

/* Where per-repository keys are stored. Set from config at startup. */
void routes_repos_set_key_dir(const char *dir);

/* How strictly backups recognise an unchanged file (file_cache_match_t,
 * from backup.file_cache_match). Set from config at startup. */
void routes_backup_set_file_cache_match(int mode);

/*
 * The job warning, journalled and alerted, for a backup that left files out
 * (B118) or kept files that never settled (OWN-45-LIVE); empty for a clean
 * backup. Exposed for its test; the backup job is the only caller.
 */
void routes_backup_describe_doubts(const backup_result_t *res, char *warning, size_t wsz);
/* Chunk workers per backup (backup.store_workers): 0 automatic, 1 serial.
 * Set from config at startup. */
void routes_backup_set_store_workers(unsigned n);
unsigned routes_backup_store_workers(void);
void routes_auth_set_config(cutils_config_t *cfg);

/* Backup submission and job inspection. Needs the job manager because a
 * backup is queued rather than run inside the request. */
struct job_manager;
int routes_register_jobs(api_server_t *srv, cutils_db_t *db, struct job_manager *jm);

/*
 * Repo-scoped sub-handlers. repos.c owns the repo-scoped URL pattern and dispatches
 * here by suffix; these do not register routes of their own.
 */
void routes_snapshots_init(cutils_db_t *db, struct job_manager *jm);

/* What a retention pass keeps when a repository has set no retention.*
 * policy of its own: the daemon's configured defaults. */
void routes_snapshots_set_retention(int keep_snaps, int keep_daily, int keep_weekly,
                                    int keep_monthly, int keep_yearly);

/* The configured defaults back again, as policy keys and values, for the
 * policy route to report what is in force where a repository sets nothing.
 * Fills up to `max` entries and returns how many. */
typedef struct { const char *key; int value; } routes_retention_default_t;
int routes_snapshots_retention_defaults(routes_retention_default_t *out, int max);
api_response_t snapshots_route_get(const api_request_t *req, int64_t repo_id,
                                   const char *rest);
api_response_t snapshots_route_post(const api_request_t *req, int64_t repo_id,
                                    const char *rest);
api_response_t snapshots_route_delete(const api_request_t *req, int64_t repo_id,
                                      int64_t snap_id);
api_response_t backup_route_post(const api_request_t *req, int64_t repo_id,
                                 const char *rest);

/* Submit a maintenance job by action name, so the scheduler starts exactly
 * the work the routes do. */
/*
 * Read a boolean policy flag, falling back to def when the repo has not set
 * one. Policy lives in policy_values as text.
 */
int routes_policy_flag(cutils_db_t *db, int64_t repo_id, const char *key, int def);

/*
 * Parse a retention count as the policy route accepts it: decimal digits
 * only, 0 to INT_MAX. Returns 0 and sets *out, or -1. Readers use it too,
 * so a value stored before the route validated is ignored rather than read
 * through atoi as 0.
 */
int routes_policy_count(const char *text, int *out);

/* Overlay a unit's unit_policy rows (retention.* keys) on *ret, which holds
 * the repository's policy already. Invalid stored values are skipped with
 * a warning, as the repository's are. */
#include "gfs.h"
void routes_unit_policy(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                        gfs_retention_t *ret);

/* Units (src/api/routes/units.c). /providers registers its own routes; the
 * repo-scoped ones are dispatched from repos.c, and units_path_entry is
 * their part of the repo path table (NULL for a path that is not theirs). */
struct job_manager;
void           routes_units_init(cutils_db_t *db, struct job_manager *jm);
int            routes_register_providers(api_server_t *srv);
const api_path_t *units_path_entry(const char *rest);

/* The /repos/<id> path tables, whole, for the test that holds every write
 * in them to a durability class (tests/test_route_classes.c). */
const api_path_t *routes_repo_paths(size_t *n);
const api_path_t *routes_units_paths(size_t *n);
api_response_t units_route_get(const api_request_t *req, int64_t repo_id, const char *rest);
api_response_t units_route_put(const api_request_t *req, int64_t repo_id, const char *rest);
api_response_t units_route_post(const api_request_t *req, int64_t repo_id, const char *rest);
/* A `units` job from a schedule's params, which have the route's shape. */
status_t       routes_submit_units(struct job_manager *jm, int64_t repo_id,
                                   const char *params, int64_t schedule_id,
                                   int64_t *out_job_id);
/* Validate and canonicalise those params, for the schedule route. */
int            routes_units_params_check(const char *params, char **canon,
                                         char *why, size_t wsz);

/*
 * Read ?limit= and (when offset is non-NULL) ?offset= for a collection
 * route. Absent means def_limit and 0; a limit above max_limit is clamped
 * and an offset past INT_MAX reads as INT_MAX. Something that is not a
 * whole number ("lots", "+5", "1.5") or a value below the minimum (0 for a
 * limit, negative for either) returns -1 with *bad set to a 400.
 */
int routes_paging(const api_request_t *req, int def_limit, int max_limit,
                  int *limit, int *offset, api_response_t *bad);

/*
 * An id from the URL path: the segment at p, up to the next '/' or the
 * end. Returns 1 with *out set and *end just past it when the segment is
 * decimal digits with a value from min to 2^53; 0 when it is empty; -1 for
 * anything else - a sign, a fraction, an exponent, letters, or too many
 * digits - which the caller answers with a 400 "invalid <kind> id".
 *
 * Ids were read with strtoll and whatever it made of the segment decided
 * the route, so /repos/1/snapshots/99999999999999999999 clamped to
 * LLONG_MAX and was a 405, 1e999 read as 1 followed by junk and was a 404,
 * and /jobs/-1 was a 404 where /repos/-1 was a 400 (NIT-8).
 */
int routes_path_id(const char *p, int64_t min, int64_t *out, const char **end);

/*
 * Can repository repo_id replicate to dest? Refuses a relative dest, and one
 * that is the source repository, lies inside it, or contains it (compared
 * canonically). Returns 0, or -1 with the reason written to why.
 */
int routes_replicate_dest_check(int64_t repo_id, const char *dest,
                                char *why, size_t whysz);

/* Is this an action a schedule may name? The list lives with the dispatcher
 * so a schedule cannot be created for something that will not run. */
int routes_action_is_schedulable(const char *action);

/* Submit a backup from a schedule's params, which carry the same shape the
 * route's request body does. */
status_t routes_submit_backup(struct job_manager *jm, int64_t repo_id,
                              const char *params_json, int64_t schedule_id,
                              int64_t *out_job_id);

/* params is the schedules.params column, action-specific and possibly empty.
 * Backup reads its source paths out of it, replication its destination.
 * schedule_id names the schedule on the job and in its journal entry. */
status_t routes_submit_scheduled(struct job_manager *jm, int64_t repo_id,
                                 const char *action, const char *params,
                                 int64_t schedule_id, int64_t *out_job_id);

/* A reindex the daemon queues for itself at start, for a repository whose
 * index lost snapshots the repository still holds (index_reconcile.h). The
 * same job a POST /repos/<id>/reindex runs, with origin "startup". Needs
 * routes_snapshots_init first. */
status_t routes_submit_startup_reindex(struct job_manager *jm, int64_t repo_id,
                                       int64_t *out_job_id);
/*
 * Queue a reindex the daemon starts itself, recorded with `origin` (a
 * string literal) as the job's origin and its journal source. Used by
 * registration: a repository adopted with snapshots the index has never
 * seen otherwise lists none and verifies "checked 0" until somebody
 * thinks to reindex it.
 */
status_t routes_submit_reindex(int64_t repo_id, const char *origin, int64_t *out_job_id);

/* Tags, notes, policy and the journal. Repo-scoped sub-handlers, dispatched
 * from repos.c like the snapshot ones. */
void routes_metadata_init(cutils_db_t *db);
api_response_t metadata_tags_get(const api_request_t *req, int64_t repo_id);
api_response_t metadata_tags_post(const api_request_t *req, int64_t repo_id);
api_response_t metadata_tags_delete(const api_request_t *req, int64_t repo_id,
                                    const char *name);
api_response_t metadata_note_get(const api_request_t *req, int64_t repo_id,
                                 int64_t snap_id);
api_response_t metadata_note_put(const api_request_t *req, int64_t repo_id,
                                 int64_t snap_id);
/* What a repository backs up, so it is configured once rather than named on
 * every run. A request that names paths still overrides. */
api_response_t metadata_sources_get(const api_request_t *req, int64_t repo_id);
api_response_t metadata_sources_put(const api_request_t *req, int64_t repo_id);

api_response_t metadata_policy_get(const api_request_t *req, int64_t repo_id);
api_response_t metadata_policy_put(const api_request_t *req, int64_t repo_id);
api_response_t metadata_journal_get(const api_request_t *req, int64_t repo_id);

/* NBD exports of a unit disk (src/api/routes/exports.c, vm-backup.md 7.3).
 * init closes rows a previous run left open; tick, from the daemon's main
 * loop, ends expired and idle exports; shutdown ends every one. */
void           routes_exports_init(cutils_db_t *db, const char *dir, unsigned ttl_s, unsigned idle_s,
                                   unsigned max_conns);
/* Revoke every open export of a repository (journalled), before it goes. */
void           exports_close_repo(int64_t repo_id, const char *reason);
/* Whether a repository has an export open: gc's gate (job_manager_set_gate),
 * so gc waits in the queue rather than failing. Memory only, never the DB. */
int            exports_repo_open(int64_t repo_id);
int            routes_register_exports(api_server_t *srv);
api_response_t exports_create(const api_request_t *req, int64_t repo_id, int64_t snap_id);
void           exports_tick(void);
void           exports_shutdown(void);

/* Record an operation in a repo's journal. Best effort by design. */
/* 0 when recorded, -1 when not (logged). Most callers ignore it: failing
 * to journal must not fail what is journalled. A job's end is the
 * exception, which keeps the entry and its state together (B159, B196). */
int journal_record(cutils_db_t *db, int64_t repo_id, const char *source,
                   const char *operation, const char *result,
                   int64_t duration_ms, const char *message);

/* Schedule CRUD. Repo-scoped sub-handlers, dispatched from repos.c. */
struct scheduler;
void routes_schedules_init(cutils_db_t *db, struct scheduler *sched);
/*
 * Hold every enabled schedule to the rules the route applies to a new one,
 * and disable each that fails, logging which and why and recording the
 * reason for the list to show. Rows written before a rule existed survive an
 * upgrade otherwise. Run at startup before the scheduler first reads them.
 * Returns how many were disabled, or -1 when the table cannot be read.
 */
int routes_schedules_audit(cutils_db_t *db);
api_response_t schedules_get(const api_request_t *req, int64_t repo_id);
api_response_t schedules_post(const api_request_t *req, int64_t repo_id);
api_response_t schedules_patch(const api_request_t *req, int64_t repo_id,
                               int64_t schedule_id);
api_response_t schedules_delete(const api_request_t *req, int64_t repo_id,
                                int64_t schedule_id);

/* Prometheus metrics. Public, like the health endpoints: a metrics endpoint
 * that needs a credential cannot report a daemon that is too unhealthy to
 * authenticate. */
int routes_register_metrics(api_server_t *srv, cutils_db_t *db);

/* SSE progress stream for one job. */
int routes_register_stream(api_server_t *srv, cutils_db_t *db);

/* Introspection: objects, packs, statistics, diff. Repo-scoped sub-handlers
 * dispatched from repos.c, like the snapshot and metadata ones. */
void routes_introspect_init(cutils_db_t *db);
api_response_t introspect_stats(const api_request_t *req, int64_t repo_id);
api_response_t introspect_summary(const api_request_t *req, int64_t repo_id);
api_response_t introspect_objects(const api_request_t *req, int64_t repo_id);
api_response_t introspect_object_locate(const api_request_t *req, int64_t repo_id,
                                        const char *hex);
api_response_t introspect_object_refs(const api_request_t *req, int64_t repo_id,
                                      const char *hex);
api_response_t introspect_object_layout(const api_request_t *req, int64_t repo_id,
                                        const char *hex);
api_response_t introspect_object_content(const api_request_t *req, int64_t repo_id,
                                         const char *hex);
api_response_t introspect_packs(const api_request_t *req, int64_t repo_id);
api_response_t introspect_pack_entries(const api_request_t *req, int64_t repo_id,
                                       int64_t pack_id);
api_response_t introspect_fanout(const api_request_t *req, int64_t repo_id);
api_response_t introspect_diff(const api_request_t *req, int64_t repo_id);
