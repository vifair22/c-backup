#pragma once

#include "server.h"
#include <cutils/db.h>
#include <cutils/config.h>
#include <stdatomic.h>
#include <stdint.h>
#include "error.h"

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
 * Run the pack phase against an already-open store.
 *
 * Shared so the backup job can finish by packing without duplicating the
 * progress adaptation: pack.c predates the job manager and takes its progress
 * callback with the counters first.
 */
struct store;
struct pack_report;
status_t routes_run_pack(struct store *store, int64_t repo_id,
                         atomic_int *cancel,
                         void (*progress)(const char *, uint64_t, uint64_t, void *),
                         void *progress_ctx, struct pack_report *out);

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

/*
 * Read ?limit= and (when offset is non-NULL) ?offset= for a collection
 * route. Absent means def_limit and 0; a limit above max_limit is clamped.
 * Anything that is not a plain decimal count - negative, signed, zero for a
 * limit, trailing junk - returns -1 with *bad set to a 400.
 */
int routes_paging(const api_request_t *req, int def_limit, int max_limit,
                  int *limit, int *offset, api_response_t *bad);

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

/* Record an operation in a repo's journal. Best effort by design. */
void journal_record(cutils_db_t *db, int64_t repo_id, const char *source,
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
