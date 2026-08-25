#pragma once

#include "server.h"
#include <cutils/db.h>
#include <cutils/config.h>
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
status_t routes_run_pack(struct store *store, int64_t repo_id,
                         volatile int *cancel,
                         void (*progress)(const char *, uint64_t, uint64_t, void *),
                         void *progress_ctx);

/*
 * Read a boolean policy flag, falling back to def when the repo has not set
 * one. Policy lives in policy_values as text.
 */
int routes_policy_flag(cutils_db_t *db, int64_t repo_id, const char *key, int def);

/* Is this an action a schedule may name? The list lives with the dispatcher
 * so a schedule cannot be created for something that will not run. */
int routes_action_is_schedulable(const char *action);

/* Submit a backup from a schedule's params, which carry the same shape the
 * route's request body does. */
status_t routes_submit_backup(struct job_manager *jm, int64_t repo_id,
                              const char *params_json, int64_t *out_job_id);

/* params is the schedules.params column, action-specific and possibly empty.
 * Backup reads its source paths out of it, replication its destination. */
status_t routes_submit_scheduled(struct job_manager *jm, int64_t repo_id,
                                 const char *action, const char *params,
                                 int64_t *out_job_id);

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
