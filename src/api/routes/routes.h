#pragma once

#include "server.h"
#include <cutils/db.h>
#include <cutils/config.h>
#include <stdint.h>

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
api_response_t metadata_policy_get(const api_request_t *req, int64_t repo_id);
api_response_t metadata_policy_put(const api_request_t *req, int64_t repo_id);
api_response_t metadata_journal_get(const api_request_t *req, int64_t repo_id);

/* Record an operation in a repo's journal. Best effort by design. */
void journal_record(cutils_db_t *db, int64_t repo_id, const char *source,
                    const char *operation, const char *result,
                    int64_t duration_ms, const char *message);
