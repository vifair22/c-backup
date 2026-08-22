#pragma once

#include "server.h"
#include <cutils/db.h>
#include <cutils/config.h>

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
