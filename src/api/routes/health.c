#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include <cutils/db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef VERSION_STRING
#define VERSION_STRING "0.0.0-dev"
#endif

/* Liveness: the process is up and answering. Says nothing about whether it
 * can do useful work, which is what /readyz is for. */
static api_response_t handle_healthz(const api_request_t *req, void *userdata)
{
    (void)req; (void)userdata;
    char *body = strdup("{\"status\":\"ok\"}");
    return body ? api_ok(body) : api_error(500, "out of memory");
}

/*
 * Readiness: the database answers. This is the thing that actually goes
 * wrong in practice (bad path, wrong permissions, disk full), and it fails
 * in a way a liveness probe cannot see, since the process stays up.
 */
static api_response_t handle_readyz(const api_request_t *req, void *userdata)
{
    (void)req;
    cutils_db_t *db = userdata;
    if (!db) return api_error(503, "database not open");

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT 1", NULL, &r) != 0) {
        db_result_free(r);
        return api_error(503, "database not responding");
    }
    db_result_free(r);

    char *body = NULL;
    if (asprintf(&body, "{\"status\":\"ready\",\"version\":\"%s\"}", VERSION_STRING) < 0)
        return api_error(500, "out of memory");
    return api_ok(body);
}

int routes_register_health(api_server_t *srv, cutils_db_t *db)
{
    if (api_server_route(srv, "/healthz", API_GET, handle_healthz, NULL) != 0) return -1;
    if (api_server_route(srv, "/readyz",  API_GET, handle_readyz,  db)   != 0) return -1;
    return 0;
}
