#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "sync_latch.h"
#include "wal_health.h"

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
    /* Still alive after a failed storage sync, and still answering reads,
     * but it says so: restarting it is what clears the state. */
    char msg[1024];
    int64_t at = 0;
    if (sync_latch_info(msg, sizeof(msg), &at)) {
        cJSON *o = cJSON_CreateObject();
        if (!o) return api_error(500, "out of memory");
        cJSON_AddStringToObject(o, "status", "ok");
        cJSON_AddStringToObject(o, "storage_sync", "failed");
        cJSON_AddStringToObject(o, "storage_sync_error", msg);
        cJSON_AddNumberToObject(o, "storage_sync_failed_at", (double)at);
        cJSON_AddNumberToObject(o, "repos_sync_failed", (double)sync_latch_repo_count());
        char *body = cJSON_PrintUnformatted(o);
        cJSON_Delete(o);
        return body ? api_ok(body) : api_error(500, "out of memory");
    }
    char *body = NULL;
    if (asprintf(&body, "{\"status\":\"ok\",\"storage_sync\":\"ok\",\"repos_sync_failed\":%u}",
                 sync_latch_repo_count()) < 0)
        return api_error(500, "out of memory");
    return api_ok(body);
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
    /* Not ready: durable writes and the jobs that need them are refused
     * until a restart (sync_latch.h). */
    char msg[1024];
    if (sync_latch_info(msg, sizeof(msg), NULL)) {
        char why[1200];
        snprintf(why, sizeof(why), "not ready: a storage sync failed (%s); restart the "
                 "daemon to re-read from disk", msg);
        return api_error(503, why);
    }

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT 1", NULL, &r) != 0) {
        db_result_free(r);
        return api_error(503, "database not responding");
    }
    db_result_free(r);
    r = NULL;     /* the repos query below is skipped when none is latched */

    /*
     * A repository whose syncs failed leaves the daemon ready: "degraded",
     * still 200, and the repositories listed. Readiness is the daemon's,
     * and what acts on a 503 - a container health check, an orchestrator -
     * restarts or routes around the whole daemon: that would stop the
     * backups of every healthy repository, and a restart clears the latch
     * without the deep verify `repo repair` runs. state.db is different:
     * nothing works without it, so that one is 503 above.
     */
    /*
     * Degraded, not 503, while state.db's WAL is not being checkpointed
     * (wal_health.h, B215): everything still works, but the WAL grows until
     * the volume fills, and an operator has to hear about it before then.
     */
    char wal_msg[600];
    int64_t wal_since = 0;
    int wal_bad = wal_health_failing(wal_msg, sizeof(wal_msg), &wal_since);
    if (sync_latch_repo_count() > 0 || wal_bad) {
        cJSON *o = cJSON_CreateObject();
        cJSON *a = o ? cJSON_AddArrayToObject(o, "repos_sync_failed") : NULL;
        if (!a) { cJSON_Delete(o); return api_error(500, "out of memory"); }
        cJSON_AddStringToObject(o, "status", "degraded");
        cJSON_AddStringToObject(o, "version", VERSION_STRING);
        if (wal_bad) {
            cJSON_AddStringToObject(o, "state_db_checkpoint", "failing");
            cJSON_AddStringToObject(o, "state_db_checkpoint_error", wal_msg);
            cJSON_AddNumberToObject(o, "state_db_checkpoint_failing_since", (double)wal_since);
        }
        if (sync_latch_repo_count() > 0 &&
            db_execute(db, "SELECT id, path FROM repos ORDER BY id", NULL, &r) == 0 && r) {
            for (int i = 0; i < r->nrows; i++) {
                char m[1024];
                int64_t at = 0;
                if (!sync_latch_repo_info(r->rows[i][1], m, sizeof(m), &at)) continue;
                cJSON *e = cJSON_CreateObject();
                if (!e) break;
                cJSON_AddNumberToObject(e, "id", (double)strtoll(r->rows[i][0], NULL, 10));
                cJSON_AddStringToObject(e, "path", r->rows[i][1]);
                cJSON_AddStringToObject(e, "error", m);
                cJSON_AddNumberToObject(e, "at", (double)at);
                cJSON_AddItemToArray(a, e);
            }
        }
        db_result_free(r);
        char *body = cJSON_PrintUnformatted(o);
        cJSON_Delete(o);
        return body ? api_ok(body) : api_error(500, "out of memory");
    }

    char *body = NULL;
    if (asprintf(&body, "{\"status\":\"ready\",\"version\":\"%s\"}", VERSION_STRING) < 0)
        return api_error(500, "out of memory");
    return api_ok(body);
}

int routes_register_health(api_server_t *srv, cutils_db_t *db)
{
    if (api_server_route(srv, "/healthz", API_GET, API_CLASS_NONE, handle_healthz, NULL) != 0) return -1;
    if (api_server_route(srv, "/readyz",  API_GET, API_CLASS_NONE, handle_readyz,  db)   != 0) return -1;
    return 0;
}
