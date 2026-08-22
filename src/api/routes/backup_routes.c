#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "backup.h"
#include "job_manager.h"
#include "lock.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Backup and job routes.
 *
 * A backup is submitted, not performed inline: it can run for hours, and an
 * HTTP request that lives that long is a request that dies to any timeout in
 * the path. The response carries a job id and the client polls or streams.
 */

typedef struct {
    cutils_db_t   *db;
    job_manager_t *jm;
} backup_ctx_t;

static backup_ctx_t g_ctx;

/* Everything one backup job needs, owned by the job manager for its lifetime. */
typedef struct {
    cutils_db_t *db;
    int64_t      repo_id;
    char        *repo_path;
    char        *label;
    char       **paths;
    int          n_paths;
} backup_job_t;

static void backup_job_free(void *p)
{
    backup_job_t *j = p;
    if (!j) return;
    for (int i = 0; i < j->n_paths; i++) free(j->paths[i]);
    free(j->paths);
    free(j->repo_path);
    free(j->label);
    free(j);
}

static status_t backup_job_run(void *arg, volatile int *cancel,
                               void (*progress)(const char *, uint64_t, uint64_t, void *),
                               void *progress_ctx)
{
    backup_job_t *j = arg;

    /* One writer per repo. Rejecting rather than queueing means a client that
     * fires two backups at the same repo learns immediately. */
    status_t st = repo_trylock_write(j->repo_id);
    if (st != OK) return st;

    store_t *store = NULL;
    store_index_t *idx = NULL;
    st = store_open(j->repo_path, NULL, &store);
    if (st != OK) { repo_unlock(j->repo_id); return st; }

    st = store_index_bind(store, j->db, j->repo_id, &idx);
    if (st != OK) {
        store_close(store);
        repo_unlock(j->repo_id);
        return st;
    }

    /* backup_opts_t takes const char **, and casting char ** straight to it
     * is the classic unsafe-intermediate-pointer conversion -Wcast-qual
     * warns about. A parallel const array costs one small allocation and
     * keeps the qualifier honest. */
    const char **cpaths = malloc((size_t)j->n_paths * sizeof(*cpaths));
    if (!cpaths) {
        store_index_free(idx);
        store_close(store);
        repo_unlock(j->repo_id);
        return set_error(ERR_NOMEM, "backup job: paths");
    }
    for (int i = 0; i < j->n_paths; i++) cpaths[i] = j->paths[i];

    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths        = cpaths;
    o.n_paths      = j->n_paths;
    o.label        = j->label;
    o.cancel       = cancel;
    o.progress_cb  = progress;
    o.progress_ctx = progress_ctx;

    backup_result_t res;
    st = backup_run(store, j->db, j->repo_id, &o, &res);

    free(cpaths);
    store_index_free(idx);
    store_close(store);
    repo_unlock(j->repo_id);
    return st;
}

/* Look up a repo's path, which is also how we check that it exists. */
static char *repo_path_for(cutils_db_t *db, int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return NULL;
    }
    char *path = strdup(r->rows[0][0]);
    db_result_free(r);
    return path;
}

api_response_t backup_route_post(const api_request_t *req, int64_t repo_id,
                                 const char *rest)
{
    backup_ctx_t *ctx = &g_ctx;
    if (strcmp(rest, "/backup") != 0) return api_error(404, "not found");

    char *repo_path = repo_path_for(ctx->db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

    backup_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return api_error(500, "out of memory"); }
    j->db = ctx->db;
    j->repo_id = repo_id;
    j->repo_path = repo_path;

    if (req->body && req->body_len) {
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        if (json_req_parse(req->body, req->body_len, &body) != 0) {
            backup_job_free(j);
            return api_error(400, "invalid JSON");
        }
        if (json_req_get_str_opt(body, "label", &j->label) != 0) j->label = NULL;

        CUTILS_AUTO_JSON_ITER cutils_json_iter_t it;
        if (json_iter_begin(body, "paths", &it) == 0) {
            while (json_iter_next(&it)) {
                char *p = NULL;
                /* Empty path, not NULL: paths is an array of bare strings,
                 * and the iterator resolves "" to the element itself while
                 * NULL means "no such field". */
                if (json_iter_get_str(&it, "", &p) != 0 || !p) continue;
                char **grown = realloc(j->paths, (size_t)(j->n_paths + 1) * sizeof(*grown));
                if (!grown) { free(p); break; }
                j->paths = grown;
                j->paths[j->n_paths++] = p;
            }
        }
    }

    if (j->n_paths == 0) {
        backup_job_free(j);
        return api_error(400, "at least one source path is required");
    }

    int64_t job_id = 0;
    status_t st = job_submit(ctx->jm, repo_id, "backup", NULL,
                             backup_job_run, j, backup_job_free, &job_id);
    if (st != OK) {
        return api_error(st == ERR_BUSY ? 503 : 500, err_msg());
    }

    char *out = NULL;
    if (asprintf(&out, "{\"job_id\":%lld}", (long long)job_id) < 0)
        return api_error(500, "out of memory");
    return api_ok_status(202, out);
}

/* ---------- jobs ---------- */

static api_response_t list_jobs(const api_request_t *req, void *ud)
{
    backup_ctx_t *ctx = ud;
    const char *limit = api_query_param(req, "limit");
    const char *lim = (limit && atoi(limit) > 0) ? limit : "50";

    db_result_t *r = NULL;
    if (db_execute(ctx->db,
            "SELECT id, repo_id, action, state, started, ended,"
            "       progress_phase, progress_current, progress_total, error"
            "  FROM jobs ORDER BY id DESC LIMIT ?",
            (const char *[]){ lim, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list jobs");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }

    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "jobs", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",     r->rows[i][0]) != 0
               || json_elem_add_str(&e, "action", r->rows[i][2]) != 0
               || json_elem_add_str(&e, "state",  r->rows[i][3]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

static api_response_t get_job(const api_request_t *req, void *ud)
{
    backup_ctx_t *ctx = ud;
    const char *tail = req->url + strlen("/jobs/");
    char *end = NULL;
    long long id = strtoll(tail, &end, 10);
    if (id <= 0) return api_error(400, "invalid job id");

    /* /jobs/<id>/cancel is a POST; a GET on it is not a job lookup. */
    if (end && *end != '\0') return api_error(404, "not found");

    char idbuf[32];
    snprintf(idbuf, sizeof(idbuf), "%lld", id);
    db_result_t *r = NULL;
    if (db_execute(ctx->db,
            "SELECT id, repo_id, action, state, started, ended,"
            "       progress_phase, progress_current, progress_total, error"
            "  FROM jobs WHERE id = ?",
            (const char *[]){ idbuf, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such job");
    }

    char *body = NULL;
    int n = asprintf(&body,
        "{\"id\":%s,\"action\":\"%s\",\"state\":\"%s\","
        "\"phase\":\"%s\",\"current\":%s,\"total\":%s,\"error\":\"%s\"}",
        r->rows[0][0], r->rows[0][2], r->rows[0][3],
        r->rows[0][6] ? r->rows[0][6] : "",
        r->rows[0][7] ? r->rows[0][7] : "0",
        r->rows[0][8] ? r->rows[0][8] : "0",
        r->rows[0][9] ? r->rows[0][9] : "");
    db_result_free(r);
    if (n < 0) return api_error(500, "out of memory");
    return api_ok(body);
}

static api_response_t cancel_job(const api_request_t *req, void *ud)
{
    backup_ctx_t *ctx = ud;
    const char *tail = req->url + strlen("/jobs/");
    char *end = NULL;
    long long id = strtoll(tail, &end, 10);
    if (id <= 0 || !end || strcmp(end, "/cancel") != 0)
        return api_error(400, "invalid job id");

    status_t st = job_cancel(ctx->jm, id);
    if (st != OK) return api_error(404, "job is not running");
    return api_no_content();
}

int routes_register_jobs(api_server_t *srv, cutils_db_t *db, job_manager_t *jm)
{
    g_ctx.db = db;
    g_ctx.jm = jm;
    if (api_server_route(srv, "/jobs",    API_GET,  list_jobs,     &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/jobs/*",  API_GET,  get_job,       &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/jobs/*",  API_POST, cancel_job,    &g_ctx) != 0) return -1;
    return 0;
}
