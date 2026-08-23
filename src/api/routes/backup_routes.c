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
    char       **excludes;
    int          n_excludes;
    int          one_filesystem;
} backup_job_t;

static void backup_job_free(void *p)
{
    backup_job_t *j = p;
    if (!j) return;
    for (int i = 0; i < j->n_paths; i++) free(j->paths[i]);
    free(j->paths);
    for (int i = 0; i < j->n_excludes; i++) free(j->excludes[i]);
    free(j->excludes);
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

    const char **cexcl = NULL;
    if (j->n_excludes > 0) {
        cexcl = malloc((size_t)j->n_excludes * sizeof(*cexcl));
        if (!cexcl) {
            free(cpaths);
            store_index_free(idx);
            store_close(store);
            repo_unlock(j->repo_id);
            return set_error(ERR_NOMEM, "backup job: excludes");
        }
        for (int i = 0; i < j->n_excludes; i++) cexcl[i] = j->excludes[i];
    }

    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths          = cpaths;
    o.n_paths        = j->n_paths;
    o.exclude        = cexcl;
    o.n_exclude      = j->n_excludes;
    o.one_filesystem = j->one_filesystem;
    o.label        = j->label;
    o.cancel       = cancel;
    o.progress_cb  = progress;
    o.progress_ctx = progress_ctx;

    backup_result_t res;
    memset(&res, 0, sizeof(res));
    st = backup_run(store, j->db, j->repo_id, &o, &res);

    /*
     * Packing finishes the backup rather than waiting for someone to ask.
     *
     * Compression only happens at pack time, so a backup that stops here has
     * written more bytes than it read and left one file per object behind.
     * Treating packing as a separate operation meant the common case - back
     * up and walk away - produced a repository that never compressed anything
     * at all. It runs inside this job, under the same write lock, so nothing
     * can interleave between the snapshot landing and its objects being
     * consolidated.
     *
     * A pack failure does not fail the backup: the snapshot is committed and
     * its objects are readable loose. It is logged and left for the next run
     * or an explicit pack.
     */
    if (st == OK && routes_policy_flag(j->db, j->repo_id, "maintenance.auto_pack", 1)) {
        status_t pst = routes_run_pack(store, j->repo_id, cancel, progress, progress_ctx);
        if (pst != OK) {
            log_warn("backup: snapshot %lld committed but packing failed: %s",
                     (long long)res.snapshot_id, err_msg());
            err_clear();
        }
    }

    free(cpaths);
    free(cexcl);
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

        /*
         * Exclusions. The engine has always supported them and the daemon
         * never passed any, so through the API nothing could be excluded at
         * all: every backup took the whole tree, caches and build
         * directories included, however the request was written.
         */
        CUTILS_AUTO_JSON_ITER cutils_json_iter_t ex;
        if (json_iter_begin(body, "exclude", &ex) == 0) {
            while (json_iter_next(&ex)) {
                char *p = NULL;
                if (json_iter_get_str(&ex, "", &p) != 0 || !p) continue;
                if (!*p) { free(p); continue; }
                char **grown = realloc(j->excludes,
                                       (size_t)(j->n_excludes + 1) * sizeof(*grown));
                if (!grown) { free(p); break; }
                j->excludes = grown;
                j->excludes[j->n_excludes++] = p;
            }
        }

        /* Staying on one filesystem is the difference between backing up a
         * home directory and backing up every network mount under it. */
        uint32_t onefs = 0;
        if (json_req_get_u32(body, "one_filesystem", &onefs, 0, 1) == 0)
            j->one_filesystem = (int)onefs;
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

    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "jobs") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
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
    /*
     * COALESCE rather than a NULL check in C: this db layer hands back an
     * empty string for a NULL column, not NULL, so the guard never fired and
     * a job that had not reported progress serialised as "current":, which is
     * not JSON at all. Every gc, gfs and reindex job hit it, because those
     * report no progress.
     */
    if (db_execute(ctx->db,
            "SELECT id, action, state,"
            "       COALESCE(NULLIF(progress_phase,''),''),"
            "       CAST(COALESCE(NULLIF(progress_current,''),0) AS INTEGER),"
            "       CAST(COALESCE(NULLIF(progress_total,''),0) AS INTEGER),"
            "       COALESCE(NULLIF(error,''),'')"
            "  FROM jobs WHERE id = ?",
            (const char *[]){ idbuf, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such job");
    }

    /* Built rather than printed: phase and error carry paths and strerror
     * text, and one quote or backslash in either would produce a body no
     * client can parse. */
    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    int bad = json_resp_add_str(resp, "id",      r->rows[0][0]) != 0
           || json_resp_add_str(resp, "action",  r->rows[0][1]) != 0
           || json_resp_add_str(resp, "state",   r->rows[0][2]) != 0
           || json_resp_add_str(resp, "phase",   r->rows[0][3]) != 0
           || json_resp_add_u64(resp, "current", strtoull(r->rows[0][4], NULL, 10)) != 0
           || json_resp_add_u64(resp, "total",   strtoull(r->rows[0][5], NULL, 10)) != 0
           || json_resp_add_str(resp, "error",   r->rows[0][6]) != 0;
    db_result_free(r);
    if (bad) return api_error(500, "cannot serialise response");

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
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
