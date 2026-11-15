#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "backup.h"
#include "job_manager.h"
#include "cJSON.h"
#include "lock.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

static void backup_opts_from_json(cutils_json_req_t *body, backup_job_t *j);

/*
 * Fall back to what the repository is configured to back up.
 *
 * A request that names paths is taken at its word - the CLI saying exactly
 * what it will read is the behaviour worth keeping. One that names none used
 * to be refused, which left the desktop button and every schedule with
 * nowhere to get them and is why neither worked.
 *
 * Excludes are only taken from configuration when the request supplied none
 * either. A request that listed excludes has said what it wants excluded,
 * and quietly adding more would mean a backup skipping files nobody asked it
 * to skip.
 */
static void backup_fill_from_config(backup_job_t *j)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)j->repo_id);

    db_result_t *r = NULL;
    if (db_execute(j->db,
            "SELECT kind, value FROM repo_backup_paths WHERE repo_id = ?"
            " ORDER BY kind, value",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return;
    }

    int want_paths = (j->n_paths == 0);
    int want_excl  = (j->n_excludes == 0);

    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        const char *kind = r->rows[i][0];
        const char *val  = r->rows[i][1];
        if (!val || !*val) continue;

        int is_excl = (kind && strcmp(kind, "exclude") == 0);
        if (is_excl && !want_excl) continue;
        if (!is_excl && !want_paths) continue;

        char *copy = strdup(val);
        if (!copy) break;

        char ***list = is_excl ? &j->excludes : &j->paths;
        int    *n    = is_excl ? &j->n_excludes : &j->n_paths;
        char  **grown = realloc(*list, (size_t)(*n + 1) * sizeof(**list));
        if (!grown) { free(copy); break; }
        *list = grown;
        (*list)[(*n)++] = copy;
    }
    db_result_free(r);
}

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
    st = repo_store_open(j->repo_path, NULL, &store);
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
    o.bytes_cb     = job_progress_bytes;

    /*
     * Say it again, every backup, until someone acts on it.
     *
     * A key that exists only on the machine being backed up makes every
     * offsite copy unreadable the moment that machine is lost, which is the
     * scenario the offsite copy was for. Mentioning that once at creation and
     * never again is how it gets forgotten, so this warns on each run and the
     * journal keeps the record.
     */
    {
        char rid[32];
        snprintf(rid, sizeof(rid), "%lld", (long long)j->repo_id);
        db_result_t *kr = NULL;
        if (db_execute(j->db,
                "SELECT encryption, key_escrowed FROM repos WHERE id = ?",
                (const char *[]){ rid, NULL }, &kr) == 0 && kr && kr->nrows == 1 &&
            strcmp(kr->rows[0][0], "none") != 0 &&
            strcmp(kr->rows[0][1], "0") == 0) {
            log_warn("repo %lld: encryption key has not been saved anywhere but "
                     "this host; run 'c-backup repo key export %lld' and store it, "
                     "then 'c-backup repo key ack %lld'",
                     (long long)j->repo_id, (long long)j->repo_id,
                     (long long)j->repo_id);
            journal_record(j->db, j->repo_id, "backup", "key-escrow", "warning", 0,
                           "encryption key not confirmed saved off this host");
        }
        db_result_free(kr);
    }

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

    /* What the backup did, on the job record: the log line had it, the job
     * did not, so a client saw only "completed". Every field is numeric. */
    if (res.snapshot_id > 0) {
        char rj[512];
        snprintf(rj, sizeof(rj),
                 "{\"snapshot_id\":%lld,\"files\":%llu,\"dirs\":%llu,"
                 "\"files_unchanged\":%llu,\"bytes_logical\":%llu,"
                 "\"objects_new\":%llu,\"bytes_stored\":%llu,"
                 "\"trees_new\":%llu,\"trees_reused\":%llu}",
                 (long long)res.snapshot_id,
                 (unsigned long long)res.files, (unsigned long long)res.dirs,
                 (unsigned long long)res.files_unchanged,
                 (unsigned long long)res.bytes_logical,
                 (unsigned long long)res.objects_new,
                 (unsigned long long)res.bytes_stored,
                 (unsigned long long)res.trees_new,
                 (unsigned long long)res.trees_reused);
        job_set_result(progress_ctx, rj);
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
        backup_opts_from_json(body, j);
    }

    backup_fill_from_config(j);

    if (j->n_paths == 0) {
        backup_job_free(j);
        return api_error(400,
                         "no source paths given and none configured; "
                         "set them with PUT /repos/<id>/sources");
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

/*
 * Fill a job from a request body.
 *
 * Shared with the scheduler, which submits the same job from the same shape
 * of JSON held in schedules.params. One parser rather than two: a schedule
 * that took a slightly different body from the route would be a second way
 * to describe a backup, and the two would drift.
 */
static void backup_opts_from_json(cutils_json_req_t *body, backup_job_t *j)
{
    {
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
}

/*
 * Submit a backup the scheduler asked for.
 *
 * A scheduled backup was accepted at creation and then failed on every tick
 * with "no scheduled job for action 'backup'", because the dispatch table
 * had no entry for it and nothing checked that at creation. The most
 * important thing a backup tool can schedule silently never ran, and the
 * schedule showed as enabled the whole time.
 *
 * The paths come from schedules.params, which is the same column and the
 * same shape the route's body uses.
 */
status_t routes_submit_backup(job_manager_t *jm, int64_t repo_id,
                              const char *params_json, int64_t *out_job_id)
{
    backup_ctx_t *ctx = &g_ctx;

    char *repo_path = repo_path_for(ctx->db, repo_id);
    if (!repo_path) return set_error(ERR_NOT_FOUND, "no such repo");

    backup_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return set_error(ERR_NOMEM, "backup job: alloc"); }
    j->db = ctx->db;
    j->repo_id = repo_id;
    j->repo_path = repo_path;

    if (params_json && *params_json) {
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        if (json_req_parse(params_json, strlen(params_json), &body) != 0) {
            backup_job_free(j);
            return set_error(ERR_INVALID, "backup schedule: parameters are not JSON");
        }
        backup_opts_from_json(body, j);
    }

    backup_fill_from_config(j);

    if (j->n_paths == 0) {
        backup_job_free(j);
        return set_error(ERR_INVALID,
                         "backup schedule for repo %lld has no source paths "
                         "and the repository has none configured",
                         (long long)repo_id);
    }

    return job_submit(jm ? jm : ctx->jm, repo_id, "backup", NULL,
                      backup_job_run, j, backup_job_free, out_job_id);
}

/* ---------- jobs ---------- */

/*
 * One job, as clients see it. Both /jobs and /jobs/<id> use this, so the
 * list carries the same detail as a single job rather than three fields.
 *
 * Numbers are JSON numbers. Times are epoch seconds; "started" and "ended"
 * are null until they happen. "elapsed_s" runs to now for a running job.
 * "rate_bytes_per_s" is the average over the job so far. "result" is what
 * the job reported about itself when it ended (a backup's snapshot and
 * counts, a restore's files and skips), or null.
 *
 * Built with cJSON rather than c-utils' builder because "result" is stored
 * JSON and has to be embedded as an object, not escaped into a string.
 */
#define JOB_COLUMNS \
    "SELECT id, COALESCE(repo_id, 0), action, state," \
    "       COALESCE(started, 0), COALESCE(ended, 0)," \
    "       COALESCE(NULLIF(progress_phase,''),'')," \
    "       CAST(COALESCE(NULLIF(progress_current,''),0) AS INTEGER)," \
    "       CAST(COALESCE(NULLIF(progress_total,''),0) AS INTEGER)," \
    "       CAST(COALESCE(NULLIF(bytes_done,''),0) AS INTEGER)," \
    "       CAST(COALESCE(NULLIF(bytes_total,''),0) AS INTEGER)," \
    "       COALESCE(NULLIF(error,''),'')," \
    "       COALESCE(result,'')" \
    "  FROM jobs"

static cJSON *job_json(char **row)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    long long started = strtoll(row[4], NULL, 10);
    long long ended   = strtoll(row[5], NULL, 10);
    unsigned long long bdone = strtoull(row[9], NULL, 10);
    long long now = (long long)time(NULL);
    long long elapsed = started > 0 ? ((ended > 0 ? ended : now) - started) : 0;
    if (elapsed < 0) elapsed = 0;

    cJSON_AddNumberToObject(o, "id",      (double)strtoll(row[0], NULL, 10));
    cJSON_AddNumberToObject(o, "repo_id", (double)strtoll(row[1], NULL, 10));
    cJSON_AddStringToObject(o, "action",  row[2]);
    cJSON_AddStringToObject(o, "state",   row[3]);
    if (started > 0) cJSON_AddNumberToObject(o, "started", (double)started);
    else             cJSON_AddNullToObject(o, "started");
    if (ended > 0)   cJSON_AddNumberToObject(o, "ended", (double)ended);
    else             cJSON_AddNullToObject(o, "ended");
    cJSON_AddNumberToObject(o, "elapsed_s", (double)elapsed);
    cJSON_AddStringToObject(o, "phase",   row[6]);
    cJSON_AddNumberToObject(o, "current", (double)strtoull(row[7], NULL, 10));
    cJSON_AddNumberToObject(o, "total",   (double)strtoull(row[8], NULL, 10));
    cJSON_AddNumberToObject(o, "bytes_done",  (double)bdone);
    cJSON_AddNumberToObject(o, "bytes_total", (double)strtoull(row[10], NULL, 10));
    cJSON_AddNumberToObject(o, "rate_bytes_per_s",
                            elapsed > 0 ? (double)(bdone / (unsigned long long)elapsed) : 0.0);
    cJSON_AddStringToObject(o, "error",   row[11]);
    cJSON *res = row[12][0] ? cJSON_Parse(row[12]) : NULL;
    if (res) cJSON_AddItemToObject(o, "result", res);
    else     cJSON_AddNullToObject(o, "result");
    return o;
}

static api_response_t json_out(cJSON *root)
{
    char *txt = root ? cJSON_PrintUnformatted(root) : NULL;
    cJSON_Delete(root);
    if (!txt) return api_error(500, "cannot serialise response");
    /* api_ok takes a malloc'd body and frees it; cJSON allocates with the
     * default hooks, which are malloc and free. */
    return api_ok(txt);
}

static api_response_t list_jobs(const api_request_t *req, void *ud)
{
    backup_ctx_t *ctx = ud;
    const char *limit = api_query_param(req, "limit");
    const char *lim = (limit && atoi(limit) > 0) ? limit : "50";

    db_result_t *r = NULL;
    if (db_execute(ctx->db, JOB_COLUMNS " ORDER BY id DESC LIMIT ?",
                   (const char *[]){ lim, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list jobs");
    }
    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "jobs") : NULL;
    for (int i = 0; arr && i < (r ? r->nrows : 0); i++) {
        cJSON *j = job_json(r->rows[i]);
        if (j) cJSON_AddItemToArray(arr, j);
    }
    db_result_free(r);
    return json_out(root);
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
    if (db_execute(ctx->db, JOB_COLUMNS " WHERE id = ?",
                   (const char *[]){ idbuf, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such job");
    }
    cJSON *j = job_json(r->rows[0]);
    db_result_free(r);
    return json_out(j);
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
