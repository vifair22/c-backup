#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "pack_presence.h"
#include "json_row.h"

#include "backup.h"
#include "file_cache.h"
#include "job_manager.h"
#include "snap_record.h"
/* c-utils fences its cJSON behind cutils/json.h; this file needs it raw. */
#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "repo.h"
#include "reserved.h"
#include "store.h"
#include "store_index.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <limits.h>
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

/* backup.file_cache_match, parsed once at start. */
static int g_file_cache_match = FILE_CACHE_MATCH_AUTO;

/* backup.store_workers, read once at start. */
static unsigned g_store_workers;

void routes_backup_set_store_workers(unsigned n)
{
    g_store_workers = n;
}

unsigned routes_backup_store_workers(void)
{
    return g_store_workers;
}

void routes_backup_set_file_cache_match(int mode)
{
    g_file_cache_match = mode;
}

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

static int backup_opts_from_json(cutils_json_req_t *body, backup_job_t *j,
                                 jreq_err_t *err);

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

/*
 * The skipped files, appended to the backup's result document as
 * ,"skipped":[{"path","reason"}...],"skipped_listed":N}. base is the
 * document so far without its closing brace, files_skipped included. The
 * list is the result's, already bounded by BACKUP_SKIPPED_LIST_MAX, so the
 * document never grows with the number skipped; files_skipped is the exact
 * count. Paths go through cJSON because, unlike every other field here,
 * they are not numbers. Returns a malloc'd document, or NULL.
 */
/* A JSON array, printed: the paths alone, or {"path"} objects. */
static char *paths_json(char *const *paths, uint32_t n, int as_objects)
{
    cJSON *arr = cJSON_CreateArray();
    if (!arr) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        cJSON *item;
        if (as_objects) {
            item = cJSON_CreateObject();
            if (item && !cJSON_AddStringToObject(item, "path", paths[i])) {
                cJSON_Delete(item);
                item = NULL;
            }
        } else {
            item = cJSON_CreateString(paths[i]);
        }
        if (!item) { cJSON_Delete(arr); return NULL; }
        cJSON_AddItemToArray(arr, item);
    }
    char *out = cJSON_PrintUnformatted(arr);
    cJSON_Delete(arr);
    return out;
}

static char *with_skipped(const char *base, const backup_result_t *res)
{
    /* Changed files (OWN-45) are stored, so they are paths alone; files
     * that never settled (OWN-45-LIVE) are {"path"} objects. */
    char *clist = paths_json(res->changed, res->changed_listed, 0);
    char *ulist = paths_json(res->unsettled, res->unsettled_listed, 1);

    cJSON *arr = cJSON_CreateArray();
    char *list = NULL;
    if (arr && clist && ulist) {
        int ok = 1;
        for (uint32_t i = 0; ok && i < res->skipped_listed; i++) {
            cJSON *o = cJSON_CreateObject();
            if (!o) { ok = 0; break; }
            cJSON_AddItemToArray(arr, o);
            if (!cJSON_AddStringToObject(o, "path", res->skipped[i].path) ||
                !cJSON_AddStringToObject(o, "reason", res->skipped[i].reason))
                ok = 0;
        }
        if (ok) list = cJSON_PrintUnformatted(arr);
    }
    cJSON_Delete(arr);

    char *js = NULL;
    if (list && clist && ulist) {
        size_t cap = strlen(base) + strlen(list) + strlen(clist) + strlen(ulist) + 256;
        js = malloc(cap);
        if (js)
            snprintf(js, cap, "%s,\"skipped\":%s,\"skipped_listed\":%u,"
                     "\"files_changed\":%llu,\"changed\":%s,\"changed_listed\":%u,"
                     "\"changed_unsettled\":%llu,\"unsettled\":%s,"
                     "\"unsettled_listed\":%u}",
                     base, list, res->skipped_listed,
                     (unsigned long long)res->files_changed, clist,
                     res->changed_listed,
                     (unsigned long long)res->changed_unsettled, ulist,
                     res->unsettled_listed);
    }
    cJSON_free(list);
    cJSON_free(clist);
    cJSON_free(ulist);
    return js;
}

/*
 * What the journal and the alert say about a backup that left files out
 * (B118) or kept files that never settled (OWN-45-LIVE). Empty when it did
 * neither: a clean backup journals no message and alerts nobody.
 */
void routes_backup_describe_doubts(const backup_result_t *res, char *summary,
                                   size_t ssz, char *warning, size_t wsz)
{
    char skip_s[PATH_MAX * 2 + 128] = "", skip_w[PATH_MAX * 2 + 160] = "";
    char uns_s[PATH_MAX + 128] = "", uns_w[PATH_MAX + 192] = "";
    if (res->files_skipped && res->skipped_listed) {
        snprintf(skip_s, sizeof(skip_s), "%llu skipped (not in the snapshot); first: %s: %s",
                 (unsigned long long)res->files_skipped,
                 res->skipped[0].path, res->skipped[0].reason);
        snprintf(skip_w, sizeof(skip_w), "%llu file(s) could not be read and are not "
                 "in snapshot %lld; first: %s: %s",
                 (unsigned long long)res->files_skipped, (long long)res->snapshot_id,
                 res->skipped[0].path, res->skipped[0].reason);
    } else if (res->files_skipped) {
        snprintf(skip_s, sizeof(skip_s), "%llu skipped (not in the snapshot); see the log",
                 (unsigned long long)res->files_skipped);
        snprintf(skip_w, sizeof(skip_w), "%llu file(s) could not be read and are not "
                 "in snapshot %lld; see the log",
                 (unsigned long long)res->files_skipped, (long long)res->snapshot_id);
    }
    if (res->changed_unsettled && res->unsettled_listed) {
        snprintf(uns_s, sizeof(uns_s), "%llu stored while still changing (may be "
                 "torn); first: %s",
                 (unsigned long long)res->changed_unsettled, res->unsettled[0]);
        snprintf(uns_w, sizeof(uns_w), "%llu file(s) were still changing and are in "
                 "snapshot %lld as read, flagged as possibly torn; first: %s",
                 (unsigned long long)res->changed_unsettled, (long long)res->snapshot_id,
                 res->unsettled[0]);
    } else if (res->changed_unsettled) {
        snprintf(uns_s, sizeof(uns_s), "%llu stored while still changing (may be "
                 "torn); see the log", (unsigned long long)res->changed_unsettled);
        snprintf(uns_w, sizeof(uns_w), "%llu file(s) were still changing and are in "
                 "snapshot %lld as read, flagged as possibly torn; see the log",
                 (unsigned long long)res->changed_unsettled, (long long)res->snapshot_id);
    }
    summary[0] = warning[0] = '\0';
    if (!skip_s[0] && !uns_s[0]) return;
    snprintf(summary, ssz, "snapshot %lld: %llu files, %s%s%s",
             (long long)res->snapshot_id, (unsigned long long)res->files,
             skip_s, skip_s[0] && uns_s[0] ? "; " : "", uns_s);
    snprintf(warning, wsz, "%s%s%s", skip_w, skip_w[0] && uns_w[0] ? "; " : "", uns_w);
}

static status_t backup_job_run(void *arg, atomic_int *cancel,
                               void (*progress)(const char *, uint64_t, uint64_t, void *),
                               void *progress_ctx)
{
    backup_job_t *j = arg;

    /* The job manager holds the repo write lock for the whole of this call
     * (job_submit takes JOB_LOCK_WRITE), so nothing here takes or releases
     * it. A request holding it at dispatch defers the job, never fails it. */
    store_t *store = NULL;
    store_index_t *idx = NULL;
    /* Before the open, which creates tmp/: a backup refused because the
     * repository is an empty mountpoint leaves nothing in it (NIT-38). */
    status_t st = pack_presence_require(j->db, j->repo_id);
    if (st != OK) return st;
    st = repo_store_open_keyed(j->repo_path, NULL, &store);
    if (st != OK) return st;

    st = store_index_bind(store, j->db, j->repo_id, &idx);
    if (st != OK) {
        store_close(store);
        return st;
    }
    /* So a cancel lands inside a file being read, not after it. */
    store_set_cancel(store, cancel);

    /* backup_opts_t takes const char **, and casting char ** straight to it
     * is the classic unsafe-intermediate-pointer conversion -Wcast-qual
     * warns about. A parallel const array costs one small allocation and
     * keeps the qualifier honest. */
    const char **cpaths = malloc((size_t)j->n_paths * sizeof(*cpaths));
    if (!cpaths) {
        store_index_free(idx);
        store_close(store);
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
    o.file_cache_match = g_file_cache_match;
    o.label        = j->label;
    o.cancel       = cancel;
    o.store_workers = g_store_workers;
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

    /* What the backup did, on the job record: the log line had it, the job
     * did not, so a client saw only "completed". Every field is numeric. */
    if (res.snapshot_id > 0) {
        const uint64_t *w = res.written.bytes;
        char rj[1024];
        snprintf(rj, sizeof(rj),
                 "{\"snapshot_id\":%lld,\"files\":%llu,\"dirs\":%llu,"
                 "\"files_unchanged\":%llu,\"bytes_logical\":%llu,"
                 "\"objects_new\":%llu,\"bytes_stored\":%llu,"
                 "\"trees_new\":%llu,\"trees_reused\":%llu,"
                 "\"store_workers\":%u,"
                 "\"bytes_written\":%llu,"
                 "\"bytes_written_pack\":%llu,\"bytes_written_pack_parity\":%llu,"
                 "\"files_skipped\":%llu",
                 (long long)res.snapshot_id,
                 (unsigned long long)res.files, (unsigned long long)res.dirs,
                 (unsigned long long)res.files_unchanged,
                 (unsigned long long)res.bytes_logical,
                 (unsigned long long)res.objects_new,
                 (unsigned long long)res.bytes_stored,
                 (unsigned long long)res.trees_new,
                 (unsigned long long)res.trees_reused,
                 res.store_workers,
                 (unsigned long long)res.written_total,
                 (unsigned long long)w[STORE_WR_PACK],
                 (unsigned long long)w[STORE_WR_PACK_PARITY],
                 (unsigned long long)res.files_skipped);
        char *full = with_skipped(rj, &res);
        if (full) {
            job_set_result(progress_ctx, full);
            free(full);
        } else {
            /* Out of memory for the list: the counts still go out, and the
             * log names every skipped file. */
            size_t n = strlen(rj);
            if (n + 1 < sizeof(rj)) { rj[n] = '}'; rj[n + 1] = '\0'; }
            job_set_result(progress_ctx, rj);
        }

        /* The journal is the permanent record, and a snapshot that lacks
         * files, or holds files that may be torn, has to say so there too,
         * not only in a job row deleted after 30 days. And alerted, through
         * the failure path at warning level: the job did what it could and
         * stays completed. A clean backup journals no message, as always. */
        char *summary = malloc(PATH_MAX * 4);
        char *warning = malloc(PATH_MAX * 4);
        if (summary && warning) {
            routes_backup_describe_doubts(&res, summary, PATH_MAX * 4,
                                          warning, PATH_MAX * 4);
            if (summary[0]) job_set_summary(progress_ctx, summary);
            if (warning[0]) job_set_warning(progress_ctx, warning);
        }
        free(summary);
        free(warning);
    }
    backup_result_free(&res);

    free(cpaths);
    free(cexcl);
    store_index_free(idx);
    store_close(store);
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

    /* The body first: what the client sent is its own to fix, and it hears
     * about that before anything about the repository's state (B112). */
    if (req->body && req->body_len) {
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        jreq_err_t pje;
        if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK) {
            backup_job_free(j);
            return api_error(400, pje.msg);
        }
        jreq_err_t je;
        if (backup_opts_from_json(body, j, &je) != 0) {
            backup_job_free(j);
            return api_error(400, je.msg);
        }
    }

    /* Said now, while the caller is listening, rather than by a job that
     * fails with "refusing to pack" once the whole tree has been read. */
    if (repo_require_key(j->repo_path) != OK) {
        backup_job_free(j);
        return api_error(409, err_msg());
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
static int backup_opts_from_json(cutils_json_req_t *body, backup_job_t *j,
                                 jreq_err_t *err)
{
    /*
     * Every field is read for its type. Each of these used to be dropped
     * when it was the wrong one: a non-string label stored no label, a path
     * that was not a string was skipped and the rest backed up, and
     * "one_filesystem":true - the spelling every other flag takes - was not
     * 0 or 1, so the backup crossed into every mount below the source.
     */
    if (jreq_str(body, "label", &j->label, err) == JREQ_BAD) return -1;
    /* Unchecked, a label went straight into the snapshot record: past about
     * 420 bytes the writer copied stack memory into the repository after it,
     * and a quote made a record no reindex could read (OWN-33). The same
     * rule reaches scheduled backups, whose parameters come through here. */
    if (snapshot_label_check(j->label) != 0) {
        snprintf(err->msg, sizeof(err->msg), "%s", SNAPSHOT_LABEL_RULE);
        return -1;
    }

    char **v = NULL;
    size_t n = 0;
    if (jreq_strv(body, "paths", (size_t)INT_MAX, &v, &n, err) == JREQ_BAD) return -1;
    for (size_t i = 0; i < n; i++) {
        if (!v[i][0]) {
            jreq_strv_free(v, n);
            snprintf(err->msg, sizeof(err->msg), "a backup path cannot be empty");
            return -1;
        }
        /* The daemon's reserved snapshot names (reserved.h): a 400 now,
         * rather than a job that fails in the engine's own check. */
        if (reserved_source_refused(v[i], err->msg, sizeof(err->msg)) != 0) {
            jreq_strv_free(v, n);
            return -1;
        }
    }
    j->paths = v;
    j->n_paths = (int)n;

    /*
     * Exclusions. The engine has always supported them and the daemon
     * never passed any, so through the API nothing could be excluded at
     * all: every backup took the whole tree, caches and build directories
     * included, however the request was written.
     */
    v = NULL;
    n = 0;
    if (jreq_strv(body, "exclude", (size_t)INT_MAX, &v, &n, err) == JREQ_BAD) return -1;
    for (size_t i = 0; i < n; i++) {
        if (!v[i][0]) {
            jreq_strv_free(v, n);
            snprintf(err->msg, sizeof(err->msg), "an exclude pattern cannot be empty");
            return -1;
        }
    }
    j->excludes = v;
    j->n_excludes = (int)n;

    /* Staying on one filesystem is the difference between backing up a
     * home directory and backing up every network mount under it. */
    if (jreq_flag(body, "one_filesystem", &j->one_filesystem) != 0) {
        snprintf(err->msg, sizeof(err->msg), "one_filesystem must be true or false");
        return -1;
    }
    return 0;
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
                              const char *params_json, int64_t schedule_id,
                              int64_t *out_job_id)
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
        jreq_err_t je;
        if (backup_opts_from_json(body, j, &je) != 0) {
            backup_job_free(j);
            return set_error(ERR_INVALID, "backup schedule: %s", je.msg);
        }
    }

    backup_fill_from_config(j);

    if (j->n_paths == 0) {
        backup_job_free(j);
        return set_error(ERR_INVALID,
                         "backup schedule for repo %lld has no source paths "
                         "and the repository has none configured",
                         (long long)repo_id);
    }

    return job_submit_scheduled(jm ? jm : ctx->jm, repo_id, "backup", NULL,
                                schedule_id, backup_job_run, j, backup_job_free,
                                out_job_id);
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
    "       COALESCE(result,'')," \
    "       COALESCE(origin,'api'), COALESCE(schedule_id, 0)" \
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
    /* Who asked: "api", "schedule" (and which), or "startup" for a job the
     * daemon queued itself at start (index_reconcile.h). */
    cJSON_AddStringToObject(o, "origin", row[13]);
    long long sched = strtoll(row[14], NULL, 10);
    if (sched > 0) cJSON_AddNumberToObject(o, "schedule_id", (double)sched);
    else           cJSON_AddNullToObject(o, "schedule_id");
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
    /* The shared paging rules: a malformed limit or offset is a 400. This
     * route used to fall back to 50 for anything it could not read, so
     * `limit=-1` or `limit=ten` answered as if it had been understood. */
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, 50, 1000, &limit, &offset, &page_err) != 0) return page_err;
    char lim[16], off[16];
    snprintf(lim, sizeof(lim), "%d", limit);
    snprintf(off, sizeof(off), "%d", offset);

    db_result_t *r = NULL;
    if (db_execute(ctx->db, JOB_COLUMNS " ORDER BY id DESC LIMIT ? OFFSET ?",
                   (const char *[]){ lim, off, NULL }, &r) != 0) {
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

/*
 * What each path under /jobs/ answers: /jobs/<id> is GET, /jobs/<id>/cancel
 * is POST, /jobs/stream/<id> is GET (its own route). A wrong method on one of
 * them is a 405 naming the right one, not the 404 or 400 the other method's
 * handler made of it (B085). NULL for anything else.
 */
static const char *jobs_allow_for_url(const char *url, void *ud)
{
    (void)ud;
    if (strncmp(url, "/jobs/", 6) != 0) return NULL;
    const char *p = url + 6;
    if (strncmp(p, "stream/", 7) == 0) p += 7;
    int64_t id = 0;
    const char *end = NULL;
    if (routes_path_id(p, 1, &id, &end) != 1) return NULL;
    if (*end == '\0') return "GET";
    if (p == url + 6 && strcmp(end, "/cancel") == 0) return "POST";
    return NULL;
}

/*
 * The job id in /jobs/<id>[/...], or -1 when that segment is not one. Such
 * a path was a 404, while /repos/-1 was a 400 "invalid repo id"; both are
 * a 400 now, in the same words (NIT-8).
 */
static int64_t job_path_id(const char *url)
{
    int64_t id = 0;
    return routes_path_id(url + strlen("/jobs/"), 1, &id, NULL) == 1 ? id : -1;
}

static api_response_t get_job(const api_request_t *req, void *ud)
{
    backup_ctx_t *ctx = ud;
    int64_t id = job_path_id(req->url);
    if (id < 0) return api_error(400, "invalid job id");
    const char *allow = jobs_allow_for_url(req->url, NULL);
    if (!allow) return api_error(404, "not found");
    if (strcmp(allow, "GET") != 0) return api_method_not_allowed(allow);

    char idbuf[32];
    snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
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
    int64_t id = job_path_id(req->url);
    if (id < 0) return api_error(400, "invalid job id");
    const char *allow = jobs_allow_for_url(req->url, NULL);
    if (!allow) return api_error(404, "not found");
    if (strcmp(allow, "POST") != 0) return api_method_not_allowed(allow);

    status_t st = job_cancel(ctx->jm, id);
    if (st != OK) return api_error(404, "job is not running");

    /*
     * The job record, as /jobs/<id> returns it, rather than an empty 204.
     * `c-backup jobs cancel` printed nothing at all, which reads as the
     * command having done nothing. Cancellation is cooperative, so the state
     * here is usually still "running": "cancel_requested" says the request
     * landed, and the job's own record says when it stops.
     */
    char idbuf[32];
    snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
    db_result_t *r = NULL;
    cJSON *j = NULL;
    if (db_execute(ctx->db, JOB_COLUMNS " WHERE id = ?",
                   (const char *[]){ idbuf, NULL }, &r) == 0 && r && r->nrows == 1)
        j = job_json(r->rows[0]);
    db_result_free(r);
    if (!j) j = cJSON_CreateObject();
    if (j) cJSON_AddBoolToObject(j, "cancel_requested", 1);
    return json_out(j);
}

int routes_register_jobs(api_server_t *srv, cutils_db_t *db, job_manager_t *jm)
{
    g_ctx.db = db;
    g_ctx.jm = jm;
    /* A cancel lost to a power cut finds its job interrupted by the cut
     * anyway, so it answers from its commit (B144). */
    if (api_server_route(srv, "/jobs",    API_GET,  API_CLASS_NONE,  list_jobs,     &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/jobs/*",  API_GET,  API_CLASS_NONE,  get_job,       &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/jobs/*",  API_POST, API_COMMIT_ONLY, cancel_job,    &g_ctx) != 0) return -1;
    if (api_server_route_allow(srv, "/jobs/*", jobs_allow_for_url, NULL) != 0) return -1;
    return 0;
}
