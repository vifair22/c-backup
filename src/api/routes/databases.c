#define _POSIX_C_SOURCE 200809L

/*
 * Database findings (docs/db-detection.md 7.4): the current view each
 * repository's backups and detection runs left in db_findings, read-only.
 *
 *   GET /repos/<id>/databases?scope=&engine=&coverage=&state=&limit=&offset=
 *   GET /repos/<id>/databases/<f>
 *
 * Every finding is answered with the fields a backup's job result gives
 * it, the evidence as the JSON object it is, and the history columns
 * (first_seen, last_seen, last_snap_id, alerted_at) and the decision
 * columns enabling and dismissing fill (template, params, decided_by,
 * decided_at, reason). One finding also carries its history: the journal
 * entries about it (B254). Run detection without a backup with
 * POST /repos/<id>/units/<p>/<u>/detect (units.c). The writes on findings
 * (preview, enable, dismiss) are D2's and live in their own route file.
 */

#include "routes.h"

#include "db_detect.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static cutils_db_t *g_db;

void routes_databases_init(cutils_db_t *db)
{
    g_db = db;
}

/* GET only: the table entries, matched by databases_path_entry. */
static const api_path_t k_db_paths[] = {
    /* path                    GET  POST  PUT  DELETE */
    { "/databases",              1,  0,    0,   0 },
    { "/databases/<id>",         1,  0,    0,   0 },
};

const api_path_t *routes_databases_paths(size_t *n)
{
    *n = sizeof(k_db_paths) / sizeof(k_db_paths[0]);
    return k_db_paths;
}

const api_path_t *databases_path_entry(const char *rest)
{
    if (strcmp(rest, "/databases") == 0) return &k_db_paths[0];
    if (strncmp(rest, "/databases/", 11) == 0) {
        int64_t id = 0;
        const char *end = NULL;
        if (routes_path_id(rest + 11, 1, &id, &end) == 1 && *end == '\0') return &k_db_paths[1];
    }
    return NULL;
}

#define FINDING_COLS \
    "id, scope, engine, COALESCE(version, ''), path, confidence, COALESCE(coverage, '')," \
    " COALESCE(coverage_why, ''), COALESCE(method, ''), COALESCE(suggestion, '')," \
    " COALESCE(optional, ''), state, evidence, first_seen, last_seen," \
    " COALESCE(last_snap_id, ''), COALESCE(alerted_at, ''), COALESCE(template, '')," \
    " COALESCE(params, ''), COALESCE(decided_by, ''), COALESCE(decided_at, '')," \
    " COALESCE(reason, '')"

/* c-utils reads a NULL column as "" (CLAUDE.md), and so do the COALESCEs
 * above: "" is null for every nullable column below. */
static void add_str_null(cJSON *o, const char *k, const char *v)
{
    if (v && *v) cJSON_AddStringToObject(o, k, v);
    else cJSON_AddNullToObject(o, k);
}

static void add_int_null(cJSON *o, const char *k, const char *v)
{
    if (v && *v) cJSON_AddNumberToObject(o, k, (double)strtoll(v, NULL, 10));
    else cJSON_AddNullToObject(o, k);
}

/* A stored JSON object as an object: the evidence and params are never
 * handed out as strings. One that does not parse as an object (it cannot,
 * from this daemon) is {} for evidence and null for params. */
static void add_object(cJSON *o, const char *k, const char *v, int null_if_bad)
{
    cJSON *x = v && *v ? cJSON_Parse(v) : NULL;
    if (x && cJSON_IsObject(x)) { cJSON_AddItemToObject(o, k, x); return; }
    cJSON_Delete(x);
    if (null_if_bad) cJSON_AddNullToObject(o, k);
    else cJSON_AddItemToObject(o, k, cJSON_CreateObject());
}

static cJSON *finding_json(char *const *row)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    cJSON_AddNumberToObject(o, "id", (double)strtoll(row[0], NULL, 10));
    cJSON_AddStringToObject(o, "scope", row[1]);
    cJSON_AddStringToObject(o, "engine", row[2]);
    add_str_null(o, "version", row[3]);
    cJSON_AddStringToObject(o, "path", row[4]);
    cJSON_AddStringToObject(o, "confidence", row[5]);
    add_str_null(o, "coverage", row[6]);
    add_str_null(o, "coverage_why", row[7]);
    add_str_null(o, "method", row[8]);
    add_str_null(o, "suggestion", row[9]);
    add_str_null(o, "optional", row[10]);
    cJSON_AddStringToObject(o, "state", row[11]);
    add_object(o, "evidence", row[12], 0);
    cJSON_AddNumberToObject(o, "first_seen", (double)strtoll(row[13], NULL, 10));
    cJSON_AddNumberToObject(o, "last_seen", (double)strtoll(row[14], NULL, 10));
    add_int_null(o, "last_snap_id", row[15]);
    add_int_null(o, "alerted_at", row[16]);
    add_str_null(o, "template", row[17]);
    add_object(o, "params", row[18], 1);
    add_str_null(o, "decided_by", row[19]);
    add_int_null(o, "decided_at", row[20]);
    add_str_null(o, "reason", row[21]);
    return o;
}

static api_response_t json_out(cJSON *root)
{
    char *txt = root ? cJSON_PrintUnformatted(root) : NULL;
    cJSON_Delete(root);
    if (!txt) return api_error(500, "cannot serialise response");
    return api_ok(txt);
}

static int state_known(const char *s)
{
    return strcmp(s, "new") == 0 || strcmp(s, "suggested") == 0 ||
           strcmp(s, "enabled") == 0 || strcmp(s, "dismissed") == 0;
}

/*
 * The filters, each exact and each refused with 400 when it names a value
 * outside its set (docs/api.md: a filter with a fixed set of values is
 * never answered unfiltered). An empty value is no filter.
 */
static int read_filters(const api_request_t *req, const char **scope, const char **engine,
                        const char **coverage, const char **state, api_response_t *bad)
{
    *scope = api_query_param(req, "scope");
    *engine = api_query_param(req, "engine");
    *coverage = api_query_param(req, "coverage");
    *state = api_query_param(req, "state");
    if (!*scope) *scope = "";
    if (!*engine) *engine = "";
    if (!*coverage) *coverage = "";
    if (!*state) *state = "";
    if (strlen(*scope) > 4200) { *bad = api_error(400, "scope is too long"); return -1; }
    if (**engine && db_engine_parse(*engine) == DB_ENGINE_NONE) {
        *bad = api_error(400, "engine must be one of postgres, mysql, sqlite, redis, prometheus, "
                              "influxdb1, influxdb2, mongodb, elasticsearch, leveldb, rocksdb, "
                              "lmdb, bbolt, clickhouse, nextcloud");
        return -1;
    }
    db_coverage_t cv;
    if (**coverage && strcmp(*coverage, "none") != 0 && db_coverage_parse(*coverage, &cv) != 0) {
        *bad = api_error(400, "coverage must be one of covered-crash, covered-quiesced, "
                              "covered-application, engine-unsafe, split, uncovered, none");
        return -1;
    }
    if (**state && !state_known(*state)) {
        *bad = api_error(400, "state must be one of new, suggested, enabled, dismissed");
        return -1;
    }
    return 0;
}

#define FILTER_SQL \
    " WHERE repo_id = ?1 AND (?2 = '' OR scope = ?2) AND (?3 = '' OR engine = ?3)" \
    " AND (?4 = '' OR (?4 = 'none' AND coverage IS NULL) OR coverage = ?4)" \
    " AND (?5 = '' OR state = ?5)"

static api_response_t db_list(const api_request_t *req, int64_t repo_id)
{
    int limit = 0, offset = 0;
    api_response_t bad;
    if (routes_paging(req, 100, 1000, &limit, &offset, &bad) != 0) return bad;
    const char *scope, *engine, *coverage, *state;
    if (read_filters(req, &scope, &engine, &coverage, &state, &bad) != 0) return bad;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim, sizeof(lim), "%d", limit);
    snprintf(off, sizeof(off), "%d", offset);

    db_result_t *c = NULL;
    if (db_execute(g_db, "SELECT COUNT(*) FROM db_findings" FILTER_SQL,
                   (const char *[]){ repo, scope, engine, coverage, state, NULL }, &c) != 0 ||
        !c || c->nrows != 1) {
        db_result_free(c);
        return api_error(500, "cannot count database findings");
    }
    double total = (double)strtoll(c->rows[0][0], NULL, 10);
    db_result_free(c);

    /* Ordered to a unique key, so paging visits every row once. */
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT " FINDING_COLS " FROM db_findings" FILTER_SQL
                         " ORDER BY scope, path, engine, id LIMIT ?6 OFFSET ?7",
                   (const char *[]){ repo, scope, engine, coverage, state, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list database findings");
    }
    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "findings") : NULL;
    for (int i = 0; arr && r && i < r->nrows; i++) {
        cJSON *o = finding_json(r->rows[i]);
        if (!o) { cJSON_Delete(root); root = NULL; break; }
        cJSON_AddItemToArray(arr, o);
    }
    db_result_free(r);
    if (root) cJSON_AddNumberToObject(root, "total", total);
    return json_out(root);
}

/*
 * A finding's history (7.4, B254): the journal entries about it, newest
 * first, at most FINDING_HISTORY_MAX. Every event names its finding the
 * same two ways: detection's "... (finding N)" (found, coverage changed,
 * and the one alert, which comes through the job's warning) and the
 * decisions' "finding N (...". The journal is the permanent record, so the
 * history is read from it rather than kept twice; nothing before the
 * finding was first seen can be about it, which bounds the read to the
 * (repo_id, ts) index.
 */
#define FINDING_HISTORY_MAX 50

static int add_history(cJSON *o, const char *repo, const char *id, const char *first_seen)
{
    char in_text[48], leading[48], lim[16];
    snprintf(in_text, sizeof(in_text), "%%(finding %s)%%", id);
    snprintf(leading, sizeof(leading), "finding %s (%%", id);
    snprintf(lim, sizeof(lim), "%d", FINDING_HISTORY_MAX + 1);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT id, ts, source, operation, result, COALESCE(message, '') FROM journal"
                         " WHERE repo_id = ? AND ts >= ? AND (message LIKE ? OR message LIKE ?)"
                         " ORDER BY id DESC LIMIT ?",
                   (const char *[]){ repo, first_seen, in_text, leading, lim, NULL }, &r) != 0) {
        db_result_free(r);
        return -1;
    }
    cJSON *h = cJSON_AddArrayToObject(o, "history");
    int n = r ? r->nrows : 0;
    for (int i = 0; h && i < n && i < FINDING_HISTORY_MAX; i++) {
        char *const *row = r->rows[i];
        cJSON *e = cJSON_CreateObject();
        if (!e) break;
        cJSON_AddNumberToObject(e, "journal_id", (double)strtoll(row[0], NULL, 10));
        cJSON_AddNumberToObject(e, "ts", (double)strtoll(row[1], NULL, 10));
        cJSON_AddStringToObject(e, "source", row[2]);
        cJSON_AddStringToObject(e, "operation", row[3]);
        cJSON_AddStringToObject(e, "result", row[4]);
        cJSON_AddStringToObject(e, "message", row[5]);
        cJSON_AddItemToArray(h, e);
    }
    cJSON_AddBoolToObject(o, "history_truncated", n > FINDING_HISTORY_MAX);
    db_result_free(r);
    return h ? 0 : -1;
}

static api_response_t db_one(int64_t repo_id, int64_t fid)
{
    char repo[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)fid);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT " FINDING_COLS " FROM db_findings WHERE repo_id = ? AND id = ?",
                   (const char *[]){ repo, id, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read the database finding");
    }
    if (!r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such database finding in this repository");
    }
    cJSON *o = finding_json(r->rows[0]);
    char first[32];
    snprintf(first, sizeof(first), "%s", r->rows[0][13]);
    db_result_free(r);
    if (o && add_history(o, repo, id, first) != 0) {
        cJSON_Delete(o);
        return api_error(500, "cannot read the finding's history");
    }
    return json_out(o);
}

api_response_t databases_route_get(const api_request_t *req, int64_t repo_id, const char *rest)
{
    if (strcmp(rest, "/databases") == 0) return db_list(req, repo_id);
    int64_t fid = 0;
    const char *end = NULL;
    if (strncmp(rest, "/databases/", 11) == 0 && routes_path_id(rest + 11, 1, &fid, &end) == 1 &&
        *end == '\0')
        return db_one(repo_id, fid);
    return api_error(404, "not found");
}
