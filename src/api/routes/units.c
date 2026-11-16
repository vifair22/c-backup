#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

/*
 * Unit routes (docs/source-providers.md section 10), the part that works
 * without a real provider:
 *
 *   GET  /providers                                   compiled-in providers, enabled or not
 *   GET  /providers/<p>/units                         live enumeration, by name, paginated
 *   GET  /providers/<p>/units/<u>                     describe, read-only
 *   GET  /repos/<id>/units                            units with history here, by name
 *   GET  /repos/<id>/units/<p>/<u>/snapshots          that unit's snapshots, newest first
 *   GET  /repos/<id>/units/<p>/<u>/policy             per-unit retention
 *   PUT  /repos/<id>/units/<p>/<u>/policy
 *   POST /repos/<id>/units/backup                     a job
 *   POST /repos/<id>/snapshots/<sid>/unit/restore     a job: files or one item, raw
 *
 * Every unit a request names is held to units.allow, enumeration included:
 * a unit outside it is not listed and cannot be described.
 */

#include "routes.h"
#include "json_row.h"

#include "db_txn.h"
#include "job_manager.h"
#include "registry.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "unit_backup.h"
#include "unit_index.h"
#include "unit_restore.h"
#include "unit_state.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <fnmatch.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static cutils_db_t   *g_db;
static job_manager_t *g_jm;

void routes_units_init(cutils_db_t *db, struct job_manager *jm)
{
    g_db = db;
    g_jm = jm;
}

static api_response_t json_out(cJSON *root)
{
    char *txt = root ? cJSON_PrintUnformatted(root) : NULL;
    cJSON_Delete(root);
    if (!txt) return api_error(500, "cannot serialise response");
    return api_ok(txt);
}

static char *repo_path_of(int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    char *p = NULL;
    if (db_execute(g_db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1)
        p = strdup(r->rows[0][0]);
    db_result_free(r);
    return p;
}

/* ------------------------------------------------------------------ */
/* Policy                                                              */
/* ------------------------------------------------------------------ */

static int *ret_field(gfs_retention_t *r, const char *k)
{
    if (strcmp(k, "retention.keep_snaps") == 0)   return &r->keep_snaps;
    if (strcmp(k, "retention.keep_daily") == 0)   return &r->keep_daily;
    if (strcmp(k, "retention.keep_weekly") == 0)  return &r->keep_weekly;
    if (strcmp(k, "retention.keep_monthly") == 0) return &r->keep_monthly;
    if (strcmp(k, "retention.keep_yearly") == 0)  return &r->keep_yearly;
    return NULL;
}

void routes_unit_policy(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                        gfs_retention_t *ret)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT key, value FROM unit_policy WHERE repo_id = ? AND unit_key = ?",
                   (const char *[]){ repo, unit_key, NULL }, &r) == 0 && r) {
        for (int i = 0; i < r->nrows; i++) {
            int v = 0;
            int *f = ret_field(ret, r->rows[i][0]);
            if (!f) continue;
            if (routes_policy_count(r->rows[i][1], &v) != 0) {
                log_warn("gfs: repo %lld unit %s: ignoring invalid %s = '%s'",
                         (long long)repo_id, unit_key, r->rows[i][0], r->rows[i][1]);
                continue;
            }
            *f = v;
        }
    }
    db_result_free(r);
}

/* ------------------------------------------------------------------ */
/* /providers                                                          */
/* ------------------------------------------------------------------ */

static api_response_t providers_list(void)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "providers") : NULL;
    const sp_provider_ops_t *const *all = NULL;
    size_t n = sp_registry_compiled(&all);
    for (size_t i = 0; arr && i < n; i++) {
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(arr, o);
        cJSON_AddStringToObject(o, "name", all[i]->name);
        const sp_provider_ops_t *ops = NULL;
        sp_ctx_t *ctx = NULL;
        int on = sp_registry_get(all[i]->name, &ops, &ctx) == OK;
        err_clear();
        cJSON_AddBoolToObject(o, "enabled", on);
        if (!on || !ops) continue;
        sp_probe_t pr;
        if (ops->probe && ops->probe(ctx, &pr) == OK) {
            cJSON *p = cJSON_AddObjectToObject(o, "probe");
            if (p) {
                cJSON_AddBoolToObject(p, "available", pr.available);
                cJSON_AddStringToObject(p, "version", pr.version);
                cJSON_AddStringToObject(p, "detail", pr.detail);
            }
        } else {
            cJSON_AddStringToObject(o, "probe_error", err_msg());
            err_clear();
        }
    }
    if (root) {
        cJSON_AddStringToObject(root, "daemon_id", units_daemon8());
        cJSON_AddStringToObject(root, "allow", units_config()->allow ? units_config()->allow : "");
        cJSON_AddNumberToObject(root, "holds_open", (double)units_intents_open(g_db, NULL));
    }
    return json_out(root);
}

typedef struct {
    const char *provider;
    cJSON      *arr;
    int         skip, take, total;
} enum_ctx_t;

static status_t enum_cb(void *ctx, const char *id, const char *name, int state)
{
    enum_ctx_t *e = ctx;
    if (!units_allowed(e->provider, id, name)) return OK;
    int at = e->total++;
    if (at < e->skip || at >= e->skip + e->take) return OK;
    cJSON *o = cJSON_CreateObject();
    if (!o) return set_error(ERR_NOMEM, "units: list");
    cJSON_AddItemToArray(e->arr, o);
    char key[UNIT_KEY_MAX];
    unit_key_format(key, sizeof(key), e->provider, id);
    cJSON_AddStringToObject(o, "unit_key", key);
    cJSON_AddStringToObject(o, "unit_id", id);
    cJSON_AddStringToObject(o, "name", name);
    cJSON_AddStringToObject(o, "state", sp_state_name(state));
    return OK;
}

static api_response_t provider_units(const api_request_t *req, const char *prov)
{
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    if (sp_registry_get(prov, &ops, &ctx) != OK) return api_error(404, err_msg());
    int limit = 0, offset = 0;
    api_response_t bad;
    if (routes_paging(req, 100, 1000, &limit, &offset, &bad) != 0) return bad;
    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "units") : NULL;
    if (!arr) { cJSON_Delete(root); return api_error(500, "out of memory"); }
    enum_ctx_t e = { ops->name, arr, offset, limit, 0 };
    if (ops->list_units(ctx, enum_cb, &e) != OK) {
        cJSON_Delete(root);
        return api_error(502, err_msg());
    }
    cJSON_AddNumberToObject(root, "total", e.total);
    return json_out(root);
}

static cJSON *desc_json(const sp_unit_desc_t *d)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    char key[UNIT_KEY_MAX];
    unit_key_format(key, sizeof(key), d->provider, d->unit_id);
    cJSON_AddStringToObject(o, "unit_key", key);
    cJSON_AddStringToObject(o, "provider", d->provider);
    cJSON_AddStringToObject(o, "unit_id", d->unit_id);
    cJSON_AddStringToObject(o, "name", d->name ? d->name : "");
    cJSON_AddStringToObject(o, "state", sp_state_name(d->state));
    cJSON *caps = cJSON_AddArrayToObject(o, "caps");
    static const struct { uint32_t bit; const char *name; } k[] = {
        { SP_CAP_QUIESCE, "quiesce" }, { SP_CAP_PAUSE, "pause" },
        { SP_CAP_SNAPSHOT, "snapshot" }, { SP_CAP_INCREMENTAL, "incremental" },
        { SP_CAP_DEFINE, "define" }, { SP_CAP_PARALLEL, "parallel" },
    };
    for (size_t i = 0; caps && i < sizeof(k) / sizeof(k[0]); i++)
        if (d->caps & k[i].bit) cJSON_AddItemToArray(caps, cJSON_CreateString(k[i].name));
    cJSON *items = cJSON_AddArrayToObject(o, "items");
    for (size_t i = 0; items && i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        cJSON *j = cJSON_CreateObject();
        if (!j) break;
        cJSON_AddItemToArray(items, j);
        cJSON_AddStringToObject(j, "id", it->id);
        cJSON_AddStringToObject(j, "kind", sp_item_kind_name(it->kind));
        cJSON_AddNumberToObject(j, "size", (double)it->size);
        if (it->format[0]) cJSON_AddStringToObject(j, "format", it->format);
        if (it->source) cJSON_AddStringToObject(j, "source", it->source);
        cJSON_AddStringToObject(j, "path", it->tree_path ? it->tree_path : "");
    }
    return o;
}

/* A unit by name or id, held to units.allow; ERR_NOT_FOUND either way so
 * the allow list does not leak what lies outside it. */
typedef struct { const char *want; char id[SP_UNIT_ID_MAX]; int found; } find_t;

static status_t find_cb(void *ctx, const char *id, const char *name, int state)
{
    (void)state;
    find_t *f = ctx;
    if (!f->found && (strcmp(name, f->want) == 0 || strcmp(id, f->want) == 0)) {
        snprintf(f->id, sizeof(f->id), "%s", id);
        f->found = 1;
    }
    return OK;
}

static status_t resolve_unit(const sp_provider_ops_t *ops, sp_ctx_t *ctx,
                             const char *want, sp_unit_desc_t **out)
{
    if (!ops || !ops->list_units) return set_error(ERR_INVALID, "provider cannot list units");
    find_t f = { want, "", 0 };
    status_t st = ops->list_units(ctx, find_cb, &f);
    if (st != OK) return st;
    if (!f.found) return set_error(ERR_NOT_FOUND, "no unit %s", want);
    sp_unit_desc_t *d = NULL;
    st = ops->describe(ctx, f.id, &d);
    if (st != OK) return st;
    if (!units_allowed(ops->name, d->unit_id, d->name)) {
        ops->desc_free(d);
        return set_error(ERR_NOT_FOUND, "no unit %s", want);
    }
    *out = d;
    return OK;
}

static api_response_t provider_unit(const char *prov, const char *unit)
{
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    if (sp_registry_get(prov, &ops, &ctx) != OK) return api_error(404, err_msg());
    sp_unit_desc_t *d = NULL;
    status_t st = resolve_unit(ops, ctx, unit, &d);
    if (st != OK) return api_error(st == ERR_NOT_FOUND ? 404 : 502, err_msg());
    cJSON *o = desc_json(d);
    ops->desc_free(d);
    return json_out(o);
}

/* /providers, /providers/<p>/units, /providers/<p>/units/<u> */
static api_response_t providers_get(const api_request_t *req, void *ud)
{
    (void)ud;
    const char *u = req->url;
    if (strcmp(u, "/providers") == 0) return providers_list();
    if (strncmp(u, "/providers/", 11) != 0) return api_error(404, "not found");
    const char *p = u + 11;
    const char *slash = strchr(p, '/');
    if (!slash || slash == p) return api_error(404, "not found");
    char prov[SP_PROVIDER_MAX];
    size_t pl = (size_t)(slash - p);
    if (pl >= sizeof(prov)) return api_error(404, "no such provider");
    memcpy(prov, p, pl);
    prov[pl] = '\0';
    if (strcmp(slash, "/units") == 0) return provider_units(req, prov);
    if (strncmp(slash, "/units/", 7) == 0 && slash[7]) return provider_unit(prov, slash + 7);
    return api_error(404, "not found");
}

static const char *providers_allow(const char *url, void *ud)
{
    (void)ud;
    if (strcmp(url, "/providers") == 0) return "GET";
    if (strncmp(url, "/providers/", 11) == 0) return "GET";
    return NULL;
}

int routes_register_providers(api_server_t *srv)
{
    if (api_server_route(srv, "/providers", API_GET, providers_get, NULL) != 0) return -1;
    if (api_server_route(srv, "/providers/*", API_GET, providers_get, NULL) != 0) return -1;
    if (api_server_route_allow(srv, "/providers*", providers_allow, NULL) != 0) return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/* /repos/<id>/units...                                                */
/* ------------------------------------------------------------------ */

/* "/units/<p>/<u>/<suffix>": the provider, the unit (which may hold a
 * '/'), and which suffix. 0 when the shape does not match. */
static int split_unit_path(const char *rest, char *prov, size_t psz, char *unit,
                           size_t usz, const char **suffix)
{
    if (strncmp(rest, "/units/", 7) != 0) return 0;
    const char *p = rest + 7;
    const char *s1 = strchr(p, '/');
    const char *last = strrchr(p, '/');
    if (!s1 || s1 == p || last == s1 || !last[1]) return 0;
    size_t pl = (size_t)(s1 - p), ul = (size_t)(last - s1 - 1);
    if (pl >= psz || ul == 0 || ul >= usz) return 0;
    memcpy(prov, p, pl);
    prov[pl] = '\0';
    memcpy(unit, s1 + 1, ul);
    unit[ul] = '\0';
    *suffix = last + 1;
    return strcmp(*suffix, "snapshots") == 0 || strcmp(*suffix, "policy") == 0;
}

const char *units_path_allow(const char *rest)
{
    if (strcmp(rest, "/units") == 0) return "GET";
    if (strcmp(rest, "/units/backup") == 0) return "POST";
    char prov[SP_PROVIDER_MAX], unit[SP_UNIT_ID_MAX];
    const char *suffix = NULL;
    if (split_unit_path(rest, prov, sizeof(prov), unit, sizeof(unit), &suffix))
        return strcmp(suffix, "policy") == 0 ? "GET, PUT" : "GET";
    if (strncmp(rest, "/snapshots/", 11) == 0) {
        int64_t sid = 0;
        const char *end = NULL;
        if (routes_path_id(rest + 11, 1, &sid, &end) == 1 && strcmp(end, "/unit/restore") == 0)
            return "POST";
    }
    return NULL;
}

/* The key a request names: the provider and the unit's id, resolved from
 * the history when it is a name. */
static void key_for(int64_t repo_id, const char *prov, const char *unit, char *key, size_t ksz)
{
    unit_key_format(key, ksz, prov, unit);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT unit_key FROM snapshot_units WHERE repo_id = ? AND provider = ?"
            " AND (unit_id = ? OR unit_name = ?) ORDER BY snap_id DESC LIMIT 1",
            (const char *[]){ repo, prov, unit, unit, NULL }, &r) == 0 && r && r->nrows == 1)
        snprintf(key, ksz, "%s", r->rows[0][0]);
    db_result_free(r);
}

static api_response_t repo_units(const api_request_t *req, int64_t repo_id)
{
    int limit = 0, offset = 0;
    api_response_t bad;
    if (routes_paging(req, 100, 1000, &limit, &offset, &bad) != 0) return bad;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim, sizeof(lim), "%d", limit);
    snprintf(off, sizeof(off), "%d", offset);
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT g.unit_key, g.provider, g.unit_id, l.unit_name, g.last, l.consistency,"
            "       g.n, s.ts"
            "  FROM (SELECT unit_key, provider, unit_id, MAX(snap_id) AS last, COUNT(*) AS n"
            "          FROM snapshot_units WHERE repo_id = ? GROUP BY unit_key) g"
            "  JOIN snapshot_units l ON l.repo_id = ? AND l.snap_id = g.last"
            "  JOIN snapshots s ON s.repo_id = l.repo_id AND s.id = g.last"
            " ORDER BY l.unit_name, g.unit_key LIMIT ? OFFSET ?",
            (const char *[]){ repo, repo, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list units");
    }
    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "units") : NULL;
    for (int i = 0; arr && r && i < r->nrows; i++) {
        char **row = r->rows[i];
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(arr, o);
        cJSON_AddStringToObject(o, "unit_key", row[0]);
        cJSON_AddStringToObject(o, "provider", row[1]);
        cJSON_AddStringToObject(o, "unit_id", row[2]);
        cJSON_AddStringToObject(o, "name", row[3]);
        cJSON_AddNumberToObject(o, "last_snapshot", (double)strtoll(row[4], NULL, 10));
        const char *cn = sp_consistency_name((sp_consistency_t)atoi(row[5]));
        cJSON_AddStringToObject(o, "consistency", cn ? cn : "inconsistent");
        cJSON_AddNumberToObject(o, "snapshots", (double)strtoll(row[6], NULL, 10));
        cJSON_AddNumberToObject(o, "last_ts", (double)strtoll(row[7], NULL, 10));
    }
    db_result_free(r);
    return json_out(root);
}

static api_response_t unit_history(const api_request_t *req, int64_t repo_id,
                                   const char *key)
{
    int limit = 0, offset = 0;
    api_response_t bad;
    if (routes_paging(req, 100, 1000, &limit, &offset, &bad) != 0) return bad;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim, sizeof(lim), "%d", limit);
    snprintf(off, sizeof(off), "%d", offset);
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT s.id, s.ts, u.unit_name, u.consistency,"
            "       COALESCE(s.file_count, 0), COALESCE(s.total_size, 0), s.gfs_flags"
            "  FROM snapshot_units u JOIN snapshots s"
            "    ON s.repo_id = u.repo_id AND s.id = u.snap_id"
            " WHERE u.repo_id = ? AND u.unit_key = ?"
            " ORDER BY s.id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, key, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read the unit's history");
    }
    cJSON *root = cJSON_CreateObject();
    if (root) cJSON_AddStringToObject(root, "unit_key", key);
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "snapshots") : NULL;
    for (int i = 0; arr && r && i < r->nrows; i++) {
        char **row = r->rows[i];
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(arr, o);
        cJSON_AddNumberToObject(o, "id", (double)strtoll(row[0], NULL, 10));
        cJSON_AddNumberToObject(o, "ts", (double)strtoll(row[1], NULL, 10));
        cJSON_AddStringToObject(o, "name", row[2]);
        const char *cn = sp_consistency_name((sp_consistency_t)atoi(row[3]));
        cJSON_AddStringToObject(o, "consistency", cn ? cn : "inconsistent");
        cJSON_AddNumberToObject(o, "files", (double)strtoll(row[4], NULL, 10));
        cJSON_AddNumberToObject(o, "bytes", (double)strtoll(row[5], NULL, 10));
        cJSON_AddNumberToObject(o, "gfs_flags", (double)strtoll(row[6], NULL, 10));
        db_result_t *it = NULL;
        cJSON *items = cJSON_AddArrayToObject(o, "items");
        if (items && db_execute(g_db,
                "SELECT item_id, kind, path, size, read_mode, reason, bytes_read"
                "  FROM snapshot_unit_items WHERE repo_id = ? AND snap_id = ? ORDER BY item_id",
                (const char *[]){ repo, row[0], NULL }, &it) == 0 && it) {
            for (int k = 0; k < it->nrows; k++) {
                cJSON *j = cJSON_CreateObject();
                if (!j) break;
                cJSON_AddItemToArray(items, j);
                cJSON_AddStringToObject(j, "id", it->rows[k][0]);
                cJSON_AddStringToObject(j, "kind", it->rows[k][1]);
                cJSON_AddStringToObject(j, "path", it->rows[k][2]);
                cJSON_AddNumberToObject(j, "size", (double)strtoll(it->rows[k][3], NULL, 10));
                cJSON_AddStringToObject(j, "read_mode", it->rows[k][4]);
                cJSON_AddStringToObject(j, "reason", it->rows[k][5]);
                cJSON_AddNumberToObject(j, "bytes_read", (double)strtoll(it->rows[k][6], NULL, 10));
            }
        }
        db_result_free(it);
    }
    db_result_free(r);
    return json_out(root);
}

static api_response_t unit_policy_get(int64_t repo_id, const char *key)
{
    gfs_retention_t ret;
    memset(&ret, 0, sizeof(ret));
    routes_retention_default_t defs[8];
    int nd = routes_snapshots_retention_defaults(defs, 8);
    for (int i = 0; i < nd; i++) {
        int *f = ret_field(&ret, defs[i].key);
        if (f) *f = defs[i].value;
    }
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT key, value FROM policy_values WHERE repo_id = ?"
                         " AND key LIKE 'retention.%'",
                   (const char *[]){ repo, NULL }, &r) == 0 && r)
        for (int i = 0; i < r->nrows; i++) {
            int v = 0, *f = ret_field(&ret, r->rows[i][0]);
            if (f && routes_policy_count(r->rows[i][1], &v) == 0) *f = v;
        }
    db_result_free(r);
    gfs_retention_t repo_ret = ret;
    routes_unit_policy(g_db, repo_id, key, &ret);

    cJSON *root = cJSON_CreateObject();
    if (!root) return api_error(500, "out of memory");
    cJSON_AddStringToObject(root, "unit_key", key);
    cJSON *eff = cJSON_AddObjectToObject(root, "effective");
    cJSON *own = cJSON_AddObjectToObject(root, "unit");
    r = NULL;
    if (own && db_execute(g_db, "SELECT key, value FROM unit_policy WHERE repo_id = ?"
                                " AND unit_key = ? ORDER BY key",
                          (const char *[]){ repo, key, NULL }, &r) == 0 && r)
        for (int i = 0; i < r->nrows; i++)
            cJSON_AddStringToObject(own, r->rows[i][0], r->rows[i][1]);
    db_result_free(r);
    if (eff) {
        cJSON_AddNumberToObject(eff, "retention.keep_snaps", ret.keep_snaps);
        cJSON_AddNumberToObject(eff, "retention.keep_daily", ret.keep_daily);
        cJSON_AddNumberToObject(eff, "retention.keep_weekly", ret.keep_weekly);
        cJSON_AddNumberToObject(eff, "retention.keep_monthly", ret.keep_monthly);
        cJSON_AddNumberToObject(eff, "retention.keep_yearly", ret.keep_yearly);
    }
    (void)repo_ret;
    return json_out(root);
}

/* Body: {"retention.keep_daily": "14", ...}; a value of null or "" drops
 * the unit's own value, so the repository's applies again. */
static api_response_t unit_policy_put(const api_request_t *req, int64_t repo_id,
                                      const char *key)
{
    if (!req->body || !req->body_len) return api_error(400, "a body of policy keys is required");
    char *txt = strndup(req->body, req->body_len);
    cJSON *o = txt ? cJSON_Parse(txt) : NULL;
    free(txt);
    if (!o || !cJSON_IsObject(o)) { cJSON_Delete(o); return api_error(400, "body must be a JSON object"); }
    const cJSON *e;
    cJSON_ArrayForEach(e, o) {
        gfs_retention_t probe;
        if (!e->string || !ret_field(&probe, e->string)) {
            char msg[160];
            snprintf(msg, sizeof(msg), "unknown unit policy key '%.100s'; use retention.keep_*",
                     e->string ? e->string : "");
            cJSON_Delete(o);
            return api_error(400, msg);
        }
        int v = 0;
        char num[32];
        const char *val = NULL;
        if (cJSON_IsNumber(e)) { snprintf(num, sizeof(num), "%.0f", e->valuedouble); val = num; }
        else if (cJSON_IsString(e)) val = e->valuestring;
        if (val && *val && routes_policy_count(val, &v) != 0) {
            cJSON_Delete(o);
            return api_error(400, "retention counts are whole numbers from 0");
        }
        if (!cJSON_IsNull(e) && !val) {
            cJSON_Delete(o);
            return api_error(400, "retention counts are whole numbers from 0");
        }
    }
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (db_txn_begin(g_db) != OK) { cJSON_Delete(o); return api_error(500, "cannot begin"); }
    int failed = 0;
    cJSON_ArrayForEach(e, o) {
        char num[32];
        const char *val = cJSON_IsNumber(e) ? (snprintf(num, sizeof(num), "%.0f", e->valuedouble), num)
                        : (cJSON_IsString(e) && e->valuestring) ? e->valuestring : "";
        int rc = (!*val)
            ? db_execute_non_query(g_db, "DELETE FROM unit_policy WHERE repo_id = ? AND unit_key = ?"
                                         " AND key = ?",
                                   (const char *[]){ repo, key, e->string, NULL }, NULL)
            : db_execute_non_query(g_db,
                  "INSERT INTO unit_policy (repo_id, unit_key, key, value) VALUES (?, ?, ?, ?)"
                  " ON CONFLICT(repo_id, unit_key, key) DO UPDATE SET value = excluded.value",
                  (const char *[]){ repo, key, e->string, val, NULL }, NULL);
        if (rc != 0) { failed = 1; break; }
    }
    cJSON_Delete(o);
    if (failed) { db_txn_rollback(g_db); return api_error(500, "cannot store the unit policy"); }
    if (db_txn_commit(g_db) != OK) return api_error(500, "cannot commit the unit policy");
    return unit_policy_get(repo_id, key);
}

/* ------------------------------------------------------------------ */
/* Jobs                                                                */
/* ------------------------------------------------------------------ */

typedef struct {
    cutils_db_t *db;
    int64_t      repo_id;
    char        *repo_path;
    char        *provider;
    int          all;
    char       **exclude;
    size_t       n_exclude;
    char       **units;
    size_t       n_units;
    sp_consistency_t require;
} units_job_t;

static void units_job_free(void *p)
{
    units_job_t *j = p;
    if (!j) return;
    free(j->repo_path);
    free(j->provider);
    jreq_strv_free(j->exclude, j->n_exclude);
    jreq_strv_free(j->units, j->n_units);
    free(j);
}

static status_t open_store(const char *path, cutils_db_t *db, int64_t repo_id,
                           atomic_int *cancel, store_t **store, store_index_t **idx)
{
    status_t st = repo_store_open_keyed(path, NULL, store);
    if (st != OK) return st;
    st = store_index_bind(*store, db, repo_id, idx);
    if (st != OK) { store_close(*store); *store = NULL; return st; }
    store_set_cancel(*store, cancel);
    return OK;
}

static status_t units_job_run(void *arg, atomic_int *cancel,
                              void (*progress)(const char *, uint64_t, uint64_t, void *),
                              void *progress_ctx)
{
    units_job_t *j = arg;
    store_t *store = NULL;
    store_index_t *idx = NULL;
    status_t st = open_store(j->repo_path, j->db, j->repo_id, cancel, &store, &idx);
    if (st != OK) return st;

    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = j->provider;
    o.all = j->all;
    o.exclude = (const char **)(void *)j->exclude;
    o.n_exclude = j->n_exclude;
    o.units = (const char **)(void *)j->units;
    o.n_units = j->n_units;
    o.require = j->require;
    o.job_id = job_progress_id(progress_ctx);
    o.cancel = cancel;
    o.progress_cb = progress;
    o.progress_ctx = progress_ctx;
    o.bytes_cb = job_progress_bytes;
    o.store_workers = routes_backup_store_workers();
    unit_backup_result_t res;
    st = unit_backup_run(store, j->db, j->repo_id, &o, &res);
    char why[ERR_MSG_MAX];
    snprintf(why, sizeof(why), "%s", st != OK ? err_msg() : "");

    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "units") : NULL;
    char warn[600] = "";
    for (size_t i = 0; arr && i < res.n; i++) {
        const unit_backup_one_t *u = &res.units[i];
        cJSON *x = cJSON_CreateObject();
        if (!x) break;
        cJSON_AddItemToArray(arr, x);
        cJSON_AddStringToObject(x, "unit_key", u->unit_key);
        cJSON_AddStringToObject(x, "name", u->name);
        cJSON_AddStringToObject(x, "status", u->st == OK ? "ok" : "failed");
        if (u->snapshot_id > 0) cJSON_AddNumberToObject(x, "snapshot_id", (double)u->snapshot_id);
        if (u->st == OK) {
            cJSON_AddStringToObject(x, "consistency", sp_consistency_name(u->achieved));
            cJSON_AddNumberToObject(x, "bytes_read", (double)u->bytes_read);
            cJSON_AddNumberToObject(x, "files", (double)u->files);
        } else {
            cJSON_AddStringToObject(x, "error", u->error);
        }
        if (u->warning[0]) {
            cJSON_AddStringToObject(x, "warning", u->warning);
            if (!warn[0]) snprintf(warn, sizeof(warn), "unit %.80s: %.500s", u->name, u->warning);
        }
    }
    if (root) {
        cJSON_AddNumberToObject(root, "ok", (double)res.ok);
        cJSON_AddNumberToObject(root, "failed", (double)res.failed);
        cJSON_AddNumberToObject(root, "filtered", (double)res.filtered);
        char *txt = cJSON_PrintUnformatted(root);
        if (txt) { job_set_result(progress_ctx, txt); cJSON_free(txt); }
        cJSON_Delete(root);
    }
    char summary[256];
    snprintf(summary, sizeof(summary), "%s: %zu unit(s) backed up, %zu failed, %zu left out "
             "by units.allow", j->provider, res.ok, res.failed, res.filtered);
    job_set_summary(progress_ctx, summary);
    if (warn[0]) job_set_warning(progress_ctx, warn);
    unit_backup_result_free(&res);
    store_index_free(idx);
    store_close(store);
    if (st != OK) set_error(st, "%s", why);
    return st;
}

/*
 * The request body or schedule params: {"provider":"fake","all":true,
 * "exclude":["cbk-*"]} or {"provider":"fake","units":["a","b"]}, with an
 * optional "require". On success *canon is the canonical form, which is
 * what the job and a schedule store and what overlap is keyed by.
 */
static int units_params(const char *body, size_t len, units_job_t *j, char **canon,
                        char *why, size_t wsz)
{
    char *txt = strndup(body ? body : "", len);
    cJSON *o = txt ? cJSON_Parse(txt) : NULL;
    free(txt);
    if (!o || !cJSON_IsObject(o)) { cJSON_Delete(o); snprintf(why, wsz, "body must be a JSON object"); return -1; }
    int rc = -1;
    const cJSON *prov = cJSON_GetObjectItemCaseSensitive(o, "provider");
    const cJSON *all = cJSON_GetObjectItemCaseSensitive(o, "all");
    const cJSON *ex = cJSON_GetObjectItemCaseSensitive(o, "exclude");
    const cJSON *un = cJSON_GetObjectItemCaseSensitive(o, "units");
    const cJSON *rq = cJSON_GetObjectItemCaseSensitive(o, "require");
    const cJSON *e;
    j->require = SP_CONSIST_CRASH;
    if (!cJSON_IsString(prov) || !*prov->valuestring || strlen(prov->valuestring) >= SP_PROVIDER_MAX) {
        snprintf(why, wsz, "provider is required");
        goto out;
    }
    if (all && !cJSON_IsBool(all)) { snprintf(why, wsz, "all must be true or false"); goto out; }
    j->all = all && cJSON_IsTrue(all);
    if (ex && !cJSON_IsArray(ex)) { snprintf(why, wsz, "exclude must be a list of globs"); goto out; }
    if (un && !cJSON_IsArray(un)) { snprintf(why, wsz, "units must be a list of names"); goto out; }
    if (j->all && un && cJSON_GetArraySize(un) > 0) { snprintf(why, wsz, "name units or all, not both"); goto out; }
    if (!j->all && (!un || cJSON_GetArraySize(un) == 0)) { snprintf(why, wsz, "name units, or all"); goto out; }
    if (!j->all && ex && cJSON_GetArraySize(ex) > 0) { snprintf(why, wsz, "exclude applies only with all"); goto out; }
    if (rq && (!cJSON_IsString(rq) || sp_consistency_parse(rq->valuestring, &j->require) != 0)) {
        snprintf(why, wsz, "require must be inconsistent, crash, quiesced, application or offline");
        goto out;
    }
    if (!(j->provider = strdup(prov->valuestring))) { snprintf(why, wsz, "out of memory"); goto out; }
    size_t n = (size_t)(ex ? cJSON_GetArraySize(ex) : 0);
    j->exclude = calloc(n ? n : 1, sizeof(char *));
    cJSON_ArrayForEach(e, ex) {
        if (!cJSON_IsString(e) || !*e->valuestring || strlen(e->valuestring) > 255) {
            snprintf(why, wsz, "every exclude must be a non-empty glob");
            goto out;
        }
        if (!(j->exclude[j->n_exclude] = strdup(e->valuestring))) goto out;
        j->n_exclude++;
    }
    n = (size_t)(un ? cJSON_GetArraySize(un) : 0);
    if (n > 256) { snprintf(why, wsz, "at most 256 units"); goto out; }
    j->units = calloc(n ? n : 1, sizeof(char *));
    cJSON_ArrayForEach(e, un) {
        if (!cJSON_IsString(e) || !*e->valuestring || strlen(e->valuestring) >= SP_UNIT_ID_MAX) {
            snprintf(why, wsz, "every unit must be a non-empty name");
            goto out;
        }
        if (!(j->units[j->n_units] = strdup(e->valuestring))) goto out;
        j->n_units++;
    }
    if (canon) {
        cJSON *c = cJSON_CreateObject();
        if (!c) goto out;
        cJSON_AddStringToObject(c, "provider", j->provider);
        if (j->all) {
            cJSON_AddBoolToObject(c, "all", 1);
            cJSON *xa = cJSON_AddArrayToObject(c, "exclude");
            for (size_t i = 0; xa && i < j->n_exclude; i++)
                cJSON_AddItemToArray(xa, cJSON_CreateString(j->exclude[i]));
        } else {
            cJSON *ua = cJSON_AddArrayToObject(c, "units");
            for (size_t i = 0; ua && i < j->n_units; i++)
                cJSON_AddItemToArray(ua, cJSON_CreateString(j->units[i]));
        }
        if (rq) cJSON_AddStringToObject(c, "require", sp_consistency_name(j->require));
        char *t = cJSON_PrintUnformatted(c);
        cJSON_Delete(c);
        if (!t) goto out;
        *canon = strdup(t);
        cJSON_free(t);
        if (!*canon) goto out;
    }
    rc = 0;
out:
    cJSON_Delete(o);
    return rc;
}

int routes_units_params_check(const char *params, char **canon, char *why, size_t wsz)
{
    units_job_t *j = calloc(1, sizeof(*j));
    if (!j) { snprintf(why, wsz, "out of memory"); return -1; }
    int rc = units_params(params, params ? strlen(params) : 0, j, canon, why, wsz);
    units_job_free(j);
    return rc;
}

static status_t submit_units(int64_t repo_id, const char *body, size_t len,
                             int64_t schedule_id, int64_t *job_id, int *http)
{
    char why[256] = "";
    units_job_t *j = calloc(1, sizeof(*j));
    if (!j) { *http = 500; return set_error(ERR_NOMEM, "unit backup: alloc"); }
    char *canon = NULL;
    if (units_params(body, len, j, &canon, why, sizeof(why)) != 0) {
        units_job_free(j);
        *http = 400;
        return set_error(ERR_INVALID, "%s", why);
    }
    if (!sp_registry_enabled(j->provider)) {
        status_t st = sp_registry_get(j->provider, NULL, NULL);
        units_job_free(j);
        free(canon);
        *http = 409;
        return st;
    }
    j->db = g_db;
    j->repo_id = repo_id;
    if (!(j->repo_path = repo_path_of(repo_id))) {
        units_job_free(j);
        free(canon);
        *http = 404;
        return set_error(ERR_NOT_FOUND, "no such repo");
    }
    if (repo_require_key(j->repo_path) != OK) {
        units_job_free(j);
        free(canon);
        *http = 409;
        return ERR_INVALID;
    }
    status_t st = job_submit_scheduled(g_jm, repo_id, "units", canon, schedule_id,
                                       units_job_run, j, units_job_free, job_id);
    free(canon);
    *http = st == ERR_BUSY ? 503 : 500;
    return st;
}

status_t routes_submit_units(struct job_manager *jm, int64_t repo_id, const char *params,
                             int64_t schedule_id, int64_t *out_job_id)
{
    (void)jm;
    int http = 0;
    return submit_units(repo_id, params, params ? strlen(params) : 0, schedule_id,
                        out_job_id, &http);
}

typedef struct {
    cutils_db_t *db;
    int64_t      repo_id, snap_id;
    char        *repo_path;
    char        *dest;
    char        *item;
    int          overwrite;
} urestore_job_t;

static void urestore_free(void *p)
{
    urestore_job_t *j = p;
    if (!j) return;
    free(j->repo_path); free(j->dest); free(j->item);
    free(j);
}

static status_t urestore_run(void *arg, atomic_int *cancel,
                             void (*progress)(const char *, uint64_t, uint64_t, void *),
                             void *progress_ctx)
{
    urestore_job_t *j = arg;
    store_t *store = NULL;
    store_index_t *idx = NULL;
    status_t st = open_store(j->repo_path, j->db, j->repo_id, cancel, &store, &idx);
    if (st != OK) return st;
    restore_opts_t o;
    memset(&o, 0, sizeof(o));
    o.overwrite = j->overwrite;
    o.cancel = cancel;
    o.progress_cb = progress;
    o.progress_ctx = progress_ctx;
    o.bytes_cb = job_progress_bytes;
    char rj[512];
    if (j->item) {
        unit_restore_item_result_t ir;
        st = unit_restore_item(store, j->db, j->repo_id, j->snap_id, j->item, j->dest, &o, &ir);
        snprintf(rj, sizeof(rj), "{\"mode\":\"item\",\"bytes\":%llu}", (unsigned long long)ir.bytes);
    } else {
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        st = restore_run(store, j->db, j->repo_id, j->snap_id, j->dest, &o, &rr);
        snprintf(rj, sizeof(rj), "{\"mode\":\"files\",\"files\":%llu,\"dirs\":%llu,"
                 "\"bytes\":%llu,\"failed\":%llu}", (unsigned long long)rr.files,
                 (unsigned long long)rr.dirs, (unsigned long long)rr.bytes,
                 (unsigned long long)rr.failed);
    }
    job_set_result(progress_ctx, rj);
    store_index_free(idx);
    store_close(store);
    return st;
}

/* {"mode":"files","dest":"/abs/dir"} or {"mode":"item","item":"vda",
 * "dest":"/abs/file","format":"raw"}. */
static api_response_t unit_restore_post(const api_request_t *req, int64_t repo_id,
                                        int64_t snap_id)
{
    if (!req->body || !req->body_len) return api_error(400, "a body with mode and dest is required");
    char *txt = strndup(req->body, req->body_len);
    cJSON *o = txt ? cJSON_Parse(txt) : NULL;
    free(txt);
    if (!o || !cJSON_IsObject(o)) { cJSON_Delete(o); return api_error(400, "body must be a JSON object"); }
    const char *mode = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "mode"));
    const char *dest = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "dest"));
    const char *item = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "item"));
    const char *fmt  = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "format"));
    const cJSON *ow  = cJSON_GetObjectItemCaseSensitive(o, "overwrite");
    const char *bad = NULL;
    if (!mode) mode = item ? "item" : "files";
    if (strcmp(mode, "files") != 0 && strcmp(mode, "item") != 0)
        bad = "mode must be files or item; unit and export need a provider that defines units";
    else if (!dest || dest[0] != '/') bad = "dest must be an absolute path";
    else if (strcmp(mode, "item") == 0 && (!item || !*item)) bad = "item is required for mode item";
    else if (strcmp(mode, "files") == 0 && item) bad = "item applies only to mode item";
    else if (fmt && strcmp(fmt, "raw") != 0) bad = "format must be raw; qcow2 comes with the libvirt provider";
    else if (ow && !cJSON_IsBool(ow)) bad = "overwrite must be true or false";
    if (bad) { cJSON_Delete(o); return api_error(400, bad); }

    char key[UNIT_KEY_MAX];
    if (unit_snapshot_key(g_db, repo_id, snap_id, key, sizeof(key)) != OK) {
        cJSON_Delete(o);
        return api_error(404, err_msg());
    }
    urestore_job_t *j = calloc(1, sizeof(*j));
    if (!j) { cJSON_Delete(o); return api_error(500, "out of memory"); }
    j->db = g_db;
    j->repo_id = repo_id;
    j->snap_id = snap_id;
    j->repo_path = repo_path_of(repo_id);
    j->dest = strdup(dest);
    j->item = (strcmp(mode, "item") == 0 && item) ? strdup(item) : NULL;
    j->overwrite = ow && cJSON_IsTrue(ow);
    cJSON_Delete(o);
    if (!j->repo_path || !j->dest || (strcmp(mode, "item") == 0 && !j->item)) {
        urestore_free(j);
        return api_error(500, "out of memory");
    }
    int64_t job = 0;
    /* A restore only reads the repository. */
    status_t st = job_submit_locked(g_jm, repo_id, JOB_LOCK_READ, "unit-restore", NULL, 0,
                                    urestore_run, j, urestore_free, &job);
    if (st != OK) return api_error(st == ERR_BUSY ? 503 : 500, err_msg());
    char *out = NULL;
    if (asprintf(&out, "{\"job_id\":%lld}", (long long)job) < 0) return api_error(500, "out of memory");
    return api_ok_status(202, out);
}

/* ------------------------------------------------------------------ */
/* Dispatch from repos.c                                               */
/* ------------------------------------------------------------------ */

api_response_t units_route_get(const api_request_t *req, int64_t repo_id, const char *rest)
{
    if (strcmp(rest, "/units") == 0) return repo_units(req, repo_id);
    char prov[SP_PROVIDER_MAX], unit[SP_UNIT_ID_MAX], key[UNIT_KEY_MAX];
    const char *suffix = NULL;
    if (!split_unit_path(rest, prov, sizeof(prov), unit, sizeof(unit), &suffix))
        return api_error(404, "not found");
    key_for(repo_id, prov, unit, key, sizeof(key));
    if (strcmp(suffix, "snapshots") == 0) return unit_history(req, repo_id, key);
    return unit_policy_get(repo_id, key);
}

api_response_t units_route_put(const api_request_t *req, int64_t repo_id, const char *rest)
{
    char prov[SP_PROVIDER_MAX], unit[SP_UNIT_ID_MAX], key[UNIT_KEY_MAX];
    const char *suffix = NULL;
    if (!split_unit_path(rest, prov, sizeof(prov), unit, sizeof(unit), &suffix) ||
        strcmp(suffix, "policy") != 0)
        return api_error(404, "not found");
    key_for(repo_id, prov, unit, key, sizeof(key));
    return unit_policy_put(req, repo_id, key);
}

api_response_t units_route_post(const api_request_t *req, int64_t repo_id, const char *rest)
{
    if (strcmp(rest, "/units/backup") == 0) {
        int64_t job = 0;
        int http = 500;
        status_t st = submit_units(repo_id, req->body, req->body_len, 0, &job, &http);
        if (st != OK) return api_error(http, err_msg());
        char *out = NULL;
        if (asprintf(&out, "{\"job_id\":%lld}", (long long)job) < 0) return api_error(500, "out of memory");
        return api_ok_status(202, out);
    }
    if (strncmp(rest, "/snapshots/", 11) == 0) {
        int64_t sid = 0;
        const char *end = NULL;
        if (routes_path_id(rest + 11, 1, &sid, &end) == 1 && strcmp(end, "/unit/restore") == 0)
            return unit_restore_post(req, repo_id, sid);
    }
    return api_error(404, "not found");
}
