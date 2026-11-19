#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

/*
 * Acting on a database finding (docs/db-detection.md 7.3 and 7.4, phase D2):
 *
 *   POST /repos/<id>/databases/<f>/preview   the exact hook and its probe
 *   POST /repos/<id>/databases/<f>/enable    {"template", "params"}
 *   POST /repos/<id>/databases/<f>/dismiss   {"reason"}
 *
 * Nothing here changes how anything is captured by itself: enable records
 * the user's choice on the finding, and the next backup of its unit (or,
 * phase D3, of its file source) reads that row and acts on it. Enabling is
 * always an explicit call from an authenticated caller, and journalled
 * with who made it; no schedule, upgrade or detection ever does it.
 *
 * A unit's template runs in the helper, whose policy has the final say:
 * enabling one the policy does not allow is refused with the policy line
 * to add instead. A file source's only engine copy is the daemon's own
 * SQLite online backup (9.2), so enabling one only records it.
 */

#include "routes.h"

#include "db_detect.h"
#include "db_template.h"
#include "db_write.h"
#include "registry.h"
#include "remote_provider.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>
#include <openssl/evp.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static cutils_db_t *g_db;

void routes_db_actions_init(cutils_db_t *db) { g_db = db; }

/* The part of the /repos/<id> table these routes serve. Commit-only: each
 * is a DB-only edit an operator can repeat, or (preview) none at all. */
#define C_ API_COMMIT_ONLY
static const api_path_t k_db_paths[] = {
    /* path                           GET  POST  PUT  DELETE */
    { "/databases/<id>/preview",        0,  C_,   0,   0 },
    { "/databases/<id>/enable",         0,  C_,   0,   0 },
    { "/databases/<id>/dismiss",        0,  C_,   0,   0 },
};
#undef C_

const api_path_t *routes_db_actions_paths(size_t *n)
{
    *n = sizeof(k_db_paths) / sizeof(k_db_paths[0]);
    return k_db_paths;
}

/* "/databases/<f>/<action>": the finding id and the action, or 0. */
static int split_path(const char *rest, int64_t *fid, const char **action)
{
    if (strncmp(rest, "/databases/", 11) != 0) return 0;
    const char *end = NULL;
    if (routes_path_id(rest + 11, 1, fid, &end) != 1 || *end != '/') return 0;
    *action = end + 1;
    return strcmp(*action, "preview") == 0 || strcmp(*action, "enable") == 0 ||
           strcmp(*action, "dismiss") == 0;
}

const api_path_t *db_actions_path_entry(const char *rest)
{
    int64_t fid = 0;
    const char *action = NULL;
    if (!split_path(rest, &fid, &action)) return NULL;
    for (size_t i = 0; i < sizeof(k_db_paths) / sizeof(k_db_paths[0]); i++) {
        const char *a = strrchr(k_db_paths[i].path, '/') + 1;
        if (strcmp(a, action) == 0) return &k_db_paths[i];
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* The finding                                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t id;
    char    scope[PATH_MAX + 16];
    char    engine[16];
    char    path[PATH_MAX];
    char    version[32];
    char    confidence[12];
    char    coverage[24];
    char    suggestion[40];
    char    state[16];
    char    tmpl[40];
    char    params[1024];
} finding_t;

static int finding_load(int64_t repo_id, int64_t fid, finding_t *f)
{
    char repo[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)fid);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT id, scope, engine, path, COALESCE(version, ''), confidence,"
                         " COALESCE(coverage, ''), COALESCE(suggestion, ''), state,"
                         " COALESCE(template, ''), COALESCE(params, '')"
                         " FROM db_findings WHERE repo_id = ? AND id = ?",
                   (const char *[]){ repo, id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    char **row = r->rows[0];
    memset(f, 0, sizeof(*f));
    f->id = fid;
    snprintf(f->scope, sizeof(f->scope), "%s", row[1]);
    snprintf(f->engine, sizeof(f->engine), "%s", row[2]);
    snprintf(f->path, sizeof(f->path), "%s", row[3]);
    snprintf(f->version, sizeof(f->version), "%s", row[4]);
    snprintf(f->confidence, sizeof(f->confidence), "%s", row[5]);
    snprintf(f->coverage, sizeof(f->coverage), "%s", row[6]);
    snprintf(f->suggestion, sizeof(f->suggestion), "%s", row[7]);
    snprintf(f->state, sizeof(f->state), "%s", row[8]);
    snprintf(f->tmpl, sizeof(f->tmpl), "%s", row[9]);
    snprintf(f->params, sizeof(f->params), "%s", row[10]);
    db_result_free(r);
    return 0;
}

static int is_unit(const finding_t *f) { return strncmp(f->scope, "unit:", 5) == 0; }
static int is_source(const finding_t *f) { return strncmp(f->scope, "source:", 7) == 0; }

/* The finding as the actions answer it. */
static cJSON *finding_json(int64_t repo_id, int64_t fid)
{
    char repo[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)fid);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT id, scope, engine, path, COALESCE(version, ''), confidence,"
                         " COALESCE(coverage, ''), COALESCE(suggestion, ''), state,"
                         " COALESCE(template, ''), COALESCE(params, ''), COALESCE(decided_by, ''),"
                         " COALESCE(decided_at, 0), COALESCE(reason, '')"
                         " FROM db_findings WHERE repo_id = ? AND id = ?",
                   (const char *[]){ repo, id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return NULL;
    }
    char **row = r->rows[0];
    cJSON *o = cJSON_CreateObject();
    if (o) {
        cJSON_AddNumberToObject(o, "id", (double)fid);
        cJSON_AddStringToObject(o, "scope", row[1]);
        cJSON_AddStringToObject(o, "engine", row[2]);
        cJSON_AddStringToObject(o, "path", row[3]);
        if (row[4][0]) cJSON_AddStringToObject(o, "version", row[4]);
        cJSON_AddStringToObject(o, "confidence", row[5]);
        if (row[6][0]) cJSON_AddStringToObject(o, "coverage", row[6]);
        if (row[7][0]) cJSON_AddStringToObject(o, "suggestion", row[7]);
        cJSON_AddStringToObject(o, "state", row[8]);
        if (row[9][0]) cJSON_AddStringToObject(o, "template", row[9]);
        cJSON *p = row[10][0] ? cJSON_Parse(row[10]) : NULL;
        if (p) cJSON_AddItemToObject(o, "params", p);
        if (row[11][0]) cJSON_AddStringToObject(o, "decided_by", row[11]);
        long long at = strtoll(row[12], NULL, 10);
        if (at) cJSON_AddNumberToObject(o, "decided_at", (double)at);
        if (row[13][0]) cJSON_AddStringToObject(o, "reason", row[13]);
    }
    db_result_free(r);
    return o;
}

static api_response_t json_out(int status, cJSON *root)
{
    char *txt = root ? cJSON_PrintUnformatted(root) : NULL;
    cJSON_Delete(root);
    if (!txt) return api_error(500, "cannot serialise response");
    return status == 200 ? api_ok(txt) : api_ok_status(status, txt);
}

/*
 * Who made the call, for decided_by and the journal: the local socket (the
 * CLI, trusted by its file permissions), or the TCP peer and a short,
 * one-way name for the session, never the token itself.
 */
static void caller_of(const api_request_t *req, char *out, size_t sz)
{
    if (req->is_local) { snprintf(out, sz, "local socket"); return; }
    const char *tok = req->auth_token ? req->auth_token : "";
    if (strncmp(tok, "Bearer ", 7) == 0) tok += 7;
    char sess[17] = "";
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned mdlen = 0;
    if (*tok && EVP_Digest(tok, strlen(tok), md, &mdlen, EVP_sha256(), NULL) == 1 && mdlen >= 8)
        for (int i = 0; i < 8; i++) snprintf(sess + i * 2, 3, "%02x", md[i]);
    snprintf(out, sz, "tcp %s%s%s", req->client_addr && *req->client_addr ? req->client_addr : "?",
             sess[0] ? " session " : "", sess);
}

/* The request body as a JSON object; NULL body is {}. */
static int body_object(const api_request_t *req, cJSON **out, api_response_t *bad)
{
    *out = NULL;
    if (!req->body || !req->body_len) { *out = cJSON_CreateObject(); return *out ? 0 : -1; }
    char *txt = strndup(req->body, req->body_len);
    cJSON *o = txt ? cJSON_Parse(txt) : NULL;
    free(txt);
    if (!cJSON_IsObject(o)) {
        cJSON_Delete(o);
        *bad = api_error(400, "body must be a JSON object");
        return -1;
    }
    *out = o;
    return 0;
}

/* Keys of o outside allowed (NULL-terminated): the first, or NULL. */
static const char *unknown_key(const cJSON *o, const char *const *allowed)
{
    const cJSON *e;
    cJSON_ArrayForEach(e, o) {
        int ok = 0;
        for (size_t i = 0; allowed[i] && !ok; i++) ok = e->string && strcmp(e->string, allowed[i]) == 0;
        if (!ok) return e->string ? e->string : "";
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Choosing a template                                                  */
/* ------------------------------------------------------------------ */

/*
 * The template and canonical params a request names for a finding, checked:
 * a unit's from the catalog, for the finding's engine, params by the
 * catalog's grammar, sqlite-backup's db filled with the finding's own path;
 * a file source's only sqlite-backup with an optional allow_rollback_lock.
 * want may be NULL (preview: the finding's suggestion, or what is enabled).
 * *params_out is malloc'd JSON text.
 */
static int choose(const finding_t *f, const char *want, const cJSON *params, const db_template_t **t_out,
                  char **params_out, api_response_t *bad)
{
    *t_out = NULL;
    *params_out = NULL;
    char msg[512];
    const char *name = want ? want : f->tmpl[0] ? f->tmpl : f->suggestion;
    /* Nothing named or suggested (a covered finding, whose dump is only
     * optional): the engine's own, the first the default policy allows. */
    if (!name[0] && is_unit(f)) {
        const db_template_t *first = NULL;
        for (size_t i = 0; i < db_template_count(); i++) {
            const db_template_t *c = db_template_at(i);
            if (strcmp(c->engine, f->engine) != 0) continue;
            if (!first || (c->default_allowed && !first->default_allowed)) first = c;
        }
        if (first) name = first->name;
    }
    if (params && !cJSON_IsObject(params)) { *bad = api_error(400, "params must be an object"); return -1; }
    if (is_source(f)) {
        if (!name[0] || strcmp(name, "sqlite-backup") != 0) {
            snprintf(msg, sizeof(msg), "a file source's finding takes only sqlite-backup, the daemon's own SQLite "
                     "online backup%s%s%s", name[0] ? " (not " : "", name, name[0] ? ")" : "");
            *bad = api_error(400, msg);
            return -1;
        }
        if (strcmp(f->engine, "sqlite") != 0) {
            snprintf(msg, sizeof(msg), "finding %lld is %s, not SQLite: a file source has no engine copy for it; "
                     "back it up as a unit, or use source snapshot mode", (long long)f->id, f->engine);
            *bad = api_error(400, msg);
            return -1;
        }
        cJSON *canon = cJSON_CreateObject();
        const cJSON *e;
        cJSON_ArrayForEach(e, params) {
            if (!e->string || strcmp(e->string, "allow_rollback_lock") != 0) {
                cJSON_Delete(canon);
                snprintf(msg, sizeof(msg), "sqlite-backup for a file source has no parameter '%.64s'; "
                         "its only one is allow_rollback_lock", e->string ? e->string : "");
                *bad = api_error(400, msg);
                return -1;
            }
            if (!cJSON_IsBool(e)) {
                cJSON_Delete(canon);
                *bad = api_error(400, "allow_rollback_lock must be true or false");
                return -1;
            }
            cJSON_AddBoolToObject(canon, "allow_rollback_lock", cJSON_IsTrue(e));
        }
        char *txt = canon ? cJSON_PrintUnformatted(canon) : NULL;
        cJSON_Delete(canon);
        if (!txt) { *bad = api_error(500, "out of memory"); return -1; }
        *params_out = txt;
        *t_out = db_template_find("sqlite-backup");
        return 0;
    }
    if (!is_unit(f)) { *bad = api_error(409, "finding has a scope this daemon cannot act on"); return -1; }
    if (!name[0]) {
        *bad = api_error(400, "the finding suggests no template; name one");
        return -1;
    }
    const db_template_t *t = db_template_find(name);
    if (!t) {
        snprintf(msg, sizeof(msg), "no template '%.64s'; the catalog is in docs/db-detection.md 7.2", name);
        *bad = api_error(400, msg);
        return -1;
    }
    if (strcmp(t->engine, f->engine) != 0) {
        snprintf(msg, sizeof(msg), "template %s is for %s, and finding %lld is %s", t->name, t->engine,
                 (long long)f->id, f->engine);
        *bad = api_error(400, msg);
        return -1;
    }
    cJSON *p = params ? cJSON_Duplicate(params, 1) : cJSON_CreateObject();
    if (!p) { *bad = api_error(500, "out of memory"); return -1; }
    /* sqlite-backup reads the finding's database: its path is the
     * finding's, never another file the caller names. */
    for (size_t i = 0; i < t->n_params; i++) {
        if (t->params[i].type != DBT_P_PATH) continue;
        const char *given = db_template_param_str(p, t->params[i].name);
        if (given && strcmp(given, f->path) != 0) {
            cJSON_Delete(p);
            snprintf(msg, sizeof(msg), "%s: %s must be the finding's own path, %.300s", t->name, t->params[i].name,
                     f->path);
            *bad = api_error(400, msg);
            return -1;
        }
        if (!given) cJSON_AddStringToObject(p, t->params[i].name, f->path);
    }
    if (db_template_check(t, p) != OK) {
        cJSON_Delete(p);
        snprintf(msg, sizeof(msg), "%.500s", err_msg());
        err_clear();
        *bad = api_error(400, msg);
        return -1;
    }
    char *txt = cJSON_PrintUnformatted(p);
    cJSON_Delete(p);
    if (!txt) { *bad = api_error(500, "out of memory"); return -1; }
    *params_out = txt;
    *t_out = t;
    return 0;
}

/* The policy line that would allow name, from the helper's last hello. */
static void policy_line(const char *name, const char *allowed, char *out, size_t sz)
{
    char list[1100] = "";
    size_t o = 0;
    for (const char *p = allowed; *p && o + 4 < sizeof(list);) {
        const char *e = strchr(p, ',');
        size_t l = e ? (size_t)(e - p) : strlen(p);
        o += (size_t)snprintf(list + o, sizeof(list) - o, "%s%.*s", o ? ", " : "", (int)l, p);
        if (!e) break;
        p = e + 1;
    }
    snprintf(out, sz, "[db] allow_templates = %s%s%s", list, list[0] ? ", " : "", name);
}

/* The output name a unit's template gets at its next backup: its name, or
 * <name>-<finding> when an earlier finding of the unit enabled it too
 * (unit_backup.c templates_load). */
static void hook_name(int64_t repo_id, const finding_t *f, const char *name, char *out, size_t sz)
{
    char repo[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)f->id);
    db_result_t *r = NULL;
    int earlier = db_execute(g_db, "SELECT COUNT(*) FROM db_findings WHERE repo_id = ? AND scope = ?"
                                   " AND state = 'enabled' AND template = ? AND id < ?",
                             (const char *[]){ repo, f->scope, name, id, NULL }, &r) == 0 &&
                  r && r->nrows == 1 && strtoll(r->rows[0][0], NULL, 10) > 0;
    db_result_free(r);
    if (earlier) snprintf(out, sz, "%s-%lld", name, (long long)f->id);
    else snprintf(out, sz, "%s", name);
}

/* ------------------------------------------------------------------ */
/* preview                                                              */
/* ------------------------------------------------------------------ */

static api_response_t preview(const api_request_t *req, int64_t repo_id, const finding_t *f)
{
    cJSON *body = NULL;
    api_response_t bad;
    if (body_object(req, &body, &bad) != 0) return bad;
    static const char *const keys[] = { "template", "params", NULL };
    const char *uk = unknown_key(body, keys);
    if (uk) {
        char msg[160];
        snprintf(msg, sizeof(msg), "unknown key '%.64s'; preview takes template and params", uk);
        cJSON_Delete(body);
        return api_error(400, msg);
    }
    const cJSON *jt = cJSON_GetObjectItemCaseSensitive(body, "template");
    if (jt && !cJSON_IsString(jt)) { cJSON_Delete(body); return api_error(400, "template must be a string"); }
    const cJSON *jp = cJSON_GetObjectItemCaseSensitive(body, "params");
    const cJSON *usep = jp ? jp : NULL;
    cJSON *stored = NULL;
    /* With no params given, an enabled finding previews what it runs. */
    if (!jp && f->params[0] && (!jt || strcmp(jt->valuestring, f->tmpl) == 0)) usep = stored = cJSON_Parse(f->params);
    const db_template_t *t = NULL;
    char *params = NULL;
    int rc = choose(f, jt ? jt->valuestring : NULL, usep, &t, &params, &bad);
    cJSON_Delete(stored);
    cJSON_Delete(body);
    if (rc != 0) return bad;

    cJSON *o = cJSON_CreateObject();
    if (!o) { free(params); return api_error(500, "out of memory"); }
    cJSON_AddNumberToObject(o, "finding", (double)f->id);
    cJSON_AddStringToObject(o, "scope", f->scope);
    cJSON_AddStringToObject(o, "template", t->name);
    cJSON *pj = cJSON_Parse(params);
    if (pj) cJSON_AddItemToObject(o, "params", pj);

    if (is_source(f)) {
        /* The daemon's own copy (9.2, built in D3): nothing to probe. */
        cJSON_AddStringToObject(o, "kind", "engine-copy");
        cJSON_AddStringToObject(o, "summary", "the daemon's own SQLite online backup: opened read-only with a 5 s "
                                "busy timeout, copied into the repository's tmp/ and stored beside the raw file");
        char out[PATH_MAX + 32];
        snprintf(out, sizeof(out), ".cbackup-captures%s", f->path);
        cJSON_AddStringToObject(o, "output", out);
        cJSON_AddBoolToObject(o, "allowed", 1);
        cJSON_AddNullToObject(o, "verified");
        cJSON_AddNullToObject(o, "probe");
        free(params);
        return json_out(200, o);
    }

    char hook[64];
    hook_name(repo_id, f, t->name, hook, sizeof(hook));
    char out[128], logp[128];
    snprintf(out, sizeof(out), ".cbackup-unit/hooks/%s.dump", hook);
    snprintf(logp, sizeof(logp), ".cbackup-unit/hooks/%s.log", hook);
    cJSON_AddStringToObject(o, "kind", t->kind == DBT_DUMP ? "dump" : "exec");
    cJSON_AddStringToObject(o, "summary", t->summary);
    if (t->kind == DBT_DUMP) cJSON_AddStringToObject(o, "output", out);
    cJSON_AddStringToObject(o, "log", logp);
    if (t->needs_quiesce) cJSON_AddBoolToObject(o, "needs_quiesce", 1);

    const char *unit = f->scope + 5;
    cJSON *res = NULL;
    char *text = NULL;
    size_t tlen = 0;
    status_t st = remote_db_probe(unit, t->name, params, f->path, &res, &text, &tlen);
    free(params);
    if (st != OK) {
        char msg[ERR_MSG_MAX];
        snprintf(msg, sizeof(msg), "%s", err_msg());
        err_clear();
        cJSON_Delete(o);
        free(text);
        return api_error(st == ERR_INVALID ? 409 : 502, msg);
    }
    const char *keep[] = { "container", "user", "argv", "post_argv" };
    for (size_t i = 0; i < sizeof(keep) / sizeof(keep[0]); i++) {
        cJSON *v = cJSON_DetachItemFromObjectCaseSensitive(res, keep[i]);
        if (v) cJSON_AddItemToObject(o, keep[i], v);
    }
    int allowed = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res, "allowed"));
    cJSON_AddBoolToObject(o, "allowed", allowed);
    if (!allowed) {
        char list[1024], line[1400];
        (void)remote_template_allowed(t->name, list, sizeof(list));
        policy_line(t->name, list, line, sizeof(line));
        cJSON_AddStringToObject(o, "policy_line", line);
    }
    cJSON *probe = cJSON_AddObjectToObject(o, "probe");
    int verified = 0;
    if (probe) {
        cJSON *pa = cJSON_DetachItemFromObjectCaseSensitive(res, "probe_argv");
        if (pa) cJSON_AddItemToObject(probe, "argv", pa);
        const cJSON *ex = cJSON_GetObjectItemCaseSensitive(res, "exit");
        int code = cJSON_IsNumber(ex) ? (int)ex->valuedouble : -1;
        int tmo = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res, "timed_out"));
        cJSON_AddNumberToObject(probe, "exit", code);
        cJSON_AddBoolToObject(probe, "timed_out", tmo);
        cJSON_AddStringToObject(probe, "output", text ? text : "");
        cJSON_AddBoolToObject(probe, "truncated", cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res, "truncated")));
        verified = code == 0 && !tmo;
        /* A probe refused for want of a credential says how to give one:
         * a template never prompts (B248). */
        const char *hint = verified || !text ? NULL : db_template_auth_hint(t, text, strlen(text));
        if (hint) cJSON_AddStringToObject(probe, "credentials", hint);
    }
    /* Unverified still enables (7.3): the preview says so, nothing more. */
    cJSON_AddBoolToObject(o, "verified", verified);
    free(text);
    cJSON_Delete(res);
    return json_out(200, o);
}

/* ------------------------------------------------------------------ */
/* enable, dismiss                                                      */
/* ------------------------------------------------------------------ */

static void what_of(const finding_t *f, char *out, size_t sz)
{
    snprintf(out, sz, "finding %lld (%s %s%s%s at %.1000s, %.1100s)", (long long)f->id, f->confidence,
             db_engine_title(db_engine_parse(f->engine)), f->version[0] ? " " : "", f->version, f->path, f->scope);
}

static api_response_t enable(const api_request_t *req, int64_t repo_id, const finding_t *f)
{
    cJSON *body = NULL;
    api_response_t bad;
    if (body_object(req, &body, &bad) != 0) return bad;
    static const char *const keys[] = { "template", "params", NULL };
    const char *uk = unknown_key(body, keys);
    if (uk) {
        char msg[160];
        snprintf(msg, sizeof(msg), "unknown key '%.64s'; enable takes template and params", uk);
        cJSON_Delete(body);
        return api_error(400, msg);
    }
    const cJSON *jt = cJSON_GetObjectItemCaseSensitive(body, "template");
    if (!cJSON_IsString(jt) || !jt->valuestring[0]) {
        cJSON_Delete(body);
        return api_error(400, "template is required: enabling names what it enables");
    }
    const db_template_t *t = NULL;
    char *params = NULL;
    int rc = choose(f, jt->valuestring, cJSON_GetObjectItemCaseSensitive(body, "params"), &t, &params, &bad);
    cJSON_Delete(body);
    if (rc != 0) return bad;

    if (is_unit(f)) {
        /* The helper's policy has the final say; one it would refuse is
         * refused here, with the line that would allow it (7.2). */
        char prov[SP_PROVIDER_MAX] = "";
        const char *u = f->scope + 5, *colon = strchr(u, ':');
        if (colon && (size_t)(colon - u) < sizeof(prov)) {
            memcpy(prov, u, (size_t)(colon - u));
            prov[colon - u] = '\0';
        }
        const sp_provider_ops_t *ops = NULL;
        sp_ctx_t *pc = NULL;
        if (sp_registry_get(prov, &ops, &pc) == OK && !remote_is_remote(ops)) {
            free(params);
            char msg[256];
            snprintf(msg, sizeof(msg), "provider %s runs in the daemon, not behind the helper; database templates "
                     "run only through the helper", prov);
            return api_error(409, msg);
        }
        err_clear();
        char list[1024];
        int allowed = remote_template_allowed(t->name, list, sizeof(list));
        if (allowed < 0) {
            free(params);
            return api_error(503, "the helper has not answered since this daemon started, so its policy cannot be "
                                  "checked; try again once it is connected (GET /providers)");
        }
        if (!allowed) {
            free(params);
            char line[1400], msg[1700];
            policy_line(t->name, list, line, sizeof(line));
            snprintf(msg, sizeof(msg), "template %s is not allowed by the helper's policy; add it there first: %s",
                     t->name, line);
            cJSON *o = cJSON_CreateObject();
            if (!o) return api_error(500, "out of memory");
            cJSON_AddStringToObject(o, "error", msg);
            cJSON_AddStringToObject(o, "policy_line", line);
            return json_out(409, o);
        }
    }

    char caller[256], repo[32], id[32], now[32];
    caller_of(req, caller, sizeof(caller));
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)f->id);
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    int n = 0;
    rc = db_write(g_db, "UPDATE db_findings SET state = 'enabled', template = ?, params = ?, decided_by = ?,"
                        " decided_at = ?, reason = NULL WHERE repo_id = ? AND id = ?",
                  (const char *[]){ t->name, params, caller, now, repo, id, NULL }, &n);
    if (rc != 0 || n != 1) {
        free(params);
        return api_error(rc != 0 ? 500 : 404, rc != 0 ? "cannot record the choice" : "no such finding");
    }
    char what[2400], msg[3200];
    what_of(f, what, sizeof(what));
    snprintf(msg, sizeof(msg), "%s: %s enabled with params %.300s by %s; it takes effect at the next backup",
             what, t->name, params, caller);
    free(params);
    journal_record(g_db, repo_id, "db", "db-enable", "success", 0, msg);
    log_info("database finding %lld: %s enabled by %s", (long long)f->id, t->name, caller);
    cJSON *o = finding_json(repo_id, f->id);
    return o ? json_out(200, o) : api_error(500, "cannot read the finding back");
}

#define DISMISS_REASON_MAX 512

static api_response_t dismiss(const api_request_t *req, int64_t repo_id, const finding_t *f)
{
    cJSON *body = NULL;
    api_response_t bad;
    if (body_object(req, &body, &bad) != 0) return bad;
    static const char *const keys[] = { "reason", NULL };
    const char *uk = unknown_key(body, keys);
    if (uk) {
        char msg[160];
        snprintf(msg, sizeof(msg), "unknown key '%.64s'; dismiss takes reason", uk);
        cJSON_Delete(body);
        return api_error(400, msg);
    }
    const cJSON *jr = cJSON_GetObjectItemCaseSensitive(body, "reason");
    if (!cJSON_IsString(jr) || !jr->valuestring[0] || strlen(jr->valuestring) > DISMISS_REASON_MAX) {
        cJSON_Delete(body);
        return api_error(400, "reason is required: 1 to 512 bytes saying why the finding is wrong or not wanted");
    }
    for (const unsigned char *c = (const unsigned char *)jr->valuestring; *c; c++)
        if (*c < 0x20 && *c != '\t') {
            cJSON_Delete(body);
            return api_error(400, "reason may not hold control characters");
        }
    char reason[DISMISS_REASON_MAX + 1];
    snprintf(reason, sizeof(reason), "%s", jr->valuestring);
    cJSON_Delete(body);
    char caller[256], repo[32], id[32], now[32];
    caller_of(req, caller, sizeof(caller));
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)f->id);
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    /* A dismissed finding runs nothing: whatever was enabled stops at the
     * next backup, and the choice is cleared with the state. */
    int n = 0;
    int rc = db_write(g_db, "UPDATE db_findings SET state = 'dismissed', template = NULL, params = NULL,"
                            " decided_by = ?, decided_at = ?, reason = ? WHERE repo_id = ? AND id = ?",
                      (const char *[]){ caller, now, reason, repo, id, NULL }, &n);
    if (rc != 0 || n != 1) return api_error(rc != 0 ? 500 : 404, rc != 0 ? "cannot record the dismissal" : "no such finding");
    char what[2400], msg[4096];
    what_of(f, what, sizeof(what));
    snprintf(msg, sizeof(msg), "%s: dismissed by %s%s%s: %s", what, caller,
             strcmp(f->state, "enabled") == 0 ? ", which stops its " : "",
             strcmp(f->state, "enabled") == 0 ? f->tmpl : "", reason);
    journal_record(g_db, repo_id, "db", "db-dismiss", "success", 0, msg);
    log_info("database finding %lld dismissed by %s", (long long)f->id, caller);
    cJSON *o = finding_json(repo_id, f->id);
    return o ? json_out(200, o) : api_error(500, "cannot read the finding back");
}

api_response_t db_actions_route_post(const api_request_t *req, int64_t repo_id, const char *rest)
{
    int64_t fid = 0;
    const char *action = NULL;
    if (!split_path(rest, &fid, &action)) return api_error(404, "not found");
    finding_t f;
    if (finding_load(repo_id, fid, &f) != 0) return api_error(404, "no such finding");
    if (strcmp(action, "preview") == 0) return preview(req, repo_id, &f);
    if (strcmp(action, "enable") == 0) return enable(req, repo_id, &f);
    return dismiss(req, repo_id, &f);
}
