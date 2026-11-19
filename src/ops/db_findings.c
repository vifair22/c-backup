#define _POSIX_C_SOURCE 200809L

#include "db_findings.h"

#include "db_txn.h"
#include "db_write.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <limits.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* Settings                                                             */
/* ------------------------------------------------------------------ */

static atomic_int g_enabled = 1;
static atomic_uint g_min_kb = 1024;
static atomic_int g_notify = 1;
static atomic_ullong g_uncovered_reads;

void db_detect_settings_set(const db_detect_settings_t *s)
{
    atomic_store(&g_enabled, s->enabled ? 1 : 0);
    atomic_store(&g_min_kb, s->sqlite_min_kb);
    atomic_store(&g_notify, s->notify_uncovered ? 1 : 0);
}

db_detect_settings_t db_detect_settings(void)
{
    db_detect_settings_t s = { atomic_load(&g_enabled), atomic_load(&g_min_kb),
                               atomic_load(&g_notify) };
    return s;
}

db_detector_t *db_detect_start(void)
{
    db_detect_settings_t s = db_detect_settings();
    if (!s.enabled) return NULL;
    db_detect_cfg_t cfg = { (uint64_t)s.sqlite_min_kb * 1024u };
    db_detector_t *d = NULL;
    if (db_detect_begin(&cfg, &d) != OK) {
        log_warn("database detection: cannot start, skipped: %s", err_msg());
        err_clear();
        return NULL;
    }
    return d;
}

uint64_t db_detect_uncovered_reads_total(void)
{
    return atomic_load(&g_uncovered_reads);
}

void db_detect_count_uncovered(const db_findings_t *f)
{
    if (!f) return;
    for (size_t i = 0; i < f->n; i++)
        if (f->v[i].confidence == DB_CONF_LIVE && f->v[i].coverage == DB_COV_UNCOVERED)
            atomic_fetch_add(&g_uncovered_reads, 1);
}

/* ------------------------------------------------------------------ */
/* Recording                                                            */
/* ------------------------------------------------------------------ */

void db_record_result_free(db_record_result_t *r)
{
    if (!r) return;
    free(r->ids);
    free(r->events);
    memset(r, 0, sizeof(*r));
}

static void add_event(db_record_result_t *r, db_event_kind_t kind, int64_t id, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));
static void add_event(db_record_result_t *r, db_event_kind_t kind, int64_t id, const char *fmt, ...)
{
    db_event_t *g = realloc(r->events, (r->n_events + 1) * sizeof(*g));
    if (!g) return;
    r->events = g;
    db_event_t *e = &g[r->n_events++];
    memset(e, 0, sizeof(*e));
    e->kind = kind;
    e->finding_id = id;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(e->text, sizeof(e->text), fmt, ap);
    va_end(ap);
}

static const char *scope_for(const db_finding_t *f, const char *const *roots, size_t n,
                             const char *scope)
{
    if (roots && f->root >= 0 && (size_t)f->root < n && roots[f->root]) return roots[f->root];
    return scope;
}

/* A sentence naming a finding: "live PostgreSQL 16 at /mnt/...". */
static void describe(const db_finding_t *f, char *buf, size_t sz)
{
    snprintf(buf, sz, "%s %s%s%s at %s", db_confidence_name(f->confidence),
             db_engine_title(f->engine), f->version[0] ? " " : "", f->version, f->path);
}

static status_t record_one(cutils_db_t *db, const char *repo, const char *snap, const char *now,
                           const db_finding_t *f, const char *scope, int notify,
                           int64_t *id_out, db_record_result_t *out)
{
    const char *engine = db_engine_name(f->engine);
    const char *conf = db_confidence_name(f->confidence);
    const char *cov = db_coverage_name(f->coverage);
    const char *sugg = f->suggestion ? f->suggestion : "";
    const char *why = f->coverage != DB_COV_NONE && f->coverage_why ? f->coverage_why : "";
    const char *method = f->method ? f->method : "";
    const char *opt = f->optional ? f->optional : "";
    int live = f->confidence == DB_CONF_LIVE;
    int uncovered = live && f->coverage == DB_COV_UNCOVERED;
    char what[PATH_MAX + 128];

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id, state, COALESCE(version, ''), COALESCE(coverage, ''),"
                       " COALESCE(alerted_at, 0) FROM db_findings"
                       " WHERE repo_id = ? AND scope = ? AND path = ? AND engine = ?",
                   (const char *[]){ repo, scope, f->path, engine, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "db findings: cannot look up %s", f->path);
    }
    describe(f, what, sizeof(what));

    if (!r || r->nrows == 0) {
        db_result_free(r);
        const char *state = f->suggestion ? "suggested" : "new";
        int alert = uncovered && notify;
        db_result_t *ir = NULL;
        if (db_execute(db, "INSERT INTO db_findings (repo_id, scope, engine, path, version,"
                           " confidence, coverage, suggestion, state, evidence, first_seen,"
                           " last_seen, last_snap_id, alerted_at, coverage_why, method, optional)"
                           " VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''),"
                           " ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''),"
                           " NULLIF(?, ''), NULLIF(?, '')) RETURNING id",
                       (const char *[]){ repo, scope, engine, f->path, f->version, conf, cov,
                                         sugg, state, f->evidence, now, now, snap,
                                         alert ? now : "", why, method, opt, NULL }, &ir) != 0 ||
            !ir || ir->nrows != 1) {
            db_result_free(ir);
            return set_error(ERR_IO, "db findings: cannot record %s", f->path);
        }
        int64_t id = strtoll(ir->rows[0][0], NULL, 10);
        db_result_free(ir);
        *id_out = id;
        if (live)
            add_event(out, DB_EVENT_NEW_LIVE, id, "database found: %s, %s (finding %lld)",
                      what, cov[0] ? cov : "coverage not assessed", (long long)id);
        if (alert)
            add_event(out, DB_EVENT_ALERT, id, "live database captured uncovered: %s; %s (finding %lld)",
                      what, f->coverage_why ? f->coverage_why : "", (long long)id);
        return OK;
    }

    int64_t id = strtoll(r->rows[0][0], NULL, 10);
    char old_state[16], old_version[32], old_cov[24];
    snprintf(old_state, sizeof(old_state), "%s", r->rows[0][1]);
    snprintf(old_version, sizeof(old_version), "%s", r->rows[0][2]);
    snprintf(old_cov, sizeof(old_cov), "%s", r->rows[0][3]);
    int was_alerted = strtoll(r->rows[0][4], NULL, 10) != 0;
    db_result_free(r);
    *id_out = id;

    /* A dismissal holds until the signature changes: the engine is part of
     * the key, so only the version can (section 4). */
    const char *state;
    if (strcmp(old_state, "enabled") == 0) state = "enabled";
    else if (strcmp(old_state, "dismissed") == 0 &&
             !(old_version[0] && f->version[0] && strcmp(old_version, f->version) != 0))
        state = "dismissed";
    else state = f->suggestion ? "suggested" : "new";

    int cov_changed = strcmp(old_cov, cov) != 0;
    /* A spell ends when the coverage changes; a relapse alerts again. */
    int alerted = was_alerted && !cov_changed;
    int alert = uncovered && notify && !alerted && strcmp(state, "dismissed") != 0;
    char id_s[32];
    snprintf(id_s, sizeof(id_s), "%lld", (long long)id);
    /* A detection run that made no snapshot (snap "") leaves the last
     * snapshot the finding was seen in as it was. */
    if (db_execute_non_query(db, "UPDATE db_findings SET version = NULLIF(?, ''), confidence = ?,"
                                 " coverage = NULLIF(?, ''), suggestion = NULLIF(?, ''), state = ?,"
                                 " evidence = ?, last_seen = ?,"
                                 " last_snap_id = COALESCE(NULLIF(?, ''), last_snap_id),"
                                 " alerted_at = CASE WHEN ? = '1' THEN ?"
                                 "                   WHEN ? = '1' THEN alerted_at ELSE NULL END,"
                                 " coverage_why = NULLIF(?, ''), method = NULLIF(?, ''),"
                                 " optional = NULLIF(?, '')"
                                 " WHERE id = ?",
                             (const char *[]){ f->version, conf, cov, sugg, state, f->evidence,
                                               now, snap, alert ? "1" : "0", now,
                                               alerted ? "1" : "0", why, method, opt, id_s,
                                               NULL }, NULL) != 0)
        return set_error(ERR_IO, "db findings: cannot update finding %lld", (long long)id);
    if (strcmp(state, "dismissed") == 0) return OK;
    if (live && cov_changed)
        add_event(out, DB_EVENT_COVERAGE, id, "database coverage changed: %s, %s to %s (finding %lld)",
                  what, old_cov[0] ? old_cov : "not assessed", cov[0] ? cov : "not assessed",
                  (long long)id);
    if (alert)
        add_event(out, DB_EVENT_ALERT, id, "live database captured uncovered: %s; %s (finding %lld)",
                  what, f->coverage_why ? f->coverage_why : "", (long long)id);
    return OK;
}

/* snap_id 0: a detection run with no snapshot (db_findings_record_detect),
 * which never alerts and tells its own rows by last_seen. */
static status_t record_impl(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                            const db_findings_t *f,
                            const char *const *root_scopes, size_t n_root_scopes,
                            const char *scope, db_record_result_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!db || !f) return set_error(ERR_INVALID, "db findings: nothing to record");
    if (f->n && !(out->ids = calloc(f->n, sizeof(*out->ids))))
        return set_error(ERR_NOMEM, "db findings");
    char repo[32], snap[32] = "", now[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (snap_id > 0) snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    /* A detection run predicts the capture rather than making one, so its
     * coverage is not what the alert is about (7.3): the next backup that
     * captures the database uncovered alerts. */
    int notify = snap_id > 0 && db_detect_settings().notify_uncovered;

    if (db_txn_begin(db) != OK) {
        db_record_result_free(out);
        return set_error_wrap(ERR_IO, "db findings: cannot begin");
    }
    status_t st = OK;
    for (size_t i = 0; i < f->n && st == OK; i++) {
        const char *sc = scope_for(&f->v[i], root_scopes, n_root_scopes, scope);
        if (!sc) continue;
        st = record_one(db, repo, snap, now, &f->v[i], sc, notify, &out->ids[i], out);
    }
    /* The rows this run's scopes no longer have: a database removed, or a
     * file that stopped looking like one. */
    size_t n_sc = root_scopes ? n_root_scopes : (scope ? 1 : 0);
    for (size_t i = 0; i < n_sc && st == OK; i++) {
        const char *sc = root_scopes ? root_scopes[i] : scope;
        if (!sc) continue;
        int rc = snap_id > 0
            ? db_execute_non_query(db, "DELETE FROM db_findings WHERE repo_id = ? AND scope = ?"
                                       " AND state IN ('new', 'suggested')"
                                       " AND (last_snap_id IS NULL OR last_snap_id <> ?)",
                                   (const char *[]){ repo, sc, snap, NULL }, NULL)
            : db_execute_non_query(db, "DELETE FROM db_findings WHERE repo_id = ? AND scope = ?"
                                       " AND state IN ('new', 'suggested') AND last_seen <> ?",
                                   (const char *[]){ repo, sc, now, NULL }, NULL);
        if (rc != 0)
            st = set_error(ERR_IO, "db findings: cannot drop stale findings for %s", sc);
    }
    if (st != OK) {
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        db_txn_rollback(db);
        db_record_result_free(out);
        return set_error(st, "%s", why);
    }
    if (db_txn_commit(db) != OK) {
        db_record_result_free(out);
        return set_error_wrap(ERR_IO, "db findings: cannot commit");
    }
    return OK;
}

status_t db_findings_record(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                            const db_findings_t *f,
                            const char *const *root_scopes, size_t n_root_scopes,
                            const char *scope, db_record_result_t *out)
{
    if (snap_id <= 0) {
        memset(out, 0, sizeof(*out));
        return set_error(ERR_INVALID, "db findings: no snapshot to record against");
    }
    return record_impl(db, repo_id, snap_id, f, root_scopes, n_root_scopes, scope, out);
}

status_t db_findings_record_detect(cutils_db_t *db, int64_t repo_id, const db_findings_t *f,
                                   const char *scope, db_record_result_t *out)
{
    return record_impl(db, repo_id, 0, f, NULL, 0, scope, out);
}

/* ------------------------------------------------------------------ */
/* Nextcloud links                                                      */
/* ------------------------------------------------------------------ */

/* capture.group of a unit, "" when it has none. */
static void unit_group(cutils_db_t *db, const char *repo, const char *unit_key, char *buf, size_t sz)
{
    buf[0] = '\0';
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT value FROM unit_policy WHERE repo_id = ? AND unit_key = ?"
                       " AND key = 'capture.group'",
                   (const char *[]){ repo, unit_key, NULL }, &r) == 0 && r && r->nrows == 1)
        snprintf(buf, sz, "%s", r->rows[0][0]);
    db_result_free(r);
}

static int names_have(const cJSON *ev, const char *name)
{
    const cJSON *it = NULL;
    cJSON_ArrayForEach(it, cJSON_GetObjectItemCaseSensitive(ev, "unit_names")) {
        const char *v = cJSON_GetStringValue(it);
        if (v && strcmp(v, name) == 0) return 1;
    }
    return 0;
}

/* The unit whose recorded findings say it answers to host, with an engine
 * row of that name; 1 with scope_out set. */
static int find_db_unit(cutils_db_t *db, const char *repo, const char *own_scope,
                        const char *engine, const char *host, char *scope_out, size_t sz)
{
    /* A prefilter only; the JSON decides. host is made of the characters
     * nextcloud_dbhost allows, of which only '_' is special to LIKE. */
    char pat[600];
    size_t o = 0;
    pat[o++] = '%';
    pat[o++] = '"';
    for (const char *c = host; *c && o + 4 < sizeof(pat); c++) {
        if (*c == '_' || *c == '%' || *c == '\\') pat[o++] = '\\';
        pat[o++] = *c;
    }
    pat[o++] = '"';
    pat[o++] = '%';
    pat[o] = '\0';
    db_result_t *r = NULL;
    int found = 0;
    if (db_execute(db, "SELECT scope, evidence FROM db_findings WHERE repo_id = ? AND scope <> ?"
                       " AND engine = ? AND scope LIKE 'unit:%' AND evidence LIKE ? ESCAPE '\\'"
                       " ORDER BY id",
                   (const char *[]){ repo, own_scope, engine, pat, NULL }, &r) == 0) {
        for (int i = 0; r && i < r->nrows && !found; i++) {
            cJSON *ev = cJSON_Parse(r->rows[i][1]);
            if (ev && names_have(ev, host)) {
                snprintf(scope_out, sz, "%s", r->rows[i][0]);
                found = 1;
            }
            cJSON_Delete(ev);
        }
    }
    db_result_free(r);
    return found;
}

/* A live Nextcloud whose database is not in its capture: its files alone
 * were judged, and whatever that said, the pair is not one instant. A
 * live read stays uncovered, which is worse. */
static void nextcloud_split(db_finding_t *x, const char *why, const char *suggestion)
{
    if (x->confidence != DB_CONF_LIVE || x->coverage == DB_COV_NONE ||
        x->coverage == DB_COV_UNCOVERED)
        return;
    char *w = strdup(why);
    if (!w) return;
    free(x->coverage_why);
    x->coverage_why = w;
    x->coverage = DB_COV_SPLIT;
    x->suggestion = suggestion;
    x->optional = NULL;
}

void db_findings_link_nextcloud(cutils_db_t *db, int64_t repo_id, const char *scope,
                                db_findings_t *f, db_unit_lookup_fn lookup, void *lookup_ctx)
{
    if (!db || !f || !scope) return;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    for (size_t i = 0; i < f->n; i++) {
        db_finding_t *x = &f->v[i];
        if (x->engine != DB_ENGINE_NEXTCLOUD || !x->evidence) continue;
        cJSON *ev = cJSON_Parse(x->evidence);
        cJSON *link = ev ? cJSON_GetObjectItemCaseSensitive(ev, "link") : NULL;
        const char *host = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(link, "host"));
        const cJSON *res = cJSON_GetObjectItemCaseSensitive(link, "resolved");
        if (!host || !*host || !cJSON_IsNull(res)) { cJSON_Delete(ev); continue; }
        /* The dbtype, not the version: the version is Nextcloud's own (B249). */
        const char *dbtype = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(link, "dbtype"));
        const char *engine = dbtype && strcmp(dbtype, "pgsql") == 0 ? "postgres" : "mysql";
        char other[600];
        /* First the units whose recorded findings answer to the name, then
         * any unit the host has by that name, backed up here or not: the
         * database's unit is often backed up after Nextcloud's, or never
         * (B249). */
        int recorded = find_db_unit(db, repo, scope, engine, host, other, sizeof(other));
        int listed = !recorded && lookup && lookup(lookup_ctx, host, other, sizeof(other)) &&
                     strcmp(other, strncmp(scope, "unit:", 5) == 0 ? scope + 5 : scope) != 0;
        if (listed) {
            /* The scope form, as a recorded one is. */
            char k[600];
            snprintf(k, sizeof(k), "unit:%.590s", other);
            snprintf(other, sizeof(other), "%s", k);
        }
        if (!recorded && !listed) {
            char note[400];
            snprintf(note, sizeof(note), "no unit on this host answers to %.200s", host);
            cJSON_DeleteItemFromObjectCaseSensitive(link, "note");
            cJSON_AddStringToObject(link, "note", note);
            char why[400];
            snprintf(why, sizeof(why), "its database (%.200s) is not in this capture: no unit "
                     "on this host answers to that name", host);
            nextcloud_split(x, why, NULL);
        } else {
            /* Two units are two captures unless they share a capture
             * group (container-backup.md 6.2). */
            char g1[256], g2[256];
            unit_group(db, repo, strncmp(scope, "unit:", 5) == 0 ? scope + 5 : scope, g1, sizeof(g1));
            unit_group(db, repo, other + 5, g2, sizeof(g2));
            int grouped = g1[0] && strcmp(g1, g2) == 0;
            cJSON_ReplaceItemInObjectCaseSensitive(link, "resolved", cJSON_CreateString(other));
            cJSON_AddBoolToObject(link, "consistent", grouped);
            char note[400];
            snprintf(note, sizeof(note), "%s%s",
                     grouped ? "its database is in another unit of the same capture group: one capture"
                             : "its database is in another unit, captured at a different instant",
                     recorded ? "" : "; that unit has no database findings in this repository yet");
            cJSON_AddStringToObject(link, "note", note);
            if (!grouped) {
                char why[700];
                snprintf(why, sizeof(why), "its database is in another unit (%.600s), captured at "
                         "a different instant", other + 5);
                nextcloud_split(x, why, "group-units");
                /* Dormant: at rest, nothing to suggest (section 2.1). A
                 * live read stays uncovered, and grouping still helps. */
                if (x->confidence == DB_CONF_LIVE) x->suggestion = "group-units";
            }
        }
        char *txt = cJSON_PrintUnformatted(ev);
        char *dup = txt ? strdup(txt) : NULL;
        cJSON_free(txt);
        if (dup) { free(x->evidence); x->evidence = dup; }
        cJSON_Delete(ev);
    }
}

/* ------------------------------------------------------------------ */
/* JSON                                                                 */
/* ------------------------------------------------------------------ */

char *db_findings_json(const db_findings_t *f, const int64_t *ids)
{
    cJSON *o = cJSON_CreateObject();
    cJSON *arr = o ? cJSON_AddArrayToObject(o, "findings") : NULL;
    if (!arr) { cJSON_Delete(o); return NULL; }
    size_t n = f ? f->n : 0, listed = 0;
    for (size_t i = 0; i < n && listed < DB_FINDINGS_LISTED_MAX; i++) {
        const db_finding_t *x = &f->v[i];
        cJSON *e = cJSON_CreateObject();
        if (!e) break;
        cJSON_AddItemToArray(arr, e);
        listed++;
        if (ids && ids[i]) cJSON_AddNumberToObject(e, "id", (double)ids[i]);
        cJSON_AddStringToObject(e, "engine", db_engine_name(x->engine));
        if (x->version[0]) cJSON_AddStringToObject(e, "version", x->version);
        cJSON_AddStringToObject(e, "path", x->path);
        cJSON_AddStringToObject(e, "confidence", db_confidence_name(x->confidence));
        if (x->coverage != DB_COV_NONE) {
            cJSON_AddStringToObject(e, "coverage", db_coverage_name(x->coverage));
            cJSON_AddStringToObject(e, "coverage_why", x->coverage_why ? x->coverage_why : "");
        }
        if (x->method) cJSON_AddStringToObject(e, "method", x->method);
        if (x->suggestion) cJSON_AddStringToObject(e, "suggestion", x->suggestion);
        if (x->optional) cJSON_AddStringToObject(e, "optional", x->optional);
        if (x->unsettled) cJSON_AddNumberToObject(e, "unsettled", x->unsettled);
        cJSON *ev = x->evidence ? cJSON_Parse(x->evidence) : NULL;
        if (ev) cJSON_AddItemToObject(e, "evidence", ev);
    }
    cJSON_AddNumberToObject(o, "listed", (double)listed);
    cJSON_AddNumberToObject(o, "total", (double)n);
    cJSON_AddNumberToObject(o, "sqlite_small", f ? (double)f->sqlite_small : 0.0);
    char *s = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    if (!s) return NULL;
    char *r = strdup(s);
    cJSON_free(s);
    return r;
}
