#define _POSIX_C_SOURCE 200809L

#include "unit_json.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const k_flags[] = {
    "hook-pre-ok", "frozen", "paused", "fs-snapshot", "watchdog-released",
};

char *unit_json_encode(const unit_json_t *u, size_t *len)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) { set_error(ERR_NOMEM, "unit.json"); return NULL; }
    const char *cn = sp_consistency_name(u->consistency);
    int ok = cJSON_AddStringToObject(o, "format", UNIT_JSON_FORMAT) &&
             cJSON_AddNumberToObject(o, "version", UNIT_JSON_VERSION) &&
             cJSON_AddStringToObject(o, "provider", u->provider) &&
             cJSON_AddStringToObject(o, "unit_id", u->unit_id) &&
             cJSON_AddStringToObject(o, "name", u->name ? u->name : "") &&
             cJSON_AddStringToObject(o, "consistency", cn ? cn : "inconsistent");
    cJSON *flags = ok ? cJSON_AddArrayToObject(o, "consist_flags") : NULL;
    for (unsigned b = 0; flags && b < sizeof(k_flags) / sizeof(k_flags[0]); b++)
        if (u->consist_flags & (1u << b))
            cJSON_AddItemToArray(flags, cJSON_CreateString(k_flags[b]));
    ok = ok && flags &&
         cJSON_AddNumberToObject(o, "freeze_ms", (double)u->freeze_ms) &&
         cJSON_AddNumberToObject(o, "pause_ms", (double)u->pause_ms);
    cJSON *items = ok ? cJSON_AddArrayToObject(o, "items") : NULL;
    for (size_t i = 0; items && i < u->n_items; i++) {
        const unit_json_item_t *it = &u->items[i];
        cJSON *j = cJSON_CreateObject();
        if (!j) { ok = 0; break; }
        cJSON_AddItemToArray(items, j);
        ok = cJSON_AddStringToObject(j, "id", it->id) &&
             cJSON_AddStringToObject(j, "kind", it->kind) &&
             cJSON_AddStringToObject(j, "path", it->path ? it->path : "");
        if (ok && strcmp(it->kind, "tree") != 0)
            ok = cJSON_AddNumberToObject(j, "size", (double)it->size) != NULL;
        if (ok && it->format[0]) ok = cJSON_AddStringToObject(j, "format", it->format) != NULL;
        if (ok && it->source) ok = cJSON_AddStringToObject(j, "source", it->source) != NULL;
        if (ok) ok = cJSON_AddStringToObject(j, "read_mode", it->read_mode[0] ? it->read_mode : "full") &&
                     cJSON_AddStringToObject(j, "reason", it->reason);
        if (ok && strcmp(it->kind, "block") == 0)
            ok = cJSON_AddNumberToObject(j, "bytes_read", (double)it->bytes_read) != NULL;
        if (ok && it->have_incr) {
            cJSON *x = cJSON_AddObjectToObject(j, "incremental");
            ok = x && cJSON_AddStringToObject(x, "checkpoint", it->checkpoint) &&
                 cJSON_AddNumberToObject(x, "base_snapshot", (double)it->base_snapshot) &&
                 cJSON_AddNumberToObject(x, "estimate", (double)it->estimate) &&
                 cJSON_AddNumberToObject(x, "data_bytes", (double)it->data_bytes) &&
                 cJSON_AddNumberToObject(x, "clusters", (double)it->clusters) &&
                 cJSON_AddNumberToObject(x, "ranges", (double)it->ranges) &&
                 cJSON_AddNumberToObject(x, "dirty_bytes", (double)it->dirty_bytes) &&
                 cJSON_AddNumberToObject(x, "resync_bytes", (double)it->resync_bytes) &&
                 cJSON_AddNumberToObject(x, "delta_full_pct", it->delta_full_pct);
            if (ok && it->audited) {
                cJSON *a = cJSON_AddObjectToObject(x, "audit");
                ok = a && cJSON_AddBoolToObject(a, "passed", !it->audit_bad) &&
                     cJSON_AddNumberToObject(a, "leaves_checked", (double)it->audit_leaves) &&
                     cJSON_AddNumberToObject(a, "bad_bytes", (double)it->audit_bad_bytes);
                if (ok && it->audit_bad)
                    ok = cJSON_AddNumberToObject(a, "first_bad", (double)it->audit_first_bad) != NULL;
            }
        }
        if (!ok) break;
    }
    cJSON *w = ok && items ? cJSON_AddArrayToObject(o, "warnings") : NULL;
    for (size_t i = 0; w && i < u->n_warnings; i++)
        cJSON_AddItemToArray(w, cJSON_CreateString(u->warnings[i]));
    char *txt = (ok && w) ? cJSON_PrintUnformatted(o) : NULL;
    cJSON_Delete(o);
    if (!txt) { set_error(ERR_NOMEM, "unit.json: cannot serialise"); return NULL; }
    size_t n = strlen(txt);
    if (n > UNIT_JSON_MAX) {
        cJSON_free(txt);
        set_error(ERR_TOO_LARGE, "unit.json would be %zu bytes, over %zu", n, UNIT_JSON_MAX);
        return NULL;
    }
    /* Copied so the caller frees it with free(), whatever cJSON's hooks. */
    char *out = malloc(n + 1);
    if (out) memcpy(out, txt, n + 1);
    cJSON_free(txt);
    if (!out) { set_error(ERR_NOMEM, "unit.json"); return NULL; }
    if (len) *len = n;
    return out;
}

static const char *js(const cJSON *o, const char *k)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    return cJSON_IsString(v) ? v->valuestring : NULL;
}

static uint64_t jn(const cJSON *o, const char *k)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (!cJSON_IsNumber(v) || v->valuedouble < 0 || v->valuedouble > 1.8e19) return 0;
    return (uint64_t)v->valuedouble;
}

void unit_json_free(unit_json_t *u)
{
    if (!u) return;
    for (size_t i = 0; i < u->n_items; i++) { free(u->items[i].path); free(u->items[i].source); }
    free(u->items);
    for (size_t i = 0; i < u->n_warnings; i++) free(u->warnings[i]);
    free(u->warnings);
    free(u->name);
    memset(u, 0, sizeof(*u));
}

status_t unit_json_parse(const void *buf, size_t len, unit_json_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!buf || len == 0 || len > UNIT_JSON_MAX)
        return set_error(ERR_CORRUPT, "unit.json: empty or over %zu bytes", UNIT_JSON_MAX);
    char *txt = malloc(len + 1);
    if (!txt) return set_error(ERR_NOMEM, "unit.json");
    memcpy(txt, buf, len);
    txt[len] = '\0';
    cJSON *o = cJSON_Parse(txt);
    free(txt);
    if (!o || !cJSON_IsObject(o)) {
        cJSON_Delete(o);
        return set_error(ERR_CORRUPT, "unit.json: not a JSON object");
    }
    status_t st = OK;
    const char *fmt = js(o, "format"), *prov = js(o, "provider"), *uid = js(o, "unit_id");
    const char *name = js(o, "name"), *cons = js(o, "consistency");
    uint64_t ver = jn(o, "version");
    sp_consistency_t c = SP_CONSIST_INCONSISTENT;
    if (!fmt || strcmp(fmt, UNIT_JSON_FORMAT) != 0)
        st = set_error(ERR_CORRUPT, "unit.json: not a cbackup-unit document");
    else if (ver < 1 || ver > UNIT_JSON_VERSION)
        st = set_error(ERR_CORRUPT, "unit.json: version %llu is not one this build reads",
                       (unsigned long long)ver);
    else if (!prov || !*prov || strlen(prov) >= SP_PROVIDER_MAX ||
             !uid || !*uid || strlen(uid) >= SP_UNIT_ID_MAX)
        st = set_error(ERR_CORRUPT, "unit.json: provider or unit id missing or too long");
    else if (sp_consistency_parse(cons, &c) != 0)
        st = set_error(ERR_CORRUPT, "unit.json: unknown consistency '%s'", cons ? cons : "");
    if (st == OK) {
        out->version = (int)ver;
        snprintf(out->provider, sizeof(out->provider), "%s", prov);
        snprintf(out->unit_id, sizeof(out->unit_id), "%s", uid);
        out->consistency = c;
        out->freeze_ms = jn(o, "freeze_ms");
        out->pause_ms = jn(o, "pause_ms");
        if (!(out->name = strdup(name ? name : ""))) st = set_error(ERR_NOMEM, "unit.json");
        const cJSON *f;
        cJSON_ArrayForEach(f, cJSON_GetObjectItemCaseSensitive(o, "consist_flags")) {
            if (!cJSON_IsString(f)) continue;
            for (unsigned b = 0; b < sizeof(k_flags) / sizeof(k_flags[0]); b++)
                if (strcmp(f->valuestring, k_flags[b]) == 0) out->consist_flags |= 1u << b;
        }
    }
    const cJSON *items = cJSON_GetObjectItemCaseSensitive(o, "items");
    int n = st == OK ? cJSON_GetArraySize(items) : 0;
    if (n > 0) {
        out->items = calloc((size_t)n, sizeof(*out->items));
        if (!out->items) st = set_error(ERR_NOMEM, "unit.json");
    }
    const cJSON *it;
    cJSON_ArrayForEach(it, items) {
        if (st != OK) break;
        const char *id = js(it, "id"), *kind = js(it, "kind"), *path = js(it, "path");
        sp_item_kind_t k;
        if (!id || strlen(id) >= SP_ITEM_ID_MAX || sp_item_kind_parse(kind, &k) != 0 || !path) {
            st = set_error(ERR_CORRUPT, "unit.json: an item lacks an id, kind or path");
            break;
        }
        unit_json_item_t *d = &out->items[out->n_items++];
        snprintf(d->id, sizeof(d->id), "%s", id);
        snprintf(d->kind, sizeof(d->kind), "%s", kind);
        d->size = jn(it, "size");
        d->bytes_read = jn(it, "bytes_read");
        const char *v;
        if ((v = js(it, "format")))    snprintf(d->format, sizeof(d->format), "%s", v);
        if ((v = js(it, "read_mode"))) snprintf(d->read_mode, sizeof(d->read_mode), "%s", v);
        if ((v = js(it, "reason")))    snprintf(d->reason, sizeof(d->reason), "%s", v);
        d->path = strdup(path);
        v = js(it, "source");
        d->source = v ? strdup(v) : NULL;
        if (!d->path || (v && !d->source)) st = set_error(ERR_NOMEM, "unit.json");
    }
    const cJSON *w;
    cJSON_ArrayForEach(w, cJSON_GetObjectItemCaseSensitive(o, "warnings")) {
        if (st != OK) break;
        if (!cJSON_IsString(w)) continue;
        char **g = realloc(out->warnings, (out->n_warnings + 1) * sizeof(*g));
        if (!g) { st = set_error(ERR_NOMEM, "unit.json"); break; }
        out->warnings = g;
        if (!(g[out->n_warnings] = strdup(w->valuestring))) { st = set_error(ERR_NOMEM, "unit.json"); break; }
        out->n_warnings++;
    }
    cJSON_Delete(o);
    if (st != OK) unit_json_free(out);
    return st;
}
