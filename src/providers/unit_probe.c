#define _POSIX_C_SOURCE 200809L

#include "unit_probe.h"
#include "helper_proto.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

sp_unit_probe_t *sp_unit_probe_new(void)
{
    return calloc(1, sizeof(sp_unit_probe_t));
}

/* ------------------------------------------------------------------ */
/* Reading                                                             */
/* ------------------------------------------------------------------ */

/* The field a refusal names, built as the walk goes down. */
typedef struct { char at[128]; } where_t;

static status_t bad(const where_t *w, const char *field, const char *what)
{
    return set_error(ERR_INVALID, "units.probe: %s%s%s %s", w->at, w->at[0] ? "." : "", field, what);
}

/* A bounded string into dst (dst_sz bytes, NUL included), the empty string
 * allowed only when may_empty. Absent is "" when optional. */
static status_t take_str(const where_t *w, const cJSON *o, const char *k, char *dst, size_t dst_sz,
                         int optional, int may_empty)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    if (!v && optional) { dst[0] = '\0'; return OK; }
    if (!cJSON_IsString(v)) return bad(w, k, "must be a string");
    const char *s = v->valuestring;
    if (!helper_str_ok(s, dst_sz - 1)) return bad(w, k, "is too long, or not UTF-8 without control characters");
    if (!may_empty && !s[0]) return bad(w, k, "must not be empty");
    snprintf(dst, dst_sz, "%s", s);
    return OK;
}

/* An optional array: NULL when absent, an error when not an array. */
static status_t take_arr(const where_t *w, const cJSON *o, const char *k, const cJSON **out)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    *out = NULL;
    if (!v) return OK;
    if (!cJSON_IsArray(v)) return bad(w, k, "must be an array");
    *out = v;
    return OK;
}

static size_t arr_cap(const cJSON *a, size_t max, int *truncated)
{
    int n = a ? cJSON_GetArraySize(a) : 0;
    if ((size_t)n > max) { *truncated = 1; return max; }
    return (size_t)n;
}

static status_t container_from(const cJSON *jc, size_t idx, sp_probe_container_t *c, int *truncated)
{
    where_t w;
    snprintf(w.at, sizeof(w.at), "containers[%zu]", idx);
    if (!cJSON_IsObject(jc)) return set_error(ERR_INVALID, "units.probe: %s must be an object", w.at);
    status_t st = take_str(&w, jc, "name", c->name, sizeof(c->name), 0, 0);
    if (st == OK) st = take_str(&w, jc, "image", c->image, sizeof(c->image), 1, 1);
    if (st != OK) return st;
    const cJSON *run = cJSON_GetObjectItemCaseSensitive(jc, "running");
    if (!cJSON_IsBool(run)) return bad(&w, "running", "must be true or false");
    c->running = cJSON_IsTrue(run);

    const cJSON *al = NULL, *pr = NULL, *po = NULL, *ow = NULL;
    if ((st = take_arr(&w, jc, "aliases", &al)) != OK || (st = take_arr(&w, jc, "processes", &pr)) != OK ||
        (st = take_arr(&w, jc, "ports", &po)) != OK || (st = take_arr(&w, jc, "open_write", &ow)) != OK)
        return st;

    /* Duplicates are dropped rather than refused: the reader of a probe
     * only asks whether a name is there. Counting is over what is kept. */
    const cJSON *e;
    size_t k = 0;
    cJSON_ArrayForEach(e, al) {
        char a[SP_PROBE_NAME_MAX];
        where_t wa;
        snprintf(wa.at, sizeof(wa.at), "%.40s.aliases[%zu]", w.at, k++);
        if (!cJSON_IsString(e) || !helper_str_ok(e->valuestring, sizeof(a) - 1) || !e->valuestring[0])
            return set_error(ERR_INVALID, "units.probe: %s must be a non-empty string of at most %zu bytes "
                             "without control characters", wa.at, sizeof(a) - 1);
        snprintf(a, sizeof(a), "%s", e->valuestring);
        int dup = 0;
        for (size_t i = 0; i < c->n_aliases && !dup; i++) dup = strcmp(c->aliases[i], a) == 0;
        if (dup) continue;
        if (c->n_aliases == SP_PROBE_ALIASES_MAX) { *truncated = 1; continue; }
        memcpy(c->aliases[c->n_aliases++], a, sizeof(a));
    }

    size_t np = arr_cap(pr, SP_PROBE_PROCS_MAX, truncated);
    if (np && !(c->procs = calloc(np, sizeof(*c->procs)))) return set_error(ERR_NOMEM, "units.probe");
    k = 0;
    cJSON_ArrayForEach(e, pr) {
        if (k == np) break;
        where_t wp;
        snprintf(wp.at, sizeof(wp.at), "%.40s.processes[%zu]", w.at, k);
        if (!cJSON_IsObject(e)) return set_error(ERR_INVALID, "units.probe: %s must be an object", wp.at);
        sp_probe_proc_t *p = &c->procs[k];
        if ((st = take_str(&wp, e, "comm", p->comm, sizeof(p->comm), 0, 0)) != OK ||
            (st = take_str(&wp, e, "args", p->args, sizeof(p->args), 1, 1)) != OK)
            return st;
        c->n_procs = ++k;
    }

    size_t nq = arr_cap(po, SP_PROBE_PORTS_MAX, truncated);
    if (nq && !(c->ports = calloc(nq, sizeof(*c->ports)))) return set_error(ERR_NOMEM, "units.probe");
    k = 0;
    cJSON_ArrayForEach(e, po) {
        double d = cJSON_IsNumber(e) ? e->valuedouble : -1;
        if (d != (double)(long)d || d < 1 || d > 65535)
            return set_error(ERR_INVALID, "units.probe: %s.ports[%zu] must be an integer from 1 to 65535", w.at, k);
        k++;
        uint16_t port = (uint16_t)d;
        int dup = 0;
        for (size_t i = 0; i < c->n_ports && !dup; i++) dup = c->ports[i] == port;
        if (dup || c->n_ports == nq) continue;
        c->ports[c->n_ports++] = port;
    }

    size_t no = arr_cap(ow, SP_PROBE_OPEN_MAX, truncated);
    if (no && !(c->open = calloc(no, sizeof(*c->open)))) return set_error(ERR_NOMEM, "units.probe");
    k = 0;
    cJSON_ArrayForEach(e, ow) {
        if (k == no) break;
        where_t wo;
        snprintf(wo.at, sizeof(wo.at), "%.40s.open_write[%zu]", w.at, k);
        if (!cJSON_IsObject(e)) return set_error(ERR_INVALID, "units.probe: %s must be an object", wo.at);
        const cJSON *jp = cJSON_GetObjectItemCaseSensitive(e, "path");
        if (!cJSON_IsString(jp) || !helper_path_clean(jp->valuestring))
            return bad(&wo, "path", "must be a clean absolute path");
        sp_probe_open_t *f = &c->open[k];
        if ((st = take_str(&wo, e, "comm", f->comm, sizeof(f->comm), 1, 1)) != OK) return st;
        if (!(f->path = strdup(jp->valuestring))) return set_error(ERR_NOMEM, "units.probe");
        c->n_open = ++k;
    }
    return OK;
}

status_t sp_unit_probe_from_json(const cJSON *o, sp_unit_probe_t **out)
{
    *out = NULL;
    if (!cJSON_IsObject(o)) return set_error(ERR_INVALID, "units.probe: the answer must be an object");
    const cJSON *cs = cJSON_GetObjectItemCaseSensitive(o, "containers");
    const cJSON *tr = cJSON_GetObjectItemCaseSensitive(o, "truncated");
    if (!cJSON_IsArray(cs)) return set_error(ERR_INVALID, "units.probe: containers must be an array");
    if (tr && !cJSON_IsBool(tr)) return set_error(ERR_INVALID, "units.probe: truncated must be true or false");
    sp_unit_probe_t *p = sp_unit_probe_new();
    if (!p) return set_error(ERR_NOMEM, "units.probe");
    p->truncated = cJSON_IsTrue(tr);
    size_t n = arr_cap(cs, SP_PROBE_CONTAINERS_MAX, &p->truncated);
    if (n && !(p->v = calloc(n, sizeof(*p->v)))) { sp_unit_probe_free(p); return set_error(ERR_NOMEM, "units.probe"); }
    status_t st = OK;
    const cJSON *jc;
    size_t i = 0;
    cJSON_ArrayForEach(jc, cs) {
        if (i == n) break;
        /* Counted before it is filled, so the free sees what a failure
         * part-way through a container allocated. */
        p->n = i + 1;
        if ((st = container_from(jc, i, &p->v[i], &p->truncated)) != OK) break;
        i++;
    }
    if (st != OK) { sp_unit_probe_free(p); return st; }
    *out = p;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Writing                                                             */
/* ------------------------------------------------------------------ */

static cJSON *container_to(const sp_probe_container_t *c)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    cJSON *al = NULL, *pr = NULL, *po = NULL, *ow = NULL;
    int ok = cJSON_AddStringToObject(o, "name", c->name) && cJSON_AddStringToObject(o, "image", c->image) &&
             cJSON_AddBoolToObject(o, "running", c->running) &&
             (al = cJSON_AddArrayToObject(o, "aliases")) && (pr = cJSON_AddArrayToObject(o, "processes")) &&
             (po = cJSON_AddArrayToObject(o, "ports")) && (ow = cJSON_AddArrayToObject(o, "open_write"));
    for (size_t i = 0; ok && i < c->n_aliases; i++) {
        cJSON *s = cJSON_CreateString(c->aliases[i]);
        ok = s && cJSON_AddItemToArray(al, s);
    }
    for (size_t i = 0; ok && i < c->n_procs; i++) {
        cJSON *x = cJSON_CreateObject();
        ok = x && cJSON_AddItemToArray(pr, x) && cJSON_AddStringToObject(x, "comm", c->procs[i].comm) &&
             cJSON_AddStringToObject(x, "args", c->procs[i].args);
    }
    for (size_t i = 0; ok && i < c->n_ports; i++) {
        cJSON *x = cJSON_CreateNumber(c->ports[i]);
        ok = x && cJSON_AddItemToArray(po, x);
    }
    for (size_t i = 0; ok && i < c->n_open; i++) {
        cJSON *x = cJSON_CreateObject();
        ok = x && cJSON_AddItemToArray(ow, x) &&
             cJSON_AddStringToObject(x, "path", c->open[i].path ? c->open[i].path : "") &&
             cJSON_AddStringToObject(x, "comm", c->open[i].comm);
    }
    if (!ok) { cJSON_Delete(o); return NULL; }
    return o;
}

static size_t printed_len(const cJSON *o)
{
    char *t = cJSON_PrintUnformatted(o);
    size_t n = t ? strlen(t) : (size_t)-1;
    cJSON_free(t);
    return n;
}

/*
 * Drops the last element of the named array in the last container that
 * has one, returning the bytes that saves (the element and its comma), or
 * 0 when no container has any left.
 */
static size_t drop_last(cJSON *containers, const char *key)
{
    for (int i = cJSON_GetArraySize(containers) - 1; i >= 0; i--) {
        cJSON *arr = cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(containers, i), key);
        int n = cJSON_GetArraySize(arr);
        if (n == 0) continue;
        cJSON *e = cJSON_DetachItemFromArray(arr, n - 1);
        size_t saved = printed_len(e) + (n > 1);
        cJSON_Delete(e);
        return saved;
    }
    return 0;
}

cJSON *sp_unit_probe_to_json(const sp_unit_probe_t *p, size_t budget)
{
    cJSON *o = cJSON_CreateObject();
    cJSON *cs = o ? cJSON_AddArrayToObject(o, "containers") : NULL;
    if (!cs) { cJSON_Delete(o); return NULL; }
    for (size_t i = 0; i < p->n; i++) {
        cJSON *c = container_to(&p->v[i]);
        if (!c || !cJSON_AddItemToArray(cs, c)) { cJSON_Delete(c); cJSON_Delete(o); return NULL; }
    }
    /* Present while measuring, so the count includes it. */
    if (!cJSON_AddBoolToObject(o, "truncated", p->truncated)) { cJSON_Delete(o); return NULL; }
    /*
     * Over budget: shed open files, then processes, then whole containers,
     * from the end, each element measured once on its own rather than the
     * document printed again per element (128 open files in each of 32
     * containers would otherwise print a 64 KiB document thousands of
     * times). The reprint after each pass is the exact check.
     */
    static const char *const order[] = { "open_write", "processes", NULL };
    for (size_t len = printed_len(o); len != (size_t)-1 && len > budget; len = printed_len(o)) {
        cJSON *t = cJSON_CreateTrue();
        if (!t || !cJSON_ReplaceItemInObjectCaseSensitive(o, "truncated", t)) {
            cJSON_Delete(t);
            cJSON_Delete(o);
            return NULL;
        }
        len = printed_len(o);
        if (len == (size_t)-1 || len <= budget) break;
        size_t need = len - budget, saved = 0;
        for (const char *const *k = order; *k && saved < need; k++) {
            size_t s;
            while (saved < need && (s = drop_last(cs, *k)) != 0) saved += s;
        }
        while (saved < need && cJSON_GetArraySize(cs) > 0) {
            int n = cJSON_GetArraySize(cs);
            cJSON *e = cJSON_DetachItemFromArray(cs, n - 1);
            saved += printed_len(e) + (n > 1);
            cJSON_Delete(e);
        }
        if (saved == 0) break;      /* nothing left to shed: the bare object */
    }
    return o;
}
