#define _GNU_SOURCE

#include "remote_provider.h"
#include "fake_provider.h"
#include "helper_proto.h"
#include "registry.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define REMOTE_MAX 4
#define REMOTE_TIMEOUT_S 120

struct sp_ctx {
    char provider[SP_PROVIDER_MAX];
    char socket[108];
};

struct sp_session {
    struct sp_ctx    *ctx;
    char              token[HELPER_TOKEN_HEX + 1];
    cJSON            *desc;        /* units.describe at begin: blobs, run maps */
    cJSON            *roots;       /* read_roots */
    sp_session_info_t info;
};

static remote_hello_t g_hello;
static atomic_long    g_req_id;

const remote_hello_t *remote_last_hello(void) { return &g_hello; }

/* ------------------------------------------------------------------ */
/* One call                                                            */
/* ------------------------------------------------------------------ */

static status_t map_error(const char *code, const char *msg)
{
    helper_err_t e = HELPER_E_FAILED;
    (void)helper_err_parse(code, &e);
    status_t st = e == HELPER_E_BUSY || e == HELPER_E_TIMEOUT ? ERR_BUSY
                : e == HELPER_E_FAILED ? ERR_IO : ERR_INVALID;
    return set_error(st, "helper: %s: %.400s", helper_err_name(e), msg ? msg : "");
}

static status_t exchange(int fd, const char *op, cJSON *args, cJSON **result, int *recv_fd)
{
    long id = atomic_fetch_add(&g_req_id, 1) + 1;
    cJSON *o = cJSON_CreateObject();
    if (!o) { cJSON_Delete(args); return set_error(ERR_NOMEM, "helper: request"); }
    cJSON_AddNumberToObject(o, "v", HELPER_PROTO_VERSION);
    cJSON_AddNumberToObject(o, "id", (double)id);
    cJSON_AddStringToObject(o, "op", op);
    cJSON_AddItemToObject(o, "args", args ? args : cJSON_CreateObject());
    char *txt = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    if (!txt) return set_error(ERR_NOMEM, "helper: request");
    status_t st = helper_send(fd, txt, strlen(txt), -1);
    cJSON_free(txt);
    if (st != OK) return st;
    char *buf = malloc(HELPER_PACKET_MAX + 2);
    if (!buf) return set_error(ERR_NOMEM, "helper: response");
    size_t got = 0;
    st = helper_recv(fd, buf, HELPER_PACKET_MAX + 2, &got, recv_fd);
    if (st != OK) { free(buf); return st; }
    cJSON *r = cJSON_ParseWithLength(buf, got);
    free(buf);
    const cJSON *rid = cJSON_GetObjectItemCaseSensitive(r, "id");
    const cJSON *ok = cJSON_GetObjectItemCaseSensitive(r, "ok");
    if (!r || !cJSON_IsNumber(rid) || (long)rid->valuedouble != id || !cJSON_IsBool(ok)) {
        cJSON_Delete(r);
        return set_error(ERR_IO, "helper: a malformed answer to %s", op);
    }
    if (!cJSON_IsTrue(ok)) {
        const cJSON *e = cJSON_GetObjectItemCaseSensitive(r, "error");
        st = map_error(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(e, "code")),
                       cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(e, "msg")));
        cJSON_Delete(r);
        return st;
    }
    if (result) *result = cJSON_DetachItemFromObjectCaseSensitive(r, "result");
    cJSON_Delete(r);
    return OK;
}

/* Connect, hello, one request. *result (may be NULL) is the result. */
static status_t rcall(const char *sock, const char *op, cJSON *args, cJSON **result)
{
    if (result) *result = NULL;
    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) { cJSON_Delete(args); return set_error_errno(ERR_IO, "helper: socket"); }
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", sock);
    struct timeval tv = { REMOTE_TIMEOUT_S, 0 };
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) != 0) {
        status_t st = set_error_errno(ERR_IO, "helper: connect %s", sock);
        close(fd);
        cJSON_Delete(args);
        return st;
    }
    cJSON *hargs = cJSON_CreateObject();
    if (hargs) cJSON_AddNumberToObject(hargs, "client_version", HELPER_PROTO_VERSION);
    cJSON *hres = NULL;
    status_t st = exchange(fd, "hello", hargs, &hres, NULL);
    cJSON_Delete(hres);
    if (st == OK) st = exchange(fd, op, args, result, NULL);
    else cJSON_Delete(args);
    close(fd);
    return st;
}

static cJSON *args_unit(const struct sp_ctx *c, const char *unit_id)
{
    cJSON *a = cJSON_CreateObject();
    char key[HELPER_UNIT_KEY_MAX + 1];
    snprintf(key, sizeof(key), "%s:%s", c->provider, unit_id);
    if (a) cJSON_AddStringToObject(a, "unit", key);
    return a;
}

static const char *str_or(const char *s, const char *def) { return s ? s : def; }

static const char *jstr(const cJSON *o, const char *k)
{
    return str_or(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, k)), "");
}

static uint64_t num(const cJSON *o, const char *k)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, k);
    return cJSON_IsNumber(v) && v->valuedouble >= 0 ? (uint64_t)v->valuedouble : 0;
}

/* ------------------------------------------------------------------ */
/* The vtable                                                          */
/* ------------------------------------------------------------------ */

static status_t r_init(const sp_config_t *cfg, sp_ctx_t **out)
{
    (void)cfg; (void)out;
    return set_error(ERR_INVALID, "remote providers are attached, not initialised");
}

static void r_fini(sp_ctx_t *c) { free(c); }

static status_t r_probe(sp_ctx_t *c, sp_probe_t *out)
{
    memset(out, 0, sizeof(*out));
    cJSON *a = cJSON_CreateObject();
    if (a) cJSON_AddNumberToObject(a, "client_version", HELPER_PROTO_VERSION);
    cJSON *res = NULL;
    /* hello as the request itself: rcall says hello first anyway. */
    status_t st = rcall(c->socket, "hello", a, &res);
    if (st != OK) {
        snprintf(out->detail, sizeof(out->detail), "helper unreachable: %.200s", err_msg());
        err_clear();
        return OK;
    }
    out->available = 1;
    snprintf(out->version, sizeof(out->version), "helper %llu", (unsigned long long)num(res, "version"));
    snprintf(out->detail, sizeof(out->detail), "through the helper at %s, policy %.16s, daemon %s",
             c->socket, jstr(res, "policy"), jstr(res, "daemon_id"));
    cJSON_Delete(res);
    return OK;
}

static status_t r_list(sp_ctx_t *c, sp_unit_cb cb, void *cbctx)
{
    cJSON *a = cJSON_CreateObject();
    if (a) cJSON_AddStringToObject(a, "provider", c->provider);
    cJSON *res = NULL;
    status_t st = rcall(c->socket, "units.list", a, &res);
    const cJSON *u;
    cJSON_ArrayForEach(u, cJSON_GetObjectItemCaseSensitive(res, "units")) {
        if (st != OK) break;
        const char *id = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(u, "unit_id"));
        const char *nm = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(u, "name"));
        if (id) st = cb(cbctx, id, nm ? nm : id, (int)num(u, "state"));
    }
    cJSON_Delete(res);
    return st;
}

static status_t desc_from(const struct sp_ctx *c, const cJSON *res, sp_unit_desc_t **out)
{
    const cJSON *items = cJSON_GetObjectItemCaseSensitive(res, "items");
    int n = cJSON_GetArraySize(items);
    sp_unit_desc_t *d = calloc(1, sizeof(*d));
    if (!d || n < 0 || n > 64) { free(d); return set_error(ERR_IO, "helper: bad description"); }
    d->items = calloc((size_t)(n ? n : 1), sizeof(*d->items));
    const char *uid = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "unit_id"));
    const char *nm = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "name"));
    d->name = strdup(nm ? nm : "");
    if (!d->items || !d->name || !uid || strlen(uid) >= sizeof(d->unit_id)) {
        sp_unit_desc_free(d);
        return set_error(ERR_IO, "helper: bad description");
    }
    snprintf(d->provider, sizeof(d->provider), "%s", c->provider);
    snprintf(d->unit_id, sizeof(d->unit_id), "%s", uid);
    d->state = (int)num(res, "state");
    d->caps = (uint32_t)num(res, "caps");
    const cJSON *it;
    cJSON_ArrayForEach(it, items) {
        sp_item_desc_t *x = &d->items[d->n_items];
        const char *id = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "id"));
        const char *kind = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "kind"));
        const char *path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "path"));
        const char *fmt = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "format"));
        const char *src = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "source"));
        if (!id || strlen(id) >= sizeof(x->id) || sp_item_kind_parse(kind, &x->kind) != 0 || !path) {
            sp_unit_desc_free(d);
            return set_error(ERR_IO, "helper: bad item in a description");
        }
        snprintf(x->id, sizeof(x->id), "%s", id);
        snprintf(x->format, sizeof(x->format), "%s", fmt ? fmt : "");
        x->size = num(it, "size");
        x->caps = (uint32_t)num(it, "caps");
        x->mtime_sec = (int64_t)num(it, "mtime");
        x->tree_path = strdup(path);
        x->source = src ? strdup(src) : NULL;
        d->n_items++;
        if (!x->tree_path || (src && !x->source)) {
            sp_unit_desc_free(d);
            return set_error(ERR_NOMEM, "helper: description");
        }
    }
    *out = d;
    return OK;
}

static status_t r_describe(sp_ctx_t *c, const char *unit_id, sp_unit_desc_t **out)
{
    cJSON *res = NULL;
    status_t st = rcall(c->socket, "units.describe", args_unit(c, unit_id), &res);
    if (st == OK) st = desc_from(c, res, out);
    cJSON_Delete(res);
    return st;
}

static void r_desc_free(sp_unit_desc_t *d) { sp_unit_desc_free(d); }

static void session_free(struct sp_session *s)
{
    if (!s) return;
    cJSON_Delete(s->desc);
    cJSON_Delete(s->roots);
    sp_session_info_free(&s->info);
    free(s);
}

static status_t r_begin(sp_ctx_t *c, const sp_unit_desc_t *unit, const sp_begin_req_t *req,
                        sp_session_t **out, const sp_session_info_t **info)
{
    struct sp_session *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "helper: session");
    s->ctx = c;
    /* The configuration and data-plane facts as of now; the capture is the
     * point in time for the trees. */
    status_t st = rcall(c->socket, "units.describe", args_unit(c, unit->unit_id), &s->desc);
    cJSON *res = NULL;
    if (st == OK) {
        cJSON *a = cJSON_CreateObject();
        cJSON *u = a ? cJSON_AddArrayToObject(a, "units") : NULL;
        char key[HELPER_UNIT_KEY_MAX + 1];
        snprintf(key, sizeof(key), "%s:%s", c->provider, unit->unit_id);
        if (u) cJSON_AddItemToArray(u, cJSON_CreateString(key));
        if (a) cJSON_AddBoolToObject(a, "pause", (unit->caps & SP_CAP_PAUSE) != 0);
        st = rcall(c->socket, "capture.begin", a, &res);
    }
    const char *tok = st == OK ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture")) : NULL;
    const cJSON *m = st == OK ? cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(res, "members"), 0) : NULL;
    if (st == OK && (!tok || strlen(tok) != HELPER_TOKEN_HEX || !m))
        st = set_error(ERR_IO, "helper: a malformed capture");
    if (st == OK) {
        snprintf(s->token, sizeof(s->token), "%s", tok);
        s->roots = cJSON_DetachItemFromObjectCaseSensitive((cJSON *)(uintptr_t)m, "read_roots");
        s->info.achieved = (sp_consistency_t)num(m, "achieved");
        s->info.consist_flags = (uint32_t)num(m, "flags");
        s->info.freeze_ms = num(m, "freeze_ms");
        s->info.pause_ms = num(m, "pause_ms");
        const cJSON *w;
        cJSON_ArrayForEach(w, cJSON_GetObjectItemCaseSensitive(m, "warnings"))
            if (cJSON_IsString(w) && st == OK) st = sp_session_info_warn(&s->info, "%s", w->valuestring);
        /* Plans point at strings that outlive the response: the item ids
         * of the caller's description and the vocabulary's own reasons. */
        const cJSON *pl = cJSON_GetObjectItemCaseSensitive(m, "plans");
        int np = cJSON_GetArraySize(pl);
        s->info.plans = calloc((size_t)(np > 0 ? np : 1), sizeof(*s->info.plans));
        if (!s->info.plans) st = set_error(ERR_NOMEM, "helper: plans");
        const cJSON *p;
        cJSON_ArrayForEach(p, pl) {
            if (st != OK) break;
            const char *item = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(p, "item"));
            const char *why = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(p, "reason"));
            for (size_t k = 0; item && k < unit->n_items; k++) {
                if (strcmp(unit->items[k].id, item) != 0) continue;
                sp_item_plan_t *x = &s->info.plans[s->info.n_plans++];
                x->item_id = unit->items[k].id;
                x->mode = SP_READ_FULL;
                x->reason = str_or(sp_reason_canonical(why), "forced");
                break;
            }
        }
        if (st == OK && req && s->info.achieved < req->require)
            st = set_error(ERR_INVALID, "unit %s: achieved %s, below the required %s",
                           unit->name ? unit->name : unit->unit_id,
                           str_or(sp_consistency_name(s->info.achieved), "?"),
                           str_or(sp_consistency_name(req->require), "?"));
        if (st != OK) {
            char why[512];
            snprintf(why, sizeof(why), "%.500s", err_msg());
            cJSON *ea = cJSON_CreateObject();
            if (ea) cJSON_AddStringToObject(ea, "capture", s->token);
            (void)rcall(c->socket, "capture.end", ea, NULL);
            set_error(st, "%s", why);
        }
    }
    cJSON_Delete(res);
    if (st != OK) { session_free(s); return st; }
    *out = s;
    *info = &s->info;
    return OK;
}

static const cJSON *item_json(const struct sp_session *s, const char *id)
{
    const cJSON *it;
    cJSON_ArrayForEach(it, cJSON_GetObjectItemCaseSensitive(s->desc, "items")) {
        const char *x = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "id"));
        if (x && strcmp(x, id) == 0) return it;
    }
    return NULL;
}

static status_t r_blob_read(sp_session_t *s, const sp_item_desc_t *item, void **data, size_t *len)
{
    const char *hex = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(item_json(s, item->id), "data"));
    if (!hex) return set_error(ERR_IO, "helper: no data for %s", item->id);
    size_t cap = strlen(hex) / 2 + 1;
    uint8_t *buf = malloc(cap);
    if (!buf) return set_error(ERR_NOMEM, "helper: blob");
    long n = helper_hex_decode(hex, buf, cap);
    if (n < 0) { free(buf); return set_error(ERR_IO, "helper: bad data for %s", item->id); }
    *data = buf;
    *len = (size_t)n;
    return OK;
}

static status_t r_block_open(sp_session_t *s, const sp_item_desc_t *item, sp_block_stream_t *out)
{
    if (strcmp(s->ctx->provider, "fake") != 0)
        return set_error(ERR_INVALID, "helper: no data plane for %s disks in this build", s->ctx->provider);
    const cJSON *fk = cJSON_GetObjectItemCaseSensitive(item_json(s, item->id), "fake");
    const cJSON *runs = cJSON_GetObjectItemCaseSensitive(fk, "runs");
    int n = cJSON_GetArraySize(runs);
    if (!fk || n < 0 || n > 100000) return set_error(ERR_IO, "helper: no run map for %s", item->id);
    fake_run_t *r = calloc((size_t)(n ? n : 1), sizeof(*r));
    if (!r) return set_error(ERR_NOMEM, "helper: runs");
    int k = 0;
    const cJSON *x;
    cJSON_ArrayForEach(x, runs) {
        const cJSON *a = cJSON_GetArrayItem(x, 0), *b = cJSON_GetArrayItem(x, 1);
        const cJSON *c = cJSON_GetArrayItem(x, 2), *d = cJSON_GetArrayItem(x, 3);
        if (!cJSON_IsNumber(a) || !cJSON_IsNumber(b) || !cJSON_IsNumber(c) || !cJSON_IsNumber(d)) {
            free(r);
            return set_error(ERR_IO, "helper: bad run map");
        }
        r[k].off = (uint64_t)a->valuedouble;
        r[k].len = (uint64_t)b->valuedouble;
        r[k].kind = (fake_run_kind_t)(int)c->valuedouble;
        r[k].seed = (uint64_t)d->valuedouble;
        k++;
    }
    status_t st = fake_stream_from_runs(item->size, r, (size_t)k,
                                        (fake_alloc_t)(int)num(fk, "alloc"), out);
    free(r);
    return st;
}

static status_t r_tree_root(sp_session_t *s, const sp_item_desc_t *item, char *read_root, size_t sz)
{
    const char *p = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(s->roots, item->id));
    if (!p || p[0] != '/') return set_error(ERR_IO, "helper: no read root for %s", item->id);
    if (snprintf(read_root, sz, "%s", p) >= (int)sz) return set_error(ERR_INVALID, "helper: read root too long");
    return OK;
}

static status_t r_end(sp_session_t *s, sp_end_outcome_t outcome, int64_t snapshot_id)
{
    (void)outcome; (void)snapshot_id;
    cJSON *a = cJSON_CreateObject();
    if (a) cJSON_AddStringToObject(a, "capture", s->token);
    /* The lease bounds it either way: a capture.end that does not arrive
     * expires at capture_max_s in the helper. */
    status_t st = rcall(s->ctx->socket, "capture.end", a, NULL);
    session_free(s);
    return st;
}

static status_t r_sweep(sp_ctx_t *c, const sp_sweep_req_t *req, sp_sweep_report_t *out)
{
    memset(out, 0, sizeof(*out));
    /* The helper keeps the holds; the daemon journals none for it. */
    for (size_t i = 0; req && req->released && i < req->n_intents; i++) req->released[i] = 0;
    cJSON *res = NULL;
    status_t st = rcall(c->socket, "sweep", cJSON_CreateObject(), &res);
    if (st == OK) {
        out->released = (unsigned)num(res, "released");
        out->removed = (unsigned)num(res, "removed");
        out->left_foreign = (unsigned)num(res, "left_foreign");
    }
    cJSON_Delete(res);
    return st;
}

static sp_provider_ops_t g_ops[REMOTE_MAX];
static char              g_names[REMOTE_MAX][SP_PROVIDER_MAX];

status_t remote_attach(const char *socket_path, const char *daemon8, remote_hello_t *out)
{
    if (!socket_path || !*socket_path || strlen(socket_path) >= 108)
        return set_error(ERR_INVALID, "helper: socket path empty or too long");
    cJSON *a = cJSON_CreateObject();
    if (a) cJSON_AddNumberToObject(a, "client_version", HELPER_PROTO_VERSION);
    cJSON *res = NULL;
    status_t st = rcall(socket_path, "hello", a, &res);
    if (st != OK) return st;
    remote_hello_t h;
    memset(&h, 0, sizeof(h));
    h.version = (int)num(res, "version");
    snprintf(h.daemon_id, sizeof(h.daemon_id), "%s", jstr(res, "daemon_id"));
    snprintf(h.policy, sizeof(h.policy), "%s", jstr(res, "policy"));
    if (daemon8 && strcmp(h.daemon_id, daemon8) != 0)
        log_warn("helper: configured for daemon %s, this daemon is %s; artefacts are named "
                 "with the helper's", h.daemon_id, daemon8);
    const cJSON *p;
    cJSON_ArrayForEach(p, cJSON_GetObjectItemCaseSensitive(res, "providers")) {
        if (!cJSON_IsString(p) || !helper_provider_known(p->valuestring)) continue;
        if (strlen(h.providers) + strlen(p->valuestring) + 2 < sizeof(h.providers)) {
            if (h.providers[0]) strcat(h.providers, ",");
            strcat(h.providers, p->valuestring);
        }
        if (sp_registry_enabled(p->valuestring)) {
            log_warn("helper: provider %s is already enabled in-process; not attaching it", p->valuestring);
            continue;
        }
        /* A slot whose provider is not enabled any more (the registry was
         * shut down and the helper attached again) is reused. */
        size_t slot = REMOTE_MAX;
        for (size_t i = 0; i < REMOTE_MAX && slot == REMOTE_MAX; i++)
            if (!g_names[i][0] || !sp_registry_enabled(g_names[i])) slot = i;
        if (slot == REMOTE_MAX) break;
        struct sp_ctx *c = calloc(1, sizeof(*c));
        if (!c) { st = set_error(ERR_NOMEM, "helper: provider"); break; }
        snprintf(c->provider, sizeof(c->provider), "%s", p->valuestring);
        snprintf(c->socket, sizeof(c->socket), "%s", socket_path);
        snprintf(g_names[slot], sizeof(g_names[slot]), "%s", p->valuestring);
        sp_provider_ops_t *o = &g_ops[slot];
        *o = (sp_provider_ops_t){
            .name = g_names[slot], .abi = SP_ABI_VERSION,
            .init = r_init, .fini = r_fini, .probe = r_probe, .list_units = r_list,
            .describe = r_describe, .desc_free = r_desc_free, .begin = r_begin,
            .blob_read = r_blob_read, .block_open = r_block_open, .tree_root = r_tree_root,
            .end = r_end, .sweep = r_sweep,
        };
        st = sp_registry_add(o, c);
        if (st != OK) { free(c); break; }
    }
    cJSON_Delete(res);
    if (st != OK) return st;
    g_hello = h;
    log_info("helper: %s, version %d, daemon %s, policy %.16s, providers %s", socket_path,
             h.version, h.daemon_id, h.policy, h.providers[0] ? h.providers : "none");
    if (out) *out = h;
    return OK;
}
