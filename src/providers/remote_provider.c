#define _GNU_SOURCE

#include "remote_provider.h"
#include "fake_provider.h"
#include "helper_proto.h"
#include "registry.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
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
    char              unit_id[SP_UNIT_ID_MAX];
    cJSON            *desc;        /* units.describe at begin: blob offsets, run maps */
    int               blob_fd;     /* the sealed memfd describe passed, or -1 */
    int               late_fd;     /* capture.begin's memfd: hook logs, or -1 */
    cJSON            *late;        /* item id -> [offset, length] in late_fd */
    struct { char hook[72]; char run[HELPER_TOKEN_HEX + 1]; char *log; size_t log_len; } dumps[4];
    size_t            n_dumps;
    cJSON            *roots;       /* read_roots */
    sp_session_info_t info;
};

static remote_hello_t g_hello;
static atomic_long    g_req_id;

const remote_hello_t *remote_last_hello(void) { return &g_hello; }

/* The supervised link (remote_start). */
static struct {
    pthread_mutex_t    mu;
    pthread_cond_t     cv;
    pthread_t          th;
    int                running, stop, wake;
    char               daemon8[16];
    remote_backoff_t   bo;
    remote_link_info_t info;
} g_link = { .mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER };

/* The helper went away under a request: say so at once and wake the
 * reconnect thread, rather than waiting for its next health check. */
static void link_lost(const char *sock, const char *why)
{
    pthread_mutex_lock(&g_link.mu);
    if (g_link.info.configured && strcmp(g_link.info.socket, sock) == 0) {
        if (g_link.info.state == REMOTE_CONNECTED) {
            /* Only the transition wakes the thread: while it is already
             * retrying, its backoff decides when to try next. */
            log_warn("helper: lost %s: %s", sock, why);
            g_link.wake = 1;
            pthread_cond_broadcast(&g_link.cv);
        }
        if (g_link.info.state != REMOTE_DISCONNECTED) {
            g_link.info.state = REMOTE_DISCONNECTED;
            g_link.info.since = time(NULL);
        }
        snprintf(g_link.info.last_error, sizeof(g_link.info.last_error), "%s", why);
    }
    pthread_mutex_unlock(&g_link.mu);
}

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

static status_t exchange_fd(int fd, const char *op, cJSON *args, cJSON **result, int send_fd,
                            int *recv_fd, int *transport);

static status_t exchange(int fd, const char *op, cJSON *args, cJSON **result, int *recv_fd,
                         int *transport)
{
    return exchange_fd(fd, op, args, result, -1, recv_fd, transport);
}

static status_t exchange_fd(int fd, const char *op, cJSON *args, cJSON **result, int send_fd,
                            int *recv_fd, int *transport)
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
    status_t st = helper_send(fd, txt, strlen(txt), send_fd);
    cJSON_free(txt);
    if (st != OK) { *transport = 1; return st; }
    char *buf = malloc(HELPER_PACKET_MAX + 2);
    if (!buf) return set_error(ERR_NOMEM, "helper: response");
    size_t got = 0;
    st = helper_recv(fd, buf, HELPER_PACKET_MAX + 2, &got, recv_fd);
    if (st != OK) { *transport = st != ERR_TOO_LARGE; free(buf); return st; }
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

/* Connect, hello, one request. *result (may be NULL) is the result; an fd
 * passed with the answer lands in *recv_fd (NULL: closed), -1 if none. */
static status_t rcall_full(const char *sock, const char *op, cJSON *args, cJSON **result,
                           int send_fd, int *recv_fd);

static status_t rcall_fd(const char *sock, const char *op, cJSON *args, cJSON **result,
                         int *recv_fd)
{
    return rcall_full(sock, op, args, result, -1, recv_fd);
}

static status_t rcall_full(const char *sock, const char *op, cJSON *args, cJSON **result,
                           int send_fd, int *recv_fd)
{
    if (result) *result = NULL;
    if (recv_fd) *recv_fd = -1;
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
        status_t st = set_error_errno(ERR_IO, "helper at %s is unreachable: connect", sock);
        link_lost(sock, err_msg());
        close(fd);
        cJSON_Delete(args);
        return st;
    }
    cJSON *hargs = cJSON_CreateObject();
    if (hargs) cJSON_AddNumberToObject(hargs, "client_version", HELPER_PROTO_VERSION);
    cJSON *hres = NULL;
    int transport = 0;
    status_t st = exchange(fd, "hello", hargs, &hres, NULL, &transport);
    cJSON_Delete(hres);
    if (st == OK) st = exchange_fd(fd, op, args, result, send_fd, recv_fd, &transport);
    else cJSON_Delete(args);
    close(fd);
    if (st != OK && recv_fd && *recv_fd >= 0) { close(*recv_fd); *recv_fd = -1; }
    if (st != OK && transport) {
        /* Not the helper refusing: the helper gone mid-request. */
        char why[512];
        snprintf(why, sizeof(why), "%.400s", err_msg());
        link_lost(sock, why);
        set_error(st, "helper at %s: %s", sock, why);
    }
    return st;
}

static status_t rcall(const char *sock, const char *op, cJSON *args, cJSON **result)
{
    return rcall_fd(sock, op, args, result, NULL);
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
    if (s->blob_fd >= 0) close(s->blob_fd);
    if (s->late_fd >= 0) close(s->late_fd);
    cJSON_Delete(s->late);
    for (size_t i = 0; i < s->n_dumps; i++) free(s->dumps[i].log);
    sp_session_info_free(&s->info);
    free(s);
}

static status_t r_begin(sp_ctx_t *c, const sp_unit_desc_t *unit, const sp_begin_req_t *req,
                        sp_session_t **out, const sp_session_info_t **info)
{
    struct sp_session *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "helper: session");
    s->ctx = c;
    s->blob_fd = -1;
    s->late_fd = -1;
    snprintf(s->unit_id, sizeof(s->unit_id), "%s", unit->unit_id);
    /* The configuration and data-plane facts as of now; the capture is the
     * point in time for the trees. */
    status_t st = rcall_fd(c->socket, "units.describe", args_unit(c, unit->unit_id), &s->desc,
                           &s->blob_fd);
    cJSON *res = NULL;
    if (st == OK) {
        cJSON *a = cJSON_CreateObject();
        cJSON *u = a ? cJSON_AddArrayToObject(a, "units") : NULL;
        char key[HELPER_UNIT_KEY_MAX + 1];
        snprintf(key, sizeof(key), "%s:%s", c->provider, unit->unit_id);
        if (u) cJSON_AddItemToArray(u, cJSON_CreateString(key));
        if (a) cJSON_AddBoolToObject(a, "pause", (unit->caps & SP_CAP_PAUSE) != 0);
        if (a && req->hold_pause) cJSON_AddBoolToObject(a, "hold", 1);
        st = rcall_fd(c->socket, "capture.begin", a, &res, &s->late_fd);
    }
    const char *tok = st == OK ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture")) : NULL;
    const cJSON *m = st == OK ? cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(res, "members"), 0) : NULL;
    if (st == OK && (!tok || strlen(tok) != HELPER_TOKEN_HEX || !m))
        st = set_error(ERR_IO, "helper: a malformed capture");
    if (st == OK) {
        snprintf(s->token, sizeof(s->token), "%s", tok);
        s->roots = cJSON_DetachItemFromObjectCaseSensitive((cJSON *)(uintptr_t)m, "read_roots");
        s->late = cJSON_DetachItemFromObjectCaseSensitive((cJSON *)(uintptr_t)m, "late_blobs");
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

/* n bytes at off of a sealed memfd the helper passed, bounds-checked. */
static status_t read_passed(int fd, uint64_t off, uint64_t n, const char *what, void **data, size_t *len)
{
    struct stat sb;
    if (fd < 0 || fstat(fd, &sb) != 0 || n > HELPER_BLOB_MAX || off + n < off || off + n > (uint64_t)sb.st_size)
        return set_error(ERR_IO, "helper: data for %s lies outside what was passed", what);
    uint8_t *buf = malloc(n ? (size_t)n : 1);
    if (!buf) return set_error(ERR_NOMEM, "helper: blob");
    size_t done = 0;
    while (done < n) {
        ssize_t r = pread(fd, buf + done, (size_t)n - done, (off_t)(off + done));
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { free(buf); return set_error_errno(ERR_IO, "helper: reading %s", what); }
        done += (size_t)r;
    }
    *data = buf;
    *len = (size_t)n;
    return OK;
}

static const char *hook_of(const sp_item_desc_t *item)
{
    return item->source && strncmp(item->source, "hook:", 5) == 0 ? item->source + 5 : NULL;
}

static status_t r_blob_read(sp_session_t *s, const sp_item_desc_t *item, void **data, size_t *len)
{
    /* A dump hook's log, from its hook.wait; then logs capture.begin
     * passed; then what describe passed. */
    const char *hk = hook_of(item);
    for (size_t i = 0; hk && i < s->n_dumps; i++) {
        if (strcmp(s->dumps[i].hook, hk) != 0 || !s->dumps[i].log) continue;
        void *cp = malloc(s->dumps[i].log_len ? s->dumps[i].log_len : 1);
        if (!cp) return set_error(ERR_NOMEM, "helper: hook log");
        memcpy(cp, s->dumps[i].log, s->dumps[i].log_len);
        *data = cp;
        *len = s->dumps[i].log_len;
        return OK;
    }
    const cJSON *lp = cJSON_GetObjectItemCaseSensitive(s->late, item->id);
    if (cJSON_GetArraySize(lp) == 2) {
        const cJSON *o = cJSON_GetArrayItem(lp, 0), *n = cJSON_GetArrayItem(lp, 1);
        if (!cJSON_IsNumber(o) || !cJSON_IsNumber(n) || o->valuedouble < 0 || n->valuedouble < 0)
            return set_error(ERR_IO, "helper: a malformed late blob for %s", item->id);
        return read_passed(s->late_fd, (uint64_t)o->valuedouble, (uint64_t)n->valuedouble, item->id, data, len);
    }
    /* The bytes are in the sealed memfd describe passed: the helper could
     * not change them under us, and any size fits. */
    const cJSON *it = item_json(s, item->id);
    const cJSON *jo = cJSON_GetObjectItemCaseSensitive(it, "data_off");
    const cJSON *jl = cJSON_GetObjectItemCaseSensitive(it, "data_len");
    if (!cJSON_IsNumber(jo) || !cJSON_IsNumber(jl) || s->blob_fd < 0)
        return set_error(ERR_IO, "helper: no data for %s", item->id);
    uint64_t off = num(it, "data_off"), n = num(it, "data_len");
    struct stat sb;
    if (fstat(s->blob_fd, &sb) != 0 || n > HELPER_BLOB_MAX || off + n < off ||
        off + n > (uint64_t)sb.st_size)
        return set_error(ERR_IO, "helper: data for %s lies outside what was passed", item->id);
    uint8_t *buf = malloc(n ? (size_t)n : 1);
    if (!buf) return set_error(ERR_NOMEM, "helper: blob");
    size_t done = 0;
    while (done < n) {
        ssize_t r = pread(s->blob_fd, buf + done, (size_t)n - done, (off_t)(off + done));
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) { free(buf); return set_error_errno(ERR_IO, "helper: reading %s", item->id); }
        done += (size_t)r;
    }
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

/* A dump hook: hook.run passes its stdout as a pipe; the helper's watchdog
 * kills it at its timeout, which ends the pipe. */
static status_t r_stream_open(sp_session_t *s, const sp_item_desc_t *item, int *fd)
{
    const char *hk = hook_of(item);
    if (!hk || strlen(hk) >= sizeof(s->dumps[0].hook)) return set_error(ERR_INVALID, "helper: %s names no hook", item->id);
    if (s->n_dumps >= sizeof(s->dumps) / sizeof(s->dumps[0])) return set_error(ERR_INVALID, "helper: too many dumps");
    cJSON *a = args_unit(s->ctx, s->unit_id);
    if (a) cJSON_AddStringToObject(a, "hook", hk);
    cJSON *res = NULL;
    int pfd = -1;
    status_t st = rcall_fd(s->ctx->socket, "hook.run", a, &res, &pfd);
    const char *run = st == OK ? cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "run")) : NULL;
    if (st == OK && (!run || strlen(run) != HELPER_TOKEN_HEX || pfd < 0))
        st = set_error(ERR_IO, "helper: hook.run for %s gave no run or no pipe", hk);
    if (st == OK) {
        memset(&s->dumps[s->n_dumps], 0, sizeof(s->dumps[0]));
        snprintf(s->dumps[s->n_dumps].hook, sizeof(s->dumps[0].hook), "%s", hk);
        snprintf(s->dumps[s->n_dumps].run, sizeof(s->dumps[0].run), "%s", run);
        s->n_dumps++;
        *fd = pfd;
        pfd = -1;
    }
    if (pfd >= 0) close(pfd);
    cJSON_Delete(res);
    return st;
}

static status_t r_stream_close(sp_session_t *s, const sp_item_desc_t *item, int *ok)
{
    const char *hk = hook_of(item);
    for (size_t i = 0; hk && i < s->n_dumps; i++) {
        if (strcmp(s->dumps[i].hook, hk) != 0 || !s->dumps[i].run[0]) continue;
        cJSON *a = cJSON_CreateObject();
        if (a) cJSON_AddStringToObject(a, "run", s->dumps[i].run);
        cJSON *res = NULL;
        int lfd = -1;
        status_t st = rcall_fd(s->ctx->socket, "hook.wait", a, &res, &lfd);
        s->dumps[i].run[0] = '\0';
        if (st != OK) { if (lfd >= 0) close(lfd); return st; }
        int code = (int)num(res, "exit");
        *ok = code == 0 && !cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res, "timed_out"));
        void *log = NULL;
        size_t len = 0;
        if (lfd >= 0 && read_passed(lfd, 0, num(res, "log_bytes"), hk, &log, &len) == OK) {
            s->dumps[i].log = log;
            s->dumps[i].log_len = len;
        } else {
            err_clear();
        }
        if (lfd >= 0) close(lfd);
        if (!*ok) (void)sp_session_info_warn(&s->info, "dump hook %s exited %d; not stored", hk, code);
        cJSON_Delete(res);
        return OK;
    }
    return set_error(ERR_NOT_FOUND, "helper: dump %s was not open", item->id);
}

/* The pause the helper kept for the second pass, let go (lease.release). */
static status_t r_release(sp_session_t *s, int *cap_hit)
{
    *cap_hit = 0;
    cJSON *a = cJSON_CreateObject();
    if (a) cJSON_AddStringToObject(a, "capture", s->token);
    cJSON *res = NULL;
    status_t st = rcall(s->ctx->socket, "lease.release", a, &res);
    if (st == OK) *cap_hit = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res, "cap_hit"));
    cJSON_Delete(res);
    return st;
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

int remote_is_remote(const sp_provider_ops_t *ops) { return ops && ops->init == r_init; }

status_t remote_op(const char *provider, const char *op, cJSON *args, int send_fd, cJSON **result)
{
    if (result) *result = NULL;
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *c = NULL;
    if (sp_registry_get(provider, &ops, &c) != OK || !remote_is_remote(ops)) {
        cJSON_Delete(args);
        err_clear();
        char why[512];
        if (remote_unusable(provider, why, sizeof(why))) return set_error(ERR_IO, "%s", why);
        return set_error(ERR_INVALID, "provider %s is not behind the helper", provider);
    }
    return rcall_full(c->socket, op, args, result, send_fd, NULL);
}

/* One hello; enable any provider offered that is not enabled yet. */
static status_t attach_once(const char *socket_path, const char *daemon8, remote_hello_t *out)
{
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
            const sp_provider_ops_t *have = NULL;
            sp_ctx_t *hc = NULL;
            if (sp_registry_get(p->valuestring, &have, &hc) == OK && !remote_is_remote(have))
                log_warn("helper: provider %s is already enabled in-process; not attaching it",
                         p->valuestring);
            err_clear();
            continue;       /* already behind the helper: back in use as it is */
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
            .stream_open = r_stream_open, .stream_close = r_stream_close, .release = r_release,
        };
        st = sp_registry_add(o, c);
        if (st != OK) { free(c); break; }
    }
    cJSON_Delete(res);
    if (st != OK) return st;
    if (g_hello.policy[0] && strcmp(g_hello.policy, h.policy) != 0)
        log_warn("helper: its policy changed (%.16s to %.16s) while this daemon ran",
                 g_hello.policy, h.policy);
    g_hello = h;
    log_info("helper: %s, version %d, daemon %s, policy %.16s, providers %s", socket_path,
             h.version, h.daemon_id, h.policy, h.providers[0] ? h.providers : "none");
    if (out) *out = h;
    return OK;
}

/* ------------------------------------------------------------------ */
/* The supervised link                                                 */
/* ------------------------------------------------------------------ */

static void link_set(remote_state_t s, const char *err, const remote_hello_t *h)
{
    pthread_mutex_lock(&g_link.mu);
    if (g_link.info.state != s) {
        if (s == REMOTE_CONNECTED && g_link.info.state == REMOTE_DISCONNECTED) g_link.info.reconnects++;
        g_link.info.state = s;
        g_link.info.since = time(NULL);
    }
    if (err) snprintf(g_link.info.last_error, sizeof(g_link.info.last_error), "%s", err);
    if (h) g_link.info.hello = *h;
    pthread_mutex_unlock(&g_link.mu);
}

/* Wait up to ms, or until woken or stopped. Caller holds mu. */
static void link_wait(unsigned ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += (time_t)(ms / 1000);
    ts.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
    while (!g_link.stop && !g_link.wake)
        if (pthread_cond_timedwait(&g_link.cv, &g_link.mu, &ts) != 0) break;
    g_link.wake = 0;
}

static void *link_main(void *arg)
{
    (void)arg;
    unsigned delay = g_link.bo.initial_ms;
    pthread_mutex_lock(&g_link.mu);
    while (!g_link.stop) {
        int connected = g_link.info.state == REMOTE_CONNECTED;
        char sock[108], d8[16];
        snprintf(sock, sizeof(sock), "%s", g_link.info.socket);
        snprintf(d8, sizeof(d8), "%s", g_link.daemon8);
        link_wait(connected ? g_link.bo.health_ms : delay);
        if (g_link.stop) break;
        pthread_mutex_unlock(&g_link.mu);
        remote_hello_t h;
        status_t st = attach_once(sock, d8, &h);
        if (st == OK) {
            int was = 0;
            pthread_mutex_lock(&g_link.mu);
            was = g_link.info.state;
            pthread_mutex_unlock(&g_link.mu);
            if (was != REMOTE_CONNECTED) log_info("helper: connected to %s", sock);
            link_set(REMOTE_CONNECTED, NULL, &h);
            delay = g_link.bo.initial_ms;
        } else {
            char why[256];
            snprintf(why, sizeof(why), "%.250s", err_msg());
            err_clear();
            link_set(REMOTE_DISCONNECTED, why, NULL);
            /* Bounded backoff while it stays away. */
            delay = delay * 2 > g_link.bo.max_ms ? g_link.bo.max_ms : delay * 2;
        }
        pthread_mutex_lock(&g_link.mu);
    }
    pthread_mutex_unlock(&g_link.mu);
    return NULL;
}

status_t remote_start(const char *socket_path, const char *daemon8, const remote_backoff_t *bo)
{
    if (!socket_path || !*socket_path || strlen(socket_path) >= 108)
        return set_error(ERR_INVALID, "helper: socket path empty or too long");
    remote_stop();
    pthread_mutex_lock(&g_link.mu);
    memset(&g_link.info, 0, sizeof(g_link.info));
    g_link.info.configured = 1;
    snprintf(g_link.info.socket, sizeof(g_link.info.socket), "%s", socket_path);
    g_link.info.state = REMOTE_NEVER;
    g_link.info.since = time(NULL);
    snprintf(g_link.daemon8, sizeof(g_link.daemon8), "%s", daemon8 ? daemon8 : "");
    g_link.bo.initial_ms = bo && bo->initial_ms ? bo->initial_ms : 500;
    g_link.bo.max_ms = bo && bo->max_ms ? bo->max_ms : 30000;
    g_link.bo.health_ms = bo && bo->health_ms ? bo->health_ms : 15000;
    g_link.stop = 0;
    g_link.wake = 0;
    pthread_mutex_unlock(&g_link.mu);

    remote_hello_t h;
    status_t st = attach_once(socket_path, daemon8, &h);
    char why[256] = "";
    if (st != OK) snprintf(why, sizeof(why), "%.250s", err_msg());
    link_set(st == OK ? REMOTE_CONNECTED : REMOTE_DISCONNECTED, st == OK ? NULL : why,
             st == OK ? &h : NULL);
    pthread_mutex_lock(&g_link.mu);
    if (pthread_create(&g_link.th, NULL, link_main, NULL) == 0) g_link.running = 1;
    pthread_mutex_unlock(&g_link.mu);
    if (st != OK) set_error(st, "%s", why);
    return st;
}

void remote_stop(void)
{
    pthread_mutex_lock(&g_link.mu);
    int run = g_link.running;
    g_link.stop = 1;
    pthread_cond_broadcast(&g_link.cv);
    pthread_mutex_unlock(&g_link.mu);
    if (run) pthread_join(g_link.th, NULL);
    pthread_mutex_lock(&g_link.mu);
    g_link.running = 0;
    pthread_mutex_unlock(&g_link.mu);
}

status_t remote_attach(const char *socket_path, const char *daemon8, remote_hello_t *out)
{
    status_t st = remote_start(socket_path, daemon8, NULL);
    if (st == OK && out) *out = g_hello;
    return st;
}

void remote_link_info(remote_link_info_t *out)
{
    pthread_mutex_lock(&g_link.mu);
    *out = g_link.info;
    pthread_mutex_unlock(&g_link.mu);
}

int remote_unusable(const char *provider, char *why, size_t wsz)
{
    remote_link_info_t li;
    remote_link_info(&li);
    if (!li.configured) return 0;
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *c = NULL;
    int enabled = sp_registry_get(provider, &ops, &c) == OK;
    err_clear();
    if (enabled && !remote_is_remote(ops)) return 0;       /* in-process */
    if (enabled && li.state == REMOTE_CONNECTED) return 0;
    char when[32] = "";
    struct tm tm;
    if (localtime_r(&li.since, &tm)) strftime(when, sizeof(when), "%Y-%m-%d %H:%M:%S", &tm);
    if (li.state == REMOTE_CONNECTED)
        snprintf(why, wsz, "provider %s is not offered by the helper at %s (it offers: %s)",
                 provider, li.socket, li.hello.providers[0] ? li.hello.providers : "none");
    else
        snprintf(why, wsz, "provider %s is behind the helper at %s, which is unreachable "
                 "since %s: %s", provider, li.socket, when,
                 li.last_error[0] ? li.last_error : "never connected");
    return 1;
}
