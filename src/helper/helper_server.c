#define _GNU_SOURCE

#include "helper_server.h"
#include "artefact.h"
#include "fake_provider.h"
#include "helper_proto.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

#define HOLDS_MAX     256
#define CAPTURES_MAX  64
#define RUNS_MAX      32
#define STAGINGS_MAX  32
#define CONNS_MAX     16
#define HOOK_LOG_MAX  ((size_t)16 * 1024)

/* ------------------------------------------------------------------ */
/* State                                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t id;
    char    unit[HELPER_UNIT_KEY_MAX + 1];  /* provider:id */
    char    kind[16];
    char    detail[192];
    time_t  created;
    int64_t capture;                        /* the capture's sequence number */
} hold_t;

struct jctx;

typedef struct {
    char    unit[HELPER_UNIT_KEY_MAX + 1];
    sp_session_t *sess;
    struct jctx  *j;          /* the session's journal context, freed after end */
} member_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    int64_t  seq;
    time_t   deadline;
    member_t members[HELPER_UNITS_MAX];
    size_t   n_members;
} capture_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    pid_t    pid;
    int      err_fd;          /* the hook's stderr, for the log */
    time_t   deadline;
    int      done, status, timed_out;
    char    *log;
    size_t   log_len;
} run_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    char     unit[HELPER_UNIT_KEY_MAX + 1];
    char     mount[HELPER_PATH_MAX + 1];
    char     path[HELPER_PATH_MAX + 1];
    int64_t  job;
    int      swapped;
} staging_t;

struct helper_server {
    const helper_policy_t *pol;
    sp_ctx_t              *fake;
    pthread_mutex_t        mu;          /* everything below but the journal */
    capture_t              caps[CAPTURES_MAX];
    run_t                  runs[RUNS_MAX];
    staging_t              stages[STAGINGS_MAX];
    time_t                 recent[3600];  /* capture start times, a ring */
    size_t                 n_recent;
    int64_t                seq;

    pthread_mutex_t        jmu;         /* the hold journal */
    hold_t                 holds[HOLDS_MAX];
    size_t                 n_holds;
    int64_t                next_hold;

    int                    lfd;
    pthread_t              accept_th, wd_th;
    int                    accept_on, wd_on, stop;
    pthread_mutex_t        cmu;
    pthread_cond_t         wcv;
    int                    conns[CONNS_MAX];
    pthread_t              conn_th[CONNS_MAX];
    int                    conn_live[CONNS_MAX];
    unsigned               wd_ms;
};

/* ------------------------------------------------------------------ */
/* The hold journal, persisted                                         */
/* ------------------------------------------------------------------ */

/* Caller holds jmu. Written whole through a temporary and a rename. */
static void journal_save(helper_server_t *s)
{
    cJSON *a = cJSON_CreateArray();
    for (size_t i = 0; a && i < s->n_holds; i++) {
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(a, o);
        cJSON_AddNumberToObject(o, "id", (double)s->holds[i].id);
        cJSON_AddStringToObject(o, "unit", s->holds[i].unit);
        cJSON_AddStringToObject(o, "kind", s->holds[i].kind);
        cJSON_AddStringToObject(o, "detail", s->holds[i].detail);
        cJSON_AddNumberToObject(o, "created", (double)s->holds[i].created);
    }
    char *txt = a ? cJSON_PrintUnformatted(a) : NULL;
    cJSON_Delete(a);
    if (!txt) { log_error("helper: cannot serialise the hold journal"); return; }
    char path[PATH_MAX + 32], tmp[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/holds.json", s->pol->state_dir);
    snprintf(tmp, sizeof(tmp), "%s/holds.json.tmp", s->pol->state_dir);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    size_t n = strlen(txt);
    int ok = fd >= 0 && write(fd, txt, n) == (ssize_t)n && fsync(fd) == 0;
    if (fd >= 0) close(fd);
    if (!ok || rename(tmp, path) != 0)
        log_error("helper: cannot write %s: %s", path, strerror(errno));
    cJSON_free(txt);
}

typedef struct jctx {
    helper_server_t *s;
    char             unit[HELPER_UNIT_KEY_MAX + 1];
    int64_t          capture;
} jctx_t;

static status_t j_record(void *ctx, const char *kind, const char *detail, int64_t *out)
{
    jctx_t *j = ctx;
    helper_server_t *s = j->s;
    pthread_mutex_lock(&s->jmu);
    if (s->n_holds >= HOLDS_MAX) {
        pthread_mutex_unlock(&s->jmu);
        return set_error(ERR_BUSY, "helper: hold journal full; not taken");
    }
    hold_t *h = &s->holds[s->n_holds++];
    memset(h, 0, sizeof(*h));
    h->id = ++s->next_hold;
    snprintf(h->unit, sizeof(h->unit), "%s", j->unit);
    snprintf(h->kind, sizeof(h->kind), "%s", kind);
    snprintf(h->detail, sizeof(h->detail), "%s", detail);
    h->created = time(NULL);
    h->capture = j->capture;
    *out = h->id;
    journal_save(s);
    pthread_mutex_unlock(&s->jmu);
    return OK;
}

static status_t j_clear(void *ctx, int64_t id)
{
    jctx_t *j = ctx;
    helper_server_t *s = j->s;
    pthread_mutex_lock(&s->jmu);
    for (size_t i = 0; i < s->n_holds; i++) {
        if (s->holds[i].id != id) continue;
        s->holds[i] = s->holds[--s->n_holds];
        journal_save(s);
        break;
    }
    pthread_mutex_unlock(&s->jmu);
    return OK;
}

static void journal_load(helper_server_t *s)
{
    char path[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/holds.json", s->pol->state_dir);
    FILE *f = fopen(path, "rb");
    if (!f) return;
    char *buf = malloc(HELPER_PACKET_MAX * 4);
    size_t n = buf ? fread(buf, 1, HELPER_PACKET_MAX * 4 - 1, f) : 0;
    fclose(f);
    if (!buf) return;
    buf[n] = '\0';
    cJSON *a = cJSON_Parse(buf);
    free(buf);
    const cJSON *o;
    cJSON_ArrayForEach(o, a) {
        if (s->n_holds >= HOLDS_MAX) break;
        const char *u = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "unit"));
        const char *k = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "kind"));
        const char *d = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "detail"));
        const cJSON *id = cJSON_GetObjectItemCaseSensitive(o, "id");
        if (!u || !k || !d || !cJSON_IsNumber(id)) continue;
        hold_t *h = &s->holds[s->n_holds++];
        memset(h, 0, sizeof(*h));
        h->id = (int64_t)id->valuedouble;
        snprintf(h->unit, sizeof(h->unit), "%s", u);
        snprintf(h->kind, sizeof(h->kind), "%s", k);
        snprintf(h->detail, sizeof(h->detail), "%s", d);
        h->created = 0;
        h->capture = -1;              /* nobody's: the process that took it is gone */
        if (h->id > s->next_hold) s->next_hold = h->id;
    }
    cJSON_Delete(a);
}

/*
 * Release the holds that select() picks, through the backend, and drop the
 * ones released. Returns how many were. Caller holds nothing.
 */
typedef int (*hold_pick_fn)(const hold_t *h, void *ctx);

static unsigned release_holds(helper_server_t *s, hold_pick_fn pick, void *pctx, const char *why)
{
    if (!s->fake) return 0;
    hold_t *picked = calloc(HOLDS_MAX, sizeof(*picked));
    if (!picked) return 0;
    size_t n = 0;
    pthread_mutex_lock(&s->jmu);
    for (size_t i = 0; i < s->n_holds; i++)
        if (strncmp(s->holds[i].unit, "fake:", 5) == 0 && pick(&s->holds[i], pctx))
            picked[n++] = s->holds[i];
    pthread_mutex_unlock(&s->jmu);
    unsigned released = 0;
    for (size_t i = 0; i < n; i++) {
        sp_intent_t in = { picked[i].id, picked[i].unit + 5, picked[i].kind, picked[i].detail, 0 };
        int rel = 0;
        sp_sweep_req_t rq;
        memset(&rq, 0, sizeof(rq));
        rq.daemon8 = s->pol->daemon_id;
        rq.intents = &in;
        rq.n_intents = 1;
        rq.released = &rel;
        sp_sweep_report_t rep;
        if (fake_provider_ops.sweep(s->fake, &rq, &rep) == OK && rel) {
            jctx_t j = { s, "", 0 };
            (void)j_clear(&j, picked[i].id);
            released++;
            log_warn("helper: %s released %s on %s (%s)", why, picked[i].kind,
                     picked[i].unit, picked[i].detail);
        } else {
            err_clear();
            log_warn("helper: %s could not release %s on %s; kept", why, picked[i].kind, picked[i].unit);
        }
    }
    free(picked);
    return released;
}

static int pick_all(const hold_t *h, void *ctx) { (void)h; (void)ctx; return 1; }

typedef struct { time_t now; unsigned pause_s; } age_t;

static int pick_old(const hold_t *h, void *ctx)
{
    const age_t *a = ctx;
    int timed = strcmp(h->kind, SP_HOLD_FREEZE) == 0 || strcmp(h->kind, SP_HOLD_PAUSE) == 0;
    return timed && h->created + (time_t)a->pause_s <= a->now;
}

/* ------------------------------------------------------------------ */
/* Responses                                                           */
/* ------------------------------------------------------------------ */

static char *resp_error(int64_t id, helper_err_t code, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
static char *resp_error(int64_t id, helper_err_t code, const char *fmt, ...)
{
    char msg[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    cJSON *o = cJSON_CreateObject();
    cJSON *e = o ? cJSON_CreateObject() : NULL;
    char *t = NULL;
    if (o && e) {
        cJSON_AddNumberToObject(o, "id", (double)id);
        cJSON_AddBoolToObject(o, "ok", 0);
        cJSON_AddStringToObject(e, "code", helper_err_name(code));
        cJSON_AddStringToObject(e, "msg", msg);
        cJSON_AddItemToObject(o, "error", e);
        e = NULL;
        t = cJSON_PrintUnformatted(o);
    }
    cJSON_Delete(e);
    cJSON_Delete(o);
    char *out = t ? strdup(t) : NULL;
    cJSON_free(t);
    return out;
}

/* Takes result. A response past the packet bound becomes `failed`. */
static char *resp_ok(int64_t id, cJSON *result)
{
    cJSON *o = cJSON_CreateObject();
    if (!o || !result) { cJSON_Delete(o); cJSON_Delete(result); return resp_error(id, HELPER_E_FAILED, "out of memory"); }
    cJSON_AddNumberToObject(o, "id", (double)id);
    cJSON_AddBoolToObject(o, "ok", 1);
    cJSON_AddItemToObject(o, "result", result);
    char *t = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    if (!t) return resp_error(id, HELPER_E_FAILED, "out of memory");
    size_t n = strlen(t);
    if (n > HELPER_PACKET_MAX) {
        cJSON_free(t);
        return resp_error(id, HELPER_E_FAILED, "the answer is %zu bytes, over one packet", n);
    }
    char *out = strdup(t);
    cJSON_free(t);
    return out;
}

/* ------------------------------------------------------------------ */
/* Units                                                               */
/* ------------------------------------------------------------------ */

/* The fake backend for a unit key, or why not. On OK *uid points into key. */
static helper_err_t backend_for(helper_server_t *s, const char *key, const char **uid,
                                char *why, size_t wsz)
{
    char prov[16], id[HELPER_UNIT_KEY_MAX + 1];
    if (!helper_unit_key_ok(key, prov, sizeof(prov), id, sizeof(id))) {
        snprintf(why, wsz, "malformed unit key");
        return HELPER_E_INVALID;
    }
    if (strcmp(prov, "fake") != 0 || !s->fake) {
        snprintf(why, wsz, "provider %s is not available in this helper", prov);
        return HELPER_E_UNSUPPORTED;
    }
    *uid = key + 5;
    return HELPER_OK;
}

/* Policy first, by key; then by name, which only the helper's own inspect
 * can supply. A unit the policy does not allow is denied whether or not it
 * exists, so the answer says nothing about what lies outside. */
static helper_err_t unit_allowed(helper_server_t *s, const char *key, char *why, size_t wsz)
{
    if (helper_policy_unit_allowed(s->pol, key, NULL)) return HELPER_OK;
    const char *uid = NULL;
    if (backend_for(s, key, &uid, why, wsz) == HELPER_OK) {
        sp_unit_desc_t *d = NULL;
        if (fake_provider_ops.describe(s->fake, uid, &d) == OK) {
            int ok = helper_policy_unit_allowed(s->pol, key, d->name);
            fake_provider_ops.desc_free(d);
            if (ok) return HELPER_OK;
        }
        err_clear();
    }
    snprintf(why, wsz, "unit %s is not in the policy", key);
    return HELPER_E_DENIED;
}

typedef struct { helper_server_t *s; cJSON *arr; } list_ctx_t;

static status_t list_cb(void *ctx, const char *id, const char *name, int state)
{
    list_ctx_t *l = ctx;
    char key[HELPER_UNIT_KEY_MAX + 8];
    snprintf(key, sizeof(key), "fake:%s", id);
    if (!helper_unit_key_ok(key, NULL, 0, NULL, 0) || !helper_policy_unit_allowed(l->s->pol, key, name))
        return OK;
    cJSON *o = cJSON_CreateObject();
    if (!o) return set_error(ERR_NOMEM, "helper: list");
    cJSON_AddItemToArray(l->arr, o);
    cJSON_AddStringToObject(o, "unit_id", id);
    cJSON_AddStringToObject(o, "name", name);
    cJSON_AddNumberToObject(o, "state", state);
    return OK;
}

static char *op_units_list(helper_server_t *s, const helper_req_t *r)
{
    if (strcmp(r->provider, "fake") != 0 || !s->fake)
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "provider %s is not available in this helper", r->provider);
    cJSON *res = cJSON_CreateObject();
    cJSON *arr = res ? cJSON_AddArrayToObject(res, "units") : NULL;
    if (!arr) { cJSON_Delete(res); return resp_error(r->id, HELPER_E_FAILED, "out of memory"); }
    list_ctx_t l = { s, arr };
    if (fake_provider_ops.list_units(s->fake, list_cb, &l) != OK) {
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
    }
    return resp_ok(r->id, res);
}

static char *op_units_describe(helper_server_t *s, const helper_req_t *r)
{
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    if ((e = backend_for(s, r->unit, &uid, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    sp_unit_desc_t *d = NULL;
    if (fake_provider_ops.describe(s->fake, uid, &d) != OK)
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
    cJSON *res = cJSON_CreateObject();
    cJSON *items = res ? cJSON_AddArrayToObject(res, "items") : NULL;
    int bad = !items;
    if (res) {
        cJSON_AddStringToObject(res, "unit_id", d->unit_id);
        cJSON_AddStringToObject(res, "name", d->name ? d->name : "");
        cJSON_AddNumberToObject(res, "state", d->state);
        cJSON_AddNumberToObject(res, "caps", d->caps);
    }
    for (size_t i = 0; !bad && i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        cJSON *o = cJSON_CreateObject();
        if (!o) { bad = 1; break; }
        cJSON_AddItemToArray(items, o);
        cJSON_AddStringToObject(o, "id", it->id);
        cJSON_AddStringToObject(o, "kind", sp_item_kind_name(it->kind));
        cJSON_AddNumberToObject(o, "size", (double)it->size);
        cJSON_AddStringToObject(o, "format", it->format);
        cJSON_AddStringToObject(o, "path", it->tree_path ? it->tree_path : "");
        if (it->source) cJSON_AddStringToObject(o, "source", it->source);
        cJSON_AddNumberToObject(o, "caps", it->caps);
        cJSON_AddNumberToObject(o, "mtime", (double)it->mtime_sec);
        /* Configuration is returned as data, read by the helper. */
        if (it->kind == SP_ITEM_BLOB) {
            void *data = NULL;
            size_t len = 0;
            if (fake_blob_data(s->fake, uid, it->id, &data, &len) != OK || len > HELPER_PACKET_MAX / 2) {
                free(data);
                err_clear();
                bad = 2;
                break;
            }
            char *hex = malloc(len * 2 + 1);
            if (hex) helper_hex_encode(data, len, hex);
            free(data);
            if (!hex) { bad = 1; break; }
            cJSON_AddStringToObject(o, "data", hex);
            free(hex);
        } else if (it->kind == SP_ITEM_BLOCK) {
            /* The fake's data plane: the run map the daemon reads through,
             * as it would read a real export through its own mount. */
            uint64_t size = 0;
            fake_run_t *runs = NULL;
            size_t n = 0;
            fake_alloc_t alloc = FAKE_ALLOC_EXACT;
            if (fake_disk_runs(s->fake, uid, it->id, &size, &runs, &n, &alloc) != OK) {
                err_clear();
                bad = 1;
                break;
            }
            cJSON *fk = cJSON_AddObjectToObject(o, "fake");
            cJSON *ra = fk ? cJSON_AddArrayToObject(fk, "runs") : NULL;
            if (fk) cJSON_AddNumberToObject(fk, "alloc", alloc);
            for (size_t k = 0; ra && k < n; k++) {
                cJSON *x = cJSON_CreateArray();
                if (!x) break;
                cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].off));
                cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].len));
                cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].kind));
                cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].seed));
                cJSON_AddItemToArray(ra, x);
            }
            free(runs);
        }
    }
    fake_provider_ops.desc_free(d);
    if (bad) {
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, bad == 2 ? "a configuration item is too large for one packet"
                                                          : "out of memory");
    }
    return resp_ok(r->id, res);
}

/* ------------------------------------------------------------------ */
/* Captures                                                            */
/* ------------------------------------------------------------------ */

/* Rate limit: max_captures_per_hour. Caller holds mu. */
static int rate_ok(helper_server_t *s, time_t now)
{
    size_t live = 0;
    for (size_t i = 0; i < s->n_recent; i++)
        if (s->recent[i] > now - 3600) s->recent[live++] = s->recent[i];
    s->n_recent = live;
    if (live >= s->pol->max_captures_per_hour) return 0;
    if (s->n_recent < sizeof(s->recent) / sizeof(s->recent[0])) s->recent[s->n_recent++] = now;
    return 1;
}

static capture_t *capture_find(helper_server_t *s, const char *token)
{
    for (size_t i = 0; i < CAPTURES_MAX; i++)
        if (s->caps[i].used && strcmp(s->caps[i].token, token) == 0) return &s->caps[i];
    return NULL;
}

/* End every member and forget the capture. Caller holds nothing; c has
 * been taken out of the table (copied) already. */
static void capture_finish(helper_server_t *s, capture_t *c, sp_end_outcome_t how)
{
    (void)s;
    for (size_t i = 0; i < c->n_members; i++) {
        if (!c->members[i].sess) continue;
        if (fake_provider_ops.end(c->members[i].sess, how, 0) != OK) {
            log_warn("helper: ending %s: %s; the sweep will retry", c->members[i].unit, err_msg());
            err_clear();
        }
        c->members[i].sess = NULL;
        free(c->members[i].j);
        c->members[i].j = NULL;
    }
}

/* Take a capture out of the table by token. Returns 1 with *out filled. */
static int capture_take(helper_server_t *s, const char *token, capture_t *out)
{
    pthread_mutex_lock(&s->mu);
    capture_t *c = capture_find(s, token);
    if (c) { *out = *c; c->used = 0; }
    pthread_mutex_unlock(&s->mu);
    return c != NULL;
}

static char *op_capture_begin(helper_server_t *s, const helper_req_t *r)
{
    char why[256];
    for (size_t i = 0; i < r->n_units; i++) {
        helper_err_t e = unit_allowed(s, r->units[i], why, sizeof(why));
        if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
        const char *uid = NULL;
        if ((e = backend_for(s, r->units[i], &uid, why, sizeof(why))) != HELPER_OK)
            return resp_error(r->id, e, "%s", why);
    }
    time_t now = time(NULL);
    pthread_mutex_lock(&s->mu);
    int ok = rate_ok(s, now);
    capture_t *slot = NULL;
    for (size_t i = 0; ok && i < CAPTURES_MAX && !slot; i++)
        if (!s->caps[i].used) slot = &s->caps[i];
    int64_t seq = ++s->seq;
    if (slot) {
        memset(slot, 0, sizeof(*slot));
        slot->used = 2;       /* reserved: not findable until filled */
        slot->seq = seq;
    }
    pthread_mutex_unlock(&s->mu);
    if (!ok) return resp_error(r->id, HELPER_E_BUSY, "more than %u captures in the last hour",
                               s->pol->max_captures_per_hour);
    if (!slot) return resp_error(r->id, HELPER_E_BUSY, "too many captures held");

    capture_t c;
    memset(&c, 0, sizeof(c));
    c.seq = seq;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    cJSON *res = cJSON_CreateObject();
    cJSON *mem = res ? cJSON_AddArrayToObject(res, "members") : NULL;
    helper_err_t fail = mem ? HELPER_OK : HELPER_E_FAILED;
    char fmsg[512] = "out of memory";
    for (size_t i = 0; fail == HELPER_OK && i < r->n_units; i++) {
        const char *uid = r->units[i] + 5;
        sp_unit_desc_t *d = NULL;
        if (fake_provider_ops.describe(s->fake, uid, &d) != OK) {
            snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
            fail = HELPER_E_FAILED;
            break;
        }
        jctx_t *j = calloc(1, sizeof(*j));
        if (!j) { fake_provider_ops.desc_free(d); fail = HELPER_E_FAILED; break; }
        j->s = s;
        snprintf(j->unit, sizeof(j->unit), "%s", r->units[i]);
        j->capture = seq;
        sp_holds_t holds = { j_record, j_clear, j };
        sp_begin_req_t q;
        memset(&q, 0, sizeof(q));
        q.job_id = seq;
        q.want = r->pause ? SP_CONSIST_QUIESCED : SP_CONSIST_CRASH;
        q.require = SP_CONSIST_INCONSISTENT;
        q.freeze_timeout_s = s->pol->max_pause_s;
        q.pause_max_s = s->pol->max_pause_s;
        q.holds = &holds;
        q.daemon8 = s->pol->daemon_id;
        sp_session_t *sess = NULL;
        const sp_session_info_t *info = NULL;
        status_t st = fake_provider_ops.begin(s->fake, d, &q, &sess, &info);
        /* The journal context outlives begin: end releases through it, so
         * it lives with the member and goes after end. */
        if (st != OK) {
            snprintf(fmsg, sizeof(fmsg), "%s: %.400s", r->units[i], err_msg());
            fake_provider_ops.desc_free(d);
            free(j);
            fail = HELPER_E_FAILED;
            break;
        }
        member_t *m = &c.members[c.n_members++];
        snprintf(m->unit, sizeof(m->unit), "%s", r->units[i]);
        m->sess = sess;
        m->j = j;
        cJSON *o = cJSON_CreateObject();
        if (!o) { fake_provider_ops.desc_free(d); fail = HELPER_E_FAILED; break; }
        cJSON_AddItemToArray(mem, o);
        cJSON_AddStringToObject(o, "unit", r->units[i]);
        cJSON_AddNumberToObject(o, "achieved", info->achieved);
        cJSON_AddNumberToObject(o, "flags", info->consist_flags);
        cJSON_AddNumberToObject(o, "freeze_ms", (double)info->freeze_ms);
        cJSON_AddNumberToObject(o, "pause_ms", (double)info->pause_ms);
        cJSON *w = cJSON_AddArrayToObject(o, "warnings");
        for (size_t k = 0; w && k < info->n_warnings; k++)
            cJSON_AddItemToArray(w, cJSON_CreateString(info->warnings[k]));
        cJSON *pl = cJSON_AddArrayToObject(o, "plans");
        for (size_t k = 0; pl && k < info->n_plans; k++) {
            cJSON *p = cJSON_CreateObject();
            if (!p) break;
            cJSON_AddItemToArray(pl, p);
            cJSON_AddStringToObject(p, "item", info->plans[k].item_id ? info->plans[k].item_id : "");
            cJSON_AddNumberToObject(p, "mode", info->plans[k].mode);
            cJSON_AddStringToObject(p, "reason", info->plans[k].reason ? info->plans[k].reason : "");
        }
        /* Read roots come from the helper's own snapshots, never from the
         * daemon. */
        cJSON *rr = cJSON_AddObjectToObject(o, "read_roots");
        for (size_t k = 0; rr && k < d->n_items; k++) {
            if (d->items[k].kind != SP_ITEM_TREE) continue;
            char root[PATH_MAX];
            if (fake_provider_ops.tree_root(sess, &d->items[k], root, sizeof(root)) != OK) {
                snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
                fail = HELPER_E_FAILED;
                break;
            }
            cJSON_AddStringToObject(rr, d->items[k].id, root);
        }
        fake_provider_ops.desc_free(d);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (fail != HELPER_OK) {
        cJSON_Delete(res);
        capture_finish(s, &c, SP_END_ABORTED);
        pthread_mutex_lock(&s->mu);
        slot->used = 0;
        pthread_mutex_unlock(&s->mu);
        err_clear();
        return resp_error(r->id, fail, "%s", fmsg);
    }
    if (helper_random_hex(c.token, 16) != OK) {
        cJSON_Delete(res);
        capture_finish(s, &c, SP_END_ABORTED);
        pthread_mutex_lock(&s->mu);
        slot->used = 0;
        pthread_mutex_unlock(&s->mu);
        return resp_error(r->id, HELPER_E_FAILED, "no randomness for a token");
    }
    c.deadline = now + (time_t)s->pol->capture_max_s;
    c.used = 1;
    pthread_mutex_lock(&s->mu);
    *slot = c;
    pthread_mutex_unlock(&s->mu);
    cJSON_AddStringToObject(res, "capture", c.token);
    cJSON_AddNumberToObject(res, "snapshot_ms",
                            (double)((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000));
    cJSON_AddNumberToObject(res, "expires", (double)c.deadline);
    return resp_ok(r->id, res);
}

/* Source snapshot mode for plain paths (db-detection.md 9.4), over the
 * policy's dir domains: each path resolved to its domain by the helper,
 * the domain checked against the policy, one snapshot name per domain from
 * the helper's own generator. The dir backend takes no real snapshot: the
 * read root is the path itself, which is what makes it a test backend. */
static char *op_capture_begin_paths(helper_server_t *s, const helper_req_t *r)
{
    for (size_t i = 0; i < r->n_paths; i++)
        if (!helper_policy_dir_domain(s->pol, r->paths[i]))
            return resp_error(r->id, HELPER_E_DENIED, "%.300s is in no snapshot domain of the policy", r->paths[i]);
    time_t now = time(NULL);
    pthread_mutex_lock(&s->mu);
    int ok = rate_ok(s, now);
    capture_t *slot = NULL;
    for (size_t i = 0; ok && i < CAPTURES_MAX && !slot; i++)
        if (!s->caps[i].used) slot = &s->caps[i];
    int64_t seq = ++s->seq;
    if (slot) {
        memset(slot, 0, sizeof(*slot));
        slot->seq = seq;
        slot->deadline = now + (time_t)s->pol->capture_max_s;
        if (helper_random_hex(slot->token, 16) == OK) slot->used = 1;
        else slot = NULL;
    }
    pthread_mutex_unlock(&s->mu);
    if (!ok) return resp_error(r->id, HELPER_E_BUSY, "more than %u captures in the last hour",
                               s->pol->max_captures_per_hour);
    if (!slot) return resp_error(r->id, HELPER_E_BUSY, "too many captures held");
    cJSON *res = cJSON_CreateObject();
    cJSON *rr = res ? cJSON_AddObjectToObject(res, "read_roots") : NULL;
    cJSON *sn = res ? cJSON_AddObjectToObject(res, "snapshots") : NULL;
    for (size_t i = 0; rr && sn && i < r->n_paths; i++) {
        const char *dom = helper_policy_dir_domain(s->pol, r->paths[i]);
        char name[ARTEFACT_NAME_MAX];
        if (artefact_name(name, sizeof(name), s->pol->daemon_id, seq, -1) == 0)
            cJSON_AddStringToObject(sn, dom, name);
        cJSON_AddStringToObject(rr, r->paths[i], r->paths[i]);
    }
    cJSON_AddStringToObject(res, "capture", slot->token);
    cJSON_AddNumberToObject(res, "expires", (double)slot->deadline);
    return resp_ok(r->id, res);
}

static char *op_capture_end(helper_server_t *s, const helper_req_t *r)
{
    capture_t c;
    if (!capture_take(s, r->token, &c))
        return resp_error(r->id, HELPER_E_INVALID, "no such capture");
    capture_finish(s, &c, SP_END_COMMITTED);
    cJSON *res = cJSON_CreateObject();
    if (res) cJSON_AddBoolToObject(res, "ended", 1);
    return resp_ok(r->id, res);
}

/* ------------------------------------------------------------------ */
/* Hooks                                                               */
/* ------------------------------------------------------------------ */

static char *op_hook_run(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const policy_hook_t *h = helper_policy_hook(s->pol, r->hook);
    if (!h || strcmp(h->unit, r->unit) != 0)
        return resp_error(r->id, HELPER_E_DENIED, "hook %s is not in the policy for %s", r->hook, r->unit);
    const char *uid = NULL;
    if ((e = backend_for(s, r->unit, &uid, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);

    /* The fake has no containers: the policy's argv runs as given, with no
     * shell, which is what docker exec and lxc-attach will wrap. */
    int out[2] = { -1, -1 }, err[2] = { -1, -1 };
    if (pipe2(err, O_CLOEXEC) != 0 || (h->dump && pipe2(out, O_CLOEXEC) != 0)) {
        if (err[0] >= 0) { close(err[0]); close(err[1]); }
        return resp_error(r->id, HELPER_E_FAILED, "pipe: %s", strerror(errno));
    }
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    if (h->dump) posix_spawn_file_actions_adddup2(&fa, out[1], 1);
    else posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_adddup2(&fa, err[1], 2);
    pid_t pid = -1;
    int rc = posix_spawn(&pid, h->argv[0], &fa, NULL, h->argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    close(err[1]);
    if (h->dump) close(out[1]);
    if (rc != 0) {
        close(err[0]);
        if (h->dump) close(out[0]);
        return resp_error(r->id, HELPER_E_FAILED, "hook %s: %s", r->hook, strerror(rc));
    }
    (void)fcntl(err[0], F_SETFL, O_NONBLOCK);
    pthread_mutex_lock(&s->mu);
    run_t *slot = NULL;
    for (size_t i = 0; i < RUNS_MAX && !slot; i++) if (!s->runs[i].used) slot = &s->runs[i];
    if (slot) {
        memset(slot, 0, sizeof(*slot));
        slot->pid = pid;
        slot->err_fd = err[0];
        slot->deadline = time(NULL) + (time_t)h->timeout_s;
        if (helper_random_hex(slot->token, 16) == OK) slot->used = 1;
        else slot = NULL;
    }
    char token[HELPER_TOKEN_HEX + 1] = "";
    if (slot) memcpy(token, slot->token, sizeof(token));
    pthread_mutex_unlock(&s->mu);
    if (!slot) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        close(err[0]);
        if (h->dump) close(out[0]);
        return resp_error(r->id, HELPER_E_BUSY, "too many hook runs");
    }
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "run", token);
        cJSON_AddBoolToObject(res, "dump", h->dump);
    }
    if (h->dump) *pass_fd = out[0];
    return resp_ok(r->id, res);
}

/* Drain what the hook wrote to stderr so far, capped. Caller holds mu. */
static void run_drain(run_t *x)
{
    char buf[4096];
    for (;;) {
        ssize_t n = read(x->err_fd, buf, sizeof(buf));
        if (n <= 0) break;
        size_t take = (size_t)n;
        if (x->log_len + take > HOOK_LOG_MAX) take = HOOK_LOG_MAX - x->log_len;
        if (take) {
            char *g = realloc(x->log, x->log_len + take + 1);
            if (!g) break;
            x->log = g;
            memcpy(x->log + x->log_len, buf, take);
            x->log_len += take;
            x->log[x->log_len] = '\0';
        }
    }
}

/* Reap a run if it ended; kill it if it is past its deadline. Caller
 * holds mu. */
static void run_poll(run_t *x, time_t now)
{
    if (x->done) return;
    run_drain(x);
    int st = 0;
    pid_t p = waitpid(x->pid, &st, WNOHANG);
    if (p == x->pid) {
        x->done = 1;
        x->status = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
        run_drain(x);
        return;
    }
    if (now >= x->deadline) {
        kill(x->pid, SIGKILL);
        waitpid(x->pid, &st, 0);
        x->done = 1;
        x->timed_out = 1;
        x->status = 128 + SIGKILL;
        run_drain(x);
    }
}

static char *op_hook_wait(helper_server_t *s, const helper_req_t *r)
{
    for (;;) {
        pthread_mutex_lock(&s->mu);
        run_t *x = NULL;
        for (size_t i = 0; i < RUNS_MAX; i++)
            if (s->runs[i].used && strcmp(s->runs[i].token, r->token) == 0) x = &s->runs[i];
        if (!x) {
            pthread_mutex_unlock(&s->mu);
            return resp_error(r->id, HELPER_E_INVALID, "no such run");
        }
        run_poll(x, time(NULL));
        if (x->done) {
            cJSON *res = cJSON_CreateObject();
            if (res) {
                cJSON_AddNumberToObject(res, "exit", x->status);
                cJSON_AddBoolToObject(res, "timed_out", x->timed_out);
                cJSON_AddStringToObject(res, "log", x->log ? x->log : "");
            }
            close(x->err_fd);
            free(x->log);
            memset(x, 0, sizeof(*x));
            pthread_mutex_unlock(&s->mu);
            return resp_ok(r->id, res);
        }
        pthread_mutex_unlock(&s->mu);
        struct timespec ts = { 0, 20 * 1000000L };
        nanosleep(&ts, NULL);
    }
}

/* ------------------------------------------------------------------ */
/* Restore                                                             */
/* ------------------------------------------------------------------ */

/* Whether mount is a mount source of the unit in the helper's own inspect:
 * the one path the daemon names, never trusted otherwise. */
static int unit_has_mount(helper_server_t *s, const char *uid, const char *mount)
{
    sp_unit_desc_t *d = NULL;
    if (fake_provider_ops.describe(s->fake, uid, &d) != OK) { err_clear(); return 0; }
    int ok = 0;
    for (size_t i = 0; i < d->n_items; i++)
        if (d->items[i].kind == SP_ITEM_TREE && d->items[i].tree_path &&
            strcmp(d->items[i].tree_path, mount) == 0) ok = 1;
    fake_provider_ops.desc_free(d);
    return ok;
}

static int mkdir_owned(const char *path, uid_t uid)
{
    if (mkdir(path, 0700) != 0 && errno != EEXIST) return -1;
    struct stat sb;
    if (lstat(path, &sb) != 0 || !S_ISDIR(sb.st_mode)) return -1;
    if (sb.st_uid != uid && geteuid() == 0 && chown(path, uid, (gid_t)-1) != 0) return -1;
    return 0;
}

static char *op_restore_stage(helper_server_t *s, const helper_req_t *r)
{
    if (!s->pol->allow_restore) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow restore");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    if ((e = backend_for(s, r->unit, &uid, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    if (!unit_has_mount(s, uid, r->mount))
        return resp_error(r->id, HELPER_E_DENIED, "%.300s is not a mount of %s", r->mount, r->unit);
    const char *dom = helper_policy_dir_domain(s->pol, r->mount);
    if (!dom || strcmp(dom, r->mount) == 0)
        return resp_error(r->id, HELPER_E_DENIED, "%.300s is in no snapshot domain of the policy", r->mount);
    char base[PATH_MAX], path[PATH_MAX];
    if (snprintf(base, sizeof(base), "%s/.cbackup-restore", dom) >= (int)sizeof(base) ||
        snprintf(path, sizeof(path), "%s/%lld", base, (long long)r->job) >= (int)sizeof(path))
        return resp_error(r->id, HELPER_E_INVALID, "path too long");
    if (mkdir_owned(base, s->pol->daemon_uid) != 0 || mkdir_owned(path, s->pol->daemon_uid) != 0)
        return resp_error(r->id, HELPER_E_FAILED, "cannot create %s: %s", path, strerror(errno));
    pthread_mutex_lock(&s->mu);
    staging_t *slot = NULL;
    for (size_t i = 0; i < STAGINGS_MAX && !slot; i++) if (!s->stages[i].used) slot = &s->stages[i];
    char token[HELPER_TOKEN_HEX + 1] = "";
    if (slot && helper_random_hex(slot->token, 16) == OK) {
        slot->used = 1;
        slot->swapped = 0;
        slot->job = r->job;
        snprintf(slot->unit, sizeof(slot->unit), "%s", r->unit);
        snprintf(slot->mount, sizeof(slot->mount), "%s", r->mount);
        snprintf(slot->path, sizeof(slot->path), "%s", path);
        memcpy(token, slot->token, sizeof(token));
    } else {
        slot = NULL;
    }
    pthread_mutex_unlock(&s->mu);
    if (!slot) return resp_error(r->id, HELPER_E_BUSY, "too many restores staged");
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "staging", token);
        cJSON_AddStringToObject(res, "path", path);
    }
    return resp_ok(r->id, res);
}

static char *op_restore_swap(helper_server_t *s, const helper_req_t *r)
{
    if (!s->pol->allow_restore) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow restore");
    pthread_mutex_lock(&s->mu);
    staging_t st, *x = NULL;
    for (size_t i = 0; i < STAGINGS_MAX && !x; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) x = &s->stages[i];
    if (x) st = *x;
    pthread_mutex_unlock(&s->mu);
    if (!x) return resp_error(r->id, HELPER_E_INVALID, "no such staging");
    if (st.swapped) return resp_error(r->id, HELPER_E_INVALID, "already swapped");
    int state = fake_unit_state(s->fake, st.unit + 5);
    if (state == SP_STATE_RUNNING || state == SP_STATE_PAUSED)
        return resp_error(r->id, HELPER_E_BUSY, "%s is running; stop it first", st.unit);
    /* Moved aside, never deleted: the operator can reverse it. */
    char old[PATH_MAX];
    if (snprintf(old, sizeof(old), "%s.cbackup-old-%lld", st.mount, (long long)st.job) >= (int)sizeof(old))
        return resp_error(r->id, HELPER_E_INVALID, "path too long");
    struct stat sb;
    if (lstat(old, &sb) == 0) return resp_error(r->id, HELPER_E_BUSY, "%.300s already exists", old);
    int had = lstat(st.mount, &sb) == 0;
    if (had && rename(st.mount, old) != 0)
        return resp_error(r->id, HELPER_E_FAILED, "move aside: %s", strerror(errno));
    if (rename(st.path, st.mount) != 0) {
        int e = errno;
        if (had) (void)rename(old, st.mount);
        return resp_error(r->id, HELPER_E_FAILED, "swap in: %s", strerror(e));
    }
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) s->stages[i].used = 0;
    pthread_mutex_unlock(&s->mu);
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "mount", st.mount);
        if (had) cJSON_AddStringToObject(res, "previous", old);
    }
    return resp_ok(r->id, res);
}

static char *op_unit_stop_start(helper_server_t *s, const helper_req_t *r, int start)
{
    if (!s->pol->allow_restore) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow restore");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    if ((e = backend_for(s, r->unit, &uid, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    if (fake_unit_set_state(s->fake, uid, start ? SP_STATE_RUNNING : SP_STATE_STOPPED) != OK)
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
    cJSON *res = cJSON_CreateObject();
    if (res) cJSON_AddStringToObject(res, "state", start ? "running" : "stopped");
    return resp_ok(r->id, res);
}

/* The helper parses a definition field by field and serialises its own;
 * none of the daemon's bytes would pass through. No backend here defines
 * units yet, so after the policy's checks the answer is unsupported. */
static char *op_unit_define(helper_server_t *s, const helper_req_t *r)
{
    if (!s->pol->allow_define) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow define");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    cJSON *d = cJSON_Parse(r->definition ? r->definition : "{}");
    int priv = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "privileged"));
    cJSON_Delete(d);
    if (priv && !s->pol->allow_privileged_define)
        return resp_error(r->id, HELPER_E_DENIED, "a privileged definition and allow_privileged_define is off");
    return resp_error(r->id, HELPER_E_UNSUPPORTED, "no provider in this helper defines units");
}

/* vm.*: validated and held to the policy here; libvirt is not built yet. */
static char *op_vm(helper_server_t *s, const helper_req_t *r)
{
    if (r->unit[0]) {
        char why[256];
        helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
        if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
        if (strncmp(r->unit, "libvirt:", 8) != 0)
            return resp_error(r->id, HELPER_E_INVALID, "%s is for libvirt units", helper_op_name(r->op));
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "the libvirt provider is not in this helper");
    }
    /* A token-only operation: no session was ever issued. */
    return resp_error(r->id, HELPER_E_INVALID, "no such session");
}

/* ------------------------------------------------------------------ */
/* Sweep                                                               */
/* ------------------------------------------------------------------ */

/* Lock order: jmu (held by release_holds) then mu. Nothing takes them the
 * other way round. */
static int pick_unowned(const hold_t *h, void *ctx)
{
    helper_server_t *s = ctx;
    int owned = 0;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < CAPTURES_MAX; i++)
        if (s->caps[i].used && s->caps[i].seq == h->capture) owned = 1;
    pthread_mutex_unlock(&s->mu);
    return !owned;
}

static void sweep_artefacts(helper_server_t *s, sp_sweep_report_t *rep)
{
    memset(rep, 0, sizeof(*rep));
    if (!s->fake) return;
    int64_t live[CAPTURES_MAX];
    size_t nl = 0;
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < CAPTURES_MAX; i++) if (s->caps[i].used) live[nl++] = s->caps[i].seq;
    pthread_mutex_unlock(&s->mu);
    sp_sweep_req_t rq;
    memset(&rq, 0, sizeof(rq));
    rq.daemon8 = s->pol->daemon_id;
    rq.scan_artefacts = 1;
    rq.live_jobs = live;
    rq.n_live_jobs = nl;
    if (fake_provider_ops.sweep(s->fake, &rq, rep) != OK) err_clear();
}

static char *op_sweep(helper_server_t *s, const helper_req_t *r)
{
    unsigned rel = release_holds(s, pick_unowned, s, "sweep");
    sp_sweep_report_t rep;
    sweep_artefacts(s, &rep);
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddNumberToObject(res, "released", rel);
        cJSON_AddNumberToObject(res, "removed", rep.removed);
        cJSON_AddNumberToObject(res, "left_foreign", rep.left_foreign);
        cJSON_AddNumberToObject(res, "holds", (double)helper_server_holds(s));
    }
    return resp_ok(r->id, res);
}

/* ------------------------------------------------------------------ */
/* Dispatch                                                            */
/* ------------------------------------------------------------------ */

static char *op_hello(helper_server_t *s, const helper_req_t *r)
{
    cJSON *res = cJSON_CreateObject();
    if (!res) return resp_error(r->id, HELPER_E_FAILED, "out of memory");
    cJSON_AddNumberToObject(res, "version", HELPER_PROTO_VERSION);
    cJSON_AddStringToObject(res, "daemon_id", s->pol->daemon_id);
    cJSON_AddStringToObject(res, "policy", s->pol->digest);
    cJSON *p = cJSON_AddArrayToObject(res, "providers");
    if (p && s->fake) cJSON_AddItemToArray(p, cJSON_CreateString("fake"));
    return resp_ok(r->id, res);
}

static char *dispatch(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    switch (r->op) {
    case HOP_HELLO:                  return op_hello(s, r);
    case HOP_UNITS_LIST:             return op_units_list(s, r);
    case HOP_UNITS_DESCRIBE:         return op_units_describe(s, r);
    case HOP_CAPTURE_BEGIN:          return op_capture_begin(s, r);
    case HOP_CAPTURE_BEGIN_PATHS:    return op_capture_begin_paths(s, r);
    case HOP_CAPTURE_END:            return op_capture_end(s, r);
    case HOP_HOOK_RUN:               return op_hook_run(s, r, pass_fd);
    case HOP_HOOK_WAIT:              return op_hook_wait(s, r);
    case HOP_VM_BACKUP_BEGIN:
    case HOP_VM_BACKUP_END:
    case HOP_VM_CHECKPOINT_REDEFINE:
    case HOP_VM_INSTALL_STATE:       return op_vm(s, r);
    case HOP_RESTORE_STAGE:          return op_restore_stage(s, r);
    case HOP_RESTORE_SWAP:           return op_restore_swap(s, r);
    case HOP_UNIT_STOP:              return op_unit_stop_start(s, r, 0);
    case HOP_UNIT_START:             return op_unit_stop_start(s, r, 1);
    case HOP_UNIT_DEFINE:            return op_unit_define(s, r);
    case HOP_LEASE_RELEASE:          return op_capture_end(s, r);
    case HOP_SWEEP:                  return op_sweep(s, r);
    case HOP_COUNT:                  break;
    }
    return resp_error(r->id, HELPER_E_DENIED, "operation does not exist");
}

void helper_server_handle(helper_server_t *s, const char *req, size_t len,
                          char **resp, int *pass_fd)
{
    *pass_fd = -1;
    helper_req_t r;
    char why[512];
    helper_err_t e = helper_request_parse(req, len, &r, why, sizeof(why));
    *resp = e == HELPER_OK ? dispatch(s, &r, pass_fd) : resp_error(r.id, e, "%s", why);
    helper_req_free(&r);
    err_clear();
}

/* ------------------------------------------------------------------ */
/* Connections                                                         */
/* ------------------------------------------------------------------ */

typedef struct { helper_server_t *s; int fd; int slot; } conn_arg_t;

static void *conn_main(void *arg)
{
    conn_arg_t *c = arg;
    helper_server_t *s = c->s;
    char *buf = malloc(HELPER_PACKET_MAX + 2);
    int greeted = 0;
    while (buf) {
        size_t got = 0;
        status_t st = helper_recv(c->fd, buf, HELPER_PACKET_MAX + 2, &got, NULL);
        if (st == ERR_TOO_LARGE) {
            char *e = resp_error(0, HELPER_E_INVALID, "a request is at most 65536 bytes");
            if (e) (void)helper_send(c->fd, e, strlen(e), -1);
            free(e);
            err_clear();
            continue;
        }
        if (st != OK) { err_clear(); break; }
        char *resp = NULL;
        int pfd = -1;
        if (!greeted) {
            helper_req_t r;
            char why[512];
            helper_err_t e = helper_request_parse(buf, got, &r, why, sizeof(why));
            if (e == HELPER_OK && r.op == HOP_HELLO) {
                resp = dispatch(s, &r, &pfd);
                greeted = 1;
            } else {
                resp = resp_error(r.id, e != HELPER_OK ? e : HELPER_E_DENIED,
                                  "%s", e != HELPER_OK ? why : "the first request must be hello");
            }
            helper_req_free(&r);
        } else {
            helper_server_handle(s, buf, got, &resp, &pfd);
        }
        if (resp) (void)helper_send(c->fd, resp, strlen(resp), pfd);
        err_clear();
        free(resp);
        if (pfd >= 0) close(pfd);
    }
    free(buf);
    pthread_mutex_lock(&s->cmu);
    close(c->fd);
    s->conns[c->slot] = -1;
    s->conn_live[c->slot] = 0;
    pthread_mutex_unlock(&s->cmu);
    free(c);
    return NULL;
}

static void *accept_main(void *arg)
{
    helper_server_t *s = arg;
    for (;;) {
        int fd = accept4(s->lfd, NULL, NULL, SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            break;
        }
        pthread_mutex_lock(&s->cmu);
        int stop = s->stop;
        pthread_mutex_unlock(&s->cmu);
        if (stop) { close(fd); break; }
        struct ucred cr;
        socklen_t cl = sizeof(cr);
        if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cr, &cl) != 0 || cr.uid != s->pol->daemon_uid) {
            log_warn("helper: refused a connection from uid %u (pid %d): only uid %u may connect",
                     (unsigned)cr.uid, (int)cr.pid, (unsigned)s->pol->daemon_uid);
            close(fd);
            continue;
        }
        /* A request with no answer in a minute is a stuck peer. */
        struct timeval tv = { 60, 0 };
        (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        pthread_mutex_lock(&s->cmu);
        int slot = -1;
        for (int i = 0; i < CONNS_MAX && slot < 0; i++) {
            if (s->conn_live[i]) continue;
            /* A finished thread's slot: join it before reuse. */
            if (s->conns[i] == -1 && s->conn_th[i]) {
                pthread_join(s->conn_th[i], NULL);
                s->conn_th[i] = 0;
            }
            slot = i;
        }
        conn_arg_t *c = slot >= 0 ? calloc(1, sizeof(*c)) : NULL;
        if (c) {
            c->s = s;
            c->fd = fd;
            c->slot = slot;
            s->conns[slot] = fd;
            s->conn_live[slot] = 1;
            if (pthread_create(&s->conn_th[slot], NULL, conn_main, c) != 0) {
                s->conns[slot] = -1;
                s->conn_live[slot] = 0;
                s->conn_th[slot] = 0;
                free(c);
                c = NULL;
            }
        }
        pthread_mutex_unlock(&s->cmu);
        if (!c) {
            log_warn("helper: too many connections; refusing one");
            close(fd);
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Watchdog                                                            */
/* ------------------------------------------------------------------ */

unsigned helper_server_tick(helper_server_t *s, time_t now)
{
    age_t a = { now, s->pol->max_pause_s };
    unsigned n = release_holds(s, pick_old, &a, "watchdog");
    /* Captures past their lease: ended, their snapshots destroyed. */
    for (;;) {
        capture_t c;
        int found = 0;
        pthread_mutex_lock(&s->mu);
        for (size_t i = 0; i < CAPTURES_MAX && !found; i++)
            if (s->caps[i].used == 1 && s->caps[i].deadline <= now) {
                c = s->caps[i];
                s->caps[i].used = 0;
                found = 1;
            }
        pthread_mutex_unlock(&s->mu);
        if (!found) break;
        log_warn("helper: capture %.8s... expired; destroying it", c.token);
        capture_finish(s, &c, SP_END_ABORTED);
        n++;
    }
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < RUNS_MAX; i++) if (s->runs[i].used) run_poll(&s->runs[i], now);
    pthread_mutex_unlock(&s->mu);
    return n;
}

static void *wd_main(void *arg)
{
    helper_server_t *s = arg;
    pthread_mutex_lock(&s->cmu);
    while (!s->stop) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += (time_t)(s->wd_ms / 1000);
        ts.tv_nsec += (long)(s->wd_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        pthread_cond_timedwait(&s->wcv, &s->cmu, &ts);
        if (s->stop) break;
        pthread_mutex_unlock(&s->cmu);
        (void)helper_server_tick(s, time(NULL));
        pthread_mutex_lock(&s->cmu);
    }
    pthread_mutex_unlock(&s->cmu);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Start and stop                                                      */
/* ------------------------------------------------------------------ */

static status_t listen_on(helper_server_t *s)
{
    const char *path = s->pol->socket;
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(a.sun_path)) return set_error(ERR_INVALID, "helper: socket path too long");
    memcpy(a.sun_path, path, strlen(path) + 1);
    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (fd < 0) return set_error_errno(ERR_IO, "helper: socket");
    (void)unlink(path);
    mode_t old = umask(0117);   /* the socket 0660 */
    int rc = bind(fd, (struct sockaddr *)&a, sizeof(a));
    umask(old);
    if (rc != 0 || listen(fd, 16) != 0) {
        close(fd);
        return set_error_errno(ERR_IO, "helper: bind %s", path);
    }
    /* 0660 root:root let nobody but root connect: the daemon, running as
     * daemon_uid with its own gid (security.run_as_uid sets both), had no
     * write permission on the socket and connect failed with EACCES.
     * The group is the daemon's; SO_PEERCRED still refuses every uid but
     * daemon_uid on each connection. */
    if (geteuid() == 0 && chown(path, (uid_t)-1, (gid_t)s->pol->daemon_uid) != 0) {
        int err = errno;
        close(fd);
        (void)unlink(path);
        errno = err;
        return set_error_errno(ERR_IO, "helper: chown %s to group %u", path, (unsigned)s->pol->daemon_uid);
    }
    s->lfd = fd;
    return OK;
}

status_t helper_server_start(const helper_server_cfg_t *cfg, helper_server_t **out)
{
    helper_server_t *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "helper: server");
    s->pol = cfg->policy;
    s->fake = cfg->fake;
    s->lfd = -1;
    s->wd_ms = cfg->watchdog_ms ? cfg->watchdog_ms : 1000;
    for (int i = 0; i < CONNS_MAX; i++) s->conns[i] = -1;
    pthread_mutex_init(&s->mu, NULL);
    pthread_mutex_init(&s->jmu, NULL);
    pthread_mutex_init(&s->cmu, NULL);
    pthread_cond_init(&s->wcv, NULL);
    if (mkdir(s->pol->state_dir, 0700) != 0 && errno != EEXIST) {
        status_t st = set_error_errno(ERR_IO, "helper: state directory %s", s->pol->state_dir);
        helper_server_stop(s);
        return st;
    }

    /* Whatever the last helper process held is released before anything
     * new is taken, and leftovers of this daemon id are destroyed. */
    journal_load(s);
    unsigned rel = release_holds(s, pick_all, NULL, "start-up sweep");
    sp_sweep_report_t rep;
    sweep_artefacts(s, &rep);
    if (rel || rep.removed)
        log_warn("helper: start-up sweep released %u hold(s), removed %u artefact(s)", rel, rep.removed);
    pthread_mutex_lock(&s->jmu);
    journal_save(s);
    pthread_mutex_unlock(&s->jmu);

    if (cfg->listen) {
        status_t st = listen_on(s);
        if (st == OK && pthread_create(&s->accept_th, NULL, accept_main, s) == 0) s->accept_on = 1;
        else if (st == OK) st = set_error(ERR_IO, "helper: accept thread");
        if (st != OK) { helper_server_stop(s); return st; }
    }
    if (pthread_create(&s->wd_th, NULL, wd_main, s) == 0) s->wd_on = 1;
    *out = s;
    return OK;
}

void helper_server_stop(helper_server_t *s)
{
    if (!s) return;
    pthread_mutex_lock(&s->cmu);
    s->stop = 1;
    pthread_cond_broadcast(&s->wcv);
    for (int i = 0; i < CONNS_MAX; i++) if (s->conns[i] >= 0) shutdown(s->conns[i], SHUT_RDWR);
    pthread_mutex_unlock(&s->cmu);
    if (s->lfd >= 0) shutdown(s->lfd, SHUT_RDWR);
    if (s->accept_on) pthread_join(s->accept_th, NULL);
    if (s->lfd >= 0) {
        close(s->lfd);
        if (s->pol) (void)unlink(s->pol->socket);
    }
    if (s->wd_on) pthread_join(s->wd_th, NULL);
    for (int i = 0; i < CONNS_MAX; i++) if (s->conn_th[i]) pthread_join(s->conn_th[i], NULL);
    /* Captures still held are ended; their holds go with them. */
    for (size_t i = 0; i < CAPTURES_MAX; i++)
        if (s->caps[i].used == 1) { capture_t c = s->caps[i]; s->caps[i].used = 0; capture_finish(s, &c, SP_END_ABORTED); }
    for (size_t i = 0; i < RUNS_MAX; i++)
        if (s->runs[i].used) {
            if (!s->runs[i].done) { kill(s->runs[i].pid, SIGKILL); waitpid(s->runs[i].pid, NULL, 0); }
            close(s->runs[i].err_fd);
            free(s->runs[i].log);
        }
    pthread_cond_destroy(&s->wcv);
    pthread_mutex_destroy(&s->cmu);
    pthread_mutex_destroy(&s->jmu);
    pthread_mutex_destroy(&s->mu);
    free(s);
}

size_t helper_server_captures(helper_server_t *s)
{
    pthread_mutex_lock(&s->mu);
    size_t n = 0;
    for (size_t i = 0; i < CAPTURES_MAX; i++) n += s->caps[i].used == 1;
    pthread_mutex_unlock(&s->mu);
    return n;
}

size_t helper_server_holds(helper_server_t *s)
{
    pthread_mutex_lock(&s->jmu);
    size_t n = s->n_holds;
    pthread_mutex_unlock(&s->jmu);
    return n;
}
