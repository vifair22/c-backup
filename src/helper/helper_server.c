#define _GNU_SOURCE

#include "helper_internal.h"
#include "helper_fd.h"
#include "helper_journal.h"
#include "msg_clip.h"
#include "artefact.h"
#include "helper_backend.h"
#include "helper_proto.h"
#include "hooklog.h"
#include "db_template.h"
#include "paths_backend.h"
#include "unit_probe.h"

#include <sys/mman.h>

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "thread_name.h"

#include <cutils/log.h>

#include <dirent.h>
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


/* ------------------------------------------------------------------ */
/* Responses                                                           */
/* ------------------------------------------------------------------ */

static char *resp_error(int64_t id, helper_err_t code, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
static char *resp_error(int64_t id, helper_err_t code, const char *fmt, ...)
{
    /* Room for a refusal that names its way round (B210, B223): it was
     * cut at 511, mid-sentence; one longer still is cut visibly. */
    char msg[MSG_CLIP_MAX];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    msg_clip_after(msg, sizeof(msg), n);
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

/* The backend for a unit key, or why not. On OK *uid points into key. */
static helper_err_t backend_for(helper_server_t *s, const char *key, const char **uid,
                                const helper_backend_t **b, char *why, size_t wsz)
{
    char prov[16], id[HELPER_UNIT_KEY_MAX + 1];
    if (!helper_unit_key_ok(key, prov, sizeof(prov), id, sizeof(id))) {
        snprintf(why, wsz, "malformed unit key");
        return HELPER_E_INVALID;
    }
    const helper_backend_t *found = hj_backend_of(s, key, uid);
    if (!found) {
        snprintf(why, wsz, "provider %s is not available in this helper", prov);
        return HELPER_E_UNSUPPORTED;
    }
    if (b) *b = found;
    return HELPER_OK;
}

static const helper_backend_t *backend_named(const helper_server_t *s, const char *prov)
{
    for (size_t i = 0; i < s->nb; i++)
        if (strcmp(s->backends[i].name, prov) == 0) return &s->backends[i];
    return NULL;
}

typedef struct { const char *id; char name[256]; int found; } name_find_t;

static status_t find_name(void *ctx, const char *id, const char *name, int state)
{
    (void)state;
    name_find_t *f = ctx;
    if (!f->found && strcmp(id, f->id) == 0) {
        snprintf(f->name, sizeof(f->name), "%s", name ? name : "");
        f->found = 1;
    }
    return OK;
}

/* Policy first, by key; then by name, which only the helper's own inspect
 * can supply. A unit the policy does not allow is denied whether or not it
 * exists, so the answer says nothing about what lies outside. */
static helper_err_t unit_allowed(helper_server_t *s, const char *key, char *why, size_t wsz)
{
    if (helper_policy_unit_allowed(s->pol, key, NULL)) return HELPER_OK;
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if (backend_for(s, key, &uid, &b, why, wsz) == HELPER_OK) {
        /* The name from the enumeration, not a describe: a unit whose
         * describe fails (a missing NVRAM file, say) is still the unit the
         * policy allows, and its describe then says what is wrong. */
        name_find_t nf = { uid, "", 0 };
        if (b->ops->list_units(b->ctx, find_name, &nf) == OK && nf.found &&
            helper_policy_unit_allowed(s->pol, key, nf.name))
            return HELPER_OK;
        err_clear();
    }
    snprintf(why, wsz, "unit %s is not in the policy", key);
    return HELPER_E_DENIED;
}

typedef struct { helper_server_t *s; cJSON *arr; const char *prov; } list_ctx_t;

static status_t list_cb(void *ctx, const char *id, const char *name, int state)
{
    list_ctx_t *l = ctx;
    char key[HELPER_UNIT_KEY_MAX + 24];
    snprintf(key, sizeof(key), "%s:%s", l->prov, id);
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
    const helper_backend_t *b = backend_named(s, r->provider);
    if (!b)
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "provider %s is not available in this helper", r->provider);
    cJSON *res = cJSON_CreateObject();
    cJSON *arr = res ? cJSON_AddArrayToObject(res, "units") : NULL;
    if (!arr) { cJSON_Delete(res); return resp_error(r->id, HELPER_E_FAILED, "out of memory"); }
    list_ctx_t l = { s, arr, b->name };
    if (b->ops->list_units(b->ctx, list_cb, &l) != OK) {
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    }
    return resp_ok(r->id, res);
}

static char *op_units_describe(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    sp_unit_desc_t *d = NULL;
    if (b->ops->describe(b->ctx, uid, &d) != OK)
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    int bfd = -1;
    uint64_t boff = 0;
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
            if (!b->blob_data || b->blob_data(b->ctx, uid, it->id, &data, &len) != OK) {
                free(data);
                err_clear();
                bad = 2;
                break;
            }
            uint64_t at = boff;
            int werr = len > HELPER_BLOB_MAX || helper_blob_fd_append(&bfd, &boff, data, len) != 0;
            free(data);
            if (werr) { bad = 3; break; }
            cJSON_AddNumberToObject(o, "data_off", (double)at);
            cJSON_AddNumberToObject(o, "data_len", (double)len);
        } else if (b->item_extra && b->item_extra(b->ctx, uid, it, o) != OK) {
            err_clear();
            bad = 1;
            break;
        }
    }
    b->ops->desc_free(d);
    if (!bad && bfd >= 0 &&
        fcntl(bfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL) != 0)
        bad = 3;
    if (bad) {
        if (bfd >= 0) close(bfd);
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "%s",
                          bad == 2 ? "a configuration item could not be read"
                          : bad == 3 ? "a configuration item could not be passed (too large, or no memfd)"
                          : "out of memory");
    }
    if (bfd >= 0) {
        cJSON_AddNumberToObject(res, "blob_bytes", (double)boff);
        *pass_fd = bfd;
    }
    return resp_ok(r->id, res);
}

/* units.probe (db-detection.md 8.1): read-only, gated like describe. The
 * answer is cut to fit one packet with its envelope, never refused for
 * size. */
static char *op_units_probe(helper_server_t *s, const helper_req_t *r)
{
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    if (!b->ops->unit_probe)
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "provider %s cannot probe its units", b->name);
    sp_unit_probe_t *p = NULL;
    if (b->ops->unit_probe(b->ctx, uid, &p) != OK)
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    /* {"id":<up to 2^53>,"ok":true,"result":...} around it. */
    cJSON *res = sp_unit_probe_to_json(p, HELPER_PACKET_MAX - 64);
    sp_unit_probe_free(p);
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
    if (c->pset) {
        /* A snapshot that will not go stays journalled: the sweep and the
         * next start retry it (snap_release). */
        sp_holds_t h = { hj_record, hj_clear, c->pj };
        if (snap_release(paths_snap(c->pb), c->pset, s->pol->daemon_id, &h) != OK) {
            log_warn("helper: ending a paths capture: %s; the sweep will retry", err_msg());
            err_clear();
        }
        free(c->pset);
        c->pset = NULL;
    }
    free(c->pj);
    c->pj = NULL;
    for (size_t i = 0; i < c->n_members; i++) {
        if (!c->members[i].sess) continue;
        if (c->members[i].b->ops->end(c->members[i].sess, how, 0) != OK) {
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

/*
 * Daemon sessions (B169). A daemon killed mid-backup never sends
 * capture.end, so its capture, and every hold under it (a pause, an
 * export), lasted until capture_max_s (a day) or a helper restart, and the
 * daemon's own holds_open said 0 meanwhile. The daemon keeps one anchor
 * connection open for as long as it runs; when the last anchor of a
 * session closes, which the kernel does for a killed process, the
 * session's captures are ended as aborted, releasing their holds through
 * the backend the way capture.end would. A capture still in its begin is
 * left to that begin, which ends it itself on finding its session gone,
 * so a hold recorded and not yet taken is never released from outside
 * (the record-before-take rule, B151). Caller holds mu for the first two.
 */
static int session_anchored(const helper_server_t *s, const char *tok)
{
    if (!tok[0]) return 0;
    for (size_t i = 0; i < CONNS_MAX; i++)
        if (s->sessions[i].anchors > 0 && strcmp(s->sessions[i].token, tok) == 0) return 1;
    return 0;
}

static void anchor_add(helper_server_t *s, const char *tok)
{
    pthread_mutex_lock(&s->mu);
    dsession_t *free_slot = NULL, *have = NULL;
    for (size_t i = 0; i < CONNS_MAX && !have; i++) {
        if (s->sessions[i].anchors > 0 && strcmp(s->sessions[i].token, tok) == 0) have = &s->sessions[i];
        else if (!free_slot && s->sessions[i].anchors == 0) free_slot = &s->sessions[i];
    }
    if (!have && free_slot) {
        have = free_slot;
        snprintf(have->token, sizeof(have->token), "%s", tok);
    }
    if (have) have->anchors++;
    pthread_mutex_unlock(&s->mu);
}

/* The anchor closed: on the session's last, end its captures. */
static void anchor_drop(helper_server_t *s, const char *tok)
{
    pthread_mutex_lock(&s->cmu);
    int stopping = s->stop;
    pthread_mutex_unlock(&s->cmu);
    capture_t *ended = calloc(CAPTURES_MAX, sizeof(*ended));
    size_t n = 0;
    pthread_mutex_lock(&s->mu);
    int last = 0;
    for (size_t i = 0; i < CONNS_MAX; i++)
        if (s->sessions[i].anchors > 0 && strcmp(s->sessions[i].token, tok) == 0)
            last = --s->sessions[i].anchors == 0;
    /* At a helper stop the stop ends every capture itself. */
    for (size_t i = 0; last && !stopping && ended && i < CAPTURES_MAX; i++)
        if (s->caps[i].used == 1 && s->caps[i].anchored && strcmp(s->caps[i].session, tok) == 0) {
            ended[n++] = s->caps[i];
            s->caps[i].used = 0;
        }
    pthread_mutex_unlock(&s->mu);
    if (n)
        log_warn("helper: the daemon's session %.8s... ended (its connection closed) with %zu capture(s) "
                 "open; ending them and releasing their holds", tok, n);
    for (size_t i = 0; i < n; i++) capture_finish(s, &ended[i], SP_END_ABORTED);
    free(ended);
}

static char *op_capture_begin(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    int lfd = -1;             /* items only known after begin: hook logs */
    uint64_t loff = 0;
    char why[256];
    for (size_t i = 0; i < r->n_units; i++) {
        helper_err_t e = unit_allowed(s, r->units[i], why, sizeof(why));
        if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
        const char *uid = NULL;
        if ((e = backend_for(s, r->units[i], &uid, NULL, why, sizeof(why))) != HELPER_OK)
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
    int anchored = session_anchored(s, r->conn_session);
    pthread_mutex_unlock(&s->mu);
    if (!ok) return resp_error(r->id, HELPER_E_BUSY, "more than %u captures in the last hour",
                               s->pol->max_captures_per_hour);
    if (!slot) return resp_error(r->id, HELPER_E_BUSY, "too many captures held");

    /* The daemon closing the connection is its job's cancel (B278): the
     * backend's pre hooks stop on it, and the begin fails rather than
     * leave a capture nobody waits for. */
    atomic_int gone = 0;
    conn_watch_t cw;
    conn_watch_start(&cw, r->conn_fd, &gone);
    capture_t c;
    memset(&c, 0, sizeof(c));
    c.seq = seq;
    snprintf(c.session, sizeof(c.session), "%s", r->conn_session);
    c.anchored = anchored;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    cJSON *res = cJSON_CreateObject();
    cJSON *mem = res ? cJSON_AddArrayToObject(res, "members") : NULL;
    helper_err_t fail = mem ? HELPER_OK : HELPER_E_FAILED;
    /* Room for the longest it is given: a unit key, ": " and 400 bytes of the
     * error (the others take at most 500), so nothing in it is cut short. It
     * was 512, under the 563 a full-length key needs, and -Wformat-truncation
     * said so in the debug and asan builds. */
    char fmsg[MSG_CLIP_MAX] = "out of memory";
    /* Several units of one backend that can capture them together: one
     * pause and one snapshot set for all (6.2), a session each. */
    sp_unit_desc_t *gd[HELPER_UNITS_MAX] = { 0 };
    sp_session_t *gs[HELPER_UNITS_MAX] = { 0 };
    const sp_session_info_t *gi[HELPER_UNITS_MAX] = { 0 };
    jctx_t *gj = NULL;
    int grp = 0;
    if (fail == HELPER_OK && r->n_units > 1) {
        const char *u0 = NULL;
        const helper_backend_t *b0 = hj_backend_of(s, r->units[0], &u0);
        grp = b0 && b0->ops->begin_group != NULL;
        for (size_t i = 1; grp && i < r->n_units; i++) grp = hj_backend_of(s, r->units[i], NULL) == b0;
        for (size_t i = 0; grp && fail == HELPER_OK && i < r->n_units; i++) {
            const char *uid = NULL;
            (void)hj_backend_of(s, r->units[i], &uid);
            if (b0->ops->describe(b0->ctx, uid, &gd[i]) != OK) {
                msg_clip(fmsg, sizeof(fmsg), err_msg());
                fail = HELPER_E_FAILED;
            }
        }
        if (grp && fail == HELPER_OK && !(gj = calloc(1, sizeof(*gj)))) fail = HELPER_E_FAILED;
        if (grp && fail == HELPER_OK) {
            gj->s = s;
            snprintf(gj->unit, sizeof(gj->unit), "%s", r->units[0]);
            gj->capture = seq;
            sp_holds_t gh = { hj_record, hj_clear, gj };
            sp_begin_req_t q;
            memset(&q, 0, sizeof(q));
            q.job_id = seq;
            q.want = r->pause ? SP_CONSIST_QUIESCED : SP_CONSIST_CRASH;
            q.require = SP_CONSIST_INCONSISTENT;
            q.freeze_timeout_s = s->pol->max_pause_s;
            q.pause_max_s = s->pol->max_pause_s;
            q.holds = &gh;
            q.cancel = &gone;
            q.daemon8 = s->pol->daemon_id;
            if (b0->ops->begin_group(b0->ctx, (const sp_unit_desc_t *const *)gd, r->n_units, &q, gs, gi) != OK) {
                msg_clip(fmsg, sizeof(fmsg), err_msg());
                fail = HELPER_E_FAILED;
            }
        }
        if (grp && fail != HELPER_OK) {
            for (size_t i = 0; i < r->n_units; i++) if (gd[i]) b0->ops->desc_free(gd[i]);
            free(gj);
            gj = NULL;
        }
    }
    for (size_t i = 0; fail == HELPER_OK && i < r->n_units; i++) {
        const char *uid = NULL;
        const helper_backend_t *b = hj_backend_of(s, r->units[i], &uid);
        sp_unit_desc_t *d = NULL;
        jctx_t *j = NULL;
        sp_session_t *sess = NULL;
        const sp_session_info_t *info = NULL;
        status_t st = OK;
        if (grp) {
            /* The last member carries the group's journal context: the
             * shared snapshots are released through it at the last end,
             * and members end in order. */
            d = gd[i];
            sess = gs[i];
            info = gi[i];
            gd[i] = NULL;
            gs[i] = NULL;
            j = i + 1 == r->n_units ? gj : NULL;
            if (j) gj = NULL;
            goto have_session;
        }
        if (!b || b->ops->describe(b->ctx, uid, &d) != OK) {
            msg_clip(fmsg, sizeof(fmsg), err_msg());
            fail = HELPER_E_FAILED;
            break;
        }
        j = calloc(1, sizeof(*j));
        if (!j) { b->ops->desc_free(d); fail = HELPER_E_FAILED; break; }
        j->s = s;
        snprintf(j->unit, sizeof(j->unit), "%s", r->units[i]);
        j->capture = seq;
        sp_holds_t holds = { hj_record, hj_clear, j };
        sp_begin_req_t q;
        memset(&q, 0, sizeof(q));
        q.job_id = seq;
        q.want = r->pause ? SP_CONSIST_QUIESCED : SP_CONSIST_CRASH;
        q.require = SP_CONSIST_INCONSISTENT;
        q.freeze_timeout_s = s->pol->max_pause_s;
        q.pause_max_s = s->pol->max_pause_s;
        q.holds = &holds;
        q.cancel = &gone;
        q.daemon8 = s->pol->daemon_id;
        /* pause: a pause may be held for the daemon's second pass over
         * mounts on no snapshot domain, released with lease.release. */
        q.hold_pause = r->hold;
        /* vm.backup_begin (vm-backup.md 5.8): the disks that take part in
         * the chain, against base where base covers them; base alone, with
         * no disk taking part, is only retired. The backend accepts base
         * only if its own records say it made it for this daemon. */
        sp_item_base_t vb[HELPER_DISKS_MAX];
        size_t nvb = 0;
        if (r->op == HOP_VM_BACKUP_BEGIN) {
            for (size_t k = 0; k < r->n_disks && nvb < HELPER_DISKS_MAX; k++) {
                if (!r->disks[k].incremental) continue;
                vb[nvb].item_id = r->disks[k].target;
                vb[nvb].base_token = r->base[0] ? r->base : NULL;
                nvb++;
            }
            q.bases = vb;
            q.n_bases = nvb;
            q.retire_token = nvb == 0 && r->base[0] ? r->base : NULL;
        }
        st = b->ops->begin(b->ctx, d, &q, &sess, &info);
        /* The journal context outlives begin: end releases through it, so
         * it lives with the member and goes after end. */
        if (st != OK) {
            msg_clip_after(fmsg, sizeof(fmsg), snprintf(fmsg, sizeof(fmsg), "%s: %s", r->units[i], err_msg()));
            b->ops->desc_free(d);
            free(j);
            fail = HELPER_E_FAILED;
            break;
        }
    have_session:;
        member_t *m = &c.members[c.n_members++];
        snprintf(m->unit, sizeof(m->unit), "%s", r->units[i]);
        m->b = b;
        m->sess = sess;
        m->j = j;
        cJSON *o = cJSON_CreateObject();
        if (!o) { b->ops->desc_free(d); fail = HELPER_E_FAILED; break; }
        cJSON_AddItemToArray(mem, o);
        cJSON_AddStringToObject(o, "unit", r->units[i]);
        cJSON_AddNumberToObject(o, "achieved", info->achieved);
        cJSON_AddNumberToObject(o, "flags", info->consist_flags);
        cJSON_AddNumberToObject(o, "freeze_ms", (double)info->freeze_ms);
        cJSON_AddNumberToObject(o, "pause_ms", (double)info->pause_ms);
        cJSON *w = cJSON_AddArrayToObject(o, "warnings");
        for (size_t k = 0; w && k < info->n_warnings; k++)
            cJSON_AddItemToArray(w, cJSON_CreateString(info->warnings[k]));
        if (info->new_token) cJSON_AddStringToObject(o, "new_token", info->new_token);
        if (info->new_token_blob) cJSON_AddStringToObject(o, "token_blob", info->new_token_blob);
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
            if (b->ops->tree_root(sess, &d->items[k], root, sizeof(root)) != OK) {
                msg_clip(fmsg, sizeof(fmsg), err_msg());
                fail = HELPER_E_FAILED;
                break;
            }
            cJSON_AddStringToObject(rr, d->items[k].id, root);
        }
        /* Where each disk is read, as the backend set it up. */
        cJSON *blk = b->block_where ? cJSON_AddObjectToObject(o, "blocks") : NULL;
        for (size_t k = 0; blk && fail == HELPER_OK && k < d->n_items; k++) {
            if (d->items[k].kind != SP_ITEM_BLOCK) continue;
            cJSON *where = cJSON_CreateObject();
            if (!where) break;
            if (b->block_where(sess, &d->items[k], where) != OK) {
                msg_clip(fmsg, sizeof(fmsg), err_msg());
                cJSON_Delete(where);
                fail = HELPER_E_FAILED;
                break;
            }
            cJSON_AddItemToObject(blk, d->items[k].id, where);
        }
        /* Hook logs exist only now that begin ran the hooks. */
        cJSON *late = cJSON_AddObjectToObject(o, "late_blobs");
        for (size_t k = 0; late && fail == HELPER_OK && k < d->n_items; k++) {
            const sp_item_desc_t *it = &d->items[k];
            if (it->kind != SP_ITEM_BLOB || strncmp(it->id, "log:", 4) != 0) continue;
            void *data = NULL;
            size_t len = 0;
            if (b->ops->blob_read(sess, it, &data, &len) != OK) {
                /* Reported, never skipped silently (B286): the empty log
                 * describe passed would be stored as if the hook said
                 * nothing. The unit's warning is what the job carries. */
                char wm[384];
                snprintf(wm, sizeof(wm), "hook log %.64s could not be read at the end of begin, so what is "
                         "stored for it may be empty: %.200s", it->id + 4, err_msg());
                log_warn("helper: %s: %s", r->units[i], wm);
                if (w) cJSON_AddItemToArray(w, cJSON_CreateString(wm));
                err_clear();
                continue;
            }
            uint64_t at = loff;
            int werr = len > HELPER_BLOB_MAX || helper_blob_fd_append(&lfd, &loff, data, len) != 0;
            free(data);
            if (werr) { snprintf(fmsg, sizeof(fmsg), "cannot pass the hook logs"); fail = HELPER_E_FAILED; break; }
            cJSON *pair = cJSON_CreateArray();
            if (!pair) break;
            cJSON_AddItemToArray(pair, cJSON_CreateNumber((double)at));
            cJSON_AddItemToArray(pair, cJSON_CreateNumber((double)len));
            cJSON_AddItemToObject(late, it->id, pair);
        }
        b->ops->desc_free(d);
    }
    conn_watch_stop(&cw, r->conn_session);
    if (fail == HELPER_OK && atomic_load(&gone)) {
        snprintf(fmsg, sizeof(fmsg), "the daemon stopped waiting for the begin (its job was cancelled)");
        fail = HELPER_E_FAILED;
    }
    if (fail == HELPER_OK && lfd >= 0 &&
        fcntl(lfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL) != 0) {
        snprintf(fmsg, sizeof(fmsg), "cannot seal the hook logs");
        fail = HELPER_E_FAILED;
    }
    if (fail != HELPER_OK && lfd >= 0) { close(lfd); lfd = -1; }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (fail != HELPER_OK) {
        cJSON_Delete(res);
        capture_finish(s, &c, SP_END_ABORTED);
        if (grp) {
            /* Group sessions that never became members end after those
             * that did; the journal context the shared snapshots are
             * released through goes last. */
            const helper_backend_t *b0 = hj_backend_of(s, r->units[0], NULL);
            for (size_t i = 0; b0 && i < r->n_units; i++) {
                if (gs[i]) (void)b0->ops->end(gs[i], SP_END_ABORTED, 0);
                if (gd[i]) b0->ops->desc_free(gd[i]);
            }
            err_clear();
            free(gj);
        }
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
        if (lfd >= 0) close(lfd);
        return resp_error(r->id, HELPER_E_FAILED, "no randomness for a token");
    }
    c.deadline = now + (time_t)s->pol->capture_max_s;
    c.used = 1;
    pthread_mutex_lock(&s->mu);
    /* Its session may have ended while it began: nobody will end it now. */
    int orphan = c.anchored && !session_anchored(s, c.session);
    if (orphan) slot->used = 0;
    else *slot = c;
    pthread_mutex_unlock(&s->mu);
    if (orphan) {
        log_warn("helper: the daemon's session %.8s... ended during a capture's begin; ending it", c.session);
        cJSON_Delete(res);
        capture_finish(s, &c, SP_END_ABORTED);
        if (lfd >= 0) close(lfd);
        return resp_error(r->id, HELPER_E_FAILED, "the daemon's session ended during the begin");
    }
    cJSON_AddStringToObject(res, "capture", c.token);
    cJSON_AddNumberToObject(res, "snapshot_ms",
                            (double)((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000));
    cJSON_AddNumberToObject(res, "expires", (double)c.deadline);
    if (lfd >= 0) *pass_fd = lfd;
    return resp_ok(r->id, res);
}

/*
 * Source snapshot mode for plain paths (db-detection.md 9.4). Every path is
 * the helper's to resolve: a dir domain of the policy (tests: no snapshot,
 * the read root is the path itself), else the ZFS dataset or btrfs pool it
 * really lives in (paths_backend.h), checked against the policy. One
 * snapshot per pool for all of them, named by the helper, no pause. A path
 * that cannot be snapshotted is reported with the reason and left to the
 * daemon to read live; a request where none can takes no capture at all.
 * The capture has the same lease, rate limit, session anchoring, expiry
 * and sweep as capture.begin, and its snapshots are journalled as
 * "paths:capture" holds before they are taken.
 */
enum { BP_LIVE = 0, BP_DIR = 1, BP_SNAP = 2, BP_DUP = 3 };

static char *op_capture_begin_paths(helper_server_t *s, const helper_req_t *r)
{
    const helper_backend_t *pb = backend_named(s, PATHS_BACKEND_NAME);
    paths_res_t *res = calloc(r->n_paths, sizeof(*res));
    int *kind = calloc(r->n_paths, sizeof(*kind));
    snap_set_t *set = calloc(1, sizeof(*set));
    jctx_t *pj = calloc(1, sizeof(*pj));
    if (!res || !kind || !set || !pj) {
        free(res);
        free(kind);
        free(set);
        free(pj);
        return resp_error(r->id, HELPER_E_FAILED, "out of memory");
    }
    char fmsg[600] = "out of memory";
    helper_err_t fail = HELPER_OK;
    size_t n_snap = 0, n_dir = 0;
    snap_probe_t *probe = NULL;
    if (fail == HELPER_OK && pb && paths_backend_any_domain(pb) && paths_probe(pb, &probe) != OK) {
        snprintf(fmsg, sizeof(fmsg), "the host's mount table: %.500s", err_msg());
        err_clear();
        fail = HELPER_E_FAILED;
    }
    for (size_t i = 0; fail == HELPER_OK && i < r->n_paths; i++) {
        for (size_t k = 0; k < i && !kind[i]; k++)
            if (strcmp(r->paths[k], r->paths[i]) == 0) kind[i] = BP_DUP;
        if (kind[i] == BP_DUP) continue;
        if (helper_policy_dir_domain(s->pol, r->paths[i])) { kind[i] = BP_DIR; n_dir++; continue; }
        if (!pb || !paths_backend_any_domain(pb)) {
            snprintf(res[i].reason, sizeof(res[i].reason), "the helper's policy lists no ZFS dataset or btrfs pool");
            continue;
        }
        if (paths_resolve(pb, probe, r->paths[i], &res[i]) != OK) {
            snprintf(fmsg, sizeof(fmsg), "%.300s: %.280s", r->paths[i], err_msg());
            err_clear();
            fail = HELPER_E_FAILED;
            break;
        }
        if (res[i].dom.kind == SNAP_NONE) continue;
        if (snap_set_add(set, &res[i].dom) != OK) {
            err_clear();
            snprintf(res[i].reason, sizeof(res[i].reason), "more than %d snapshot domains in one capture", SNAP_DOMS_MAX);
            continue;
        }
        kind[i] = BP_SNAP;
        n_snap++;
    }

    time_t now = time(NULL);
    capture_t *slot = NULL;
    int64_t seq = 0;
    int anchored = 0;
    if (fail == HELPER_OK && n_snap + n_dir > 0) {
        pthread_mutex_lock(&s->mu);
        int ok = rate_ok(s, now);
        for (size_t i = 0; ok && i < CAPTURES_MAX && !slot; i++)
            if (!s->caps[i].used) slot = &s->caps[i];
        seq = ++s->seq;
        if (slot) {
            memset(slot, 0, sizeof(*slot));
            slot->used = 2;       /* reserved: not findable until filled */
            slot->seq = seq;
        }
        anchored = session_anchored(s, r->conn_session);
        pthread_mutex_unlock(&s->mu);
        if (!ok) {
            fail = HELPER_E_BUSY;
            snprintf(fmsg, sizeof(fmsg), "more than %u captures in the last hour", s->pol->max_captures_per_hour);
        } else if (!slot) {
            fail = HELPER_E_BUSY;
            snprintf(fmsg, sizeof(fmsg), "too many captures held");
        }
    }

    capture_t c;
    memset(&c, 0, sizeof(c));
    c.seq = seq;
    snprintf(c.session, sizeof(c.session), "%s", r->conn_session);
    c.anchored = anchored;
    c.pb = pb;
    if (fail == HELPER_OK && n_snap > 0) {
        pj->s = s;
        snprintf(pj->unit, sizeof(pj->unit), "%s:capture", PATHS_BACKEND_NAME);
        pj->capture = seq;
        sp_holds_t h = { hj_record, hj_clear, pj };
        if (snap_take(paths_snap(pb), set, s->pol->daemon_id, seq, &h) != OK) {
            snprintf(fmsg, sizeof(fmsg), "taking the snapshots: %.500s", err_msg());
            err_clear();
            fail = HELPER_E_FAILED;
        } else {
            c.pset = set;
            c.pj = pj;
            set = NULL;
            pj = NULL;
        }
    }

    cJSON *out = fail == HELPER_OK ? cJSON_CreateObject() : NULL;
    if (fail == HELPER_OK && !out) fail = HELPER_E_FAILED;
    cJSON *rr = out ? cJSON_AddObjectToObject(out, "read_roots") : NULL;
    cJSON *kd = out ? cJSON_AddObjectToObject(out, "kinds") : NULL;
    cJSON *lv = out ? cJSON_AddObjectToObject(out, "live") : NULL;
    cJSON *ns = out ? cJSON_AddObjectToObject(out, "nested") : NULL;
    cJSON *sn = out ? cJSON_AddObjectToObject(out, "snapshots") : NULL;
    if (out && (!rr || !kd || !lv || !ns || !sn)) fail = HELPER_E_FAILED;
    for (size_t i = 0; fail == HELPER_OK && i < r->n_paths; i++) {
        const char *p = r->paths[i];
        if (kind[i] == BP_DUP) continue;
        if (kind[i] == BP_LIVE) { cJSON_AddStringToObject(lv, p, res[i].reason); continue; }
        if (kind[i] == BP_DIR) {
            cJSON_AddStringToObject(rr, p, p);
            cJSON_AddStringToObject(kd, p, "dir");
            continue;
        }
        /* Read roots come from the helper's own snapshots, never from the
         * daemon. */
        char root[PATH_MAX];
        if (snap_read_path(paths_snap(pb), c.pset, res[i].real, root, sizeof(root)) != OK) {
            snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
            err_clear();
            fail = HELPER_E_FAILED;
            break;
        }
        cJSON_AddStringToObject(rr, p, root);
        cJSON_AddStringToObject(kd, p, res[i].dom.kind == SNAP_ZFS ? "zfs" : "btrfs");
        if (res[i].nested_total) {
            cJSON *o = cJSON_CreateObject();
            if (o) {
                cJSON_AddNumberToObject(o, "total", (double)res[i].nested_total);
                cJSON_AddStringToObject(o, "names", res[i].nested);
                cJSON_AddItemToObject(ns, p, o);
            }
        }
    }
    for (size_t i = 0; fail == HELPER_OK && c.pset && i < c.pset->n; i++)
        cJSON_AddStringToObject(sn, c.pset->doms[i].kind == SNAP_ZFS ? c.pset->doms[i].dataset : c.pset->doms[i].root,
                                c.pset->names[i]);

    if (fail == HELPER_OK && slot && helper_random_hex(c.token, 16) != OK) {
        snprintf(fmsg, sizeof(fmsg), "no randomness for a token");
        fail = HELPER_E_FAILED;
    }
    int orphan = 0;
    if (slot) {
        c.deadline = now + (time_t)s->pol->capture_max_s;
        c.used = 1;
        pthread_mutex_lock(&s->mu);
        /* Its session may have ended while it began: nobody will end it. */
        orphan = fail == HELPER_OK && c.anchored && !session_anchored(s, c.session);
        if (fail != HELPER_OK || orphan) slot->used = 0;
        else *slot = c;
        pthread_mutex_unlock(&s->mu);
        if (orphan) {
            log_warn("helper: the daemon's session %.8s... ended during a paths capture's begin; ending it", c.session);
            snprintf(fmsg, sizeof(fmsg), "the daemon's session ended during the begin");
            fail = HELPER_E_FAILED;
        }
        if (fail != HELPER_OK) capture_finish(s, &c, SP_END_ABORTED);
    }
    snap_probe_free(probe);
    free(res);
    free(kind);
    free(set);
    free(pj);
    if (fail != HELPER_OK) {
        cJSON_Delete(out);
        return resp_error(r->id, fail, "%s", fmsg);
    }
    if (slot) {
        cJSON_AddStringToObject(out, "capture", c.token);
        cJSON_AddNumberToObject(out, "expires", (double)c.deadline);
        cJSON_AddNumberToObject(out, "snapshot_ms", c.pset ? (double)c.pset->snapshot_ms : 0.0);
    }
    return resp_ok(r->id, out);
}

/* lease.release: let go what a capture holds beyond its snapshots (a
 * pause kept for the fallback's second pass), keeping the capture; a
 * capture with no member sessions (begin_paths) is ended, as before. */
static char *op_capture_end(helper_server_t *s, const helper_req_t *r);

static char *op_lease_release(helper_server_t *s, const helper_req_t *r)
{
    member_t mem[HELPER_UNITS_MAX];
    size_t n = 0;
    pthread_mutex_lock(&s->mu);
    capture_t *c = capture_find(s, r->token);
    if (c) { n = c->n_members; memcpy(mem, c->members, n * sizeof(*mem)); }
    pthread_mutex_unlock(&s->mu);
    if (!c) return resp_error(r->id, HELPER_E_INVALID, "no such capture");
    if (n == 0) return op_capture_end(s, r);
    int cap_hit = 0;
    unsigned released = 0;
    for (size_t i = 0; i < n; i++) {
        if (!mem[i].sess || !mem[i].b->ops->release) continue;
        int hit = 0;
        if (mem[i].b->ops->release(mem[i].sess, &hit) != OK) {
            log_warn("helper: releasing %s: %s", mem[i].unit, err_msg());
            err_clear();
            continue;
        }
        released++;
        cap_hit |= hit;
    }
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddNumberToObject(res, "released", released);
        cJSON_AddBoolToObject(res, "cap_hit", cap_hit);
    }
    return resp_ok(r->id, res);
}

static char *op_capture_end(helper_server_t *s, const helper_req_t *r)
{
    capture_t c;
    if (!capture_take(s, r->token, &c))
        return resp_error(r->id, HELPER_E_INVALID, r->op == HOP_VM_BACKUP_END ? "no such session" : "no such capture");
    /* vm.backup_end says how the snapshot went, which settles the
     * checkpoints (vm-backup.md 5.8); a capture always ends as committed. */
    int aborted = r->op == HOP_VM_BACKUP_END && strcmp(r->outcome, "aborted") == 0;
    capture_finish(s, &c, aborted ? SP_END_ABORTED : SP_END_COMMITTED);
    cJSON *res = cJSON_CreateObject();
    if (res) cJSON_AddBoolToObject(res, "ended", 1);
    return resp_ok(r->id, res);
}

/* ------------------------------------------------------------------ */
/* Hooks                                                               */
/* ------------------------------------------------------------------ */

/*
 * A database template as a hook (db-detection.md 7.2): the catalog's argv
 * for the phase, run in the member whose mount holds the finding's path,
 * as the backend's own inspect says. The policy decides which templates
 * hook.run may build; a probe runs whatever it says (it reads no data).
 * h borrows av's strings, so av outlives every use of h.
 */
static helper_err_t template_hook(helper_server_t *s, const helper_req_t *r, const helper_backend_t *b,
                                  const char *uid, dbt_phase_t ph, int any_policy, policy_hook_t *h,
                                  dbt_argv_t *av, char *why, size_t wsz)
{
    memset(h, 0, sizeof(*h));
    memset(av, 0, sizeof(*av));
    const db_template_t *t = db_template_find(r->tmpl);
    if (!t) {
        snprintf(why, wsz, "no template %s in this helper's catalog", r->tmpl);
        return HELPER_E_INVALID;
    }
    if (!b->hook_start && strcmp(b->name, "fake") != 0) {
        snprintf(why, wsz, "templates run inside a container, and %s units have none", b->name);
        return HELPER_E_UNSUPPORTED;
    }
    if (ph != DBT_PHASE_PROBE && !any_policy && !helper_policy_template_allowed(s->pol, t->name)) {
        snprintf(why, wsz, "template %s is not allowed by this host's policy; add it to [db] allow_templates",
                 t->name);
        return HELPER_E_DENIED;
    }
    if (ph == DBT_PHASE_POST && !t->post) {
        snprintf(why, wsz, "template %s has no post run", t->name);
        return HELPER_E_INVALID;
    }
    cJSON *params = r->params ? cJSON_Parse(r->params) : NULL;
    if (r->params && !params) {
        snprintf(why, wsz, "params are not JSON");
        return HELPER_E_INVALID;
    }
    helper_err_t e = HELPER_OK;
    if (db_template_check(t, params) != OK) {
        snprintf(why, wsz, "%.400s", err_msg());
        err_clear();
        e = HELPER_E_INVALID;
    }
    /* The path that picks the container: the database file a template
     * reads (its DBT_P_PATH parameter), else the finding's path. */
    const char *pick = NULL;
    for (size_t i = 0; i < t->n_params; i++)
        if (t->params[i].type == DBT_P_PATH) pick = db_template_param_str(params, t->params[i].name);
    int mapped = pick != NULL;
    if (!pick && r->dbpath[0]) pick = r->dbpath;
    char cpath[HELPER_PATH_MAX + 1] = "";
    if (e == HELPER_OK && b->db_target) {
        status_t st = b->db_target(b->ctx, uid, pick, h->container, sizeof(h->container), cpath, sizeof(cpath));
        if (st != OK) {
            snprintf(why, wsz, "template %s: %.400s", t->name, err_msg());
            err_clear();
            e = st == ERR_NOT_FOUND || st == ERR_INVALID ? HELPER_E_INVALID : HELPER_E_FAILED;
        }
    }
    if (e == HELPER_OK && db_template_argv(t, ph, params, mapped && cpath[0] ? cpath : NULL, av) != OK) {
        snprintf(why, wsz, "%.400s", err_msg());
        err_clear();
        e = HELPER_E_INVALID;
    }
    cJSON_Delete(params);
    if (e != HELPER_OK) return e;
    snprintf(h->name, sizeof(h->name), "%s", t->name);
    snprintf(h->unit, sizeof(h->unit), "%s", r->unit);
    snprintf(h->user, sizeof(h->user), "%s", t->user);
    h->dump = t->kind == DBT_DUMP && ph == DBT_PHASE_RUN;
    h->pre = ph != DBT_PHASE_POST;
    h->timeout_s = ph == DBT_PHASE_PROBE ? DBT_PROBE_TIMEOUT_S : t->timeout_s;
    _Static_assert(DBT_ARGV_MAX <= POLICY_ARGV_MAX, "a template's argv fits a hook's");
    for (int i = 0; i < av->argc && i < DBT_ARGV_MAX; i++) h->argv[i] = av->argv[i];
    h->argc = av->argc < DBT_ARGV_MAX ? av->argc : DBT_ARGV_MAX;
    return HELPER_OK;
}

/*
 * Start h: inside the unit through the backend, or (the fake, which has no
 * unit to enter) as a local process with no shell of the helper's own. A
 * template's argv names its tools by name, so a local one is looked up on
 * PATH; a policy hook's argv.0 is the path the operator wrote.
 */
static status_t hook_spawn(const helper_backend_t *b, const char *uid, const policy_hook_t *h, int by_path,
                           int out_fd, int err_fd, pid_t *pid, helper_run_t **hr)
{
    *pid = -1;
    *hr = NULL;
    if (b->hook_start) return b->hook_start(b->ctx, uid, h, out_fd, err_fd, hr);
    /* A process group of its own, so a stop reaches whatever it started
     * (B248), as lxc-attach's does. */
    posix_spawn_file_actions_t fa;
    posix_spawnattr_t at;
    posix_spawn_file_actions_init(&fa);
    posix_spawnattr_init(&at);
    posix_spawnattr_setflags(&at, POSIX_SPAWN_SETPGROUP);
    posix_spawnattr_setpgroup(&at, 0);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    if (out_fd >= 0) posix_spawn_file_actions_adddup2(&fa, out_fd, 1);
    else posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_adddup2(&fa, err_fd, 2);
    int rc = by_path ? posix_spawnp(pid, h->argv[0], &fa, &at, h->argv, environ)
                     : posix_spawn(pid, h->argv[0], &fa, &at, h->argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    posix_spawnattr_destroy(&at);
    if (rc != 0) return set_error(ERR_IO, "%s", strerror(rc));
    return OK;
}

static char *op_hook_run(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    char why[512];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    policy_hook_t th;
    dbt_argv_t av;
    memset(&av, 0, sizeof(av));
    const policy_hook_t *h = NULL;
    const char *what = r->tmpl[0] ? r->tmpl : r->hook;
    if (r->tmpl[0]) {
        if ((e = template_hook(s, r, b, uid, r->post ? DBT_PHASE_POST : DBT_PHASE_RUN, 0, &th, &av, why,
                               sizeof(why))) != HELPER_OK)
            return resp_error(r->id, e, "%s", why);
        h = &th;
    } else {
        h = helper_policy_hook(s->pol, r->hook);
        if (!h || strcmp(h->unit, r->unit) != 0)
            return resp_error(r->id, HELPER_E_DENIED, "hook %s is not in the policy for %s", r->hook, r->unit);
    }

    int out[2] = { -1, -1 }, err[2] = { -1, -1 };
    if (pipe2(err, O_CLOEXEC) != 0 || (h->dump && pipe2(out, O_CLOEXEC) != 0)) {
        if (err[0] >= 0) { close(err[0]); close(err[1]); }
        dbt_argv_free(&av);
        return resp_error(r->id, HELPER_E_FAILED, "pipe: %s", strerror(errno));
    }
    pid_t pid = -1;
    helper_run_t *hr = NULL;
    /* Inside the unit (docker exec, lxc-attach), argv as given and no
     * shell; or a local process for the fake. */
    status_t hst = hook_spawn(b, uid, h, r->tmpl[0] != '\0', h->dump ? out[1] : -1, err[1], &pid, &hr);
    close(err[1]);
    if (h->dump) close(out[1]);
    unsigned timeout_s = h->timeout_s;
    dbt_argv_free(&av);
    if (hst != OK) {
        close(err[0]);
        if (out[0] >= 0) close(out[0]);
        return resp_error(r->id, HELPER_E_FAILED, "hook %s: %s", what, err_msg());
    }
    int dump = out[0] >= 0;
    (void)fcntl(err[0], F_SETFL, O_NONBLOCK);
    pthread_mutex_lock(&s->mu);
    run_t *slot = NULL;
    for (size_t i = 0; i < RUNS_MAX && !slot; i++) if (!s->runs[i].used) slot = &s->runs[i];
    if (slot) {
        memset(slot, 0, sizeof(*slot));
        slot->pid = pid;
        slot->b = hr ? b : NULL;
        slot->hr = hr;
        slot->err_fd = err[0];
        slot->deadline = time(NULL) + (time_t)timeout_s;
        if (helper_random_hex(slot->token, 16) == OK) slot->used = 1;
        else slot = NULL;
    }
    char token[HELPER_TOKEN_HEX + 1] = "";
    if (slot) memcpy(token, slot->token, sizeof(token));
    pthread_mutex_unlock(&s->mu);
    if (!slot) {
        if (hr) { b->hook_kill(hr); b->hook_free(hr); }
        else { kill(-pid, SIGKILL); waitpid(pid, NULL, 0); }
        close(err[0]);
        if (dump) close(out[0]);
        return resp_error(r->id, HELPER_E_BUSY, "too many hook runs");
    }
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "run", token);
        cJSON_AddBoolToObject(res, "dump", dump);
    }
    if (dump) *pass_fd = out[0];
    return resp_ok(r->id, res);
}

/*
 * db.probe (db-detection.md 8.1): a template's dry run, stdout and stderr
 * together, capped at DBT_PROBE_OUTPUT_MAX and bounded by
 * DBT_PROBE_TIMEOUT_S, on this connection's own thread. The output comes
 * back in a sealed memfd, since 64 KiB of it escaped into JSON would not
 * fit a packet. Also returned: the argv the template's run (and post run)
 * would use, and where, for a preview to show exactly what Enable runs.
 */
static char *op_db_probe(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    char why[512];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    policy_hook_t h;
    dbt_argv_t av;
    if ((e = template_hook(s, r, b, uid, DBT_PHASE_PROBE, 0, &h, &av, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    const db_template_t *t = db_template_find(r->tmpl);
    cJSON *res = cJSON_CreateObject();
    if (!res) { dbt_argv_free(&av); return resp_error(r->id, HELPER_E_FAILED, "out of memory"); }
    cJSON_AddStringToObject(res, "template", t->name);
    cJSON_AddStringToObject(res, "container", h.container);
    cJSON_AddStringToObject(res, "user", h.user);
    cJSON_AddBoolToObject(res, "allowed", helper_policy_template_allowed(s->pol, t->name));
    cJSON_AddItemToObject(res, "probe_argv", dbt_argv_json(&av));
    /* What Enable would run, built as hook.run builds it; shown even when
     * the policy would refuse it, beside "allowed". */
    for (int ph = DBT_PHASE_RUN; ph <= DBT_PHASE_POST; ph++) {
        if (ph == DBT_PHASE_POST && !t->post) break;
        policy_hook_t xh;
        dbt_argv_t xa;
        char xwhy[256];
        if (template_hook(s, r, b, uid, (dbt_phase_t)ph, 1, &xh, &xa, xwhy, sizeof(xwhy)) == HELPER_OK)
            cJSON_AddItemToObject(res, ph == DBT_PHASE_POST ? "post_argv" : "argv", dbt_argv_json(&xa));
        dbt_argv_free(&xa);
    }

    int p[2];
    if (pipe2(p, O_CLOEXEC) != 0) {
        dbt_argv_free(&av);
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "pipe: %s", strerror(errno));
    }
    pid_t pid = -1;
    helper_run_t *hr = NULL;
    status_t hst = hook_spawn(b, uid, &h, 1, p[1], p[1], &pid, &hr);
    close(p[1]);
    dbt_argv_free(&av);
    if (hst != OK) {
        close(p[0]);
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "probe %s: %s", r->tmpl, err_msg());
    }
    char *out = malloc(DBT_PROBE_OUTPUT_MAX);
    size_t got = 0;
    uint64_t total = 0;
    time_t deadline = time(NULL) + (time_t)DBT_PROBE_TIMEOUT_S;
    int timed_out = 0, eof = 0, status = 0, done = 0;
    (void)fcntl(p[0], F_SETFL, O_NONBLOCK);
    while (!done) {
        if (!eof) {
            struct pollfd pf = { p[0], POLLIN, 0 };
            (void)poll(&pf, 1, 100);
            char buf[4096];
            for (;;) {
                ssize_t n = read(p[0], buf, sizeof(buf));
                if (n > 0) {
                    size_t keep = out && got < DBT_PROBE_OUTPUT_MAX ? DBT_PROBE_OUTPUT_MAX - got : 0;
                    if (keep > (size_t)n) keep = (size_t)n;
                    if (keep) memcpy(out + got, buf, keep);
                    got += keep;
                    total += (uint64_t)n;
                    continue;
                }
                if (n == 0) eof = 1;
                break;
            }
        } else {
            struct timespec ts = { 0, 20 * 1000000L };
            nanosleep(&ts, NULL);
        }
        if (hr) {
            if (b->hook_poll(hr, &status)) done = eof || time(NULL) >= deadline;
        } else {
            int st = 0;
            if (pid > 0 && waitpid(pid, &st, WNOHANG) == pid) {
                status = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
                pid = -1;
            }
            /* A child that left something holding the pipe has still ended. */
            if (pid < 0) done = eof || time(NULL) >= deadline;
        }
        if (!done && time(NULL) >= deadline) {
            timed_out = 1;
            if (hr) b->hook_kill(hr);
            else if (pid > 0) { kill(-pid, SIGKILL); waitpid(pid, NULL, 0); pid = -1; }
            status = 128 + SIGKILL;
            done = 1;
        }
    }
    close(p[0]);
    if (hr) b->hook_free(hr);
    cJSON_AddNumberToObject(res, "exit", status);
    cJSON_AddBoolToObject(res, "timed_out", timed_out);
    cJSON_AddNumberToObject(res, "output_bytes", (double)got);
    cJSON_AddBoolToObject(res, "truncated", total > got);
    int lfd = -1;
    uint64_t loff = 0;
    if (out && got && (helper_blob_fd_append(&lfd, &loff, out, got) != 0 ||
                       fcntl(lfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL) != 0)) {
        if (lfd >= 0) close(lfd);
        lfd = -1;
        cJSON_ReplaceItemInObject(res, "output_bytes", cJSON_CreateNumber(0));
    }
    free(out);
    if (lfd >= 0) *pass_fd = lfd;
    return resp_ok(r->id, res);
}

/* Drain what the hook wrote to stderr so far, capped. Caller holds mu. */
static void run_drain(run_t *x)
{
    char buf[4096];
    for (;;) {
        ssize_t n = read(x->err_fd, buf, sizeof(buf));
        if (n <= 0) break;
        if (hooklog_append(&x->log, buf, (size_t)n) != 0) break;
    }
}

/*
 * A run has ended: the rest of its stderr, then how it ended, into its log
 * (B141). A hook inside a unit writes its stderr through the backend (the
 * docker exec stream is demultiplexed on the backend's own thread), which
 * may still be writing when the process is reported gone, so the pipe is
 * read to its end, not to what happens to be there: up to 5 s for the last
 * writer to close it. The closing line means a log is never empty, and a
 * failed dump, which is not stored, always says why.
 */
static void run_finish(run_t *x)
{
    char buf[4096];
    for (int waited_ms = 0; waited_ms < 5000;) {
        ssize_t n = read(x->err_fd, buf, sizeof(buf));
        if (n > 0) { if (hooklog_append(&x->log, buf, (size_t)n) != 0) break; continue; }
        if (n == 0) break;                         /* every writer is gone */
        if (errno != EAGAIN && errno != EINTR) break;
        struct pollfd pf = { x->err_fd, POLLIN, 0 };
        (void)poll(&pf, 1, 100);
        waited_ms += 100;
    }
    char line[96];
    if (x->cancelled)
        snprintf(line, sizeof(line), "\n[c-backup: stopped, the job was cancelled; exit status %d]\n", x->status);
    else if (x->timed_out)
        snprintf(line, sizeof(line), "\n[c-backup: killed at its timeout, exit status %d]\n", x->status);
    else
        snprintf(line, sizeof(line), "\n[c-backup: exit status %d]\n", x->status);
    (void)hooklog_append(&x->log, line, strlen(line));
}

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* One signal to a run's whole process group. */
static void run_signal(run_t *x, int sig)
{
    if (x->hr) {
        if (x->b->hook_signal) x->b->hook_signal(x->hr, sig);
        else if (sig == SIGKILL) x->b->hook_kill(x->hr);
        return;
    }
    /* Its own group (hook_spawn): never the bare pid, which may be reaped
     * and reused by now. */
    if (x->pid > 0) kill(-x->pid, sig);
}

/*
 * Reap a run if it ended. One past its deadline, or cancelled, is stopped:
 * SIGTERM to its process group, SIGKILL HOOK_KILL_GRACE_S later, each
 * sent from here as the polls come rather than waited out, so nothing
 * holds mu through a grace period (B248). Caller holds mu.
 */
static void run_poll(run_t *x, time_t now)
{
    if (x->done) return;
    run_drain(x);
    int ended = 0, code = 0;
    if (x->hr) {
        ended = x->b->hook_poll(x->hr, &code);
    } else {
        int st = 0;
        if (waitpid(x->pid, &st, WNOHANG) == x->pid) {
            ended = 1;
            code = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
        }
    }
    if (ended) {
        /* A stopped run's leader can die of the SIGTERM and leave a member
         * that ignores it; the run is over, so the rest of its group goes
         * now rather than outliving it. */
        if (x->term_ms && !x->killed) {
            run_signal(x, SIGKILL);
            x->killed = 1;
        }
        x->done = 1;
        x->status = code;
        run_finish(x);
        return;
    }
    if (!x->term_ms && (x->cancelled || now >= x->deadline)) {
        if (!x->cancelled) x->timed_out = 1;
        run_signal(x, SIGTERM);
        int64_t t = mono_ms();
        x->term_ms = t > 0 ? t : 1;
    } else if (x->term_ms && !x->killed && mono_ms() - x->term_ms >= (int64_t)HOOK_KILL_GRACE_S * 1000) {
        run_signal(x, SIGKILL);
        x->killed = 1;
    }
}

static char *op_hook_wait(helper_server_t *s, const helper_req_t *r, int *pass_fd)
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
        /* kill: the daemon's job was cancelled; stop the run and answer
         * as for any other end (B248). */
        if (r->kill) x->cancelled = 1;
        run_poll(x, time(NULL));
        /* A plain wait the daemon left when it sent the kill must not take
         * the run's answer from the wait that did. */
        if (x->done && x->cancelled && !r->kill) {
            pthread_mutex_unlock(&s->mu);
            return resp_error(r->id, HELPER_E_INVALID, "the run was stopped by a cancel; its answer is the kill's");
        }
        if (x->done) {
            /* The whole log, up to 1 MiB, in a sealed memfd; a tail of it
             * inline for messages. */
            size_t len = 0;
            char *txt = hooklog_render(&x->log, &len);
            int lfd = -1;
            uint64_t loff = 0;
            if (txt && (helper_blob_fd_append(&lfd, &loff, txt, len) != 0 ||
                        fcntl(lfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL) != 0)) {
                if (lfd >= 0) close(lfd);
                lfd = -1;
            }
            cJSON *res = cJSON_CreateObject();
            if (res) {
                cJSON_AddNumberToObject(res, "exit", x->status);
                cJSON_AddBoolToObject(res, "timed_out", x->timed_out);
                cJSON_AddBoolToObject(res, "cancelled", x->cancelled);
                cJSON_AddStringToObject(res, "log", txt ? txt + (len > 1024 ? len - 1024 : 0) : "");
                if (lfd >= 0) cJSON_AddNumberToObject(res, "log_bytes", (double)len);
            }
            free(txt);
            if (lfd >= 0) *pass_fd = lfd;
            close(x->err_fd);
            hooklog_free(&x->log);
            if (x->hr) x->b->hook_free(x->hr);
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

/*
 * Whether mount is the unit's in its own describe: 1 a tree, 2 a disk
 * image (a BLOCK item's source), 3 a configuration file (a BLOB's source,
 * staged for vm.install_state, never swapped); 0 not the unit's. The path
 * the daemon names is never trusted otherwise.
 *
 * real gets where the bytes are now (B178). A tree is recorded where the
 * container binds it (/mnt/user/appdata/x, /var/lib/docker/volumes/v/_data)
 * and its describe's source is the same path resolved through shfs,
 * symlinks and host bind mounts (mounts.c), freshly, so that is what the
 * domain, the staging and the swap must use. A disk or a file is its own
 * real path.
 */
static int unit_has_mount(const helper_backend_t *b, const char *uid, const char *mount,
                          char *real, size_t rsz)
{
    sp_unit_desc_t *d = NULL;
    snprintf(real, rsz, "%s", mount);
    if (b->ops->describe(b->ctx, uid, &d) != OK) { err_clear(); return 0; }
    int ok = 0;
    for (size_t i = 0; i < d->n_items && !ok; i++) {
        const sp_item_desc_t *it = &d->items[i];
        if (it->kind == SP_ITEM_TREE && it->tree_path && strcmp(it->tree_path, mount) == 0) {
            ok = 1;
            if (it->source && it->source[0] == '/') snprintf(real, rsz, "%s", it->source);
        }
        else if (it->kind == SP_ITEM_BLOCK && it->source && strcmp(it->source, mount) == 0) ok = 2;
        else if (it->kind == SP_ITEM_BLOB && it->source && strcmp(it->source, mount) == 0) ok = 3;
    }
    b->ops->desc_free(d);
    return ok;
}

/* Is a under (or equal to) b, on a component boundary? */
static int path_under(const char *a, const char *b)
{
    size_t n = strlen(b);
    while (n > 1 && b[n - 1] == '/') n--;
    return strncmp(a, b, n) == 0 && (a[n] == '\0' || a[n] == '/');
}

/* How many components p has below root, or -1 when it is not under it. */
static int depth_below(const char *p, const char *root)
{
    if (!path_under(p, root)) return -1;
    const char *rest = p + strlen(root);
    int d = 0;
    for (const char *c = rest; *c; c++) if (*c == '/' && c[1] && c[1] != '/') d++;
    return d;
}

/* The value of key= on a share .cfg line, quotes dropped; NULL if another key. */
static const char *cfg_value(const char *line, const char *key)
{
    size_t n = strlen(key);
    if (strncmp(line, key, n) != 0 || line[n] != '=') return NULL;
    const char *v = line + n + 1;
    return *v == '"' ? v + 1 : v;
}

static int cfg_is(const char *v, const char *want)
{
    size_t n = strlen(want);
    return strncmp(v, want, n) == 0 && (v[n] == '"' || v[n] == '\n' || v[n] == '\r' || v[n] == '\0');
}

/*
 * Where restore.stage stages for a mount in domain dom (Manual review item
 * 4, B171). The first restore_staging_dirs entry inside the domain; else,
 * for a pool-top domain (mnt_root/<pool>), <pool>/system/.cbackup-restore,
 * beside the snapshot directory; else <domain>/.cbackup-restore. Then,
 * wherever it came from, refused at the top of a pool, and in a share
 * whose mover sends files to the array (<boot_config>/shares/<share>.cfg
 * shareUseCache="yes"), which could carry staged files off the pool
 * mid-restore. Whether the share is exported does not matter: anyone who
 * can write the share can already edit the data in it. -1 with why.
 */
static int staging_base(const helper_server_t *s, const char *dom, char *out, size_t sz,
                        char *why, size_t wsz)
{
    const helper_policy_t *p = s->pol;
    out[0] = '\0';
    for (size_t i = 0; i < p->n_staging && !out[0]; i++)
        if (path_under(p->restore_staging_dirs[i], dom) && strcmp(p->restore_staging_dirs[i], dom) != 0)
            snprintf(out, sz, "%s", p->restore_staging_dirs[i]);
    if (!out[0]) {
        int pool_top = depth_below(dom, p->mnt_root) == 1;
        if (snprintf(out, sz, pool_top ? "%s/system/.cbackup-restore" : "%s/.cbackup-restore", dom) >= (int)sz) {
            snprintf(why, wsz, "path too long");
            return -1;
        }
    }
    int d = depth_below(out, p->mnt_root);
    if (d < 0) return 0;                       /* not an Unraid pool path */
    if (d < 3) {
        snprintf(why, wsz, "restore staging %.300s would sit at the top of a pool; set [snapshot] "
                 "restore_staging_dirs to a directory inside a share", out);
        return -1;
    }
    /* mnt_root/<pool>/<share>/...: the share is the second component. */
    const char *rest = out + strlen(p->mnt_root) + 1;
    const char *sh = strchr(rest, '/');
    if (!sh) return 0;
    sh++;
    int k = (int)strcspn(sh, "/");
    char cfg[PATH_MAX + 512];
    if (snprintf(cfg, sizeof(cfg), "%s%s/shares/%.*s.cfg", p->host_root, p->boot_config, k, sh) >= (int)sizeof(cfg))
        return 0;
    FILE *f = fopen(cfg, "re");
    if (!f) return 0;                          /* not an Unraid share */
    char line[512];
    int mover = 0;
    while (fgets(line, sizeof(line), f)) {
        const char *v = cfg_value(line, "shareUseCache");
        if (v) mover = cfg_is(v, "yes");
    }
    fclose(f);
    if (mover) {
        snprintf(why, wsz, "restore staging %.300s is in share %.*s, which the mover moves to the array "
                 "(shareUseCache=yes); a staged restore could be split across devices. Keep the share "
                 "on the pool, or set [snapshot] restore_staging_dirs", out, k, sh);
        return -1;
    }
    return 0;
}

/*
 * Open base below dom one component at a time, never following a
 * symlink, creating what is missing (0755 on the way, 0700 for the base
 * itself). The fd is what the swap renames through later, so nothing done
 * to the path in between can redirect it.
 */
static int open_base(const char *dom, const char *base, int *out, char *why, size_t wsz)
{
    int fd = open(dom, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        snprintf(why, wsz, "cannot open %.300s: %s", dom, strerror(errno));
        return -1;
    }
    char rest[PATH_MAX];
    snprintf(rest, sizeof(rest), "%s", base + strlen(dom));
    char *save = NULL;
    char *c = strtok_r(rest, "/", &save);
    while (c) {
        char *next = strtok_r(NULL, "/", &save);
        if (mkdirat(fd, c, next ? 0755 : 0700) != 0 && errno != EEXIST) {
            snprintf(why, wsz, "cannot create %.300s (at %.100s): %s", base, c, strerror(errno));
            close(fd);
            return -1;
        }
        int nfd = openat(fd, c, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (nfd < 0) {
            int e = errno;
            close(fd);
            if (e == ELOOP || e == ENOTDIR)
                snprintf(why, wsz, "restore staging %.300s: %.100s is a symlink or not a directory; not followed", base, c);
            else
                snprintf(why, wsz, "cannot open %.300s (at %.100s): %s", base, c, strerror(e));
            return -1;
        }
        close(fd);
        fd = nfd;
        c = next;
    }
    *out = fd;
    return 0;
}

/*
 * The daemon creates and writes inside the staging (the restore makes its
 * root directory's entries and fixes its metadata) as daemon_uid, with no
 * write capability on directories it does not own. A base root owns, made
 * by the helper or by hand, is given to it; one somebody else owns is used
 * as it is (real-engines job 20955). -1 with why.
 */
static int give_base(const helper_server_t *s, int bfd, const char *base, char *why, size_t wsz)
{
    struct stat b;
    if (fstat(bfd, &b) != 0) { snprintf(why, wsz, "cannot stat %.300s: %s", base, strerror(errno)); return -1; }
    if (geteuid() == 0 && b.st_uid == 0 && b.st_uid != (uid_t)s->pol->daemon_uid &&
        fchown(bfd, (uid_t)s->pol->daemon_uid, (gid_t)-1) != 0) {
        snprintf(why, wsz, "cannot give %.300s to the daemon: %s", base, strerror(errno));
        return -1;
    }
    return 0;
}

static void stage_close(staging_t *x)
{
    if (x->base_fd >= 0) close(x->base_fd);
    if (x->job_fd >= 0) close(x->job_fd);
    x->base_fd = x->job_fd = -1;
}

/* Caller holds mu. */
static void stage_drop(helper_server_t *s, const char *token)
{
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, token) == 0) {
            stage_close(&s->stages[i]);
            s->stages[i].used = 0;
        }
}

#ifndef STATX_MNT_ID
#define STATX_MNT_ID 0x00001000U
#endif

/*
 * Whether two directories are on one mount: the swap is a rename, which is
 * EXDEV across a mount boundary even within one filesystem (a bind mount,
 * as dockerd makes of a data root that is not already a mount point), and
 * st_dev does not show that. 1 when they are, 0 when not, and 1 too where
 * the kernel cannot say (no STATX_MNT_ID before 5.8): the swap still
 * refuses then, only later.
 */
static int same_mount(int afd, const char *apath, int bfd, const char *bpath)
{
    struct statx a, b;
    if (statx(afd, apath, AT_SYMLINK_NOFOLLOW | (apath[0] ? 0 : AT_EMPTY_PATH), STATX_MNT_ID, &a) != 0 ||
        statx(bfd, bpath, AT_SYMLINK_NOFOLLOW | (bpath[0] ? 0 : AT_EMPTY_PATH), STATX_MNT_ID, &b) != 0 ||
        !(a.stx_mask & STATX_MNT_ID) || !(b.stx_mask & STATX_MNT_ID))
        return 1;
    return a.stx_mnt_id == b.stx_mnt_id;
}

/* The directory holding real, for the same-filesystem checks. */
static void parent_of(const char *real, char *parent, size_t psz)
{
    if (snprintf(parent, psz, "%.4000s", real) >= (int)psz) { parent[0] = '\0'; return; }
    char *sl = strrchr(parent, '/');
    if (sl && sl != parent) *sl = '\0';
}

/*
 * restore.stage with check (B189): every refusal of a real stage, made
 * before the daemon stops the unit, and nothing created. A refusal used to
 * come only once the unit was stopped, and a restore refused for, say, no
 * snapshot domain left the unit down. The base is walked as open_base
 * walks it, without creating what is missing: what exists must be a
 * directory and not a symlink, and the deepest part that exists must be on
 * the mount's filesystem and mount.
 */
static char *stage_check(helper_server_t *s, const helper_req_t *r, const char *dom, const char *base,
                         const char *real)
{
    (void)s;
    char rest[PATH_MAX], at[PATH_MAX];
    snprintf(rest, sizeof(rest), "%s", base + strlen(dom));
    snprintf(at, sizeof(at), "%s", dom);
    char *save = NULL;
    for (char *c = strtok_r(rest, "/", &save); c; c = strtok_r(NULL, "/", &save)) {
        char next[PATH_MAX];
        if (snprintf(next, sizeof(next), "%s/%s", at, c) >= (int)sizeof(next))
            return resp_error(r->id, HELPER_E_INVALID, "path too long");
        struct stat sb;
        if (lstat(next, &sb) != 0) break;            /* made at the stage */
        if (!S_ISDIR(sb.st_mode))
            return resp_error(r->id, HELPER_E_DENIED, "restore staging %.300s: %.100s is a symlink or not a "
                              "directory; not followed", base, c);
        snprintf(at, sizeof(at), "%s", next);
    }
    char parent[PATH_MAX];
    parent_of(real, parent, sizeof(parent));
    struct stat as, ps;
    if (stat(at, &as) != 0 || stat(parent, &ps) != 0)
        return resp_error(r->id, HELPER_E_FAILED, "cannot stat %.300s or %.300s: %s", at, parent, strerror(errno));
    if (as.st_dev != ps.st_dev || !same_mount(AT_FDCWD, at, AT_FDCWD, parent))
        return resp_error(r->id, HELPER_E_DENIED, "restore staging %.300s is on another filesystem or mount "
                          "from %.300s, so the swap could not be a rename; set [snapshot] "
                          "restore_staging_dirs to a directory on its filesystem", base, real);
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddBoolToObject(res, "checked", 1);
        cJSON_AddStringToObject(res, "base", base);
        cJSON_AddStringToObject(res, "real", real);
    }
    return resp_ok(r->id, res);
}

static char *op_restore_stage(helper_server_t *s, const helper_req_t *r)
{
    if (!s->pol->allow_restore) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow restore");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    char real[HELPER_PATH_MAX + 1];
    int kind = unit_has_mount(b, uid, r->mount, real, sizeof(real));
    if (!kind)
        return resp_error(r->id, HELPER_E_DENIED, "%.300s is not a mount of %s", r->mount, r->unit);
    /* Where the backup read it must be where it is now: restoring the
     * bytes of one directory over another that the same recorded path has
     * come to mean (a share moved pools, Docker's directory moved) is not
     * a restore in place. */
    if (r->real[0] && strcmp(r->real, real) != 0) {
        log_error("helper: %s of %s resolves to %s now, but the backup read it at %s; not staged",
                  r->mount, r->unit, real, r->real);
        return resp_error(r->id, HELPER_E_DENIED, "%.300s resolves to %.300s now, but the backup read it "
                          "at %.300s; not restored in place. Restore it to a new location, or put the "
                          "data back where it was", r->mount, real, r->real);
    }
    /* The staging directory sits in the mount's own snapshot domain, so the
     * swap is a rename within one filesystem. */
    char droot[PATH_MAX] = "";
    const char *dom = NULL;
    if (b->mount_root) {
        if (b->mount_root(b->ctx, real, droot, sizeof(droot)) == OK) dom = droot;
        else err_clear();
    } else {
        dom = helper_policy_dir_domain(s->pol, real);
    }
    if (!dom || strcmp(dom, real) == 0) {
        if (strcmp(real, r->mount) != 0)
            return resp_error(r->id, HELPER_E_DENIED, "%.300s (on the host %.300s) is in no snapshot domain "
                              "of the policy", r->mount, real);
        return resp_error(r->id, HELPER_E_DENIED, "%.300s is in no snapshot domain of the policy", r->mount);
    }
    /* One directory per staging: a VM's disks share a job and a domain. */
    char base[PATH_MAX], path[PATH_MAX], swhy[600], uniq[HELPER_TOKEN_HEX + 1], jname[64];
    if (helper_random_hex(uniq, 4) != OK) return resp_error(r->id, HELPER_E_FAILED, "no randomness");
    if (staging_base(s, dom, base, sizeof(base), swhy, sizeof(swhy)) != 0)
        return resp_error(r->id, HELPER_E_DENIED, "%s", swhy);
    if (r->check) return stage_check(s, r, dom, base, real);
    snprintf(jname, sizeof(jname), "%lld-%s", (long long)r->job, uniq);
    if (snprintf(path, sizeof(path), "%s/%s", base, jname) >= (int)sizeof(path))
        return resp_error(r->id, HELPER_E_INVALID, "path too long");
    /* Under mu, with staging_tidy removing another job's directory. */
    int bfd = -1, jfd = -1;
    pthread_mutex_lock(&s->mu);
    int ob = open_base(dom, base, &bfd, swhy, sizeof(swhy));
    int gb = ob == 0 ? give_base(s, bfd, base, swhy, sizeof(swhy)) : -1;
    int mk = gb == 0 ? mkdirat(bfd, jname, 0700) : -1;
    int er = errno;
    pthread_mutex_unlock(&s->mu);
    if (ob != 0) return resp_error(r->id, HELPER_E_DENIED, "%s", swhy);
    if (gb != 0) { close(bfd); return resp_error(r->id, HELPER_E_FAILED, "%s", swhy); }
    if (mk != 0) {
        close(bfd);
        return resp_error(r->id, HELPER_E_FAILED, "cannot create %s: %s", path, strerror(er));
    }
    jfd = openat(bfd, jname, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    struct stat bs, js;
    const char *fail = NULL;
    if (jfd < 0) fail = "cannot open it";
    else if (geteuid() == 0 && fchown(jfd, (uid_t)s->pol->daemon_uid, (gid_t)-1) != 0) fail = "cannot give it to the daemon";
    else if (fstat(bfd, &bs) != 0 || fstat(jfd, &js) != 0) fail = "cannot stat it";
    /* The swap is a rename: the staging must be on the mount's filesystem
     * (a btrfs subvolume or another dataset would be EXDEV at the swap,
     * after the whole restore was written). */
    int other_fs = 0;
    if (!fail) {
        char parent[PATH_MAX];
        parent_of(real, parent, sizeof(parent));
        struct stat ps;
        other_fs = stat(parent, &ps) == 0 && (ps.st_dev != js.st_dev || !same_mount(jfd, "", AT_FDCWD, parent));
    }
    if (fail || other_fs) {
        if (jfd >= 0) close(jfd);
        (void)unlinkat(bfd, jname, AT_REMOVEDIR);
        close(bfd);
        if (other_fs)
            return resp_error(r->id, HELPER_E_DENIED, "restore staging %.300s is on another filesystem "
                              "or mount from %.300s, so the swap could not be a rename; set [snapshot] "
                              "restore_staging_dirs to a directory on its filesystem", base, real);
        return resp_error(r->id, HELPER_E_FAILED, "restore staging %s: %s", path, fail);
    }
    pthread_mutex_lock(&s->mu);
    staging_t *slot = NULL;
    for (size_t i = 0; i < STAGINGS_MAX && !slot; i++) if (!s->stages[i].used) slot = &s->stages[i];
    /* A swapped staging is only kept for an undo (its fds already closed):
     * the first to go when the table is full. */
    for (size_t i = 0; i < STAGINGS_MAX && !slot; i++) if (s->stages[i].swapped && !s->stages[i].busy) slot = &s->stages[i];
    char token[HELPER_TOKEN_HEX + 1] = "";
    if (slot && helper_random_hex(slot->token, 16) == OK) {
        slot->used = 1;
        slot->swapped = 0;
        slot->busy = 0;
        slot->had = 0;
        slot->old[0] = '\0';
        snprintf(slot->dom, sizeof(slot->dom), "%s", dom);
        slot->job = r->job;
        snprintf(slot->unit, sizeof(slot->unit), "%s", r->unit);
        snprintf(slot->mount, sizeof(slot->mount), "%s", r->mount);
        snprintf(slot->real, sizeof(slot->real), "%s", real);
        snprintf(slot->path, sizeof(slot->path), "%s", path);
        snprintf(slot->base, sizeof(slot->base), "%s", base);
        snprintf(slot->job_name, sizeof(slot->job_name), "%s", jname);
        slot->file = kind;
        slot->base_fd = bfd;
        slot->job_fd = jfd;
        slot->base_dev = bs.st_dev;
        slot->base_ino = bs.st_ino;
        slot->job_dev = js.st_dev;
        slot->job_ino = js.st_ino;
        memcpy(token, slot->token, sizeof(token));
    } else {
        slot = NULL;
    }
    pthread_mutex_unlock(&s->mu);
    if (!slot) {
        close(jfd);
        close(bfd);
        return resp_error(r->id, HELPER_E_BUSY, "too many restores staged");
    }
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "staging", token);
        cJSON_AddStringToObject(res, "path", path);
        cJSON_AddStringToObject(res, "real", real);
    }
    return resp_ok(r->id, res);
}

/* renameat2 with RENAME_NOREPLACE; where the filesystem has no flags, a
 * check then a plain rename (the window is the helper's own). */
static int rename_noreplace(int ofd, const char *on, int nfd, const char *nn)
{
    if (renameat2(ofd, on, nfd, nn, RENAME_NOREPLACE) == 0) return 0;
    if (errno != EINVAL && errno != ENOSYS) return -1;
    struct stat sb;
    if (fstatat(nfd, nn, &sb, AT_SYMLINK_NOFOLLOW) == 0) { errno = EEXIST; return -1; }
    return renameat(ofd, on, nfd, nn);
}

/*
 * Whether what was staged is still what the fds hold: the base at its path
 * is the pinned directory and the job directory inside it is the pinned
 * one. Planting a directory or a symlink at the path, renaming the base
 * away or swapping the job directory all fail here, so the helper never
 * renames anything it did not create. -1 with why.
 */
static int stage_intact(const staging_t *x, char *why, size_t wsz)
{
    struct stat ps, js, es;
    if (lstat(x->base, &ps) != 0 || !S_ISDIR(ps.st_mode) || ps.st_dev != x->base_dev || ps.st_ino != x->base_ino) {
        snprintf(why, wsz, "restore staging %.300s was replaced or moved after it was staged; not swapped", x->base);
        return -1;
    }
    if (fstatat(x->base_fd, x->job_name, &es, AT_SYMLINK_NOFOLLOW) != 0 || fstat(x->job_fd, &js) != 0 ||
        es.st_dev != x->job_dev || es.st_ino != x->job_ino || js.st_ino != x->job_ino || !S_ISDIR(es.st_mode)) {
        snprintf(why, wsz, "restore staging %.300s was replaced after it was staged; not swapped", x->path);
        return -1;
    }
    return 0;
}

static void swap_unclaim(helper_server_t *s, const char *token)
{
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, token) == 0) s->stages[i].busy = 0;
    pthread_mutex_unlock(&s->mu);
}

/*
 * After a swap, the staging directory of a disk image, which the image has
 * moved out of, goes. Nothing else does, and never the base (B187): the
 * base is the directory the daemon has bind-mounted (<domain>/.cbackup-restore
 * in the deployment), and removing it once it was empty (B168) detached
 * that bind, so every later in-place restore into the domain, and the
 * second mount of a unit with two in one domain, failed EROFS writing into
 * a directory that no longer existed. An empty base is left in place.
 * What is removed is chosen by identity, not by name: the job directory
 * pinned at stage, through the pinned base fd, and only while it is still
 * that directory, so no spelling of a path (an operator's
 * restore_staging_dirs entry, a symlink, a planted directory) can make the
 * tidy remove anything it did not create. rmdir semantics, so a directory
 * that is not empty stays. Caller holds the pinned fds open.
 */
static void staging_tidy(helper_server_t *s, const staging_t *st)
{
    if (st->file != 2 || st->base_fd < 0) return;
    struct stat bs, js;
    pthread_mutex_lock(&s->mu);
    if (fstat(st->base_fd, &bs) == 0 && bs.st_dev == st->base_dev && bs.st_ino == st->base_ino &&
        fstatat(st->base_fd, st->job_name, &js, AT_SYMLINK_NOFOLLOW) == 0 && S_ISDIR(js.st_mode) &&
        js.st_dev == st->job_dev && js.st_ino == st->job_ino &&
        !(js.st_dev == bs.st_dev && js.st_ino == bs.st_ino))
        (void)unlinkat(st->base_fd, st->job_name, AT_REMOVEDIR);
    pthread_mutex_unlock(&s->mu);
}

/*
 * restore.swap with undo (B167): what a swap did, reversed, for a restore
 * that failed after it. The unit must still be stopped. The restored copy
 * goes back to its staging directory, out of sight as it was, and the data
 * moved aside comes back to the mount. A staging whose slot a later stage
 * has taken has no undo; its data is still at <mount>.cbackup-old-<job>.
 * Like the swap, through directory fds opened without following symlinks
 * (B171): the staging base is opened again from the domain, and the
 * renames never replace anything.
 */
static char *swap_undo(helper_server_t *s, const helper_req_t *r, const staging_t *st)
{
    if (!st->swapped) return resp_error(r->id, HELPER_E_INVALID, "not swapped; nothing to undo");
    char why[700];
    helper_err_t code = HELPER_E_FAILED;
    int bfd = -1, jfd = -1, pfd = -1;
    char parent[HELPER_PATH_MAX + 1], name[HELPER_PATH_MAX + 1], oldname[HELPER_PATH_MAX + 1], back[HELPER_PATH_MAX + 8];
    snprintf(back, sizeof(back), st->file == 2 ? "%.4000s/image" : "%.4000s", st->path);
    if (st->had < 0) {
        snprintf(why, sizeof(why), "the previous copy stayed at %.300s at the swap; move it back by hand", st->old);
        goto out;
    }
    const char *suid = NULL;
    const helper_backend_t *sb_ = hj_backend_of(s, st->unit, &suid);
    int state = sb_ && sb_->unit_state ? sb_->unit_state(sb_->ctx, suid) : -1;
    if (state < 0) { snprintf(why, sizeof(why), "cannot tell whether %s is running", st->unit); goto out; }
    if (state == SP_STATE_RUNNING || state == SP_STATE_PAUSED) {
        snprintf(why, sizeof(why), "%s is running; stop it first", st->unit);
        code = HELPER_E_BUSY;
        goto out;
    }
    snprintf(parent, sizeof(parent), "%s", st->real);
    char *sl = strrchr(parent, '/');
    if (!sl || !sl[1]) { snprintf(why, sizeof(why), "%.300s has no parent", st->real); code = HELPER_E_INVALID; goto out; }
    snprintf(name, sizeof(name), "%s", sl + 1);
    if (sl == parent) sl[1] = '\0'; else *sl = '\0';
    const char *on = strrchr(st->old, '/');
    snprintf(oldname, sizeof(oldname), "%s", on ? on + 1 : st->old);
    /* The staging directory again (a disk image's was removed at the
     * swap), through its base opened again from the domain. */
    pthread_mutex_lock(&s->mu);
    int bad = open_base(st->dom, st->base, &bfd, why, sizeof(why)) != 0 ||
              give_base(s, bfd, st->base, why, sizeof(why)) != 0;
    if (!bad && st->file == 2) {
        if (mkdirat(bfd, st->job_name, 0700) != 0 && errno != EEXIST) {
            snprintf(why, sizeof(why), "cannot create %.300s: %s", st->path, strerror(errno));
            bad = 1;
        } else if ((jfd = openat(bfd, st->job_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)) < 0) {
            snprintf(why, sizeof(why), "cannot open %.300s: %s", st->path, strerror(errno));
            bad = 1;
        } else if (geteuid() == 0 && fchown(jfd, (uid_t)s->pol->daemon_uid, (gid_t)-1) != 0) {
            snprintf(why, sizeof(why), "cannot give %.300s to the daemon: %s", st->path, strerror(errno));
            bad = 1;
        }
    }
    pthread_mutex_unlock(&s->mu);
    if (bad) goto out;
    int dfd = st->file == 2 ? jfd : bfd;
    const char *dname = st->file == 2 ? "image" : st->job_name;
    pfd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (pfd < 0) { snprintf(why, sizeof(why), "cannot open %.300s: %s", parent, strerror(errno)); goto out; }
    if (rename_noreplace(pfd, name, dfd, dname) != 0) {
        code = errno == EEXIST ? HELPER_E_BUSY : HELPER_E_FAILED;
        snprintf(why, sizeof(why), "move the restored copy back to %.300s: %s", back, strerror(errno));
        goto out;
    }
    if (st->had && rename_noreplace(pfd, oldname, pfd, name) != 0) {
        int err = errno;
        (void)rename_noreplace(dfd, dname, pfd, name);
        snprintf(why, sizeof(why), "put %.300s back: %s", st->old, strerror(err));
        goto out;
    }
    if (fsync(pfd) != 0) log_warn("helper: undo of %s: %s could not be synced: %s", st->real, parent, strerror(errno));
    close(pfd);
    if (jfd >= 0) close(jfd);
    close(bfd);
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) s->stages[i].used = s->stages[i].swapped = s->stages[i].busy = 0;
    pthread_mutex_unlock(&s->mu);
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "mount", st->mount);
        cJSON_AddStringToObject(res, "real", st->real);
        cJSON_AddStringToObject(res, "restored_copy", back);
    }
    return resp_ok(r->id, res);
out:
    if (pfd >= 0) close(pfd);
    if (jfd >= 0) close(jfd);
    if (bfd >= 0) close(bfd);
    swap_unclaim(s, r->token);
    return resp_error(r->id, code, "%s", why);
}

static char *op_restore_swap(helper_server_t *s, const helper_req_t *r)
{
    if (!s->pol->allow_restore) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow restore");
    pthread_mutex_lock(&s->mu);
    staging_t st, *x = NULL;
    for (size_t i = 0; i < STAGINGS_MAX && !x; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) x = &s->stages[i];
    int busy = x && x->busy;
    if (x && !busy && (r->undo ? x->swapped : (!x->swapped && x->file != 3))) x->busy = 1;
    if (x) st = *x;
    pthread_mutex_unlock(&s->mu);
    if (!x) return resp_error(r->id, HELPER_E_INVALID, "no such staging");
    if (busy) return resp_error(r->id, HELPER_E_BUSY, "this staging is being swapped");
    if (r->undo) return swap_undo(s, r, &st);
    if (st.swapped) return resp_error(r->id, HELPER_E_INVALID, "already swapped");
    if (st.file == 3) return resp_error(r->id, HELPER_E_INVALID, "this staging is for vm.install_state");
    char why[700];
    helper_err_t code = HELPER_E_FAILED;
    const char *suid = NULL;
    const helper_backend_t *sb_ = hj_backend_of(s, st.unit, &suid);
    int state = sb_ && sb_->unit_state ? sb_->unit_state(sb_->ctx, suid) : -1;
    /* What moves in: the job directory from the base, or for a disk image
     * the image inside it. Both through the fds pinned at stage. */
    int src_fd = st.file == 2 ? st.job_fd : st.base_fd;
    const char *src_name = st.file == 2 ? "image" : st.job_name;
    char parent[HELPER_PATH_MAX + 1], name[HELPER_PATH_MAX + 1], old[HELPER_PATH_MAX + 40], prev[HELPER_PATH_MAX + 72] = "";
    int pfd = -1, had = 0, old_aside = 1;
    struct stat sb;
    snprintf(parent, sizeof(parent), "%s", st.real);
    char *sl = strrchr(parent, '/');
    if (!sl || !sl[1]) { snprintf(why, sizeof(why), "%.300s has no parent", st.real); code = HELPER_E_INVALID; goto refuse; }
    snprintf(name, sizeof(name), "%s", sl + 1);
    if (sl == parent) sl[1] = '\0'; else *sl = '\0';
    if (state < 0) { snprintf(why, sizeof(why), "cannot tell whether %s is running", st.unit); goto refuse; }
    if (state == SP_STATE_RUNNING || state == SP_STATE_PAUSED) {
        snprintf(why, sizeof(why), "%s is running; stop it first", st.unit);
        code = HELPER_E_BUSY;
        goto refuse;
    }
    if (stage_intact(&st, why, sizeof(why)) != 0) { code = HELPER_E_DENIED; goto refuse; }
    if (st.file == 2 && (fstatat(src_fd, src_name, &sb, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(sb.st_mode))) {
        snprintf(why, sizeof(why), "restore staging %.300s/image is not a regular file; not swapped", st.path);
        code = HELPER_E_DENIED;
        goto refuse;
    }
    pfd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (pfd < 0) { snprintf(why, sizeof(why), "cannot open %.300s: %s", parent, strerror(errno)); goto refuse; }
    if (fstat(pfd, &sb) != 0 || sb.st_dev != st.job_dev) {
        snprintf(why, sizeof(why), "%.300s is no longer on the staging's filesystem; not swapped", parent);
        code = HELPER_E_DENIED;
        goto refuse;
    }
    /* Moved aside, never deleted: the operator can reverse it. */
    if (snprintf(old, sizeof(old), "%s.cbackup-old-%lld", name, (long long)st.job) >= (int)sizeof(old) ||
        strlen(old) > NAME_MAX) {
        snprintf(why, sizeof(why), "path too long");
        code = HELPER_E_INVALID;
        goto refuse;
    }
    if (fstatat(pfd, old, &sb, AT_SYMLINK_NOFOLLOW) == 0) {
        snprintf(why, sizeof(why), "%.300s.cbackup-old-%lld already exists", st.real, (long long)st.job);
        code = HELPER_E_BUSY;
        goto refuse;
    }
    had = fstatat(pfd, name, &sb, AT_SYMLINK_NOFOLLOW) == 0;
    if (!had) {
        if (rename_noreplace(src_fd, src_name, pfd, name) != 0) {
            snprintf(why, sizeof(why), "swap in: %s", strerror(errno));
            goto refuse;
        }
    } else if (renameat2(src_fd, src_name, pfd, name, RENAME_EXCHANGE) == 0) {
        /* One atomic step: the mount is the restored tree, and the old one
         * sits where the staging was; then it goes beside the mount. */
        if (rename_noreplace(src_fd, src_name, pfd, old) == 0) {
            snprintf(prev, sizeof(prev), "%s.cbackup-old-%lld", st.real, (long long)st.job);
        } else {
            snprintf(prev, sizeof(prev), "%s/%s", st.file == 2 ? st.path : st.base, src_name);
            old_aside = 0;
            log_warn("helper: %s is swapped in; the previous copy stays at %s (%s)", st.real, prev, strerror(errno));
        }
    } else if (errno == EINVAL || errno == ENOSYS) {
        /* No RENAME_EXCHANGE here: aside, then in, and back on failure. */
        if (rename_noreplace(pfd, name, pfd, old) != 0) {
            snprintf(why, sizeof(why), "move aside: %s", strerror(errno));
            goto refuse;
        }
        if (rename_noreplace(src_fd, src_name, pfd, name) != 0) {
            int er = errno;
            if (rename_noreplace(pfd, old, pfd, name) != 0) {
                int e2 = errno;
                log_error("helper: swap of %s failed and the original could not be put back; "
                          "it is at %s.cbackup-old-%lld: %s", st.real, st.real, (long long)st.job, strerror(e2));
                snprintf(why, sizeof(why), "swap in: %s; the original could not be put back and is at "
                         "%.300s.cbackup-old-%lld (%s)", strerror(er), st.real, (long long)st.job, strerror(e2));
                goto refuse;
            }
            snprintf(why, sizeof(why), "swap in: %s", strerror(er));
            goto refuse;
        }
        snprintf(prev, sizeof(prev), "%s.cbackup-old-%lld", st.real, (long long)st.job);
    } else {
        snprintf(why, sizeof(why), "swap: %s", strerror(errno));
        goto refuse;
    }
    /* The emptied staging of a disk image, through the fds still pinned. */
    staging_tidy(s, &st);
    /* Kept for an undo (B167); the pinned fds are done with. had: 1 the
     * old data is beside the mount, -1 it stayed in the staging and an
     * undo is left to the operator. */
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) {
            stage_close(&s->stages[i]);
            s->stages[i].swapped = 1;
            s->stages[i].busy = 0;
            s->stages[i].had = had ? (old_aside ? 1 : -1) : 0;
            snprintf(s->stages[i].old, sizeof(s->stages[i].old), "%.*s", (int)sizeof(s->stages[i].old) - 1, prev);
        }
    pthread_mutex_unlock(&s->mu);
    /* Durable before it is reported: a power cut must not undo a swap the
     * daemon was told happened. The staging is tidied first either way. */
    {
        int ok = fsync(pfd) == 0;
        int e = errno;
        close(pfd);
        if (!ok)
            return resp_error(r->id, HELPER_E_FAILED, "swapped in, but %.300s could not be "
                              "synced, so the swap may not survive a power cut: %s%s%.300s",
                              parent, strerror(e), prev[0] ? "; the original is at " : "", prev);
    }
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "mount", st.mount);
        cJSON_AddStringToObject(res, "real", st.real);
        if (prev[0]) cJSON_AddStringToObject(res, "previous", prev);
    }
    return resp_ok(r->id, res);
refuse:
    if (pfd >= 0) close(pfd);
    swap_unclaim(s, r->token);
    if (code == HELPER_E_DENIED) log_error("helper: %s", why);
    return resp_error(r->id, code, "%s", why);
}

static char *op_unit_stop_start(helper_server_t *s, const helper_req_t *r, int start)
{
    if (!s->pol->allow_restore) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow restore");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    if (!b->set_running)
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "%s cannot be stopped or started here", r->unit);
    /* What it was before, so a restore that fails can put that back. */
    int was = b->unit_state ? b->unit_state(b->ctx, uid) : -1;
    if (b->set_running(b->ctx, uid, start) != OK)
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    cJSON *res = cJSON_CreateObject();
    if (res) cJSON_AddStringToObject(res, "state", start ? "running" : "stopped");
    if (res && was >= 0) cJSON_AddBoolToObject(res, "was_running", was == SP_STATE_RUNNING || was == SP_STATE_PAUSED);
    return resp_ok(r->id, res);
}

/* The helper parses a definition field by field and serialises its own;
 * none of the daemon's bytes would pass through. No backend here defines
 * units yet, so after the policy's checks the answer is unsupported. */
static char *op_unit_define(helper_server_t *s, const helper_req_t *r, int in_fd)
{
    if (!s->pol->allow_define) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow define");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    cJSON *d = cJSON_Parse(r->definition ? r->definition : "{}");
    int priv = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "privileged"));
    int by_fd = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "memfd"));
    int check = by_fd && cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "check"));
    cJSON_Delete(d);
    char *passed = NULL;
    if (by_fd) {
        size_t plen = 0;
        char fwhy[160];
        if (in_fd < 0) return resp_error(r->id, HELPER_E_INVALID, "the definition is said to be passed, and no fd came");
        if (!(passed = helper_read_sealed(in_fd, HELPER_BLOB_MAX, &plen, fwhy, sizeof(fwhy))))
            return resp_error(r->id, HELPER_E_INVALID, "%s", fwhy);
        cJSON *pd = cJSON_Parse(passed);
        if (!cJSON_IsObject(pd)) { cJSON_Delete(pd); free(passed); return resp_error(r->id, HELPER_E_INVALID, "the passed definition is not a JSON object"); }
        priv = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(pd, "privileged"));
        cJSON_Delete(pd);
    }
    if (priv && !s->pol->allow_privileged_define) {
        free(passed);
        return resp_error(r->id, HELPER_E_DENIED, "a privileged definition and allow_privileged_define is off");
    }
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK) {
        free(passed);
        return resp_error(r->id, e, "%s", why);
    }
    if (!b->define) {
        free(passed);
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "provider %s does not define units", b->name);
    }
    /* {"memfd": true, "check": true}: every refusal above, and nothing
     * defined (B167). A restore asks before it writes anything, so a
     * policy that would refuse the define refuses the restore instead of
     * leaving its data swapped in; it learns here too whether it may
     * stage and swap, and whether the unit runs. */
    if (check) {
        status_t cst = b->define_check
                     ? b->define_check(b->ctx, uid, passed ? passed : "{}", s->pol) : OK;
        free(passed);
        if (cst != OK) return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
        cJSON *cr = cJSON_CreateObject();
        if (cr) {
            cJSON_AddBoolToObject(cr, "checked", 1);
            cJSON_AddBoolToObject(cr, "allow_restore", s->pol->allow_restore);
            cJSON_AddNumberToObject(cr, "state", b->unit_state ? b->unit_state(b->ctx, uid) : -1);
        }
        return resp_ok(r->id, cr);
    }
    cJSON *res = NULL;
    status_t dst = b->define(b->ctx, uid, passed ? passed : (r->definition ? r->definition : "{}"), s->pol, &res);
    free(passed);
    if (dst != OK) {
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    }
    return resp_ok(r->id, res ? res : cJSON_CreateObject());
}

/* vm.install_state (vm-backup.md 7.2): NVRAM and TPM state the daemon
 * staged, installed for this UUID by the backend. */
static char *op_vm_install_state(helper_server_t *s, const helper_req_t *r)
{
    if (!s->pol->allow_define) return resp_error(r->id, HELPER_E_DENIED, "the policy does not allow define");
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK) return resp_error(r->id, e, "%s", why);
    if (!b->install_state) return resp_error(r->id, HELPER_E_UNSUPPORTED, "provider %s installs no state", b->name);
    pthread_mutex_lock(&s->mu);
    staging_t st, *x = NULL;
    for (size_t i = 0; i < STAGINGS_MAX && !x; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) x = &s->stages[i];
    if (x) st = *x;
    pthread_mutex_unlock(&s->mu);
    if (!x) return resp_error(r->id, HELPER_E_INVALID, "no such staging");
    cJSON *res = NULL;
    if (b->install_state(b->ctx, uid, st.path, st.job, &res) != OK) {
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    }
    pthread_mutex_lock(&s->mu);
    stage_drop(s, r->token);
    pthread_mutex_unlock(&s->mu);
    return resp_ok(r->id, res ? res : cJSON_CreateObject());
}

/* vm.backup_begin and vm.backup_end: a capture of one libvirt unit, by
 * another name; the session token is the capture token. */
static char *op_capture_begin(helper_server_t *s, const helper_req_t *r, int *pass_fd);
static char *op_capture_end(helper_server_t *s, const helper_req_t *r);

static char *op_vm_backup(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    if (r->op == HOP_VM_BACKUP_END) {
        helper_req_t q = *r;
        return op_capture_end(s, &q);
    }
    if (strncmp(r->unit, "libvirt:", 8) != 0)
        return resp_error(r->id, HELPER_E_INVALID, "vm.backup_begin is for libvirt units");
    helper_req_t q = *r;
    q.n_units = 1;
    snprintf(q.units[0], sizeof(q.units[0]), "%s", r->unit);
    q.pause = strcmp(r->freeze, "off") != 0;
    q.hold = 0;
    return op_capture_begin(s, &q, pass_fd);
}

/*
 * vm.checkpoint_redefine (vm-backup.md 5.8): re-create a checkpoint libvirt
 * has lost (Unraid keeps their metadata on the RAM rootfs) from the XML
 * this helper stored when it made it. Only a checkpoint of its own records
 * for that domain; the daemon names it and nothing else.
 */
static char *op_vm(helper_server_t *s, const helper_req_t *r)
{
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    if (strncmp(r->unit, "libvirt:", 8) != 0)
        return resp_error(r->id, HELPER_E_INVALID, "%s is for libvirt units", helper_op_name(r->op));
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK) return resp_error(r->id, e, "%s", why);
    if (!b->redefine) return resp_error(r->id, HELPER_E_UNSUPPORTED, "provider %s keeps no checkpoints", b->name);
    cJSON *res = NULL;
    if (b->redefine(b->ctx, uid, r->checkpoint, &res) != OK) {
        cJSON_Delete(res);
        return resp_error(r->id, HELPER_E_FAILED, "%s", err_msg());
    }
    return resp_ok(r->id, res ? res : cJSON_CreateObject());
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
    for (size_t i = 0; i < s->nb; i++) {
        const helper_backend_t *b = &s->backends[i];
        sp_sweep_report_t one;
        memset(&one, 0, sizeof(one));
        if (!b->ops->sweep || b->ops->sweep(b->ctx, &rq, &one) != OK) { err_clear(); continue; }
        rep->removed += one.removed;
        rep->left_foreign += one.left_foreign;
    }
}

static char *op_sweep(helper_server_t *s, const helper_req_t *r)
{
    unsigned rel = hj_release(s, pick_unowned, s, 0, "sweep");
    sp_sweep_report_t rep;
    sweep_artefacts(s, &rep);
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddNumberToObject(res, "released", rel);
        cJSON_AddNumberToObject(res, "removed", rep.removed);
        cJSON_AddNumberToObject(res, "left_foreign", rep.left_foreign);
        cJSON_AddNumberToObject(res, "holds", (double)helper_server_holds(s));
        /* Holds whose release keeps failing, for the daemon to alert on. */
        cJSON *stuck = cJSON_AddArrayToObject(res, "stuck");
        pthread_mutex_lock(&s->jmu);
        for (size_t i = 0; stuck && i < s->n_holds; i++) {
            const hold_t *h = &s->holds[i];
            if (!h->tries) continue;
            cJSON *o = cJSON_CreateObject();
            if (!o) break;
            cJSON_AddItemToArray(stuck, o);
            cJSON_AddStringToObject(o, "unit", h->unit);
            cJSON_AddStringToObject(o, "kind", h->kind);
            cJSON_AddStringToObject(o, "detail", h->detail);
            cJSON_AddNumberToObject(o, "tries", h->tries);
            cJSON_AddNumberToObject(o, "since", (double)(h->fail_since_ms / 1000));
            cJSON_AddStringToObject(o, "error", h->last_err);
        }
        pthread_mutex_unlock(&s->jmu);
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
    /* What the helper holds now, for the daemon's holds_open (B169). */
    cJSON_AddNumberToObject(res, "holds", (double)helper_server_holds(s));
    cJSON_AddNumberToObject(res, "captures", (double)helper_server_captures(s));
    cJSON *p = cJSON_AddArrayToObject(res, "providers");
    for (size_t i = 0; p && i < s->nb; i++)
        if (!s->backends[i].internal) cJSON_AddItemToArray(p, cJSON_CreateString(s->backends[i].name));
    /* Whether capture.begin_paths can snapshot anything here (db-detection.md 9.4). */
    cJSON_AddBoolToObject(res, "path_snapshots",
                          paths_backend_any_domain(backend_named(s, PATHS_BACKEND_NAME)));
    /* The database templates hook.run will build (db-detection.md 7.2),
     * so the daemon can tell an operator the policy line to add before
     * enabling one that would be refused. */
    cJSON *t = cJSON_AddArrayToObject(res, "templates");
    for (size_t i = 0; t && i < s->pol->n_db_allow; i++)
        cJSON_AddItemToArray(t, cJSON_CreateString(s->pol->db_allow[i]));
    /* This process started without a usable hold journal (B285): the
     * report, for the daemon to alert on once per id. Written before
     * the helper served, and never after. */
    if (s->recovery_msg) {
        cJSON *rc = cJSON_AddObjectToObject(res, "recovery");
        if (rc) {
            cJSON_AddStringToObject(rc, "id", s->recovery_id);
            char msg[MSG_CLIP_MAX];
            msg_clip(msg, sizeof(msg), s->recovery_msg);
            cJSON_AddStringToObject(rc, "msg", msg);
        }
    }
    return resp_ok(r->id, res);
}

static char *dispatch(helper_server_t *s, const helper_req_t *r, int *pass_fd, int in_fd)
{
    switch (r->op) {
    case HOP_HELLO:                  return op_hello(s, r);
    case HOP_UNITS_LIST:             return op_units_list(s, r);
    case HOP_UNITS_DESCRIBE:         return op_units_describe(s, r, pass_fd);
    case HOP_CAPTURE_BEGIN:          return op_capture_begin(s, r, pass_fd);
    case HOP_CAPTURE_BEGIN_PATHS:    return op_capture_begin_paths(s, r);
    case HOP_CAPTURE_END:            return op_capture_end(s, r);
    case HOP_HOOK_RUN:               return op_hook_run(s, r, pass_fd);
    case HOP_HOOK_WAIT:              return op_hook_wait(s, r, pass_fd);
    case HOP_VM_BACKUP_BEGIN:
    case HOP_VM_BACKUP_END:          return op_vm_backup(s, r, pass_fd);
    case HOP_VM_INSTALL_STATE:       return op_vm_install_state(s, r);
    case HOP_VM_CHECKPOINT_REDEFINE: return op_vm(s, r);
    case HOP_RESTORE_STAGE:          return op_restore_stage(s, r);
    case HOP_RESTORE_SWAP:           return op_restore_swap(s, r);
    case HOP_UNIT_STOP:              return op_unit_stop_start(s, r, 0);
    case HOP_UNIT_START:             return op_unit_stop_start(s, r, 1);
    case HOP_UNIT_DEFINE:            return op_unit_define(s, r, in_fd);
    case HOP_LEASE_RELEASE:          return op_lease_release(s, r);
    case HOP_SWEEP:                  return op_sweep(s, r);
    case HOP_UNITS_PROBE:            return op_units_probe(s, r);
    case HOP_DB_PROBE:               return op_db_probe(s, r, pass_fd);
    case HOP_COUNT:                  break;
    }
    return resp_error(r->id, HELPER_E_DENIED, "operation does not exist");
}

void helper_server_handle(helper_server_t *s, const char *req, size_t len,
                          char **resp, int *pass_fd)
{
    helper_server_handle_fd(s, req, len, -1, resp, pass_fd);
}

/* One request on a connection whose hello named session ("" for none). */
static void handle_in_session(helper_server_t *s, const char *req, size_t len, int in_fd,
                              const char *session, int conn_fd, char **resp, int *pass_fd)
{
    *pass_fd = -1;
    helper_req_t r;
    char why[512];
    helper_err_t e = helper_request_parse(req, len, &r, why, sizeof(why));
    snprintf(r.conn_session, sizeof(r.conn_session), "%s", session);
    r.conn_fd = conn_fd;
    *resp = e == HELPER_OK ? dispatch(s, &r, pass_fd, in_fd) : resp_error(r.id, e, "%s", why);
    helper_req_free(&r);
    err_clear();
}

void helper_server_handle_fd(helper_server_t *s, const char *req, size_t len, int in_fd,
                             char **resp, int *pass_fd)
{
    handle_in_session(s, req, len, in_fd, "", -1, resp, pass_fd);
}

/* ------------------------------------------------------------------ */
/* Connections                                                         */
/* ------------------------------------------------------------------ */

typedef struct { helper_server_t *s; int fd; int slot; } conn_arg_t;

static void *conn_main(void *arg)
{
    thread_name("cbh-session");
    conn_arg_t *c = arg;
    helper_server_t *s = c->s;
    char *buf = malloc(HELPER_PACKET_MAX + 2);
    int greeted = 0, anchor = 0;
    char session[HELPER_TOKEN_HEX + 1] = "";
    while (buf) {
        size_t got = 0;
        int rfd = -1;
        status_t st = helper_recv(c->fd, buf, HELPER_PACKET_MAX + 2, &got, &rfd);
        if (st == ERR_TOO_LARGE) {
            char *e = resp_error(0, HELPER_E_INVALID, "a request is at most 65536 bytes");
            if (e) (void)helper_send(c->fd, e, strlen(e), -1);
            free(e);
            err_clear();
            if (rfd >= 0) close(rfd);
            continue;
        }
        if (st != OK) { err_clear(); if (rfd >= 0) close(rfd); break; }
        char *resp = NULL;
        int pfd = -1;
        if (!greeted) {
            helper_req_t r;
            char why[512];
            helper_err_t e = helper_request_parse(buf, got, &r, why, sizeof(why));
            if (e == HELPER_OK && r.op == HOP_HELLO) {
                resp = dispatch(s, &r, &pfd, -1);
                greeted = 1;
                snprintf(session, sizeof(session), "%s", r.token);
                if (r.anchor && session[0]) { anchor = 1; anchor_add(s, session); }
            } else {
                resp = resp_error(r.id, e != HELPER_OK ? e : HELPER_E_DENIED,
                                  "%s", e != HELPER_OK ? why : "the first request must be hello");
            }
            helper_req_free(&r);
        } else {
            handle_in_session(s, buf, got, rfd, session, c->fd, &resp, &pfd);
        }
        if (rfd >= 0) close(rfd);
        if (resp) (void)helper_send(c->fd, resp, strlen(resp), pfd);
        err_clear();
        free(resp);
        if (pfd >= 0) close(pfd);
    }
    free(buf);
    /* The daemon gone, killed or stopped: its captures go with it. */
    if (anchor) anchor_drop(s, session);
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
    thread_name("cbh-accept");
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

unsigned helper_server_tick(helper_server_t *s, int64_t now_ms)
{
    time_t now = (time_t)(now_ms / 1000);
    age_t a = { now_ms, s->pol->max_pause_s };
    unsigned n = hj_release(s, hj_pick_old, &a, 1, "watchdog");
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
    thread_name("cbh-watchdog");
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
        (void)helper_server_tick(s, now_ms());
        pthread_mutex_lock(&s->cmu);
    }
    pthread_mutex_unlock(&s->cmu);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Start and stop                                                      */
/* ------------------------------------------------------------------ */

/*
 * The socket's owner, group and mode, logged at start; and a loud warning
 * when the daemon could not reach it: each directory on the way must let a
 * member of daemon_gid (or daemon_uid itself) search it, and the socket
 * must be writable by that group. Nothing here fails the start, because
 * the helper cannot know every group the daemon's uid holds; it says what
 * it sees instead of leaving a refused connect as the only symptom.
 */
static void socket_report(const char *path, uid_t uid, gid_t gid)
{
    struct stat sb;
    if (stat(path, &sb) != 0) { log_warn("helper: cannot stat %s: %s", path, strerror(errno)); return; }
    log_info("helper: socket %s owner %u group %u mode %04o", path, (unsigned)sb.st_uid,
             (unsigned)sb.st_gid, (unsigned)(sb.st_mode & 07777));
    if (!(sb.st_gid == gid && (sb.st_mode & S_IWGRP)) && !(sb.st_uid == uid && (sb.st_mode & S_IWUSR)) &&
        !(sb.st_mode & S_IWOTH))
        log_warn("helper: WARNING: the daemon (uid %u, gid %u) cannot write to socket %s (owner %u "
                 "group %u mode %04o); it will be refused at connect", (unsigned)uid, (unsigned)gid, path,
                 (unsigned)sb.st_uid, (unsigned)sb.st_gid, (unsigned)(sb.st_mode & 07777));
    char p[4096];
    snprintf(p, sizeof(p), "%s", path);
    for (char *c = strrchr(p, '/'); c && c > p; c = strrchr(p, '/')) {
        *c = '\0';
        struct stat d;
        if (stat(p, &d) != 0) continue;
        int ok = (d.st_mode & S_IXOTH) || (d.st_gid == gid && (d.st_mode & S_IXGRP)) ||
                 (d.st_uid == uid && (d.st_mode & S_IXUSR));
        if (!ok)
            log_warn("helper: WARNING: no member of group %u (nor uid %u) can enter %s (owner %u "
                     "group %u mode %04o), so the daemon cannot reach socket %s; give the "
                     "directory to that group or make it searchable", (unsigned)gid, (unsigned)uid, p,
                     (unsigned)d.st_uid, (unsigned)d.st_gid, (unsigned)(d.st_mode & 07777), path);
    }
}

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
     * daemon_uid with its own gid, had no write permission on the socket
     * and connect failed with EACCES. The group is daemon_gid (default
     * daemon_uid, what security.run_as_uid gives the daemon); SO_PEERCRED
     * still refuses every uid but daemon_uid on each connection. Root may
     * give any group; anyone else only one of their own, which is all a
     * helper run unprivileged (tests) can serve anyway. */
    if (chown(path, (uid_t)-1, s->pol->daemon_gid) != 0 && (geteuid() == 0 || errno != EPERM)) {
        int err = errno;
        close(fd);
        (void)unlink(path);
        errno = err;
        return set_error_errno(ERR_IO, "helper: chown %s to group %u", path, (unsigned)s->pol->daemon_gid);
    }
    socket_report(path, s->pol->daemon_uid, s->pol->daemon_gid);
    s->lfd = fd;
    return OK;
}

/*
 * What the last helper process held is released, and leftovers of this
 * daemon id destroyed, on a thread of its own: a release can block (a
 * thaw waits up to 30 s for a freeze still pending in libvirt, B175), and
 * the helper answers meanwhile. Only holds no live capture owns are
 * picked, so a capture begun meanwhile is never touched. One that fails
 * stays journalled and the watchdog retries it.
 */
static void *boot_main(void *arg)
{
    helper_server_t *s = arg;
    unsigned rel = hj_release(s, pick_unowned, s, 0, "start-up sweep");
    recov_t *rv = s->recovering ? hj_recov_new() : NULL;
    if (rv) hj_recover_units(s, rv);
    else if (s->recovering) log_error("helper: out of memory; the backends were not asked what a lost journal left");
    sp_sweep_report_t rep;
    sweep_artefacts(s, &rep);
    if (rel || rep.removed)
        log_warn("helper: start-up sweep released %u hold(s), removed %u artefact(s)", rel, rep.removed);
    size_t left = helper_server_holds(s);
    if (rv) hj_recovery_report(s, rel, left, rv, rep.removed);
    free(rv);
    pthread_mutex_lock(&s->jmu);
    if (hj_save(s) != OK) err_clear();   /* logged; holds are still in memory */
    pthread_mutex_unlock(&s->jmu);
    if (left) log_warn("helper: %zu hold(s) still journalled after the start-up sweep; the watchdog retries them", left);
    pthread_mutex_lock(&s->cmu);
    s->boot_done = 1;
    pthread_cond_broadcast(&s->wcv);
    pthread_mutex_unlock(&s->cmu);
    return NULL;
}

status_t helper_server_start(const helper_server_cfg_t *cfg, helper_server_t **out)
{
    helper_server_t *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "helper: server");
    s->pol = cfg->policy;
    if (cfg->fake) helper_backend_fake(&s->backends[s->nb++], cfg->fake);
    for (size_t i = 0; i < cfg->n_backends && s->nb < HELPER_BACKENDS_MAX; i++)
        s->backends[s->nb++] = cfg->backends[i];
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

    /* Whatever the last helper process held is released, on its own
     * thread (boot_main). Start waits for it a moment, which is all a
     * sweep that does not block takes, so a quick one is done before the
     * first request; a slow one goes on while the helper serves. */
    /* A lost journal (B285) is recovered before anything is served: the
     * backends are asked what it could have left held, and a capture begun
     * meanwhile would be among what they find. */
    char mdir[PATH_MAX + 32];
    snprintf(mdir, sizeof(mdir), "%s/" MARKER_DIR, s->pol->state_dir);
    if (mkdir(mdir, 0700) != 0 && errno != EEXIST)
        log_error("helper: cannot make %s: %s; no hold can be taken until it exists", mdir, strerror(errno));
    hj_load(s);
    if (pthread_create(&s->boot_th, NULL, boot_main, s) == 0) {
        s->boot_on = 1;
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_sec += 2;
        pthread_mutex_lock(&s->cmu);
        while (!s->boot_done)
            if (s->recovering) pthread_cond_wait(&s->wcv, &s->cmu);
            else if (pthread_cond_timedwait(&s->wcv, &s->cmu, &dl) != 0) break;
        int done = s->boot_done;
        pthread_mutex_unlock(&s->cmu);
        if (!done) log_warn("helper: the start-up sweep is still running; serving meanwhile");
    } else {
        boot_main(s);
    }

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
    if (s->boot_on) pthread_join(s->boot_th, NULL);
    for (int i = 0; i < CONNS_MAX; i++) if (s->conn_th[i]) pthread_join(s->conn_th[i], NULL);
    /* Captures still held are ended; their holds go with them. */
    for (size_t i = 0; i < CAPTURES_MAX; i++)
        if (s->caps[i].used == 1) { capture_t c = s->caps[i]; s->caps[i].used = 0; capture_finish(s, &c, SP_END_ABORTED); }
    for (size_t i = 0; i < RUNS_MAX; i++)
        if (s->runs[i].used) {
            if (s->runs[i].hr) {
                if (!s->runs[i].done) s->runs[i].b->hook_kill(s->runs[i].hr);
                s->runs[i].b->hook_free(s->runs[i].hr);
            } else if (!s->runs[i].done) {
                kill(-s->runs[i].pid, SIGKILL);
                waitpid(s->runs[i].pid, NULL, 0);
            }
            close(s->runs[i].err_fd);
            hooklog_free(&s->runs[i].log);
        }
    for (size_t i = 0; i < STAGINGS_MAX; i++) if (s->stages[i].used) stage_close(&s->stages[i]);
    pthread_cond_destroy(&s->wcv);
    pthread_mutex_destroy(&s->cmu);
    pthread_mutex_destroy(&s->jmu);
    pthread_mutex_destroy(&s->mu);
    free(s->recovery_msg);
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

status_t helper_server_record(helper_server_t *s, const char *unit, const char *kind,
                              const char *detail)
{
    jctx_t j = { s, "", 0 };
    snprintf(j.unit, sizeof(j.unit), "%s", unit);
    int64_t id = 0;
    return hj_record(&j, kind, detail, &id);
}
