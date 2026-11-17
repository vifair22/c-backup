#define _GNU_SOURCE

#include "helper_server.h"
#include "artefact.h"
#include "helper_backend.h"
#include "helper_proto.h"
#include "hooklog.h"

#include <sys/mman.h>

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

/* ------------------------------------------------------------------ */
/* State                                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t id;
    char    unit[HELPER_UNIT_KEY_MAX + 1];  /* provider:id */
    char    kind[16];
    char    detail[192];
    int64_t created_ms;                     /* wall clock; 0 after a restart */
    int64_t capture;                        /* the capture's sequence number */
} hold_t;

struct jctx;

/* Wall-clock milliseconds: a hold's age against max_pause_s. In whole
 * seconds the watchdog fired up to a second early. */
static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

typedef struct {
    char    unit[HELPER_UNIT_KEY_MAX + 1];
    const helper_backend_t *b;
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
    /* The daemon session that began it, and whether that session held an
     * anchor connection then: only such a capture ends with its session
     * (B169); one begun with no anchor (a client that keeps none) lives
     * to its lease as before. */
    char     session[HELPER_TOKEN_HEX + 1];
    int      anchored;
} capture_t;

/* A daemon session: the daemon process, by the token its hellos carry,
 * and how many anchor connections it has open. */
typedef struct {
    char     token[HELPER_TOKEN_HEX + 1];
    int      anchors;
} dsession_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    pid_t    pid;             /* a local process, or */
    const helper_backend_t *b;/* a run inside the unit, through its backend */
    helper_run_t *hr;
    int      err_fd;          /* the hook's stderr, for the log */
    time_t   deadline;
    int      done, status, timed_out;
    hooklog_t log;            /* stderr, head and tail kept (6.6) */
} run_t;

typedef struct {
    int      used;
    char     token[HELPER_TOKEN_HEX + 1];
    char     unit[HELPER_UNIT_KEY_MAX + 1];
    char     mount[HELPER_PATH_MAX + 1];
    char     path[HELPER_PATH_MAX + 1];
    int64_t  job;
    int      swapped;         /* swapped in: kept for an undo until the slot is wanted */
    int      had;             /* the mount existed, and was moved to old */
    char     old[HELPER_PATH_MAX + 1];
    int      file;            /* the mount is a disk image: <path>/image replaces it */
} staging_t;

struct helper_server {
    const helper_policy_t *pol;
    helper_backend_t       backends[HELPER_BACKENDS_MAX];
    size_t                 nb;
    pthread_mutex_t        mu;          /* everything below but the journal */
    capture_t              caps[CAPTURES_MAX];
    run_t                  runs[RUNS_MAX];
    staging_t              stages[STAGINGS_MAX];
    dsession_t             sessions[CONNS_MAX];
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

/* Caller holds jmu. Written whole through a temporary and a rename, and
 * the rename synced: a hold the journal does not list is one a restarted
 * helper cannot release. Returns OK, or the failure (also logged). */
static status_t journal_save(helper_server_t *s)
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
        cJSON_AddNumberToObject(o, "created", (double)(s->holds[i].created_ms / 1000));
    }
    char *txt = a ? cJSON_PrintUnformatted(a) : NULL;
    cJSON_Delete(a);
    if (!txt) {
        log_error("helper: cannot serialise the hold journal");
        return set_error(ERR_NOMEM, "helper: cannot serialise the hold journal");
    }
    char path[PATH_MAX + 32], tmp[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/holds.json", s->pol->state_dir);
    snprintf(tmp, sizeof(tmp), "%s/holds.json.tmp", s->pol->state_dir);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    size_t n = strlen(txt);
    const char *step = "create";
    int ok = fd >= 0;
    for (size_t done = 0; ok && done < n; ) {
        ssize_t w = write(fd, txt + done, n - done);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) { if (w == 0) errno = EIO; ok = 0; step = "write"; break; }
        done += (size_t)w;
    }
    if (ok && fsync(fd) != 0) { ok = 0; step = "sync"; }
    int err = errno;
    if (fd >= 0 && close(fd) != 0 && ok) { ok = 0; step = "close"; err = errno; }
    if (ok && rename(tmp, path) != 0) { ok = 0; step = "rename"; err = errno; }
    if (ok) {
        int dfd = open(s->pol->state_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0 || fsync(dfd) != 0) { ok = 0; step = "sync the directory of"; err = errno; }
        if (dfd >= 0) close(dfd);
    }
    cJSON_free(txt);
    if (ok) return OK;
    if (strcmp(step, "rename") != 0 && strcmp(step, "sync the directory of") != 0)
        (void)unlink(tmp);
    errno = err;
    status_t st = set_error_errno(ERR_IO, "helper: cannot %s %s", step,
                                  strcmp(step, "create") == 0 || strcmp(step, "write") == 0 ||
                                  strcmp(step, "sync") == 0 || strcmp(step, "close") == 0
                                  ? tmp : path);
    log_error("%s", err_msg());
    return st;
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
    h->created_ms = now_ms();
    h->capture = j->capture;
    /* Recorded before the hold is taken, and a hold the journal could not
     * keep is not taken at all: after a crash nothing would release it. */
    status_t st = journal_save(s);
    if (st != OK) {
        s->n_holds--;
        pthread_mutex_unlock(&s->jmu);
        return st;
    }
    *out = h->id;
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
        /* Released already; a journal still naming it costs a redundant
         * release after a crash, nothing more. Logged by journal_save. */
        if (journal_save(s) != OK) err_clear();
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
        h->created_ms = 0;
        h->capture = -1;              /* nobody's: the process that took it is gone */
        if (h->id > s->next_hold) s->next_hold = h->id;
    }
    cJSON_Delete(a);
}

/*
 * Release the holds that select() picks, through the backend, and drop the
 * ones released. Returns how many were. Caller holds nothing. With
 * keep_absent a hold the backend found nothing under stays journalled:
 * the watchdog acts on live captures, whose holds are recorded before they
 * are taken, so nothing held may mean not taken yet (sp_sweep_req_t).
 */
typedef int (*hold_pick_fn)(const hold_t *h, void *ctx);

/* The backend a unit key's provider names; *uid points past the colon. */
static const helper_backend_t *backend_of(const helper_server_t *s, const char *key,
                                          const char **uid)
{
    const char *colon = strchr(key, ':');
    if (!colon) return NULL;
    size_t pl = (size_t)(colon - key);
    for (size_t i = 0; i < s->nb; i++)
        if (strlen(s->backends[i].name) == pl && strncmp(s->backends[i].name, key, pl) == 0) {
            if (uid) *uid = colon + 1;
            return &s->backends[i];
        }
    return NULL;
}

static unsigned release_holds(helper_server_t *s, hold_pick_fn pick, void *pctx, int keep_absent,
                              const char *why)
{
    hold_t *picked = calloc(HOLDS_MAX, sizeof(*picked));
    if (!picked) return 0;
    size_t n = 0;
    pthread_mutex_lock(&s->jmu);
    for (size_t i = 0; i < s->n_holds; i++)
        if (backend_of(s, s->holds[i].unit, NULL) && pick(&s->holds[i], pctx))
            picked[n++] = s->holds[i];
    pthread_mutex_unlock(&s->jmu);
    unsigned released = 0;
    for (size_t i = 0; i < n; i++) {
        const char *uid = NULL;
        const helper_backend_t *b = backend_of(s, picked[i].unit, &uid);
        sp_intent_t in = { picked[i].id, uid, picked[i].kind, picked[i].detail, 0 };
        int rel = 0, absent = 0;
        sp_sweep_req_t rq;
        memset(&rq, 0, sizeof(rq));
        rq.daemon8 = s->pol->daemon_id;
        rq.intents = &in;
        rq.n_intents = 1;
        rq.released = &rel;
        rq.absent = &absent;
        sp_sweep_report_t rep;
        status_t st = b && b->ops->sweep ? b->ops->sweep(b->ctx, &rq, &rep) : ERR_INVALID;
        if (st == OK && rel && absent && keep_absent) continue;
        if (st == OK && rel) {
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

typedef struct { int64_t now_ms; unsigned pause_s; } age_t;

static int pick_old(const hold_t *h, void *ctx)
{
    const age_t *a = ctx;
    int timed = strcmp(h->kind, SP_HOLD_FREEZE) == 0 || strcmp(h->kind, SP_HOLD_PAUSE) == 0;
    return timed && h->created_ms + (int64_t)a->pause_s * 1000 <= a->now_ms;
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

/* The backend for a unit key, or why not. On OK *uid points into key. */
static helper_err_t backend_for(helper_server_t *s, const char *key, const char **uid,
                                const helper_backend_t **b, char *why, size_t wsz)
{
    char prov[16], id[HELPER_UNIT_KEY_MAX + 1];
    if (!helper_unit_key_ok(key, prov, sizeof(prov), id, sizeof(id))) {
        snprintf(why, wsz, "malformed unit key");
        return HELPER_E_INVALID;
    }
    const helper_backend_t *found = backend_of(s, key, uid);
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
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
    }
    return resp_ok(r->id, res);
}

/*
 * Configuration blobs travel in one sealed memfd passed with the response,
 * each item naming its offset and length in it: a container's inspect JSON
 * or an LXC config can exceed what a 64 KiB packet holds, and the daemon
 * reads sealed bytes that cannot change under it.
 */
static int blob_fd_append(int *fd, uint64_t *off, const void *data, size_t len)
{
    if (*fd < 0) {
        *fd = memfd_create("cbk-blobs", MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (*fd < 0) return -1;
    }
    const char *p = data;
    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(*fd, p + done, len - done, (off_t)(*off + done));
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    *off += len;
    return 0;
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
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
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
            int werr = len > HELPER_BLOB_MAX || blob_fd_append(&bfd, &boff, data, len) != 0;
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
    char fmsg[HELPER_UNIT_KEY_MAX + sizeof(": ") + 400] = "out of memory";
    /* Several units of one backend that can capture them together: one
     * pause and one snapshot set for all (6.2), a session each. */
    sp_unit_desc_t *gd[HELPER_UNITS_MAX] = { 0 };
    sp_session_t *gs[HELPER_UNITS_MAX] = { 0 };
    const sp_session_info_t *gi[HELPER_UNITS_MAX] = { 0 };
    jctx_t *gj = NULL;
    int grp = 0;
    if (fail == HELPER_OK && r->n_units > 1) {
        const char *u0 = NULL;
        const helper_backend_t *b0 = backend_of(s, r->units[0], &u0);
        grp = b0 && b0->ops->begin_group != NULL;
        for (size_t i = 1; grp && i < r->n_units; i++) grp = backend_of(s, r->units[i], NULL) == b0;
        for (size_t i = 0; grp && fail == HELPER_OK && i < r->n_units; i++) {
            const char *uid = NULL;
            (void)backend_of(s, r->units[i], &uid);
            if (b0->ops->describe(b0->ctx, uid, &gd[i]) != OK) {
                snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
                fail = HELPER_E_FAILED;
            }
        }
        if (grp && fail == HELPER_OK && !(gj = calloc(1, sizeof(*gj)))) fail = HELPER_E_FAILED;
        if (grp && fail == HELPER_OK) {
            gj->s = s;
            snprintf(gj->unit, sizeof(gj->unit), "%s", r->units[0]);
            gj->capture = seq;
            sp_holds_t gh = { j_record, j_clear, gj };
            sp_begin_req_t q;
            memset(&q, 0, sizeof(q));
            q.job_id = seq;
            q.want = r->pause ? SP_CONSIST_QUIESCED : SP_CONSIST_CRASH;
            q.require = SP_CONSIST_INCONSISTENT;
            q.freeze_timeout_s = s->pol->max_pause_s;
            q.pause_max_s = s->pol->max_pause_s;
            q.holds = &gh;
            q.daemon8 = s->pol->daemon_id;
            if (b0->ops->begin_group(b0->ctx, (const sp_unit_desc_t *const *)gd, r->n_units, &q, gs, gi) != OK) {
                snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
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
        const helper_backend_t *b = backend_of(s, r->units[i], &uid);
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
            snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
            fail = HELPER_E_FAILED;
            break;
        }
        j = calloc(1, sizeof(*j));
        if (!j) { b->ops->desc_free(d); fail = HELPER_E_FAILED; break; }
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
            snprintf(fmsg, sizeof(fmsg), "%s: %.400s", r->units[i], err_msg());
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
                snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
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
                snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg());
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
            if (b->ops->blob_read(sess, it, &data, &len) != OK) { err_clear(); continue; }
            uint64_t at = loff;
            int werr = len > HELPER_BLOB_MAX || blob_fd_append(&lfd, &loff, data, len) != 0;
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
            const helper_backend_t *b0 = backend_of(s, r->units[0], NULL);
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
        snprintf(slot->session, sizeof(slot->session), "%s", r->conn_session);
        slot->anchored = session_anchored(s, r->conn_session);
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

static char *op_hook_run(helper_server_t *s, const helper_req_t *r, int *pass_fd)
{
    char why[256];
    helper_err_t e = unit_allowed(s, r->unit, why, sizeof(why));
    if (e != HELPER_OK) return resp_error(r->id, e, "%s", why);
    const policy_hook_t *h = helper_policy_hook(s->pol, r->hook);
    if (!h || strcmp(h->unit, r->unit) != 0)
        return resp_error(r->id, HELPER_E_DENIED, "hook %s is not in the policy for %s", r->hook, r->unit);
    const char *uid = NULL;
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);

    int out[2] = { -1, -1 }, err[2] = { -1, -1 };
    if (pipe2(err, O_CLOEXEC) != 0 || (h->dump && pipe2(out, O_CLOEXEC) != 0)) {
        if (err[0] >= 0) { close(err[0]); close(err[1]); }
        return resp_error(r->id, HELPER_E_FAILED, "pipe: %s", strerror(errno));
    }
    pid_t pid = -1;
    helper_run_t *hr = NULL;
    if (b->hook_start) {
        /* Inside the unit (docker exec, lxc-attach), argv as the policy
         * gives it and no shell. */
        status_t hst = b->hook_start(b->ctx, uid, h, h->dump ? out[1] : -1, err[1], &hr);
        close(err[1]);
        if (h->dump) close(out[1]);
        if (hst != OK) {
            close(err[0]);
            if (h->dump) close(out[0]);
            return resp_error(r->id, HELPER_E_FAILED, "hook %s: %.300s", r->hook, err_msg());
        }
    } else {
    /* No unit to enter (the fake): the policy's argv runs as given, with no
     * shell. */
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    if (h->dump) posix_spawn_file_actions_adddup2(&fa, out[1], 1);
    else posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_adddup2(&fa, err[1], 2);
    int rc = posix_spawn(&pid, h->argv[0], &fa, NULL, h->argv, environ);
    posix_spawn_file_actions_destroy(&fa);
    close(err[1]);
    if (h->dump) close(out[1]);
    if (rc != 0) {
        close(err[0]);
        if (h->dump) close(out[0]);
        return resp_error(r->id, HELPER_E_FAILED, "hook %s: %s", r->hook, strerror(rc));
    }
    }
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
        slot->deadline = time(NULL) + (time_t)h->timeout_s;
        if (helper_random_hex(slot->token, 16) == OK) slot->used = 1;
        else slot = NULL;
    }
    char token[HELPER_TOKEN_HEX + 1] = "";
    if (slot) memcpy(token, slot->token, sizeof(token));
    pthread_mutex_unlock(&s->mu);
    if (!slot) {
        if (hr) { b->hook_kill(hr); b->hook_free(hr); }
        else { kill(pid, SIGKILL); waitpid(pid, NULL, 0); }
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
    if (x->timed_out)
        snprintf(line, sizeof(line), "\n[c-backup: killed at its timeout, exit status %d]\n", x->status);
    else
        snprintf(line, sizeof(line), "\n[c-backup: exit status %d]\n", x->status);
    (void)hooklog_append(&x->log, line, strlen(line));
}

/* Reap a run if it ended; kill it if it is past its deadline. Caller
 * holds mu. */
static void run_poll(run_t *x, time_t now)
{
    if (x->done) return;
    run_drain(x);
    int st = 0;
    if (x->hr) {
        int code = 0;
        if (x->b->hook_poll(x->hr, &code)) {
            x->done = 1;
            x->status = code;
            run_finish(x);
        } else if (now >= x->deadline) {
            x->b->hook_kill(x->hr);
            x->done = 1;
            x->timed_out = 1;
            x->status = 128 + SIGKILL;
            run_finish(x);
        }
        return;
    }
    pid_t p = waitpid(x->pid, &st, WNOHANG);
    if (p == x->pid) {
        x->done = 1;
        x->status = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
        run_finish(x);
        return;
    }
    if (now >= x->deadline) {
        kill(x->pid, SIGKILL);
        waitpid(x->pid, &st, 0);
        x->done = 1;
        x->timed_out = 1;
        x->status = 128 + SIGKILL;
        run_finish(x);
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
        run_poll(x, time(NULL));
        if (x->done) {
            /* The whole log, up to 1 MiB, in a sealed memfd; a tail of it
             * inline for messages. */
            size_t len = 0;
            char *txt = hooklog_render(&x->log, &len);
            int lfd = -1;
            uint64_t loff = 0;
            if (txt && (blob_fd_append(&lfd, &loff, txt, len) != 0 ||
                        fcntl(lfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL) != 0)) {
                if (lfd >= 0) close(lfd);
                lfd = -1;
            }
            cJSON *res = cJSON_CreateObject();
            if (res) {
                cJSON_AddNumberToObject(res, "exit", x->status);
                cJSON_AddBoolToObject(res, "timed_out", x->timed_out);
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

/* Whether mount is a mount source of the unit in the helper's own inspect:
 * the one path the daemon names, never trusted otherwise. */
/* Whether mount is the unit's in its own describe: 1 a tree, 2 a disk
 * image (a BLOCK item's source), 3 a configuration file (a BLOB's source,
 * staged for vm.install_state, never swapped); 0 not the unit's. */
static int unit_has_mount(const helper_backend_t *b, const char *uid, const char *mount)
{
    sp_unit_desc_t *d = NULL;
    if (b->ops->describe(b->ctx, uid, &d) != OK) { err_clear(); return 0; }
    int ok = 0;
    for (size_t i = 0; i < d->n_items && !ok; i++) {
        const sp_item_desc_t *it = &d->items[i];
        if (it->kind == SP_ITEM_TREE && it->tree_path && strcmp(it->tree_path, mount) == 0) ok = 1;
        else if (it->kind == SP_ITEM_BLOCK && it->source && strcmp(it->source, mount) == 0) ok = 2;
        else if (it->kind == SP_ITEM_BLOB && it->source && strcmp(it->source, mount) == 0) ok = 3;
    }
    b->ops->desc_free(d);
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

/*
 * Where restore.stage stages for a mount in domain dom (Manual review item
 * 4). On FS2 the domain is the whole pool, and <domain>/.cbackup-restore
 * was a dot directory at the pool's top, the placement the snapshot
 * directory avoids: anything at the top of a pool, or in an exported share,
 * is visible over the network. So: the first restore_staging_dirs entry
 * inside the domain; else, for a pool-top domain (mnt_root/<pool>),
 * <pool>/system/.cbackup-restore, beside the snapshot directory; else
 * <domain>/.cbackup-restore. Then, wherever it came from, it is refused at
 * the top of a pool or in a share exported over SMB or NFS
 * (<boot_config>/shares/<share>.cfg). -1 with why on a refusal.
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
    if (d < 0) return 0;                       /* not an Unraid pool path: nothing exports it */
    if (d < 3) {
        snprintf(why, wsz, "restore staging %.300s would sit at the top of a pool, visible over the "
                 "network; set [snapshot] restore_staging_dirs to a directory inside a share", out);
        return -1;
    }
    /* mnt_root/<pool>/<share>/...: the share is the second component. */
    const char *rest = out + strlen(p->mnt_root) + 1;
    const char *sh = strchr(rest, '/');
    if (!sh) return 0;
    sh++;
    size_t k = strcspn(sh, "/");
    char cfg[PATH_MAX + 512];
    if (snprintf(cfg, sizeof(cfg), "%s%s/shares/%.*s.cfg", p->host_root, p->boot_config, (int)k, sh) >= (int)sizeof(cfg))
        return 0;
    FILE *f = fopen(cfg, "re");
    if (!f) return 0;                          /* not an Unraid share */
    char line[512];
    int exported = 0;
    while (!exported && fgets(line, sizeof(line), f)) {
        const char *keys[] = { "shareExport=", "shareExportNFS=" };
        for (size_t i = 0; i < 2 && !exported; i++) {
            if (strncmp(line, keys[i], strlen(keys[i])) != 0) continue;
            const char *v = line + strlen(keys[i]);
            if (*v == '"') v++;
            exported = *v && *v != '-' && *v != '"' && *v != '\n';
        }
    }
    fclose(f);
    if (exported) {
        snprintf(why, wsz, "restore staging %.300s is in share %.*s, which is exported over the network; "
                 "set [snapshot] restore_staging_dirs to a directory in a share that is not", out, (int)k, sh);
        return -1;
    }
    return 0;
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
    int kind = unit_has_mount(b, uid, r->mount);
    if (!kind)
        return resp_error(r->id, HELPER_E_DENIED, "%.300s is not a mount of %s", r->mount, r->unit);
    /* The staging directory sits in the mount's own snapshot domain, so the
     * swap is a rename within one filesystem. */
    char droot[PATH_MAX] = "";
    const char *dom = NULL;
    if (b->mount_root) {
        if (b->mount_root(b->ctx, r->mount, droot, sizeof(droot)) == OK) dom = droot;
        else err_clear();
    } else {
        dom = helper_policy_dir_domain(s->pol, r->mount);
    }
    if (!dom || strcmp(dom, r->mount) == 0)
        return resp_error(r->id, HELPER_E_DENIED, "%.300s is in no snapshot domain of the policy", r->mount);
    /* One directory per staging: a VM's disks share a job and a domain. */
    char base[PATH_MAX], path[PATH_MAX], swhy[512], uniq[HELPER_TOKEN_HEX + 1];
    if (helper_random_hex(uniq, 4) != OK) return resp_error(r->id, HELPER_E_FAILED, "no randomness");
    if (staging_base(s, dom, base, sizeof(base), swhy, sizeof(swhy)) != 0)
        return resp_error(r->id, HELPER_E_DENIED, "%s", swhy);
    if (snprintf(path, sizeof(path), "%s/%lld-%s", base, (long long)r->job, uniq) >= (int)sizeof(path))
        return resp_error(r->id, HELPER_E_INVALID, "path too long");
    /* Made down to the staging base, which may be a share directory not
     * there yet (system/), each level the helper's own except the last.
     * Under mu: a swap removes the base once it is empty (staging_tidy),
     * and must not do so between this mkdir of it and the one of path. */
    {
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s", base);
        pthread_mutex_lock(&s->mu);
        for (char *c = p + strlen(dom) + 1; *c; c++) {
            if (*c != '/') continue;
            *c = '\0';
            if (mkdir(p, 0755) != 0 && errno != EEXIST) {
                int err = errno;
                pthread_mutex_unlock(&s->mu);
                return resp_error(r->id, HELPER_E_FAILED, "cannot create %s: %s", p, strerror(err));
            }
            *c = '/';
        }
        int bad = mkdir_owned(base, s->pol->daemon_uid) != 0 || mkdir_owned(path, s->pol->daemon_uid) != 0;
        int err = errno;
        pthread_mutex_unlock(&s->mu);
        if (bad) return resp_error(r->id, HELPER_E_FAILED, "cannot create %s: %s", path, strerror(err));
    }
    /* The swap is a rename: the staging must be on the mount's filesystem
     * (a btrfs subvolume or another dataset would be EXDEV at the swap,
     * after the whole restore was written). */
    {
        char parent[PATH_MAX];
        if (snprintf(parent, sizeof(parent), "%.4000s", r->mount) >= (int)sizeof(parent)) parent[0] = '\0';
        char *sl = strrchr(parent, '/');
        if (sl && sl != parent) *sl = '\0';
        struct stat a1, a2;
        if (stat(path, &a1) == 0 && stat(parent, &a2) == 0 && a1.st_dev != a2.st_dev)
            return resp_error(r->id, HELPER_E_DENIED, "restore staging %.300s is on another filesystem "
                              "from %.300s, so the swap could not be a rename; set [snapshot] "
                              "restore_staging_dirs to a directory on its filesystem", base, r->mount);
    }
    pthread_mutex_lock(&s->mu);
    staging_t *slot = NULL;
    for (size_t i = 0; i < STAGINGS_MAX && !slot; i++) if (!s->stages[i].used) slot = &s->stages[i];
    /* A swapped staging is only kept for an undo: the first to go when
     * the table is full. */
    for (size_t i = 0; i < STAGINGS_MAX && !slot; i++) if (s->stages[i].swapped) slot = &s->stages[i];
    char token[HELPER_TOKEN_HEX + 1] = "";
    if (slot && helper_random_hex(slot->token, 16) == OK) {
        slot->used = 1;
        slot->swapped = 0;
        slot->had = 0;
        slot->old[0] = '\0';
        slot->job = r->job;
        snprintf(slot->unit, sizeof(slot->unit), "%s", r->unit);
        snprintf(slot->mount, sizeof(slot->mount), "%s", r->mount);
        snprintf(slot->path, sizeof(slot->path), "%s", path);
        slot->file = kind;
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

/*
 * After a swap, what of the staging is empty goes (B168: an empty
 * <domain>/.cbackup-restore was left at the top of the domain): the
 * staging directory of a disk image, whose image moved out of it, and the
 * base when it is the helper's own .cbackup-restore with nothing else in
 * it. An operator's restore_staging_dirs entry is theirs and stays. rmdir
 * only, so anything another restore still has there is safe, and under mu
 * against a stage making the base at the same time.
 */
static void staging_tidy(helper_server_t *s, const char *dir, int file)
{
    char base[HELPER_PATH_MAX + 1];
    if (snprintf(base, sizeof(base), "%s", dir) >= (int)sizeof(base)) return;
    char *sl = strrchr(base, '/');
    if (!sl || sl == base) return;
    *sl = '\0';
    const char *leaf = strrchr(base, '/');
    pthread_mutex_lock(&s->mu);
    if (file == 2) (void)rmdir(dir);
    if (leaf && strcmp(leaf, "/.cbackup-restore") == 0) (void)rmdir(base);
    pthread_mutex_unlock(&s->mu);
}

/*
 * restore.swap with undo (B167): what a swap did, reversed, for a restore
 * that failed after it. The unit must still be stopped. The restored copy
 * goes back to its staging directory, out of sight as it was, and the data
 * moved aside comes back to the mount. A staging whose slot a later stage
 * has taken has no undo; its data is still at <mount>.cbackup-old-<job>.
 */
static char *swap_undo(helper_server_t *s, const helper_req_t *r, const staging_t *st)
{
    if (!st->swapped) return resp_error(r->id, HELPER_E_INVALID, "not swapped; nothing to undo");
    const char *suid = NULL;
    const helper_backend_t *sb_ = backend_of(s, st->unit, &suid);
    int state = sb_ && sb_->unit_state ? sb_->unit_state(sb_->ctx, suid) : -1;
    if (state < 0)
        return resp_error(r->id, HELPER_E_FAILED, "cannot tell whether %s is running", st->unit);
    if (state == SP_STATE_RUNNING || state == SP_STATE_PAUSED)
        return resp_error(r->id, HELPER_E_BUSY, "%s is running; stop it first", st->unit);
    /* The staging directory again, and its base, which the swap may have
     * removed as empty. */
    char base[HELPER_PATH_MAX + 1], back[HELPER_PATH_MAX + 8];
    snprintf(base, sizeof(base), "%s", st->path);
    char *sl = strrchr(base, '/');
    if (sl && sl != base) *sl = '\0';
    pthread_mutex_lock(&s->mu);
    int bad = mkdir_owned(base, s->pol->daemon_uid) != 0 ||
              (st->file == 2 && mkdir_owned(st->path, s->pol->daemon_uid) != 0);
    int err = errno;
    pthread_mutex_unlock(&s->mu);
    if (bad) return resp_error(r->id, HELPER_E_FAILED, "cannot create %s: %s", st->path, strerror(err));
    snprintf(back, sizeof(back), st->file == 2 ? "%.4000s/image" : "%.4000s", st->path);
    struct stat sb;
    if (lstat(back, &sb) == 0) return resp_error(r->id, HELPER_E_BUSY, "%.300s already exists", back);
    if (rename(st->mount, back) != 0)
        return resp_error(r->id, HELPER_E_FAILED, "move the restored copy back: %s", strerror(errno));
    if (st->had && rename(st->old, st->mount) != 0) {
        err = errno;
        (void)rename(back, st->mount);
        return resp_error(r->id, HELPER_E_FAILED, "put %.300s back: %s", st->old, strerror(err));
    }
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) s->stages[i].used = s->stages[i].swapped = 0;
    pthread_mutex_unlock(&s->mu);
    cJSON *res = cJSON_CreateObject();
    if (res) {
        cJSON_AddStringToObject(res, "mount", st->mount);
        cJSON_AddStringToObject(res, "restored_copy", back);
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
    if (r->undo) return swap_undo(s, r, &st);
    if (st.swapped) return resp_error(r->id, HELPER_E_INVALID, "already swapped");
    if (st.file == 3) return resp_error(r->id, HELPER_E_INVALID, "this staging is for vm.install_state");
    char sdir[HELPER_PATH_MAX + 1];
    snprintf(sdir, sizeof(sdir), "%s", st.path);
    /* A disk image is replaced by the image the daemon wrote in staging. */
    if (st.file == 2) {
        char img[PATH_MAX + 8];
        snprintf(img, sizeof(img), "%.4000s/image", st.path);
        snprintf(st.path, sizeof(st.path), "%.4090s", img);
    }
    const char *suid = NULL;
    const helper_backend_t *sb_ = backend_of(s, st.unit, &suid);
    int state = sb_ && sb_->unit_state ? sb_->unit_state(sb_->ctx, suid) : -1;
    if (state < 0)
        return resp_error(r->id, HELPER_E_FAILED, "cannot tell whether %s is running", st.unit);
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
        if (had && rename(old, st.mount) != 0) {
            int e2 = errno;
            log_error("helper: swap of %s failed and the original could not be put back; "
                      "it is at %s: %s", st.mount, old, strerror(e2));
            return resp_error(r->id, HELPER_E_FAILED, "swap in: %s; the original could not be "
                              "put back and is at %.300s (%s)", strerror(e), old, strerror(e2));
        }
        return resp_error(r->id, HELPER_E_FAILED, "swap in: %s", strerror(e));
    }
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) {
            s->stages[i].swapped = 1;
            s->stages[i].had = had;
            snprintf(s->stages[i].old, sizeof(s->stages[i].old), "%s", old);
        }
    pthread_mutex_unlock(&s->mu);
    staging_tidy(s, sdir, st.file);
    /* Durable before it is reported: a power cut must not undo a swap the
     * daemon was told happened. The staging is tidied first either way. */
    {
        char parent[PATH_MAX];
        snprintf(parent, sizeof(parent), "%.4000s", st.mount);
        char *sl = strrchr(parent, '/');
        if (sl && sl != parent) *sl = '\0';
        else if (sl) sl[1] = '\0';
        int dfd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        int ok = dfd >= 0 && fsync(dfd) == 0;
        int e = errno;
        if (dfd >= 0) close(dfd);
        if (!ok)
            return resp_error(r->id, HELPER_E_FAILED, "swapped in, but %.300s could not be "
                              "synced, so the swap may not survive a power cut: %s%s%.300s",
                              parent, strerror(e), had ? "; the original is at " : "",
                              had ? old : "");
    }
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
    const helper_backend_t *b = NULL;
    if ((e = backend_for(s, r->unit, &uid, &b, why, sizeof(why))) != HELPER_OK)
        return resp_error(r->id, e, "%s", why);
    if (!b->set_running)
        return resp_error(r->id, HELPER_E_UNSUPPORTED, "%s cannot be stopped or started here", r->unit);
    /* What it was before, so a restore that fails can put that back. */
    int was = b->unit_state ? b->unit_state(b->ctx, uid) : -1;
    if (b->set_running(b->ctx, uid, start) != OK)
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
    cJSON *res = cJSON_CreateObject();
    if (res) cJSON_AddStringToObject(res, "state", start ? "running" : "stopped");
    if (res && was >= 0) cJSON_AddBoolToObject(res, "was_running", was == SP_STATE_RUNNING || was == SP_STATE_PAUSED);
    return resp_ok(r->id, res);
}

/* The helper parses a definition field by field and serialises its own;
 * none of the daemon's bytes would pass through. No backend here defines
 * units yet, so after the policy's checks the answer is unsupported. */
/* A definition passed as a sealed memfd with the request ({"memfd": true}
 * in place of the object): no 64 KiB limit, and bytes that cannot change
 * while the helper reads them. */
static char *read_sealed(int fd, size_t max, size_t *len, char *why, size_t wsz)
{
    struct stat sb;
    int seals = fcntl(fd, F_GET_SEALS);
    int need = F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE;
    if (fstat(fd, &sb) != 0 || seals < 0 || (seals & need) != need) {
        snprintf(why, wsz, "the definition must be a sealed memfd");
        return NULL;
    }
    if ((uint64_t)sb.st_size > max) { snprintf(why, wsz, "the definition is over %zu bytes", max); return NULL; }
    char *buf = malloc((size_t)sb.st_size + 1);
    if (!buf) { snprintf(why, wsz, "out of memory"); return NULL; }
    size_t done = 0;
    while (done < (size_t)sb.st_size) {
        ssize_t n = pread(fd, buf + done, (size_t)sb.st_size - done, (off_t)done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { free(buf); snprintf(why, wsz, "cannot read the definition"); return NULL; }
        done += (size_t)n;
    }
    buf[done] = '\0';
    *len = done;
    return buf;
}

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
        if (!(passed = read_sealed(in_fd, HELPER_BLOB_MAX, &plen, fwhy, sizeof(fwhy))))
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
        free(passed);
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
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
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
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
    }
    pthread_mutex_lock(&s->mu);
    for (size_t i = 0; i < STAGINGS_MAX; i++)
        if (s->stages[i].used && strcmp(s->stages[i].token, r->token) == 0) s->stages[i].used = 0;
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
        return resp_error(r->id, HELPER_E_FAILED, "%.400s", err_msg());
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
    unsigned rel = release_holds(s, pick_unowned, s, 0, "sweep");
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
    /* What the helper holds now, for the daemon's holds_open (B169). */
    cJSON_AddNumberToObject(res, "holds", (double)helper_server_holds(s));
    cJSON_AddNumberToObject(res, "captures", (double)helper_server_captures(s));
    cJSON *p = cJSON_AddArrayToObject(res, "providers");
    for (size_t i = 0; p && i < s->nb; i++)
        cJSON_AddItemToArray(p, cJSON_CreateString(s->backends[i].name));
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
                              const char *session, char **resp, int *pass_fd)
{
    *pass_fd = -1;
    helper_req_t r;
    char why[512];
    helper_err_t e = helper_request_parse(req, len, &r, why, sizeof(why));
    snprintf(r.conn_session, sizeof(r.conn_session), "%s", session);
    *resp = e == HELPER_OK ? dispatch(s, &r, pass_fd, in_fd) : resp_error(r.id, e, "%s", why);
    helper_req_free(&r);
    err_clear();
}

void helper_server_handle_fd(helper_server_t *s, const char *req, size_t len, int in_fd,
                             char **resp, int *pass_fd)
{
    handle_in_session(s, req, len, in_fd, "", resp, pass_fd);
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
            handle_in_session(s, buf, got, rfd, session, &resp, &pfd);
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
    unsigned n = release_holds(s, pick_old, &a, 1, "watchdog");
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

    /* Whatever the last helper process held is released before anything
     * new is taken, and leftovers of this daemon id are destroyed. */
    journal_load(s);
    unsigned rel = release_holds(s, pick_all, NULL, 0, "start-up sweep");
    sp_sweep_report_t rep;
    sweep_artefacts(s, &rep);
    if (rel || rep.removed)
        log_warn("helper: start-up sweep released %u hold(s), removed %u artefact(s)", rel, rep.removed);
    pthread_mutex_lock(&s->jmu);
    if (journal_save(s) != OK) err_clear();   /* logged; holds are still in memory */
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
            if (s->runs[i].hr) {
                if (!s->runs[i].done) s->runs[i].b->hook_kill(s->runs[i].hr);
                s->runs[i].b->hook_free(s->runs[i].hr);
            } else if (!s->runs[i].done) {
                kill(s->runs[i].pid, SIGKILL);
                waitpid(s->runs[i].pid, NULL, 0);
            }
            close(s->runs[i].err_fd);
            hooklog_free(&s->runs[i].log);
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

status_t helper_server_record(helper_server_t *s, const char *unit, const char *kind,
                              const char *detail)
{
    jctx_t j = { s, "", 0 };
    snprintf(j.unit, sizeof(j.unit), "%s", unit);
    int64_t id = 0;
    return j_record(&j, kind, detail, &id);
}
