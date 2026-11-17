#define _GNU_SOURCE

#include "helper_proto.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

static const char *const k_err[] = {
    "ok", "denied", "invalid", "busy", "timeout", "failed", "unsupported",
};

const char *helper_err_name(helper_err_t e)
{
    return (unsigned)e < sizeof(k_err) / sizeof(k_err[0]) ? k_err[e] : "failed";
}

int helper_err_parse(const char *name, helper_err_t *out)
{
    for (unsigned i = 1; name && i < sizeof(k_err) / sizeof(k_err[0]); i++)
        if (strcmp(name, k_err[i]) == 0) { *out = (helper_err_t)i; return 0; }
    return -1;
}

static const char *const k_ops[HOP_COUNT] = {
    "hello", "units.list", "units.describe", "capture.begin", "capture.begin_paths",
    "capture.end", "hook.run", "hook.wait", "vm.backup_begin", "vm.backup_end",
    "vm.checkpoint_redefine", "vm.install_state", "restore.stage", "restore.swap",
    "unit.stop", "unit.start", "unit.define", "lease.release", "sweep",
};

const char *helper_op_name(helper_op_t op)
{
    return (unsigned)op < HOP_COUNT ? k_ops[op] : "?";
}

int helper_provider_known(const char *p)
{
    return p && (strcmp(p, "libvirt") == 0 || strcmp(p, "docker") == 0 ||
                 strcmp(p, "lxc") == 0 || strcmp(p, "fake") == 0);
}

/* ------------------------------------------------------------------ */
/* Strings                                                             */
/* ------------------------------------------------------------------ */

/* Valid UTF-8 (no overlongs, no surrogates), no control character, at most
 * max bytes. */
static int str_ok(const char *s, size_t max)
{
    const unsigned char *p = (const unsigned char *)s;
    size_t n = 0;
    while (*p) {
        unsigned c = *p;
        size_t len;
        unsigned cp;
        if (c < 0x80) { len = 1; cp = c; }
        else if ((c & 0xE0) == 0xC0) { len = 2; cp = c & 0x1F; }
        else if ((c & 0xF0) == 0xE0) { len = 3; cp = c & 0x0F; }
        else if ((c & 0xF8) == 0xF0) { len = 4; cp = c & 0x07; }
        else return 0;
        for (size_t k = 1; k < len; k++) {
            if ((p[k] & 0xC0) != 0x80) return 0;
            cp = (cp << 6) | (p[k] & 0x3F);
        }
        if ((len == 2 && cp < 0x80) || (len == 3 && cp < 0x800) ||
            (len == 4 && (cp < 0x10000 || cp > 0x10FFFF)) ||
            (cp >= 0xD800 && cp <= 0xDFFF))
            return 0;
        if (cp < 0x20 || cp == 0x7F || (cp >= 0x80 && cp < 0xA0)) return 0;
        p += len;
        n += len;
        if (n > max) return 0;
    }
    return 1;
}

/* An identifier: [A-Za-z0-9][A-Za-z0-9_.:-]{0,max-1}. */
static int ident_ok(const char *s, size_t max, const char *extra)
{
    size_t n = strlen(s);
    if (n == 0 || n > max) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        int alnum = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
        if (alnum) continue;
        if (i == 0 || !strchr(extra, c)) return 0;
    }
    return 1;
}

static int uuid_ok(const char *s)
{
    if (strlen(s) != 36) return 0;
    for (int i = 0; i < 36; i++) {
        char c = s[i];
        if (i == 8 || i == 13 || i == 18 || i == 23) { if (c != '-') return 0; continue; }
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return 0;
    }
    return 1;
}

int helper_unit_key_ok(const char *key, char *provider, size_t psz, char *id, size_t isz)
{
    if (!key || !str_ok(key, HELPER_UNIT_KEY_MAX)) return 0;
    const char *colon = strchr(key, ':');
    if (!colon || colon == key) return 0;
    char prov[16];
    size_t pl = (size_t)(colon - key);
    if (pl >= sizeof(prov)) return 0;
    memcpy(prov, key, pl);
    prov[pl] = '\0';
    const char *u = colon + 1;
    int ok = 0;
    if (strcmp(prov, "docker") == 0) {
        if (strncmp(u, "compose:", 8) == 0) {
            const char *pj = u + 8;
            ok = strlen(pj) <= 64 && pj[0] &&
                 ((pj[0] >= 'a' && pj[0] <= 'z') || (pj[0] >= '0' && pj[0] <= '9'));
            for (const char *c = pj; ok && *c; c++)
                ok = (*c >= 'a' && *c <= 'z') || (*c >= '0' && *c <= '9') || *c == '_' || *c == '-';
        } else {
            ok = ident_ok(u, 128, "_.-");
        }
    } else if (strcmp(prov, "libvirt") == 0) {
        ok = uuid_ok(u);
    } else if (strcmp(prov, "lxc") == 0) {
        ok = ident_ok(u, 64, "_.-");
    } else if (strcmp(prov, "fake") == 0) {
        ok = ident_ok(u, 127, "_.:-");
    }
    if (!ok) return 0;
    if (provider) snprintf(provider, psz, "%s", prov);
    if (id) snprintf(id, isz, "%s", u);
    return 1;
}

int helper_path_clean(const char *p)
{
    if (!p || p[0] != '/' || !str_ok(p, HELPER_PATH_MAX)) return 0;
    if (p[1] == '\0') return 1;
    if (strstr(p, "//") || strstr(p, "/./") || strstr(p, "/../")) return 0;
    size_t n = strlen(p);
    if (p[n - 1] == '/') return 0;
    if (n >= 2 && strcmp(p + n - 2, "/.") == 0) return 0;
    if (n >= 3 && strcmp(p + n - 3, "/..") == 0) return 0;
    return 1;
}

static int token_ok(const char *s)
{
    if (strlen(s) != HELPER_TOKEN_HEX) return 0;
    for (int i = 0; i < HELPER_TOKEN_HEX; i++)
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return 0;
    return 1;
}

/* ------------------------------------------------------------------ */
/* Schema                                                              */
/* ------------------------------------------------------------------ */

typedef enum {
    A_INT, A_BOOL, A_PROVIDER, A_UNIT, A_UNITS, A_PATHS, A_TOKEN, A_NAME,
    A_DISKS, A_NAME_OR_NULL, A_FREEZE, A_OUTCOME, A_PATH, A_JOB, A_OBJECT,
} akind_t;

typedef struct { const char *key; akind_t kind; } akey_t;

typedef struct {
    akey_t keys[4];
    int    n;
    int    one_of;      /* exactly one of the keys (lease.release) */
    int    n_opt;       /* the last n_opt keys may be left out */
} schema_t;

static const schema_t k_schema[HOP_COUNT] = {
    [HOP_HELLO]                  = { { { "client_version", A_INT } }, 1, 0 },
    [HOP_UNITS_LIST]             = { { { "provider", A_PROVIDER } }, 1, 0 },
    [HOP_UNITS_DESCRIBE]         = { { { "unit", A_UNIT } }, 1, 0 },
    [HOP_CAPTURE_BEGIN]          = { { { "units", A_UNITS }, { "pause", A_BOOL }, { "hold", A_BOOL } }, 3, 0, 1 },
    [HOP_CAPTURE_BEGIN_PATHS]    = { { { "paths", A_PATHS } }, 1, 0 },
    [HOP_CAPTURE_END]            = { { { "capture", A_TOKEN } }, 1, 0 },
    [HOP_HOOK_RUN]               = { { { "unit", A_UNIT }, { "hook", A_NAME } }, 2, 0 },
    [HOP_HOOK_WAIT]              = { { { "run", A_TOKEN } }, 1, 0 },
    [HOP_VM_BACKUP_BEGIN]        = { { { "unit", A_UNIT }, { "disks", A_DISKS },
                                       { "base", A_NAME_OR_NULL }, { "freeze", A_FREEZE } }, 4, 0 },
    [HOP_VM_BACKUP_END]          = { { { "session", A_TOKEN }, { "outcome", A_OUTCOME } }, 2, 0 },
    [HOP_VM_CHECKPOINT_REDEFINE] = { { { "unit", A_UNIT }, { "checkpoint", A_NAME } }, 2, 0 },
    [HOP_VM_INSTALL_STATE]       = { { { "unit", A_UNIT }, { "staging", A_TOKEN } }, 2, 0 },
    [HOP_RESTORE_STAGE]          = { { { "unit", A_UNIT }, { "mount", A_PATH }, { "job", A_JOB } }, 3, 0 },
    [HOP_RESTORE_SWAP]           = { { { "staging", A_TOKEN } }, 1, 0 },
    [HOP_UNIT_STOP]              = { { { "unit", A_UNIT } }, 1, 0 },
    [HOP_UNIT_START]             = { { { "unit", A_UNIT } }, 1, 0 },
    [HOP_UNIT_DEFINE]            = { { { "unit", A_UNIT }, { "definition", A_OBJECT } }, 2, 0 },
    [HOP_LEASE_RELEASE]          = { { { "capture", A_TOKEN }, { "session", A_TOKEN } }, 2, 1 },
    [HOP_SWEEP]                  = { { { NULL, A_INT } }, 0, 0 },
};

static int name_ok(const char *s) { return str_ok(s, HELPER_NAME_MAX) && ident_ok(s, HELPER_NAME_MAX, "_.:-"); }

static int json_int(const cJSON *v, int64_t lo, int64_t hi, int64_t *out)
{
    if (!cJSON_IsNumber(v)) return 0;
    double d = v->valuedouble;
    if (d != (double)(int64_t)d || d < (double)lo || d > (double)hi) return 0;
    *out = (int64_t)d;
    return 1;
}

static int set_bad(char *why, size_t wsz, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
static int set_bad(char *why, size_t wsz, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(why, wsz, fmt, ap);
    va_end(ap);
    return -1;
}

/* One argument into req. 0, or -1 with why. */
static int take_arg(helper_req_t *r, const akey_t *k, const cJSON *v, char *why, size_t wsz)
{
    const char *op = helper_op_name(r->op);
    switch (k->kind) {
    case A_INT: {
        int64_t x = 0;
        if (!json_int(v, 0, 1000000, &x)) return set_bad(why, wsz, "%s: %s must be an integer from 0 to 1000000", op, k->key);
        r->client_version = x;
        return 0;
    }
    case A_JOB: {
        int64_t x = 0;
        if (!json_int(v, 1, (int64_t)1 << 53, &x)) return set_bad(why, wsz, "%s: %s must be a positive integer", op, k->key);
        r->job = x;
        return 0;
    }
    case A_BOOL:
        if (!cJSON_IsBool(v)) return set_bad(why, wsz, "%s: %s must be true or false", op, k->key);
        if (strcmp(k->key, "hold") == 0) r->hold = cJSON_IsTrue(v);
        else r->pause = cJSON_IsTrue(v);
        return 0;
    case A_PROVIDER:
        if (!cJSON_IsString(v) || !helper_provider_known(v->valuestring))
            return set_bad(why, wsz, "%s: %s must be one of " HELPER_PROVIDERS, op, k->key);
        snprintf(r->provider, sizeof(r->provider), "%s", v->valuestring);
        return 0;
    case A_UNIT:
        if (!cJSON_IsString(v) || !helper_unit_key_ok(v->valuestring, NULL, 0, NULL, 0))
            return set_bad(why, wsz, "%s: %s is not a well-formed unit key", op, k->key);
        snprintf(r->unit, sizeof(r->unit), "%s", v->valuestring);
        return 0;
    case A_UNITS: {
        int n = cJSON_IsArray(v) ? cJSON_GetArraySize(v) : -1;
        if (n < 1 || n > HELPER_UNITS_MAX)
            return set_bad(why, wsz, "%s: %s must list 1 to 32 unit keys", op, k->key);
        const cJSON *e;
        cJSON_ArrayForEach(e, v) {
            if (!cJSON_IsString(e) || !helper_unit_key_ok(e->valuestring, NULL, 0, NULL, 0))
                return set_bad(why, wsz, "%s: %s holds a malformed unit key", op, k->key);
            for (size_t i = 0; i < r->n_units; i++)
                if (strcmp(r->units[i], e->valuestring) == 0)
                    return set_bad(why, wsz, "%s: %s names a unit twice", op, k->key);
            snprintf(r->units[r->n_units++], HELPER_UNIT_KEY_MAX + 1, "%s", e->valuestring);
        }
        return 0;
    }
    case A_PATHS: {
        int n = cJSON_IsArray(v) ? cJSON_GetArraySize(v) : -1;
        if (n < 1 || n > HELPER_PATHS_MAX)
            return set_bad(why, wsz, "%s: %s must list 1 to 64 absolute paths", op, k->key);
        const cJSON *e;
        cJSON_ArrayForEach(e, v) {
            if (!cJSON_IsString(e) || !helper_path_clean(e->valuestring))
                return set_bad(why, wsz, "%s: %s holds a path that is not clean and absolute", op, k->key);
            if (!(r->paths[r->n_paths] = strdup(e->valuestring)))
                return set_bad(why, wsz, "%s: out of memory%s", op, "");
            r->n_paths++;
        }
        return 0;
    }
    case A_TOKEN:
        if (!cJSON_IsString(v) || !token_ok(v->valuestring))
            return set_bad(why, wsz, "%s: %s is not a token", op, k->key);
        snprintf(r->token, sizeof(r->token), "%s", v->valuestring);
        return 0;
    case A_NAME: {
        if (!cJSON_IsString(v) || !name_ok(v->valuestring))
            return set_bad(why, wsz, "%s: %s is not a valid name", op, k->key);
        char *dst = r->op == HOP_HOOK_RUN ? r->hook : r->checkpoint;
        snprintf(dst, HELPER_NAME_MAX + 1, "%s", v->valuestring);
        return 0;
    }
    case A_NAME_OR_NULL:
        if (cJSON_IsNull(v)) { r->base[0] = '\0'; return 0; }
        if (!cJSON_IsString(v) || !name_ok(v->valuestring))
            return set_bad(why, wsz, "%s: %s must be a checkpoint name or null", op, k->key);
        snprintf(r->base, sizeof(r->base), "%s", v->valuestring);
        return 0;
    case A_FREEZE:
        if (!cJSON_IsString(v) || (strcmp(v->valuestring, "auto") != 0 &&
                                   strcmp(v->valuestring, "off") != 0 &&
                                   strcmp(v->valuestring, "required") != 0))
            return set_bad(why, wsz, "%s: %s must be auto, off or required", op, k->key);
        snprintf(r->freeze, sizeof(r->freeze), "%s", v->valuestring);
        return 0;
    case A_OUTCOME:
        if (!cJSON_IsString(v) || (strcmp(v->valuestring, "committed") != 0 &&
                                   strcmp(v->valuestring, "aborted") != 0))
            return set_bad(why, wsz, "%s: %s must be committed or aborted", op, k->key);
        snprintf(r->outcome, sizeof(r->outcome), "%s", v->valuestring);
        return 0;
    case A_PATH:
        if (!cJSON_IsString(v) || !helper_path_clean(v->valuestring) || strcmp(v->valuestring, "/") == 0)
            return set_bad(why, wsz, "%s: %s must be a clean absolute path", op, k->key);
        snprintf(r->mount, sizeof(r->mount), "%s", v->valuestring);
        return 0;
    case A_DISKS: {
        int n = cJSON_IsArray(v) ? cJSON_GetArraySize(v) : -1;
        if (n < 1 || n > HELPER_DISKS_MAX)
            return set_bad(why, wsz, "%s: %s must list 1 to 32 disks", op, k->key);
        const cJSON *e;
        cJSON_ArrayForEach(e, v) {
            const cJSON *t = cJSON_GetObjectItemCaseSensitive(e, "target");
            const cJSON *m = cJSON_GetObjectItemCaseSensitive(e, "mode");
            if (!cJSON_IsObject(e) || cJSON_GetArraySize(e) != 2 || !cJSON_IsString(t) ||
                !name_ok(t->valuestring) || !cJSON_IsString(m) ||
                (strcmp(m->valuestring, "full") != 0 && strcmp(m->valuestring, "incremental") != 0))
                return set_bad(why, wsz, "%s: %s entries are {target, mode full|incremental}", op, k->key);
            helper_disk_arg_t *d = &r->disks[r->n_disks++];
            snprintf(d->target, sizeof(d->target), "%s", t->valuestring);
            d->incremental = strcmp(m->valuestring, "incremental") == 0;
        }
        return 0;
    }
    case A_OBJECT: {
        if (!cJSON_IsObject(v)) return set_bad(why, wsz, "%s: %s must be an object", op, k->key);
        char *t = cJSON_PrintUnformatted(v);
        if (!t) return set_bad(why, wsz, "%s: out of memory%s", op, "");
        r->definition = strdup(t);
        cJSON_free(t);
        return r->definition ? 0 : set_bad(why, wsz, "%s: out of memory%s", op, "");
    }
    }
    return set_bad(why, wsz, "%s: %s has no schema", op, k->key);
}

void helper_req_free(helper_req_t *req)
{
    if (!req) return;
    for (size_t i = 0; i < req->n_paths; i++) free(req->paths[i]);
    free(req->definition);
    req->n_paths = 0;
    req->definition = NULL;
}

helper_err_t helper_request_parse(const void *buf, size_t len, helper_req_t *req,
                                  char *why, size_t wsz)
{
    memset(req, 0, sizeof(*req));
    why[0] = '\0';
    if (!buf || len == 0 || len > HELPER_PACKET_MAX) {
        snprintf(why, wsz, "a request is one JSON object of 1 to 65536 bytes");
        return HELPER_E_INVALID;
    }
    if (memchr(buf, '\0', len)) {
        snprintf(why, wsz, "a request may not hold a NUL byte");
        return HELPER_E_INVALID;
    }
    cJSON *o = cJSON_ParseWithLength(buf, len);
    if (!o || !cJSON_IsObject(o)) {
        cJSON_Delete(o);
        snprintf(why, wsz, "a request is one JSON object");
        return HELPER_E_INVALID;
    }
    helper_err_t e = HELPER_E_INVALID;
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(o, "v");
    const cJSON *id = cJSON_GetObjectItemCaseSensitive(o, "id");
    const cJSON *op = cJSON_GetObjectItemCaseSensitive(o, "op");
    const cJSON *args = cJSON_GetObjectItemCaseSensitive(o, "args");
    int64_t x = 0;
    if (json_int(id, 0, (int64_t)1 << 53, &x)) req->id = x;
    if (cJSON_GetArraySize(o) != 4 || !v || !id || !op || !args) {
        snprintf(why, wsz, "a request has exactly the keys v, id, op and args");
        goto out;
    }
    if (!json_int(v, HELPER_PROTO_VERSION, HELPER_PROTO_VERSION, &x)) {
        snprintf(why, wsz, "protocol version must be %d", HELPER_PROTO_VERSION);
        goto out;
    }
    if (!json_int(id, 0, (int64_t)1 << 53, &x)) {
        snprintf(why, wsz, "id must be a non-negative integer");
        goto out;
    }
    if (!cJSON_IsString(op) || !str_ok(op->valuestring, 64)) {
        snprintf(why, wsz, "op must be a string");
        goto out;
    }
    int found = -1;
    for (int i = 0; i < HOP_COUNT; i++)
        if (strcmp(op->valuestring, k_ops[i]) == 0) found = i;
    if (found < 0) {
        /* Not an operation of 1.0: there is nothing to run it with. */
        snprintf(why, wsz, "operation '%.60s' does not exist", op->valuestring);
        e = HELPER_E_DENIED;
        goto out;
    }
    req->op = (helper_op_t)found;
    if (!cJSON_IsObject(args)) {
        snprintf(why, wsz, "%s: args must be an object", k_ops[found]);
        goto out;
    }
    const schema_t *s = &k_schema[found];
    int present = 0;
    const cJSON *a;
    cJSON_ArrayForEach(a, args) {
        int known = 0;
        for (int k = 0; k < s->n; k++) {
            if (!a->string || strcmp(a->string, s->keys[k].key) != 0) continue;
            known = 1;
            if (take_arg(req, &s->keys[k], a, why, wsz) != 0) goto out;
            present++;
        }
        if (!known) {
            snprintf(why, wsz, "%s: unknown argument '%.60s'", k_ops[found], a->string ? a->string : "");
            goto out;
        }
    }
    if (cJSON_GetArraySize(args) != present ||
        (s->one_of ? present != 1 : (present < s->n - s->n_opt || present > s->n))) {
        snprintf(why, wsz, "%s: wrong arguments; it takes %s", k_ops[found],
                 s->one_of ? "exactly one of its token keys" : "exactly its listed keys");
        goto out;
    }
    e = HELPER_OK;
out:
    cJSON_Delete(o);
    return e;
}

/* ------------------------------------------------------------------ */
/* Transport                                                           */
/* ------------------------------------------------------------------ */

status_t helper_send(int fd, const void *buf, size_t len, int pass_fd)
{
    if (len > HELPER_PACKET_MAX) return set_error(ERR_TOO_LARGE, "helper: packet of %zu bytes", len);
    struct iovec iov = { (void *)(uintptr_t)buf, len };
    struct msghdr m;
    memset(&m, 0, sizeof(m));
    m.msg_iov = &iov;
    m.msg_iovlen = 1;
    union { char b[CMSG_SPACE(sizeof(int))]; struct cmsghdr align; } u;
    if (pass_fd >= 0) {
        memset(&u, 0, sizeof(u));
        m.msg_control = u.b;
        m.msg_controllen = sizeof(u.b);
        struct cmsghdr *c = CMSG_FIRSTHDR(&m);
        c->cmsg_level = SOL_SOCKET;
        c->cmsg_type = SCM_RIGHTS;
        c->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(c), &pass_fd, sizeof(int));
    }
    for (;;) {
        ssize_t w = sendmsg(fd, &m, MSG_NOSIGNAL);
        if (w == (ssize_t)len) return OK;
        if (w < 0 && errno == EINTR) continue;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return set_error(ERR_BUSY, "helper: send timed out");
        return set_error_errno(ERR_IO, "helper: send");
    }
}

status_t helper_recv(int fd, char *buf, size_t cap, size_t *got, int *recv_fd)
{
    if (recv_fd) *recv_fd = -1;
    struct iovec iov = { buf, cap - 1 };
    struct msghdr m;
    memset(&m, 0, sizeof(m));
    m.msg_iov = &iov;
    m.msg_iovlen = 1;
    union { char b[CMSG_SPACE(sizeof(int))]; struct cmsghdr align; } u;
    memset(&u, 0, sizeof(u));
    m.msg_control = u.b;
    m.msg_controllen = sizeof(u.b);
    ssize_t r;
    do { r = recvmsg(fd, &m, MSG_CMSG_CLOEXEC); } while (r < 0 && errno == EINTR);
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return set_error(ERR_BUSY, "helper: no answer in time");
    if (r < 0) return set_error_errno(ERR_IO, "helper: receive");
    for (struct cmsghdr *c = CMSG_FIRSTHDR(&m); c; c = CMSG_NXTHDR(&m, c)) {
        if (c->cmsg_level != SOL_SOCKET || c->cmsg_type != SCM_RIGHTS) continue;
        int pfd = -1;
        memcpy(&pfd, CMSG_DATA(c), sizeof(int));
        if (recv_fd && *recv_fd < 0) *recv_fd = pfd;
        else close(pfd);
    }
    if (r == 0) return set_error(ERR_NOT_FOUND, "helper: connection closed");
    if (m.msg_flags & MSG_TRUNC) {
        if (recv_fd && *recv_fd >= 0) { close(*recv_fd); *recv_fd = -1; }
        return set_error(ERR_TOO_LARGE, "helper: packet over %zu bytes", cap - 1);
    }
    buf[r] = '\0';
    *got = (size_t)r;
    return OK;
}

status_t helper_random_hex(char *out, size_t nbytes)
{
    uint8_t raw[64];
    if (nbytes > sizeof(raw)) return set_error(ERR_INVALID, "helper: token too long");
    if (getrandom(raw, nbytes, 0) != (ssize_t)nbytes)
        return set_error_errno(ERR_IO, "helper: random");
    helper_hex_encode(raw, nbytes, out);
    return OK;
}

void helper_hex_encode(const uint8_t *in, size_t n, char *out)
{
    static const char d[] = "0123456789abcdef";
    for (size_t i = 0; i < n; i++) {
        out[2 * i] = d[in[i] >> 4];
        out[2 * i + 1] = d[in[i] & 15];
    }
    out[2 * n] = '\0';
}

static int hexval(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

long helper_hex_decode(const char *in, uint8_t *out, size_t cap)
{
    size_t n = strlen(in);
    if (n % 2 || n / 2 > cap) return -1;
    for (size_t i = 0; i < n / 2; i++) {
        int a = hexval(in[2 * i]), b = hexval(in[2 * i + 1]);
        if (a < 0 || b < 0) return -1;
        out[i] = (uint8_t)(a << 4 | b);
    }
    return (long)(n / 2);
}
