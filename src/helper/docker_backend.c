#define _GNU_SOURCE

#include "docker_backend.h"
#include "docker_api.h"
#include "helper_proto.h"
#include "hooklog.h"
#include "mounts.h"
#include "snap.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MEMBERS_MAX   32
#define HOOK_TAIL     2048
#define CFG_DIR       ".cbackup-unit/config"

typedef struct {
    char id[80];
    char name[129];
    int  running, paused;
    int  never;                /* never paused: our own containers, never_pause */
} member_t;

typedef struct {
    char   unit[SP_UNIT_ID_MAX];
    char   item[SP_ITEM_ID_MAX];
    char  *data;
    size_t len;
} blob_t;

struct sp_ctx {
    const helper_policy_t *pol;
    docker_api_t api;
    host_tools_t tools;
    mount_cfg_t  mounts;
    snap_ctx_t   snap;
    pthread_mutex_t mu;        /* the blob cache */
    blob_t      *blobs;
    size_t       nblobs;
};

typedef struct {
    char id[SP_ITEM_ID_MAX];
    char real[PATH_MAX];
    int  live;                 /* no snapshot domain: read in place */
} sitem_t;

struct sp_session {
    struct sp_ctx    *c;
    char              unit[SP_UNIT_ID_MAX];
    sp_holds_t        holds;   /* copied: begin's is the caller's stack */
    int               has_holds;
    snap_set_t        snaps;
    sitem_t          *items;
    size_t            n_items;
    member_t          members[MEMBERS_MAX];
    size_t            n_members;
    sp_session_info_t info;
    struct { char name[65]; char *text; size_t len; } logs[POLICY_HOOKS_MAX];
    size_t            n_logs;
    struct { char item[SP_ITEM_ID_MAX]; helper_run_t *run; int err_fd; } streams[4];
    size_t            n_streams;
};

static void keep_log(struct sp_session *s, const char *name, char *text, size_t len)
{
    for (size_t i = 0; i < s->n_logs; i++)
        if (strcmp(s->logs[i].name, name) == 0) { free(s->logs[i].text); s->logs[i].text = text; s->logs[i].len = len; return; }
    if (s->n_logs >= POLICY_HOOKS_MAX) { free(text); return; }
    snprintf(s->logs[s->n_logs].name, sizeof(s->logs[s->n_logs].name), "%s", name);
    s->logs[s->n_logs].text = text;
    s->logs[s->n_logs].len = len;
    s->n_logs++;
}

static void d_hook_kill(helper_run_t *r);
static void d_hook_free(helper_run_t *r);

/* ------------------------------------------------------------------ */
/* Members                                                             */
/* ------------------------------------------------------------------ */

static const char *jstr(const cJSON *o, const char *k)
{
    const char *s = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, k));
    return s ? s : "";
}

static const char *label(const cJSON *c, const char *k)
{
    return jstr(cJSON_GetObjectItemCaseSensitive(c, "Labels"), k);
}

static int glob_any(char *const *g, size_t n, const char *s)
{
    for (size_t i = 0; i < n; i++) if (fnmatch(g[i], s, 0) == 0) return 1;
    return 0;
}

/* A container's unit id: its compose project, or its own name. */
static void unit_of(const cJSON *c, char *buf, size_t sz)
{
    const char *proj = label(c, "com.docker.compose.project");
    const char *nm = cJSON_GetStringValue(cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(c, "Names"), 0));
    if (proj[0]) snprintf(buf, sz, "compose:%s", proj);
    else snprintf(buf, sz, "%s", nm ? (nm[0] == '/' ? nm + 1 : nm) : "");
}

static status_t list_all(struct sp_ctx *c, cJSON **out)
{
    return docker_call(&c->api, "GET", "/containers/json?all=1", NULL, 0, NULL, out);
}

static status_t members_of(struct sp_ctx *c, const char *unit_id, member_t *m, size_t *n)
{
    *n = 0;
    cJSON *all = NULL;
    status_t st = list_all(c, &all);
    if (st != OK) return st;
    const cJSON *x;
    cJSON_ArrayForEach(x, all) {
        char u[SP_UNIT_ID_MAX];
        unit_of(x, u, sizeof(u));
        if (strcmp(u, unit_id) != 0) continue;
        if (*n >= MEMBERS_MAX) { cJSON_Delete(all); return set_error(ERR_INVALID, "docker: %s has more than %d containers", unit_id, MEMBERS_MAX); }
        member_t *e = &m[(*n)++];
        memset(e, 0, sizeof(*e));
        snprintf(e->id, sizeof(e->id), "%s", jstr(x, "Id"));
        const char *nm = cJSON_GetStringValue(cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(x, "Names"), 0));
        snprintf(e->name, sizeof(e->name), "%s", nm ? (nm[0] == '/' ? nm + 1 : nm) : e->id);
        const char *state = jstr(x, "State");
        e->running = strcmp(state, "running") == 0 || strcmp(state, "paused") == 0;
        e->paused = strcmp(state, "paused") == 0;
        e->never = label(x, "net.cbackup.role")[0] != '\0' ||
                   glob_any(c->pol->never_pause, c->pol->n_never_pause, e->name);
    }
    cJSON_Delete(all);
    if (*n == 0) return set_error(ERR_NOT_FOUND, "docker: no container belongs to %s", unit_id);
    return OK;
}

static int unit_state(const member_t *m, size_t n)
{
    int run = 0, paused = 0;
    for (size_t i = 0; i < n; i++) { run |= m[i].running; paused |= m[i].paused; }
    return paused ? SP_STATE_PAUSED : run ? SP_STATE_RUNNING : SP_STATE_STOPPED;
}

/* ------------------------------------------------------------------ */
/* The vtable: probe, list, describe                                   */
/* ------------------------------------------------------------------ */

static status_t d_init(const sp_config_t *cfg, sp_ctx_t **out)
{
    (void)cfg; (void)out;
    return set_error(ERR_INVALID, "docker: built from the helper's policy, not initialised");
}

static void d_fini(sp_ctx_t *c) { (void)c; }

static status_t d_probe(sp_ctx_t *c, sp_probe_t *out)
{
    memset(out, 0, sizeof(*out));
    docker_api_t a = c->api;
    if (docker_negotiate(&a) != OK) {
        snprintf(out->detail, sizeof(out->detail), "%.250s", err_msg());
        err_clear();
        return OK;
    }
    out->available = 1;
    snprintf(out->version, sizeof(out->version), "Engine API %s", a.version);
    snprintf(out->detail, sizeof(out->detail), "docker at %s", c->api.socket);
    return OK;
}

static status_t d_list(sp_ctx_t *c, sp_unit_cb cb, void *cbctx)
{
    cJSON *all = NULL;
    status_t st = list_all(c, &all);
    if (st != OK) return st;
    /* One callback per unit, with the unit's state over its members. */
    size_t n = (size_t)cJSON_GetArraySize(all);
    char (*seen)[SP_UNIT_ID_MAX] = calloc(n ? n : 1, sizeof(*seen));
    if (!seen) { cJSON_Delete(all); return set_error(ERR_NOMEM, "docker: list"); }
    size_t ns = 0;
    const cJSON *x;
    cJSON_ArrayForEach(x, all) {
        if (st != OK) break;
        char u[SP_UNIT_ID_MAX];
        unit_of(x, u, sizeof(u));
        int dup = 0;
        for (size_t i = 0; i < ns && !dup; i++) dup = strcmp(seen[i], u) == 0;
        if (dup || !u[0]) continue;
        memcpy(seen[ns++], u, sizeof(u));
        int run = 0, paused = 0;
        const cJSON *y;
        cJSON_ArrayForEach(y, all) {
            char v[SP_UNIT_ID_MAX];
            unit_of(y, v, sizeof(v));
            if (strcmp(u, v) != 0) continue;
            run |= strcmp(jstr(y, "State"), "running") == 0;
            paused |= strcmp(jstr(y, "State"), "paused") == 0;
        }
        st = cb(cbctx, u, u, paused ? SP_STATE_PAUSED : run ? SP_STATE_RUNNING : SP_STATE_STOPPED);
    }
    free(seen);
    cJSON_Delete(all);
    return st;
}

static uint64_t fnv(const char *s)
{
    uint64_t h = 1469598103934665603ull;
    for (; *s; s++) { h ^= (unsigned char)*s; h *= 1099511628211ull; }
    return h;
}

/* A stable item id: readable when short, hashed when not. */
static void item_id(char *buf, size_t sz, const char *prefix, const char *s)
{
    if (strlen(prefix) + strlen(s) < 56 && !strchr(s, '/'))
        snprintf(buf, sz, "%s%s", prefix, s);
    else
        snprintf(buf, sz, "%s%016llx", prefix, (unsigned long long)fnv(s));
}

typedef struct { sp_unit_desc_t *d; size_t cap; } dbuild_t;

static status_t add_item(dbuild_t *b, sp_item_kind_t kind, const char *id, const char *tree_path,
                         const char *source, uint64_t size, uint32_t caps)
{
    for (size_t i = 0; i < b->d->n_items; i++)
        if (strcmp(b->d->items[i].id, id) == 0) return OK;
    if (b->d->n_items == b->cap) {
        size_t nc = b->cap ? b->cap * 2 : 16;
        sp_item_desc_t *g = realloc(b->d->items, nc * sizeof(*g));
        if (!g) return set_error(ERR_NOMEM, "docker: items");
        b->d->items = g;
        b->cap = nc;
    }
    sp_item_desc_t *it = &b->d->items[b->d->n_items];
    memset(it, 0, sizeof(*it));
    snprintf(it->id, sizeof(it->id), "%s", id);
    it->kind = kind;
    it->size = size;
    it->caps = caps;
    it->tree_path = strdup(tree_path);
    it->source = source ? strdup(source) : NULL;
    if (!it->tree_path || (source && !it->source)) {
        free(it->tree_path);
        free(it->source);
        return set_error(ERR_NOMEM, "docker: items");
    }
    b->d->n_items++;
    return OK;
}

/* Keep a blob's bytes for blob_data; describe and blob_data are one op. */
static status_t cache_blob(struct sp_ctx *c, const char *unit, const char *item, char *data, size_t len)
{
    pthread_mutex_lock(&c->mu);
    blob_t *g = realloc(c->blobs, (c->nblobs + 1) * sizeof(*g));
    if (!g) { pthread_mutex_unlock(&c->mu); free(data); return set_error(ERR_NOMEM, "docker: blob"); }
    c->blobs = g;
    blob_t *b = &c->blobs[c->nblobs++];
    snprintf(b->unit, sizeof(b->unit), "%s", unit);
    snprintf(b->item, sizeof(b->item), "%s", item);
    b->data = data;
    b->len = len;
    pthread_mutex_unlock(&c->mu);
    return OK;
}

static void drop_blobs(struct sp_ctx *c, const char *unit)
{
    pthread_mutex_lock(&c->mu);
    size_t k = 0;
    for (size_t i = 0; i < c->nblobs; i++) {
        if (strcmp(c->blobs[i].unit, unit) == 0) { free(c->blobs[i].data); continue; }
        c->blobs[k++] = c->blobs[i];
    }
    c->nblobs = k;
    pthread_mutex_unlock(&c->mu);
}

static status_t blob(dbuild_t *b, struct sp_ctx *c, const char *unit, const char *id, const char *tree_path,
                     char *data, size_t len)
{
    status_t st = add_item(b, SP_ITEM_BLOB, id, tree_path, NULL, len, 0);
    if (st != OK) { free(data); return st; }
    return cache_blob(c, unit, id, data, len);
}

/* A host file's bytes, or ERR_NOT_FOUND. */
static status_t host_file(struct sp_ctx *c, const char *path, char **data, size_t *len)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "docker: path too long");
    FILE *f = fopen(hp, "re");
    if (!f) return errno == ENOENT ? set_error(ERR_NOT_FOUND, "docker: %s is missing", path)
                                   : set_error_errno(ERR_IO, "docker: %s", path);
    char *buf = NULL;
    size_t n = 0, cap = 0;
    char chunk[8192];
    size_t r;
    while ((r = fread(chunk, 1, sizeof(chunk), f)) > 0) {
        if (n + r > SP_BLOB_MAX) { fclose(f); free(buf); return set_error(ERR_TOO_LARGE, "docker: %s is too large", path); }
        if (n + r + 1 > cap) {
            cap = (n + r + 1) * 2;
            char *g = realloc(buf, cap);
            if (!g) { fclose(f); free(buf); return set_error(ERR_NOMEM, "docker: %s", path); }
            buf = g;
        }
        memcpy(buf + n, chunk, r);
        n += r;
    }
    fclose(f);
    if (!buf && !(buf = calloc(1, 1))) return set_error(ERR_NOMEM, "docker: %s", path);
    *data = buf;
    *len = n;
    return OK;
}

/* Directories that are usually re-creatable, offered as exclusions and
 * never applied unless enrolled (4.2, C7). */
static void suggest(struct sp_ctx *c, const mount_class_t *mc, cJSON *out)
{
    static const char *const names[] = { "cache", "Cache", "ml_cache", "Codecs", "Transcode",
                                         "transcode", "logs", "log", "tmp" };
    for (size_t i = 0; out && i < sizeof(names) / sizeof(names[0]); i++) {
        char p[PATH_MAX + 32], hp[PATH_MAX + HOST_TOOL_PATH_MAX + 32];
        snprintf(p, sizeof(p), "%s/%s", mc->real, names[i]);
        struct stat sb;
        if (host_path(&c->tools, p, hp, sizeof(hp)) != 0 || lstat(hp, &sb) != 0 || !S_ISDIR(sb.st_mode)) continue;
        snprintf(p, sizeof(p), "%s/%s", mc->recorded, names[i]);
        cJSON_AddItemToArray(out, cJSON_CreateString(p));
    }
}

static status_t describe_member(struct sp_ctx *c, dbuild_t *b, const char *unit, const member_t *m,
                                cJSON *mounts_out, cJSON *suggest_out, int *compose_done)
{
    char path[256], id[SP_ITEM_ID_MAX], tp[PATH_MAX];
    snprintf(path, sizeof(path), "/containers/%.79s/json", m->id);
    char *raw = NULL;
    size_t rlen = 0;
    status_t st = docker_get_raw(&c->api, path, &raw, &rlen);
    if (st != OK) return st;
    cJSON *in = cJSON_ParseWithLength(raw, rlen);
    if (!in) { free(raw); return set_error(ERR_IO, "docker: inspect of %s is not JSON", m->name); }
    /* Verbatim: backup never mutates what it captures (4.1). */
    item_id(id, sizeof(id), "inspect:", m->name);
    snprintf(tp, sizeof(tp), CFG_DIR "/containers/%s/inspect.json", m->name);
    if ((st = blob(b, c, unit, id, tp, raw, rlen)) != OK) goto out;

    const char *image = jstr(in, "Image");
    if (image[0] && strlen(image) < 128 && !strchr(image, '/') && !strchr(image, '?')) {
        snprintf(path, sizeof(path), "/images/%s/json", image);
        cJSON *im = NULL;
        if (docker_call(&c->api, "GET", path, NULL, 0, NULL, &im) == OK) {
            cJSON *rec = cJSON_CreateObject();
            if (rec) {
                cJSON_AddStringToObject(rec, "Id", jstr(im, "Id"));
                cJSON *t = cJSON_GetObjectItemCaseSensitive(im, "RepoTags");
                cJSON *dg = cJSON_GetObjectItemCaseSensitive(im, "RepoDigests");
                cJSON_AddItemToObject(rec, "RepoTags", t ? cJSON_Duplicate(t, 1) : cJSON_CreateArray());
                cJSON_AddItemToObject(rec, "RepoDigests", dg ? cJSON_Duplicate(dg, 1) : cJSON_CreateArray());
                char *txt = cJSON_PrintUnformatted(rec);
                cJSON_Delete(rec);
                item_id(id, sizeof(id), "image:", m->name);
                snprintf(tp, sizeof(tp), CFG_DIR "/containers/%s/image.json", m->name);
                if (txt) st = blob(b, c, unit, id, tp, txt, strlen(txt));
            }
            cJSON_Delete(im);
        } else {
            /* The image may have been removed; the inspect still names it. */
            log_warn("docker: image of %s: %s", m->name, err_msg());
            err_clear();
        }
        if (st != OK) goto out;
    }

    /* The Unraid template, when the container came from one. */
    if (c->pol->templates_dir[0]) {
        char tf[PATH_MAX];
        snprintf(tf, sizeof(tf), "%.3900s/my-%.128s.xml", c->pol->templates_dir, m->name);
        char *data = NULL;
        size_t len = 0;
        st = host_file(c, tf, &data, &len);
        if (st == OK) {
            item_id(id, sizeof(id), "template:", m->name);
            snprintf(tp, sizeof(tp), CFG_DIR "/containers/%s/unraid-template.xml", m->name);
            st = blob(b, c, unit, id, tp, data, len);
        } else if (st == ERR_NOT_FOUND) {
            err_clear();
            st = OK;
        }
        if (st != OK) goto out;
    }

    /* The compose files, once per project. */
    const cJSON *cfg = cJSON_GetObjectItemCaseSensitive(in, "Config");
    const char *files = label(cfg, "com.docker.compose.project.config_files");
    const char *wd = label(cfg, "com.docker.compose.project.working_dir");
    if (!*compose_done && (files[0] || wd[0])) {
        *compose_done = 1;
        char list[4096];
        snprintf(list, sizeof(list), "%s", files);
        char *save = NULL;
        for (char *f = strtok_r(list, ",", &save); f && st == OK; f = strtok_r(NULL, ",", &save)) {
            if (f[0] != '/' || !helper_path_clean(f)) continue;
            const char *base = strrchr(f, '/') + 1;
            char *data = NULL;
            size_t len = 0;
            if ((st = host_file(c, f, &data, &len)) != OK) break;
            item_id(id, sizeof(id), "compose:", base);
            snprintf(tp, sizeof(tp), CFG_DIR "/compose/%s", base);
            st = blob(b, c, unit, id, tp, data, len);
        }
        if (st == OK && wd[0] == '/' && helper_path_clean(wd)) {
            char env[PATH_MAX];
            snprintf(env, sizeof(env), "%s/.env", wd);
            char *data = NULL;
            size_t len = 0;
            st = host_file(c, env, &data, &len);
            if (st == OK) st = blob(b, c, unit, "compose:.env", CFG_DIR "/compose/.env", data, len);
            else if (st == ERR_NOT_FOUND) { err_clear(); st = OK; }
        }
        if (st != OK) goto out;
    }

    /* Every mount, classified; the included ones are TREE items. */
    const cJSON *mnt;
    cJSON_ArrayForEach(mnt, cJSON_GetObjectItemCaseSensitive(in, "Mounts")) {
        mount_class_t mc;
        if ((st = mount_classify(&c->mounts, jstr(mnt, "Type"), jstr(mnt, "Source"), jstr(mnt, "Driver"), &mc)) != OK) {
            char why[400];
            snprintf(why, sizeof(why), "%.390s", err_msg());
            err_clear();
            st = set_error(ERR_NOT_FOUND, "docker: %s: %s", m->name, why);
            goto out;
        }
        char outcome[64];
        snap_dom_t dom;
        memset(&dom, 0, sizeof(dom));
        if (mc.verdict == MOUNT_EXCLUDED) {
            snprintf(outcome, sizeof(outcome), "excluded:%s", mc.reason);
        } else {
            if ((st = snap_domain_of(&c->snap, mc.real, &dom)) != OK) goto out;
            snprintf(outcome, sizeof(outcome), "%s", dom.kind != SNAP_NONE ? "snapshot" : "fallback-live");
            item_id(id, sizeof(id), "mnt:", mc.recorded);
            if ((st = add_item(b, SP_ITEM_TREE, id, mc.recorded, mc.real, 0,
                               dom.kind != SNAP_NONE ? SP_CAP_SNAPSHOT : 0)) != OK) goto out;
            suggest(c, &mc, suggest_out);
        }
        cJSON *o = cJSON_CreateObject();
        if (!o) { st = set_error(ERR_NOMEM, "docker: mounts"); goto out; }
        cJSON_AddItemToArray(mounts_out, o);
        cJSON_AddStringToObject(o, "container", m->name);
        cJSON_AddStringToObject(o, "destination", jstr(mnt, "Destination"));
        cJSON_AddStringToObject(o, "source", mc.recorded);
        cJSON_AddStringToObject(o, "real", mc.real);
        cJSON_AddStringToObject(o, "outcome", outcome);
        if (mc.reason[0] && mc.verdict == MOUNT_INCLUDED) cJSON_AddStringToObject(o, "note", mc.reason);
        if (dom.kind == SNAP_ZFS) cJSON_AddStringToObject(o, "domain", dom.dataset);
        else if (dom.kind == SNAP_BTRFS) cJSON_AddStringToObject(o, "domain", dom.root);
    }
out:
    cJSON_Delete(in);
    return st;
}

static status_t d_describe(sp_ctx_t *c, const char *unit_id, sp_unit_desc_t **out)
{
    member_t m[MEMBERS_MAX];
    size_t n = 0;
    status_t st = members_of(c, unit_id, m, &n);
    if (st != OK) return st;
    sp_unit_desc_t *d = calloc(1, sizeof(*d));
    if (!d) return set_error(ERR_NOMEM, "docker: describe");
    snprintf(d->provider, sizeof(d->provider), "docker");
    snprintf(d->unit_id, sizeof(d->unit_id), "%s", unit_id);
    d->name = strdup(unit_id);
    d->state = unit_state(m, n);
    d->caps = SP_CAP_PAUSE;
    dbuild_t b = { d, 0 };
    drop_blobs(c, unit_id);
    cJSON *doc = cJSON_CreateObject();
    cJSON *mounts = doc ? cJSON_AddArrayToObject(doc, "mounts") : NULL;
    cJSON *sugg = doc ? cJSON_AddArrayToObject(doc, "suggested_excludes") : NULL;
    int compose_done = 0;
    if (!d->name || !mounts) st = set_error(ERR_NOMEM, "docker: describe");
    for (size_t i = 0; st == OK && i < n; i++) st = describe_member(c, &b, unit_id, &m[i], mounts, sugg, &compose_done);
    /* The unit's hooks: a log for each, a STREAM for each dump (6.6). Dumps
     * come before their logs, so a dump's log is read after it ends. */
    char key[HELPER_UNIT_KEY_MAX + 1];
    snprintf(key, sizeof(key), "docker:%s", unit_id);
    for (size_t i = 0; st == OK && i < c->pol->n_hooks; i++) {
        const policy_hook_t *h = &c->pol->hooks[i];
        if (strcmp(h->unit, key) != 0) continue;
        char id[SP_ITEM_ID_MAX], tp[PATH_MAX], src[96];
        snprintf(src, sizeof(src), "hook:%s", h->name);
        if (h->dump) {
            item_id(id, sizeof(id), "dump:", h->name);
            snprintf(tp, sizeof(tp), ".cbackup-unit/hooks/%s.dump", h->name);
            if ((st = add_item(&b, SP_ITEM_STREAM, id, tp, src, 0, 0)) != OK) break;
        }
        item_id(id, sizeof(id), "log:", h->name);
        snprintf(tp, sizeof(tp), ".cbackup-unit/hooks/%s.log", h->name);
        char *empty = calloc(1, 1);
        st = empty ? add_item(&b, SP_ITEM_BLOB, id, tp, src, 0, 0) : set_error(ERR_NOMEM, "docker: hook log");
        if (st == OK) st = cache_blob(c, unit_id, id, empty, 0);
        else free(empty);
    }
    if (st == OK) {
        char *txt = cJSON_Print(doc);
        st = txt ? blob(&b, c, unit_id, "mounts", CFG_DIR "/mounts.json", txt, strlen(txt))
                 : set_error(ERR_NOMEM, "docker: mounts");
    }
    cJSON_Delete(doc);
    if (st != OK) { drop_blobs(c, unit_id); sp_unit_desc_free(d); return st; }
    *out = d;
    return OK;
}

static status_t d_blob_data(sp_ctx_t *c, const char *unit_id, const char *item_id_, void **data, size_t *len)
{
    status_t st = set_error(ERR_NOT_FOUND, "docker: no blob %s of %s", item_id_, unit_id);
    pthread_mutex_lock(&c->mu);
    for (size_t i = 0; i < c->nblobs; i++) {
        if (strcmp(c->blobs[i].unit, unit_id) != 0 || strcmp(c->blobs[i].item, item_id_) != 0) continue;
        void *cp = malloc(c->blobs[i].len ? c->blobs[i].len : 1);
        if (!cp) { st = set_error(ERR_NOMEM, "docker: blob"); break; }
        memcpy(cp, c->blobs[i].data, c->blobs[i].len);
        *data = cp;
        *len = c->blobs[i].len;
        err_clear();
        st = OK;
        break;
    }
    pthread_mutex_unlock(&c->mu);
    return st;
}

static const char *hook_of(const sp_item_desc_t *item)
{
    return item->source && strncmp(item->source, "hook:", 5) == 0 ? item->source + 5 : NULL;
}

static status_t d_blob_read(sp_session_t *s, const sp_item_desc_t *item, void **data, size_t *len)
{
    const char *hk = strncmp(item->id, "log:", 4) == 0 ? hook_of(item) : NULL;
    if (!hk) return d_blob_data(s->c, s->unit, item->id, data, len);
    const char *text = "c-backup: this hook did not run (the unit was not running)\n";
    size_t n = strlen(text);
    for (size_t i = 0; i < s->n_logs; i++)
        if (strcmp(s->logs[i].name, hk) == 0) { text = s->logs[i].text; n = s->logs[i].len; }
    void *cp = malloc(n ? n : 1);
    if (!cp) return set_error(ERR_NOMEM, "docker: hook log");
    memcpy(cp, text, n);
    *data = cp;
    *len = n;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Hooks: docker exec into a member                                    */
/* ------------------------------------------------------------------ */

struct helper_run {
    docker_api_t api;
    char        exec_id[80];
    pthread_t   th;
    atomic_int  done;
    int         status;
    int         out_fd, err_fd;
};

static void write_all(int fd, const unsigned char *p, size_t n)
{
    while (fd >= 0 && n) {
        ssize_t w = write(fd, p, n);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return;      /* the reader went away: drop the rest */
        p += w;
        n -= (size_t)w;
    }
}

static void on_exec(void *ctx, int stream, const unsigned char *d, size_t n)
{
    struct helper_run *r = ctx;
    write_all(stream == 1 ? r->out_fd : r->err_fd, d, n);
}

static void *exec_main(void *arg)
{
    struct helper_run *r = arg;
    status_t st = docker_exec_stream(&r->api, r->exec_id, on_exec, r);
    int code = 255;
    if (st == OK) {
        char path[160];
        snprintf(path, sizeof(path), "/exec/%s/json", r->exec_id);
        cJSON *j = NULL;
        if (docker_call(&r->api, "GET", path, NULL, 0, NULL, &j) == OK) {
            const cJSON *e = cJSON_GetObjectItemCaseSensitive(j, "ExitCode");
            if (cJSON_IsNumber(e)) code = (int)e->valuedouble;
        }
        cJSON_Delete(j);
    }
    if (st != OK || code == 255) {
        char m[256];
        snprintf(m, sizeof(m), "c-backup-helper: %.200s\n", st != OK ? err_msg() : "no exit code");
        write_all(r->err_fd, (const unsigned char *)m, strlen(m));
    }
    err_clear();
    if (r->out_fd >= 0) close(r->out_fd);
    if (r->err_fd >= 0) close(r->err_fd);
    r->out_fd = r->err_fd = -1;
    r->status = code;
    atomic_store(&r->done, 1);
    return NULL;
}

static status_t d_hook_start(sp_ctx_t *c, const char *unit_id, const policy_hook_t *h, int out_fd,
                             int err_fd, helper_run_t **out)
{
    member_t m[MEMBERS_MAX];
    size_t n = 0;
    status_t st = members_of(c, unit_id, m, &n);
    if (st != OK) return st;
    const member_t *t = NULL;
    for (size_t i = 0; i < n && !t; i++)
        if (strcmp(m[i].name, h->container[0] ? h->container : unit_id) == 0) t = &m[i];
    if (!t) return set_error(ERR_INVALID, "docker: hook %s names %s, which is not a member of %s",
                             h->name, h->container[0] ? h->container : unit_id, unit_id);
    if (!t->running) return set_error(ERR_INVALID, "docker: hook %s: %s is not running", h->name, t->name);
    cJSON *body = cJSON_CreateObject();
    cJSON *cmd = body ? cJSON_AddArrayToObject(body, "Cmd") : NULL;
    for (int i = 0; cmd && i < h->argc; i++) cJSON_AddItemToArray(cmd, cJSON_CreateString(h->argv[i]));
    if (body) {
        cJSON_AddBoolToObject(body, "AttachStdout", 1);
        cJSON_AddBoolToObject(body, "AttachStderr", 1);
        cJSON_AddBoolToObject(body, "Tty", 0);
    }
    char *txt = body ? cJSON_PrintUnformatted(body) : NULL;
    cJSON_Delete(body);
    if (!txt) return set_error(ERR_NOMEM, "docker: exec");
    char path[160];
    snprintf(path, sizeof(path), "/containers/%s/exec", t->id);
    cJSON *res = NULL;
    st = docker_call(&c->api, "POST", path, txt, 0, NULL, &res);
    free(txt);
    if (st != OK) return st;
    const char *eid = jstr(res, "Id");
    struct helper_run *r = calloc(1, sizeof(*r));
    if (!r || !eid[0] || strlen(eid) >= sizeof(r->exec_id) || strspn(eid, "0123456789abcdef") != strlen(eid)) {
        free(r);
        cJSON_Delete(res);
        return set_error(r ? ERR_IO : ERR_NOMEM, "docker: exec create gave no usable id");
    }
    snprintf(r->exec_id, sizeof(r->exec_id), "%s", eid);
    cJSON_Delete(res);
    r->api = c->api;
    r->api.timeout_s = 30;
    r->out_fd = out_fd >= 0 ? fcntl(out_fd, F_DUPFD_CLOEXEC, 0) : -1;
    r->err_fd = err_fd >= 0 ? fcntl(err_fd, F_DUPFD_CLOEXEC, 0) : -1;
    if (pthread_create(&r->th, NULL, exec_main, r) != 0) {
        if (r->out_fd >= 0) close(r->out_fd);
        if (r->err_fd >= 0) close(r->err_fd);
        free(r);
        return set_error(ERR_IO, "docker: cannot start the exec thread");
    }
    *out = r;
    return OK;
}

static int d_hook_poll(helper_run_t *r, int *status)
{
    if (!atomic_load(&r->done)) return 0;
    *status = r->status;
    return 1;
}

/* The Engine API cannot kill an exec: signal its host pid (the helper runs
 * with --pid=host), SIGTERM, then SIGKILL after 10 s. */
static void d_hook_kill(helper_run_t *r)
{
    char path[160];
    snprintf(path, sizeof(path), "/exec/%s/json", r->exec_id);
    cJSON *j = NULL;
    int pid = 0;
    if (docker_call(&r->api, "GET", path, NULL, 0, NULL, &j) == OK) {
        const cJSON *p = cJSON_GetObjectItemCaseSensitive(j, "Pid");
        if (cJSON_IsNumber(p)) pid = (int)p->valuedouble;
    }
    cJSON_Delete(j);
    err_clear();
    if (pid > 1) {
        kill(pid, SIGTERM);
        for (int i = 0; i < 100 && !atomic_load(&r->done); i++) {
            struct timespec ts = { 0, 100 * 1000000L };
            nanosleep(&ts, NULL);
        }
        if (!atomic_load(&r->done)) kill(pid, SIGKILL);
    }
}

static void d_hook_free(helper_run_t *r)
{
    if (!r) return;
    pthread_join(r->th, NULL);
    free(r);
}

/* Run one policy hook to its end, keeping the tail of its output. */
static int run_hook(struct sp_ctx *c, const char *unit_id, const policy_hook_t *h, char *tail, size_t tsz,
                    hooklog_t *log)
{
    tail[0] = '\0';
    int p[2];
    if (pipe2(p, O_CLOEXEC) != 0) { snprintf(tail, tsz, "pipe: %s", strerror(errno)); return 255; }
    helper_run_t *r = NULL;
    if (d_hook_start(c, unit_id, h, p[1], p[1], &r) != OK) {
        snprintf(tail, tsz, "%.400s", err_msg());
        err_clear();
        close(p[0]);
        close(p[1]);
        return 255;
    }
    close(p[1]);
    (void)fcntl(p[0], F_SETFL, O_NONBLOCK);
    time_t deadline = time(NULL) + (time_t)h->timeout_s;
    size_t tl = 0;
    int status = 0, timed_out = 0;
    for (;;) {
        char buf[1024];
        ssize_t got;
        while ((got = read(p[0], buf, sizeof(buf))) > 0) {
            if (log) (void)hooklog_append(log, buf, (size_t)got);
            /* A rolling tail: the end of a failing hook is where its error is. */
            size_t g = (size_t)got;
            if (g > tsz - 1) g = tsz - 1;      /* buf is smaller than the tail */
            if (tl + g > tsz - 1) { memmove(tail, tail + (tl + g - (tsz - 1)), tsz - 1 - g); tl = tsz - 1 - g; }
            memcpy(tail + tl, buf, g);
            tl += g;
            tail[tl] = '\0';
        }
        if (d_hook_poll(r, &status)) break;
        if (time(NULL) >= deadline) { timed_out = 1; d_hook_kill(r); }
        struct timespec ts = { 0, 20 * 1000000L };
        nanosleep(&ts, NULL);
        if (timed_out && d_hook_poll(r, &status)) break;
    }
    close(p[0]);
    d_hook_free(r);
    if (timed_out) { snprintf(tail + tl, tsz - tl, "%s", tl ? " [timed out]" : "timed out"); return 128 + SIGKILL; }
    return status;
}

/* Every policy hook for this unit in a phase. Returns -1 when one with
 * on_fail = fail failed (the unit fails), else how many failed. */
static int run_hooks(struct sp_ctx *c, const char *unit_id, int pre, struct sp_session *sess, int *ran)
{
    sp_session_info_t *info = sess ? &sess->info : NULL;
    char key[HELPER_UNIT_KEY_MAX + 1];
    snprintf(key, sizeof(key), "docker:%s", unit_id);
    int failed = 0;
    *ran = 0;
    for (size_t i = 0; i < c->pol->n_hooks; i++) {
        const policy_hook_t *h = &c->pol->hooks[i];
        if (strcmp(h->unit, key) != 0 || h->pre != pre || h->dump) continue;
        (*ran)++;
        char tail[HOOK_TAIL];
        hooklog_t log;
        memset(&log, 0, sizeof(log));
        int code = run_hook(c, unit_id, h, tail, sizeof(tail), &log);
        if (code != 0) {
            char line[64];
            snprintf(line, sizeof(line), "\n[c-backup: exit status %d]\n", code);
            (void)hooklog_append(&log, line, strlen(line));
        }
        size_t llen = 0;
        char *ltxt = hooklog_render(&log, &llen);
        hooklog_free(&log);
        if (sess && ltxt) keep_log(sess, h->name, ltxt, llen);
        else free(ltxt);
        if (code == 0) continue;
        failed++;
        log_warn("docker: %s hook %s on %s failed (%d): %s", pre ? "pre" : "post", h->name, unit_id, code, tail);
        if (info) (void)sp_session_info_warn(info, "hook %s exited %d: %.300s", h->name, code, tail);
        if (h->fail_on_error) {
            set_error(ERR_IO, "docker: hook %s on %s exited %d: %.300s", h->name, unit_id, code, tail);
            return -1;
        }
    }
    return failed;
}

/* ------------------------------------------------------------------ */
/* Capture                                                             */
/* ------------------------------------------------------------------ */

static status_t pause_one(struct sp_ctx *c, const member_t *m, int pause)
{
    char path[160];
    snprintf(path, sizeof(path), "/containers/%s/%s", m->id, pause ? "pause" : "unpause");
    docker_api_t a = c->api;
    a.timeout_s = c->pol->pause_timeout_s ? c->pol->pause_timeout_s : 5;
    /* 409: already in that state, which is the state wanted. */
    return docker_call(&a, "POST", path, NULL, 409, NULL, NULL);
}

typedef struct {
    struct sp_ctx  *c;
    const member_t *m;
    status_t        st;
    char            why[256];
} pause_job_t;

static void *pause_main(void *arg)
{
    pause_job_t *j = arg;
    j->st = pause_one(j->c, j->m, 1);
    if (j->st != OK) snprintf(j->why, sizeof(j->why), "%.250s", err_msg());
    err_clear();
    return NULL;
}

static void syncfs_root(struct sp_ctx *c, const char *root)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, root, hp, sizeof(hp)) != 0) return;
    int fd = open(hp, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return;
    (void)syncfs(fd);
    close(fd);
}

static uint64_t ms_now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

static void session_free(struct sp_session *s)
{
    if (!s) return;
    for (size_t i = 0; i < s->n_logs; i++) free(s->logs[i].text);
    for (size_t i = 0; i < s->n_streams; i++) {
        if (s->streams[i].run) { d_hook_kill(s->streams[i].run); d_hook_free(s->streams[i].run); }
        if (s->streams[i].err_fd >= 0) close(s->streams[i].err_fd);
    }
    free(s->items);
    sp_session_info_free(&s->info);
    free(s);
}

/* Unpause every member this session paused, clearing each hold. */
static void unpause_all(struct sp_session *s, int64_t *hold, int *paused)
{
    for (size_t i = 0; i < s->n_members; i++) {
        if (!paused[i]) continue;
        if (pause_one(s->c, &s->members[i], 0) == OK) {
            paused[i] = 0;
            if (s->has_holds && hold[i] && s->holds.clear(s->holds.ctx, hold[i]) != OK) err_clear();
        } else {
            /* Left journalled: the watchdog and the sweep retry it. */
            log_error("docker: cannot unpause %s: %s", s->members[i].name, err_msg());
            err_clear();
        }
    }
}

static status_t d_begin(sp_ctx_t *c, const sp_unit_desc_t *unit, const sp_begin_req_t *req,
                        sp_session_t **out, const sp_session_info_t **info)
{
    struct sp_session *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "docker: session");
    s->c = c;
    snprintf(s->unit, sizeof(s->unit), "%s", unit->unit_id);
    if (req->holds) { s->holds = *req->holds; s->has_holds = 1; }
    status_t st = members_of(c, unit->unit_id, s->members, &s->n_members);
    s->items = calloc(unit->n_items ? unit->n_items : 1, sizeof(*s->items));
    if (st == OK && !s->items) st = set_error(ERR_NOMEM, "docker: session");
    size_t fallback = 0;
    for (size_t i = 0; st == OK && i < unit->n_items; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_TREE || !it->source) continue;
        sitem_t *x = &s->items[s->n_items++];
        snprintf(x->id, sizeof(x->id), "%s", it->id);
        snprintf(x->real, sizeof(x->real), "%s", it->source);
        snap_dom_t d;
        if ((st = snap_domain_of(&c->snap, x->real, &d)) != OK) break;
        if (d.kind == SNAP_NONE) { x->live = 1; fallback++; continue; }
        st = snap_set_add(&s->snaps, &d);
    }
    if (st != OK) { session_free(s); return st; }

    int running = 0;
    for (size_t i = 0; i < s->n_members; i++) running |= s->members[i].running;
    unsigned instants = snap_instants(s->snaps.doms, s->snaps.n);

    /* Pre hooks, live, before any pause (6.6). */
    int ran = 0, failed = running ? run_hooks(c, unit->unit_id, 1, s, &ran) : 0;
    if (failed < 0) { session_free(s); return ERR_IO; }

    /* pause: auto pauses only when more than one instant would otherwise
     * tear the unit (6.4); always per unit; never for our own containers. */
    int mode = c->pol->docker_pause;
    if (glob_any(c->pol->pause_always, c->pol->n_pause_always, unit->unit_id)) mode = 1;
    int want = running && (mode == 1 || (mode == 0 && instants > 1));
    int paused[MEMBERS_MAX] = { 0 };
    int64_t hold[MEMBERS_MAX] = { 0 };
    int pause_failed = 0;
    /* Pre-flush outside any pause, so the snapshot's own commit is short. */
    for (size_t i = 0; i < s->snaps.n; i++) syncfs_root(c, s->snaps.doms[i].root);
    uint64_t t0 = ms_now();
    /* Journal every pause first, then pause the members together: the
     * window is the slowest member's freeze, not the sum (6.1). */
    pause_job_t jobs[MEMBERS_MAX];
    pthread_t th[MEMBERS_MAX];
    int started[MEMBERS_MAX] = { 0 };
    for (size_t i = 0; want && !pause_failed && i < s->n_members; i++) {
        member_t *m = &s->members[i];
        if (!m->running || m->paused || m->never) continue;
        jobs[i] = (pause_job_t){ c, m, ERR_IO, "not attempted" };
        char detail[128];
        snprintf(detail, sizeof(detail), "docker:%s", m->id);
        if (s->has_holds && s->holds.record(s->holds.ctx, SP_HOLD_PAUSE, detail, &hold[i]) != OK) {
            snprintf(jobs[i].why, sizeof(jobs[i].why), "%.250s", err_msg());
            err_clear();
            pause_failed = 1;
        }
    }
    for (size_t i = 0; want && !pause_failed && i < s->n_members; i++) {
        member_t *m = &s->members[i];
        if (!m->running || m->paused || m->never) continue;
        if (pthread_create(&th[i], NULL, pause_main, &jobs[i]) == 0) started[i] = 1;
        else { jobs[i].st = ERR_IO; snprintf(jobs[i].why, sizeof(jobs[i].why), "cannot start a thread"); }
    }
    for (size_t i = 0; want && i < s->n_members; i++) {
        member_t *m = &s->members[i];
        if (!m->running || m->paused || m->never) continue;
        if (started[i]) pthread_join(th[i], NULL);
        /* Whatever paused is recorded, so unpause_all releases it. */
        if (started[i] && jobs[i].st == OK) { paused[i] = 1; continue; }
        log_warn("docker: %s did not pause: %s", m->name, jobs[i].why);
        if (s->has_holds && hold[i] && s->holds.clear(s->holds.ctx, hold[i]) != OK) err_clear();
        hold[i] = 0;
        pause_failed = 1;
    }
    if (pause_failed) {
        /* 6.5: unpause what paused and take the snapshot unpaused, which is
         * still crash-consistent in one instant, and fails otherwise. */
        unpause_all(s, hold, paused);
        if (instants > 1) {
            session_free(s);
            return set_error(ERR_IO, "docker: %s did not pause and spans %u snapshot instants", unit->unit_id, instants);
        }
        (void)sp_session_info_warn(&s->info, "a member did not pause within %u s; captured unpaused",
                                   c->pol->pause_timeout_s);
    }
    int any_paused = 0;
    for (size_t i = 0; i < s->n_members; i++) any_paused |= paused[i];
    for (size_t i = 0; any_paused && i < s->snaps.n; i++) syncfs_root(c, s->snaps.doms[i].root);
    st = s->snaps.n ? snap_take(&c->snap, &s->snaps, c->pol->daemon_id, req->job_id,
                                s->has_holds ? &s->holds : NULL) : OK;
    char why[512] = "";
    if (st != OK) snprintf(why, sizeof(why), "%.500s", err_msg());
    unpause_all(s, hold, paused);
    s->info.pause_ms = any_paused ? ms_now() - t0 : 0;
    if (st != OK) { session_free(s); return set_error(st, "%s", why); }

    /* Post hooks run after the unpause and the snapshot (6.6). */
    int pran = 0;
    if (running && run_hooks(c, unit->unit_id, 0, s, &pran) < 0) {
        (void)sp_session_info_warn(&s->info, "%.400s", err_msg());
        err_clear();
    }

    sp_consistency_t lvl;
    if (!running) lvl = SP_CONSIST_OFFLINE;
    else if (fallback || (instants > 1 && !any_paused)) lvl = SP_CONSIST_INCONSISTENT;
    else if (ran && failed == 0) lvl = SP_CONSIST_APPLICATION;
    else lvl = SP_CONSIST_CRASH;
    if (fallback && running)
        (void)sp_session_info_warn(&s->info, "%zu mount(s) on no snapshot domain read live (fallback-live)", fallback);
    if (instants > 1 && !any_paused && running)
        (void)sp_session_info_warn(&s->info, "%u snapshot instants without a pause", instants);
    s->info.achieved = lvl;
    s->info.consist_flags = (any_paused ? SP_CF_PAUSED : 0) | (s->snaps.n ? SP_CF_FS_SNAPSHOT : 0) |
                            (ran && failed == 0 ? SP_CF_HOOK_PRE_OK : 0);
    if (lvl < req->require) {
        (void)snap_release(&c->snap, &s->snaps, c->pol->daemon_id, s->has_holds ? &s->holds : NULL);
        session_free(s);
        return set_error(ERR_IO, "docker: %s reached %s, below the %s required", unit->unit_id,
                         sp_consistency_name(lvl), sp_consistency_name(req->require));
    }
    *out = s;
    *info = &s->info;
    return OK;
}

static status_t d_tree_root(sp_session_t *s, const sp_item_desc_t *item, char *buf, size_t sz)
{
    for (size_t i = 0; i < s->n_items; i++) {
        if (strcmp(s->items[i].id, item->id) != 0) continue;
        if (s->items[i].live) {
            if (strlen(s->items[i].real) >= sz) return set_error(ERR_INVALID, "docker: path too long");
            snprintf(buf, sz, "%s", s->items[i].real);
            return OK;
        }
        return snap_read_path(&s->c->snap, &s->snaps, s->items[i].real, buf, sz);
    }
    return set_error(ERR_NOT_FOUND, "docker: no tree %s in this session", item->id);
}

static status_t d_end(sp_session_t *s, sp_end_outcome_t outcome, int64_t snapshot_id)
{
    (void)outcome; (void)snapshot_id;
    status_t st = snap_release(&s->c->snap, &s->snaps, s->c->pol->daemon_id, s->has_holds ? &s->holds : NULL);
    if (st != OK) { log_warn("docker: %s", err_msg()); err_clear(); }
    session_free(s);
    return OK;
}

static status_t d_sweep(sp_ctx_t *c, const sp_sweep_req_t *req, sp_sweep_report_t *out)
{
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < req->n_intents; i++) {
        const sp_intent_t *in = &req->intents[i];
        status_t st;
        if (strcmp(in->kind, SP_HOLD_PAUSE) == 0 && strncmp(in->detail, "docker:", 7) == 0) {
            member_t m;
            memset(&m, 0, sizeof(m));
            snprintf(m.id, sizeof(m.id), "%s", in->detail + 7);
            st = strspn(m.id, "0123456789abcdef") == strlen(m.id) && m.id[0]
                     ? pause_one(c, &m, 0) : set_error(ERR_INVALID, "docker: bad pause detail");
            /* A container that is gone is not paused. */
            if (st == ERR_NOT_FOUND) { err_clear(); st = OK; }
        } else if (strcmp(in->kind, SP_HOLD_FS_SNAPSHOT) == 0) {
            st = snap_release_detail(&c->snap, in->detail, req->daemon8 ? req->daemon8 : c->pol->daemon_id);
        } else {
            st = set_error(ERR_INVALID, "docker: unknown hold %s", in->kind);
        }
        if (st == OK) { out->released++; if (req->released) req->released[i] = 1; }
        else { out->failed++; log_warn("docker: sweep: %s", err_msg()); err_clear(); }
    }
    if (req->scan_artefacts) {
        unsigned removed = 0, foreign = 0;
        (void)snap_sweep(&c->snap, req->daemon8 ? req->daemon8 : c->pol->daemon_id, req->live_jobs,
                         req->n_live_jobs, &removed, &foreign);
        out->removed += removed;
        out->left_foreign += foreign;
    }
    return OK;
}

/* A dump hook in-process: stdout to the caller's pipe, stderr to a memfd
 * (never blocks the exec), its exit status at close. Through the helper the
 * daemon uses hook.run and hook.wait instead, under the helper's watchdog. */
static status_t d_stream_open(sp_session_t *s, const sp_item_desc_t *item, int *fd)
{
    const char *hk = hook_of(item);
    const policy_hook_t *h = hk ? helper_policy_hook(s->c->pol, hk) : NULL;
    if (!h || !h->dump) return set_error(ERR_NOT_FOUND, "docker: no dump hook for %s", item->id);
    if (s->n_streams >= sizeof(s->streams) / sizeof(s->streams[0])) return set_error(ERR_INVALID, "docker: too many dumps");
    int pp[2];
    if (pipe2(pp, O_CLOEXEC) != 0) return set_error_errno(ERR_IO, "docker: pipe");
    int efd = memfd_create("cbk-hook-err", MFD_CLOEXEC);
    helper_run_t *run = NULL;
    status_t st = efd < 0 ? set_error_errno(ERR_IO, "docker: memfd")
                          : d_hook_start(s->c, s->unit, h, pp[1], efd, &run);
    close(pp[1]);
    if (st != OK) { close(pp[0]); if (efd >= 0) close(efd); return st; }
    snprintf(s->streams[s->n_streams].item, sizeof(s->streams[0].item), "%s", item->id);
    s->streams[s->n_streams].run = run;
    s->streams[s->n_streams].err_fd = efd;
    s->n_streams++;
    *fd = pp[0];
    return OK;
}

static status_t d_stream_close(sp_session_t *s, const sp_item_desc_t *item, int *ok)
{
    for (size_t i = 0; i < s->n_streams; i++) {
        if (!s->streams[i].run || strcmp(s->streams[i].item, item->id) != 0) continue;
        int code = 255;
        while (!d_hook_poll(s->streams[i].run, &code)) {
            struct timespec ts = { 0, 20 * 1000000L };
            nanosleep(&ts, NULL);
        }
        d_hook_free(s->streams[i].run);
        s->streams[i].run = NULL;
        hooklog_t log;
        memset(&log, 0, sizeof(log));
        char buf[8192];
        ssize_t n;
        off_t off = 0;
        while ((n = pread(s->streams[i].err_fd, buf, sizeof(buf), off)) > 0) { (void)hooklog_append(&log, buf, (size_t)n); off += n; }
        close(s->streams[i].err_fd);
        s->streams[i].err_fd = -1;
        char line[64];
        snprintf(line, sizeof(line), "\n[c-backup: exit status %d]\n", code);
        (void)hooklog_append(&log, line, strlen(line));
        size_t len = 0;
        char *txt = hooklog_render(&log, &len);
        hooklog_free(&log);
        if (txt) keep_log(s, hook_of(item), txt, len);
        *ok = code == 0;
        if (code) (void)sp_session_info_warn(&s->info, "dump hook %s exited %d; not stored", hook_of(item), code);
        return OK;
    }
    return set_error(ERR_NOT_FOUND, "docker: dump %s was not open", item->id);
}

static const sp_provider_ops_t k_docker_ops = {
    .name = "docker", .abi = SP_ABI_VERSION,
    .init = d_init, .fini = d_fini, .probe = d_probe, .list_units = d_list,
    .describe = d_describe, .desc_free = sp_unit_desc_free, .begin = d_begin,
    .blob_read = d_blob_read, .tree_root = d_tree_root, .end = d_end, .sweep = d_sweep,
    .stream_open = d_stream_open, .stream_close = d_stream_close,
};

/* ------------------------------------------------------------------ */
/* Backend extras                                                      */
/* ------------------------------------------------------------------ */

static int d_unit_state(sp_ctx_t *c, const char *unit_id)
{
    member_t m[MEMBERS_MAX];
    size_t n = 0;
    if (members_of(c, unit_id, m, &n) != OK) { err_clear(); return -1; }
    return unit_state(m, n);
}

static status_t d_set_running(sp_ctx_t *c, const char *unit_id, int run)
{
    member_t m[MEMBERS_MAX];
    size_t n = 0;
    status_t st = members_of(c, unit_id, m, &n);
    for (size_t i = 0; st == OK && i < n; i++) {
        char path[160];
        snprintf(path, sizeof(path), "/containers/%.79s/%s", m[i].id, run ? "start" : "stop");
        /* 304: already started or stopped. */
        st = docker_call(&c->api, "POST", path, NULL, 304, NULL, NULL);
    }
    return st;
}

static status_t d_mount_root(sp_ctx_t *c, const char *mount, char *buf, size_t sz)
{
    snap_dom_t d;
    status_t st = snap_domain_of(&c->snap, mount, &d);
    if (st != OK) return st;
    if (d.kind == SNAP_NONE) return set_error(ERR_NOT_FOUND, "docker: %s is in no snapshot domain", mount);
    snprintf(buf, sz, "%s", d.root);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Restore: define (container-backup.md 11.2)                          */
/* ------------------------------------------------------------------ */

static void urlenc(const char *in, char *out, size_t sz)
{
    static const char hex[] = "0123456789ABCDEF";
    size_t o = 0;
    for (; *in && o + 4 < sz; in++) {
        unsigned char ch = (unsigned char)*in;
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
            ch == '-' || ch == '_' || ch == '.' || ch == '~') out[o++] = (char)ch;
        else { out[o++] = '%'; out[o++] = hex[ch >> 4]; out[o++] = hex[ch & 15]; }
    }
    out[o] = '\0';
}

static int name_ok(const char *n)
{
    size_t l = strlen(n);
    if (l == 0 || l > 128 || !((n[0] >= 'a' && n[0] <= 'z') || (n[0] >= 'A' && n[0] <= 'Z') || (n[0] >= '0' && n[0] <= '9')))
        return 0;
    return strspn(n, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") == l;
}

/* Settings that make a container root on the host, refused without
 * allow_privileged_define. */
static const char *privileged_reason(const cJSON *hc)
{
    if (cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(hc, "Privileged"))) return "Privileged";
    if (cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(hc, "CapAdd")) > 0) return "CapAdd";
    if (cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(hc, "Devices")) > 0) return "Devices";
    const char *pid = jstr(hc, "PidMode"), *net = jstr(hc, "NetworkMode");
    if (strcmp(pid, "host") == 0) return "PidMode host";
    if (strcmp(net, "host") == 0) return "NetworkMode host";
    return NULL;
}

/* Pull by the recorded digest; if the registry no longer serves it, the
 * recorded tag with a warning, unless pinned (C3). *ref is what to create
 * from. */
static status_t pull_image(struct sp_ctx *c, const cJSON *image, int pin, char *ref, size_t rsz, cJSON *warn)
{
    const char *dg = cJSON_GetStringValue(cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(image, "RepoDigests"), 0));
    const char *tag = cJSON_GetStringValue(cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(image, "RepoTags"), 0));
    docker_api_t a = c->api;
    a.timeout_s = 3600;            /* a first pull of a large image */
    char q[1200], enc[1024];
    if (dg && strchr(dg, '@')) {
        urlenc(dg, enc, sizeof(enc));
        snprintf(q, sizeof(q), "/images/create?fromImage=%s", enc);
        if (docker_call(&a, "POST", q, NULL, 0, NULL, NULL) == OK) { snprintf(ref, rsz, "%s", dg); return OK; }
        char why[300];
        snprintf(why, sizeof(why), "%.290s", err_msg());
        err_clear();
        if (pin || !tag) return set_error(ERR_NOT_FOUND, "docker: image %s is no longer available%s (%s)", dg,
                                          pin ? " and pin_digest refuses a tag" : ", and no tag is recorded", why);
        char w[600];
        snprintf(w, sizeof(w), "image digest %s no longer available; pulled tag %s instead", strchr(dg, '@') + 1, tag);
        cJSON_AddItemToArray(warn, cJSON_CreateString(w));
        log_warn("docker: %s", w);
    } else if (!tag) {
        return set_error(ERR_INVALID, "docker: the definition records no image digest or tag");
    } else if (pin) {
        return set_error(ERR_INVALID, "docker: no digest recorded and pin_digest refuses a tag");
    }
    urlenc(tag, enc, sizeof(enc));
    snprintf(q, sizeof(q), "/images/create?fromImage=%s", enc);
    status_t st = docker_call(&a, "POST", q, NULL, 0, NULL, NULL);
    if (st == OK) snprintf(ref, rsz, "%s", tag);
    return st;
}

/* Move a file aside under the same directory, never delete it. */
static status_t move_aside(struct sp_ctx *c, const char *path, char *moved, size_t msz)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX], hm[PATH_MAX + HOST_TOOL_PATH_MAX];
    snprintf(moved, msz, "%s.cbackup-old-%lld", path, (long long)time(NULL));
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0 || host_path(&c->tools, moved, hm, sizeof(hm)) != 0)
        return set_error(ERR_INVALID, "docker: path too long");
    struct stat sb;
    if (lstat(hp, &sb) != 0) { moved[0] = '\0'; return OK; }
    if (rename(hp, hm) != 0) return set_error_errno(ERR_IO, "docker: moving %s aside", path);
    return OK;
}

/*
 * The definition, as the daemon read it from the snapshot:
 *   {"inspect": {...}, "image": {"RepoDigests": [...], "RepoTags": [...]},
 *    "template": "<xml>", "name": "", "replace": false, "pin_digest": false,
 *    "start": false}
 * Every field is read by name and the create body is built here; nothing
 * the daemon sent is forwarded as it is.
 */
static status_t d_define(sp_ctx_t *c, const char *unit_id, const char *definition,
                         const helper_policy_t *pol, cJSON **result)
{
    *result = NULL;
    if (strncmp(unit_id, "compose:", 8) == 0)
        return set_error(ERR_INVALID, "docker: define of a compose project is not built; restore its files "
                         "and data, then run docker compose up");
    cJSON *d = cJSON_Parse(definition);
    const cJSON *in = cJSON_GetObjectItemCaseSensitive(d, "inspect");
    const cJSON *cfg = cJSON_GetObjectItemCaseSensitive(in, "Config");
    const cJSON *hc = cJSON_GetObjectItemCaseSensitive(in, "HostConfig");
    if (!cJSON_IsObject(in) || !cJSON_IsObject(cfg)) {
        cJSON_Delete(d);
        return set_error(ERR_INVALID, "docker: the definition has no inspect with a Config");
    }
    int replace = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "replace"));
    int pin = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "pin_digest"));
    int start = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "start"));
    char name[160];
    const char *want = jstr(d, "name");
    const char *orig = jstr(in, "Name");
    snprintf(name, sizeof(name), "%s", want[0] ? want : (orig[0] == '/' ? orig + 1 : orig));
    status_t st = OK;
    if (!name_ok(name)) st = set_error(ERR_INVALID, "docker: bad container name %.100s", name);
    const char *why = st == OK ? privileged_reason(hc) : NULL;
    if (st == OK && why && !pol->allow_privileged_define)
        st = set_error(ERR_INVALID, "docker: the definition sets %s, and allow_privileged_define is off", why);

    /* A name in use is refused unless replacing; the one replaced must be
     * stopped, and is renamed aside, never removed. */
    cJSON *res = cJSON_CreateObject();
    cJSON *warn = res ? cJSON_AddArrayToObject(res, "warnings") : NULL;
    if (st == OK && !warn) st = set_error(ERR_NOMEM, "docker: define");
    char path[512];
    snprintf(path, sizeof(path), "/containers/%s/json", name);
    cJSON *have = NULL;
    if (st == OK && docker_call(&c->api, "GET", path, NULL, 0, NULL, &have) == OK) {
        if (!replace) {
            st = set_error(ERR_BUSY, "docker: a container named %s exists; pass replace, or a new name", name);
        } else if (cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(have, "State"), "Running"))) {
            st = set_error(ERR_BUSY, "docker: %s is running; stop it before replacing it", name);
        } else {
            char aside[200], q[512];
            snprintf(aside, sizeof(aside), "%.128s.cbackup-old-%lld", name, (long long)time(NULL));
            snprintf(q, sizeof(q), "/containers/%s/rename?name=%s", jstr(have, "Id"), aside);
            st = docker_call(&c->api, "POST", q, NULL, 0, NULL, NULL);
            if (st == OK) cJSON_AddStringToObject(res, "previous", aside);
        }
    } else if (st == OK) {
        err_clear();           /* no such container: the name is free */
    }
    cJSON_Delete(have);

    char ref[600] = "";
    if (st == OK) st = pull_image(c, cJSON_GetObjectItemCaseSensitive(d, "image"), pin, ref, sizeof(ref), warn);

    /* The create body: Config and HostConfig as recorded, the image pinned,
     * and the networks by name with their static addresses. */
    char *body = NULL;
    if (st == OK) {
        cJSON *b = cJSON_CreateObject();
        cJSON *bc = cJSON_Duplicate(cfg, 1);
        if (bc) {
            cJSON_DeleteItemFromObjectCaseSensitive(bc, "Image");
            cJSON_AddStringToObject(bc, "Image", ref);
            cJSON_DeleteItemFromObjectCaseSensitive(bc, "Hostname");
        }
        if (b && bc) {
            cJSON *child = bc->child;
            bc->child = NULL;
            cJSON_Delete(bc);
            for (cJSON *x = child; x; ) { cJSON *nx = x->next; x->prev = x->next = NULL; cJSON_AddItemToObject(b, x->string, x); x = nx; }
            if (hc) cJSON_AddItemToObject(b, "HostConfig", cJSON_Duplicate(hc, 1));
            const cJSON *nets = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(in, "NetworkSettings"), "Networks");
            cJSON *ep = cJSON_CreateObject();
            const cJSON *n;
            cJSON_ArrayForEach(n, nets) {
                cJSON *e = cJSON_CreateObject();
                const cJSON *ipam = cJSON_GetObjectItemCaseSensitive(n, "IPAMConfig");
                if (e && cJSON_IsObject(ipam)) cJSON_AddItemToObject(e, "IPAMConfig", cJSON_Duplicate(ipam, 1));
                if (e && ep) cJSON_AddItemToObject(ep, n->string, e);
            }
            cJSON *nc = cJSON_CreateObject();
            if (nc && ep) cJSON_AddItemToObject(nc, "EndpointsConfig", ep);
            if (nc) cJSON_AddItemToObject(b, "NetworkingConfig", nc);
            body = cJSON_PrintUnformatted(b);
        } else {
            cJSON_Delete(bc);
        }
        cJSON_Delete(b);
        if (!body) st = set_error(ERR_NOMEM, "docker: create body");
    }
    cJSON *made = NULL;
    if (st == OK) {
        snprintf(path, sizeof(path), "/containers/create?name=%s", name);
        st = docker_call(&c->api, "POST", path, body, 0, NULL, &made);
    }
    free(body);
    const char *id = jstr(made, "Id");

    /* The Unraid template, so Unraid's UI knows the container. */
    const char *tmpl = jstr(d, "template");
    if (st == OK && tmpl[0] && pol->templates_dir[0]) {
        char tp[PATH_MAX], moved[PATH_MAX + 40], hp[PATH_MAX + HOST_TOOL_PATH_MAX];
        snprintf(tp, sizeof(tp), "%.3900s/my-%s.xml", pol->templates_dir, name);
        st = move_aside(c, tp, moved, sizeof(moved));
        if (st == OK && moved[0]) cJSON_AddStringToObject(res, "previous_template", moved);
        FILE *f = st == OK && host_path(&c->tools, tp, hp, sizeof(hp)) == 0 ? fopen(hp, "wxe") : NULL;
        if (st == OK && !f) st = set_error_errno(ERR_IO, "docker: writing %s", tp);
        if (f) {
            if (fputs(tmpl, f) == EOF) st = set_error_errno(ERR_IO, "docker: writing %s", tp);
            if (fclose(f) != 0 && st == OK) st = set_error_errno(ERR_IO, "docker: writing %s", tp);
            if (st == OK) cJSON_AddStringToObject(res, "template", tp);
        }
    }
    if (st == OK && start) {
        snprintf(path, sizeof(path), "/containers/%s/start", id);
        st = docker_call(&c->api, "POST", path, NULL, 304, NULL, NULL);
    }
    if (st == OK) {
        cJSON_AddStringToObject(res, "container", id);
        cJSON_AddStringToObject(res, "name", name);
        cJSON_AddStringToObject(res, "image", ref);
        cJSON_AddBoolToObject(res, "started", start);
        *result = res;
        res = NULL;
    }
    cJSON_Delete(made);
    cJSON_Delete(res);
    cJSON_Delete(d);
    return st;
}

status_t docker_backend_init(const helper_policy_t *pol, helper_backend_t *out)
{
    struct sp_ctx *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "docker: backend");
    c->pol = pol;
    pthread_mutex_init(&c->mu, NULL);
    snprintf(c->api.socket, sizeof(c->api.socket), "%s", pol->docker_socket);
    c->api.timeout_s = 60;
    snap_host_setup(pol, &c->tools, &c->snap);
    c->mounts.tools = &c->tools;
    for (size_t i = 0; i < pol->n_include && i < MOUNT_ROOTS_MAX; i++) c->mounts.include_roots[c->mounts.n_include++] = pol->include_roots[i];
    snprintf(c->mounts.user_root, sizeof(c->mounts.user_root), "%s", pol->user_root);
    snprintf(c->mounts.mnt_root, sizeof(c->mounts.mnt_root), "%s", pol->mnt_root);
    /* Negotiate now if dockerd is up; requests before that go unversioned,
     * which the Engine serves at its default. */
    if (docker_negotiate(&c->api) != OK) {
        log_warn("docker: %s; continuing, the Engine API is negotiated on the next probe", err_msg());
        err_clear();
    }
    *out = (helper_backend_t){
        .name = "docker", .ops = &k_docker_ops, .ctx = c,
        .blob_data = d_blob_data, .unit_state = d_unit_state, .set_running = d_set_running,
        .mount_root = d_mount_root, .hook_start = d_hook_start, .hook_poll = d_hook_poll,
        .hook_kill = d_hook_kill, .hook_free = d_hook_free, .define = d_define,
    };
    return OK;
}

void docker_backend_fini(helper_backend_t *b)
{
    if (!b || !b->ctx) return;
    struct sp_ctx *c = b->ctx;
    for (size_t i = 0; i < c->nblobs; i++) free(c->blobs[i].data);
    free(c->blobs);
    pthread_mutex_destroy(&c->mu);
    free(c);
    b->ctx = NULL;
}
