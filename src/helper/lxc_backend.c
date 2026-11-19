#define _GNU_SOURCE

#include "lxc_backend.h"
#include "helper_proto.h"
#include "mounts.h"
#include "proc_probe.h"
#include "snap.h"
#include "unit_probe.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

#define LXC_TIMEOUT_S 60
#define CFG_MAX       ((size_t)1024 * 1024)

struct sp_ctx {
    const helper_policy_t *pol;
    host_tools_t tools;
    mount_cfg_t  mounts;
    snap_ctx_t   snap;
};

typedef struct {
    char id[SP_ITEM_ID_MAX];
    char real[PATH_MAX];
    int  live;
} litem_t;

struct sp_session {
    struct sp_ctx    *c;
    char              name[SP_UNIT_ID_MAX];
    sp_holds_t        holds;
    int               has_holds;
    snap_set_t        snaps;
    litem_t          *items;
    size_t            n_items;
    char             *config;
    size_t            config_len;
    sp_session_info_t info;
};

static status_t lxc(struct sp_ctx *c, const char *tool, const char *name, const char *extra, char **out)
{
    char bin[HOST_TOOL_PATH_MAX + 16];
    snprintf(bin, sizeof(bin), "%s/%s", c->tools.lxc_dir, tool);
    const char *argv[10];
    size_t n = 0;
    argv[n++] = bin;
    argv[n++] = "-P";
    argv[n++] = c->pol->lxc_path;
    if (name) { argv[n++] = "-n"; argv[n++] = name; }
    if (extra) argv[n++] = extra;
    argv[n] = NULL;
    /* LXC's command socket is abstract, in the host's network namespace.
     * And the host's cgroup namespace (B190): from the helper's own,
     * lxc-start resolves the cgroup it escapes to through the namespace's
     * root and fails "Failed to open 10/../..: Invalid cross-device link";
     * from the host's it places the container in the host's tree, outside
     * the helper's container, as a start on the host would. */
    return host_run(&c->tools, HOST_NS_NET | HOST_NS_CGROUP, argv, LXC_TIMEOUT_S, out, 1024 * 1024);
}

/* sp_state_t from lxc-info -s. */
static int state_of(struct sp_ctx *c, const char *name)
{
    char *out = NULL;
    if (lxc(c, "lxc-info", name, "-s", &out) != OK) { err_clear(); return -1; }
    int st = strstr(out, "RUNNING") ? SP_STATE_RUNNING : strstr(out, "FROZEN") ? SP_STATE_PAUSED
           : strstr(out, "STOPPED") ? SP_STATE_STOPPED : SP_STATE_UNKNOWN;
    free(out);
    return st;
}

static int valid_name(const char *n)
{
    char key[HELPER_UNIT_KEY_MAX + 1];
    snprintf(key, sizeof(key), "lxc:%s", n);
    return strlen(n) < 64 && helper_unit_key_ok(key, NULL, 0, NULL, 0);
}

static status_t x_init(const sp_config_t *cfg, sp_ctx_t **out)
{
    (void)cfg; (void)out;
    return set_error(ERR_INVALID, "lxc: built from the helper's policy, not initialised");
}

static void x_fini(sp_ctx_t *c) { (void)c; }

static status_t x_probe(sp_ctx_t *c, sp_probe_t *out)
{
    memset(out, 0, sizeof(*out));
    char *v = NULL;
    char bin[HOST_TOOL_PATH_MAX + 16];
    snprintf(bin, sizeof(bin), "%s/lxc-ls", c->tools.lxc_dir);
    const char *argv[] = { bin, "--version", NULL };
    if (host_run(&c->tools, HOST_NS_NET | HOST_NS_CGROUP, argv, LXC_TIMEOUT_S, &v, 256) != OK) {
        snprintf(out->detail, sizeof(out->detail), "%.250s", err_msg());
        err_clear();
        return OK;
    }
    v[strcspn(v, "\n")] = '\0';
    out->available = 1;
    snprintf(out->version, sizeof(out->version), "lxc %.50s", v);
    snprintf(out->detail, sizeof(out->detail), "containers under %.200s", c->pol->lxc_path);
    free(v);
    return OK;
}

static status_t x_list(sp_ctx_t *c, sp_unit_cb cb, void *cbctx)
{
    char *out = NULL;
    status_t st = lxc(c, "lxc-ls", NULL, "--line", &out);
    if (st != OK) return st;
    char *save = NULL;
    for (char *l = strtok_r(out, "\n", &save); l && st == OK; l = strtok_r(NULL, "\n", &save)) {
        if (!valid_name(l)) continue;
        int s = state_of(c, l);
        st = cb(cbctx, l, l, s < 0 ? SP_STATE_UNKNOWN : s);
    }
    free(out);
    return st;
}

static status_t read_config(struct sp_ctx *c, const char *name, char **data, size_t *len)
{
    char p[PATH_MAX], hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    snprintf(p, sizeof(p), "%.3900s/%.64s/config", c->pol->lxc_path, name);
    if (host_path(&c->tools, p, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "lxc: path too long");
    FILE *f = fopen(hp, "re");
    if (!f) return set_error_errno(ERR_NOT_FOUND, "lxc: %s", p);
    char *buf = malloc(CFG_MAX + 1);
    if (!buf) { fclose(f); return set_error(ERR_NOMEM, "lxc: config"); }
    size_t n = fread(buf, 1, CFG_MAX + 1, f);
    fclose(f);
    if (n > CFG_MAX) { free(buf); return set_error(ERR_TOO_LARGE, "lxc: %s is too large", p); }
    buf[n] = '\0';
    *data = buf;
    *len = n;
    return OK;
}

static char *trim(char *s)
{
    while (*s == ' ' || *s == '\t') s++;
    size_t n = strlen(s);
    while (n && (s[n - 1] == ' ' || s[n - 1] == '\t' || s[n - 1] == '\r')) s[--n] = '\0';
    return s;
}

static status_t add_item(sp_unit_desc_t *d, size_t *cap, sp_item_kind_t kind, const char *id,
                         const char *tp, const char *src, uint64_t size, uint32_t caps)
{
    for (size_t i = 0; i < d->n_items; i++) if (strcmp(d->items[i].id, id) == 0) return OK;
    if (d->n_items == *cap) {
        size_t nc = *cap ? *cap * 2 : 8;
        sp_item_desc_t *g = realloc(d->items, nc * sizeof(*g));
        if (!g) return set_error(ERR_NOMEM, "lxc: items");
        d->items = g;
        *cap = nc;
    }
    sp_item_desc_t *it = &d->items[d->n_items];
    memset(it, 0, sizeof(*it));
    snprintf(it->id, sizeof(it->id), "%s", id);
    it->kind = kind;
    it->size = size;
    it->caps = caps;
    it->tree_path = strdup(tp);
    it->source = src ? strdup(src) : NULL;
    if (!it->tree_path || (src && !it->source)) { free(it->tree_path); free(it->source); return set_error(ERR_NOMEM, "lxc: items"); }
    d->n_items++;
    return OK;
}

/* B231: what a snapshot of the domain would show empty below real makes
 * it read live, as a mount on no domain is; *nested says so. The rootfs of
 * an LXC container on btrfs is often a subvolume with others inside. */
static status_t nested_below(snap_probe_t *probe, const snap_dom_t *d, const char *real,
                             sp_session_info_t *info, int *nested)
{
    size_t n = 0;
    char names[1024];
    *nested = 0;
    status_t st = snap_probe_below(probe, d, real, &n, names, sizeof(names));
    if (st != OK || !n) return st;
    *nested = 1;
    log_warn("lxc: %s has %zu mount(s) or btrfs subvolume(s) below it (%s%s), which its snapshot would "
             "show empty; read live instead", real, n, names, n > 3 ? ", ..." : "");
    if (info)
        (void)sp_session_info_warn(info, "%.300s has %zu mount(s) or btrfs subvolume(s) below it (%.400s%s), "
                                   "which its snapshot would show empty: read live (fallback-live)", real, n,
                                   names, n > 3 ? ", ..." : "");
    return OK;
}

/* A source as describe classifies it: the rootfs is the unit, so always
 * data wherever it is; any other source is data only under include_roots. */
static status_t classify_src(struct sp_ctx *c, int rootfs, const char *src, mount_class_t *mc)
{
    status_t st = mount_classify(&c->mounts, "bind", src, NULL, mc);
    if (st != OK || !rootfs || mc->verdict != MOUNT_EXCLUDED) return st;
    mount_cfg_t all = c->mounts;
    char root[] = "/";
    all.include_roots[0] = root;
    all.n_include = 1;
    return mount_classify(&all, "bind", src, NULL, mc);
}

static status_t add_tree(struct sp_ctx *c, snap_probe_t *probe, sp_unit_desc_t *d, size_t *cap, const char *id,
                         const char *src)
{
    mount_class_t mc;
    status_t st = classify_src(c, strcmp(id, "rootfs") == 0, src, &mc);
    if (st != OK) return st;
    if (mc.verdict == MOUNT_EXCLUDED) return OK;
    snap_dom_t dom;
    if ((st = snap_domain_of(&c->snap, mc.real, &dom)) != OK) return st;
    int nested = 0;
    if (dom.kind != SNAP_NONE && (st = nested_below(probe, &dom, mc.real, NULL, &nested)) != OK) return st;
    if (nested) dom.kind = SNAP_NONE;
    return add_item(d, cap, SP_ITEM_TREE, id, mc.recorded, mc.real, 0, dom.kind != SNAP_NONE ? SP_CAP_SNAPSHOT : 0);
}

/*
 * The config's rootfs and mount entries, in order, for describe and probe:
 * fn(ctx, src, target) with target NULL for the rootfs, whose path must be
 * clean and absolute (ERR_INVALID otherwise). A mount entry's src and
 * target are its first two fields as written; bind says whether its
 * options bind it. cfg is consumed.
 */
typedef status_t (*cfg_fn)(void *ctx, const char *src, const char *target, int bind);

static status_t walk_config(char *cfg, const char *unit_id, cfg_fn fn, void *ctx)
{
    status_t st = OK;
    char *save = NULL;
    char *src = malloc(PATH_MAX), *tgt = malloc(PATH_MAX);
    if (!src || !tgt) st = set_error(ERR_NOMEM, "lxc: config");
    for (char *l = strtok_r(cfg, "\n", &save); l && st == OK; l = strtok_r(NULL, "\n", &save)) {
        char *eq = strchr(l, '=');
        if (!eq || l[0] == '#') continue;
        *eq = '\0';
        char *k = trim(l), *v = trim(eq + 1);
        if (strcmp(k, "lxc.rootfs.path") == 0) {
            if (strncmp(v, "dir:", 4) == 0) v += 4;
            if (v[0] != '/' || !helper_path_clean(v)) { st = set_error(ERR_INVALID, "lxc: %s: unsupported rootfs %s", unit_id, v); break; }
            st = fn(ctx, v, NULL, 0);
        } else if (strcmp(k, "lxc.mount.entry") == 0) {
            /* <source> <target> <type> <options> */
            int nf = sscanf(v, "%4095s %4095s", src, tgt);
            if (nf < 1) continue;
            st = fn(ctx, src, nf == 2 ? tgt : "", strstr(v, "bind") != NULL);
        }
    }
    free(src);
    free(tgt);
    return st;
}

typedef struct {
    struct sp_ctx *c;
    snap_probe_t *probe;
    sp_unit_desc_t *d;
    size_t cap;
    int nmount;
} desc_walk_t;

static status_t desc_entry(void *ctx, const char *src, const char *target, int bind)
{
    desc_walk_t *w = ctx;
    if (!target) return add_tree(w->c, w->probe, w->d, &w->cap, "rootfs", src);
    /* Bind sources only. */
    if (src[0] != '/' || !bind) return OK;
    char id[SP_ITEM_ID_MAX];
    snprintf(id, sizeof(id), "mount:%d", w->nmount++);
    return add_tree(w->c, w->probe, w->d, &w->cap, id, src);
}

static status_t x_describe(sp_ctx_t *c, const char *unit_id, sp_unit_desc_t **out)
{
    if (!valid_name(unit_id)) return set_error(ERR_INVALID, "lxc: bad container name");
    char *cfg = NULL;
    size_t clen = 0;
    status_t st = read_config(c, unit_id, &cfg, &clen);
    if (st != OK) return st;
    sp_unit_desc_t *d = calloc(1, sizeof(*d));
    if (!d || !(d->name = strdup(unit_id))) { free(cfg); free(d); return set_error(ERR_NOMEM, "lxc: describe"); }
    snprintf(d->provider, sizeof(d->provider), "lxc");
    snprintf(d->unit_id, sizeof(d->unit_id), "%s", unit_id);
    int s = state_of(c, unit_id);
    d->state = s < 0 ? SP_STATE_UNKNOWN : s;
    d->caps = SP_CAP_PAUSE;
    desc_walk_t w = { c, NULL, d, 0, 0 };
    st = add_item(d, &w.cap, SP_ITEM_BLOB, "config", ".cbackup-unit/config/lxc/config", NULL, clen, 0);
    if (st == OK) st = snap_probe_new(&c->snap, c->mounts.mountinfo, &w.probe);
    if (st == OK) st = walk_config(cfg, unit_id, desc_entry, &w);
    snap_probe_free(w.probe);
    free(cfg);
    if (st != OK) { sp_unit_desc_free(d); return st; }
    *out = d;
    return OK;
}

/* ------------------------------------------------------------------ */
/* units.probe                                                         */
/* ------------------------------------------------------------------ */

#define PROBE_MOUNTS_MAX 64

typedef struct {
    struct sp_ctx *c;
    proc_mount_t   pm[PROBE_MOUNTS_MAX];
    char          *dest[PROBE_MOUNTS_MAX];
    char          *rec[PROBE_MOUNTS_MAX];
    size_t         n;
} probe_walk_t;

/* Every entry with a clean target is known, so an excluded or kernel
 * mount inside the rootfs keeps what is under it unnamed; only what
 * describe includes maps to a recorded path. */
static status_t probe_entry(void *ctx, const char *src, const char *target, int bind)
{
    probe_walk_t *w = ctx;
    if (w->n == PROBE_MOUNTS_MAX) return OK;
    char dest[PATH_MAX];
    if (!target) snprintf(dest, sizeof(dest), "/");
    else snprintf(dest, sizeof(dest), "%s%s", target[0] == '/' ? "" : "/", target);
    if (!helper_path_clean(dest)) return OK;
    mount_class_t *mc = malloc(sizeof(*mc));
    if (!mc) return set_error(ERR_NOMEM, "lxc: probe");
    int included = (target == NULL || (src[0] == '/' && bind)) &&
                   classify_src(w->c, target == NULL, src, mc) == OK && mc->verdict == MOUNT_INCLUDED;
    err_clear();
    size_t i = w->n;
    w->dest[i] = strdup(dest);
    w->rec[i] = included ? strdup(mc->recorded) : NULL;
    free(mc);
    if (!w->dest[i] || (included && !w->rec[i])) { free(w->dest[i]); free(w->rec[i]); return set_error(ERR_NOMEM, "lxc: probe"); }
    w->pm[i].dest = w->dest[i];
    w->pm[i].recorded = w->rec[i];
    w->n++;
    return OK;
}

/* The init pid from lxc-info -p, 0 when it is not running. */
static int init_pid(struct sp_ctx *c, const char *name)
{
    char *out = NULL;
    if (lxc(c, "lxc-info", name, "-p", &out) != OK) { err_clear(); return 0; }
    const char *p = strstr(out, "PID:");
    long v = p ? strtol(p + 4, NULL, 10) : 0;
    free(out);
    return v > 0 && v <= 4194304 ? (int)v : 0;
}

static status_t x_unit_probe(sp_ctx_t *c, const char *unit_id, sp_unit_probe_t **out)
{
    *out = NULL;
    if (!valid_name(unit_id)) return set_error(ERR_INVALID, "lxc: bad container name");
    char *cfg = NULL;
    size_t clen = 0;
    status_t st = read_config(c, unit_id, &cfg, &clen);
    if (st != OK) return st;
    sp_unit_probe_t *p = sp_unit_probe_new();
    probe_walk_t *w = calloc(1, sizeof(*w));
    if (!p || !w || !(p->v = calloc(1, sizeof(*p->v)))) st = set_error(ERR_NOMEM, "lxc: probe");
    sp_probe_container_t *pc = st == OK ? &p->v[0] : NULL;
    if (st == OK) {
        p->n = 1;
        snprintf(pc->name, sizeof(pc->name), "%s", unit_id);
        snprintf(pc->aliases[0], sizeof(pc->aliases[0]), "%s", unit_id);
        pc->n_aliases = 1;
        w->c = c;
        st = walk_config(cfg, unit_id, probe_entry, w);
    }
    int pid = st == OK ? init_pid(c, unit_id) : 0;
    if (pid > 0) {
        pc->running = 1;
        st = proc_probe_container(c->pol->proc_root, pid, w->pm, w->n, pc, &p->truncated);
        if (st == ERR_NOT_FOUND) { pc->running = 0; err_clear(); st = OK; }  /* stopped meanwhile */
    }
    for (size_t i = 0; w && i < w->n; i++) { free(w->dest[i]); free(w->rec[i]); }
    free(w);
    free(cfg);
    if (st != OK) { sp_unit_probe_free(p); return st; }
    *out = p;
    return OK;
}

static status_t x_blob_data(sp_ctx_t *c, const char *unit_id, const char *item, void **data, size_t *len)
{
    if (strcmp(item, "config") != 0) return set_error(ERR_NOT_FOUND, "lxc: no blob %s", item);
    char *d = NULL;
    status_t st = read_config(c, unit_id, &d, len);
    if (st == OK) *data = d;
    return st;
}

static status_t x_blob_read(sp_session_t *s, const sp_item_desc_t *item, void **data, size_t *len)
{
    if (strcmp(item->id, "config") != 0 || !s->config) return set_error(ERR_NOT_FOUND, "lxc: no blob %s", item->id);
    void *cp = malloc(s->config_len ? s->config_len : 1);
    if (!cp) return set_error(ERR_NOMEM, "lxc: blob");
    memcpy(cp, s->config, s->config_len);
    *data = cp;
    *len = s->config_len;
    return OK;
}

static void session_free(struct sp_session *s)
{
    if (!s) return;
    free(s->items);
    free(s->config);
    sp_session_info_free(&s->info);
    free(s);
}

static uint64_t ms_now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

static status_t x_begin(sp_ctx_t *c, const sp_unit_desc_t *unit, const sp_begin_req_t *req,
                        sp_session_t **out, const sp_session_info_t **info)
{
    struct sp_session *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "lxc: session");
    s->c = c;
    snprintf(s->name, sizeof(s->name), "%s", unit->unit_id);
    if (req->holds) { s->holds = *req->holds; s->has_holds = 1; }
    s->items = calloc(unit->n_items ? unit->n_items : 1, sizeof(*s->items));
    status_t st = s->items ? read_config(c, s->name, &s->config, &s->config_len) : set_error(ERR_NOMEM, "lxc: session");
    size_t fallback = 0;
    snap_probe_t *probe = NULL;
    if (st == OK) st = snap_probe_new(&c->snap, c->mounts.mountinfo, &probe);
    for (size_t i = 0; st == OK && i < unit->n_items; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_TREE || !it->source) continue;
        litem_t *x = &s->items[s->n_items++];
        snprintf(x->id, sizeof(x->id), "%s", it->id);
        snprintf(x->real, sizeof(x->real), "%s", it->source);
        snap_dom_t d;
        if ((st = snap_domain_of(&c->snap, x->real, &d)) != OK) break;
        if (d.kind == SNAP_NONE && d.unlisted[0])
            (void)sp_session_info_warn(&s->info, "%.300s is in ZFS dataset %.200s, which the helper's policy does "
                                       "not list in [snapshot] zfs_domains: read without a snapshot (pause-read)",
                                       x->real, d.unlisted);
        int nested = 0;
        if (d.kind != SNAP_NONE && (st = nested_below(probe, &d, x->real, &s->info, &nested)) != OK) break;
        if (nested) d.kind = SNAP_NONE;
        if (d.kind == SNAP_NONE) { x->live = 1; fallback++; continue; }
        st = snap_set_add(&s->snaps, &d);
    }
    snap_probe_free(probe);
    if (st != OK) { session_free(s); return st; }
    int before = state_of(c, s->name);
    int running = before == SP_STATE_RUNNING;
    int64_t hold = 0;
    int frozen = 0;
    uint64_t t0 = ms_now();
    if (running) {
        char detail[160];
        snprintf(detail, sizeof(detail), "lxc:%s", s->name);
        if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_FREEZE, detail, &hold)) != OK) { session_free(s); return st; }
        if (lxc(c, "lxc-freeze", s->name, NULL, NULL) == OK) {
            frozen = 1;
        } else {
            (void)sp_session_info_warn(&s->info, "lxc-freeze failed: %s", err_msg());
            err_clear();
            if (s->has_holds && hold && s->holds.clear(s->holds.ctx, hold) != OK) err_clear();
            hold = 0;
        }
    }
    st = s->snaps.n ? snap_take(&c->snap, &s->snaps, c->pol->daemon_id, req->job_id, s->has_holds ? &s->holds : NULL) : OK;
    char why[512] = "";
    if (st != OK) snprintf(why, sizeof(why), "%.500s", err_msg());
    int lost = 0;
    if (frozen) {
        /* No longer frozen: the watchdog thawed it, perhaps before the
         * snapshot, which then saw a running container. Its journal row
         * went with that release. */
        lost = state_of(c, s->name) != SP_STATE_PAUSED;
        if (lost) {
            if (s->has_holds && hold && s->holds.clear(s->holds.ctx, hold) != OK) err_clear();
        } else if (lxc(c, "lxc-unfreeze", s->name, NULL, NULL) == OK) {
            if (s->has_holds && hold && s->holds.clear(s->holds.ctx, hold) != OK) err_clear();
        } else {
            log_error("lxc: cannot unfreeze %s: %s; the watchdog retries", s->name, err_msg());
            err_clear();
        }
        s->info.freeze_ms = ms_now() - t0;
        if (lost) frozen = 0;
    }
    if (st != OK) { session_free(s); return set_error(st, "%s", why); }
    int after = state_of(c, s->name);
    unsigned instants = snap_instants(s->snaps.doms, s->snaps.n);
    sp_consistency_t lvl;
    if (before == SP_STATE_STOPPED && after == SP_STATE_STOPPED) lvl = SP_CONSIST_OFFLINE;
    else if (fallback || (instants > 1 && !frozen)) lvl = SP_CONSIST_INCONSISTENT;
    else lvl = SP_CONSIST_CRASH;
    if (fallback) (void)sp_session_info_warn(&s->info, "%zu mount(s) on no snapshot domain read live (fallback-live)", fallback);
    if (lost)
        (void)sp_session_info_warn(&s->info, "freeze on %s released by the watchdog within %llu ms; "
                                   "consistency is %s", s->name,
                                   (unsigned long long)s->info.freeze_ms, sp_consistency_name(lvl));
    s->info.achieved = lvl;
    s->info.consist_flags = (frozen ? SP_CF_FROZEN : 0) | (s->snaps.n ? SP_CF_FS_SNAPSHOT : 0) |
                            (lost ? SP_CF_WATCHDOG_RELEASED : 0);
    if (lvl < req->require) {
        (void)snap_release(&c->snap, &s->snaps, c->pol->daemon_id, s->has_holds ? &s->holds : NULL);
        session_free(s);
        return set_error(ERR_IO, "lxc: %s reached %s, below the %s required", unit->unit_id,
                         sp_consistency_name(lvl), sp_consistency_name(req->require));
    }
    *out = s;
    *info = &s->info;
    return OK;
}

static status_t x_tree_root(sp_session_t *s, const sp_item_desc_t *item, char *buf, size_t sz)
{
    for (size_t i = 0; i < s->n_items; i++) {
        if (strcmp(s->items[i].id, item->id) != 0) continue;
        if (s->items[i].live) {
            if (strlen(s->items[i].real) >= sz) return set_error(ERR_INVALID, "lxc: path too long");
            snprintf(buf, sz, "%s", s->items[i].real);
            return OK;
        }
        return snap_read_path(&s->c->snap, &s->snaps, s->items[i].real, buf, sz);
    }
    return set_error(ERR_NOT_FOUND, "lxc: no tree %s in this session", item->id);
}

static status_t x_end(sp_session_t *s, sp_end_outcome_t outcome, int64_t snapshot_id)
{
    (void)outcome; (void)snapshot_id;
    if (snap_release(&s->c->snap, &s->snaps, s->c->pol->daemon_id, s->has_holds ? &s->holds : NULL) != OK) {
        log_warn("lxc: %s", err_msg());
        err_clear();
    }
    session_free(s);
    return OK;
}

static status_t x_sweep(sp_ctx_t *c, const sp_sweep_req_t *req, sp_sweep_report_t *out)
{
    memset(out, 0, sizeof(*out));
    const char *d8 = req->daemon8 ? req->daemon8 : c->pol->daemon_id;
    for (size_t i = 0; i < req->n_intents; i++) {
        const sp_intent_t *in = &req->intents[i];
        status_t st;
        if (strcmp(in->kind, SP_HOLD_FREEZE) == 0 && strncmp(in->detail, "lxc:", 4) == 0 && valid_name(in->detail + 4)) {
            /* Unfreezing a container that is not frozen is harmless, and
             * reported absent: it may be frozen in a moment (provider.h). */
            int was = state_of(c, in->detail + 4) == SP_STATE_PAUSED;
            st = was ? lxc(c, "lxc-unfreeze", in->detail + 4, NULL, NULL) : OK;
            if (req->absent) req->absent[i] = !was;
        } else if (strcmp(in->kind, SP_HOLD_FS_SNAPSHOT) == 0) {
            st = snap_release_detail(&c->snap, in->detail, d8);
        } else {
            st = set_error(ERR_INVALID, "lxc: unknown hold %s", in->kind);
        }
        if (st == OK) { out->released++; if (req->released) req->released[i] = 1; }
        else { out->failed++; log_warn("lxc: sweep: %s", err_msg()); err_clear(); }
    }
    if (req->scan_artefacts) {
        unsigned removed = 0, foreign = 0;
        (void)snap_sweep(&c->snap, d8, req->live_jobs, req->n_live_jobs, &removed, &foreign);
        out->removed += removed;
        out->left_foreign += foreign;
    }
    return OK;
}

static const sp_provider_ops_t k_lxc_ops = {
    .name = "lxc", .abi = SP_ABI_VERSION,
    .init = x_init, .fini = x_fini, .probe = x_probe, .list_units = x_list,
    .describe = x_describe, .desc_free = sp_unit_desc_free, .begin = x_begin,
    .blob_read = x_blob_read, .tree_root = x_tree_root, .end = x_end, .sweep = x_sweep,
    .unit_probe = x_unit_probe,
};

/* ---- hooks: lxc-attach, in its own process group ---- */

struct helper_run { pid_t pid; int done, status; };

static status_t x_hook_start(sp_ctx_t *c, const char *unit_id, const policy_hook_t *h, int out_fd,
                             int err_fd, helper_run_t **out)
{
    if (!valid_name(unit_id)) return set_error(ERR_INVALID, "lxc: bad container name");
    if (h->container[0] && strcmp(h->container, unit_id) != 0)
        return set_error(ERR_INVALID, "lxc: hook %s names %s, not %s", h->name, h->container, unit_id);
    /* lxc-attach takes a uid, not a name the container resolves; the one
     * template with a user (nextcloud-maintenance) is not offered here. */
    if (h->user[0]) return set_error(ERR_INVALID, "lxc: hook %s runs as %s, which lxc-attach cannot name", h->name, h->user);
    char bin[HOST_TOOL_PATH_MAX + 16];
    snprintf(bin, sizeof(bin), "%s/lxc-attach", c->tools.lxc_dir);
    const char *argv[POLICY_ARGV_MAX + 16];
    size_t n = 0;
    if (c->tools.nsenter) {
        argv[n++] = c->tools.nsenter_bin;
        argv[n++] = "-t"; argv[n++] = "1"; argv[n++] = "-m"; argv[n++] = "-n"; argv[n++] = "--";
    }
    argv[n++] = bin;
    argv[n++] = "-P"; argv[n++] = c->pol->lxc_path; argv[n++] = "-n"; argv[n++] = unit_id; argv[n++] = "--";
    for (int i = 0; i < h->argc; i++) argv[n++] = h->argv[i];
    argv[n] = NULL;
    posix_spawn_file_actions_t fa;
    posix_spawnattr_t at;
    posix_spawn_file_actions_init(&fa);
    posix_spawnattr_init(&at);
    posix_spawnattr_setflags(&at, POSIX_SPAWN_SETPGROUP);
    posix_spawnattr_setpgroup(&at, 0);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    if (out_fd >= 0) posix_spawn_file_actions_adddup2(&fa, out_fd, 1);
    else posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0);
    if (err_fd >= 0) posix_spawn_file_actions_adddup2(&fa, err_fd, 2);
    struct helper_run *r = calloc(1, sizeof(*r));
    char *const *av;
    memcpy(&av, &(const char *const *){ argv }, sizeof(av));
    int rc = r ? posix_spawn(&r->pid, argv[0], &fa, &at, av, environ) : ENOMEM;
    posix_spawn_file_actions_destroy(&fa);
    posix_spawnattr_destroy(&at);
    if (rc != 0) { free(r); errno = rc; return set_error_errno(ERR_IO, "lxc: cannot run lxc-attach"); }
    *out = r;
    return OK;
}

static int x_hook_poll(helper_run_t *r, int *status)
{
    if (!r->done) {
        int st = 0;
        if (waitpid(r->pid, &st, WNOHANG) != r->pid) return 0;
        r->done = 1;
        r->status = WIFEXITED(st) ? WEXITSTATUS(st) : 128 + (WIFSIGNALED(st) ? WTERMSIG(st) : 0);
    }
    *status = r->status;
    return 1;
}

static void x_hook_kill(helper_run_t *r)
{
    if (r->done) return;
    kill(-r->pid, SIGTERM);
    for (int i = 0; i < 100; i++) {
        int s;
        if (x_hook_poll(r, &s)) return;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    kill(-r->pid, SIGKILL);
}

static void x_hook_free(helper_run_t *r)
{
    if (!r) return;
    if (!r->done) { kill(-r->pid, SIGKILL); waitpid(r->pid, NULL, 0); }
    free(r);
}

static int x_unit_state(sp_ctx_t *c, const char *unit_id) { return valid_name(unit_id) ? state_of(c, unit_id) : -1; }

static status_t x_set_running(sp_ctx_t *c, const char *unit_id, int run)
{
    if (!valid_name(unit_id)) return set_error(ERR_INVALID, "lxc: bad container name");
    /* Already so is done, as Docker's 304: lxc-stop of a stopped container
     * fails, and an in-place restore stops the unit whatever its state. */
    int s = state_of(c, unit_id);
    if (run ? s == SP_STATE_RUNNING : s == SP_STATE_STOPPED) return OK;
    return lxc(c, run ? "lxc-start" : "lxc-stop", unit_id, NULL, NULL);
}

/*
 * The snapshot domain holding a mount's bytes (B191), as Docker's: the
 * rootfs and every bind lxc.mount.entry source, by the real path describe
 * resolved. Without it the helper fell back to the policy's dir_domains,
 * which name no ZFS dataset or btrfs pool, and every in-place LXC restore
 * was refused as being in no snapshot domain.
 */
static status_t x_mount_root(sp_ctx_t *c, const char *mount, char *buf, size_t sz)
{
    snap_dom_t d;
    status_t st = snap_domain_of(&c->snap, mount, &d);
    if (st != OK) return st;
    if (d.kind == SNAP_NONE) return set_error(ERR_NOT_FOUND, "lxc: %s is in no snapshot domain", mount);
    snprintf(buf, sz, "%s", d.root);
    return OK;
}

/*
 * Restore define (container-backup.md 11.2): the recorded config written to
 * <lxcpath>/<name>/config with lxc.uts.name and lxc.rootfs.path rewritten
 * for the name, and every lxc.net.*.hwaddr dropped (LXC generates one)
 * unless keep_macs. The rootfs itself is data, restored to the rootfs path
 * the result names. A name in use is refused unless replacing a stopped
 * container, whose config is moved aside, never deleted.
 *   {"config": "<text>", "name": "", "replace": false, "keep_macs": false,
 *    "start": false}
 */
static status_t x_define(sp_ctx_t *c, const char *unit_id, const char *definition,
                         const helper_policy_t *pol, cJSON **result)
{
    (void)pol;
    *result = NULL;
    cJSON *d = cJSON_Parse(definition);
    const char *cfg = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(d, "config"));
    const char *want = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(d, "name"));
    int replace = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "replace"));
    int keep_macs = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "keep_macs"));
    int start = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "start"));
    char name[64];
    snprintf(name, sizeof(name), "%s", want && *want ? want : unit_id);
    status_t st = OK;
    if (!cfg) st = set_error(ERR_INVALID, "lxc: the definition has no config");
    else if (!valid_name(name)) st = set_error(ERR_INVALID, "lxc: bad container name");
    char dir[PATH_MAX], conf[PATH_MAX + 16], rootfs[PATH_MAX + 16], hp[PATH_MAX + HOST_TOOL_PATH_MAX + 16];
    snprintf(dir, sizeof(dir), "%.3900s/%s", c->pol->lxc_path, name);
    snprintf(conf, sizeof(conf), "%s/config", dir);
    snprintf(rootfs, sizeof(rootfs), "%s/rootfs", dir);
    /* Restored under a target root: the daemon says where the rootfs and
     * each bind source landed. Both must be clean absolute paths. */
    const char *rf = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(d, "rootfs"));
    if (st == OK && rf) {
        if (rf[0] != '/' || !helper_path_clean(rf)) st = set_error(ERR_INVALID, "lxc: bad rootfs path");
        else snprintf(rootfs, sizeof(rootfs), "%s", rf);
    }
    const cJSON *mmap = cJSON_GetObjectItemCaseSensitive(d, "mount_map");
    cJSON *res = cJSON_CreateObject();
    if (st == OK && !res) st = set_error(ERR_NOMEM, "lxc: define");
    struct stat sb;
    /* Where the old config went, so a failed write can put it back. */
    char ha[PATH_MAX + HOST_TOOL_PATH_MAX + 64] = "";
    /* The config's mode (B212): it can hold secrets (a hook's
     * environment, a key path), and fopen's 0666 under the helper's umask
     * wrote it 0644 where LXC keeps 0640. The snapshot records the config
     * as a blob without its mode, so the original's on the host decides:
     * the one replaced, else (a copy) the original unit's, never wider than
     * it was; 0640 when there is neither. */
    mode_t cmode = 0640;
    {
        char oc[PATH_MAX + 16], oh[PATH_MAX + HOST_TOOL_PATH_MAX + 16];
        struct stat ob;
        snprintf(oc, sizeof(oc), "%.3900s/%s/config", c->pol->lxc_path, valid_name(unit_id) ? unit_id : name);
        if (st == OK && host_path(&c->tools, conf, oh, sizeof(oh)) == 0 && lstat(oh, &ob) == 0 && S_ISREG(ob.st_mode))
            cmode = ob.st_mode & 0777;
        else if (st == OK && host_path(&c->tools, oc, oh, sizeof(oh)) == 0 && lstat(oh, &ob) == 0 && S_ISREG(ob.st_mode))
            cmode = ob.st_mode & 0777;
    }
    if (st == OK && host_path(&c->tools, conf, hp, sizeof(hp)) == 0 && lstat(hp, &sb) == 0) {
        int state = state_of(c, name);
        if (!replace) st = set_error(ERR_BUSY, "lxc: a container named %s exists; pass replace, or a new name", name);
        else if (state != SP_STATE_STOPPED) st = set_error(ERR_BUSY, "lxc: %s is not stopped; stop it before replacing it", name);
        else {
            char aside[PATH_MAX + 64];
            snprintf(aside, sizeof(aside), "%s.cbackup-old-%lld", conf, (long long)time(NULL));
            if (host_path(&c->tools, aside, ha, sizeof(ha)) != 0 || rename(hp, ha) != 0) {
                ha[0] = '\0';
                st = set_error_errno(ERR_IO, "lxc: moving %s aside", conf);
            } else {
                cJSON_AddStringToObject(res, "previous_config", aside);
            }
        }
    }
    /* Rewrite line by line; everything else is kept as recorded. */
    size_t lines = 0;
    for (const char *q = cfg; q && *q; q++) lines += *q == '\n';
    size_t cap = cfg ? strlen(cfg) + (lines + 4) * (PATH_MAX + 64) : 0;
    char *out = st == OK ? malloc(cap) : NULL;
    if (st == OK && !out) st = set_error(ERR_NOMEM, "lxc: config");
    size_t o = 0;
    for (const char *l = cfg; st == OK && l && *l; ) {
        size_t n = strcspn(l, "\n");
        char key[128] = "";
        const char *eq = memchr(l, '=', n);
        if (eq) {
            size_t k = (size_t)(eq - l);
            while (k && (l[k - 1] == ' ' || l[k - 1] == '\t')) k--;
            const char *ks = l;
            while (ks < l + k && (*ks == ' ' || *ks == '\t')) { ks++; }
            snprintf(key, sizeof(key), "%.*s", (int)(l + k - ks), ks);
        }
        if (strcmp(key, "lxc.uts.name") == 0) o += (size_t)snprintf(out + o, cap - o, "lxc.uts.name = %s\n", name);
        else if (strcmp(key, "lxc.rootfs.path") == 0) o += (size_t)snprintf(out + o, cap - o, "lxc.rootfs.path = dir:%s\n", rootfs);
        else if (!keep_macs && strncmp(key, "lxc.net.", 8) == 0 && strstr(key, ".hwaddr")) { /* LXC makes a new one */ }
        else if (strcmp(key, "lxc.mount.entry") == 0 && mmap && eq) {
            /* The source is the first field after '='. */
            const char *v = eq + 1;
            while (*v == ' ' || *v == '\t') v++;
            size_t sl = strcspn(v, " \t\n");
            char src[PATH_MAX];
            snprintf(src, sizeof(src), "%.*s", (int)sl, v);
            const char *to = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(mmap, src));
            if (to && to[0] == '/' && helper_path_clean(to))
                o += (size_t)snprintf(out + o, cap - o, "lxc.mount.entry = %s%.*s\n", to, (int)(l + n - (v + sl)), v + sl);
            else { memcpy(out + o, l, n); o += n; out[o++] = '\n'; }
        }
        else { memcpy(out + o, l, n); o += n; out[o++] = '\n'; }
        l += n + (l[n] == '\n');
    }
    if (st == OK) {
        char hd[PATH_MAX + HOST_TOOL_PATH_MAX];
        if (host_path(&c->tools, dir, hd, sizeof(hd)) != 0 || (mkdir(hd, 0755) != 0 && errno != EEXIST) ||
            host_path(&c->tools, conf, hp, sizeof(hp)) != 0)
            st = set_error_errno(ERR_IO, "lxc: creating %s", dir);
    }
    if (st == OK) st = host_write_new_file_mode(hp, conf, out, o, ha, cmode);
    free(out);
    if (st == OK && start) st = lxc(c, "lxc-start", name, NULL, NULL);
    if (st == OK) {
        cJSON_AddStringToObject(res, "name", name);
        cJSON_AddStringToObject(res, "config", conf);
        cJSON_AddStringToObject(res, "rootfs", rootfs);
        cJSON_AddBoolToObject(res, "started", start);
        *result = res;
        res = NULL;
    }
    cJSON_Delete(res);
    cJSON_Delete(d);
    return st;
}

/*
 * Where a database template runs (db-detection.md 7.2): the container
 * itself, and a path inside its rootfs as the container sees it. A
 * database on a bind mount entry is not mapped: lxc.mount.entry's target
 * is relative to the rootfs and not kept in the description.
 */
static status_t x_db_target(sp_ctx_t *c, const char *unit_id, const char *path, char *container,
                            size_t csz, char *cpath, size_t psz)
{
    snprintf(container, csz, "%s", unit_id);
    cpath[0] = '\0';
    if (!path) return OK;
    sp_unit_desc_t *d = NULL;
    status_t st = x_describe(c, unit_id, &d);
    if (st != OK) return st;
    st = set_error(ERR_NOT_FOUND, "lxc: %s is not inside the rootfs of %s", path, unit_id);
    for (size_t i = 0; i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        size_t l = it->tree_path ? strlen(it->tree_path) : 0;
        if (it->kind != SP_ITEM_TREE || strcmp(it->id, "rootfs") != 0 || !l ||
            strncmp(path, it->tree_path, l) != 0 || (path[l] != '\0' && path[l] != '/'))
            continue;
        snprintf(cpath, psz, "%s", path[l] ? path + l : "/");
        err_clear();
        st = OK;
    }
    sp_unit_desc_free(d);
    return st;
}

status_t lxc_backend_init(const helper_policy_t *pol, helper_backend_t *out)
{
    struct sp_ctx *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "lxc: backend");
    c->pol = pol;
    snap_host_setup(pol, &c->tools, &c->snap);
    c->mounts.tools = &c->tools;
    for (size_t i = 0; i < pol->n_include && i < MOUNT_ROOTS_MAX; i++) c->mounts.include_roots[c->mounts.n_include++] = pol->include_roots[i];
    snprintf(c->mounts.user_root, sizeof(c->mounts.user_root), "%s", pol->user_root);
    snprintf(c->mounts.mnt_root, sizeof(c->mounts.mnt_root), "%s", pol->mnt_root);
    if (pol->mountinfo[0]) snprintf(c->mounts.mountinfo, sizeof(c->mounts.mountinfo), "%s", pol->mountinfo);
    else mount_default_mountinfo(&c->tools, c->mounts.mountinfo, sizeof(c->mounts.mountinfo));
    *out = (helper_backend_t){
        .name = "lxc", .ops = &k_lxc_ops, .ctx = c,
        .blob_data = x_blob_data, .unit_state = x_unit_state, .set_running = x_set_running,
        .mount_root = x_mount_root, .hook_start = x_hook_start, .hook_poll = x_hook_poll, .hook_kill = x_hook_kill, .hook_free = x_hook_free,
        .define = x_define, .db_target = x_db_target,
    };
    return OK;
}

void lxc_backend_fini(helper_backend_t *b)
{
    if (!b || !b->ctx) return;
    free(b->ctx);
    b->ctx = NULL;
}
