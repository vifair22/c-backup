#define _GNU_SOURCE

#include "lxc_backend.h"
#include "helper_proto.h"
#include "mounts.h"
#include "snap.h"

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
    /* LXC's command socket is abstract, in the host's network namespace. */
    return host_run(&c->tools, 1, argv, LXC_TIMEOUT_S, out, 1024 * 1024);
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
    if (host_run(&c->tools, 1, argv, LXC_TIMEOUT_S, &v, 256) != OK) {
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

static status_t add_tree(struct sp_ctx *c, sp_unit_desc_t *d, size_t *cap, const char *id, const char *src)
{
    mount_class_t mc;
    status_t st = mount_classify(&c->mounts, "bind", src, NULL, &mc);
    if (st != OK) return st;
    if (mc.verdict == MOUNT_EXCLUDED && strcmp(id, "rootfs") != 0) return OK;
    if (strcmp(id, "rootfs") == 0 && mc.verdict == MOUNT_EXCLUDED) {
        /* The rootfs is the unit: always data, wherever it is. */
        mount_cfg_t all = c->mounts;
        char root[] = "/";
        all.include_roots[0] = root;
        all.n_include = 1;
        if ((st = mount_classify(&all, "bind", src, NULL, &mc)) != OK) return st;
    }
    snap_dom_t dom;
    if ((st = snap_domain_of(&c->snap, mc.real, &dom)) != OK) return st;
    return add_item(d, cap, SP_ITEM_TREE, id, mc.recorded, mc.real, 0, dom.kind != SNAP_NONE ? SP_CAP_SNAPSHOT : 0);
}

static status_t x_describe(sp_ctx_t *c, const char *unit_id, sp_unit_desc_t **out)
{
    if (!valid_name(unit_id)) return set_error(ERR_INVALID, "lxc: bad container name");
    char *cfg = NULL;
    size_t clen = 0;
    status_t st = read_config(c, unit_id, &cfg, &clen);
    if (st != OK) return st;
    sp_unit_desc_t *d = calloc(1, sizeof(*d));
    size_t cap = 0;
    if (!d || !(d->name = strdup(unit_id))) { free(cfg); free(d); return set_error(ERR_NOMEM, "lxc: describe"); }
    snprintf(d->provider, sizeof(d->provider), "lxc");
    snprintf(d->unit_id, sizeof(d->unit_id), "%s", unit_id);
    int s = state_of(c, unit_id);
    d->state = s < 0 ? SP_STATE_UNKNOWN : s;
    d->caps = SP_CAP_PAUSE;
    st = add_item(d, &cap, SP_ITEM_BLOB, "config", ".cbackup-unit/config/lxc/config", NULL, clen, 0);
    char *save = NULL;
    int nmount = 0;
    for (char *l = strtok_r(cfg, "\n", &save); l && st == OK; l = strtok_r(NULL, "\n", &save)) {
        char *eq = strchr(l, '=');
        if (!eq || l[0] == '#') continue;
        *eq = '\0';
        char *k = trim(l), *v = trim(eq + 1);
        if (strcmp(k, "lxc.rootfs.path") == 0) {
            if (strncmp(v, "dir:", 4) == 0) v += 4;
            if (v[0] != '/' || !helper_path_clean(v)) { st = set_error(ERR_INVALID, "lxc: %s: unsupported rootfs %s", unit_id, v); break; }
            st = add_tree(c, d, &cap, "rootfs", v);
        } else if (strcmp(k, "lxc.mount.entry") == 0) {
            /* <source> <target> <type> <options>: bind sources only. */
            char src[PATH_MAX];
            if (sscanf(v, "%4095s", src) != 1 || src[0] != '/' || !strstr(v, "bind")) continue;
            char id[SP_ITEM_ID_MAX];
            snprintf(id, sizeof(id), "mount:%d", nmount++);
            st = add_tree(c, d, &cap, id, src);
        }
    }
    free(cfg);
    if (st != OK) { sp_unit_desc_free(d); return st; }
    *out = d;
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
    for (size_t i = 0; st == OK && i < unit->n_items; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_TREE || !it->source) continue;
        litem_t *x = &s->items[s->n_items++];
        snprintf(x->id, sizeof(x->id), "%s", it->id);
        snprintf(x->real, sizeof(x->real), "%s", it->source);
        snap_dom_t d;
        if ((st = snap_domain_of(&c->snap, x->real, &d)) != OK) break;
        if (d.kind == SNAP_NONE) { x->live = 1; fallback++; continue; }
        st = snap_set_add(&s->snaps, &d);
    }
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
            (void)sp_session_info_warn(&s->info, "lxc-freeze failed: %.300s", err_msg());
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
};

/* ---- hooks: lxc-attach, in its own process group ---- */

struct helper_run { pid_t pid; int done, status; };

static status_t x_hook_start(sp_ctx_t *c, const char *unit_id, const policy_hook_t *h, int out_fd,
                             int err_fd, helper_run_t **out)
{
    if (!valid_name(unit_id)) return set_error(ERR_INVALID, "lxc: bad container name");
    if (h->container[0] && strcmp(h->container, unit_id) != 0)
        return set_error(ERR_INVALID, "lxc: hook %s names %s, not %s", h->name, h->container, unit_id);
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
    return lxc(c, run ? "lxc-start" : "lxc-stop", unit_id, NULL, NULL);
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
    if (st == OK && host_path(&c->tools, conf, hp, sizeof(hp)) == 0 && lstat(hp, &sb) == 0) {
        int state = state_of(c, name);
        if (!replace) st = set_error(ERR_BUSY, "lxc: a container named %s exists; pass replace, or a new name", name);
        else if (state != SP_STATE_STOPPED) st = set_error(ERR_BUSY, "lxc: %s is not stopped; stop it before replacing it", name);
        else {
            char aside[PATH_MAX + 64], ha[PATH_MAX + HOST_TOOL_PATH_MAX + 64];
            snprintf(aside, sizeof(aside), "%s.cbackup-old-%lld", conf, (long long)time(NULL));
            if (host_path(&c->tools, aside, ha, sizeof(ha)) != 0 || rename(hp, ha) != 0)
                st = set_error_errno(ERR_IO, "lxc: moving %s aside", conf);
            else cJSON_AddStringToObject(res, "previous_config", aside);
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
    FILE *f = st == OK ? fopen(hp, "wxe") : NULL;
    if (st == OK && !f) st = set_error_errno(ERR_IO, "lxc: writing %s", conf);
    if (f) {
        if (fwrite(out, 1, o, f) != o) st = set_error_errno(ERR_IO, "lxc: writing %s", conf);
        if (fclose(f) != 0 && st == OK) st = set_error_errno(ERR_IO, "lxc: writing %s", conf);
    }
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
        .hook_start = x_hook_start, .hook_poll = x_hook_poll, .hook_kill = x_hook_kill, .hook_free = x_hook_free,
        .define = x_define,
    };
    return OK;
}

void lxc_backend_fini(helper_backend_t *b)
{
    if (!b || !b->ctx) return;
    free(b->ctx);
    b->ctx = NULL;
}
