#define _GNU_SOURCE

#include "paths_backend.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct sp_ctx {
    const helper_policy_t *pol;
    host_tools_t           tools;
    snap_ctx_t             snap;
    mount_cfg_t            mounts;
};

/* The holds a paths capture journals are its snapshots; the leftovers are
 * this daemon's snapshots in the policy's domains. */
static status_t p_sweep(sp_ctx_t *c, const sp_sweep_req_t *req, sp_sweep_report_t *out)
{
    memset(out, 0, sizeof(*out));
    const char *d8 = req->daemon8 ? req->daemon8 : c->pol->daemon_id;
    for (size_t i = 0; i < req->n_intents; i++) {
        const sp_intent_t *in = &req->intents[i];
        status_t st = strcmp(in->kind, SP_HOLD_FS_SNAPSHOT) == 0
                          ? snap_release_detail(&c->snap, in->detail, d8)
                          : set_error(ERR_INVALID, "paths: unknown hold %s", in->kind);
        if (st == OK) { out->released++; if (req->released) req->released[i] = 1; }
        else { out->failed++; log_warn("paths: sweep: %s", err_msg()); err_clear(); }
    }
    if (req->scan_artefacts && (c->pol->n_zfs || c->pol->n_btrfs)) {
        unsigned removed = 0, foreign = 0;
        (void)snap_sweep(&c->snap, d8, req->live_jobs, req->n_live_jobs, &removed, &foreign);
        out->removed += removed;
        out->left_foreign += foreign;
    }
    return OK;
}

static const sp_provider_ops_t k_paths_ops = {
    .name = PATHS_BACKEND_NAME,
    .abi = SP_ABI_VERSION,
    .sweep = p_sweep,
};

status_t paths_backend_init(const helper_policy_t *pol, helper_backend_t *out)
{
    struct sp_ctx *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "paths: backend");
    c->pol = pol;
    snap_host_setup(pol, &c->tools, &c->snap);
    c->mounts.tools = &c->tools;
    snprintf(c->mounts.user_root, sizeof(c->mounts.user_root), "%s", pol->user_root);
    snprintf(c->mounts.mnt_root, sizeof(c->mounts.mnt_root), "%s", pol->mnt_root);
    if (pol->mountinfo[0]) snprintf(c->mounts.mountinfo, sizeof(c->mounts.mountinfo), "%s", pol->mountinfo);
    else mount_default_mountinfo(&c->tools, c->mounts.mountinfo, sizeof(c->mounts.mountinfo));
    *out = (helper_backend_t){ .name = PATHS_BACKEND_NAME, .ops = &k_paths_ops, .ctx = c, .internal = 1 };
    return OK;
}

void paths_backend_fini(helper_backend_t *b)
{
    if (!b || !b->ctx) return;
    free(b->ctx);
    b->ctx = NULL;
}

int paths_backend_any_domain(const helper_backend_t *b)
{
    return b && b->ctx && (b->ctx->pol->n_zfs > 0 || b->ctx->pol->n_btrfs > 0);
}

const snap_ctx_t *paths_snap(const helper_backend_t *b)
{
    return &b->ctx->snap;
}

void paths_res_free(paths_res_t *r)
{
    for (size_t i = 0; i < r->n_nested; i++) free(r->nested[i]);
    r->n_nested = 0;
}

status_t paths_resolve(const helper_backend_t *b, const char *path, paths_res_t *out)
{
    const struct sp_ctx *c = b->ctx;
    memset(out, 0, sizeof(*out));
    mount_class_t mc;
    status_t st = mount_resolve(&c->mounts, path, &mc);
    if (st != OK) {
        /* Missing, or unresolvable: the daemon's own read reports it. */
        snprintf(out->reason, sizeof(out->reason), "cannot be resolved on the host: %.250s", err_msg());
        err_clear();
        return OK;
    }
    if (mc.verdict == MOUNT_EXCLUDED) {
        snprintf(out->reason, sizeof(out->reason), "is a host facility (/dev, /proc, /etc, ...), never snapshotted");
        return OK;
    }
    snprintf(out->real, sizeof(out->real), "%s", mc.real);
    if (strcmp(mc.reason, "share-split") == 0) {
        snprintf(out->reason, sizeof(out->reason), "is split across more than one pool or disk, "
                 "so no one snapshot covers it");
        return OK;
    }
    if ((st = snap_domain_of(&c->snap, mc.real, &out->dom)) != OK) return st;
    if (out->dom.kind == SNAP_NONE) {
        snprintf(out->reason, sizeof(out->reason), "%s%.200s is on no ZFS dataset or btrfs pool the helper's "
                 "policy lists%s", strcmp(mc.real, path) ? "resolved to " : "", mc.real,
                 strcmp(mc.reason, "array") == 0 ? " (an array disk)" : "");
        return OK;
    }
    if (c->mounts.mountinfo[0] &&
        mount_below(c->mounts.mountinfo, mc.real, out->nested, PATHS_NESTED_MAX, &out->n_nested,
                    &out->nested_total) != OK) {
        /* Without the list the daemon cannot know what the snapshot hides;
         * reading live is the safe answer. */
        snprintf(out->reason, sizeof(out->reason), "the host's mount table cannot be read (%.200s)", err_msg());
        err_clear();
        memset(&out->dom, 0, sizeof(out->dom));
    }
    return OK;
}
