#define _POSIX_C_SOURCE 200809L

#include "provider.h"
#include "msg_clip.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *const k_consist[] = {
    "inconsistent", "crash", "quiesced", "application", "offline",
};

const char *sp_consistency_name(sp_consistency_t c)
{
    if ((unsigned)c >= sizeof(k_consist) / sizeof(k_consist[0])) return NULL;
    return k_consist[c];
}

int sp_consistency_parse(const char *name, sp_consistency_t *out)
{
    if (!name) return -1;
    for (unsigned i = 0; i < sizeof(k_consist) / sizeof(k_consist[0]); i++)
        if (strcmp(name, k_consist[i]) == 0) { *out = (sp_consistency_t)i; return 0; }
    return -1;
}

const char *sp_item_kind_name(sp_item_kind_t k)
{
    switch (k) {
    case SP_ITEM_BLOB:  return "blob";
    case SP_ITEM_BLOCK: return "block";
    case SP_ITEM_TREE:  return "tree";
    case SP_ITEM_STREAM: return "stream";
    }
    return NULL;
}

int sp_item_kind_parse(const char *name, sp_item_kind_t *out)
{
    if (!name) return -1;
    if (strcmp(name, "blob") == 0)  { *out = SP_ITEM_BLOB;  return 0; }
    if (strcmp(name, "block") == 0) { *out = SP_ITEM_BLOCK; return 0; }
    if (strcmp(name, "tree") == 0)  { *out = SP_ITEM_TREE;  return 0; }
    if (strcmp(name, "stream") == 0) { *out = SP_ITEM_STREAM; return 0; }
    return -1;
}

const char *sp_state_name(int state)
{
    switch (state) {
    case SP_STATE_RUNNING:   return "running";
    case SP_STATE_PAUSED:    return "paused";
    case SP_STATE_STOPPED:   return "stopped";
    case SP_STATE_SUSPENDED: return "suspended";
    default:                 return "unknown";
    }
}

const char *sp_read_mode_name(sp_read_mode_t m)
{
    switch (m) {
    case SP_READ_FULL:        return "full";
    case SP_READ_INCREMENTAL: return "incremental";
    case SP_READ_UNCHANGED:   return "unchanged";
    }
    return "full";
}

/* Section 6.4. Bounded so a reason can label a metric. */
const char *sp_reason_canonical(const char *reason)
{
    static const char *const k[] = {
        "first", "no-incremental-support", "no-base", "base-pruned",
        "token-missing", "token-invalid", "item-new", "item-resized",
        "item-source-changed", "unit-redefined", "audit", "delta-too-costly",
        "forced", "unchanged-offline", "incremental",
    };
    if (!reason) return NULL;
    for (size_t i = 0; i < sizeof(k) / sizeof(k[0]); i++)
        if (strcmp(reason, k[i]) == 0) return k[i];
    return NULL;
}

int sp_reason_known(const char *reason) { return sp_reason_canonical(reason) != NULL; }

void sp_session_info_free(sp_session_info_t *info)
{
    if (!info) return;
    free(info->plans);
    free(info->new_token);
    free(info->new_token_blob);
    for (size_t i = 0; i < info->n_warnings; i++) free(info->warnings[i]);
    free(info->warnings);
    memset(info, 0, sizeof(*info));
}

status_t sp_session_info_warn(sp_session_info_t *info, const char *fmt, ...)
{
    char buf[MSG_CLIP_MAX];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    msg_clip_after(buf, sizeof(buf), n);
    char **grown = realloc(info->warnings, (info->n_warnings + 1) * sizeof(*grown));
    if (!grown) return set_error(ERR_NOMEM, "provider: warning");
    info->warnings = grown;
    if (!(grown[info->n_warnings] = strdup(buf)))
        return set_error(ERR_NOMEM, "provider: warning");
    info->n_warnings++;
    return OK;
}

void sp_unit_desc_free(sp_unit_desc_t *d)
{
    if (!d) return;
    for (size_t i = 0; i < d->n_items; i++) {
        free(d->items[i].source);
        free(d->items[i].tree_path);
    }
    free(d->items);
    free(d->name);
    free(d);
}

void sp_unit_probe_free(sp_unit_probe_t *p)
{
    if (!p) return;
    for (size_t i = 0; i < p->n; i++) {
        sp_probe_container_t *c = &p->v[i];
        for (size_t k = 0; k < c->n_open; k++) free(c->open[k].path);
        free(c->open);
        free(c->procs);
        free(c->ports);
    }
    free(p->v);
    free(p);
}

status_t sp_unit_desc_dup(const sp_unit_desc_t *in, sp_unit_desc_t **out)
{
    sp_unit_desc_t *d = calloc(1, sizeof(*d));
    if (!d) return set_error(ERR_NOMEM, "provider: unit description");
    memcpy(d->provider, in->provider, sizeof(d->provider));
    memcpy(d->unit_id, in->unit_id, sizeof(d->unit_id));
    d->state = in->state;
    d->caps = in->caps;
    d->name = strdup(in->name ? in->name : "");
    d->items = in->n_items ? calloc(in->n_items, sizeof(*d->items)) : NULL;
    if (!d->name || (in->n_items && !d->items)) {
        sp_unit_desc_free(d);
        return set_error(ERR_NOMEM, "provider: unit description");
    }
    for (size_t i = 0; i < in->n_items; i++) {
        d->items[i] = in->items[i];
        d->items[i].source = in->items[i].source ? strdup(in->items[i].source) : NULL;
        d->items[i].tree_path = in->items[i].tree_path ? strdup(in->items[i].tree_path)
                                                       : NULL;
        d->n_items = i + 1;
        if ((in->items[i].source && !d->items[i].source) ||
            (in->items[i].tree_path && !d->items[i].tree_path)) {
            sp_unit_desc_free(d);
            return set_error(ERR_NOMEM, "provider: unit description");
        }
    }
    *out = d;
    return OK;
}
