#define _POSIX_C_SOURCE 200809L

#include "unit_backup.h"
#include "backup.h"
#include "block_source.h"
#include "registry.h"
#include "reserved.h"
#include "unit_index.h"
#include "unit_json.h"
#include "unit_state.h"

#include <cutils/log.h>

#include <fnmatch.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define UNIT_ITEMS_MAX 64u

/* ------------------------------------------------------------------ */
/* Selection                                                           */
/* ------------------------------------------------------------------ */

typedef struct {
    char  id[SP_UNIT_ID_MAX];
    char *name;
    int   state;
} listed_t;

typedef struct {
    listed_t *v;
    size_t    n, cap;
} listing_t;

static status_t listing_add(void *ctx, const char *id, const char *name, int state)
{
    listing_t *l = ctx;
    if (!id || !*id || strlen(id) >= SP_UNIT_ID_MAX)
        return set_error(ERR_INVALID, "provider listed a unit id that is empty or too long");
    if (l->n == l->cap) {
        size_t nc = l->cap ? l->cap * 2 : 16;
        listed_t *g = realloc(l->v, nc * sizeof(*g));
        if (!g) return set_error(ERR_NOMEM, "units: listing");
        l->v = g;
        l->cap = nc;
    }
    listed_t *u = &l->v[l->n];
    snprintf(u->id, sizeof(u->id), "%s", id);
    if (!(u->name = strdup(name ? name : id))) return set_error(ERR_NOMEM, "units: listing");
    u->state = state;
    l->n++;
    return OK;
}

static void listing_free(listing_t *l)
{
    for (size_t i = 0; i < l->n; i++) free(l->v[i].name);
    free(l->v);
}

static int excluded(const unit_backup_opts_t *o, const char *name)
{
    for (size_t i = 0; i < o->n_exclude; i++)
        if (o->exclude[i] && fnmatch(o->exclude[i], name, 0) == 0) return 1;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Validating what a provider describes                                */
/* ------------------------------------------------------------------ */

static int path_clean(const char *p)
{
    if (!p || !*p || strlen(p) >= PATH_MAX) return 0;
    if (strstr(p, "//") || strstr(p, "/./") || strstr(p, "/../")) return 0;
    size_t n = strlen(p);
    if (p[n - 1] == '/') return 0;
    if (strncmp(p, "./", 2) == 0 || strncmp(p, "../", 3) == 0) return 0;
    if (n >= 2 && strcmp(p + n - 2, "/.") == 0) return 0;
    if (n >= 3 && strcmp(p + n - 3, "/..") == 0) return 0;
    return 1;
}

/* One path a prefix of the other at a component boundary. */
static int nested(const char *a, const char *b)
{
    size_t na = strlen(a), nb = strlen(b);
    const char *s = na <= nb ? a : b, *l = na <= nb ? b : a;
    size_t ns = na <= nb ? na : nb;
    return strncmp(s, l, ns) == 0 && (l[ns] == '\0' || l[ns] == '/');
}

/*
 * Everything the assembler is about to write is checked first, because the
 * provider's description decides paths in the snapshot: BLOB and BLOCK
 * items inside the reserved directory and nowhere else, never unit.json;
 * TREE items absolute, clean, not a reserved name and not inside each
 * other (two scans of one tree would record it twice).
 */
static status_t validate(const sp_unit_desc_t *d)
{
    if (d->n_items > UNIT_ITEMS_MAX)
        return set_error(ERR_INVALID, "unit %s has %zu items, over %u", d->unit_id,
                         d->n_items, UNIT_ITEMS_MAX);
    static const char unit_dir[] = RESERVED_UNIT_DIR "/";
    for (size_t i = 0; i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        if (!it->id[0] || strchr(it->id, '/') || memchr(it->id, '\0', sizeof(it->id)) == NULL)
            return set_error(ERR_INVALID, "unit %s: an item id is empty or has a '/'", d->unit_id);
        for (size_t k = 0; k < i; k++)
            if (strcmp(d->items[k].id, it->id) == 0)
                return set_error(ERR_INVALID, "unit %s: item id %s twice", d->unit_id, it->id);
        const char *tp = it->tree_path;
        if (!path_clean(tp))
            return set_error(ERR_INVALID, "unit %s: item %s has no clean path", d->unit_id, it->id);
        if (it->kind == SP_ITEM_BLOB || it->kind == SP_ITEM_BLOCK) {
            if (strncmp(tp, unit_dir, sizeof(unit_dir) - 1) != 0 || tp[sizeof(unit_dir) - 1] == '\0' ||
                strcmp(tp, RESERVED_UNIT_DIR "/unit.json") == 0)
                return set_error(ERR_INVALID, "unit %s: item %s would land at %s, outside "
                                 "/%s or on unit.json", d->unit_id, it->id, tp, RESERVED_UNIT_DIR);
            if (it->kind == SP_ITEM_BLOB && it->size > SP_BLOB_MAX)
                return set_error(ERR_INVALID, "unit %s: blob %s is over 16 MiB", d->unit_id, it->id);
        } else if (it->kind == SP_ITEM_TREE) {
            char why[PATH_MAX + 160];
            if (tp[0] != '/' || tp[1] == '\0')
                return set_error(ERR_INVALID, "unit %s: tree %s must be recorded at an "
                                 "absolute path other than /", d->unit_id, it->id);
            if (reserved_source_refused(tp, why, sizeof(why)) != 0)
                return set_error(ERR_INVALID, "unit %s: %s", d->unit_id, why);
        } else {
            return set_error(ERR_INVALID, "unit %s: item %s of unknown kind %d", d->unit_id,
                             it->id, (int)it->kind);
        }
        for (size_t k = 0; k < i; k++) {
            const char *op = d->items[k].tree_path;
            int both_tree = it->kind == SP_ITEM_TREE && d->items[k].kind == SP_ITEM_TREE;
            if (strcmp(op, tp) == 0 || (both_tree && nested(op, tp)))
                return set_error(ERR_INVALID, "unit %s: items %s and %s overlap at %s",
                                 d->unit_id, d->items[k].id, it->id, tp);
        }
    }
    return OK;
}

/* ------------------------------------------------------------------ */
/* One session's content                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    const uint8_t *d;
    size_t         n;
    int            done;
} memsrc_t;

static status_t mem_next(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    memsrc_t *m = ctx;
    *got = !m->done && m->n > 0;
    if (*got) { *s = 0; *e = m->n; m->done = 1; }
    return OK;
}

static status_t mem_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    memsrc_t *m = ctx;
    if (off > m->n || n > m->n - off) return set_error(ERR_INVALID, "blob read past its end");
    memcpy(buf, m->d + off, n);
    return OK;
}

typedef struct {
    int               item;        /* index into desc->items; -1 for unit.json */
    void             *blob;
    size_t            blob_len;
    memsrc_t          mem;
    sp_block_stream_t bs;
    int               bs_open;
    block_source_t   *src;
    uint64_t          bytes_read;
} part_t;

typedef struct {
    const sp_provider_ops_t  *ops;
    sp_session_t             *sess;
    const sp_session_info_t  *info;
    const sp_unit_desc_t     *desc;
    part_t                   *parts;
    size_t                    n_parts;
    unit_json_t               uj;
    char                     *uj_text;
    uint64_t                  bytes_read;
} uctx_t;

static const sp_item_plan_t *plan_for(const uctx_t *u, const char *item)
{
    for (size_t i = 0; u->info && i < u->info->n_plans; i++)
        if (u->info->plans[i].item_id && strcmp(u->info->plans[i].item_id, item) == 0)
            return &u->info->plans[i];
    return NULL;
}

/* unit.json, from the description, the session and what each part read. */
static status_t build_unit_json(uctx_t *u)
{
    unit_json_t *j = &u->uj;
    memset(j, 0, sizeof(*j));
    j->version = UNIT_JSON_VERSION;
    snprintf(j->provider, sizeof(j->provider), "%s", u->desc->provider);
    snprintf(j->unit_id, sizeof(j->unit_id), "%s", u->desc->unit_id);
    j->name = strdup(u->desc->name ? u->desc->name : "");
    j->consistency = u->info->achieved;
    j->consist_flags = u->info->consist_flags;
    j->freeze_ms = u->info->freeze_ms;
    j->pause_ms = u->info->pause_ms;
    j->items = calloc(u->desc->n_items ? u->desc->n_items : 1, sizeof(*j->items));
    j->warnings = calloc(u->info->n_warnings ? u->info->n_warnings : 1, sizeof(char *));
    if (!j->name || !j->items || !j->warnings) return set_error(ERR_NOMEM, "unit.json");
    for (size_t i = 0; i < u->desc->n_items; i++) {
        const sp_item_desc_t *it = &u->desc->items[i];
        unit_json_item_t *o = &j->items[j->n_items++];
        snprintf(o->id, sizeof(o->id), "%s", it->id);
        snprintf(o->kind, sizeof(o->kind), "%s", sp_item_kind_name(it->kind));
        o->path = strdup(it->tree_path);
        o->source = it->source ? strdup(it->source) : NULL;
        if (!o->path || (it->source && !o->source)) return set_error(ERR_NOMEM, "unit.json");
        o->size = it->size;
        snprintf(o->format, sizeof(o->format), "%s", it->format);
        /* This build reads everything in full; a plan for more is noted as
         * forced, never claimed. */
        const sp_item_plan_t *pl = plan_for(u, it->id);
        snprintf(o->read_mode, sizeof(o->read_mode), "full");
        const char *why = pl && pl->mode == SP_READ_FULL && sp_reason_known(pl->reason)
                        ? pl->reason : "forced";
        snprintf(o->reason, sizeof(o->reason), "%s", why);
        for (size_t p = 0; p < u->n_parts; p++)
            if (u->parts[p].item == (int)i) o->bytes_read = u->parts[p].bytes_read;
        if (it->kind == SP_ITEM_BLOB) o->size = 0;
    }
    for (size_t p = 0; p < u->n_parts; p++)
        if (u->parts[p].item >= 0 && u->desc->items[u->parts[p].item].kind == SP_ITEM_BLOB)
            j->items[u->parts[p].item].size = u->parts[p].blob_len;
    for (size_t i = 0; i < u->info->n_warnings; i++) {
        if (!(j->warnings[i] = strdup(u->info->warnings[i]))) return set_error(ERR_NOMEM, "unit.json");
        j->n_warnings++;
    }
    return OK;
}

static status_t part_open(void *ctx, size_t index, cdc_source_t *src, uint64_t *size)
{
    uctx_t *u = ctx;
    part_t *p = &u->parts[index];
    memset(src, 0, sizeof(*src));
    if (p->item < 0) {
        status_t st = build_unit_json(u);
        if (st != OK) return st;
        size_t len = 0;
        if (!(u->uj_text = unit_json_encode(&u->uj, &len))) return ERR_NOMEM;
        p->mem = (memsrc_t){ (const uint8_t *)u->uj_text, len, 0 };
        *src = (cdc_source_t){ mem_next, mem_read, &p->mem, NULL };
        *size = len;
        return OK;
    }
    const sp_item_desc_t *it = &u->desc->items[p->item];
    if (it->kind == SP_ITEM_BLOB) {
        status_t st = u->ops->blob_read(u->sess, it, &p->blob, &p->blob_len);
        if (st != OK) return st;
        if (p->blob_len > SP_BLOB_MAX)
            return set_error(ERR_TOO_LARGE, "blob %s is %zu bytes, over 16 MiB", it->id, p->blob_len);
        p->mem = (memsrc_t){ p->blob, p->blob_len, 0 };
        *src = (cdc_source_t){ mem_next, mem_read, &p->mem, NULL };
        *size = p->blob_len;
        return OK;
    }
    status_t st = u->ops->block_open(u->sess, it, &p->bs);
    if (st != OK) return st;
    p->bs_open = 1;
    block_source_opts_t bo = { 1, units_config()->trust_zero, 0 };
    st = block_source_open(&p->bs, it->size, &bo, &p->src);
    if (st != OK) return st;
    *src = block_source_iface(p->src);
    *size = it->size;
    return OK;
}

static void part_release(part_t *p)
{
    if (p->src) {
        block_source_stats_t bs;
        block_source_stats(p->src, &bs);
        p->bytes_read = bs.bytes_read;
        block_source_close(p->src);
        p->src = NULL;
    }
    if (p->bs_open && p->bs.close) p->bs.close(p->bs.ctx);
    p->bs_open = 0;
    free(p->blob);
    p->blob = NULL;
}

static void part_close(void *ctx, size_t index, status_t st, const cdc_result_t *cr)
{
    (void)st; (void)cr;
    uctx_t *u = ctx;
    part_release(&u->parts[index]);
    u->bytes_read += u->parts[index].bytes_read;
}

typedef struct {
    uctx_t      *u;
    int64_t      repo_id;
} hook_t;

static status_t index_hook(void *ctx, cutils_db_t *db, int64_t snap_id)
{
    hook_t *h = ctx;
    return unit_index_put(db, h->repo_id, snap_id, &h->u->uj);
}

/* Progress, with the unit in the phase: "unit 3/12 win11-test: store". */
typedef struct {
    const unit_backup_opts_t *o;
    char                      prefix[300];
    char                      phase[400];
} prog_t;

static void prog_cb(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    prog_t *p = ctx;
    snprintf(p->phase, sizeof(p->phase), "%s: %s", p->prefix, phase);
    p->o->progress_cb(p->phase, done, total, p->o->progress_ctx);
}

static void prog_bytes(void *ctx, uint64_t done, uint64_t total)
{
    prog_t *p = ctx;
    if (p->o->bytes_cb) p->o->bytes_cb(p->o->progress_ctx, done, total);
}

/* ------------------------------------------------------------------ */
/* One unit                                                            */
/* ------------------------------------------------------------------ */

static status_t run_one(store_t *store, cutils_db_t *db, int64_t repo_id,
                        const unit_backup_opts_t *o, const sp_provider_ops_t *ops,
                        sp_ctx_t *pctx, const listed_t *lu, size_t index, size_t total,
                        unit_backup_one_t *res)
{
    char key[UNIT_KEY_MAX];
    unit_key_format(key, sizeof(key), ops->name, lu->id);
    snprintf(res->unit_key, sizeof(res->unit_key), "%s", key);
    snprintf(res->name, sizeof(res->name), "%s", lu->name);

    status_t st = units_lock(key, units_config()->unit_wait_s, o->cancel);
    if (st != OK) return st;

    sp_unit_desc_t *d = NULL;
    st = ops->describe(pctx, lu->id, &d);
    if (st == OK && !units_allowed(ops->name, d->unit_id, d->name)) {
        /* Renamed out of the allow list since it was listed. */
        st = set_error(ERR_INVALID, "unit %s (%s) is not in units.allow", d->name, key);
    }
    if (st == OK) st = validate(d);
    if (st != OK) {
        if (d) ops->desc_free(d);
        units_unlock(key);
        return st;
    }
    if (d->name) snprintf(res->name, sizeof(res->name), "%s", d->name);

    units_journal_t jr;
    memset(&jr, 0, sizeof(jr));
    jr.db = db;
    snprintf(jr.provider, sizeof(jr.provider), "%s", ops->name);
    snprintf(jr.unit_id, sizeof(jr.unit_id), "%s", d->unit_id);
    jr.job_id = o->job_id;
    sp_holds_t holds = units_journal_holds(&jr);
    sp_begin_req_t req;
    memset(&req, 0, sizeof(req));
    req.job_id = o->job_id;
    req.want = SP_CONSIST_APPLICATION;
    req.require = o->require;
    req.freeze_timeout_s = units_config()->freeze_timeout_s;
    req.pause_max_s = units_config()->max_pause_s;
    req.cancel = o->cancel;
    req.holds = &holds;
    req.daemon8 = units_daemon8();

    uctx_t u;
    memset(&u, 0, sizeof(u));
    u.ops = ops;
    u.desc = d;
    st = ops->begin(pctx, d, &req, &u.sess, &u.info);
    if (st != OK) {
        ops->desc_free(d);
        units_unlock(key);
        return st;
    }
    res->achieved = u.info->achieved;
    res->n_warnings = (unsigned)u.info->n_warnings;
    if (u.info->n_warnings) snprintf(res->warning, sizeof(res->warning), "%s", u.info->warnings[0]);
    for (size_t i = 0; i < u.info->n_warnings; i++)
        log_warn("unit %s: %s", key, u.info->warnings[i]);

    /* Parts: blobs and disks as synthetic files, unit.json last; trees as
     * sources read from the provider's root. */
    size_t n_trees = 0;
    for (size_t i = 0; i < d->n_items; i++) n_trees += d->items[i].kind == SP_ITEM_TREE;
    size_t n_synth = d->n_items - n_trees + 1;
    u.parts = calloc(n_synth, sizeof(*u.parts));
    backup_synth_t *syn = calloc(n_synth, sizeof(*syn));
    char **synpath = calloc(n_synth, sizeof(*synpath));
    const char **paths = calloc(n_trees ? n_trees : 1, sizeof(*paths));
    const char **rpaths = calloc(n_trees ? n_trees : 1, sizeof(*rpaths));
    char **rbuf = calloc(n_trees ? n_trees : 1, sizeof(*rbuf));
    int have_all = u.parts && syn && synpath && paths && rpaths && rbuf;
    if (!have_all) st = set_error(ERR_NOMEM, "unit backup");
    size_t ns = 0, nt = 0;
    for (size_t i = 0; have_all && st == OK && i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        if (it->kind == SP_ITEM_TREE) {
            if (!(rbuf[nt] = malloc(PATH_MAX))) { st = set_error(ERR_NOMEM, "unit backup"); break; }
            st = ops->tree_root(u.sess, it, rbuf[nt], PATH_MAX);
            if (st == OK && rbuf[nt][0] != '/')
                st = set_error(ERR_INVALID, "unit %s: tree %s has a relative read root", key, it->id);
            paths[nt] = it->tree_path;
            rpaths[nt] = rbuf[nt];
            nt++;
            continue;
        }
        size_t n = strlen(it->tree_path) + 2;
        if (!(synpath[ns] = malloc(n))) { st = set_error(ERR_NOMEM, "unit backup"); break; }
        snprintf(synpath[ns], n, "/%s", it->tree_path);
        syn[ns].path = synpath[ns];
        syn[ns].perm = 0600;
        syn[ns].size = it->kind == SP_ITEM_BLOCK ? it->size : 0;
        /* Configuration is mtime 0 so an unchanged blob serialises alike;
         * a disk carries its image's mtime, which is real metadata. */
        syn[ns].mtime_sec = it->kind == SP_ITEM_BLOCK ? it->mtime_sec : 0;
        u.parts[ns].item = (int)i;
        ns++;
    }
    if (st == OK) {
        syn[ns].path = UNIT_JSON_PATH;
        syn[ns].perm = 0600;
        u.parts[ns].item = -1;
        ns++;
        u.n_parts = ns;
    }

    int64_t snap = 0;
    if (st == OK) {
        prog_t pg;
        memset(&pg, 0, sizeof(pg));
        pg.o = o;
        snprintf(pg.prefix, sizeof(pg.prefix), "unit %zu/%zu %s", index + 1, total, res->name);
        backup_synth_ops_t sops = { part_open, part_close, &u };
        hook_t hk = { &u, repo_id };
        backup_opts_t bo;
        memset(&bo, 0, sizeof(bo));
        bo.paths = paths;
        bo.n_paths = (int)nt;
        bo.read_paths = rpaths;
        bo.synth = syn;
        bo.n_synth = ns;
        bo.synth_ops = &sops;
        bo.unit_snapshot = 1;
        bo.index_hook = index_hook;
        bo.index_hook_ctx = &hk;
        bo.cancel = o->cancel;
        bo.store_workers = o->store_workers;
        bo.file_cache_match = o->file_cache_match;
        if (o->progress_cb) {
            bo.progress_cb = prog_cb;
            bo.progress_ctx = &pg;
            bo.bytes_cb = prog_bytes;
        }
        backup_result_t br;
        memset(&br, 0, sizeof(br));
        st = backup_run(store, db, repo_id, &bo, &br);
        if (st == OK) {
            snap = br.snapshot_id;
            res->files = br.files;
            if (br.files_skipped && !res->warning[0])
                snprintf(res->warning, sizeof(res->warning), "%llu file(s) could not be read "
                         "and are not in snapshot %lld", (unsigned long long)br.files_skipped,
                         (long long)snap);
        }
        backup_result_free(&br);
    }
    /* Parts a failure left open. */
    for (size_t i = 0; u.parts && i < n_synth; i++) part_release(&u.parts[i]);

    char why[ERR_MSG_MAX];
    snprintf(why, sizeof(why), "%s", st != OK ? err_msg() : "");
    status_t est = ops->end(u.sess, st == OK ? SP_END_COMMITTED : SP_END_ABORTED, snap);
    if (est != OK) {
        /* Never fails a snapshot that committed: the sweep has the rows. */
        log_warn("unit %s: releasing the session failed: %s; the sweep will retry",
                 key, err_msg());
        if (st == OK && !res->warning[0])
            snprintf(res->warning, sizeof(res->warning), "session release failed: %.400s",
                     err_msg());
        err_clear();
    }
    res->snapshot_id = snap;
    res->bytes_read = u.bytes_read;
    if (st == OK)
        log_info("unit %s: snapshot %lld, %s, %llu disk bytes read", key, (long long)snap,
                 sp_consistency_name(res->achieved), (unsigned long long)u.bytes_read);

    unit_json_free(&u.uj);
    free(u.uj_text);
    free(u.parts);
    free(syn);
    for (size_t i = 0; synpath && i < n_synth; i++) free(synpath[i]);
    free(synpath);
    for (size_t i = 0; rbuf && i < n_trees; i++) free(rbuf[i]);
    free(rbuf);
    free(paths);
    free(rpaths);
    ops->desc_free(d);
    units_unlock(key);
    if (st != OK) return set_error(st, "%s", why);
    return OK;
}

/* ------------------------------------------------------------------ */

void unit_backup_result_free(unit_backup_result_t *r)
{
    if (!r) return;
    free(r->units);
    memset(r, 0, sizeof(*r));
}

static unit_backup_one_t *result_add(unit_backup_result_t *r)
{
    unit_backup_one_t *g = realloc(r->units, (r->n + 1) * sizeof(*g));
    if (!g) return NULL;
    r->units = g;
    memset(&g[r->n], 0, sizeof(g[r->n]));
    return &g[r->n++];
}

status_t unit_backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                         const unit_backup_opts_t *o, unit_backup_result_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!o || !o->provider || (!o->all && o->n_units == 0))
        return set_error(ERR_INVALID, "unit backup: name a provider and units, or all");
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *pctx = NULL;
    status_t st = sp_registry_get(o->provider, &ops, &pctx);
    if (st != OK) return st;
    if (!ops) return set_error(ERR_INVALID, "unit backup: no provider %s", o->provider);

    listing_t l = { 0 };
    st = ops->list_units(pctx, listing_add, &l);
    if (st != OK) { listing_free(&l); return st; }

    /* What to back up, resolved now: for a schedule with all, this is fire
     * time, so a unit added since is included. */
    size_t *pick = calloc(l.n + o->n_units + 1, sizeof(*pick));
    if (!pick) { listing_free(&l); return set_error(ERR_NOMEM, "unit backup"); }
    size_t np = 0;
    char first_err[512] = "";
    status_t first_st = OK;
    if (o->all) {
        for (size_t i = 0; i < l.n; i++) {
            if (excluded(o, l.v[i].name)) continue;
            if (!units_allowed(ops->name, l.v[i].id, l.v[i].name)) { out->filtered++; continue; }
            pick[np++] = i;
        }
    } else {
        for (size_t k = 0; k < o->n_units; k++) {
            const char *want = o->units[k];
            size_t i = 0;
            while (i < l.n && strcmp(l.v[i].name, want) != 0) i++;
            if (i == l.n) { i = 0; while (i < l.n && strcmp(l.v[i].id, want) != 0) i++; }
            int dup = 0;
            for (size_t q = 0; q < np && i < l.n; q++) dup |= pick[q] == i;
            if (dup) continue;
            const char *why = NULL;
            if (i == l.n) why = "is not listed by the provider";
            else if (!units_allowed(ops->name, l.v[i].id, l.v[i].name)) why = "is not in units.allow";
            if (why) {
                unit_backup_one_t *r = result_add(out);
                if (!r) { st = set_error(ERR_NOMEM, "unit backup"); break; }
                snprintf(r->name, sizeof(r->name), "%s", want);
                snprintf(r->unit_key, sizeof(r->unit_key), "%s:%s", ops->name,
                         i < l.n ? l.v[i].id : want);
                r->st = ERR_INVALID;
                snprintf(r->error, sizeof(r->error), "unit %s %s; refused", want, why);
                log_warn("unit backup: %s", r->error);
                out->failed++;
                if (first_st == OK) { first_st = ERR_INVALID; snprintf(first_err, sizeof(first_err), "%s", r->error); }
                continue;
            }
            pick[np++] = i;
        }
    }

    for (size_t k = 0; st == OK && k < np; k++) {
        unit_backup_one_t *r = result_add(out);
        if (!r) { st = set_error(ERR_NOMEM, "unit backup"); break; }
        if (o->cancel && atomic_load(o->cancel)) {
            r->st = ERR_BUSY;
            snprintf(r->name, sizeof(r->name), "%s", l.v[pick[k]].name);
            snprintf(r->error, sizeof(r->error), "cancelled before it started");
            out->failed++;
            if (first_st == OK) { first_st = ERR_BUSY; snprintf(first_err, sizeof(first_err), "cancelled"); }
            continue;
        }
        status_t one = run_one(store, db, repo_id, o, ops, pctx, &l.v[pick[k]], k, np, r);
        r->st = one;
        if (one == OK) {
            out->ok++;
        } else {
            snprintf(r->error, sizeof(r->error), "%.500s", err_msg());
            log_warn("unit backup: %s: %s", r->unit_key, r->error);
            err_clear();
            out->failed++;
            if (first_st == OK) {
                first_st = one;
                snprintf(first_err, sizeof(first_err), "%.150s: %.350s", r->unit_key, r->error);
            }
        }
    }
    free(pick);
    listing_free(&l);

    /* Hygiene after every job: whatever a failed release left journalled
     * is released now rather than at the next start. */
    units_sweep_result_t sw;
    if (units_sweep(db, o->job_id, &sw) != OK) {
        log_warn("unit backup: sweep: %s", err_msg());
        err_clear();
    }
    if (st != OK) return st;
    if (o->all && np == 0 && out->failed == 0)
        log_info("unit backup: provider %s listed no unit to back up (%zu left out by "
                 "units.allow)", ops->name, out->filtered);
    if (first_st != OK)
        return set_error(first_st, "%zu of %zu unit(s) failed; first: %s",
                         out->failed, out->failed + out->ok, first_err);
    return OK;
}
