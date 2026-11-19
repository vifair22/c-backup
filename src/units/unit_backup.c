#define _POSIX_C_SOURCE 200809L

#include "unit_backup.h"
#include "msg_clip.h"
#include "backup.h"
#include "block_source.h"
#include "db_template.h"
#include "db_txn.h"
#include "unit_detect.h"
#include "registry.h"
#include "remote_provider.h"
#include "reserved.h"
#include "unit_index.h"
#include "unit_json.h"
#include "unit_state.h"
#include "unit_tokens.h"

#include <cutils/log.h>

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <errno.h>
#include <fnmatch.h>
#include <stdarg.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
        if (it->kind == SP_ITEM_BLOB || it->kind == SP_ITEM_BLOCK || it->kind == SP_ITEM_STREAM) {
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

/* A STREAM item's pipe as a chunker source: one span handed over in
 * pieces as the bytes arrive, closed by an empty piece at EOF. Memory is
 * the one buffer, whatever the dump's size. */
#define PIPE_BUF_BYTES ((size_t)1024 * 1024)

typedef struct {
    int      fd;
    uint8_t *buf;
    uint64_t off, buf_off;
    size_t   buf_len;
    int      open, eof;
} pipesrc_t;

static status_t pipe_piece(void *ctx, uint64_t *s, uint64_t *e, int *got, int *more)
{
    pipesrc_t *p = ctx;
    *got = 0;
    if (p->eof) return OK;
    size_t n = 0;
    while (n < PIPE_BUF_BYTES) {
        ssize_t r = read(p->fd, p->buf + n, PIPE_BUF_BYTES - n);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) return set_error_errno(ERR_IO, "unit backup: reading a stream");
        if (r == 0) break;
        n += (size_t)r;
        if (n >= PIPE_BUF_BYTES / 4) break;     /* enough for one piece */
    }
    p->buf_off = p->off;
    p->buf_len = n;
    if (n > 0) {
        *s = p->off;
        *e = p->off + n;
        *got = 1;
        *more = 1;
        p->open = 1;
        p->off += n;
        return OK;
    }
    p->eof = 1;
    if (p->open) { *s = *e = p->off; *got = 1; *more = 0; }
    return OK;
}

static status_t pipe_span(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    (void)ctx; (void)s; (void)e; (void)got;
    return set_error(ERR_INVALID, "unit backup: a stream is read in pieces");
}

static status_t pipe_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    pipesrc_t *p = ctx;
    if (off < p->buf_off || off - p->buf_off + n > p->buf_len)
        return set_error(ERR_IO, "unit backup: a stream read outside its piece");
    memcpy(buf, p->buf + (off - p->buf_off), n);
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
    pipesrc_t         pipe;
    int               stream_open, stream_ok;
    /* A disk's read (vm-backup.md 5.3, 5.4): how it was stored and why,
     * what the estimate and the delta said, an audit's result, and the
     * content hash it ended with. */
    int               vb;              /* index into uctx_t.bases, -1 for none */
    backup_synth_how_t how;
    const char       *read_mode, *reason;
    cdc_delta_estimate_t est;
    int               have_est;
    cdc_delta_result_t dres;
    cdc_audit_t      *audit;
    cdc_audit_result_t ares;
    int               audited;
    uint8_t           hash[OBJECT_HASH_SIZE];
    int               stored;
} part_t;

/*
 * What the daemon knows about a disk's chain before begin (5.4, the token
 * row layer): whether its token binds, and why not when it does not.
 */
typedef struct {
    char        item[SP_ITEM_ID_MAX];
    char        identity[UNIT_IDENTITY_MAX];
    int         found, usable, covered;
    const char *reason;        /* when not usable */
    unit_token_t t;
} vbase_t;

/* ------------------------------------------------------------------ */
/* Database templates (db-detection.md 7.2, phase D2)                  */
/* ------------------------------------------------------------------ */

/*
 * A unit's enrolment of templates is its enabled findings: db_findings rows
 * of scope unit:<key> in state enabled with a template, as the enable call
 * left them. There is no second copy that could disagree. Each becomes a
 * hook named after its template; when two findings of one unit enable the
 * same template, the later finding's hook is <template>-<finding id>.
 */
#define UNIT_TEMPLATES_MAX 8u

typedef struct {
    int64_t finding;
    char    engine[16];
    char   *path;
    char   *params;
    char    name[40];
    char    hook[48];
    int     dump_item;          /* desc index of its STREAM item, -1 */
    int     log_item;
} utmpl_meta_t;

typedef struct {
    sp_template_run_t *runs;
    utmpl_meta_t      *meta;
    size_t             n;
    char             **warn;    /* said in the unit's result */
    size_t             n_warn;
} utmpl_t;

static void utmpl_warn(utmpl_t *t, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void utmpl_warn(utmpl_t *t, const char *fmt, ...)
{
    char buf[MSG_CLIP_MAX];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    msg_clip_after(buf, sizeof(buf), n);
    char **g = realloc(t->warn, (t->n_warn + 1) * sizeof(*g));
    if (!g) return;
    t->warn = g;
    if ((g[t->n_warn] = strdup(buf)) != NULL) t->n_warn++;
}

static void utmpl_free(utmpl_t *t)
{
    for (size_t i = 0; i < t->n; i++) {
        free(t->runs[i].log);
        free(t->meta[i].path);
        free(t->meta[i].params);
    }
    for (size_t i = 0; i < t->n_warn; i++) free(t->warn[i]);
    free(t->runs);
    free(t->meta);
    free(t->warn);
    memset(t, 0, sizeof(*t));
}

static status_t desc_add_item(sp_unit_desc_t *d, sp_item_kind_t kind, const char *id, const char *source,
                              const char *tree_path)
{
    sp_item_desc_t *g = realloc(d->items, (d->n_items + 1) * sizeof(*g));
    if (!g) return set_error(ERR_NOMEM, "unit backup: template items");
    d->items = g;
    sp_item_desc_t *it = &g[d->n_items];
    memset(it, 0, sizeof(*it));
    snprintf(it->id, sizeof(it->id), "%s", id);
    it->kind = kind;
    it->source = strdup(source);
    it->tree_path = strdup(tree_path);
    if (!it->source || !it->tree_path) {
        free(it->source);
        free(it->tree_path);
        return set_error(ERR_NOMEM, "unit backup: template items");
    }
    d->n_items++;
    return OK;
}

static int desc_has_path(const sp_unit_desc_t *d, const char *tp)
{
    for (size_t i = 0; i < d->n_items; i++)
        if (d->items[i].tree_path && strcmp(d->items[i].tree_path, tp) == 0) return 1;
    return 0;
}

/*
 * The unit's enabled templates, each with its items appended to the
 * description: a dump's STREAM and every template's log. A template the
 * provider cannot run, or whose output path a policy hook already writes,
 * is left out with a warning; nothing here fails the unit.
 */
static void templates_load(cutils_db_t *db, int64_t repo_id, const sp_provider_ops_t *ops, const char *key,
                           sp_unit_desc_t *d, utmpl_t *t)
{
    memset(t, 0, sizeof(*t));
    char repo[32], scope[UNIT_KEY_MAX + 8];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(scope, sizeof(scope), "unit:%s", key);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id, template, COALESCE(params, ''), path, engine FROM db_findings"
                       " WHERE repo_id = ? AND scope = ? AND state = 'enabled'"
                       " AND template IS NOT NULL AND template <> '' ORDER BY id",
                   (const char *[]){ repo, scope, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        log_warn("unit %s: enabled database templates not read: %s", key, err_msg());
        err_clear();
        return;
    }
    if (r->nrows == 0) { db_result_free(r); return; }
    size_t cap = (size_t)r->nrows < UNIT_TEMPLATES_MAX ? (size_t)r->nrows : UNIT_TEMPLATES_MAX;
    t->runs = calloc(cap, sizeof(*t->runs));
    t->meta = calloc(cap, sizeof(*t->meta));
    if (!t->runs || !t->meta) { db_result_free(r); utmpl_free(t); return; }
    for (int i = 0; i < r->nrows; i++) {
        const char *const *row = (const char *const *)r->rows[i];
        long long fid = strtoll(row[0], NULL, 10);
        const db_template_t *dt = db_template_find(row[1]);
        if (!dt) {
            utmpl_warn(t, "database template %s (finding %lld) is not in this build's catalog; not run", row[1], fid);
            continue;
        }
        if (!ops->template_exec || !ops->templates_set) {
            utmpl_warn(t, "database template %s (finding %lld) not run: provider %s runs no templates",
                       dt->name, fid, ops->name);
            continue;
        }
        if (t->n >= cap) {
            utmpl_warn(t, "database template %s (finding %lld) not run: a unit runs at most %u",
                       dt->name, fid, UNIT_TEMPLATES_MAX);
            continue;
        }
        utmpl_meta_t *m = &t->meta[t->n];
        memset(m, 0, sizeof(*m));
        m->finding = fid;
        m->dump_item = m->log_item = -1;
        snprintf(m->name, sizeof(m->name), "%s", dt->name);
        snprintf(m->engine, sizeof(m->engine), "%s", row[4]);
        int twice = 0;
        for (size_t k = 0; k < t->n; k++) twice |= strcmp(t->meta[k].name, dt->name) == 0;
        if (twice) snprintf(m->hook, sizeof(m->hook), "%s-%lld", dt->name, fid);
        else snprintf(m->hook, sizeof(m->hook), "%s", dt->name);
        char dump_tp[128], log_tp[128], id[SP_ITEM_ID_MAX], src[96];
        snprintf(dump_tp, sizeof(dump_tp), RESERVED_UNIT_DIR "/hooks/%s.dump", m->hook);
        snprintf(log_tp, sizeof(log_tp), RESERVED_UNIT_DIR "/hooks/%s.log", m->hook);
        if ((dt->kind == DBT_DUMP && desc_has_path(d, dump_tp)) || desc_has_path(d, log_tp)) {
            utmpl_warn(t, "database template %s (finding %lld) not run: a policy hook already writes %s",
                       dt->name, fid, dt->kind == DBT_DUMP ? dump_tp : log_tp);
            continue;
        }
        if (!(m->path = strdup(row[3])) || !(m->params = strdup(row[2][0] ? row[2] : "{}"))) {
            free(m->path);
            m->path = NULL;
            continue;
        }
        snprintf(src, sizeof(src), SP_TEMPLATE_SOURCE "%s", m->hook);
        status_t st = OK;
        if (dt->kind == DBT_DUMP) {
            snprintf(id, sizeof(id), "tdump:%s", m->hook);
            m->dump_item = (int)d->n_items;
            st = desc_add_item(d, SP_ITEM_STREAM, id, src, dump_tp);
        }
        if (st == OK) {
            snprintf(id, sizeof(id), "tlog:%s", m->hook);
            m->log_item = (int)d->n_items;
            st = desc_add_item(d, SP_ITEM_BLOB, id, src, log_tp);
        }
        if (st != OK) {
            log_warn("unit %s: template %s left out: %s", key, dt->name, err_msg());
            err_clear();
            free(m->path);
            free(m->params);
            continue;
        }
        sp_template_run_t *x = &t->runs[t->n];
        memset(x, 0, sizeof(*x));
        x->name = dt->name;
        x->hook = m->hook;
        x->params = m->params;
        x->path = m->path;
        x->dump = dt->kind == DBT_DUMP;
        x->post = dt->post;
        t->n++;
        log_info("unit %s: database template %s enabled (finding %lld)", key, m->hook, fid);
    }
    db_result_free(r);
}

/* Every exec template's pre (post 0) or post run, in order, before begin
 * and after it. A failure is a warning: the capture goes ahead, and the
 * finding's coverage stays what the capture itself achieved. */
static void templates_exec(const sp_provider_ops_t *ops, sp_ctx_t *pctx, const char *unit_id, utmpl_t *t,
                           int post)
{
    for (size_t i = 0; i < t->n; i++) {
        sp_template_run_t *x = &t->runs[i];
        if (x->dump || (post && (!x->post || !x->pre_ran))) continue;
        if (ops->template_exec(pctx, unit_id, x, post) != OK) {
            log_warn("unit %s: template %s: %s", unit_id, x->hook, err_msg());
            err_clear();
        }
        int ok = post ? x->post_ok : x->pre_ok;
        if (!ok)
            utmpl_warn(t, "database template %s %s run failed; see .cbackup-unit/hooks/%s.log", x->hook,
                       post ? "post" : "pre", x->hook);
    }
}

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
    size_t                    streams_failed;   /* dumps not stored */
    char                      stream_failed[256]; /* the first's path, for the warning */
    int                       held;      /* begin kept a pause for pass 2 */
    int                       cap_hit;   /* and the watchdog let it go first */
    /* Incremental reads. */
    store_t                  *store;
    cutils_db_t              *db;
    int64_t                   repo_id;
    const char               *key;
    vbase_t                  *bases;
    size_t                    n_bases;
    int                       audit_due;
    char                    **warn;      /* the daemon's own, after the provider's */
    size_t                    n_warn;
    /* Database detection: fed by the backup, finished when unit.json is
     * built (it is stored last for that), recorded once the snapshot is. */
    db_detector_t            *detect;
    db_findings_t            *findings;
    /* The trees' recorded paths and read roots, for detection's one
     * re-read of a Nextcloud config the cache vouched for. */
    const char *const        *rec_roots;
    const char *const        *read_roots;
    size_t                    n_roots;
    utmpl_t                  *tmpl;      /* the unit's enabled templates */
    /* Raised to application by its templates (B246), else 0. */
    int                       app_by_templates;
} uctx_t;

static void uwarn(uctx_t *u, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void uwarn(uctx_t *u, const char *fmt, ...)
{
    char buf[MSG_CLIP_MAX];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    msg_clip_after(buf, sizeof(buf), n);
    log_warn("unit %s: %s", u->key ? u->key : "?", buf);
    char **g = realloc(u->warn, (u->n_warn + 1) * sizeof(*g));
    if (!g) return;
    u->warn = g;
    if ((g[u->n_warn] = strdup(buf)) != NULL) u->n_warn++;
}

static vbase_t *vbase_for(uctx_t *u, const char *item)
{
    for (size_t i = 0; i < u->n_bases; i++)
        if (strcmp(u->bases[i].item, item) == 0) return &u->bases[i];
    return NULL;
}

static const sp_item_plan_t *plan_for(const uctx_t *u, const char *item)
{
    for (size_t i = 0; u->info && i < u->info->n_plans; i++)
        if (u->info->plans[i].item_id && strcmp(u->info->plans[i].item_id, item) == 0)
            return &u->info->plans[i];
    return NULL;
}

/* unit.json, from the description, the session and what each part read. */
static int is_fallback(const sp_item_desc_t *it);

/*
 * Judge the session's findings (docs/db-detection.md section 5) and put
 * them in unit.json. A tree read from a filesystem snapshot is atomic; one
 * on no snapshot domain is read live, or paused when begin held the pause
 * for it and the watchdog did not let it go first. At crash or better, the
 * provider guarantees one instant for everything (it pauses exactly when a
 * plan has more than one, container-backup.md 6.4); below it, each
 * snapshotted tree is counted as its own instant, which is conservative
 * for several datasets of one pool. Never fails the unit.
 */
static const utmpl_meta_t *template_for(const uctx_t *u, const db_finding_t *f, size_t *idx)
{
    for (size_t i = 0; u->tmpl && i < u->tmpl->n; i++) {
        const utmpl_meta_t *m = &u->tmpl->meta[i];
        if (strcmp(m->engine, db_engine_name(f->engine)) == 0 && m->path && strcmp(m->path, f->path) == 0) {
            *idx = i;
            return m;
        }
    }
    return NULL;
}

/* Whether a finding's enabled template did its part this session: the
 * dump stored, or an exec template's pre run exited 0. */
static int template_ok(const uctx_t *u, size_t i)
{
    const utmpl_meta_t *m = &u->tmpl->meta[i];
    if (!u->tmpl->runs[i].dump) return u->tmpl->runs[i].pre_ran && u->tmpl->runs[i].pre_ok;
    for (size_t k = 0; u->parts && k < u->n_parts; k++)
        if (u->parts[k].item == m->dump_item) return u->parts[k].stream_ok;
    return 0;
}

/*
 * A live finding's coverage, given its template (db-detection.md 5): a
 * stored dump is a consistent copy at its own instant, whatever the capture
 * did, so covered-application. An exec template (a BGSAVE, a TSDB
 * snapshot, maintenance mode) is only as good as the capture that follows
 * it: it lifts a capture that was already one instant (covered-crash or
 * covered-quiesced), and leaves split, uncovered or engine-unsafe alone.
 * A template that failed changes nothing.
 */
static void templates_cover(const uctx_t *u, db_findings_t *fs)
{
    for (size_t i = 0; i < fs->n; i++) {
        db_finding_t *f = &fs->v[i];
        size_t ti = 0;
        const utmpl_meta_t *m = template_for(u, f, &ti);
        if (!m || f->confidence != DB_CONF_LIVE || !template_ok(u, ti)) continue;
        int dump = u->tmpl->runs[ti].dump;
        if (!dump && f->coverage != DB_COV_COVERED_CRASH && f->coverage != DB_COV_COVERED_QUIESCED) continue;
        char why[256];
        if (dump)
            snprintf(why, sizeof(why), "its enabled %s dump is a consistent copy at its own instant, in this "
                     "snapshot at .cbackup-unit/hooks/%s.dump", m->name, m->hook);
        else
            snprintf(why, sizeof(why), "captured at one instant after its enabled %s ran", m->name);
        char *w = strdup(why);
        if (!w) continue;
        free(f->coverage_why);
        f->coverage_why = w;
        f->coverage = DB_COV_COVERED_APPLICATION;
        f->suggestion = NULL;
    }
}

/*
 * The unit's own level, from its findings (B246). A template is the unit's
 * application taking part, as a container's own pre hook is, so it raises a
 * capture of one instant (crash or quiesced) to application when it did
 * that for every live database in the unit: each one covered-application
 * by its enabled template, which did its part. A database left without one
 * keeps the unit where the capture put it, and nothing raises a capture
 * that was not one instant, whatever a dump made of its own finding.
 */
static int templates_raise(const uctx_t *u, sp_consistency_t a, const db_findings_t *fs)
{
    if (a != SP_CONSIST_CRASH && a != SP_CONSIST_QUIESCED) return 0;
    size_t live = 0;
    for (size_t i = 0; i < fs->n; i++) {
        const db_finding_t *f = &fs->v[i];
        if (f->confidence != DB_CONF_LIVE || f->coverage == DB_COV_NONE) continue;
        size_t ti = 0;
        if (f->coverage != DB_COV_COVERED_APPLICATION || !template_for(u, f, &ti) || !template_ok(u, ti))
            return 0;
        live++;
    }
    return live > 0;
}

static void unit_detect(uctx_t *u, unit_json_t *j)
{
    if (!u->detect || u->findings) return;
    size_t n = 0;
    for (size_t i = 0; i < u->desc->n_items; i++) n += u->desc->items[i].kind == SP_ITEM_TREE;
    db_cap_root_t *roots = calloc(n ? n : 1, sizeof(*roots));
    if (!roots) { log_warn("unit %s: database detection skipped: out of memory", u->key); return; }
    sp_consistency_t a = u->cap_hit ? SP_CONSIST_INCONSISTENT : u->info->achieved;
    int one_instant = a >= SP_CONSIST_CRASH;
    size_t k = 0;
    for (size_t i = 0; i < u->desc->n_items; i++) {
        const sp_item_desc_t *it = &u->desc->items[i];
        if (it->kind != SP_ITEM_TREE) continue;
        db_cap_root_t *r = &roots[k++];
        r->root = it->tree_path;
        if (a == SP_CONSIST_OFFLINE)
            r->kind = DB_CAP_ATOMIC;
        else if (!is_fallback(it) && (u->info->consist_flags & SP_CF_FS_SNAPSHOT))
            r->kind = DB_CAP_ATOMIC;
        else if (is_fallback(it) && u->held && !u->cap_hit)
            r->kind = DB_CAP_PAUSED;
        else
            r->kind = DB_CAP_LIVE;
        r->instant = one_instant ? 0 : (int)k;
    }
    db_capture_desc_t cap = { roots, k, a == SP_CONSIST_APPLICATION, a == SP_CONSIST_QUIESCED, 1, 1 };
    unit_detect_reread(u->detect, u->rec_roots, u->read_roots, u->n_roots);
    if (db_detect_finish(u->detect, &cap, &u->findings) != OK) {
        log_warn("unit %s: database detection skipped: %s", u->key, err_msg());
        err_clear();
        u->findings = NULL;
    }
    free(roots);
    if (!u->findings) return;
    templates_cover(u, u->findings);
    if (templates_raise(u, a, u->findings)) {
        u->app_by_templates = 1;
        j->consistency = SP_CONSIST_APPLICATION;
        log_info("unit %s: application, from %s: its enabled database templates covered every live "
                 "database", u->key, sp_consistency_name(a));
    }
    j->have_dbs = 1;
    j->dbs_total = u->findings->n;
    size_t m = u->findings->n < UNIT_JSON_DB_MAX ? u->findings->n : UNIT_JSON_DB_MAX;
    if (m && !(j->dbs = calloc(m, sizeof(*j->dbs)))) { j->have_dbs = 0; return; }
    for (size_t i = 0; i < m; i++) {
        const db_finding_t *f = &u->findings->v[i];
        unit_json_db_t *d = &j->dbs[i];
        snprintf(d->engine, sizeof(d->engine), "%s", db_engine_name(f->engine));
        snprintf(d->version, sizeof(d->version), "%s", f->version);
        snprintf(d->confidence, sizeof(d->confidence), "%s", db_confidence_name(f->confidence));
        snprintf(d->coverage, sizeof(d->coverage), "%s", db_coverage_name(f->coverage));
        snprintf(d->method, sizeof(d->method), "%s", f->method ? f->method : "");
        size_t ti = 0;
        const utmpl_meta_t *tm = template_for(u, f, &ti);
        if (tm) {
            snprintf(d->tmpl, sizeof(d->tmpl), "%s", tm->name);
            d->tmpl_ok = template_ok(u, ti);
            if (d->tmpl_ok && u->tmpl->runs[ti].dump)
                snprintf(d->dump, sizeof(d->dump), RESERVED_UNIT_DIR "/hooks/%s.dump", tm->hook);
        }
        if (!(d->path = strdup(f->path))) break;
        j->n_dbs++;
    }
}

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
    j->warnings = calloc(u->info->n_warnings + u->n_warn + 1, sizeof(char *));
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
        /* What each item actually did: a disk's part says how it was read
         * and why; anything else is a full read, its plan's reason if it
         * had one the vocabulary knows. */
        const sp_item_plan_t *pl = plan_for(u, it->id);
        snprintf(o->read_mode, sizeof(o->read_mode), "full");
        const char *why = pl && pl->mode == SP_READ_FULL && sp_reason_known(pl->reason)
                        ? pl->reason : "forced";
        snprintf(o->reason, sizeof(o->reason), "%s", why);
        for (size_t p = 0; p < u->n_parts; p++) {
            const part_t *pt = &u->parts[p];
            if (pt->item != (int)i) continue;
            o->bytes_read = pt->bytes_read;
            if (pt->read_mode) snprintf(o->read_mode, sizeof(o->read_mode), "%s", pt->read_mode);
            if (pt->reason) snprintf(o->reason, sizeof(o->reason), "%s", pt->reason);
            const vbase_t *vb = pt->vb >= 0 ? &u->bases[pt->vb] : NULL;
            if (vb && (pt->have_est || pt->audited || pt->how != BACKUP_SYNTH_FULL)) {
                o->have_incr = 1;
                snprintf(o->checkpoint, sizeof(o->checkpoint), "%s", u->info->new_token ? u->info->new_token : "");
                o->base_snapshot = vb->usable ? vb->t.snap_id : 0;
                o->estimate = pt->est.estimate;
                o->data_bytes = pt->est.data_bytes;
                o->clusters = pt->est.clusters;
                o->ranges = pt->est.ranges;
                o->dirty_bytes = pt->est.dirty_bytes;
                o->resync_bytes = pt->dres.resync_bytes;
                o->delta_full_pct = units_config()->delta_full_pct;
                o->audited = pt->audited;
                o->audit_bad = pt->ares.bad;
                o->audit_leaves = pt->ares.leaves_checked;
                o->audit_bad_bytes = pt->ares.bad_bytes;
                o->audit_first_bad = pt->ares.first_bad;
            }
        }
        if (it->kind == SP_ITEM_BLOB) o->size = 0;
    }
    for (size_t p = 0; p < u->n_parts; p++)
        if (u->parts[p].item >= 0 && u->desc->items[u->parts[p].item].kind == SP_ITEM_BLOB)
            j->items[u->parts[p].item].size = u->parts[p].blob_len;
    for (size_t i = 0; i < u->info->n_warnings; i++) {
        if (!(j->warnings[i] = strdup(u->info->warnings[i]))) return set_error(ERR_NOMEM, "unit.json");
        j->n_warnings++;
    }
    for (size_t i = 0; i < u->n_warn; i++) {
        if (!(j->warnings[j->n_warnings] = strdup(u->warn[i]))) return set_error(ERR_NOMEM, "unit.json");
        j->n_warnings++;
    }
    if (u->cap_hit) {
        /* The second pass outlived the pause cap: what it read after the
         * watchdog unpaused the unit is not from the snapshot's instant. */
        j->consistency = SP_CONSIST_INCONSISTENT;
        char **g = realloc(j->warnings, (j->n_warnings + 1) * sizeof(*g));
        if (!g) return set_error(ERR_NOMEM, "unit.json");
        j->warnings = g;
        if (!(g[j->n_warnings] = strdup("the pause hit its cap before the second pass over mounts on no "
                                        "snapshot domain finished; those mounts are flagged inconsistent")))
            return set_error(ERR_NOMEM, "unit.json");
        j->n_warnings++;
    }
    size_t dropped = 0;
    for (size_t p = 0; p < u->n_parts; p++) {
        if (u->parts[p].item < 0) continue;
        const sp_item_desc_t *it = &u->desc->items[u->parts[p].item];
        if (it->kind != SP_ITEM_STREAM) continue;
        j->items[u->parts[p].item].size = u->parts[p].bytes_read;
        if (!u->parts[p].stream_ok) dropped++;
    }
    if (dropped) {
        char **g = realloc(j->warnings, (j->n_warnings + 1) * sizeof(*g));
        if (!g) return set_error(ERR_NOMEM, "unit.json");
        j->warnings = g;
        char msg[96];
        snprintf(msg, sizeof(msg), "%zu stream(s) failed and were not stored", dropped);
        if (!(g[j->n_warnings] = strdup(msg))) return set_error(ERR_NOMEM, "unit.json");
        j->n_warnings++;
    }
    unit_detect(u, j);
    return OK;
}

static status_t audit_tap(void *ctx, uint64_t off, const uint8_t *buf, size_t len)
{
    return cdc_audit_feed(ctx, off, buf, len);
}

/*
 * Full, incremental or unchanged, for a disk the provider has opened
 * (5.3, 5.4): incremental only when the daemon's token binds and the
 * provider can give the changed ranges since it; then the estimate decides,
 * an empty changed set is the base's content as it is, and an audit that
 * is due or a read the estimate says costs too much is a full read checked
 * against the bitmap. Anything that goes wrong here is a full read, never a
 * failed backup: the incremental is an optimisation.
 */
static void decide_read(uctx_t *u, part_t *p, const sp_item_desc_t *it)
{
    p->how = BACKUP_SYNTH_FULL;
    p->read_mode = "full";
    vbase_t *vb = vbase_for(u, it->id);
    p->vb = vb ? (int)(vb - u->bases) : -1;
    const sp_item_plan_t *pl = plan_for(u, it->id);
    if (vb && !vb->usable) { p->reason = vb->reason; return; }
    if (!vb || !pl) return;
    if (pl->mode != SP_READ_INCREMENTAL) {
        if (sp_reason_known(pl->reason)) p->reason = pl->reason;
        return;
    }
    p->reason = "token-invalid";
    if (!p->bs.changed) {
        uwarn(u, "disk %s: the provider offered an incremental and no changed ranges; read in full", it->id);
        return;
    }
    obj_loader_t ld = obj_loader_store(u->store);
    status_t st = cdc_delta_estimate(&ld, vb->t.root, it->size, p->bs.changed, p->bs.ctx, &p->est);
    if (st != OK) {
        uwarn(u, "disk %s: the incremental's estimate failed (%s); read in full", it->id, err_msg());
        err_clear();
        memset(&p->est, 0, sizeof(p->est));
        return;
    }
    p->have_est = 1;
    unsigned pct = units_config()->delta_full_pct;
    int audit = u->audit_due;
    const char *why = NULL;
    if (audit) why = "audit";
    else if (p->est.ranges == 0) {
        p->how = BACKUP_SYNTH_REUSE;
        p->read_mode = "unchanged";
        p->reason = "incremental";
        return;
    } else if ((double)p->est.estimate * 100.0 > (double)pct * (double)p->est.data_bytes) {
        why = "delta-too-costly";
        audit = 1;          /* a full read with the bitmap in hand is a free audit */
    }
    if (why) {
        p->reason = why;
        if (audit) {
            st = cdc_audit_open(&ld, vb->t.root, it->size, p->bs.changed, p->bs.ctx, &p->audit);
            if (st != OK) {
                uwarn(u, "disk %s: the audit could not start (%s)", it->id, err_msg());
                err_clear();
                p->audit = NULL;
            }
        }
        return;
    }
    p->how = BACKUP_SYNTH_DELTA;
    p->read_mode = "incremental";
    p->reason = "incremental";
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
        *src = (cdc_source_t){ mem_next, mem_read, &p->mem, NULL, NULL };
        *size = len;
        return OK;
    }
    const sp_item_desc_t *it = &u->desc->items[p->item];
    if (it->kind == SP_ITEM_STREAM) {
        if (!u->ops->stream_open || !u->ops->stream_close)
            return set_error(ERR_INVALID, "provider %s has no streams for %s", u->desc->provider, it->id);
        memset(&p->pipe, 0, sizeof(p->pipe));
        p->pipe.fd = -1;
        if (!(p->pipe.buf = malloc(PIPE_BUF_BYTES))) return set_error(ERR_NOMEM, "unit backup: stream");
        status_t st = u->ops->stream_open(u->sess, it, &p->pipe.fd);
        if (st != OK) return st;
        p->stream_open = 1;
        *src = (cdc_source_t){ pipe_span, pipe_read, &p->pipe, pipe_piece, NULL };
        *size = CDC_SIZE_STREAM;
        return OK;
    }
    if (it->kind == SP_ITEM_BLOB) {
        status_t st = u->ops->blob_read(u->sess, it, &p->blob, &p->blob_len);
        if (st != OK) return st;
        if (p->blob_len > SP_BLOB_MAX)
            return set_error(ERR_TOO_LARGE, "blob %s is %zu bytes, over 16 MiB", it->id, p->blob_len);
        p->mem = (memsrc_t){ p->blob, p->blob_len, 0 };
        *src = (cdc_source_t){ mem_next, mem_read, &p->mem, NULL, NULL };
        *size = p->blob_len;
        return OK;
    }
    status_t st = u->ops->block_open(u->sess, it, &p->bs);
    if (st != OK) return st;
    p->bs_open = 1;
    decide_read(u, p, it);
    block_source_opts_t bo = { 1, units_config()->trust_zero, 0, NULL, NULL };
    if (p->audit) { bo.tap = audit_tap; bo.tap_ctx = p->audit; }
    /* An incremental restarts the read at every changed range: a window of
     * one mean chunk keeps what it reads past a resync to that, where the
     * default 4 MiB would read four times as much around each range. */
    if (p->how == BACKUP_SYNTH_DELTA) bo.window = (size_t)1024 * 1024;
    st = block_source_open(&p->bs, it->size, &bo, &p->src);
    if (st != OK) return st;
    *src = block_source_iface(p->src);
    *size = it->size;
    return OK;
}

/* How a part is stored, for the backup engine. */
static status_t part_method(void *ctx, size_t index, backup_synth_method_t *out)
{
    uctx_t *u = ctx;
    part_t *p = &u->parts[index];
    memset(out, 0, sizeof(*out));
    out->how = p->how;
    if (p->how == BACKUP_SYNTH_FULL || p->vb < 0) { out->how = BACKUP_SYNTH_FULL; return OK; }
    memcpy(out->base, u->bases[p->vb].t.root, OBJECT_HASH_SIZE);
    out->changed = p->bs.changed;
    out->changed_ctx = p->bs.ctx;
    out->delta = &p->dres;
    return OK;
}

static void part_release(part_t *p)
{
    cdc_audit_close(p->audit);
    p->audit = NULL;
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
    uctx_t *u = ctx;
    part_t *p = &u->parts[index];
    if (st == OK && cr) {
        memcpy(p->hash, cr->hash, OBJECT_HASH_SIZE);
        p->stored = 1;
    }
    if (st == OK && p->audit) {
        if (cdc_audit_finish(p->audit, &p->ares) == OK) p->audited = 1;
        else { log_warn("unit %s: audit: %s", u->key, err_msg()); err_clear(); }
    }
    if (st == OK && p->how == BACKUP_SYNTH_DELTA) units_vm_count_resync(p->dres.resync_bytes);
    if (p->stream_open) {
        /* Drain what is left so the producer is not stuck writing, then
         * ask whether it succeeded: only then is the stream kept. */
        if (p->pipe.fd >= 0) {
            if (st != OK) { char sink[4096]; while (read(p->pipe.fd, sink, sizeof(sink)) > 0) {} }
            close(p->pipe.fd);
            p->pipe.fd = -1;
        }
        int ok = 0;
        const sp_item_desc_t *it = &u->desc->items[p->item];
        if (u->ops->stream_close(u->sess, it, &ok) != OK) {
            log_warn("unit %s: stream %s: %s", u->desc->unit_id, it->id, err_msg());
            err_clear();
            ok = 0;
        }
        p->stream_ok = st == OK && ok;
        p->stream_open = 0;
        if (st == OK && cr) p->bytes_read = cr->size;
        if (!p->stream_ok) {
            log_warn("unit %s: %s failed; it is not stored", u->desc->unit_id, it->tree_path);
            if (!u->streams_failed++)
                snprintf(u->stream_failed, sizeof(u->stream_failed), "%.250s", it->tree_path);
        }
        free(p->pipe.buf);
        p->pipe.buf = NULL;
    }
    part_release(p);
    /* Disk bytes only: a dump's bytes are its item's size, not a read of
     * a disk, and the result calls this disk bytes read. */
    if (p->item < 0 || u->desc->items[p->item].kind != SP_ITEM_STREAM) u->bytes_read += p->bytes_read;
}

/* A failed dump is never stored: it looks valid and is not (6.6). */
static int part_keep(void *ctx, size_t index)
{
    uctx_t *u = ctx;
    const part_t *p = &u->parts[index];
    if (p->item < 0 || u->desc->items[p->item].kind != SP_ITEM_STREAM) return 1;
    return p->stream_ok;
}

typedef struct {
    uctx_t      *u;
    int64_t      repo_id;
} hook_t;

/*
 * Tokens settle with the snapshot, in its transaction (5.8): with a new
 * checkpoint, every disk it covers is bound to it with the content just
 * stored, and rows of disks it does not cover are dropped; without one
 * (a shut-off VM, or incrementals off) the chain's rows stay as they are,
 * still a valid base, and only rows of disks that are gone are dropped. A
 * failed audit turns incrementals off for the unit in the same
 * transaction; its rows still name the new checkpoint, so later sessions
 * retire it rather than leave it behind.
 */
static status_t bind_tokens(uctx_t *u, cutils_db_t *db, int64_t repo_id, int64_t snap)
{
    const char *keep[UNIT_ITEMS_MAX] = { NULL };
    size_t nk = 0;
    status_t st = OK;
    const char *nt = u->info->new_token && *u->info->new_token ? u->info->new_token : NULL;
    int bound = 0, all_full = 1;
    for (size_t p = 0; st == OK && p < u->n_parts; p++) {
        const part_t *pt = &u->parts[p];
        if (pt->vb < 0 || nk >= UNIT_ITEMS_MAX) continue;
        const vbase_t *vb = &u->bases[pt->vb];
        if (!nt) { keep[nk++] = vb->item; continue; }
        const sp_item_plan_t *pl = plan_for(u, vb->item);
        int covered = vb->covered && pt->stored &&
                      !(pl && pl->reason && strcmp(pl->reason, "no-incremental-support") == 0);
        if (!covered) continue;
        st = unit_token_put(db, repo_id, u->key, vb->item, nt, u->info->new_token_blob, snap,
                            pt->hash, vb->identity);
        keep[nk++] = vb->item;
        bound = 1;
        if (pt->how != BACKUP_SYNTH_FULL) all_full = 0;
    }
    if (st == OK) st = unit_tokens_drop_except(db, repo_id, u->key, keep, nk);
    /* Every disk the chain starts from or continues with was read in full
     * (a first read, a broken chain, an audit): that is the point the next
     * audit counts from. */
    if (st == OK && bound && all_full) st = unit_incr_set_audit(db, repo_id, u->key, (int64_t)time(NULL));
    for (size_t p = 0; st == OK && p < u->n_parts; p++) {
        const part_t *pt = &u->parts[p];
        if (!pt->audited || !pt->ares.bad) continue;
        char why[400];
        snprintf(why, sizeof(why), "bitmap audit failed on disk %s in snapshot %lld: %llu bytes of chunks "
                 "changed outside the dirty bitmap, the first at offset %llu",
                 u->bases[pt->vb].item, (long long)snap, (unsigned long long)pt->ares.bad_bytes,
                 (unsigned long long)pt->ares.first_bad);
        st = unit_incr_disable(db, repo_id, u->key, why);
        break;
    }
    return st;
}

static status_t index_hook(void *ctx, cutils_db_t *db, int64_t snap_id)
{
    hook_t *h = ctx;
    status_t st = unit_index_put(db, h->repo_id, snap_id, &h->u->uj);
    if (st == OK && h->u->bases) st = bind_tokens(h->u, db, h->repo_id, snap_id);
    return st;
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

/*
 * The unit's enrolled exclusions (unit_policy capture.exclude, one pattern
 * per line), never applied unless enrolled (container-backup.md 4.2, C7).
 * Patterns name recorded paths; the scan walks read roots (a snapshot), so
 * a pattern under a tree's recorded path is rewritten onto its read root.
 * Patterns without a leading '/' match anywhere and pass as they are.
 */
size_t unit_backup_excludes(cutils_db_t *db, int64_t repo_id, const char *key,
                            const char *const *paths, const char *const *rpaths, size_t nt,
                            char **out)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    size_t n = 0;
    if (db_execute(db, "SELECT value FROM unit_policy WHERE repo_id = ? AND unit_key = ?"
                       " AND key = 'capture.exclude'",
                   (const char *[]){ repo, key, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0;
    }
    char *copy = strdup(r->rows[0][0]);
    db_result_free(r);
    char *save = NULL;
    for (char *pat = copy ? strtok_r(copy, "\n", &save) : NULL; pat && n < UNIT_EXCLUDE_MAX;
         pat = strtok_r(NULL, "\n", &save)) {
        if (!*pat) continue;
        const char *base = NULL, *rest = NULL;
        for (size_t i = 0; i < nt && !base; i++) {
            size_t l = strlen(paths[i]);
            if (strncmp(pat, paths[i], l) == 0 && (pat[l] == '/' || pat[l] == '\0')) { base = rpaths[i]; rest = pat + l; }
        }
        size_t need = (base ? strlen(base) + strlen(rest) : strlen(pat)) + 1;
        if (!(out[n] = malloc(need))) break;
        if (base) snprintf(out[n], need, "%s%s", base, rest);
        else snprintf(out[n], need, "%s", pat);
        n++;
    }
    free(copy);
    return n;
}

/* Trees on no snapshot domain: read live, so pause-read re-reads them. */
static int is_fallback(const sp_item_desc_t *it)
{
    return it->kind == SP_ITEM_TREE && !(it->caps & SP_CAP_SNAPSHOT);
}

/*
 * pause-read, pass 1 (container-backup.md section 7): every fallback tree
 * read live and its content and cache rows stored, no snapshot written, so
 * the paused pass re-reads only what changed since.
 */
static status_t fallback_pass1(store_t *store, cutils_db_t *db, int64_t repo_id,
                               const unit_backup_opts_t *o, const sp_unit_desc_t *d, const char *key)
{
    const char **paths = calloc(d->n_items ? d->n_items : 1, sizeof(*paths));
    const char **rpaths = calloc(d->n_items ? d->n_items : 1, sizeof(*rpaths));
    if (!paths || !rpaths) { free(paths); free(rpaths); return set_error(ERR_NOMEM, "unit backup"); }
    int n = 0;
    for (size_t i = 0; i < d->n_items; i++) {
        if (!is_fallback(&d->items[i])) continue;
        paths[n] = d->items[i].tree_path;
        rpaths[n] = d->items[i].source && d->items[i].source[0] == '/' ? d->items[i].source : d->items[i].tree_path;
        n++;
    }
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.paths = paths;
    bo.n_paths = n;
    bo.read_paths = rpaths;
    bo.content_only = 1;
    bo.cancel = o->cancel;
    bo.store_workers = o->store_workers;
    bo.file_cache_match = o->file_cache_match;
    /* Only so the heads this pass reads are kept: the paused pass finds
     * their files unchanged and detects from what is kept. */
    bo.detect = db_detect_start();
    log_info("unit %s: pause-read pass 1 over %d mount(s) on no snapshot domain", key, n);
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    status_t st = backup_run(store, db, repo_id, &bo, &br);
    db_detect_free(bo.detect);
    backup_result_free(&br);
    free(paths);
    free(rpaths);
    return st;
}

/* The held roots are stored: the pause goes. */
static void held_done(void *ctx)
{
    uctx_t *u = ctx;
    int cap = 0;
    if (!u->ops->release || u->ops->release(u->sess, &cap) != OK) {
        log_warn("unit %s: releasing the pause: %s; the helper's watchdog will", u->desc->unit_id, err_msg());
        err_clear();
        cap = 1;
    }
    u->held = 0;
    u->cap_hit = cap;
}

/* A unit made ready for begin: locked, described, checked, journalled. */
typedef struct {
    char                     key[UNIT_KEY_MAX];
    sp_unit_desc_t          *d;
    units_journal_t          jr;
    sp_holds_t               holds;
    sp_session_t            *sess;
    const sp_session_info_t *info;
    /* Incremental chain (vm-backup.md 5.4, 5.8). */
    vbase_t                 *bases;
    size_t                   n_bases;
    sp_item_base_t          *ib;
    size_t                   n_ib;
    char                     retire[UNIT_TOKEN_MAX];
    int                      audit_due;
    char                     off_why[600];   /* incrementals off for this unit, and why */
    /* units.probe before begin, while the unit runs unpaused: liveness
     * for detection (db-detection.md 8.1). NULL when there is none. */
    sp_unit_probe_t         *probe;
    utmpl_t                  tmpl;           /* enabled database templates */
} unit_prep_t;

/*
 * The chain before begin. With vm.incremental on and the unit not
 * disabled, every disk that can carry a checkpoint takes part: it goes to
 * the provider as a base entry with the chain's token (NULL at the start
 * of a chain), so the new checkpoint covers it. Whether the daemon will
 * trust the changed ranges since that token is decided here, from the
 * token row alone (5.4): the snapshot still names the content, the disk is
 * the same disk, every disk's row names the same checkpoint. A provider
 * that offers an incremental for a disk whose token does not bind is read
 * in full anyway. With incrementals off, no disk takes part, and a token
 * left from before is handed over to be retired.
 */
static status_t bases_prepare(store_t *store, cutils_db_t *db, int64_t repo_id, unit_prep_t *pp)
{
    const sp_unit_desc_t *d = pp->d;
    const units_config_t *cfg = units_config();
    size_t nb = 0;
    for (size_t i = 0; i < d->n_items; i++) nb += d->items[i].kind == SP_ITEM_BLOCK;
    if (nb == 0) return OK;
    pp->bases = calloc(nb, sizeof(*pp->bases));
    pp->ib = calloc(nb, sizeof(*pp->ib));
    if (!pp->bases || !pp->ib) return set_error(ERR_NOMEM, "unit backup: bases");
    unit_incr_state_t is;
    status_t st = unit_incr_get(db, repo_id, pp->key, &is);
    if (st != OK) return st;
    int on = cfg->incremental && cfg->delta_full_pct > 0;
    if (on && is.disabled_reason[0]) {
        snprintf(pp->off_why, sizeof(pp->off_why), "incrementals are off for this unit until reset-incremental: %.500s",
                 is.disabled_reason);
        on = 0;
    }
    /* Has the unit ever been backed up here? "first" against "no-base". */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int seen = 0;
    if (db_execute(db, "SELECT 1 FROM snapshot_units WHERE repo_id = ? AND unit_key = ? LIMIT 1",
                   (const char *[]){ repo, pp->key, NULL }, &r) == 0 && r && r->nrows > 0) seen = 1;
    db_result_free(r);
    char chain[UNIT_TOKEN_MAX] = "";
    int mixed = 0;
    for (size_t i = 0; st == OK && i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        if (it->kind != SP_ITEM_BLOCK) continue;
        vbase_t *vb = &pp->bases[pp->n_bases++];
        snprintf(vb->item, sizeof(vb->item), "%s", it->id);
        unit_identity(it, vb->identity, sizeof(vb->identity));
        st = unit_token_get(db, repo_id, pp->key, it->id, &vb->t, &vb->found);
        if (st != OK) break;
        if (vb->found) {
            if (!chain[0]) snprintf(chain, sizeof(chain), "%s", vb->t.token);
            else if (strcmp(chain, vb->t.token) != 0) mixed = 1;
        }
        if (!on) { vb->reason = pp->off_why[0] ? "forced" : NULL; continue; }
        if (!(it->caps & SP_CAP_INCREMENTAL)) { vb->reason = "no-incremental-support"; continue; }
        vb->covered = 1;
        if (!vb->found) { vb->reason = seen ? "no-base" : "first"; continue; }
        if (vb->t.snap_id <= 0) { vb->reason = "forced"; continue; }      /* reset-incremental */
        int ok = 0;
        const char *why = NULL;
        st = unit_token_binds(store, db, repo_id, &vb->t, vb->identity, &ok, &why);
        vb->usable = st == OK && ok;
        vb->reason = why;
    }
    if (st != OK) return st;
    if (mixed) {
        /* One checkpoint per session covers every disk: rows naming two
         * cannot both be right. */
        for (size_t i = 0; i < pp->n_bases; i++)
            if (pp->bases[i].usable) { pp->bases[i].usable = 0; pp->bases[i].reason = "token-invalid"; }
    }
    int any = 0;
    for (size_t i = 0; i < pp->n_bases; i++) {
        if (!pp->bases[i].covered) continue;
        pp->ib[pp->n_ib].item_id = pp->bases[i].item;
        pp->ib[pp->n_ib].base_token = chain[0] && !mixed ? pp->bases[i].t.token : NULL;
        pp->n_ib++;
        any = 1;
    }
    if (!any && chain[0]) snprintf(pp->retire, sizeof(pp->retire), "%s", chain);
    for (size_t i = 0; any && i < pp->n_bases && !pp->audit_due; i++) {
        if (!pp->bases[i].usable) continue;
        pp->audit_due = cfg->audit_days > 0 &&
            (int64_t)time(NULL) - is.last_audit_at >= (int64_t)cfg->audit_days * 86400;
    }
    return OK;
}

static void bases_free(unit_prep_t *pp)
{
    free(pp->bases);
    free(pp->ib);
    pp->bases = NULL;
    pp->ib = NULL;
    pp->n_bases = pp->n_ib = 0;
}

static status_t unit_prepare(store_t *store, cutils_db_t *db, int64_t repo_id, const unit_backup_opts_t *o,
                             const sp_provider_ops_t *ops, sp_ctx_t *pctx, const listed_t *lu,
                             unit_backup_one_t *res, unit_prep_t *pp)
{
    memset(pp, 0, sizeof(*pp));
    unit_key_format(pp->key, sizeof(pp->key), ops->name, lu->id);
    const char *key = pp->key;
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
    if (st == OK) {
        pp->d = d;
        st = bases_prepare(store, db, repo_id, pp);
    }
    if (st == OK) templates_load(db, repo_id, ops, key, d, &pp->tmpl);
    if (st != OK) {
        bases_free(pp);
        pp->d = NULL;
        if (d) ops->desc_free(d);
        units_unlock(key);
        return st;
    }
    if (d->name) snprintf(res->name, sizeof(res->name), "%s", d->name);
    pp->jr.db = db;
    snprintf(pp->jr.provider, sizeof(pp->jr.provider), "%s", ops->name);
    snprintf(pp->jr.unit_id, sizeof(pp->jr.unit_id), "%s", d->unit_id);
    pp->jr.job_id = o->job_id;
    pp->holds = units_journal_holds(&pp->jr);
    pp->probe = unit_detect_probe(ops, pctx, d->unit_id, key);
    return OK;
}

/* Undo unit_prepare for a unit that never began. */
static void unit_unprepare(const sp_provider_ops_t *ops, unit_prep_t *pp)
{
    sp_unit_probe_free(pp->probe);
    pp->probe = NULL;
    bases_free(pp);
    utmpl_free(&pp->tmpl);
    if (pp->d) ops->desc_free(pp->d);
    pp->d = NULL;
    units_unlock(pp->key);
}

static void begin_req(const unit_backup_opts_t *o, sp_holds_t *holds, const unit_prep_t *pp,
                      sp_begin_req_t *req)
{
    memset(req, 0, sizeof(*req));
    if (pp) {
        req->bases = pp->ib;
        req->n_bases = pp->n_ib;
        req->retire_token = pp->retire[0] ? pp->retire : NULL;
    }
    req->job_id = o->job_id;
    req->want = SP_CONSIST_APPLICATION;
    req->require = o->require;
    req->freeze_timeout_s = units_config()->freeze_timeout_s;
    req->pause_max_s = units_config()->max_pause_s;
    req->cancel = o->cancel;
    req->holds = holds;
    req->daemon8 = units_daemon8();
}

/*
 * One warning for a unit's result: every one is kept (B202), and the first,
 * or one that outranks the rest, is also its summary line. Allocation
 * failures drop the copy, never the summary.
 */
static void one_warn(unit_backup_one_t *r, int outranks, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
static void one_warn(unit_backup_one_t *r, int outranks, const char *fmt, ...)
{
    char text[MSG_CLIP_MAX];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(text, sizeof(text), fmt, ap);
    va_end(ap);
    msg_clip_after(text, sizeof(text), n);
    if (outranks || !r->warning[0]) msg_clip(r->warning, sizeof(r->warning), text);
    char **g = realloc(r->warnings, (r->n_warnings + 1) * sizeof(*g));
    if (!g) return;
    r->warnings = g;
    if ((g[r->n_warnings] = strdup(text))) r->n_warnings++;
}

/* After begin: the unit's content from its session, then end and unlock. */
static status_t unit_store(store_t *store, cutils_db_t *db, int64_t repo_id,
                           const unit_backup_opts_t *o, const sp_provider_ops_t *ops,
                           unit_prep_t *pp, size_t index, size_t total, unit_backup_one_t *res)
{
    const char *key = pp->key;
    sp_unit_desc_t *d = pp->d;
    status_t st = OK;
    uctx_t u;
    memset(&u, 0, sizeof(u));
    u.ops = ops;
    u.desc = d;
    u.sess = pp->sess;
    u.info = pp->info;
    u.held = (u.info->consist_flags & SP_CF_PAUSE_HELD) != 0;
    u.store = store;
    u.db = db;
    u.repo_id = repo_id;
    u.key = key;
    u.bases = pp->bases;
    u.n_bases = pp->n_bases;
    u.audit_due = pp->audit_due;
    u.tmpl = &pp->tmpl;
    if (pp->off_why[0]) uwarn(&u, "%s", pp->off_why);
    for (size_t i = 0; i < pp->tmpl.n_warn; i++) one_warn(res, 0, "%s", pp->tmpl.warn[i]);
    res->achieved = u.info->achieved;
    for (size_t i = 0; i < u.info->n_warnings; i++) {
        one_warn(res, 0, "%s", u.info->warnings[i]);
        log_warn("unit %s: %s", key, u.info->warnings[i]);
    }

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
    /* Trees first, and under a held pause the fallback ones before the
     * rest, so the pause covers them alone (backup_opts_t.held_roots). */
    size_t held_roots = 0;
    for (int pass = 0; have_all && st == OK && pass < 2; pass++) {
        for (size_t i = 0; st == OK && i < d->n_items; i++) {
            const sp_item_desc_t *it = &d->items[i];
            if (it->kind != SP_ITEM_TREE) continue;
            int first = u.held && is_fallback(it);
            if ((pass == 0) != first) continue;
            if (!(rbuf[nt] = malloc(PATH_MAX))) { st = set_error(ERR_NOMEM, "unit backup"); break; }
            st = ops->tree_root(u.sess, it, rbuf[nt], PATH_MAX);
            if (st == OK && rbuf[nt][0] != '/')
                st = set_error(ERR_INVALID, "unit %s: tree %s has a relative read root", key, it->id);
            paths[nt] = it->tree_path;
            rpaths[nt] = rbuf[nt];
            nt++;
            if (first) held_roots++;
        }
    }
    for (size_t i = 0; have_all && st == OK && i < d->n_items; i++) {
        const sp_item_desc_t *it = &d->items[i];
        if (it->kind == SP_ITEM_TREE) continue;
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
        u.parts[ns].vb = -1;
        ns++;
    }
    if (st == OK) {
        syn[ns].path = UNIT_JSON_PATH;
        syn[ns].perm = 0600;
        /* After the trees, so it can say what reading them found. */
        syn[ns].late = 1;
        u.parts[ns].item = -1;
        u.parts[ns].vb = -1;
        ns++;
        u.n_parts = ns;
    }

    int64_t snap = 0;
    char *excl[UNIT_EXCLUDE_MAX];
    size_t n_excl = 0;
    if (st == OK) {
        prog_t pg;
        memset(&pg, 0, sizeof(pg));
        pg.o = o;
        snprintf(pg.prefix, sizeof(pg.prefix), "unit %zu/%zu %s", index + 1, total, res->name);
        backup_synth_ops_t sops = { part_open, part_close, &u, part_keep, part_method };
        hook_t hk = { &u, repo_id };
        backup_opts_t bo;
        memset(&bo, 0, sizeof(bo));
        bo.paths = paths;
        bo.n_paths = (int)nt;
        bo.read_paths = rpaths;
        if (u.held && held_roots) {
            bo.held_roots = (int)held_roots;
            bo.held_done = held_done;
            bo.held_ctx = &u;
        }
        n_excl = unit_backup_excludes(db, repo_id, key, paths, rpaths, nt, excl);
        if (n_excl) {
            bo.exclude = (const char **)(void *)excl;
            bo.n_exclude = (int)n_excl;
            log_info("unit %s: %zu enrolled exclusion(s)", key, n_excl);
        }
        bo.synth = syn;
        bo.n_synth = ns;
        bo.synth_ops = &sops;
        bo.unit_snapshot = 1;
        bo.index_hook = index_hook;
        bo.index_hook_ctx = &hk;
        bo.cancel = o->cancel;
        bo.store_workers = o->store_workers;
        bo.file_cache_match = o->file_cache_match;
        u.detect = db_detect_start();
        db_detect_unit(u.detect, pp->probe);
        u.rec_roots = paths;
        u.read_roots = rpaths;
        u.n_roots = nt;
        bo.detect = u.detect;
        if (o->progress_cb) {
            bo.progress_cb = prog_cb;
            bo.progress_ctx = &pg;
            bo.bytes_cb = prog_bytes;
        }
        backup_result_t br;
        memset(&br, 0, sizeof(br));
        st = backup_run(store, db, repo_id, &bo, &br);
        if (st == OK && u.findings) {
            char scope[SP_PROVIDER_MAX + SP_UNIT_ID_MAX + 8];
            snprintf(scope, sizeof(scope), "unit:%s", key);
            db_detect_count_uncovered(u.findings);
            db_findings_link_nextcloud(db, repo_id, scope, u.findings);
            db_record_result_t rr;
            memset(&rr, 0, sizeof(rr));
            if (db_findings_record(db, repo_id, br.snapshot_id, u.findings, NULL, 0, scope, &rr) != OK) {
                log_warn("unit %s: database findings not recorded: %s", key, err_msg());
                err_clear();
            }
            res->databases_json = db_findings_json(u.findings, rr.ids);
            res->db_events = rr.events;
            res->n_db_events = rr.n_events;
            rr.events = NULL;
            rr.n_events = 0;
            db_record_result_free(&rr);
        }
        if (st == OK) {
            snap = br.snapshot_id;
            res->files = br.files;
            /* A failed dump is one of the entries left out; it is named as
             * the dump it is, not counted as a file that could not be read. */
            uint64_t unread = br.files_skipped > u.streams_failed ? br.files_skipped - u.streams_failed : 0;
            if (u.streams_failed)
                one_warn(res, 0, "%zu dump(s) failed and are not in snapshot %lld; first: %s",
                         u.streams_failed, (long long)snap, u.stream_failed);
            if (unread)
                one_warn(res, 0, "%llu file(s) could not be read and are not in snapshot %lld",
                         (unsigned long long)unread, (long long)snap);
        }
        backup_result_free(&br);
    }
    /* Parts a failure left open. */
    for (size_t i = 0; u.parts && i < n_synth; i++) part_release(&u.parts[i]);

    /*
     * A bitmap that missed a change (5.4). The snapshot is right (it was a
     * full read) and stays; the unit fails so the job does, which is what
     * raises the alert, and the suspects are named.
     */
    for (size_t i = 0; st == OK && u.parts && i < n_synth; i++) {
        const part_t *pt = &u.parts[i];
        if (!pt->audited || !pt->ares.bad) continue;
        units_vm_count_audit_failure();
        char suspects[256] = "";
        size_t so = 0;
        char repo[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
        db_result_t *r = NULL;
        /* Suspect: what was read incrementally since the chain's last full
         * read (a first read, a broken chain, an audit that passed). */
        char sn[32];
        snprintf(sn, sizeof(sn), "%lld", (long long)snap);
        if (db_execute(db, "SELECT DISTINCT i.snap_id FROM snapshot_unit_items i JOIN snapshot_units s"
                           " ON s.repo_id = i.repo_id AND s.snap_id = i.snap_id WHERE s.repo_id = ?1"
                           " AND s.unit_key = ?2 AND i.read_mode IN ('incremental', 'unchanged')"
                           " AND i.snap_id > COALESCE((SELECT MAX(j.snap_id) FROM snapshot_unit_items j"
                           " JOIN snapshot_units t ON t.repo_id = j.repo_id AND t.snap_id = j.snap_id"
                           " WHERE t.repo_id = ?1 AND t.unit_key = ?2 AND j.kind = 'block'"
                           " AND j.read_mode = 'full' AND j.reason NOT IN ('no-incremental-support', 'forced')"
                           " AND j.snap_id < ?3), 0) ORDER BY i.snap_id LIMIT 20",
                       (const char *[]){ repo, key, sn, NULL }, &r) == 0 && r) {
            for (int k = 0; k < r->nrows && so + 24 < sizeof(suspects); k++)
                so += (size_t)snprintf(suspects + so, sizeof(suspects) - so, "%s%s", k ? ", " : "", r->rows[k][0]);
        }
        db_result_free(r);
        const char *item = u.bases[pt->vb].item;
        log_error("unit %s: bitmap audit failed on disk %s: %llu bytes of chunks changed outside the dirty "
                  "bitmap, the first at offset %llu; incrementals are off for this unit until reset-incremental; "
                  "snapshots read incrementally that are now suspect: %s", key, item,
                  (unsigned long long)pt->ares.bad_bytes, (unsigned long long)pt->ares.first_bad,
                  suspects[0] ? suspects : "none");
        st = set_error(ERR_CORRUPT, "bitmap audit failed: %llu bytes of chunks changed outside the dirty bitmap, "
                       "the first at offset %llu (disk %s); snapshot %lld is a full read and correct; incrementals are off until "
                       "reset-incremental; suspect incremental snapshots: %s",
                       (unsigned long long)pt->ares.bad_bytes, (unsigned long long)pt->ares.first_bad, item,
                       (long long)snap, suspects[0] ? suspects : "none");
        break;
    }

    char why[ERR_MSG_MAX];
    snprintf(why, sizeof(why), "%s", st != OK ? err_msg() : "");
    /* Committed is about the snapshot: one that a failed audit then fails
     * is still committed, and its checkpoint settles as such. */
    sp_end_outcome_t outcome = snap > 0 ? SP_END_COMMITTED : SP_END_ABORTED;
    /*
     * Committing settles the session's new checkpoint on the domain and
     * retires the old one, which no power cut on this host undoes, and the
     * token rows binding the new one are only in the WAL (B152). A cut
     * between the two kept the checkpoint and lost its binding: the next
     * session read in full, safely, and nothing ever retired the orphan.
     * So the binding is made durable first. If it cannot be, the new
     * checkpoint is not settled: the rows naming it then name nothing, and
     * the next session reads in full and makes another.
     */
    if (outcome == SP_END_COMMITTED && u.info->new_token && *u.info->new_token &&
        db_txn_make_durable(db) != OK) {
        log_warn("unit %s: the checkpoint binding is not durable (%s); its new checkpoint is "
                 "discarded and the next session reads in full", key, err_msg());
        err_clear();
        outcome = SP_END_ABORTED;
    }
    /* u.info lives in the session, which end frees (remote_provider). */
    /* Frozen and thawed by a watchdog still counts: the guest was frozen. */
    const uint64_t freeze_ms = u.info->freeze_ms;
    status_t est = ops->end(u.sess, outcome, snap);
    if (est != OK) {
        /*
         * Never fails a snapshot that committed: the sweep has the rows. A
         * helper killed or restarted mid-read lands here (B177). The read
         * itself could not have been cut short and still commit: every
         * part is read to its end or the backup fails (backup.c, a read
         * error aborts it; nbd_client, a closed connection is an error;
         * disk_stream, an image changed under an offline read fails it).
         * So the snapshot is whole, read from exports that outlived the
         * helper, and flagged: this warning outranks any other, so the job
         * warns and alerts with it.
         */
        log_warn("unit %s: releasing the session failed: %s; the sweep will retry",
                 key, err_msg());
        if (st == OK) {
            one_warn(res, 1, "the session could not be ended (%.200s): the provider went away or restarted "
                     "during the backup. Snapshot %lld is whole, every part read to its end; what the "
                     "session held is released by its sweep", err_msg(), (long long)snap);
        }
        err_clear();
    }
    res->snapshot_id = snap;
    res->freeze_ms = freeze_ms;
    if (u.cap_hit) res->achieved = SP_CONSIST_INCONSISTENT;
    else if (u.app_by_templates) res->achieved = SP_CONSIST_APPLICATION;
    res->bytes_read = u.bytes_read;
    if (st == OK)
        log_info("unit %s: snapshot %lld, %s, %llu disk bytes read", key, (long long)snap,
                 sp_consistency_name(res->achieved), (unsigned long long)u.bytes_read);

    unit_json_free(&u.uj);
    free(u.uj_text);
    db_findings_free(u.findings);
    db_detect_free(u.detect);
    sp_unit_probe_free(pp->probe);
    pp->probe = NULL;
    free(u.parts);
    for (size_t i = 0; i < u.n_warn; i++) free(u.warn[i]);
    free(u.warn);
    bases_free(pp);
    utmpl_free(&pp->tmpl);
    free(syn);
    for (size_t i = 0; synpath && i < n_synth; i++) free(synpath[i]);
    free(synpath);
    for (size_t i = 0; rbuf && i < n_trees; i++) free(rbuf[i]);
    for (size_t i = 0; i < n_excl; i++) free(excl[i]);
    free(rbuf);
    free(paths);
    free(rpaths);
    ops->desc_free(d);
    units_unlock(key);
    if (st != OK) return set_error(st, "%s", why);
    return OK;
}

static status_t run_one(store_t *store, cutils_db_t *db, int64_t repo_id,
                        const unit_backup_opts_t *o, const sp_provider_ops_t *ops,
                        sp_ctx_t *pctx, const listed_t *lu, size_t index, size_t total,
                        unit_backup_one_t *res)
{
    unit_prep_t pp;
    status_t st = unit_prepare(store, db, repo_id, o, ops, pctx, lu, res, &pp);
    if (st != OK) return st;
    sp_begin_req_t req;
    begin_req(o, &pp.holds, &pp, &req);
    size_t n_fb = 0;
    for (size_t i = 0; i < pp.d->n_items; i++) n_fb += is_fallback(&pp.d->items[i]) ? 1u : 0u;
    req.hold_pause = n_fb > 0 && ops->release && pp.d->state == SP_STATE_RUNNING;
    if (req.hold_pause && (st = fallback_pass1(store, db, repo_id, o, pp.d, pp.key)) != OK) {
        unit_unprepare(ops, &pp);
        return st;
    }
    /* Exec templates around begin (db-detection.md 7.2): the pre runs
     * while the unit is live, the post runs once begin has its point in
     * time, or has failed (a maintenance mode is never left on). */
    templates_exec(ops, pctx, pp.d->unit_id, &pp.tmpl, 0);
    st = ops->begin(pctx, pp.d, &req, &pp.sess, &pp.info);
    if (st != OK) {
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        templates_exec(ops, pctx, pp.d->unit_id, &pp.tmpl, 1);
        unit_unprepare(ops, &pp);
        return set_error(st, "%s", why);
    }
    templates_exec(ops, pctx, pp.d->unit_id, &pp.tmpl, 1);
    if (pp.tmpl.n && ops->templates_set(pp.sess, pp.tmpl.runs, pp.tmpl.n) != OK) {
        log_warn("unit %s: templates: %s", pp.key, err_msg());
        err_clear();
    }
    return unit_store(store, db, repo_id, o, ops, &pp, index, total, res);
}

/* ------------------------------------------------------------------ */

typedef struct {
    unsigned checked, redefined, refused, kept;
    char     last[UNIT_KEY_MAX + UNIT_TOKEN_MAX];   /* (unit, token) pairs come sorted: one call each */
    /* The libvirt units the helper lists, which is every one its policy
     * allows that still exists; listed 0 when the list failed, and then
     * every checkpoint is asked about as before (NIT-45). */
    int      listed;
    char   **ids;
    size_t   n_ids;
    char     last_kept[UNIT_KEY_MAX];
    const char *prov;
} redef_t;

static status_t listed_cb(void *ctx, const char *id, const char *name, int state)
{
    (void)name; (void)state;
    redef_t *r = ctx;
    char **g = realloc(r->ids, (r->n_ids + 1) * sizeof(*g));
    if (!g) return set_error(ERR_NOMEM, "units: checkpoint redefine");
    r->ids = g;
    if (!(g[r->n_ids] = strdup(id))) return set_error(ERR_NOMEM, "units: checkpoint redefine");
    r->n_ids++;
    return OK;
}

static int is_listed(const redef_t *r, const char *id)
{
    for (size_t i = 0; i < r->n_ids; i++) if (strcmp(r->ids[i], id) == 0) return 1;
    return 0;
}

static status_t redefine_one(void *ctx, int64_t repo_id, const char *unit_id, const char *token)
{
    (void)repo_id;
    redef_t *r = ctx;
    char pair[sizeof(r->last)];
    snprintf(pair, sizeof(pair), "%s %s", unit_id, token);
    if (strcmp(pair, r->last) == 0) return OK;
    snprintf(r->last, sizeof(r->last), "%s", pair);
    /*
     * A VM deleted, or taken out of the helper's policy, keeps its
     * checkpoint tokens: they are the repository's record, and the VM can
     * come back. The helper denies any question about a unit outside its
     * policy, and asking anyway logged "denied: ... is not in the policy" at
     * every start for every stored checkpoint (NIT-45). Not asked: kept,
     * and said once per unit at debug.
     */
    if (r->listed && !is_listed(r, unit_id)) {
        r->kept++;
        if (strcmp(r->last_kept, unit_id) != 0) {
            snprintf(r->last_kept, sizeof(r->last_kept), "%s", unit_id);
            log_debug("units: checkpoint of a removed unit kept: %s:%s is not a unit the helper lists "
                      "(deleted, or outside its policy); its tokens stay", r->prov, unit_id);
        }
        return OK;
    }
    cJSON *a = cJSON_CreateObject();
    char key[UNIT_KEY_MAX];
    unit_key_format(key, sizeof(key), r->prov, unit_id);
    if (a) {
        cJSON_AddStringToObject(a, "unit", key);
        cJSON_AddStringToObject(a, "checkpoint", token);
    }
    cJSON *res = NULL;
    r->checked++;
    if (remote_op(r->prov, "vm.checkpoint_redefine", a, -1, &res) != OK) {
        log_warn("units: checkpoint %s of %s: %s", token, key, err_msg());
        err_clear();
        r->refused++;
        return OK;
    }
    const char *state = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "state"));
    if (state && strcmp(state, "redefined") == 0) r->redefined++;
    else if (state && strcmp(state, "present") != 0) {
        r->refused++;
        const char *why = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "why"));
        log_warn("units: checkpoint %s of %s is %s%s%s; its disks read in full next time", token, key, state,
                 why ? ": " : "", why ? why : "");
    }
    cJSON_Delete(res);
    return OK;
}

status_t unit_checkpoints_redefine(cutils_db_t *db, unsigned *checked, unsigned *redefined,
                                   unsigned *refused, unsigned *kept)
{
    return unit_checkpoints_redefine_for(db, "libvirt", checked, redefined, refused, kept);
}

status_t unit_checkpoints_redefine_for(cutils_db_t *db, const char *prov, unsigned *checked,
                                       unsigned *redefined, unsigned *refused, unsigned *kept)
{
    redef_t r;
    memset(&r, 0, sizeof(r));
    r.prov = prov;
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *pctx = NULL;
    if (sp_registry_get(prov, &ops, &pctx) == OK && ops->list_units &&
        ops->list_units(pctx, listed_cb, &r) == OK) {
        r.listed = 1;
    } else {
        log_debug("units: checkpoint redefine: the unit list failed (%s); every checkpoint is asked about",
                  err_msg()[0] ? err_msg() : "no such provider");
        err_clear();
    }
    status_t st = unit_tokens_each(db, prov, redefine_one, &r);
    for (size_t i = 0; i < r.n_ids; i++) free(r.ids[i]);
    free(r.ids);
    if (checked) *checked = r.checked;
    if (redefined) *redefined = r.redefined;
    if (refused) *refused = r.refused;
    if (kept) *kept = r.kept;
    return st;
}

void unit_backup_result_free(unit_backup_result_t *r)
{
    if (!r) return;
    for (size_t i = 0; r->units && i < r->n; i++) {
        for (unsigned k = 0; r->units[i].warnings && k < r->units[i].n_warnings; k++)
            free(r->units[i].warnings[k]);
        free(r->units[i].warnings);
        free(r->units[i].databases_json);
        free(r->units[i].db_events);
    }
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

/* A unit's capture group (unit_policy capture.group), "" for none. */
static void group_of(cutils_db_t *db, int64_t repo_id, const char *key, char *buf, size_t sz)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    buf[0] = '\0';
    if (db_execute(db, "SELECT value FROM unit_policy WHERE repo_id = ? AND unit_key = ? AND key = 'capture.group'",
                   (const char *[]){ repo, key, NULL }, &r) == 0 && r && r->nrows == 1)
        snprintf(buf, sz, "%s", r->rows[0][0]);
    db_result_free(r);
}

/* What one unit's attempt leaves in the job's result. */
static void tally(unit_backup_result_t *out, unit_backup_one_t *r, status_t one,
                  status_t *first_st, char *first_err, size_t fsz)
{
    r->st = one;
    if (one == OK) { out->ok++; return; }
    msg_clip(r->error, sizeof(r->error), err_msg());
    log_warn("unit backup: %s: %s", r->unit_key, r->error);
    err_clear();
    out->failed++;
    if (*first_st == OK) {
        *first_st = one;
        msg_clip_after(first_err, fsz, snprintf(first_err, fsz, "%.150s: %s", r->unit_key, r->error));
    }
}

/*
 * Several units in one capture (pause scope group or job, 6.2): each
 * prepared, then one begin_group, then each read from its own session. A
 * unit that cannot be prepared fails alone; a begin that fails fails them
 * all.
 */
static void run_group(store_t *store, cutils_db_t *db, int64_t repo_id, const unit_backup_opts_t *o,
                      const sp_provider_ops_t *ops, sp_ctx_t *pctx, const listed_t *const *lus,
                      unit_backup_one_t **rs, size_t n, size_t first_index, size_t total,
                      unit_backup_result_t *out, status_t *first_st, char *first_err, size_t fsz)
{
    unit_prep_t *pp = calloc(n, sizeof(*pp));
    size_t *who = calloc(n, sizeof(*who));
    const sp_unit_desc_t **descs = calloc(n, sizeof(*descs));
    sp_session_t **sess = calloc(n, sizeof(*sess));
    const sp_session_info_t **infos = calloc(n, sizeof(*infos));
    if (!pp || !who || !descs || !sess || !infos) {
        set_error(ERR_NOMEM, "unit backup: group");
        for (size_t k = 0; k < n; k++) tally(out, rs[k], ERR_NOMEM, first_st, first_err, fsz);
        free(pp); free(who); free(descs); free(sess); free(infos);
        return;
    }
    size_t m = 0;
    for (size_t k = 0; k < n; k++) {
        status_t st = unit_prepare(store, db, repo_id, o, ops, pctx, lus[k], rs[k], &pp[m]);
        if (st != OK) { tally(out, rs[k], st, first_st, first_err, fsz); continue; }
        who[m] = k;
        descs[m] = pp[m].d;
        m++;
    }
    if (m) {
        log_info("unit backup: %zu unit(s) in one capture", m);
        sp_begin_req_t req;
        begin_req(o, &pp[0].holds, NULL, &req);
        for (size_t q = 0; q < m; q++) templates_exec(ops, pctx, pp[q].d->unit_id, &pp[q].tmpl, 0);
        status_t st = ops->begin_group(pctx, descs, m, &req, sess, infos);
        char gwhy[ERR_MSG_MAX];
        snprintf(gwhy, sizeof(gwhy), "%s", err_msg());
        for (size_t q = 0; q < m; q++) templates_exec(ops, pctx, pp[q].d->unit_id, &pp[q].tmpl, 1);
        if (st != OK) set_error(st, "%s", gwhy);
        if (st != OK) {
            char why[ERR_MSG_MAX];
            snprintf(why, sizeof(why), "%s", err_msg());
            for (size_t q = 0; q < m; q++) {
                unit_unprepare(ops, &pp[q]);
                set_error(st, "%s", why);
                tally(out, rs[who[q]], st, first_st, first_err, fsz);
            }
        } else {
            for (size_t q = 0; q < m; q++) {
                pp[q].sess = sess[q];
                pp[q].info = infos[q];
                if (pp[q].tmpl.n && ops->templates_set(sess[q], pp[q].tmpl.runs, pp[q].tmpl.n) != OK) {
                    log_warn("unit %s: templates: %s", pp[q].key, err_msg());
                    err_clear();
                }
                status_t one = unit_store(store, db, repo_id, o, ops, &pp[q], first_index + who[q], total, rs[who[q]]);
                tally(out, rs[who[q]], one, first_st, first_err, fsz);
            }
        }
    }
    free(pp); free(who); free(descs); free(sess); free(infos);
}

status_t unit_backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                         const unit_backup_opts_t *o, unit_backup_result_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!o || !o->provider || (!o->all && o->n_units == 0))
        return set_error(ERR_INVALID, "unit backup: name a provider and units, or all");
    /* A provider behind a helper that is not there now: the job fails,
     * and its failure (and alert) names the helper's socket. Never a
     * quiet skip. */
    char hwhy[512];
    if (remote_unusable_for_job(o->provider, hwhy, sizeof(hwhy)))
        return set_error(ERR_IO, "%s", hwhy);
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
    char first_err[MSG_CLIP_MAX + 160] = "";
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

    /* Capture groups: everything under job scope, units sharing a
     * capture.group under group scope; a group of one is a unit. */
    char (*grp)[64] = calloc(np ? np : 1, sizeof(*grp));
    int *done = calloc(np ? np : 1, sizeof(*done));
    if (!grp || !done) st = set_error(ERR_NOMEM, "unit backup");
    /* Group and job scope need a provider that captures units together.
     * One that cannot (an in-process provider) takes each unit alone, and
     * says so: it used to do that without a word. */
    if (st == OK && !ops->begin_group && o->pause_scope != UNIT_SCOPE_UNIT && np > 1) {
        snprintf(out->scope_note, sizeof(out->scope_note), "pause scope %s needs a provider that captures "
                 "units together; provider %s does not, so each unit was its own capture",
                 o->pause_scope == UNIT_SCOPE_JOB ? "job" : "group", ops->name);
        log_warn("unit backup: %s", out->scope_note);
    }
    if (st == OK && ops->begin_group && o->pause_scope != UNIT_SCOPE_UNIT) {
        for (size_t k = 0; k < np; k++) {
            if (o->pause_scope == UNIT_SCOPE_JOB) { snprintf(grp[k], sizeof(grp[k]), "job"); continue; }
            char key[UNIT_KEY_MAX];
            unit_key_format(key, sizeof(key), ops->name, l.v[pick[k]].id);
            group_of(db, repo_id, key, grp[k], sizeof(grp[k]));
        }
    }
    for (size_t k = 0; st == OK && grp && done && k < np; k++) {
        if (done[k]) continue;
        size_t members = 0;
        for (size_t q = k; grp[k][0] && q < np; q++) members += !done[q] && strcmp(grp[q], grp[k]) == 0;
        if (members > 1 && !(o->cancel && atomic_load(o->cancel))) {
            const listed_t **lus = calloc(members, sizeof(*lus));
            unit_backup_one_t **rs = calloc(members, sizeof(*rs));
            size_t g = 0;
            for (size_t q = k; lus && rs && q < np; q++) {
                if (done[q] || strcmp(grp[q], grp[k]) != 0) continue;
                unit_backup_one_t *r = result_add(out);
                if (!r) break;
                lus[g] = &l.v[pick[q]];
                rs[g++] = r;
                done[q] = 1;
            }
            /* result_add may move the array: take the pointers afresh. */
            for (size_t q = 0; rs && q < g; q++) rs[q] = &out->units[out->n - g + q];
            if (!lus || !rs || g != members) st = set_error(ERR_NOMEM, "unit backup");
            else run_group(store, db, repo_id, o, ops, pctx, lus, rs, g, k, np, out, &first_st, first_err, sizeof(first_err));
            free(lus);
            free(rs);
            continue;
        }
        done[k] = 1;
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
        tally(out, r, one, &first_st, first_err, sizeof(first_err));
    }
    free(grp);
    free(done);
    free(pick);
    listing_free(&l);

    /* Hygiene after every job: whatever a failed release left journalled
     * is released now rather than at the next start. */
    units_sweep_result_t sw;
    if (units_sweep(db, o->job_id, &sw) != OK) {
        log_warn("unit backup: sweep: %s", err_msg());
        err_clear();
    } else if (sw.stuck[0]) {
        snprintf(out->warning, sizeof(out->warning), "%s", sw.stuck);
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
