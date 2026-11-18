#define _GNU_SOURCE

#include "unit_detect.h"

#include "db_findings.h"
#include "db_txn.h"
#include "open_scanned.h"
#include "registry.h"
#include "scan.h"
#include "unit_backup.h"
#include "unit_index.h"
#include "unit_state.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

sp_unit_probe_t *unit_detect_probe(const sp_provider_ops_t *ops, sp_ctx_t *ctx,
                                   const char *unit_id, const char *key)
{
    if (!ops || !ops->unit_probe || !db_detect_settings().enabled) return NULL;
    sp_unit_probe_t *p = NULL;
    if (ops->unit_probe(ctx, unit_id, &p) != OK) {
        log_warn("unit %s: units.probe failed, databases are judged on their files alone: %s",
                 key, err_msg());
        err_clear();
        sp_unit_probe_free(p);
        return NULL;
    }
    if (p && p->truncated)
        log_info("unit %s: units.probe had more than it could return; liveness is judged on part", key);
    return p;
}

/* The tree holding a recorded path, longest root first; -1 for none. */
static int tree_of(const char *const *rec, size_t n, const char *p)
{
    int best = -1;
    size_t bl = 0;
    for (size_t i = 0; i < n; i++) {
        size_t l = strlen(rec[i]);
        if (strncmp(p, rec[i], l) != 0 || (p[l] != '/' && p[l] != '\0')) continue;
        if (best < 0 || l > bl) { best = (int)i; bl = l; }
    }
    return best;
}

typedef struct { db_detector_t *d; const char *const *rec, *const *rd; size_t n; } reread_t;

static void reread_one(void *ctx, const char *path)
{
    const reread_t *r = ctx;
    int t = tree_of(r->rec, r->n, path);
    if (t < 0) return;
    char rp[PATH_MAX];
    if (snprintf(rp, sizeof(rp), "%s%s", r->rd[t], path + strlen(r->rec[t])) >= (int)sizeof(rp)) return;
    int fd = open(rp, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) == 0 && S_ISREG(st.st_mode))
        db_detect_head_fd(r->d, path, fd, (uint64_t)st.st_size);
    close(fd);
}

void unit_detect_reread(db_detector_t *d, const char *const *rec, const char *const *rd, size_t n)
{
    if (!d || !rec || !rd || !n) return;
    reread_t r = { d, rec, rd, n };
    db_detect_nextcloud_unread(d, reread_one, &r);
}

/* ------------------------------------------------------------------ */
/* The detect run                                                       */
/* ------------------------------------------------------------------ */

typedef struct { const char *want; char id[SP_UNIT_ID_MAX]; char name[256]; int hit; } find_t;

static status_t find_cb(void *ctx, const char *id, const char *name, int state)
{
    (void)state;
    find_t *f = ctx;
    if (f->hit || (strcmp(id, f->want) != 0 && strcmp(name, f->want) != 0)) return OK;
    snprintf(f->id, sizeof(f->id), "%s", id);
    snprintf(f->name, sizeof(f->name), "%s", name);
    f->hit = 1;
    return OK;
}

/* A classification kept for the next backup, only for a file the cache
 * row still describes: a file changed since is read by the backup anyway,
 * and a row for it must not vouch for a head it never had. */
typedef struct {
    cutils_db_t       *db;
    const char        *repo;
    const char *const *rec, *const *rd;
    size_t             n;
    unsigned           kept, since;
    int                failed;
} keep_t;

#define KEEP_BATCH 1000u

static void keep_head(void *ctx, const char *path, uint32_t code)
{
    keep_t *k = ctx;
    if (k->failed) return;
    int t = tree_of(k->rec, k->n, path);
    char rp[PATH_MAX];
    struct stat st;
    if (t < 0 || snprintf(rp, sizeof(rp), "%s%s", k->rd[t], path + strlen(k->rec[t])) >= (int)sizeof(rp) ||
        lstat(rp, &st) != 0 || !S_ISREG(st.st_mode))
        return;
    if (k->since == KEEP_BATCH) {
        k->since = 0;
        if (db_txn_commit(k->db) != OK || db_txn_begin_bulk(k->db) != OK) { k->failed = 2; return; }
    }
    k->since++;
    char c[16], sz[32], mt[32], ct[32];
    snprintf(c, sizeof(c), "%u", code);
    snprintf(sz, sizeof(sz), "%lld", (long long)st.st_size);
    snprintf(mt, sizeof(mt), "%lld", (long long)st.st_mtim.tv_sec * 1000000000LL + st.st_mtim.tv_nsec);
    snprintf(ct, sizeof(ct), "%lld", (long long)st.st_ctim.tv_sec * 1000000000LL + st.st_ctim.tv_nsec);
    db_result_t *r = NULL;
    if (db_execute(k->db, "UPDATE file_cache SET db_head = ? WHERE repo_id = ? AND path = ?"
                          " AND size = ? AND mtime_ns = ? AND ctime_ns = ? AND db_head = 0"
                          " RETURNING 1",
                   (const char *[]){ c, k->repo, path, sz, mt, ct, NULL }, &r) != 0)
        k->failed = 1;
    else if (r && r->nrows) k->kept++;
    db_result_free(r);
}

static void keep_heads(cutils_db_t *db, int64_t repo_id, const db_detector_t *d,
                       const char *const *rec, const char *const *rd, size_t n, unsigned *kept)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    keep_t k = { db, repo, rec, rd, n, 0, 0, 0 };
    if (db_txn_begin_bulk(db) != OK) {
        log_warn("detect: classifications not kept for the next backup: %s", err_msg());
        err_clear();
        return;
    }
    db_detect_heads_read(d, keep_head, &k);
    if (k.failed) {
        if (k.failed == 1) db_txn_rollback(db);
        log_warn("detect: classifications not kept for the next backup");
    } else if (db_txn_commit(db) != OK) {
        log_warn("detect: classifications not kept for the next backup: %s", err_msg());
    }
    err_clear();
    *kept = k.kept;
}

/* One tree, scanned where a live read would read it. Entries are dropped
 * with the tree, so memory is the largest tree's, not the unit's. */
static status_t detect_tree(db_detector_t *d, const char *rd, const char *rec,
                            const char *const *excl, size_t n_excl, atomic_int *cancel,
                            unit_detect_result_t *out)
{
    scan_imap_t *imap = scan_imap_new();
    if (!imap) return set_error(ERR_NOMEM, "detect: scan");
    scan_opts_t so;
    memset(&so, 0, sizeof(so));
    so.exclude = (const char **)(uintptr_t)excl;
    so.n_exclude = (int)n_excl;
    so.collect_meta = 0;
    scan_result_t *scan = NULL;
    status_t st = strcmp(rd, rec) != 0 ? scan_tree_as(rd, rec, imap, &so, &scan)
                                       : scan_tree(rec, imap, &so, &scan);
    scan_imap_free(imap);
    if (st != OK) { scan_result_free(scan); return st; }
    scan_roots_t *roots = NULL;
    if ((st = scan_roots_new(scan, &roots)) != OK) { scan_result_free(scan); return st; }
    for (uint32_t i = 0; i < scan->count; i++) {
        if ((i & 1023u) == 0 && cancel && atomic_load(cancel)) {
            st = set_error(ERR_BUSY, "detect cancelled");
            break;
        }
        const scan_entry_t *e = &scan->entries[i];
        int is_dir = e->node.type == NODE_TYPE_DIR, is_link = e->node.type == NODE_TYPE_SYMLINK;
        db_detect_entry(d, e->path, is_dir, is_link, e->node.size, e->symlink_target);
        out->entries++;
        if (e->node.type != NODE_TYPE_REG || e->node.size == 0 || e->synthetic) continue;
        struct stat fst;
        char why[256];
        int fd = open_scanned(roots, e, 0, &fst, why, sizeof(why));
        if (fd < 0) continue;
        db_detect_head_fd(d, e->path, fd, (uint64_t)fst.st_size);
        close(fd);
        out->files_read++;
    }
    scan_roots_free(roots);
    scan_result_free(scan);
    return st;
}

status_t unit_detect_run(cutils_db_t *db, int64_t repo_id, const char *prov, const char *unit,
                         atomic_int *cancel,
                         void (*progress)(const char *phase, uint64_t done, uint64_t total, void *ctx),
                         void *progress_ctx, unit_detect_result_t *out)
{
    memset(out, 0, sizeof(*out));
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    status_t st = sp_registry_get(prov, &ops, &ctx);
    if (st != OK) return st;
    find_t f;
    memset(&f, 0, sizeof(f));
    f.want = unit;
    if (!ops->list_units || ops->list_units(ctx, find_cb, &f) != OK)
        return set_error_wrap(ERR_IO, "detect: cannot list provider %s", prov);
    if (!f.hit) return set_error(ERR_NOT_FOUND, "provider %s lists no unit %s", prov, unit);
    if (!units_allowed(prov, f.id, f.name))
        return set_error(ERR_INVALID, "unit %s is not in units.allow", unit);
    unit_key_format(out->unit_key, sizeof(out->unit_key), prov, f.id);
    snprintf(out->name, sizeof(out->name), "%s", f.name);
    const char *key = out->unit_key;

    db_detector_t *d = db_detect_start();
    if (!d) return set_error(ERR_INVALID, "database detection is off (db_detect.enabled = 0)");
    sp_unit_desc_t *desc = NULL;
    if ((st = ops->describe(ctx, f.id, &desc)) != OK) { db_detect_free(d); return st; }
    sp_unit_probe_t *probe = unit_detect_probe(ops, ctx, f.id, key);
    out->probed = probe != NULL;
    db_detect_unit(d, probe);
    sp_unit_probe_free(probe);

    size_t nt = 0;
    for (size_t i = 0; i < desc->n_items; i++) nt += desc->items[i].kind == SP_ITEM_TREE;
    const char **rec = calloc(nt ? nt : 1, sizeof(*rec));
    const char **rd = calloc(nt ? nt : 1, sizeof(*rd));
    db_cap_root_t *roots = calloc(nt ? nt : 1, sizeof(*roots));
    char *excl[UNIT_EXCLUDE_MAX];
    size_t n_excl = 0;
    if (!rec || !rd || !roots) st = set_error(ERR_NOMEM, "detect");
    size_t k = 0;
    for (size_t i = 0; st == OK && i < desc->n_items; i++) {
        const sp_item_desc_t *it = &desc->items[i];
        if (it->kind != SP_ITEM_TREE) continue;
        /* Where pause-read's first pass reads a tree on no snapshot domain:
         * the host path, which the daemon sees as the host does. */
        rec[k] = it->tree_path;
        rd[k] = it->source && it->source[0] == '/' ? it->source : it->tree_path;
        /* The capture the next backup would make (unit_backup.c
         * unit_detect): a snapshot-domain tree is atomic; one on no domain
         * is read with the unit paused when it is running and can be, live
         * otherwise; a stopped unit is offline, at rest. */
        roots[k].root = it->tree_path;
        if (desc->state == SP_STATE_STOPPED || (it->caps & SP_CAP_SNAPSHOT))
            roots[k].kind = DB_CAP_ATOMIC;
        else if (desc->state == SP_STATE_RUNNING && ops->release)
            roots[k].kind = DB_CAP_PAUSED;
        else
            roots[k].kind = DB_CAP_LIVE;
        roots[k].instant = 0;
        k++;
    }
    if (st == OK) n_excl = unit_backup_excludes(db, repo_id, key, rec, rd, nt, excl);
    for (size_t i = 0; st == OK && i < nt; i++) {
        if (progress) progress("detect", i, nt, progress_ctx);
        status_t ts = detect_tree(d, rd[i], rec[i], (const char *const *)excl, n_excl, cancel, out);
        if (ts == ERR_BUSY) { st = ts; break; }
        if (ts != OK) {
            /* One mount that cannot be read leaves the others' findings. */
            log_warn("unit %s: detect: %s not scanned: %s", key, rec[i], err_msg());
            err_clear();
        }
    }
    if (st == OK && progress) progress("detect", nt, nt, progress_ctx);
    if (st == OK) {
        db_capture_desc_t cap = { roots, nt, 0, 0, 1, 1 };
        st = db_detect_finish(d, &cap, &out->findings);
    }
    if (st == OK) {
        char scope[UNIT_KEY_MAX + 8];
        snprintf(scope, sizeof(scope), "unit:%s", key);
        db_findings_link_nextcloud(db, repo_id, scope, out->findings);
        keep_heads(db, repo_id, d, rec, rd, nt, &out->heads_kept);
    }
    for (size_t i = 0; i < n_excl; i++) free(excl[i]);
    free(rec);
    free(rd);
    free(roots);
    ops->desc_free(desc);
    db_detect_free(d);
    if (st != OK) {
        db_findings_free(out->findings);
        out->findings = NULL;
    }
    return st;
}
