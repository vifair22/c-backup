#define _GNU_SOURCE

#include "snap.h"
#include "artefact.h"

#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define SNAP_TOOL_TIMEOUT_S 120
#define SNAP_LIST_CAP       ((size_t)4 * 1024 * 1024)
#define OWNER_PROP          "com.cbackup:owner"

/* path is root or below it. */
static int under(const char *path, const char *root)
{
    size_t n = strlen(root);
    if (n == 1 && root[0] == '/') return 1;
    return strncmp(path, root, n) == 0 && (path[n] == '\0' || path[n] == '/');
}

static int zfs_allowed(const helper_policy_t *p, const char *ds)
{
    for (size_t i = 0; i < p->n_zfs; i++)
        if (strcmp(p->zfs_domains[i], ds) == 0) return 1;
    return 0;
}

status_t snap_domain_of(const snap_ctx_t *c, const char *real_path, snap_dom_t *out)
{
    memset(out, 0, sizeof(*out));
    size_t best = 0;
    for (size_t i = 0; i < c->pol->n_btrfs; i++) {
        const char *r = c->pol->btrfs_domains[i];
        if (under(real_path, r) && strlen(r) > best) {
            best = strlen(r);
            out->kind = SNAP_BTRFS;
            out->dataset[0] = '\0';
            snprintf(out->root, sizeof(out->root), "%s", r);
        }
    }
    if (c->pol->n_zfs == 0) return OK;
    const char *argv[] = { c->tools->zfs, "list", "-H", "-o", "name,mountpoint", "-t", "filesystem", NULL };
    char *txt = NULL;
    status_t st = host_run(c->tools, 0, argv, SNAP_TOOL_TIMEOUT_S, &txt, SNAP_LIST_CAP);
    if (st != OK) return st;
    /* The dataset with the longest mountpoint over the path is the one the
     * path's bytes live in; only that one may be snapshotted, and only if
     * the policy names it (a parent would cover far more than asked). */
    char zbest_ds[256] = "", zbest_mp[PATH_MAX] = "";
    size_t zbest = 0;
    char *save = NULL;
    for (char *l = strtok_r(txt, "\n", &save); l; l = strtok_r(NULL, "\n", &save)) {
        char *tab = strchr(l, '\t');
        if (!tab) continue;
        *tab = '\0';
        const char *mp = tab + 1;
        if (mp[0] != '/' || !under(real_path, mp) || strlen(mp) < zbest) continue;
        zbest = strlen(mp);
        snprintf(zbest_ds, sizeof(zbest_ds), "%s", l);
        snprintf(zbest_mp, sizeof(zbest_mp), "%s", mp);
    }
    free(txt);
    if (zbest_ds[0] && zbest >= best) {
        if (!zfs_allowed(c->pol, zbest_ds)) {
            memset(out, 0, sizeof(*out));
            log_info("snap: %s is in dataset %s, which the policy does not list", real_path, zbest_ds);
            return OK;
        }
        out->kind = SNAP_ZFS;
        snprintf(out->dataset, sizeof(out->dataset), "%s", zbest_ds);
        snprintf(out->root, sizeof(out->root), "%s", zbest_mp);
    }
    return OK;
}

static void pool_of(const char *ds, char *buf, size_t sz)
{
    size_t k = strcspn(ds, "/");
    snprintf(buf, sz, "%.*s", (int)k, ds);
}

unsigned snap_instants(const snap_dom_t *doms, size_t n)
{
    unsigned k = 0;
    for (size_t i = 0; i < n; i++) {
        if (doms[i].kind == SNAP_BTRFS) { k++; continue; }
        if (doms[i].kind != SNAP_ZFS) continue;
        char pi[256];
        pool_of(doms[i].dataset, pi, sizeof(pi));
        int seen = 0;
        for (size_t j = 0; j < i && !seen; j++) {
            char pj[256];
            if (doms[j].kind != SNAP_ZFS) continue;
            pool_of(doms[j].dataset, pj, sizeof(pj));
            seen = strcmp(pi, pj) == 0;
        }
        if (!seen) k++;
    }
    return k;
}

status_t snap_set_add(snap_set_t *s, const snap_dom_t *d)
{
    if (d->kind == SNAP_NONE) return OK;
    for (size_t i = 0; i < s->n; i++)
        if (s->doms[i].kind == d->kind && strcmp(s->doms[i].dataset, d->dataset) == 0 &&
            strcmp(s->doms[i].root, d->root) == 0) return OK;
    if (s->n >= SNAP_DOMS_MAX) return set_error(ERR_INVALID, "snap: more than %d snapshot domains", SNAP_DOMS_MAX);
    s->doms[s->n++] = *d;
    return OK;
}

static void btrfs_snapdir(const snap_ctx_t *c, const char *root, char *buf, size_t sz)
{
    if (c->btrfs_snapdir[0] && under(c->btrfs_snapdir, root))
        snprintf(buf, sz, "%s", c->btrfs_snapdir);
    else
        snprintf(buf, sz, "%s/system/.cbackup-snapshots", strcmp(root, "/") == 0 ? "" : root);
}

static status_t journal(const sp_holds_t *h, const char *detail, int64_t *id)
{
    *id = 0;
    return h && h->record ? h->record(h->ctx, SP_HOLD_FS_SNAPSHOT, detail, id) : OK;
}

static void unjournal(const sp_holds_t *h, int64_t *id)
{
    if (h && h->clear && *id) {
        if (h->clear(h->ctx, *id) != OK) err_clear();
    }
    *id = 0;
}

static uint64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

status_t snap_take(const snap_ctx_t *c, snap_set_t *s, const char *daemon8, int64_t seq,
                   const sp_holds_t *holds)
{
    uint64_t t0 = now_ms();
    status_t st = OK;
    /* One name per instant: every dataset of a pool shares its pool's. */
    int64_t k = 0;
    for (size_t i = 0; i < s->n && st == OK; i++) {
        if (s->names[i][0]) continue;
        if (artefact_name(s->names[i], sizeof(s->names[i]), daemon8, seq, k) != 0) {
            st = set_error(ERR_INVALID, "snap: cannot name a snapshot");
            break;
        }
        if (s->doms[i].kind == SNAP_ZFS) {
            char pi[256];
            pool_of(s->doms[i].dataset, pi, sizeof(pi));
            for (size_t j = i + 1; j < s->n; j++) {
                char pj[256];
                if (s->doms[j].kind != SNAP_ZFS) continue;
                pool_of(s->doms[j].dataset, pj, sizeof(pj));
                if (strcmp(pi, pj) == 0) memcpy(s->names[j], s->names[i], sizeof(s->names[i]));
            }
        }
        k++;
    }
    /* ZFS: one command per pool, so the pool's datasets share one instant. */
    for (size_t i = 0; i < s->n && st == OK; i++) {
        if (s->doms[i].kind != SNAP_ZFS || s->taken[i]) continue;
        char pi[256];
        pool_of(s->doms[i].dataset, pi, sizeof(pi));
        const char *argv[SNAP_DOMS_MAX + 8];
        char targets[SNAP_DOMS_MAX][320];
        char owner[64];
        size_t n = 0, members[SNAP_DOMS_MAX], nm = 0;
        snprintf(owner, sizeof(owner), OWNER_PROP "=%s", daemon8);
        argv[n++] = c->tools->zfs;
        argv[n++] = "snapshot";
        argv[n++] = "-o";
        argv[n++] = owner;
        for (size_t j = i; j < s->n && st == OK; j++) {
            char pj[256];
            if (s->doms[j].kind != SNAP_ZFS || s->taken[j]) continue;
            pool_of(s->doms[j].dataset, pj, sizeof(pj));
            if (strcmp(pi, pj) != 0) continue;
            snprintf(targets[nm], sizeof(targets[nm]), "%s@%s", s->doms[j].dataset, s->names[j]);
            char detail[340];
            snprintf(detail, sizeof(detail), "zfs:%s", targets[nm]);
            st = journal(holds, detail, &s->hold[j]);
            argv[n++] = targets[nm];
            members[nm++] = j;
        }
        argv[n] = NULL;
        if (st == OK) st = host_run(c->tools, 0, argv, SNAP_TOOL_TIMEOUT_S, NULL, 0);
        for (size_t m = 0; m < nm; m++) {
            if (st == OK) s->taken[members[m]] = 1;
            else unjournal(holds, &s->hold[members[m]]);
        }
    }
    for (size_t i = 0; i < s->n && st == OK; i++) {
        if (s->doms[i].kind != SNAP_BTRFS || s->taken[i]) continue;
        char dir[PATH_MAX], dst[PATH_MAX + 80], hdir[PATH_MAX + HOST_TOOL_PATH_MAX];
        btrfs_snapdir(c, s->doms[i].root, dir, sizeof(dir));
        if ((st = snap_check_snapdir(c, s->doms[i].root)) != OK) break;
        snprintf(dst, sizeof(dst), "%s/%s", dir, s->names[i]);
        if (host_path(c->tools, dir, hdir, sizeof(hdir)) != 0 ||
            (mkdir(hdir, 0700) != 0 && errno != EEXIST)) {
            st = set_error_errno(ERR_IO, "snap: cannot create %s", dir);
            break;
        }
        char detail[PATH_MAX + 96];
        snprintf(detail, sizeof(detail), "btrfs:%s", dst);
        if ((st = journal(holds, detail, &s->hold[i])) != OK) break;
        const char *argv[] = { c->tools->btrfs, "subvolume", "snapshot", "-r", s->doms[i].root, dst, NULL };
        st = host_run(c->tools, 0, argv, SNAP_TOOL_TIMEOUT_S, NULL, 0);
        if (st == OK) s->taken[i] = 1;
        else unjournal(holds, &s->hold[i]);
    }
    s->snapshot_ms = now_ms() - t0;
    if (st != OK) {
        char why[512];
        snprintf(why, sizeof(why), "%.500s", err_msg());
        (void)snap_release(c, s, daemon8, holds);
        err_clear();
        return set_error(st, "%s", why);
    }
    return OK;
}

status_t snap_read_path(const snap_ctx_t *c, const snap_set_t *s, const char *real_path,
                        char *buf, size_t sz)
{
    size_t best = 0, at = SIZE_MAX;
    for (size_t i = 0; i < s->n; i++)
        if (s->taken[i] && under(real_path, s->doms[i].root) && strlen(s->doms[i].root) >= best) {
            best = strlen(s->doms[i].root);
            at = i;
        }
    if (at == SIZE_MAX) return set_error(ERR_NOT_FOUND, "snap: %s is in no snapshot of this capture", real_path);
    const char *rest = real_path + (strcmp(s->doms[at].root, "/") == 0 ? 0 : best);
    int n;
    if (s->doms[at].kind == SNAP_ZFS) {
        n = snprintf(buf, sz, "%s/.zfs/snapshot/%s%s", strcmp(s->doms[at].root, "/") == 0 ? "" : s->doms[at].root,
                     s->names[at], rest);
    } else {
        char dir[PATH_MAX];
        btrfs_snapdir(c, s->doms[at].root, dir, sizeof(dir));
        n = snprintf(buf, sz, "%s/%s%s", dir, s->names[at], rest);
    }
    if (n < 0 || (size_t)n >= sz) return set_error(ERR_INVALID, "snap: read path too long");
    return OK;
}

/* ---- destroy, with the ownership proofs ---- */

static int ours(const char *name, const char *daemon8, int64_t *seq)
{
    char d8[9];
    int64_t job = 0, n = 0;
    if (artefact_parse(name, d8, &job, &n) != 0) return 0;
    if (seq) *seq = job;
    return strcmp(d8, daemon8) == 0;
}

static status_t zfs_destroy(const snap_ctx_t *c, const char *target, const char *daemon8)
{
    const char *at = strchr(target, '@');
    if (!at || !ours(at + 1, daemon8, NULL))
        return set_error(ERR_INVALID, "snap: %s is not named as ours; left alone", target);
    const char *g[] = { c->tools->zfs, "get", "-H", "-o", "value", OWNER_PROP, target, NULL };
    char *val = NULL;
    status_t st = host_run(c->tools, 0, g, SNAP_TOOL_TIMEOUT_S, &val, 4096);
    if (st != OK) {
        if (strstr(err_msg(), "does not exist")) { err_clear(); return OK; }
        return st;
    }
    val[strcspn(val, "\n")] = '\0';
    int mine = strcmp(val, daemon8) == 0;
    free(val);
    if (!mine) return set_error(ERR_INVALID, "snap: %s is not owned by this daemon; left alone", target);
    const char *d[] = { c->tools->zfs, "destroy", target, NULL };
    return host_run(c->tools, 0, d, SNAP_TOOL_TIMEOUT_S, NULL, 0);
}

static void line_value(const char *txt, const char *key, char *buf, size_t sz)
{
    buf[0] = '\0';
    size_t kl = strlen(key);
    for (const char *l = txt; l && *l; ) {
        while (*l == ' ' || *l == '\t') l++;
        if (strncmp(l, key, kl) == 0) {
            const char *v = l + kl;
            while (*v == ' ' || *v == '\t') v++;
            size_t n = strcspn(v, "\n");
            snprintf(buf, sz, "%.*s", (int)n, v);
            return;
        }
        l = strchr(l, '\n');
        if (l) l++;
    }
}

/* The btrfs domain whose snapshot directory holds path. */
static const char *btrfs_root_of(const snap_ctx_t *c, const char *path)
{
    for (size_t i = 0; i < c->pol->n_btrfs; i++) {
        char dir[PATH_MAX];
        btrfs_snapdir(c, c->pol->btrfs_domains[i], dir, sizeof(dir));
        if (under(path, dir) && strcmp(path, dir) != 0) return c->pol->btrfs_domains[i];
    }
    return NULL;
}

static status_t btrfs_destroy(const snap_ctx_t *c, const char *path, const char *daemon8)
{
    const char *root = btrfs_root_of(c, path);
    const char *base = strrchr(path, '/');
    if (!root || !base || !ours(base + 1, daemon8, NULL))
        return set_error(ERR_INVALID, "snap: %s is not a snapshot of ours; left alone", path);
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    struct stat sb;
    if (host_path(c->tools, path, hp, sizeof(hp)) == 0 && lstat(hp, &sb) != 0 && errno == ENOENT) return OK;
    /* Read-only, and a snapshot of the pool's top level: nothing else. */
    const char *pr[] = { c->tools->btrfs, "property", "get", "-ts", path, "ro", NULL };
    char *ro = NULL;
    status_t st = host_run(c->tools, 0, pr, SNAP_TOOL_TIMEOUT_S, &ro, 4096);
    if (st != OK) return st;
    int is_ro = strstr(ro, "ro=true") != NULL;
    free(ro);
    if (!is_ro) return set_error(ERR_INVALID, "snap: %s is not read-only; left alone", path);
    const char *sh1[] = { c->tools->btrfs, "subvolume", "show", path, NULL };
    const char *sh2[] = { c->tools->btrfs, "subvolume", "show", root, NULL };
    char *a = NULL, *b = NULL;
    if ((st = host_run(c->tools, 0, sh1, SNAP_TOOL_TIMEOUT_S, &a, 65536)) != OK) return st;
    if ((st = host_run(c->tools, 0, sh2, SNAP_TOOL_TIMEOUT_S, &b, 65536)) != OK) { free(a); return st; }
    char parent[128], top[128];
    line_value(a, "Parent UUID:", parent, sizeof(parent));
    line_value(b, "UUID:", top, sizeof(top));
    free(a);
    free(b);
    if (!parent[0] || strcmp(parent, top) != 0)
        return set_error(ERR_INVALID, "snap: %s is not a snapshot of %s (parent %s); left alone",
                         path, root, parent[0] ? parent : "none");
    const char *del[] = { c->tools->btrfs, "subvolume", "delete", path, NULL };
    return host_run(c->tools, 0, del, SNAP_TOOL_TIMEOUT_S, NULL, 0);
}

status_t snap_release_detail(const snap_ctx_t *c, const char *detail, const char *daemon8)
{
    if (strncmp(detail, "zfs:", 4) == 0) return zfs_destroy(c, detail + 4, daemon8);
    if (strncmp(detail, "btrfs:", 6) == 0) return btrfs_destroy(c, detail + 6, daemon8);
    return set_error(ERR_INVALID, "snap: no such snapshot kind in %s", detail);
}

status_t snap_release(const snap_ctx_t *c, snap_set_t *s, const char *daemon8,
                      const sp_holds_t *holds)
{
    status_t first = OK;
    char fmsg[512] = "";
    for (size_t i = 0; i < s->n; i++) {
        if (!s->taken[i]) continue;
        char detail[PATH_MAX + 96];
        if (s->doms[i].kind == SNAP_ZFS) {
            snprintf(detail, sizeof(detail), "zfs:%s@%s", s->doms[i].dataset, s->names[i]);
        } else {
            char dir[PATH_MAX];
            btrfs_snapdir(c, s->doms[i].root, dir, sizeof(dir));
            snprintf(detail, sizeof(detail), "btrfs:%s/%s", dir, s->names[i]);
        }
        status_t st = snap_release_detail(c, detail, daemon8);
        if (st == OK) {
            s->taken[i] = 0;
            unjournal(holds, &s->hold[i]);
        } else {
            if (first == OK) { first = st; snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg()); }
            log_warn("snap: releasing %s: %s; the sweep will retry", detail, err_msg());
            err_clear();
        }
    }
    if (first != OK) return set_error(first, "%s", fmsg);
    return OK;
}

static int is_live(int64_t seq, const int64_t *live, size_t n)
{
    for (size_t i = 0; i < n; i++) if (live[i] == seq) return 1;
    return 0;
}

status_t snap_sweep(const snap_ctx_t *c, const char *daemon8, const int64_t *live, size_t nlive,
                    unsigned *removed, unsigned *foreign)
{
    *removed = 0;
    *foreign = 0;
    if (c->pol->n_zfs) {
        const char *argv[] = { c->tools->zfs, "list", "-H", "-t", "snapshot", "-o", "name," OWNER_PROP, NULL };
        char *txt = NULL;
        if (host_run(c->tools, 0, argv, SNAP_TOOL_TIMEOUT_S, &txt, SNAP_LIST_CAP) == OK) {
            char *save = NULL;
            for (char *l = strtok_r(txt, "\n", &save); l; l = strtok_r(NULL, "\n", &save)) {
                char *tab = strchr(l, '\t');
                if (tab) *tab = '\0';
                char *at = strchr(l, '@');
                if (!at) continue;
                *at = '\0';
                int allowed = zfs_allowed(c->pol, l);
                *at = '@';
                int64_t seq = 0;
                if (!allowed || artefact_parse(at + 1, (char[9]){ 0 }, &seq, &(int64_t){ 0 }) != 0) continue;
                if (!ours(at + 1, daemon8, &seq) || !tab || strcmp(tab + 1, daemon8) != 0) {
                    (*foreign)++;
                    continue;
                }
                if (is_live(seq, live, nlive)) continue;
                if (zfs_destroy(c, l, daemon8) == OK) (*removed)++;
                else { log_warn("snap: sweep: %s", err_msg()); err_clear(); }
            }
            free(txt);
        } else {
            log_warn("snap: sweep: %s", err_msg());
            err_clear();
        }
    }
    for (size_t i = 0; i < c->pol->n_btrfs; i++) {
        char dir[PATH_MAX], hdir[PATH_MAX + HOST_TOOL_PATH_MAX];
        btrfs_snapdir(c, c->pol->btrfs_domains[i], dir, sizeof(dir));
        if (host_path(c->tools, dir, hdir, sizeof(hdir)) != 0) continue;
        DIR *d = opendir(hdir);
        if (!d) continue;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (de->d_name[0] == '.') continue;
            int64_t seq = 0;
            if (!ours(de->d_name, daemon8, &seq)) { (*foreign)++; continue; }
            if (is_live(seq, live, nlive)) continue;
            char p[PATH_MAX + 256];
            snprintf(p, sizeof(p), "%s/%s", dir, de->d_name);
            if (btrfs_destroy(c, p, daemon8) == OK) (*removed)++;
            else { log_warn("snap: sweep: %s", err_msg()); err_clear(); }
        }
        closedir(d);
    }
    return OK;
}

status_t snap_check_snapdir(const snap_ctx_t *c, const char *btrfs_root)
{
    if (!c->boot_config[0]) return OK;
    char dir[PATH_MAX];
    btrfs_snapdir(c, btrfs_root, dir, sizeof(dir));
    /* The share is the first component below the pool root. */
    const char *rest = dir + (strcmp(btrfs_root, "/") == 0 ? 0 : strlen(btrfs_root));
    if (*rest != '/') return set_error(ERR_INVALID, "snap: %s is not inside %s", dir, btrfs_root);
    rest++;
    size_t k = strcspn(rest, "/");
    if (k == 0 || rest[k] == '\0')
        return set_error(ERR_INVALID, "snap: the snapshot directory %s must be inside a share, "
                         "not at the top of the pool", dir);
    char cfg[PATH_MAX + 64], hcfg[PATH_MAX + HOST_TOOL_PATH_MAX + 64];
    snprintf(cfg, sizeof(cfg), "%s/shares/%.*s.cfg", c->boot_config, (int)k, rest);
    if (host_path(c->tools, cfg, hcfg, sizeof(hcfg)) != 0) return set_error(ERR_INVALID, "snap: path too long");
    FILE *f = fopen(hcfg, "re");
    if (!f) return OK;          /* not an Unraid share: nothing exports it */
    char line[512];
    status_t st = OK;
    while (st == OK && fgets(line, sizeof(line), f)) {
        const char *keys[] = { "shareExport=", "shareExportNFS=" };
        for (size_t i = 0; i < 2; i++) {
            if (strncmp(line, keys[i], strlen(keys[i])) != 0) continue;
            const char *v = line + strlen(keys[i]);
            if (*v == '"') v++;
            if (*v && *v != '-' && *v != '"' && *v != '\n')
                st = set_error(ERR_INVALID, "snap: share %.*s is exported (%.*s); a snapshot directory "
                               "there would be visible over the network, refused", (int)k, rest,
                               (int)strcspn(line, "\n"), line);
        }
    }
    fclose(f);
    return st;
}
