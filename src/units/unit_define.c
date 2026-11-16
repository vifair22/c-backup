#define _GNU_SOURCE

#include "unit_define.h"
#include "remote_provider.h"
#include "unit_restore.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define DEF_FILE_MAX ((size_t)16 * 1024 * 1024)
#define TREES_MAX    256

static int path_clean(const char *p)
{
    if (!p || p[0] != '/' || strcmp(p, "/") == 0 || strstr(p, "//") || strstr(p, "/./") || strstr(p, "/../"))
        return 0;
    size_t n = strlen(p);
    return p[n - 1] != '/' && !(n >= 3 && strcmp(p + n - 3, "/..") == 0) && !(n >= 2 && strcmp(p + n - 2, "/.") == 0);
}

int unit_define_check(const char *unit_id, const unit_define_opts_t *o, char *why, size_t wsz)
{
    int new_name = o->name && *o->name && strcmp(o->name, unit_id) != 0;
    if (!new_name && !o->replace) {
        snprintf(why, wsz, "restoring %s in place replaces its data and definition: pass replace "
                 "(it is stopped and its data moved aside, never deleted), or a new name with a "
                 "target_root", unit_id);
        return -1;
    }
    if (new_name && (!o->target_root || !*o->target_root)) {
        snprintf(why, wsz, "a restore under a new name needs target_root: its data goes under "
                 "target_root/<original path>, never onto the paths of %s", unit_id);
        return -1;
    }
    if (new_name && !path_clean(o->target_root)) {
        snprintf(why, wsz, "target_root must be a clean absolute path other than /");
        return -1;
    }
    return 0;
}

void unit_define_result_free(unit_define_result_t *r)
{
    if (!r) return;
    free(r->json);
    for (size_t i = 0; i < r->n_warnings; i++) free(r->warnings[i]);
    memset(r, 0, sizeof(*r));
}

typedef struct { char id[64]; char path[PATH_MAX]; char dest[PATH_MAX + 8]; } tree_t;

static status_t trees_of(cutils_db_t *db, int64_t repo_id, int64_t snap_id, tree_t *t, size_t *n)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    *n = 0;
    if (db_execute(db, "SELECT item_id, path FROM snapshot_unit_items WHERE repo_id = ? AND snap_id = ?"
                       " AND kind = 'tree' ORDER BY item_id",
                   (const char *[]){ repo, snap, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "unit restore: cannot list the snapshot's trees");
    }
    for (int i = 0; i < r->nrows && *n < TREES_MAX; i++) {
        snprintf(t[*n].id, sizeof(t[*n].id), "%s", r->rows[i][0]);
        snprintf(t[*n].path, sizeof(t[*n].path), "%s", r->rows[i][1]);
        (*n)++;
    }
    db_result_free(r);
    return OK;
}

static status_t mkdir_p(const char *path)
{
    char p[PATH_MAX + 8];
    snprintf(p, sizeof(p), "%s", path);
    for (char *s = p + 1; *s; s++) {
        if (*s != '/') continue;
        *s = '\0';
        if (mkdir(p, 0755) != 0 && errno != EEXIST) return set_error_errno(ERR_IO, "unit restore: mkdir %s", p);
        *s = '/';
    }
    return OK;
}

/* The tree whose recorded path is p, or holds it; NULL if none. */
static const tree_t *tree_over(const tree_t *t, size_t n, const char *p, size_t *plen)
{
    const tree_t *best = NULL;
    size_t bl = 0;
    for (size_t i = 0; i < n; i++) {
        size_t l = strlen(t[i].path);
        if (strncmp(p, t[i].path, l) == 0 && (p[l] == '\0' || p[l] == '/') && l > bl) { best = &t[i]; bl = l; }
    }
    if (plen) *plen = bl;
    return best;
}

/* src rewritten onto its tree's new home, into buf; 0 if it is in no tree. */
static int remap(const tree_t *t, size_t n, const char *src, char *buf, size_t sz)
{
    size_t l = 0;
    const tree_t *x = tree_over(t, n, src, &l);
    if (!x) return 0;
    snprintf(buf, sz, "%s%s", x->dest, src + l);
    return 1;
}

/* Docker: Binds ("src:dst[:opts]") and Mounts[].Source onto the new paths. */
static void remap_docker(cJSON *inspect, const tree_t *t, size_t n)
{
    cJSON *hc = cJSON_GetObjectItemCaseSensitive(inspect, "HostConfig");
    cJSON *b;
    cJSON_ArrayForEach(b, cJSON_GetObjectItemCaseSensitive(hc, "Binds")) {
        const char *v = cJSON_GetStringValue(b);
        if (!v || v[0] != '/') continue;
        size_t sl = strcspn(v, ":");
        char src[PATH_MAX], to[PATH_MAX * 2];
        snprintf(src, sizeof(src), "%.*s", (int)sl, v);
        if (!remap(t, n, src, to, sizeof(to))) continue;
        size_t need = strlen(to) + strlen(v + sl) + 1;
        char *nv = malloc(need);
        if (!nv) continue;
        snprintf(nv, need, "%s%s", to, v + sl);
        cJSON_SetValuestring(b, nv);
        free(nv);
    }
    cJSON *m;
    cJSON_ArrayForEach(m, cJSON_GetObjectItemCaseSensitive(hc, "Mounts")) {
        cJSON *s = cJSON_GetObjectItemCaseSensitive(m, "Source");
        char to[PATH_MAX * 2];
        if (cJSON_IsString(s) && remap(t, n, s->valuestring, to, sizeof(to))) cJSON_SetValuestring(s, to);
    }
}

static status_t sealed_memfd(const char *text, int *out)
{
    int fd = memfd_create("cbk-definition", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (fd < 0) return set_error_errno(ERR_IO, "unit restore: memfd");
    size_t n = strlen(text), done = 0;
    while (done < n) {
        ssize_t w = write(fd, text + done, n - done);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) { close(fd); return set_error_errno(ERR_IO, "unit restore: memfd"); }
        done += (size_t)w;
    }
    if (fcntl(fd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE | F_SEAL_SEAL) != 0) {
        close(fd);
        return set_error_errno(ERR_IO, "unit restore: sealing the definition");
    }
    *out = fd;
    return OK;
}

static cJSON *args_unit(const char *prov, const char *unit)
{
    cJSON *a = cJSON_CreateObject();
    char key[256];
    snprintf(key, sizeof(key), "%s:%s", prov, unit);
    if (a) cJSON_AddStringToObject(a, "unit", key);
    return a;
}

/* The provider's definition, from the snapshot's .cbackup-unit/config. */
static status_t build_definition(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                                 const char *prov, const char *unit, const unit_define_opts_t *o,
                                 int new_name, const tree_t *t, size_t n, cJSON **out)
{
    cJSON *d = cJSON_CreateObject();
    if (!d) return set_error(ERR_NOMEM, "unit restore: definition");
    status_t st = OK;
    char *data = NULL;
    size_t len = 0;
    char rel[PATH_MAX];
    if (strcmp(prov, "docker") == 0) {
        if (strncmp(unit, "compose:", 8) == 0) {
            cJSON_Delete(d);
            return set_error(ERR_INVALID, "unit restore: define of a compose project is not built");
        }
        snprintf(rel, sizeof(rel), ".cbackup-unit/config/containers/%s/inspect.json", unit);
        st = unit_read_file(store, db, repo_id, snap_id, rel, DEF_FILE_MAX, &data, &len);
        cJSON *in = st == OK ? cJSON_ParseWithLength(data, len) : NULL;
        free(data);
        data = NULL;
        if (st == OK && !in) st = set_error(ERR_CORRUPT, "unit restore: %s is not JSON", rel);
        if (st == OK) {
            if (new_name) remap_docker(in, t, n);
            cJSON_AddItemToObject(d, "inspect", in);
            snprintf(rel, sizeof(rel), ".cbackup-unit/config/containers/%s/image.json", unit);
            if (unit_read_file(store, db, repo_id, snap_id, rel, DEF_FILE_MAX, &data, &len) == OK) {
                cJSON *im = cJSON_ParseWithLength(data, len);
                if (im) cJSON_AddItemToObject(d, "image", im);
            } else {
                err_clear();
            }
            free(data);
            data = NULL;
            snprintf(rel, sizeof(rel), ".cbackup-unit/config/containers/%s/unraid-template.xml", unit);
            if (unit_read_file(store, db, repo_id, snap_id, rel, DEF_FILE_MAX, &data, &len) == OK)
                cJSON_AddStringToObject(d, "template", data);
            else
                err_clear();
            free(data);
            data = NULL;
        }
    } else if (strcmp(prov, "lxc") == 0) {
        st = unit_read_file(store, db, repo_id, snap_id, ".cbackup-unit/config/lxc/config", DEF_FILE_MAX, &data, &len);
        if (st == OK) cJSON_AddStringToObject(d, "config", data);
        free(data);
        if (st == OK && new_name) {
            cJSON *mm = cJSON_AddObjectToObject(d, "mount_map");
            for (size_t i = 0; i < n; i++) {
                if (strcmp(t[i].id, "rootfs") == 0) cJSON_AddStringToObject(d, "rootfs", t[i].dest);
                else if (mm) cJSON_AddStringToObject(mm, t[i].path, t[i].dest);
            }
        }
    } else {
        /* Any other provider: the trees' new homes, for it to apply. */
        cJSON *mm = cJSON_AddObjectToObject(d, "mount_map");
        for (size_t i = 0; mm && new_name && i < n; i++) cJSON_AddStringToObject(mm, t[i].path, t[i].dest);
    }
    if (st == OK) {
        cJSON_AddStringToObject(d, "name", new_name ? o->name : "");
        cJSON_AddBoolToObject(d, "replace", !new_name && o->replace);
        cJSON_AddBoolToObject(d, "pin_digest", o->pin_digest);
        cJSON_AddBoolToObject(d, "start", o->start);
        cJSON_AddBoolToObject(d, "keep_macs", o->keep_macs);
        *out = d;
        return OK;
    }
    cJSON_Delete(d);
    return st;
}

status_t unit_define_run(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                         int64_t job_id, const unit_define_opts_t *o, unit_define_result_t *out)
{
    memset(out, 0, sizeof(*out));
    char key[256];
    status_t st = unit_snapshot_key(db, repo_id, snap_id, key, sizeof(key));
    if (st != OK) return st;
    char prov[32];
    const char *colon = strchr(key, ':');
    if (!colon || (size_t)(colon - key) >= sizeof(prov)) return set_error(ERR_CORRUPT, "unit restore: bad unit key %s", key);
    snprintf(prov, sizeof(prov), "%.*s", (int)(colon - key), key);
    const char *unit = colon + 1;
    char why[512];
    if (unit_define_check(unit, o, why, sizeof(why)) != 0) return set_error(ERR_INVALID, "%s", why);
    int new_name = o->name && *o->name && strcmp(o->name, unit) != 0;

    tree_t *t = calloc(TREES_MAX, sizeof(*t));
    if (!t) return set_error(ERR_NOMEM, "unit restore");
    size_t n = 0;
    st = trees_of(db, repo_id, snap_id, t, &n);
    cJSON *res = cJSON_CreateObject();
    cJSON *done = res ? cJSON_AddArrayToObject(res, "trees") : NULL;
    if (st == OK && !done) st = set_error(ERR_NOMEM, "unit restore");
    if (res) {
        cJSON_AddStringToObject(res, "mode", "unit");
        cJSON_AddStringToObject(res, "unit", key);
        cJSON_AddStringToObject(res, "name", new_name ? o->name : unit);
    }

    if (st == OK && new_name) {
        /* Under target_root only; refuse rather than merge into what is there. */
        for (size_t i = 0; st == OK && i < n; i++) {
            snprintf(t[i].dest, sizeof(t[i].dest), "%s%s", o->target_root, t[i].path);
            struct stat sb;
            if (lstat(t[i].dest, &sb) == 0) st = set_error(ERR_INVALID, "unit restore: %s exists; the target root must not hold the unit's paths yet", t[i].dest);
        }
        for (size_t i = 0; st == OK && i < n; i++) {
            if ((st = mkdir_p(t[i].dest)) != OK) break;
            /* The tree lands at target_root/<its path>. */
            restore_opts_t ro = o->ropts;
            ro.subpath = t[i].path;
            restore_result_t rr;
            memset(&rr, 0, sizeof(rr));
            st = restore_run(store, db, repo_id, snap_id, o->target_root, &ro, &rr);
            cJSON *x = cJSON_CreateObject();
            if (x) {
                cJSON_AddStringToObject(x, "path", t[i].path);
                cJSON_AddStringToObject(x, "restored_to", t[i].dest);
                cJSON_AddItemToArray(done, x);
            }
        }
        if (res) cJSON_AddStringToObject(res, "target_root", o->target_root);
    } else if (st == OK) {
        /* In place: stopped, staged in its own domain, swapped in, the old
         * data kept beside it. */
        st = remote_op(prov, "unit.stop", args_unit(prov, unit), -1, NULL);
        for (size_t i = 0; st == OK && i < n; i++) {
            cJSON *a = args_unit(prov, unit), *sr = NULL;
            if (a) { cJSON_AddStringToObject(a, "mount", t[i].path); cJSON_AddNumberToObject(a, "job", (double)job_id); }
            st = remote_op(prov, "restore.stage", a, -1, &sr);
            const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sr, "staging"));
            const char *sp = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sr, "path"));
            if (st == OK && (!tok || !sp || sp[0] != '/')) st = set_error(ERR_IO, "unit restore: a malformed staging for %s", t[i].path);
            if (st == OK) {
                restore_opts_t ro = o->ropts;
                ro.subpath = t[i].path;
                ro.strip_subpath = 1;          /* the staging dir is the new mount */
                restore_result_t rr;
                memset(&rr, 0, sizeof(rr));
                st = restore_run(store, db, repo_id, snap_id, sp, &ro, &rr);
            }
            cJSON *sw = NULL;
            if (st == OK) {
                cJSON *b = cJSON_CreateObject();
                if (b) cJSON_AddStringToObject(b, "staging", tok);
                st = remote_op(prov, "restore.swap", b, -1, &sw);
            }
            if (st == OK) {
                cJSON *x = cJSON_CreateObject();
                const char *prev = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sw, "previous"));
                if (x) {
                    cJSON_AddStringToObject(x, "path", t[i].path);
                    if (prev) cJSON_AddStringToObject(x, "previous", prev);
                    cJSON_AddItemToArray(done, x);
                }
            }
            cJSON_Delete(sr);
            cJSON_Delete(sw);
        }
    }

    /* Then the definition, by memfd. */
    cJSON *def = NULL;
    if (st == OK) st = build_definition(store, db, repo_id, snap_id, prov, unit, o, new_name, t, n, &def);
    char *txt = def ? cJSON_PrintUnformatted(def) : NULL;
    cJSON_Delete(def);
    if (st == OK && !txt) st = set_error(ERR_NOMEM, "unit restore: definition");
    int fd = -1;
    if (st == OK) st = sealed_memfd(txt, &fd);
    free(txt);
    cJSON *dr = NULL;
    if (st == OK) {
        cJSON *a = args_unit(prov, unit);
        cJSON *ph = a ? cJSON_AddObjectToObject(a, "definition") : NULL;
        if (ph) cJSON_AddBoolToObject(ph, "memfd", 1);
        st = remote_op(prov, "unit.define", a, fd, &dr);
    }
    if (fd >= 0) close(fd);
    if (st == OK) {
        const cJSON *w;
        cJSON_ArrayForEach(w, cJSON_GetObjectItemCaseSensitive(dr, "warnings")) {
            if (!cJSON_IsString(w) || out->n_warnings >= sizeof(out->warnings) / sizeof(out->warnings[0])) continue;
            out->warnings[out->n_warnings] = strdup(w->valuestring);
            if (out->warnings[out->n_warnings]) out->n_warnings++;
            log_warn("unit restore %s: %s", key, w->valuestring);
        }
        if (res && dr) { cJSON_AddItemToObject(res, "define", dr); dr = NULL; }
        out->json = res ? cJSON_PrintUnformatted(res) : NULL;
    }
    cJSON_Delete(dr);
    cJSON_Delete(res);
    free(t);
    return st;
}
