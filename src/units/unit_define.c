#define _GNU_SOURCE

#include "unit_define.h"
#include "msg_clip.h"
#include "remote_provider.h"
#include "unit_restore.h"
#include "snap_record.h"

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

const char *unit_define_name(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                             const char *unit_id, const char *name)
{
    if (!name || !*name || strcmp(name, unit_id) == 0) return NULL;
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    int same = db_execute(db, "SELECT unit_name FROM snapshot_units WHERE repo_id = ? AND snap_id = ?",
                          (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1 &&
               strcmp(r->rows[0][0], name) == 0;
    db_result_free(r);
    return same ? NULL : name;
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
    /*
     * Volumes, named and anonymous (B188). A volume is named in Binds
     * ("name:/dst") or HostConfig.Mounts, or not at all (an anonymous one,
     * from the image's VOLUME), and the copy re-created from that kept the
     * original's volume or got an empty new one. Its data was backed up
     * from where the volume lives (the inspect's Mounts[].Source), which
     * the restore wrote under target_root: the copy binds that instead.
     * A volume with no restored copy (a remote driver's, excluded) is left
     * as recorded, and the helper refuses the define over it.
     */
    if (!cJSON_IsObject(hc)) return;
    cJSON *binds = cJSON_GetObjectItemCaseSensitive(hc, "Binds");
    if (!cJSON_IsArray(binds)) {
        cJSON_DeleteItemFromObjectCaseSensitive(hc, "Binds");
        binds = cJSON_AddArrayToObject(hc, "Binds");
        if (!binds) return;
    }
    cJSON_ArrayForEach(m, cJSON_GetObjectItemCaseSensitive(inspect, "Mounts")) {
        const char *type = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "Type"));
        const char *name = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "Name"));
        const char *src = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "Source"));
        const char *dst = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(m, "Destination"));
        char to[PATH_MAX * 2];
        if (!type || strcmp(type, "volume") != 0 || !src || !dst || !remap(t, n, src, to, sizeof(to))) continue;
        /* Whatever named this volume or its destination goes. */
        for (int i = cJSON_GetArraySize(binds); i-- > 0;) {
            const char *v = cJSON_GetStringValue(cJSON_GetArrayItem(binds, i));
            if (!v) continue;
            size_t sl = strcspn(v, ":");
            const char *d = v[sl] ? v + sl + 1 : "";
            size_t dl = strcspn(d, ":");
            if ((name && strlen(name) == sl && strncmp(v, name, sl) == 0) || (strlen(dst) == dl && strncmp(d, dst, dl) == 0))
                cJSON_DeleteItemFromArray(binds, i);
        }
        cJSON *hm = cJSON_GetObjectItemCaseSensitive(hc, "Mounts");
        for (int i = cJSON_GetArraySize(hm); i-- > 0;) {
            const cJSON *x = cJSON_GetArrayItem(hm, i);
            const char *xt = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(x, "Target"));
            const char *xs = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(x, "Source"));
            if ((xt && strcmp(xt, dst) == 0) || (xs && name && strcmp(xs, name) == 0)) cJSON_DeleteItemFromArray(hm, i);
        }
        int ro = cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(m, "RW"));
        size_t need = strlen(to) + strlen(dst) + 6;
        char *nv = malloc(need);
        if (!nv) continue;
        snprintf(nv, need, "%s:%s%s", to, dst, ro ? ":ro" : "");
        cJSON_AddItemToArray(binds, cJSON_CreateString(nv));
        free(nv);
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

/* A compose project's definition: its files and .env, and per member its
 * service, image record and (under a new name) its binds remapped. */
static status_t compose_definition(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                                   int new_name, const tree_t *t, size_t n, cJSON *d)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path FROM snapshot_unit_items WHERE repo_id = ? AND snap_id = ? AND kind = 'blob'"
                       " ORDER BY path", (const char *[]){ repo, snap, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "unit restore: cannot list the snapshot's configuration");
    }
    cJSON *cp = cJSON_AddObjectToObject(d, "compose");
    cJSON *files = cp ? cJSON_AddObjectToObject(cp, "files") : NULL;
    cJSON *svcs = cp ? cJSON_AddArrayToObject(cp, "services") : NULL;
    status_t st = svcs ? OK : set_error(ERR_NOMEM, "unit restore: definition");
    static const char cdir[] = ".cbackup-unit/config/compose/";
    static const char mdir[] = ".cbackup-unit/config/containers/";
    for (int i = 0; st == OK && i < r->nrows; i++) {
        const char *path = r->rows[i][0];
        char *data = NULL;
        size_t len = 0;
        if (strncmp(path, cdir, sizeof(cdir) - 1) == 0) {
            const char *base = path + sizeof(cdir) - 1;
            if ((st = unit_read_file(store, db, repo_id, snap_id, path, DEF_FILE_MAX, &data, &len)) != OK) break;
            if (strcmp(base, ".env") == 0) cJSON_AddStringToObject(cp, "env", data);
            else cJSON_AddStringToObject(files, base, data);
            free(data);
            continue;
        }
        size_t pl = strlen(path);
        if (strncmp(path, mdir, sizeof(mdir) - 1) != 0 || pl < 13 || strcmp(path + pl - 13, "/inspect.json") != 0) continue;
        if ((st = unit_read_file(store, db, repo_id, snap_id, path, DEF_FILE_MAX, &data, &len)) != OK) break;
        cJSON *in = cJSON_ParseWithLength(data, len);
        free(data);
        const cJSON *labels = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(in, "Config"), "Labels");
        const char *svc = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(labels, "com.docker.compose.service"));
        const char *wd = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(labels, "com.docker.compose.project.working_dir"));
        if (!svc) { cJSON_Delete(in); continue; }
        if (wd && !cJSON_GetObjectItemCaseSensitive(cp, "working_dir")) cJSON_AddStringToObject(cp, "working_dir", wd);
        cJSON *o = cJSON_CreateObject();
        if (!o) { cJSON_Delete(in); st = set_error(ERR_NOMEM, "unit restore"); break; }
        cJSON_AddItemToArray(svcs, o);
        cJSON_AddStringToObject(o, "service", svc);
        char ip[PATH_MAX];
        snprintf(ip, sizeof(ip), "%.*simage.json", (int)(pl - 12), path);
        if (unit_read_file(store, db, repo_id, snap_id, ip, DEF_FILE_MAX, &data, &len) == OK) {
            cJSON *im = cJSON_ParseWithLength(data, len);
            if (im) cJSON_AddItemToObject(o, "image", im);
            free(data);
        } else {
            err_clear();
        }
        /* Every mount the service had, bind or volume, named or anonymous,
         * onto its restored copy (B188). These come from the recorded
         * Mounts, not HostConfig.Binds: compose gives long-syntax binds
         * and every volume through HostConfig.Mounts, and the copy kept
         * those on the original's paths and volumes. One with no restored
         * copy is left out, and the helper refuses the define over it. */
        cJSON *vols = cJSON_AddArrayToObject(o, "volumes");
        const cJSON *mt;
        cJSON_ArrayForEach(mt, cJSON_GetObjectItemCaseSensitive(in, "Mounts")) {
            const char *type = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(mt, "Type"));
            const char *src = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(mt, "Source"));
            const char *dst = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(mt, "Destination"));
            char to[PATH_MAX * 2], full[PATH_MAX * 3];
            if (!new_name || !vols || !type || (strcmp(type, "volume") != 0 && strcmp(type, "bind") != 0) ||
                !src || !dst || !remap(t, n, src, to, sizeof(to))) continue;
            snprintf(full, sizeof(full), "%s:%s%s", to, dst,
                     cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(mt, "RW")) ? ":ro" : "");
            cJSON_AddItemToArray(vols, cJSON_CreateString(full));
        }
        const cJSON *rec = cJSON_GetObjectItemCaseSensitive(in, "Mounts");
        if (new_name && cJSON_IsArray(rec)) cJSON_AddItemToObject(o, "mounts", cJSON_Duplicate(rec, 1));
        cJSON_Delete(in);
    }
    db_result_free(r);
    return st;
}

const char *unit_recorded_type_name(int found, uint8_t type)
{
    if (!found) return "missing";
    switch (type) {
    case NODE_TYPE_REG:      return "reg";
    case NODE_TYPE_DIR:      return "dir";
    case NODE_TYPE_SYMLINK:  return "lnk";
    case NODE_TYPE_HARDLINK: return "link";
    case NODE_TYPE_FIFO:     return "fifo";
    case NODE_TYPE_CHR:      return "chr";
    case NODE_TYPE_BLK:      return "blk";
    /* Type 0 is how a scan records what it cannot classify, a socket in
     * practice (restore.c skips it as one); the entry is there, so not
     * "missing" (NIT-44). */
    case 0:                  return "sock";
    default:                 return "other";
    }
}

/*
 * A copy's bind source under target_root, as the snapshot records it, into
 * rec (B210): "blk", "chr", "dir", "reg", "lnk", "link", "fifo", "sock"
 * (unclassified, a socket in practice), "other" (a type this build does
 * not know) or "missing". The helper's define check runs before the restore writes
 * target_root, so the source does not exist yet and only the original can
 * say what it is; with the original gone from the host, this is what the
 * helper has left. Reading the manifest changes nothing in the format.
 */
static void record_one(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                       const char *troot, const char *src, cJSON *rec)
{
    size_t tl = strlen(troot);
    if (!src || strncmp(src, troot, tl) != 0 || src[tl] != '/' || cJSON_GetObjectItemCaseSensitive(rec, src)) return;
    uint8_t type = 0;
    int found = unit_entry_type(store, db, repo_id, snap_id, src + tl, &type) == OK;
    if (!found) err_clear();
    const char *name = unit_recorded_type_name(found, type);
    cJSON_AddStringToObject(rec, src, name);
}

static void bind_src_record(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                            const char *troot, const char *bind, cJSON *rec)
{
    if (!bind || bind[0] != '/') return;
    char src[PATH_MAX];
    snprintf(src, sizeof(src), "%.*s", (int)strcspn(bind, ":"), bind);
    record_one(store, db, repo_id, snap_id, troot, src, rec);
}

static void add_recorded_types(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                               const char *troot, cJSON *d)
{
    cJSON *rec = cJSON_AddObjectToObject(d, "recorded_types");
    if (!rec) return;
    const cJSON *x, *y;
    const cJSON *hc = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(d, "inspect"), "HostConfig");
    cJSON_ArrayForEach(x, cJSON_GetObjectItemCaseSensitive(hc, "Binds"))
        bind_src_record(store, db, repo_id, snap_id, troot, cJSON_GetStringValue(x), rec);
    cJSON_ArrayForEach(x, cJSON_GetObjectItemCaseSensitive(hc, "Mounts"))
        record_one(store, db, repo_id, snap_id, troot,
                   cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(x, "Source")), rec);
    cJSON_ArrayForEach(x, cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(d, "compose"), "services"))
        cJSON_ArrayForEach(y, cJSON_GetObjectItemCaseSensitive(x, "volumes"))
            bind_src_record(store, db, repo_id, snap_id, troot, cJSON_GetStringValue(y), rec);
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
            st = compose_definition(store, db, repo_id, snap_id, new_name, t, n, d);
            if (st == OK && new_name) cJSON_AddStringToObject(d, "target_root", o->target_root);
            goto common;
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
            if (new_name) cJSON_AddStringToObject(d, "target_root", o->target_root);
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
common:
    if (st == OK && new_name && strcmp(prov, "docker") == 0 && o->target_root)
        add_recorded_types(store, db, repo_id, snap_id, o->target_root, d);
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

status_t unit_define_build(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           const char *prov, const char *unit, const unit_define_opts_t *o, char **text)
{
    *text = NULL;
    int new_name = o->name && *o->name && strcmp(o->name, unit) != 0;
    tree_t *t = calloc(TREES_MAX, sizeof(*t));
    if (!t) return set_error(ERR_NOMEM, "unit restore");
    size_t n = 0;
    status_t st = trees_of(db, repo_id, snap_id, t, &n);
    for (size_t i = 0; st == OK && new_name && i < n; i++)
        snprintf(t[i].dest, sizeof(t[i].dest), "%s%s", o->target_root ? o->target_root : "", t[i].path);
    cJSON *def = NULL;
    if (st == OK) st = build_definition(store, db, repo_id, snap_id, prov, unit, o, new_name, t, n, &def);
    if (st == OK && !(*text = cJSON_PrintUnformatted(def))) st = set_error(ERR_NOMEM, "unit restore");
    cJSON_Delete(def);
    free(t);
    return st;
}

/*
 * Byte progress over every restore_run of a unit restore. Each run reports
 * its own done and total, so the job's bytes_total was one mount's, and
 * its done went back to 0 at each. A run that starts (its total changes, or
 * its done goes back) has the one before it folded into a base: done only
 * grows, and the total is every mount so far plus the current one.
 */
typedef struct {
    restore_opts_t orig;
    uint64_t base_done, base_total, cur_done, cur_total;
} bytes_agg_t;

static void agg_progress(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    const bytes_agg_t *a = ctx;
    if (a->orig.progress_cb) a->orig.progress_cb(phase, done, total, a->orig.progress_ctx);
}

static void agg_bytes(void *ctx, uint64_t done, uint64_t total)
{
    bytes_agg_t *a = ctx;
    if (total != a->cur_total || done < a->cur_done) {
        a->base_done += a->cur_done;
        a->base_total += a->cur_total;
    }
    a->cur_done = done;
    a->cur_total = total;
    if (a->orig.bytes_cb) a->orig.bytes_cb(a->orig.progress_ctx, a->base_done + done, a->base_total + total);
}

/* One file of the snapshot (a disk image, the NVRAM) restored to dest. */
static status_t restore_file(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                             const unit_define_opts_t *o, const char *rel, const char *dest)
{
    /* restore_run writes into a directory: a fresh one beside dest, the
     * file moved out of it to dest (one filesystem), the directory gone. */
    char tmp[PATH_MAX + 32], at[2 * PATH_MAX + 64];
    snprintf(tmp, sizeof(tmp), "%.4000s.cbk-restore", dest);
    snprintf(at, sizeof(at), "%s/%s", tmp, rel[0] == '/' ? rel + 1 : rel);
    restore_opts_t ro = o->ropts;
    ro.subpath = rel;
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    status_t st = restore_run(store, db, repo_id, snap_id, tmp, &ro, &rr);
    if (st == OK && rr.files == 0 && rr.dirs == 0) st = set_error(ERR_NOT_FOUND, "unit restore: %s is not in the snapshot", rel);
    if (st == OK && rename(at, dest) != 0) st = set_error_errno(ERR_IO, "unit restore: moving %s into place", rel);
    /* The restore synced the file; the move out of tmp is durable once the
     * directory it landed in is. */
    if (st == OK && snapshot_fsync_parent(dest) != 0)
        st = set_error_errno(ERR_IO, "unit restore: sync the directory of %s", dest);
    /* Empty directories of the path inside tmp, deepest first. */
    for (char *p2 = strrchr(at, '/'); p2 && p2 > at + strlen(tmp); p2 = strrchr(at, '/')) {
        *p2 = '\0';
        if (rmdir(at) != 0) break;
    }
    (void)rmdir(tmp);
    return st;
}

/* A path of the helper's answer, checked. */
static const char *abs_of(const cJSON *o, const char *k)
{
    const char *v = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, k));
    return v && v[0] == '/' ? v : NULL;
}

/* Where the backup read tree id (unit.json's source), or NULL. */
static const char *tree_source(const cJSON *uj, const char *id)
{
    const cJSON *it;
    cJSON_ArrayForEach(it, cJSON_GetObjectItemCaseSensitive(uj, "items")) {
        const char *iid = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "id"));
        const char *kd = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "kind"));
        if (iid && kd && strcmp(kd, "tree") == 0 && strcmp(iid, id) == 0) return abs_of(it, "source");
    }
    return NULL;
}

/* unit.define with the definition txt by sealed memfd; check asks for the
 * refusals only, and defines nothing. */
static status_t define_send(const char *prov, const char *unit, const char *txt, int check, cJSON **res)
{
    *res = NULL;
    int fd = -1;
    status_t st = sealed_memfd(txt, &fd);
    if (st == OK) {
        cJSON *a = args_unit(prov, unit);
        cJSON *ph = a ? cJSON_AddObjectToObject(a, "definition") : NULL;
        if (ph) cJSON_AddBoolToObject(ph, "memfd", 1);
        if (ph && check) cJSON_AddBoolToObject(ph, "check", 1);
        st = remote_op(prov, "unit.define", a, fd, res);
    }
    if (fd >= 0) close(fd);
    return st;
}

typedef struct {
    int allow_restore;         /* the helper's policy lets it stage, swap, stop and start */
    int state;                 /* sp_state_t, or -1 when the helper cannot tell */
} define_check_t;

/*
 * Everything that can refuse the define, asked before anything is written
 * (B167). The define used to be the last step, so a policy without
 * allow_define (or a provider not behind the helper) refused it only after
 * the unit had been stopped and its data swapped: the restore failed with
 * the unit down and the new data in place. The helper runs every check of
 * a real unit.define and stops short of defining.
 */
static status_t define_check(const char *prov, const char *unit, const char *txt, define_check_t *out)
{
    out->allow_restore = 0;
    out->state = -1;
    cJSON *cr = NULL;
    status_t st = define_send(prov, unit, txt, 1, &cr);
    if (st == OK && !cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cr, "checked")))
        st = set_error(ERR_IO, "the helper answered a define check with a define");
    if (st == OK) {
        const cJSON *sv = cJSON_GetObjectItemCaseSensitive(cr, "state");
        out->allow_restore = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cr, "allow_restore"));
        out->state = cJSON_IsNumber(sv) ? (int)sv->valuedouble : -1;
    }
    cJSON_Delete(cr);
    if (st != OK) {
        char why[MSG_CLIP_MAX];
        msg_clip(why, sizeof(why), err_msg());
        return set_error(st, "unit restore refused, nothing written: %s", why);
    }
    return OK;
}

/*
 * A failure after the unit was stopped or its data swapped (B167): each
 * swap undone, newest first (the restored copy back to its staging
 * directory, the data moved aside back to the mount), then the unit started
 * again if it was running. st and the current error are the failure; the
 * error returned says that and what the rollback did. A swap that cannot be
 * undone leaves the unit stopped, since its data is no longer whole.
 */
static status_t roll_back(const char *prov, const char *unit, char (*tok)[64], size_t n,
                          int restart, status_t st)
{
    char why[MSG_CLIP_MAX], fail[1024] = "", copy[300] = "";
    msg_clip(why, sizeof(why), err_msg());
    err_clear();
    size_t undone = 0;
    for (size_t i = n; i-- > 0;) {
        cJSON *b = cJSON_CreateObject(), *ur = NULL;
        if (b) {
            cJSON_AddStringToObject(b, "staging", tok[i]);
            cJSON_AddBoolToObject(b, "undo", 1);
        }
        if (remote_op(prov, "restore.swap", b, -1, &ur) == OK) {
            const char *c = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(ur, "restored_copy"));
            if (c && !copy[0]) snprintf(copy, sizeof(copy), "%.290s", c);
            undone++;
            cJSON_Delete(ur);
        } else {
            if (!fail[0]) msg_clip(fail, sizeof(fail), err_msg());
            err_clear();
        }
    }
    int restarted = 0;
    if (restart && !fail[0]) {
        if (remote_op(prov, "unit.start", args_unit(prov, unit), -1, NULL) == OK) restarted = 1;
        else {
            msg_clip_after(fail, sizeof(fail), snprintf(fail, sizeof(fail), "starting it again: %s", err_msg()));
            err_clear();
        }
    }
    if (fail[0]) {
        log_error("unit restore %s:%s: rollback incomplete (%zu of %zu swaps undone): %s", prov, unit,
                  undone, n, fail);
        return set_error(st, "%s; the rollback did not finish (%zu of %zu swaps undone): %s; a mount "
                         "not put back has its original data at <mount>.cbackup-old-<job>", why, undone, n, fail);
    }
    log_warn("unit restore %s:%s failed and was rolled back (%zu swaps undone%s)", prov, unit, n,
             restarted ? ", unit started again" : "");
    char kept[400] = "";
    if (copy[0])
        snprintf(kept, sizeof(kept), " (the restored copy is kept at %s%s, for a look or to delete)", copy,
                 n > 1 ? " and beside it" : "");
    return set_error(st, "%s; rolled back: %s%s%s", why,
                     n ? "the original data is back in place" : "nothing had been replaced",
                     restarted ? ", and the unit is running again" : "", kept);
}

/* A new-name restore that failed after writing under target_root: what it
 * wrote stays, and a retry is refused until it is gone. Say so. */
static status_t left_under(const char *target_root, status_t st)
{
    char why[MSG_CLIP_MAX];
    msg_clip(why, sizeof(why), err_msg());
    return set_error(st, "%s; what was restored under %s stays there: remove it, or give another "
                     "target_root, to try again", why, target_root);
}

#define VM_DISKS_MAX 64

/*
 * A VM (vm-backup.md 7.1, 7.2): every disk restored to a sparse raw image
 * (under target_root at its original path for a new domain; staged beside
 * the original and swapped in, the old image moved aside, for replace),
 * NVRAM and TPM state staged, the recorded XML defined by the helper with
 * the disks pointing at their images, then vm.install_state for the UUID
 * the define returned, and a start only on request.
 *
 * Every disk's destination and the definition are settled first, and the
 * helper checks the define before any disk is written (B167). A failure
 * from the first swap through the define undoes the swaps; once the define
 * has succeeded the domain and its disks agree, and nothing is undone.
 */
static status_t vm_define_run(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                              int64_t job_id, const unit_define_opts_t *o, const char *unit,
                              int new_name, cJSON *res, unit_define_result_t *out)
{
    char *uj = NULL, *xml = NULL;
    size_t ul = 0, xl = 0;
    status_t st = unit_read_file(store, db, repo_id, snap_id, ".cbackup-unit/unit.json", DEF_FILE_MAX, &uj, &ul);
    if (st == OK) st = unit_read_file(store, db, repo_id, snap_id, ".cbackup-unit/config/domain.xml", DEF_FILE_MAX, &xml, &xl);
    cJSON *u = st == OK ? cJSON_ParseWithLength(uj, ul) : NULL;
    free(uj);
    if (st == OK && !u) st = set_error(ERR_CORRUPT, "unit restore: unit.json does not parse");
    cJSON *disks = cJSON_CreateObject(), *done = cJSON_GetObjectItemCaseSensitive(res, "trees");
    if (st == OK && !disks) st = set_error(ERR_NOMEM, "unit restore");
    const char *nvram = NULL, *tpm = NULL;
    /* Per disk: its id, its recorded source, its path in the snapshot, and
     * where it lands. Plain arrays: cppcheck 2.17 (Debian trixie) reads an
     * snprintf into a member of an array-of-struct element as a use of
     * uninitialised memory. */
    const char *did[VM_DISKS_MAX], *dsrc[VM_DISKS_MAX], *drel[VM_DISKS_MAX];
    char (*ddest)[PATH_MAX + 8] = calloc(VM_DISKS_MAX, sizeof(*ddest));
    char (*tok)[64] = calloc(VM_DISKS_MAX, sizeof(*tok));
    if (st == OK && (!ddest || !tok)) st = set_error(ERR_NOMEM, "unit restore");
    size_t nd = 0;
    const cJSON *it;
    cJSON_ArrayForEach(it, cJSON_GetObjectItemCaseSensitive(u, "items")) {
        const char *kind = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "kind"));
        const char *id = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "id"));
        if (kind && id && strcmp(id, "nvram") == 0) nvram = abs_of(it, "source");
        if (kind && id && strcmp(kind, "tree") == 0 && strcmp(id, "tpm") == 0) tpm = abs_of(it, "path");
        if (st != OK || !kind || strcmp(kind, "block") != 0 || !id) continue;
        const char *src = abs_of(it, "source");
        const char *rel = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, "path"));
        if (!src || !rel) { st = set_error(ERR_CORRUPT, "unit restore: disk %s has no source in unit.json", id); break; }
        if (nd >= VM_DISKS_MAX) { st = set_error(ERR_INVALID, "unit restore: more than %d disks", VM_DISKS_MAX); break; }
        did[nd] = id;
        dsrc[nd] = src;
        drel[nd] = rel;
        snprintf(ddest[nd], sizeof(ddest[nd]), "%s%s", new_name ? o->target_root : "", src);
        struct stat sb;
        if (new_name && lstat(ddest[nd], &sb) == 0) { st = set_error(ERR_INVALID, "unit restore: %s exists", ddest[nd]); break; }
        cJSON_AddStringToObject(disks, id, ddest[nd]);
        nd++;
    }
    /* The definition, settled before anything is written. */
    char *txt = NULL;
    if (st == OK) {
        cJSON *d = cJSON_CreateObject();
        if (d) {
            cJSON_AddStringToObject(d, "xml", xml);
            cJSON_AddStringToObject(d, "name", new_name ? o->name : "");
            cJSON_AddBoolToObject(d, "replace", !new_name && o->replace);
            cJSON_AddBoolToObject(d, "keep_macs", o->keep_macs);
            cJSON_AddItemToObject(d, "disks", cJSON_Duplicate(disks, 1));
        }
        txt = d ? cJSON_PrintUnformatted(d) : NULL;
        cJSON_Delete(d);
        if (!txt) st = set_error(ERR_NOMEM, "unit restore");
    }
    define_check_t dc;
    if (st == OK) st = define_check("libvirt", unit, txt, &dc);
    if (st == OK && (!new_name || nvram || tpm) && !dc.allow_restore)
        st = set_error(ERR_INVALID, "unit restore refused, nothing written: the helper's policy does not "
                       "allow restore (allow_restore), which %s needs", !new_name ? "an in-place restore"
                       : "staging the VM's NVRAM and TPM state");
    if (st == OK && !new_name && (dc.state == SP_STATE_RUNNING || dc.state == SP_STATE_PAUSED))
        st = set_error(ERR_BUSY, "unit restore refused, nothing written: %s is running; shut it down "
                       "before restoring it in place", unit);

    /* The disks. */
    size_t nstaged = 0, nswapped = 0;
    int began = st == OK && nd > 0;
    for (size_t i = 0; st == OK && i < nd; i++) {
        if (new_name) {
            if ((st = mkdir_p(ddest[i])) == OK) st = restore_file(store, db, repo_id, snap_id, o, drel[i], ddest[i]);
        } else {
            cJSON *a = args_unit("libvirt", unit), *sr = NULL;
            if (a) { cJSON_AddStringToObject(a, "mount", dsrc[i]); cJSON_AddNumberToObject(a, "job", (double)job_id); }
            st = remote_op("libvirt", "restore.stage", a, -1, &sr);
            const char *stok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sr, "staging"));
            const char *sp = abs_of(sr, "path");
            if (st == OK && (!stok || !sp)) st = set_error(ERR_IO, "unit restore: a malformed staging for %s", dsrc[i]);
            if (st == OK) {
                char dest[PATH_MAX + 8];
                snprintf(dest, sizeof(dest), "%.4000s/image", sp);
                st = restore_file(store, db, repo_id, snap_id, o, drel[i], dest);
            }
            if (st == OK) snprintf(tok[nstaged++], sizeof(tok[0]), "%s", stok);
            cJSON_Delete(sr);
        }
        cJSON *x = cJSON_CreateObject();
        if (x && st == OK) {
            cJSON_AddStringToObject(x, "disk", did[i]);
            cJSON_AddStringToObject(x, "image", ddest[i]);
            cJSON_AddItemToArray(done, x);
        } else {
            cJSON_Delete(x);
        }
    }
    /* Every disk is in: only now do the originals move aside. */
    for (size_t i = 0; st == OK && i < nstaged; i++) {
        cJSON *b = cJSON_CreateObject(), *sw = NULL;
        if (b) cJSON_AddStringToObject(b, "staging", tok[i]);
        st = remote_op("libvirt", "restore.swap", b, -1, &sw);
        if (st == OK) nswapped++;
        const char *prev = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sw, "previous"));
        if (st == OK && prev) {
            cJSON *pa = cJSON_GetObjectItemCaseSensitive(res, "previous");
            if (!pa) pa = cJSON_AddArrayToObject(res, "previous");
            cJSON_AddItemToArray(pa, cJSON_CreateString(prev));
        }
        cJSON_Delete(sw);
    }
    /* NVRAM and TPM state, staged for vm.install_state. */
    char state_tok[64] = "";
    if (st == OK && (nvram || tpm)) {
        cJSON *a = args_unit("libvirt", unit), *sr = NULL;
        if (a) { cJSON_AddStringToObject(a, "mount", nvram ? nvram : tpm); cJSON_AddNumberToObject(a, "job", (double)job_id); }
        st = remote_op("libvirt", "restore.stage", a, -1, &sr);
        const char *stok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sr, "staging"));
        const char *sp = abs_of(sr, "path");
        if (st == OK && (!stok || !sp)) st = set_error(ERR_IO, "unit restore: a malformed state staging");
        char dest[PATH_MAX + 16];
        if (st == OK && nvram) {
            snprintf(dest, sizeof(dest), "%.4000s/nvram.fd", sp);
            st = restore_file(store, db, repo_id, snap_id, o, ".cbackup-unit/config/nvram.fd", dest);
        }
        if (st == OK && tpm) {
            snprintf(dest, sizeof(dest), "%.4000s/tpm", sp);
            st = restore_file(store, db, repo_id, snap_id, o, tpm, dest);
        }
        if (st == OK) snprintf(state_tok, sizeof(state_tok), "%s", stok);
        cJSON_Delete(sr);
    }
    /* The definition, then the state for the UUID it got. */
    cJSON *dr = NULL;
    if (st == OK) st = define_send("libvirt", unit, txt, 0, &dr);
    const char *nuuid = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(dr, "uuid"));
    if (st == OK && !nuuid) st = set_error(ERR_IO, "unit restore: the define returned no uuid");
    if (st != OK && !new_name && nswapped > 0) st = roll_back("libvirt", unit, tok, nswapped, 0, st);
    else if (st != OK && new_name && began) st = left_under(o->target_root, st);
    if (st == OK && state_tok[0]) {
        cJSON *a = args_unit("libvirt", nuuid), *ir = NULL;
        if (a) cJSON_AddStringToObject(a, "staging", state_tok);
        st = remote_op("libvirt", "vm.install_state", a, -1, &ir);
        if (st == OK && ir) cJSON_AddItemToObject(res, "state", ir);
        else cJSON_Delete(ir);
    }
    if (st == OK && o->start) st = remote_op("libvirt", "unit.start", args_unit("libvirt", nuuid), -1, NULL);
    if (st == OK) {
        const cJSON *w;
        cJSON_ArrayForEach(w, cJSON_GetObjectItemCaseSensitive(dr, "warnings"))
            if (cJSON_IsString(w) && out->n_warnings < sizeof(out->warnings) / sizeof(out->warnings[0]) &&
                (out->warnings[out->n_warnings] = strdup(w->valuestring)) != NULL) out->n_warnings++;
        cJSON_AddItemToObject(res, "define", dr);
        dr = NULL;
    }
    cJSON_Delete(dr);
    cJSON_Delete(disks);
    cJSON_Delete(u);
    free(txt);
    free(ddest);
    free(tok);
    free(xml);
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
    /* --name with the unit's own name is the unit in place: it was taken
     * for a new name, compared with the id alone. */
    unit_define_opts_t own = *o;
    own.name = unit_define_name(db, repo_id, snap_id, unit, o->name);
    bytes_agg_t agg;
    memset(&agg, 0, sizeof(agg));
    agg.orig = o->ropts;
    if (o->ropts.bytes_cb || o->ropts.progress_cb) {
        own.ropts.progress_cb = o->ropts.progress_cb ? agg_progress : NULL;
        own.ropts.bytes_cb = o->ropts.bytes_cb ? agg_bytes : NULL;
        own.ropts.progress_ctx = &agg;
    }
    o = &own;
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

    if (st == OK && strcmp(prov, "libvirt") == 0) {
        st = vm_define_run(store, db, repo_id, snap_id, job_id, o, unit, new_name, res, out);
        for (size_t i = 0; st == OK && i < out->n_warnings; i++) log_warn("unit restore %s: %s", key, out->warnings[i]);
        if (st == OK && !(out->json = cJSON_PrintUnformatted(res))) st = set_error(ERR_NOMEM, "unit restore");
        cJSON_Delete(res);
        free(t);
        return st;
    }

    /* Every refusal before anything is written (B167): the target root's
     * paths, the definition built and checked by the helper, and for in
     * place the policy's leave to stop, stage and swap. */
    if (st == OK && new_name) {
        /* Under target_root only; refuse rather than merge into what is there. */
        for (size_t i = 0; st == OK && i < n; i++) {
            snprintf(t[i].dest, sizeof(t[i].dest), "%s%s", o->target_root, t[i].path);
            struct stat sb;
            if (lstat(t[i].dest, &sb) == 0) st = set_error(ERR_INVALID, "unit restore: %s exists; the target root must not hold the unit's paths yet", t[i].dest);
        }
    }
    cJSON *def = NULL;
    if (st == OK) st = build_definition(store, db, repo_id, snap_id, prov, unit, o, new_name, t, n, &def);
    char *txt = def ? cJSON_PrintUnformatted(def) : NULL;
    cJSON_Delete(def);
    if (st == OK && !txt) st = set_error(ERR_NOMEM, "unit restore: definition");
    define_check_t dc;
    if (st == OK) st = define_check(prov, unit, txt, &dc);
    if (st == OK && !new_name && !dc.allow_restore)
        st = set_error(ERR_INVALID, "unit restore refused, nothing written: the helper's policy does not "
                       "allow restore (allow_restore), which an in-place restore needs to stop the unit "
                       "and swap its data");

    char (*tok)[64] = calloc(TREES_MAX, sizeof(*tok));
    if (st == OK && !tok) st = set_error(ERR_NOMEM, "unit restore");
    size_t nswapped = 0;
    int stopped = 0, was_running = 0, rolled = 0, wrote = 0;
    if (st == OK && new_name) {
        for (size_t i = 0; st == OK && i < n; i++) {
            if ((st = mkdir_p(t[i].dest)) != OK) break;
            /* The tree lands at target_root/<its path>. */
            restore_opts_t ro = o->ropts;
            ro.subpath = t[i].path;
            restore_result_t rr;
            memset(&rr, 0, sizeof(rr));
            wrote = 1;
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
        /* Where the backup read each tree (unit.json's source): the helper
         * refuses a swap when the recorded path resolves elsewhere now
         * (B178). A snapshot without one is staged without the check. */
        cJSON *uj = NULL;
        {
            char *ujt = NULL;
            size_t tl = 0;
            if (unit_read_file(store, db, repo_id, snap_id, ".cbackup-unit/unit.json", DEF_FILE_MAX, &ujt, &tl) == OK)
                uj = cJSON_ParseWithLength(ujt, tl);
            else
                err_clear();
            free(ujt);
        }
        /* Every tree's staging asked first, nothing created (B189): a
         * refusal (no snapshot domain, a staging on another mount, a path
         * that resolves elsewhere now) used to come only once the unit was
         * stopped, and left it stopped. */
        for (size_t i = 0; st == OK && i < n; i++) {
            cJSON *a = args_unit(prov, unit), *cr = NULL;
            const char *real = tree_source(uj, t[i].id);
            if (a) {
                cJSON_AddStringToObject(a, "mount", t[i].path);
                cJSON_AddNumberToObject(a, "job", (double)job_id);
                if (real) cJSON_AddStringToObject(a, "real", real);
                cJSON_AddBoolToObject(a, "check", 1);
            }
            st = remote_op(prov, "restore.stage", a, -1, &cr);
            if (st == OK && !cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cr, "checked")))
                st = set_error(ERR_IO, "the helper answered a staging check with a staging");
            cJSON_Delete(cr);
            if (st != OK) {
                char cwhy[MSG_CLIP_MAX];
                msg_clip(cwhy, sizeof(cwhy), err_msg());
                st = set_error(st, "unit restore refused, nothing written, %s:%s left as it was: %s", prov, unit, cwhy);
            }
        }
        cJSON *sr = NULL;
        if (st == OK) st = remote_op(prov, "unit.stop", args_unit(prov, unit), -1, &sr);
        if (st == OK) {
            const cJSON *wr = cJSON_GetObjectItemCaseSensitive(sr, "was_running");
            stopped = 1;
            was_running = cJSON_IsBool(wr) ? cJSON_IsTrue(wr)
                        : dc.state == SP_STATE_RUNNING || dc.state == SP_STATE_PAUSED;
        }
        cJSON_Delete(sr);
        for (size_t i = 0; st == OK && i < n; i++) {
            cJSON *a = args_unit(prov, unit);
            sr = NULL;
            const char *real = tree_source(uj, t[i].id);
            if (a) {
                cJSON_AddStringToObject(a, "mount", t[i].path);
                cJSON_AddNumberToObject(a, "job", (double)job_id);
                if (real) cJSON_AddStringToObject(a, "real", real);
            }
            st = remote_op(prov, "restore.stage", a, -1, &sr);
            const char *stok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sr, "staging"));
            const char *sp = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(sr, "path"));
            if (st == OK && (!stok || !sp || sp[0] != '/')) st = set_error(ERR_IO, "unit restore: a malformed staging for %s", t[i].path);
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
                if (b) cJSON_AddStringToObject(b, "staging", stok);
                st = remote_op(prov, "restore.swap", b, -1, &sw);
                if (st == OK) snprintf(tok[nswapped++], sizeof(tok[0]), "%s", stok);
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
        cJSON_Delete(uj);
        if (st != OK && stopped) { st = roll_back(prov, unit, tok, nswapped, was_running, st); rolled = 1; }
    }

    /* Then the definition, by memfd. */
    cJSON *dr = NULL;
    if (st == OK) st = define_send(prov, unit, txt, 0, &dr);
    if (st != OK && stopped && !rolled) st = roll_back(prov, unit, tok, nswapped, was_running, st);
    else if (st != OK && new_name && wrote) st = left_under(o->target_root, st);
    free(txt);
    free(tok);
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
