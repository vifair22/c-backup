#define _GNU_SOURCE

#include "mounts.h"

#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int under(const char *path, const char *root)
{
    size_t n = strlen(root);
    if (n == 1 && root[0] == '/') return path[0] == '/';
    return n && strncmp(path, root, n) == 0 && (path[n] == '\0' || path[n] == '/');
}

int mount_is_system(const char *src)
{
    static const char *const sys[] = { "/dev", "/proc", "/sys", "/run", "/var/run", "/etc", "/boot" };
    for (size_t i = 0; i < sizeof(sys) / sizeof(sys[0]); i++)
        if (under(src, sys[i])) return 1;
    size_t n = strlen(src);
    return n >= 11 && strcmp(src + n - 11, "docker.sock") == 0;
}

static void set(mount_class_t *o, mount_verdict_t v, const char *reason)
{
    o->verdict = v;
    snprintf(o->reason, sizeof(o->reason), "%s", reason);
}

/* The names under /mnt that are not one pool or one disk. */
static int not_a_device(const char *n)
{
    return n[0] == '.' || strcmp(n, "user") == 0 || strcmp(n, "user0") == 0 || strcmp(n, "disks") == 0 ||
           strcmp(n, "remotes") == 0 || strcmp(n, "addons") == 0 || strcmp(n, "rootshare") == 0;
}

static int is_array_disk(const char *n)
{
    return strncmp(n, "disk", 4) == 0 && n[4] >= '0' && n[4] <= '9';
}

/* Resolve symlinks in the host's namespace: the helper's own realpath(3)
 * would follow an absolute link into its own container. */
static status_t host_realpath(const mount_cfg_t *c, const char *p, char *buf, size_t sz)
{
    char rp[HOST_TOOL_PATH_MAX];
    snprintf(rp, sizeof(rp), "%s", c->tools->realpath[0] ? c->tools->realpath : "/usr/bin/realpath");
    const char *argv[] = { rp, "-e", "--", p, NULL };
    char *out = NULL;
    status_t st = host_run(c->tools, 0, argv, 30, &out, PATH_MAX + 2);
    if (st != OK) return st;
    out[strcspn(out, "\n")] = '\0';
    if (out[0] != '/' || strlen(out) >= sz) { free(out); return set_error(ERR_IO, "mounts: realpath of %s", p); }
    snprintf(buf, sz, "%s", out);
    free(out);
    return OK;
}

/* ---- the mount table (B170) ---- */

typedef struct {
    unsigned maj, min;
    char *root, *mp;
    int loop;
} minfo_t;

/* mountinfo escapes space, tab, newline and backslash as \ooo. */
static void unescape(char *s)
{
    char *w = s;
    for (const char *r = s; *r; ) {
        if (r[0] == '\\' && r[1] >= '0' && r[1] <= '3' && r[2] >= '0' && r[2] <= '7' && r[3] >= '0' && r[3] <= '7') {
            *w++ = (char)(((r[1] - '0') << 6) | ((r[2] - '0') << 3) | (r[3] - '0'));
            r += 4;
        } else {
            *w++ = *r++;
        }
    }
    *w = '\0';
}

static void minfo_free(minfo_t *m, size_t n)
{
    for (size_t i = 0; i < n; i++) { free(m[i].root); free(m[i].mp); }
    free(m);
}

/* "id parent maj:min root mountpoint opts [optional...] - fstype source super" */
static int minfo_line(char *l, minfo_t *e)
{
    char *save = NULL;
    char *f[5];
    for (int i = 0; i < 5; i++) if (!(f[i] = strtok_r(i ? NULL : l, " \n", &save))) return -1;
    if (sscanf(f[2], "%u:%u", &e->maj, &e->min) != 2) return -1;
    char *t;
    while ((t = strtok_r(NULL, " \n", &save)) != NULL && strcmp(t, "-") != 0) {}
    char *fstype = t ? strtok_r(NULL, " \n", &save) : NULL;
    char *src = fstype ? strtok_r(NULL, " \n", &save) : NULL;
    unescape(f[3]);
    unescape(f[4]);
    if (f[3][0] != '/' || f[4][0] != '/') return -1;
    e->loop = src && strncmp(src, "/dev/loop", 9) == 0;
    e->root = strdup(f[3]);
    e->mp = strdup(f[4]);
    if (!e->root || !e->mp) { free(e->root); free(e->mp); return -2; }
    return 0;
}

static status_t minfo_read(const char *path, minfo_t **out, size_t *n)
{
    *out = NULL;
    *n = 0;
    FILE *f = fopen(path, "re");
    if (!f) return set_error_errno(ERR_IO, "mounts: cannot read %s", path);
    minfo_t *m = NULL;
    size_t cap = 0;
    char *line = NULL;
    size_t lsz = 0;
    status_t st = OK;
    while (getline(&line, &lsz, f) > 0) {
        if (*n == cap) {
            size_t nc = cap ? cap * 2 : 64;
            minfo_t *g = realloc(m, nc * sizeof(*m));
            if (!g) { st = set_error(ERR_NOMEM, "mounts: mount table"); break; }
            m = g;
            cap = nc;
        }
        int rc = minfo_line(line, &m[*n]);
        if (rc == -2) { st = set_error(ERR_NOMEM, "mounts: mount table"); break; }
        if (rc == 0) (*n)++;
    }
    free(line);
    fclose(f);
    if (st != OK) { minfo_free(m, *n); *n = 0; return st; }
    *out = m;
    return OK;
}

void mount_default_mountinfo(const host_tools_t *t, char *buf, size_t sz)
{
    snprintf(buf, sz, "%s", t->host_root[0] ? "/proc/1/mountinfo" : "/proc/self/mountinfo");
}

status_t mount_unbind(const char *mountinfo, const char *mnt_root, const char *path,
                      char *out, size_t sz, int *loop)
{
    *loop = 0;
    minfo_t *m = NULL;
    size_t n = 0;
    status_t st = minfo_read(mountinfo, &m, &n);
    if (st != OK) return st;
    /* The mount path is on: the longest mount point over it, the last
     * listed of equals, since a later mount covers an earlier one. */
    size_t at = SIZE_MAX, best = 0;
    for (size_t i = 0; i < n; i++) {
        size_t l = strlen(m[i].mp);
        if (under(path, m[i].mp) && (at == SIZE_MAX || l >= best)) { at = i; best = l; }
    }
    int done = at == SIZE_MAX || strcmp(m[at].root, "/") == 0;
    size_t via = SIZE_MAX;
    if (!done) {
        /* Another mount of the same filesystem whose root holds this one's:
         * a pool path first, then the filesystem's own root, then the
         * closest root, then the shortest mount point. Never one inside
         * the bind itself. */
        long vs = -1;
        for (size_t i = 0; i < n; i++) {
            if (i == at || m[i].maj != m[at].maj || m[i].min != m[at].min) continue;
            if (!under(m[at].root, m[i].root) || under(m[i].mp, m[at].mp)) continue;
            long sc = (mnt_root[0] && under(m[i].mp, mnt_root) ? 1L << 30 : 0) +
                      (strcmp(m[i].root, "/") == 0 ? 1L << 29 : 0) +
                      ((long)strlen(m[i].root) << 12) + (4095 - (long)(strlen(m[i].mp) & 4095));
            if (sc > vs) { vs = sc; via = i; }
        }
        if (via == SIZE_MAX) done = 1;
    }
    /* Readable only where it is: say so when that is an image file. */
    if (done && at != SIZE_MAX) *loop = m[at].loop;
    int w;
    if (done) {
        w = snprintf(out, sz, "%s", path);
    } else {
        const char *mid = strcmp(m[via].root, "/") == 0 ? m[at].root : m[at].root + strlen(m[via].root);
        const char *rest = strcmp(m[at].mp, "/") == 0 ? path : path + strlen(m[at].mp);
        const char *base = strcmp(m[via].mp, "/") == 0 ? "" : m[via].mp;
        if (strcmp(mid, "/") == 0) mid = "";
        w = snprintf(out, sz, "%s%s%s", base, mid, rest);
        if (w > 0 && out[0] == '\0') w = snprintf(out, sz, "/");
    }
    minfo_free(m, n);
    if (w < 0 || (size_t)w >= sz) return set_error(ERR_INVALID, "mounts: path too long for %s", path);
    return OK;
}

/* Where source's bytes are: /mnt/user to a pool or disk, symlinks, then
 * bind mounts. again: already one bind deep. */
static status_t resolve(const mount_cfg_t *c, const char *source, mount_class_t *out, int again)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s", source);
    /* /mnt/user/<share>/<rest>: the one device holding it, if only one. */
    if (c->user_root[0] && under(path, c->user_root) && strlen(path) > strlen(c->user_root)) {
        const char *rel = path + strlen(c->user_root);      /* "/<share>/<rest>" */
        char mnt[PATH_MAX + HOST_TOOL_PATH_MAX];
        if (host_path(c->tools, c->mnt_root, mnt, sizeof(mnt)) != 0) return set_error(ERR_INVALID, "mounts: path too long");
        DIR *d = opendir(mnt);
        if (!d) return set_error_errno(ERR_IO, "mounts: cannot list %s", c->mnt_root);
        char found[PATH_MAX] = "";
        int nfound = 0, array = 0;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (not_a_device(de->d_name)) continue;
            char cand[PATH_MAX], hc[PATH_MAX + HOST_TOOL_PATH_MAX];
            if (snprintf(cand, sizeof(cand), "%s/%s%s", c->mnt_root, de->d_name, rel) >= (int)sizeof(cand)) continue;
            struct stat sb;
            if (host_path(c->tools, cand, hc, sizeof(hc)) != 0 || lstat(hc, &sb) != 0) continue;
            if (++nfound == 1) { snprintf(found, sizeof(found), "%s", cand); array = is_array_disk(de->d_name); }
        }
        closedir(d);
        if (nfound == 0)
            return set_error(ERR_NOT_FOUND, "mounts: %s exists on no pool or disk under %s", source, c->mnt_root);
        if (nfound > 1) {
            /* Split across devices (or the mover is running): no one
             * snapshot covers it, so it is read through shfs. */
            snprintf(out->real, sizeof(out->real), "%s", source);
            set(out, MOUNT_INCLUDED, "share-split");
            return OK;
        }
        snprintf(path, sizeof(path), "%s", found);
        if (array) set(out, MOUNT_INCLUDED, "array");
    }
    status_t st = host_realpath(c, path, out->real, sizeof(out->real));
    if (st != OK) {
        if (strstr(err_msg(), "No such file")) {
            char why[512];
            snprintf(why, sizeof(why), "%.500s", err_msg());
            err_clear();
            return set_error(ERR_NOT_FOUND, "mounts: the source %s is missing (%s)", source, why);
        }
        return st;
    }
    /* A bind mount on the host (Unraid's /var/lib/docker over
     * /mnt/cache/system/docker) hides which filesystem the bytes are on:
     * no snapshot domain matches it, and the daemon does not mount it.
     * Read it through the filesystem's own mount instead (B170). */
    if (c->mountinfo[0]) {
        char hp[PATH_MAX];
        int loop = 0;
        if ((st = mount_unbind(c->mountinfo, c->mnt_root, out->real, hp, sizeof(hp), &loop)) != OK) return st;
        if (strcmp(hp, out->real) != 0) {
            char chk[PATH_MAX + HOST_TOOL_PATH_MAX];
            struct stat sb;
            if (host_path(c->tools, hp, chk, sizeof(chk)) != 0 || lstat(chk, &sb) != 0)
                return set_error(ERR_NOT_FOUND, "mounts: %s is bind-mounted from %s, which does not exist",
                                 out->real, hp);
            /* A bind of a /mnt/user path (Docker's directory on a share)
             * lands on shfs: resolve that to its pool or disk once more. */
            if (!again && c->user_root[0] && under(hp, c->user_root)) return resolve(c, hp, out, 1);
            snprintf(out->real, sizeof(out->real), "%s", hp);
        } else if (loop && !out->reason[0]) {
            set(out, MOUNT_INCLUDED, "loop-image");
        }
    }
    return OK;
}

status_t mount_classify(const mount_cfg_t *c, const char *type, const char *source,
                        const char *driver, mount_class_t *out)
{
    memset(out, 0, sizeof(*out));
    snprintf(out->recorded, sizeof(out->recorded), "%s", source ? source : "");
    if (!type || strcmp(type, "tmpfs") == 0 || strcmp(type, "npipe") == 0 || !source || source[0] != '/') {
        set(out, MOUNT_EXCLUDED, "system");
        return OK;
    }
    if (strcmp(type, "volume") == 0) {
        if (driver && *driver && strcmp(driver, "local") != 0) { set(out, MOUNT_EXCLUDED, "remote-volume"); return OK; }
    } else if (strcmp(type, "bind") == 0) {
        if (mount_is_system(source)) { set(out, MOUNT_EXCLUDED, "system"); return OK; }
        int inc = 0;
        for (size_t i = 0; i < c->n_include && !inc; i++) inc = under(source, c->include_roots[i]);
        if (!inc) { set(out, MOUNT_EXCLUDED, "outside-include-roots"); return OK; }
    } else {
        set(out, MOUNT_EXCLUDED, "system");
        return OK;
    }
    set(out, MOUNT_INCLUDED, "");

    return resolve(c, source, out, 0);
}
