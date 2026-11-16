#define _GNU_SOURCE

#include "mounts.h"

#include <dirent.h>
#include <errno.h>
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

static int is_system(const char *src)
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
        if (is_system(source)) { set(out, MOUNT_EXCLUDED, "system"); return OK; }
        int inc = 0;
        for (size_t i = 0; i < c->n_include && !inc; i++) inc = under(source, c->include_roots[i]);
        if (!inc) { set(out, MOUNT_EXCLUDED, "outside-include-roots"); return OK; }
    } else {
        set(out, MOUNT_EXCLUDED, "system");
        return OK;
    }
    set(out, MOUNT_INCLUDED, "");

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
    return OK;
}
