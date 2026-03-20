#define _POSIX_C_SOURCE 200809L
#include "tag.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static void tag_dir(repo_t *repo, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/tags", repo_path(repo));
}

static void tag_path(repo_t *repo, const char *name, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/tags/%s", repo_path(repo), name);
}

static int tag_name_valid(const char *name) {
    if (!name || !*name) return 0;
    for (const char *p = name; *p; p++) {
        /* allow alphanumeric, dash, underscore, dot */
        if (!isalnum((unsigned char)*p) && *p != '-' && *p != '_' && *p != '.') return 0;
    }
    return 1;
}

status_t tag_set(repo_t *repo, const char *name, uint32_t snap_id) {
    if (!tag_name_valid(name)) return ERR_INVALID;

    char dir[PATH_MAX];
    tag_dir(repo, dir, sizeof(dir));
    if (mkdir(dir, 0755) == -1 && errno != EEXIST) return ERR_IO;

    char path[PATH_MAX];
    tag_path(repo, name, path, sizeof(path));
    FILE *f = fopen(path, "w");
    if (!f) return ERR_IO;
    fprintf(f, "%u\n", snap_id);
    fclose(f);
    return OK;
}

status_t tag_get(repo_t *repo, const char *name, uint32_t *out_id) {
    if (!tag_name_valid(name)) return ERR_INVALID;
    char path[PATH_MAX];
    tag_path(repo, name, path, sizeof(path));
    FILE *f = fopen(path, "r");
    if (!f) return ERR_NOT_FOUND;
    unsigned id = 0;
    int ok = (fscanf(f, "%u", &id) == 1);
    fclose(f);
    if (!ok || id == 0) return ERR_CORRUPT;
    *out_id = (uint32_t)id;
    return OK;
}

status_t tag_delete(repo_t *repo, const char *name) {
    if (!tag_name_valid(name)) return ERR_INVALID;
    char path[PATH_MAX];
    tag_path(repo, name, path, sizeof(path));
    if (unlink(path) == -1) return ERR_IO;
    return OK;
}

status_t tag_list(repo_t *repo) {
    char dir[PATH_MAX];
    tag_dir(repo, dir, sizeof(dir));
    DIR *d = opendir(dir);
    if (!d) {
        printf("(no tags)\n");
        return OK;
    }
    struct dirent *de;
    int any = 0;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        if (!tag_name_valid(de->d_name)) continue;
        uint32_t id = 0;
        if (tag_get(repo, de->d_name, &id) == OK) {
            printf("  %-30s -> snapshot %08u\n", de->d_name, id);
            any = 1;
        }
    }
    closedir(d);
    if (!any) printf("(no tags)\n");
    return OK;
}

status_t tag_resolve(repo_t *repo, const char *arg, uint32_t *out_id) {
    /* Try numeric first */
    char *end;
    unsigned long v = strtoul(arg, &end, 10);
    if (*end == '\0' && v > 0) {
        *out_id = (uint32_t)v;
        return OK;
    }
    /* Try tag lookup */
    return tag_get(repo, arg, out_id);
}
