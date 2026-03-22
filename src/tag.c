#define _POSIX_C_SOURCE 200809L
#include "tag.h"
#include "snapshot.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int parse_u32_str(const char *s, uint32_t *out) {
    char *end = NULL;
    unsigned long v;
    if (!s || !*s || !out) return 0;
    errno = 0;
    v = strtoul(s, &end, 10);
    if (errno != 0 || *end != '\0' || v == 0 || v > UINT32_MAX) return 0;
    *out = (uint32_t)v;
    return 1;
}

static int tag_dir(repo_t *repo, char *buf, size_t sz) {
    int n = snprintf(buf, sz, "%s/tags", repo_path(repo));
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

static int tag_path(repo_t *repo, const char *name, char *buf, size_t sz) {
    int n = snprintf(buf, sz, "%s/tags/%s", repo_path(repo), name);
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

static int tag_name_valid(const char *name) {
    if (!name || !*name) return 0;
    for (const char *p = name; *p; p++) {
        if (!isalnum((unsigned char)*p) && *p != '-' && *p != '_' && *p != '.') return 0;
    }
    return 1;
}

status_t tag_set(repo_t *repo, const char *name, uint32_t snap_id, int preserve) {
    if (!tag_name_valid(name)) return ERR_INVALID;

    char dir[PATH_MAX];
    if (tag_dir(repo, dir, sizeof(dir)) != 0) return ERR_IO;
    if (mkdir(dir, 0755) == -1 && errno != EEXIST) return ERR_IO;

    char path[PATH_MAX];
    if (tag_path(repo, name, path, sizeof(path)) != 0) return ERR_IO;
    FILE *f = fopen(path, "w");
    if (!f) return ERR_IO;
    fprintf(f, "id = %u\n", snap_id);
    fprintf(f, "preserve = %s\n", preserve ? "true" : "false");
    fclose(f);
    return OK;
}

/* Parse a tag file in key=value format. */
static status_t tag_parse(const char *path, uint32_t *out_id, int *out_preserve) {
    FILE *f = fopen(path, "r");
    if (!f) return ERR_NOT_FOUND;

    char line[256];
    int got_id = 0;
    *out_id = 0;
    if (out_preserve) *out_preserve = 0;

    while (fgets(line, sizeof(line), f)) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';
        char *eq = strstr(line, " = ");
        if (!eq) continue;
        *eq = '\0';
        char *key = line;
        char *val = eq + 3;
        if (strcmp(key, "id") == 0) {
            if (parse_u32_str(val, out_id)) got_id = 1;
        } else if (strcmp(key, "preserve") == 0 && out_preserve) {
            *out_preserve = (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        }
    }
    fclose(f);
    return (got_id && *out_id > 0) ? OK : ERR_CORRUPT;
}

status_t tag_get(repo_t *repo, const char *name, uint32_t *out_id) {
    if (!tag_name_valid(name)) return ERR_INVALID;
    char path[PATH_MAX];
    if (tag_path(repo, name, path, sizeof(path)) != 0) return ERR_IO;
    return tag_parse(path, out_id, NULL);
}

status_t tag_delete(repo_t *repo, const char *name) {
    if (!tag_name_valid(name)) return ERR_INVALID;
    char path[PATH_MAX];
    if (tag_path(repo, name, path, sizeof(path)) != 0) return ERR_IO;
    if (unlink(path) == -1) return ERR_IO;
    return OK;
}

status_t tag_list(repo_t *repo) {
    char dir[PATH_MAX];
    if (tag_dir(repo, dir, sizeof(dir)) != 0) return ERR_IO;
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
        char path[PATH_MAX];
        if (tag_path(repo, de->d_name, path, sizeof(path)) != 0) continue;
        uint32_t id = 0;
        int preserve = 0;
        if (tag_parse(path, &id, &preserve) == OK) {
            printf("  %-30s -> snapshot %08u%s\n",
                   de->d_name, id, preserve ? "  [preserved]" : "");
            any = 1;
        }
    }
    closedir(d);
    if (!any) printf("(no tags)\n");
    return OK;
}

int tag_snap_is_preserved(repo_t *repo, uint32_t snap_id,
                          char *name_out, size_t name_sz) {
    char dir[PATH_MAX];
    if (tag_dir(repo, dir, sizeof(dir)) != 0) return 0;
    DIR *d = opendir(dir);
    if (!d) return 0;

    struct dirent *de;
    int found = 0;
    while ((de = readdir(d)) != NULL && !found) {
        if (de->d_name[0] == '.') continue;
        if (!tag_name_valid(de->d_name)) continue;
        char path[PATH_MAX];
        if (tag_path(repo, de->d_name, path, sizeof(path)) != 0) continue;
        uint32_t id = 0;
        int preserve = 0;
        if (tag_parse(path, &id, &preserve) == OK && id == snap_id && preserve) {
            if (name_out) snprintf(name_out, name_sz, "%s", de->d_name);
            found = 1;
        }
    }
    closedir(d);
    return found;
}

status_t tag_resolve(repo_t *repo, const char *arg, uint32_t *out_id) {
    if (arg && (strcmp(arg, "HEAD") == 0 || strcmp(arg, "head") == 0)) {
        return snapshot_read_head(repo, out_id);
    }
    if (parse_u32_str(arg, out_id)) return OK;
    return tag_get(repo, arg, out_id);
}
