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
        if (!isalnum((unsigned char)*p) && *p != '-' && *p != '_' && *p != '.') return 0;
    }
    return 1;
}

status_t tag_set(repo_t *repo, const char *name, uint32_t snap_id, int preserve) {
    if (!tag_name_valid(name)) return ERR_INVALID;

    char dir[PATH_MAX];
    tag_dir(repo, dir, sizeof(dir));
    if (mkdir(dir, 0755) == -1 && errno != EEXIST) return ERR_IO;

    char path[PATH_MAX];
    tag_path(repo, name, path, sizeof(path));
    FILE *f = fopen(path, "w");
    if (!f) return ERR_IO;
    fprintf(f, "id = %u\n", snap_id);
    fprintf(f, "preserve = %s\n", preserve ? "true" : "false");
    fclose(f);
    return OK;
}

/* Parse a tag file; both old format (plain number) and new key=value format. */
static status_t tag_parse(const char *path, uint32_t *out_id, int *out_preserve) {
    FILE *f = fopen(path, "r");
    if (!f) return ERR_NOT_FOUND;

    char line[256];
    int got_id = 0;
    *out_id = 0;
    if (out_preserve) *out_preserve = 0;

    /* Peek first char to detect old format (starts with a digit) */
    int ch = fgetc(f);
    if (ch == EOF) { fclose(f); return ERR_CORRUPT; }
    ungetc(ch, f);

    if (isdigit(ch)) {
        /* Old plain-number format */
        unsigned id = 0;
        int ok = (fscanf(f, "%u", &id) == 1);
        fclose(f);
        if (!ok || id == 0) return ERR_CORRUPT;
        *out_id = (uint32_t)id;
        return OK;
    }

    /* New key = value format */
    while (fgets(line, sizeof(line), f)) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';
        char *eq = strstr(line, " = ");
        if (!eq) continue;
        *eq = '\0';
        char *key = line;
        char *val = eq + 3;
        if (strcmp(key, "id") == 0) {
            *out_id = (uint32_t)atoi(val);
            got_id = 1;
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
    tag_path(repo, name, path, sizeof(path));
    return tag_parse(path, out_id, NULL);
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
        char path[PATH_MAX];
        tag_path(repo, de->d_name, path, sizeof(path));
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
    tag_dir(repo, dir, sizeof(dir));
    DIR *d = opendir(dir);
    if (!d) return 0;

    struct dirent *de;
    int found = 0;
    while ((de = readdir(d)) != NULL && !found) {
        if (de->d_name[0] == '.') continue;
        if (!tag_name_valid(de->d_name)) continue;
        char path[PATH_MAX];
        tag_path(repo, de->d_name, path, sizeof(path));
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
    char *end;
    unsigned long v = strtoul(arg, &end, 10);
    if (*end == '\0' && v > 0) {
        *out_id = (uint32_t)v;
        return OK;
    }
    return tag_get(repo, arg, out_id);
}
