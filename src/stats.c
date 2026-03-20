#define _POSIX_C_SOURCE 200809L
#include "stats.h"
#include "snapshot.h"

#include <dirent.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static uint64_t file_size_at(const char *dir, const char *name) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    struct stat st;
    if (stat(path, &st) == 0) return (uint64_t)st.st_size;
    return 0;
}

static void fmt_bytes(uint64_t n, char *buf, size_t sz) {
    if      (n >= (uint64_t)1024*1024*1024)
        snprintf(buf, sz, "%.1f GB", (double)n / (1024.0*1024*1024));
    else if (n >= 1024*1024)
        snprintf(buf, sz, "%.1f MB", (double)n / (1024.0*1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1f KB", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%" PRIu64 " B", n);
}

status_t repo_stats(repo_t *repo, repo_stat_t *out) {
    const char *root = repo_path(repo);
    memset(out, 0, sizeof(*out));

    snapshot_read_head(repo, &out->snap_total);

    /* Snapshots */
    {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s/snapshots", root);
        DIR *d = opendir(dir);
        if (d) {
            struct dirent *de;
            while ((de = readdir(d)) != NULL) {
                if (de->d_name[0] == '.') continue;
                uint32_t id; char dummy;
                if (sscanf(de->d_name, "%u.snap%c", &id, &dummy) == 1) {
                    out->snap_count++;
                    out->total_bytes += file_size_at(dir, de->d_name);
                }
            }
            closedir(d);
        }
    }

    /* Loose objects */
    {
        char objdir[PATH_MAX];
        snprintf(objdir, sizeof(objdir), "%s/objects", root);
        DIR *top = opendir(objdir);
        if (top) {
            struct dirent *de;
            while ((de = readdir(top)) != NULL) {
                if (de->d_name[0] == '.' || strlen(de->d_name) != 2) continue;
                char subdir[PATH_MAX];
                snprintf(subdir, sizeof(subdir), "%s/%s", objdir, de->d_name);
                DIR *sub = opendir(subdir);
                if (!sub) continue;
                struct dirent *sde;
                while ((sde = readdir(sub)) != NULL) {
                    if (sde->d_name[0] == '.') continue;
                    uint64_t sz = file_size_at(subdir, sde->d_name);
                    out->loose_objects++;
                    out->loose_bytes  += sz;
                    out->total_bytes  += sz;
                }
                closedir(sub);
            }
            closedir(top);
        }
    }

    /* Pack files */
    {
        char pdir[PATH_MAX];
        snprintf(pdir, sizeof(pdir), "%s/packs", root);
        DIR *d = opendir(pdir);
        if (d) {
            struct dirent *de;
            while ((de = readdir(d)) != NULL) {
                if (de->d_name[0] == '.') continue;
                uint32_t num; char suffix[8];
                if (sscanf(de->d_name, "pack-%08u.%7s", &num, suffix) == 2) {
                    uint64_t sz = file_size_at(pdir, de->d_name);
                    if (strcmp(suffix, "dat") == 0) out->pack_files++;
                    out->pack_bytes  += sz;
                    out->total_bytes += sz;
                }
            }
            closedir(d);
        }
    }

    /* Reverse records */
    {
        char rdir[PATH_MAX];
        snprintf(rdir, sizeof(rdir), "%s/reverse", root);
        DIR *d = opendir(rdir);
        if (d) {
            struct dirent *de;
            while ((de = readdir(d)) != NULL) {
                if (de->d_name[0] == '.') continue;
                uint32_t id; char dummy;
                if (sscanf(de->d_name, "%u.rev%c", &id, &dummy) == 1) {
                    uint64_t sz = file_size_at(rdir, de->d_name);
                    out->reverse_records++;
                    out->reverse_bytes += sz;
                    out->total_bytes   += sz;
                }
            }
            closedir(d);
        }
    }

    return OK;
}

void repo_stats_print(const repo_stat_t *s) {
    char buf[32];
    printf("snapshots:       %u present", s->snap_count);
    if (s->snap_total > 0) printf(" / %u total (HEAD)", s->snap_total);
    printf("\n");

    fmt_bytes(s->loose_bytes, buf, sizeof(buf));
    printf("loose objects:   %u  (%s)\n", s->loose_objects, buf);

    fmt_bytes(s->pack_bytes, buf, sizeof(buf));
    printf("pack files:      %u  (%s)\n", s->pack_files, buf);

    fmt_bytes(s->reverse_bytes, buf, sizeof(buf));
    printf("reverse records: %u  (%s)\n", s->reverse_records, buf);

    fmt_bytes(s->total_bytes, buf, sizeof(buf));
    printf("total repo size: %s\n", buf);
}
