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
    if (snprintf(path, sizeof(path), "%s/%s", dir, name) >= (int)sizeof(path))
        return 0;
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

    if (out->snap_total > 0) {
        snapshot_t *head = NULL;
        if (snapshot_load(repo, out->snap_total, &head) == OK) {
            out->head_entries = head->node_count;
            for (uint32_t i = 0; i < head->node_count; i++) {
                if (head->nodes[i].type == NODE_TYPE_REG)
                    out->head_logical_bytes += head->nodes[i].size;
            }
            snapshot_free(head);
        }
    }

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
                    uint64_t sz = file_size_at(dir, de->d_name);
                    out->snap_count++;
                    out->snap_bytes += sz;
                    out->total_bytes += sz;
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
                if (snprintf(subdir, sizeof(subdir), "%s/%s", objdir, de->d_name)
                        >= (int)sizeof(subdir)) continue;
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

    return OK;
}

void repo_stats_print(const repo_stat_t *s) {
    char buf[32];
    printf("snapshots:             %u present", s->snap_count);
    if (s->snap_total > 0) printf(" / %u total (HEAD)", s->snap_total);
    printf("\n");

    fmt_bytes(s->head_logical_bytes, buf, sizeof(buf));
    printf("head logical size:     %s  (%u entries)\n", buf, s->head_entries);

    fmt_bytes(s->snap_bytes, buf, sizeof(buf));
    printf("manifests physical:    %u  (%s)\n", s->snap_count, buf);

    fmt_bytes(s->loose_bytes, buf, sizeof(buf));
    printf("loose objects physical: %u  (%s)\n", s->loose_objects, buf);

    fmt_bytes(s->pack_bytes, buf, sizeof(buf));
    printf("pack files physical:   %u  (%s)\n", s->pack_files, buf);

    fmt_bytes(s->total_bytes, buf, sizeof(buf));
    printf("repo physical total:   %s\n", buf);
}
