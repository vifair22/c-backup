#define _POSIX_C_SOURCE 200809L
#include "stats.h"
#include "snapshot.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* ---- Stats cache ---------------------------------------------------- */

#define STATS_CACHE_MAGIC   0x42535443u  /* "BSTC" */
#define STATS_CACHE_VERSION 1u

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t snap_count;
    uint64_t snap_bytes;
    uint32_t loose_objects;
    uint64_t loose_bytes;
    uint32_t pack_files;
    uint64_t pack_bytes;
} stats_cache_hdr_t;  /* 40 bytes */

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

/* Try loading cached stats. Returns OK on success, ERR_NOT_FOUND if
 * the cache is missing or invalid. Does NOT fill HEAD-dependent fields
 * (snap_total, head_entries, head_logical_bytes). */
static status_t stats_cache_load(repo_t *repo, repo_stat_t *out) {
    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/stats.cache", repo_path(repo))
        >= (int)sizeof(path)) return ERR_NOT_FOUND;

    int fd = open(path, O_RDONLY);
    if (fd == -1) return ERR_NOT_FOUND;

    stats_cache_hdr_t hdr;
    if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr) ||
        hdr.magic != STATS_CACHE_MAGIC ||
        hdr.version != STATS_CACHE_VERSION) {
        close(fd);
        return ERR_NOT_FOUND;
    }
    close(fd);

    out->snap_count     = hdr.snap_count;
    out->snap_bytes     = hdr.snap_bytes;
    out->loose_objects  = hdr.loose_objects;
    out->loose_bytes    = hdr.loose_bytes;
    out->pack_files     = hdr.pack_files;
    out->pack_bytes     = hdr.pack_bytes;
    out->total_bytes    = hdr.snap_bytes + hdr.loose_bytes + hdr.pack_bytes;
    return OK;
}

/* Full filesystem scan — the slow path. Populates snap/loose/pack fields. */
static status_t stats_full_scan(repo_t *repo, repo_stat_t *out) {
    const char *root = repo_path(repo);

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

    /* Pack files (flat + sharded layout) */
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
                } else if (strlen(de->d_name) == 4) {
                    char subdir[PATH_MAX];
                    if (snprintf(subdir, sizeof(subdir), "%s/%s", pdir,
                                 de->d_name) >= (int)sizeof(subdir))
                        continue;
                    struct stat sb;
                    if (stat(subdir, &sb) != 0 || !S_ISDIR(sb.st_mode))
                        continue;
                    DIR *sd = opendir(subdir);
                    if (!sd) continue;
                    struct dirent *se;
                    while ((se = readdir(sd)) != NULL) {
                        if (sscanf(se->d_name, "pack-%08u.%7s", &num, suffix) == 2) {
                            uint64_t sz = file_size_at(subdir, se->d_name);
                            if (strcmp(suffix, "dat") == 0) out->pack_files++;
                            out->pack_bytes  += sz;
                            out->total_bytes += sz;
                        }
                    }
                    closedir(sd);
                }
            }
            closedir(d);
        }
    }

    return OK;
}

status_t repo_stats(repo_t *repo, repo_stat_t *out) {
    memset(out, 0, sizeof(*out));

    /* HEAD-dependent fields: always read fresh (cheap with V6 headers). */
    snapshot_read_head(repo, &out->snap_total);
    if (out->snap_total > 0) {
        snapshot_t *head = NULL;
        /* Try header-only first (V6: has logical_bytes precomputed). */
        if (snapshot_load_header_only(repo, out->snap_total, &head) == OK) {
            out->head_entries = head->node_count;
            if (head->logical_bytes > 0) {
                out->head_logical_bytes = head->logical_bytes;
                snapshot_free(head);
            } else {
                /* Pre-V6: fall back to loading nodes. */
                snapshot_free(head);
                head = NULL;
                if (snapshot_load_nodes_only(repo, out->snap_total, &head) == OK) {
                    out->head_entries = head->node_count;
                    for (uint32_t i = 0; i < head->node_count; i++) {
                        if (head->nodes[i].type == NODE_TYPE_REG)
                            out->head_logical_bytes += head->nodes[i].size;
                    }
                    snapshot_free(head);
                }
            }
        }
    }

    /* Try cache for the expensive fields. */
    if (stats_cache_load(repo, out) == OK)
        return OK;

    /* Cache miss — full scan. */
    return stats_full_scan(repo, out);
}

status_t stats_cache_rebuild(repo_t *repo) {
    repo_stat_t s;
    memset(&s, 0, sizeof(s));
    status_t st = stats_full_scan(repo, &s);
    if (st != OK) return st;

    stats_cache_hdr_t hdr = {
        .magic          = STATS_CACHE_MAGIC,
        .version        = STATS_CACHE_VERSION,
        .snap_count     = s.snap_count,
        .snap_bytes     = s.snap_bytes,
        .loose_objects  = s.loose_objects,
        .loose_bytes    = s.loose_bytes,
        .pack_files     = s.pack_files,
        .pack_bytes     = s.pack_bytes,
    };

    char tmp[PATH_MAX], final[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp/.stats.cache.tmp", repo_path(repo))
        >= (int)sizeof(tmp)) return ERR_IO;
    if (snprintf(final, sizeof(final), "%s/stats.cache", repo_path(repo))
        >= (int)sizeof(final)) return ERR_IO;

    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) return ERR_IO;
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) { close(fd); unlink(tmp); return ERR_IO; }
    fdatasync(fd);
    close(fd);
    rename(tmp, final);
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
