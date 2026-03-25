#define _POSIX_C_SOURCE 200809L
#include "ls.h"
#include "object.h"
#include "snapshot.h"
#include "../vendor/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fnmatch.h>

/* ------------------------------------------------------------------ */
/* Internal path table — mirrors snap_paths_build in restore.c        */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t parent_id;
    uint64_t node_id;
    char    *name;
    char    *full_path;   /* lazily built */
} ls_dirent_t;

static ls_dirent_t *find_dr(ls_dirent_t *arr, uint32_t n, uint64_t id) {
    for (uint32_t i = 0; i < n; i++)
        if (arr[i].node_id == id) return &arr[i];
    return NULL;
}

static char *build_path(ls_dirent_t *arr, uint32_t n, ls_dirent_t *e) {
    if (e->full_path) return e->full_path;
    if (e->parent_id == 0) {
        e->full_path = strdup(e->name);
        return e->full_path;
    }
    ls_dirent_t *parent = find_dr(arr, n, e->parent_id);
    if (!parent) { e->full_path = strdup(e->name); return e->full_path; }
    char *pp = build_path(arr, n, parent);
    if (!pp) return NULL;
    size_t plen = strlen(pp), nlen = strlen(e->name);
    char *fp = malloc(plen + 1 + nlen + 1);
    if (!fp) return NULL;
    memcpy(fp, pp, plen);
    fp[plen] = '/';
    memcpy(fp + plen + 1, e->name, nlen + 1);
    e->full_path = fp;
    return fp;
}

/* ------------------------------------------------------------------ */
/* Formatting helpers                                                  */
/* ------------------------------------------------------------------ */

static char type_char(uint8_t node_type) {
    switch (node_type) {
        case NODE_TYPE_DIR:      return 'd';
        case NODE_TYPE_SYMLINK:  return 'l';
        case NODE_TYPE_FIFO:     return 'p';
        case NODE_TYPE_CHR:      return 'c';
        case NODE_TYPE_BLK:      return 'b';
        default:                 return '-';
    }
}

static void mode_str(uint8_t node_type, uint32_t mode, char out[11]) {
    out[0] = type_char(node_type);
    out[1] = (mode & 0400) ? 'r' : '-';
    out[2] = (mode & 0200) ? 'w' : '-';
    out[3] = (mode & 04000) ? 's' : (mode & 0100) ? 'x' : '-';
    out[4] = (mode & 0040) ? 'r' : '-';
    out[5] = (mode & 0020) ? 'w' : '-';
    out[6] = (mode & 02000) ? 's' : (mode & 0010) ? 'x' : '-';
    out[7] = (mode & 0004) ? 'r' : '-';
    out[8] = (mode & 0002) ? 'w' : '-';
    out[9] = (mode & 01000) ? 't' : (mode & 0001) ? 'x' : '-';
    out[10] = '\0';
}

static void fmt_time(uint64_t sec, char out[20]) {
    time_t t = (time_t)sec;
    struct tm *tm = localtime(&t);
    if (tm) strftime(out, 20, "%Y-%m-%d %H:%M", tm);
    else    snprintf(out, 20, "?");
}

static void fmt_size_human(uint64_t n, char out[16]) {
    if (n >= (uint64_t)1024 * 1024 * 1024 * 1024)
        snprintf(out, 16, "%.1fT", (double)n / (1024.0 * 1024.0 * 1024.0 * 1024.0));
    else if (n >= (uint64_t)1024 * 1024 * 1024)
        snprintf(out, 16, "%.1fG", (double)n / (1024.0 * 1024.0 * 1024.0));
    else if (n >= (uint64_t)1024 * 1024)
        snprintf(out, 16, "%.1fM", (double)n / (1024.0 * 1024.0));
    else if (n >= 1024)
        snprintf(out, 16, "%.1fK", (double)n / 1024.0);
    else
        snprintf(out, 16, "%lluB", (unsigned long long)n);
}

static const char *name_color_for_node(const node_t *nd) {
    if (!nd) return NULL;
    switch (nd->type) {
        case NODE_TYPE_DIR:     return "\x1b[1;34m";
        case NODE_TYPE_SYMLINK: return "\x1b[1;36m";
        case NODE_TYPE_FIFO:    return "\x1b[33m";
        case NODE_TYPE_CHR:
        case NODE_TYPE_BLK:     return "\x1b[1;33m";
        case NODE_TYPE_REG:
        case NODE_TYPE_HARDLINK:
            if (nd->mode & 0111) return "\x1b[1;32m";
            return NULL;
        default:
            return NULL;
    }
}

/* ------------------------------------------------------------------ */
/* Comparison for sorting child entries by name                        */
/* ------------------------------------------------------------------ */

typedef struct {
    const char    *name;
    const node_t  *node;
    char           symlink_target[256];   /* empty if not a symlink */
} ls_entry_t;

static int ls_entry_cmp(const void *a, const void *b) {
    return strcmp(((const ls_entry_t *)a)->name,
                  ((const ls_entry_t *)b)->name);
}

/* ------------------------------------------------------------------ */
/* snapshot_ls                                                         */
/* ------------------------------------------------------------------ */

status_t snapshot_ls(repo_t *repo, uint32_t snap_id, const char *dir_path,
                     int recursive, char type_filter, const char *name_glob) {
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    /* Normalise dir_path: strip leading/trailing slashes, treat "." as "" */
    char norm[4096] = "";
    if (dir_path && strcmp(dir_path, ".") != 0 && strcmp(dir_path, "/") != 0) {
        const char *s = dir_path;
        while (*s == '/') s++;
        size_t len = strlen(s);
        while (len > 0 && s[len - 1] == '/') len--;
        if (len >= sizeof(norm)) { snapshot_free(snap); return set_error(ERR_INVALID, "ls: path too long"); }
        memcpy(norm, s, len);
        norm[len] = '\0';
    }

    /* Build flat dirent list from the snapshot's binary dirent_data */
    ls_dirent_t *flat = calloc(snap->dirent_count + 1, sizeof(ls_dirent_t));
    if (!flat) { snapshot_free(snap); return set_error(ERR_NOMEM, "ls: alloc dirent list failed"); }
    uint32_t n_flat = 0;

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;
    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;
        char *name = malloc(dr.name_len + 1);
        if (!name) { st = ERR_NOMEM; goto done; }
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;
        flat[n_flat].parent_id  = dr.parent_node;
        flat[n_flat].node_id    = dr.node_id;
        flat[n_flat].name       = name;
        flat[n_flat].full_path  = NULL;
        n_flat++;
    }

    /* Force full path computation for all entries */
    for (uint32_t i = 0; i < n_flat; i++)
        build_path(flat, n_flat, &flat[i]);

    /* If dir_path is non-empty, confirm it exists as a directory in the snapshot */
    if (norm[0] != '\0') {
        int found_dir = 0;
        for (uint32_t i = 0; i < n_flat; i++) {
            if (!flat[i].full_path || strcmp(flat[i].full_path, norm) != 0) continue;
            /* Look up node type */
            for (uint32_t j = 0; j < snap->node_count; j++) {
                if (snap->nodes[j].node_id == flat[i].node_id &&
                    snap->nodes[j].type == NODE_TYPE_DIR) {
                    found_dir = 1; break;
                }
            }
            break;
        }
        if (!found_dir) {
            fprintf(stderr, "ls: '%s' is not a directory in snapshot %u\n",
                    norm, snap_id);
            st = ERR_INVALID;
            goto done;
        }
    }

    /* Collect direct children (or recursive descendants) of norm */
    ls_entry_t *children = malloc(n_flat * sizeof(ls_entry_t));
    if (!children) { st = ERR_NOMEM; goto done; }
    uint32_t n_children = 0;

    size_t norm_len = strlen(norm);

    for (uint32_t i = 0; i < n_flat; i++) {
        if (!flat[i].full_path) continue;

        /* Compute parent path of this entry */
        const char *fp   = flat[i].full_path;
        const char *slash = strrchr(fp, '/');
        const char *basename;
        char        parent[4096] = "";

        if (!slash) {
            /* top-level entry */
            basename = fp;
            /* parent = "" */
        } else {
            basename = slash + 1;
            size_t plen = (size_t)(slash - fp);
            if (plen >= sizeof(parent)) continue;
            memcpy(parent, fp, plen);
            parent[plen] = '\0';
        }

        const char *disp_name = NULL;
        if (recursive) {
            if (norm[0] == '\0') {
                disp_name = fp;
            } else {
                if (strcmp(fp, norm) == 0) continue; /* skip queried dir itself */
                if (strncmp(fp, norm, norm_len) != 0 || fp[norm_len] != '/') continue;
                disp_name = fp + norm_len + 1;
            }
        } else {
            if (strcmp(parent, norm) != 0) continue;
            disp_name = basename;
        }

        /* Find node */
        const node_t *nd = NULL;
        for (uint32_t j = 0; j < snap->node_count; j++) {
            if (snap->nodes[j].node_id == flat[i].node_id) {
                nd = &snap->nodes[j]; break;
            }
        }

        if (!nd) continue;
        int type_ok = 1;
        if (type_filter) {
            switch (type_filter) {
                case 'f': type_ok = (nd->type == NODE_TYPE_REG || nd->type == NODE_TYPE_HARDLINK); break;
                case 'd': type_ok = (nd->type == NODE_TYPE_DIR); break;
                case 'l': type_ok = (nd->type == NODE_TYPE_SYMLINK); break;
                case 'p': type_ok = (nd->type == NODE_TYPE_FIFO); break;
                case 'c': type_ok = (nd->type == NODE_TYPE_CHR); break;
                case 'b': type_ok = (nd->type == NODE_TYPE_BLK); break;
                default: type_ok = 1; break;
            }
        }
        if (!type_ok) continue;
        if (name_glob && *name_glob && fnmatch(name_glob, disp_name, 0) != 0) continue;

        ls_entry_t *ce = &children[n_children++];
        ce->name = disp_name;   /* points into flat[i].full_path */
        ce->node = nd;
        ce->symlink_target[0] = '\0';

        /* Load symlink target for display */
        if (nd && nd->type == NODE_TYPE_SYMLINK) {
            uint8_t zero[OBJECT_HASH_SIZE] = {0};
            if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void  *tdata = NULL;
                size_t tlen  = 0;
                if (object_load(repo, nd->content_hash, &tdata, &tlen, NULL) == OK) {
                    size_t copy = tlen < sizeof(ce->symlink_target) - 1
                                  ? tlen : sizeof(ce->symlink_target) - 1;
                    memcpy(ce->symlink_target, tdata, copy);
                    ce->symlink_target[copy] = '\0';
                    free(tdata);
                }
            }
        }
    }

    qsort(children, n_children, sizeof(*children), ls_entry_cmp);

    int use_color = isatty(STDOUT_FILENO) ? 1 : 0;
    const char *no_color = getenv("NO_COLOR");
    if (no_color && *no_color) use_color = 0;

    /* Print header */
    if (norm[0] == '\0') printf("snapshot %u  /\n", snap_id);
    else                  printf("snapshot %u  /%s\n", snap_id, norm);
    if (recursive) printf("(recursive)\n");

    if (n_children == 0) {
        printf("(empty)\n");
        free(children);
        goto done;
    }

    printf("MODE        UID   GID          SIZE  MTIME             NAME\n");
    printf("----------  ----  ----  ------------  ----------------  ----\n");

    /* Print entries */
    for (uint32_t i = 0; i < n_children; i++) {
        const ls_entry_t *ce = &children[i];
        if (!ce->node) {
            printf("??????????  ???  ???  %12s  %s\n", "?", ce->name);
            continue;
        }
        char mstr[11];
        mode_str(ce->node->type, ce->node->mode, mstr);

        char tstr[20];
        fmt_time(ce->node->mtime_sec, tstr);

        char sizebuf[16];
        fmt_size_human(ce->node->size, sizebuf);

        const char *color_on = "";
        const char *color_off = "";
        if (use_color) {
            const char *c = name_color_for_node(ce->node);
            if (c) {
                color_on = c;
                color_off = "\x1b[0m";
            }
        }

        if (ce->node->type == NODE_TYPE_SYMLINK) {
            printf("%s  %4u  %4u  %12s  %s  %s%s%s -> %s\n",
                   mstr, ce->node->uid, ce->node->gid,
                   sizebuf, tstr, color_on, ce->name, color_off, ce->symlink_target);
        } else if (ce->node->type == NODE_TYPE_CHR ||
                   ce->node->type == NODE_TYPE_BLK) {
            printf("%s  %4u  %4u  %5u,%5u  %s  %s%s%s\n",
                   mstr, ce->node->uid, ce->node->gid,
                   ce->node->device.major, ce->node->device.minor,
                   tstr, color_on, ce->name, color_off);
        } else {
            printf("%s  %4u  %4u  %12s  %s  %s%s%s\n",
                   mstr, ce->node->uid, ce->node->gid,
                   sizebuf, tstr, color_on, ce->name, color_off);
        }
    }
    free(children);

done:
    for (uint32_t i = 0; i < n_flat; i++) {
        free(flat[i].name);
        free(flat[i].full_path);
    }
    free(flat);
    snapshot_free(snap);
    return st;
}
