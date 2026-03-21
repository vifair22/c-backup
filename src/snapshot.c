#define _POSIX_C_SOURCE 200809L
#include "snapshot.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SNAP_MAGIC    0x43424B50u  /* "CBKP" */
#define SNAP_VERSION  2u
#define SNAP_VERSION1 1u           /* legacy — gfs_flags absent */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t snap_id;
    uint64_t created_sec;
    uint32_t node_count;
    uint32_t dirent_count;
    uint64_t dirent_data_len;
    uint32_t gfs_flags;
} snap_file_header_t;


static int snap_path(repo_t *repo, uint32_t id, char *buf, size_t bufsz) {
    return snprintf(buf, bufsz, "%s/snapshots/%08u.snap", repo_path(repo), id) >= 0 ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* Snapshot I/O                                                        */
/* ------------------------------------------------------------------ */

status_t snapshot_load(repo_t *repo, uint32_t snap_id, snapshot_t **out) {
    char path[PATH_MAX];
    snap_path(repo, snap_id, path, sizeof(path));

    int fd = open(path, O_RDONLY);
    if (fd == -1) { log_msg("ERROR", "cannot open snapshot file"); return ERR_IO; }

    /* Read the magic + version first to decide which header size to read */
    uint32_t magic = 0, version = 0;
    if (read(fd, &magic,   sizeof(magic))   != sizeof(magic)   ||
        read(fd, &version, sizeof(version)) != sizeof(version)) {
        close(fd); return ERR_CORRUPT;
    }
    if (magic != SNAP_MAGIC ||
        (version != SNAP_VERSION && version != SNAP_VERSION1)) {
        close(fd); log_msg("ERROR", "invalid snapshot magic/version"); return ERR_CORRUPT;
    }

    /* Read the remaining header fields individually */
    uint32_t snap_id_f = 0, node_count = 0, dirent_count = 0, gfs_flags = 0;
    uint64_t created_sec = 0, dirent_data_len = 0;

#define RD32(v) (read(fd, &(v), 4) != 4)
#define RD64(v) (read(fd, &(v), 8) != 8)
    if (RD32(snap_id_f) || RD64(created_sec) ||
        RD32(node_count) || RD32(dirent_count) || RD64(dirent_data_len)) {
        close(fd); return ERR_CORRUPT;
    }
    if (version == SNAP_VERSION) {
        if (RD32(gfs_flags)) { close(fd); return ERR_CORRUPT; }
    }
#undef RD32
#undef RD64

    snapshot_t *snap = calloc(1, sizeof(*snap));
    if (!snap) { close(fd); return ERR_NOMEM; }
    snap->snap_id         = snap_id_f;
    snap->created_sec     = created_sec;
    snap->node_count      = node_count;
    snap->dirent_count    = dirent_count;
    snap->dirent_data_len = (size_t)dirent_data_len;
    snap->gfs_flags       = gfs_flags;

    snap->nodes = malloc(node_count * sizeof(node_t));
    if (!snap->nodes && node_count > 0) { free(snap); close(fd); return ERR_NOMEM; }
    if (node_count > 0 &&
        read(fd, snap->nodes, node_count * sizeof(node_t)) !=
            (ssize_t)(node_count * sizeof(node_t))) {
        free(snap->nodes); free(snap); close(fd); return ERR_CORRUPT;
    }

    snap->dirent_data = malloc(snap->dirent_data_len);
    if (!snap->dirent_data && snap->dirent_data_len > 0) {
        free(snap->nodes); free(snap); close(fd); return ERR_NOMEM;
    }
    if (snap->dirent_data_len > 0 &&
        read(fd, snap->dirent_data, snap->dirent_data_len) !=
            (ssize_t)snap->dirent_data_len) {
        free(snap->dirent_data); free(snap->nodes); free(snap); close(fd); return ERR_CORRUPT;
    }

    close(fd);
    *out = snap;
    return OK;
}

status_t snapshot_write(repo_t *repo, snapshot_t *snap) {
    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s/tmp/snap.XXXXXX", repo_path(repo));
    int fd = mkstemp(tmppath);
    if (fd == -1) return ERR_IO;

    snap_file_header_t fhdr = {
        .magic           = SNAP_MAGIC,
        .version         = SNAP_VERSION,
        .snap_id         = snap->snap_id,
        .created_sec     = snap->created_sec,
        .node_count      = snap->node_count,
        .dirent_count    = snap->dirent_count,
        .dirent_data_len = snap->dirent_data_len,
        .gfs_flags       = snap->gfs_flags,
    };

    status_t st = OK;
    if (write(fd, &fhdr, sizeof(fhdr)) != sizeof(fhdr)) { st = ERR_IO; goto fail; }
    if (snap->node_count > 0 &&
        write(fd, snap->nodes, snap->node_count * sizeof(node_t)) !=
            (ssize_t)(snap->node_count * sizeof(node_t))) { st = ERR_IO; goto fail; }
    if (snap->dirent_data_len > 0 &&
        write(fd, snap->dirent_data, snap->dirent_data_len) !=
            (ssize_t)snap->dirent_data_len) { st = ERR_IO; goto fail; }
    if (fsync(fd) == -1) { st = ERR_IO; goto fail; }
    close(fd); fd = -1;

    char dstpath[PATH_MAX];
    snap_path(repo, snap->snap_id, dstpath, sizeof(dstpath));
    if (rename(tmppath, dstpath) == -1) { st = ERR_IO; goto fail; }
    {
        char dirpath[PATH_MAX];
        snprintf(dirpath, sizeof(dirpath), "%s/snapshots", repo_path(repo));
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    return OK;
fail:
    if (fd >= 0) { close(fd); unlink(tmppath); }
    return st;
}

status_t snapshot_read_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t *out_flags) {
    char path[PATH_MAX];
    snap_path(repo, snap_id, path, sizeof(path));

    int fd = open(path, O_RDONLY);
    if (fd == -1) return ERR_IO;

    uint32_t magic = 0, version = 0;
    if (read(fd, &magic,   sizeof(magic))   != sizeof(magic)   ||
        read(fd, &version, sizeof(version)) != sizeof(version)) {
        close(fd); return ERR_CORRUPT;
    }
    if (magic != SNAP_MAGIC) { close(fd); return ERR_CORRUPT; }

    if (version == SNAP_VERSION1) {
        /* V1 has no gfs_flags field */
        close(fd);
        *out_flags = 0;
        return OK;
    }

    /* Skip past snap_id(4) + created_sec(8) + node_count(4) +
     * dirent_count(4) + dirent_data_len(8) = 28 bytes to reach gfs_flags */
    if (lseek(fd, 28, SEEK_CUR) == (off_t)-1) { close(fd); return ERR_IO; }

    uint32_t flags = 0;
    if (read(fd, &flags, sizeof(flags)) != sizeof(flags)) { close(fd); return ERR_CORRUPT; }
    close(fd);
    *out_flags = flags;
    return OK;
}

status_t snapshot_set_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t new_flags) {
    /* Load the full snap, OR in the new flags, rewrite atomically. */
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap->gfs_flags |= new_flags;
    st = snapshot_write(repo, snap);
    snapshot_free(snap);
    return st;
}

void snapshot_free(snapshot_t *snap) {
    if (!snap) return;
    free(snap->nodes);
    free(snap->dirent_data);
    free(snap);
}

status_t snapshot_read_head(repo_t *repo, uint32_t *out_id) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/refs/HEAD", repo_path(repo));
    FILE *f = fopen(path, "r");
    if (!f) return ERR_IO;
    unsigned long id = 0;
    if (fscanf(f, "%lu", &id) != 1) { fclose(f); return ERR_CORRUPT; }
    fclose(f);
    *out_id = (uint32_t)id;
    return OK;
}

status_t snapshot_write_head(repo_t *repo, uint32_t snap_id) {
    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s/tmp/HEAD.XXXXXX", repo_path(repo));
    int fd = mkstemp(tmppath);
    if (fd == -1) return ERR_IO;
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "%u\n", snap_id);
    if (write(fd, buf, n) != n) { close(fd); unlink(tmppath); return ERR_IO; }
    if (fsync(fd) == -1) { close(fd); unlink(tmppath); return ERR_IO; }
    close(fd);
    char dstpath[PATH_MAX];
    snprintf(dstpath, sizeof(dstpath), "%s/refs/HEAD", repo_path(repo));
    if (rename(tmppath, dstpath) == -1) { unlink(tmppath); return ERR_IO; }
    char dirpath[PATH_MAX];
    snprintf(dirpath, sizeof(dirpath), "%s/refs", repo_path(repo));
    int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }
    return OK;
}

const node_t *snapshot_find_node(const snapshot_t *snap, uint64_t node_id) {
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].node_id == node_id) return &snap->nodes[i];
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Path map implementation                                             */
/* ------------------------------------------------------------------ */

#define PM_LOAD_MAX_NUM 7
#define PM_LOAD_MAX_DEN 10   /* resize when count > capacity * 0.7 */

static uint64_t pm_fnv1a(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h;
}

static size_t pm_next_pow2(size_t n) {
    size_t p = 8;
    while (p < n) p <<= 1;
    return p;
}

static int pm_resize(pathmap_t *m) {
    size_t newcap = m->capacity * 2;
    pm_slot_t *newslots = calloc(newcap, sizeof(pm_slot_t));
    if (!newslots) return -1;
    for (size_t i = 0; i < m->capacity; i++) {
        if (!m->slots[i].key) continue;
        size_t h = (size_t)(pm_fnv1a(m->slots[i].key) & (uint64_t)(newcap - 1));
        while (newslots[h].key) h = (h + 1) & (newcap - 1);
        newslots[h] = m->slots[i];
    }
    free(m->slots);
    m->slots    = newslots;
    m->capacity = newcap;
    return 0;
}

static int pm_insert_node(pathmap_t *m, const char *key, const node_t *value) {
    if (m->count * PM_LOAD_MAX_DEN >= m->capacity * PM_LOAD_MAX_NUM) {
        if (pm_resize(m) != 0) return -1;
    }
    size_t h = (size_t)(pm_fnv1a(key) & (uint64_t)(m->capacity - 1));
    while (m->slots[h].key) {
        if (strcmp(m->slots[h].key, key) == 0) {
            m->slots[h].value = *value;
            return 0;
        }
        h = (h + 1) & (m->capacity - 1);
    }
    m->slots[h].key   = strdup(key);
    if (!m->slots[h].key) return -1;
    m->slots[h].value = *value;
    m->slots[h].seen  = 0;
    m->count++;
    return 0;
}

const node_t *pathmap_lookup(const pathmap_t *m, const char *path) {
    size_t h = (size_t)(pm_fnv1a(path) & (uint64_t)(m->capacity - 1));
    while (m->slots[h].key) {
        if (strcmp(m->slots[h].key, path) == 0) return &m->slots[h].value;
        h = (h + 1) & (m->capacity - 1);
    }
    return NULL;
}

void pathmap_mark_seen(pathmap_t *m, const char *path) {
    size_t h = (size_t)(pm_fnv1a(path) & (uint64_t)(m->capacity - 1));
    while (m->slots[h].key) {
        if (strcmp(m->slots[h].key, path) == 0) { m->slots[h].seen = 1; return; }
        h = (h + 1) & (m->capacity - 1);
    }
}

void pathmap_foreach_unseen(const pathmap_t *m,
                            void (*cb)(const char *path, const node_t *node, void *ctx),
                            void *ctx) {
    for (size_t i = 0; i < m->capacity; i++) {
        if (m->slots[i].key && !m->slots[i].seen)
            cb(m->slots[i].key, &m->slots[i].value, ctx);
    }
}

void pathmap_free(pathmap_t *m) {
    if (!m) return;
    for (size_t i = 0; i < m->capacity; i++) free(m->slots[i].key);
    free(m->slots);
    free(m);
}

/* ------------------------------------------------------------------ */
/* pathmap_build: reconstruct paths from the snapshot's dirent tree   */
/* ------------------------------------------------------------------ */

/* Temporary flat entry used during path reconstruction */
typedef struct {
    uint64_t parent_node_id;
    uint64_t node_id;
    char    *name;
    char    *full_path;   /* built incrementally */
} dr_flat_t;

static dr_flat_t *find_flat_by_id(dr_flat_t *arr, size_t n, uint64_t id) {
    for (size_t i = 0; i < n; i++)
        if (arr[i].node_id == id) return &arr[i];
    return NULL;
}

/* Recursively build the full path for a dirent entry. */
static char *build_full_path(dr_flat_t *arr, size_t n, dr_flat_t *e) {
    if (e->full_path) return e->full_path;   /* already built */

    if (e->parent_node_id == 0) {
        e->full_path = strdup(e->name);
        return e->full_path;
    }

    dr_flat_t *parent = find_flat_by_id(arr, n, e->parent_node_id);
    if (!parent) {
        e->full_path = strdup(e->name);
        return e->full_path;
    }

    char *parent_path = build_full_path(arr, n, parent);
    if (!parent_path) return NULL;

    size_t plen = strlen(parent_path);
    size_t nlen = strlen(e->name);
    char *fp = malloc(plen + 1 + nlen + 1);
    if (!fp) return NULL;
    memcpy(fp, parent_path, plen);
    fp[plen] = '/';
    memcpy(fp + plen + 1, e->name, nlen + 1);
    e->full_path = fp;
    return fp;
}

status_t pathmap_build(const snapshot_t *snap, pathmap_t **out) {
    size_t cap = pm_next_pow2(snap->node_count < 8 ? 16 : snap->node_count * 2);
    pathmap_t *m = calloc(1, sizeof(*m));
    if (!m) return ERR_NOMEM;
    m->slots    = calloc(cap, sizeof(pm_slot_t));
    m->capacity = cap;
    if (!m->slots) { free(m); return ERR_NOMEM; }

    /* Parse the dirent_data blob into a flat array */
    dr_flat_t *flat = calloc(snap->dirent_count, sizeof(dr_flat_t));
    if (!flat && snap->dirent_count > 0) { pathmap_free(m); return ERR_NOMEM; }
    size_t n_flat = 0;

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;

    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;

        char *name = malloc(dr.name_len + 1);
        if (!name) goto oom;
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;

        flat[n_flat].parent_node_id = dr.parent_node;
        flat[n_flat].node_id        = dr.node_id;
        flat[n_flat].name           = name;
        flat[n_flat].full_path      = NULL;
        n_flat++;
    }

    /* Reconstruct full paths and insert into map */
    for (size_t i = 0; i < n_flat; i++) {
        char *fp = build_full_path(flat, n_flat, &flat[i]);
        if (!fp) goto oom;

        const node_t *nd = snapshot_find_node(snap, flat[i].node_id);
        if (nd && pm_insert_node(m, fp, nd) != 0) goto oom;
    }

    for (size_t i = 0; i < n_flat; i++) {
        free(flat[i].name);
        free(flat[i].full_path);
    }
    free(flat);
    *out = m;
    return OK;

oom:
    for (size_t i = 0; i < n_flat; i++) { free(flat[i].name); free(flat[i].full_path); }
    free(flat);
    pathmap_free(m);
    return ERR_NOMEM;
}
