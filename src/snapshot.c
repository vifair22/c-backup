#define _POSIX_C_SOURCE 200809L
#include "snapshot.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SNAP_MAGIC   0x43424B50u  /* "CBKP" */
#define SNAP_VERSION 1u

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t snap_id;
    uint32_t node_count;
    uint32_t dirent_count;
    uint64_t dirent_data_len;
} snap_file_header_t;

static int snap_path(repo_t *repo, uint32_t id, char *buf, size_t bufsz) {
    return snprintf(buf, bufsz, "%s/snapshots/%08u.snap", repo_path(repo), id) >= 0 ? 0 : -1;
}

status_t snapshot_load(repo_t *repo, uint32_t snap_id, snapshot_t **out) {
    char path[PATH_MAX];
    snap_path(repo, snap_id, path, sizeof(path));

    int fd = open(path, O_RDONLY);
    if (fd == -1) { log_msg("ERROR", "cannot open snapshot file"); return ERR_IO; }

    snap_file_header_t fhdr;
    if (read(fd, &fhdr, sizeof(fhdr)) != sizeof(fhdr)) { close(fd); return ERR_CORRUPT; }
    if (fhdr.magic != SNAP_MAGIC || fhdr.version != SNAP_VERSION) {
        close(fd); log_msg("ERROR", "invalid snapshot magic/version"); return ERR_CORRUPT;
    }

    snapshot_t *snap = calloc(1, sizeof(*snap));
    if (!snap) { close(fd); return ERR_NOMEM; }
    snap->snap_id      = fhdr.snap_id;
    snap->node_count   = fhdr.node_count;
    snap->dirent_count = fhdr.dirent_count;
    snap->dirent_data_len = (size_t)fhdr.dirent_data_len;

    snap->nodes = malloc(fhdr.node_count * sizeof(node_t));
    if (!snap->nodes && fhdr.node_count > 0) { free(snap); close(fd); return ERR_NOMEM; }
    if (fhdr.node_count > 0 &&
        read(fd, snap->nodes, fhdr.node_count * sizeof(node_t)) !=
            (ssize_t)(fhdr.node_count * sizeof(node_t))) {
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
        .node_count      = snap->node_count,
        .dirent_count    = snap->dirent_count,
        .dirent_data_len = snap->dirent_data_len,
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
    /* fsync snapshots dir */
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
    /* fsync refs/ dir */
    char dirpath[PATH_MAX];
    snprintf(dirpath, sizeof(dirpath), "%s/refs", repo_path(repo));
    int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }
    return OK;
}

const node_t *snapshot_find_node(const snapshot_t *snap, uint64_t node_id) {
    /* linear scan – can be replaced with a hash table if needed */
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].node_id == node_id) return &snap->nodes[i];
    }
    return NULL;
}
