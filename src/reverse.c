#define _POSIX_C_SOURCE 200809L
#include "reverse.h"
#include "../vendor/log.h"

#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define REV_MAGIC   0x43425256u  /* "CBRV" */
#define REV_VERSION 1u

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t snap_id;
    uint32_t entry_count;
} rev_file_header_t;

static void rev_path(repo_t *repo, uint32_t id, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/reverse/%08u.rev", repo_path(repo), id);
}

status_t reverse_write(repo_t *repo, const rev_record_t *rec) {
    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s/tmp/rev.XXXXXX", repo_path(repo));
    int fd = mkstemp(tmppath);
    if (fd == -1) return ERR_IO;

    rev_file_header_t fhdr = {
        .magic       = REV_MAGIC,
        .version     = REV_VERSION,
        .snap_id     = rec->snap_id,
        .entry_count = rec->entry_count,
    };

    status_t st = OK;
    if (write(fd, &fhdr, sizeof(fhdr)) != sizeof(fhdr)) { st = ERR_IO; goto fail; }

    for (uint32_t i = 0; i < rec->entry_count; i++) {
        const rev_entry_t *e = &rec->entries[i];
        uint16_t path_len = (uint16_t)strlen(e->path);
        uint8_t op = e->op_type;
        if (write(fd, &op, 1) != 1) { st = ERR_IO; goto fail; }
        if (write(fd, &path_len, sizeof(path_len)) != sizeof(path_len)) { st = ERR_IO; goto fail; }
        if (write(fd, e->path, path_len) != path_len) { st = ERR_IO; goto fail; }
        if (write(fd, &e->prev_node, sizeof(node_t)) != sizeof(node_t)) { st = ERR_IO; goto fail; }
    }

    if (fsync(fd) == -1) { st = ERR_IO; goto fail; }
    close(fd); fd = -1;

    char dstpath[PATH_MAX];
    rev_path(repo, rec->snap_id, dstpath, sizeof(dstpath));
    if (rename(tmppath, dstpath) == -1) { st = ERR_IO; goto fail; }
    {
        char dirpath[PATH_MAX];
        snprintf(dirpath, sizeof(dirpath), "%s/reverse", repo_path(repo));
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    return OK;
fail:
    if (fd >= 0) { close(fd); unlink(tmppath); }
    return st;
}

status_t reverse_load(repo_t *repo, uint32_t snap_id, rev_record_t **out) {
    char path[PATH_MAX];
    rev_path(repo, snap_id, path, sizeof(path));
    int fd = open(path, O_RDONLY);
    if (fd == -1) return ERR_IO;

    rev_file_header_t fhdr;
    if (read(fd, &fhdr, sizeof(fhdr)) != sizeof(fhdr)) { close(fd); return ERR_CORRUPT; }
    if (fhdr.magic != REV_MAGIC || fhdr.version != REV_VERSION) {
        close(fd); return ERR_CORRUPT;
    }

    rev_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) { close(fd); return ERR_NOMEM; }
    rec->snap_id     = fhdr.snap_id;
    rec->entry_count = fhdr.entry_count;
    rec->entries     = calloc(fhdr.entry_count, sizeof(rev_entry_t));
    if (!rec->entries && fhdr.entry_count > 0) {
        free(rec); close(fd); return ERR_NOMEM;
    }

    for (uint32_t i = 0; i < fhdr.entry_count; i++) {
        rev_entry_t *e = &rec->entries[i];
        uint8_t op;
        uint16_t path_len;
        if (read(fd, &op, 1) != 1) goto corrupt;
        if (read(fd, &path_len, sizeof(path_len)) != sizeof(path_len)) goto corrupt;
        e->op_type = op;
        e->path = malloc(path_len + 1);
        if (!e->path) goto oom;
        if (read(fd, e->path, path_len) != path_len) goto corrupt;
        e->path[path_len] = '\0';
        if (read(fd, &e->prev_node, sizeof(node_t)) != sizeof(node_t)) goto corrupt;
        continue;
corrupt:
        close(fd); reverse_free(rec); return ERR_CORRUPT;
oom:
        close(fd); reverse_free(rec); return ERR_NOMEM;
    }

    close(fd);
    *out = rec;
    return OK;
}

void reverse_free(rev_record_t *rec) {
    if (!rec) return;
    for (uint32_t i = 0; i < rec->entry_count; i++) free(rec->entries[i].path);
    free(rec->entries);
    free(rec);
}
