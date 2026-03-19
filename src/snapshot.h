#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>

/*
 * In-memory snapshot representation.
 * node_table and dirent_table are flat arrays loaded from the .snap file.
 */
typedef struct {
    uint32_t    snap_id;
    uint32_t    node_count;
    uint32_t    dirent_count;
    node_t     *nodes;
    /* dirents are variable-size; stored as raw bytes */
    uint8_t    *dirent_data;
    size_t      dirent_data_len;
} snapshot_t;

status_t snapshot_load(repo_t *repo, uint32_t snap_id, snapshot_t **out);
status_t snapshot_write(repo_t *repo, snapshot_t *snap);
void     snapshot_free(snapshot_t *snap);

/* HEAD helpers */
status_t snapshot_read_head(repo_t *repo, uint32_t *out_id);
status_t snapshot_write_head(repo_t *repo, uint32_t snap_id);

/* Lookup helpers */
const node_t *snapshot_find_node(const snapshot_t *snap, uint64_t node_id);
