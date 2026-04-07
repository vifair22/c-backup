#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stdint.h>

/*
 * Print the diff between two snapshots to stdout.
 * Works for snapshots with on-disk manifests.
 * Output lines: "A  path", "D  path", "M  path", "m  path"
 * (Added, Deleted, Modified-content, modified-Metadata-only)
 */
status_t snapshot_diff(repo_t *repo, uint32_t snap_id1, uint32_t snap_id2);

/*
 * Structured diff result — core logic separated from presentation.
 * snapshot_diff_collect() returns the full change set; callers format output.
 */
typedef struct {
    char    change;    /* 'A', 'D', 'M', 'm' */
    char   *path;
    node_t  old_node;  /* zeroed for 'A' */
    node_t  new_node;  /* zeroed for 'D' */
} diff_change_t;

typedef struct {
    diff_change_t *changes;
    uint32_t       count;
    uint32_t       capacity;
} diff_result_t;

status_t snapshot_diff_collect(repo_t *repo, uint32_t snap_id1, uint32_t snap_id2,
                                diff_result_t **out);
void     diff_result_free(diff_result_t *r);
