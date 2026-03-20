#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/*
 * Print the diff between two snapshots to stdout.
 * Works for both live and pruned snapshots.
 * Output lines: "A  path", "D  path", "M  path", "m  path"
 * (Added, Deleted, Modified-content, modified-Metadata-only)
 */
status_t snapshot_diff(repo_t *repo, uint32_t snap_id1, uint32_t snap_id2);
