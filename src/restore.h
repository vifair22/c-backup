#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/* Restore latest snapshot to dest_path. */
status_t restore_latest(repo_t *repo, const char *dest_path);

/* Restore a specific historical snapshot to dest_path. */
status_t restore_snapshot(repo_t *repo, uint32_t snap_id, const char *dest_path);

/* Restore a single file at file_path from snapshot snap_id. */
status_t restore_file(repo_t *repo, uint32_t snap_id,
                      const char *file_path, const char *dest_path);
