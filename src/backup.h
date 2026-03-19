#pragma once

#include "error.h"
#include "repo.h"

/*
 * Run a full backup of source_paths[] into repo.
 * On success, HEAD is updated to the new snapshot ID.
 */
status_t backup_run(repo_t *repo, const char **source_paths, int path_count);
