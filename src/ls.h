#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/*
 * List the contents of dir_path inside snapshot snap_id.
 * dir_path is repo-relative (e.g. "" or "." for root, "foo/bar" for a subdir).
 * Output goes to stdout in long format (like ls -l).
 * Returns ERR_INVALID if the path does not name a directory in that snapshot.
 */
status_t snapshot_ls(repo_t *repo, uint32_t snap_id, const char *dir_path);
