#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

typedef enum {
    XFER_SCOPE_SNAPSHOT = 1,
    XFER_SCOPE_REPO = 2,
} xfer_scope_t;

/* Export snapshot tree as tar.gz using restore materialization. */
status_t export_snapshot_targz(repo_t *repo, uint32_t snap_id, const char *output_path);

/* Export native bundle (.cbb), currently LZ4-compressed payload records. */
status_t export_bundle(repo_t *repo, xfer_scope_t scope, uint32_t snap_id,
                       const char *output_path);

/* Import native bundle (.cbb). */
status_t import_bundle(repo_t *repo, const char *input_path,
                       int dry_run, int no_head_update, int quiet);

/* Validate a bundle without importing it. */
status_t verify_bundle(const char *input_path, int quiet);
