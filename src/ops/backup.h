#pragma once

/*
 * Backup pipeline.
 *
 * Scan source paths, store file content, assemble manifest trees bottom-up,
 * write a snapshot record into the repo, and index the result into the
 * database. The repo half is authoritative; the database half is the index
 * that makes it queryable.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    const char **paths;      /* absolute source roots */
    int          n_paths;
    const char **exclude;
    int          n_exclude;
    int          one_filesystem;
    const char  *label;

    /* Cooperative cancellation. The job manager sets this; the pipeline
     * checks it between entries so a cancelled backup stops within one file
     * rather than at the end of the run. */
    volatile int *cancel;

    void (*progress_cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *progress_ctx;
} backup_opts_t;

typedef struct {
    int64_t  snapshot_id;
    uint64_t files;
    uint64_t dirs;
    uint64_t bytes_logical;
    uint64_t objects_new;
    uint64_t trees_new;
    uint64_t trees_reused;
} backup_result_t;

status_t backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    const backup_opts_t *opts, backup_result_t *out);
