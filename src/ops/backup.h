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
     * checks it between entries, and backup_run lends it to the store for
     * the run so the object writers check it between chunks of one file.
     * A cancel lands within one read, not at the end of a large file. */
    volatile int *cancel;

    void (*progress_cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *progress_ctx;
    /* Optional: bytes of regular-file content handled so far, and expected
     * in total, called alongside progress_cb with the same context. Entry
     * counts crawl through large files and say little about how far a
     * backup has to go; bytes do. */
    void (*bytes_cb)(void *ctx, uint64_t done, uint64_t total);
} backup_opts_t;

typedef struct {
    int64_t  snapshot_id;
    uint64_t files;
    uint64_t dirs;
    uint64_t bytes_logical;
    uint64_t objects_new;
    /* Files the cache vouched for, so never opened. On a healthy incremental
     * this is nearly everything, and it is the number to watch if backups
     * start taking as long as the first one did. */
    uint64_t files_unchanged;
    uint64_t trees_new;
    uint64_t trees_reused;
    /* Bytes this run added to the repository for new file content, before
     * packing compresses them. Everything else was already there. */
    uint64_t bytes_stored;
} backup_result_t;

status_t backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    const backup_opts_t *opts, backup_result_t *out);
