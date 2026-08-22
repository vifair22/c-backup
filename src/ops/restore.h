#pragma once

/*
 * Restore.
 *
 * Walks a snapshot's nodes out of the index and recreates them under a
 * destination directory. The index is used rather than the manifest trees
 * because it is ordered and indexed for exactly this traversal; the trees are
 * the recovery path when the index is gone (see reindex.h).
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    const char   *subpath;    /* restore only this path within the snapshot */
    int           overwrite;  /* replace existing files at the destination */
    volatile int *cancel;
    void (*progress_cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *progress_ctx;
} restore_opts_t;

typedef struct {
    uint64_t files;
    uint64_t dirs;
    uint64_t symlinks;
    uint64_t hardlinks;
    /* FIFOs and device nodes: recreated by mknod, no content involved. */
    uint64_t specials;
    uint64_t bytes;
    uint64_t skipped;
} restore_result_t;

status_t restore_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                     int64_t snap_id, const char *dest,
                     const restore_opts_t *opts, restore_result_t *out);
