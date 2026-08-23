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
    /*
     * Not restored, and that is not the repository's fault: already present
     * at the destination with overwrite off, which is what a resumed restore
     * does to everything an earlier attempt wrote; or a device node the
     * kernel refused to create for an unprivileged process. No content was
     * lost - a device node has none.
     */
    uint64_t skipped;
    /*
     * The repository could not produce it: an object that would not read, a
     * path that could not be reconstructed, a symlink whose target was never
     * recorded. These are the ones an operator has to hear about, so
     * restore_run returns an error when any occurred - after finishing the
     * rest, because a partial restore is worth keeping and only worth
     * keeping if its gaps are known.
     */
    uint64_t failed;
} restore_result_t;

/*
 * Returns OK only when nothing failed. On ERR_CORRUPT the restore still ran
 * to the end and *out is filled in; what could be written was written.
 *
 * It used to return OK unconditionally. Restoring an encrypted repository
 * with no key then reported a completed job, no error, and an empty
 * destination directory - the failure mode this whole file is otherwise
 * careful about.
 */
status_t restore_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                     int64_t snap_id, const char *dest,
                     const restore_opts_t *opts, restore_result_t *out);
