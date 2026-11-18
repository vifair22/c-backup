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
    int           strip_subpath; /* the subpath's contents go straight into dest */
    int           overwrite;  /* replace existing files at the destination */
    atomic_int *cancel;
    void (*progress_cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *progress_ctx;
    /* Optional: bytes of file content restored or skipped so far, and in
     * total, called alongside progress_cb with the same context. */
    void (*bytes_cb)(void *ctx, uint64_t done, uint64_t total);
    /*
     * A database with an engine copy in the snapshot (db-detection.md 9.3,
     * 9.5) is restored from the copy, at its original path, and its stale
     * -wal, -shm and -journal are not restored: a backup-API copy is a
     * complete database, and the old WAL beside it would corrupt it.
     */
    int   prefer_engine_copy;
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
     * at the destination as the snapshot's version (same size, mtime within
     * two seconds) with overwrite off, which is what a resumed restore does
     * to everything an earlier attempt wrote; or a device node the kernel
     * refused to create for an unprivileged process. No content was lost -
     * a device node has none.
     */
    uint64_t skipped;
    /*
     * Regular files not restored because something else is at the path -
     * another size or mtime, a directory, a symlink - and overwrite is off.
     * What is there is the operator's and stays; the snapshot's version is
     * not in the destination, which is why this is not counted as skipped.
     * Not a failure: restoring into a live tree without overwrite, to bring
     * back what was deleted, keeps every changed file by design.
     */
    uint64_t kept;
    /*
     * The repository could not produce it: an object that would not read, a
     * path that could not be reconstructed, a symlink whose target was never
     * recorded. These are the ones an operator has to hear about, so
     * restore_run returns an error when any occurred - after finishing the
     * rest, because a partial restore is worth keeping and only worth
     * keeping if its gaps are known.
     */
    uint64_t failed;
    /* The first failed entry and why, "<path>: <reason>"; empty when none
     * failed. Every one is logged with its path and reason; the error
     * restore_run returns quotes this one. */
    char     first_failure[512];
    /*
     * Engine copies (db-detection.md 9.5): databases restored in scope that
     * have one in the snapshot, those restored from it (prefer_engine_copy),
     * and the -wal, -shm and -journal files left out beside them.
     */
    uint64_t engine_copies;
    uint64_t engine_copies_used;
    uint64_t engine_siblings_left_out;
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
