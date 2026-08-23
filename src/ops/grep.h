#pragma once

/*
 * Content search across a snapshot.
 *
 * The pre-daemon CLI had this and the rewrite did not bring it back: the only
 * search that survived matches filenames. Finding which backup still holds a
 * config line, a key, or a paragraph someone deleted means reading the
 * content, and the alternative today is restoring a snapshot somewhere and
 * running grep(1) over it.
 *
 * It runs in the daemon rather than the client on purpose. The daemon is the
 * only side that can resolve an object, and once repositories are encrypted
 * it will be the only side that can read one, so searching here is the shape
 * that keeps working.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    const char *pattern;      /* POSIX extended regular expression */
    const char *path_prefix;  /* restrict to a subtree; NULL for all */
    int         ignore_case;
    /* Stop after this many matching files. 0 means no limit. A search that
     * has found what it was looking for should not read the rest of the
     * repository. */
    uint32_t    max_files;
    /* Sample matching lines recorded per file. Only the first few are worth
     * keeping: a hit list is for finding the file, not for reading it. */
    uint32_t    max_hits_per_file;
} grep_opts_t;

typedef struct {
    uint64_t files_scanned;
    uint64_t files_skipped;   /* unreadable or without content */
    uint64_t files_matched;
    uint64_t bytes_scanned;
    uint64_t hits;
    int      truncated;       /* stopped early on max_files */
    int64_t  run_id;
} grep_result_t;

status_t grep_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                  int64_t snap_id, const grep_opts_t *opts,
                  volatile int *cancel,
                  void (*progress)(const char *phase, uint64_t done,
                                   uint64_t total, void *ctx),
                  void *progress_ctx,
                  grep_result_t *out);
