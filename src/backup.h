#pragma once

#include "error.h"
#include "repo.h"

/*
 * Run a full backup of source_paths[] into repo.
 * On success, HEAD is updated to the new snapshot ID.
 */
status_t backup_run(repo_t *repo, const char **source_paths, int path_count);

/* Extended backup with options. */
typedef struct {
    const char **exclude;      /* fnmatch patterns to skip (matched against basename) */
    int          n_exclude;
    int          no_pack;      /* skip automatic pack after backup (default: pack runs) */
    int          quiet;        /* suppress progress output to stderr */
    int          verify_after; /* verify all objects exist after committing snapshot */
} backup_opts_t;

status_t backup_run_opts(repo_t *repo, const char **source_paths, int path_count,
                         const backup_opts_t *opts);   /* opts may be NULL */
