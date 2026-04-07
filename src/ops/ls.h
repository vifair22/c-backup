#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <limits.h>
#include <stdint.h>

/*
 * List the contents of dir_path inside snapshot snap_id.
 * dir_path is repo-relative (e.g. "" or "." for root, "foo/bar" for a subdir).
 * Output goes to stdout in long format (like ls -lh).
 * Returns ERR_INVALID if the path does not name a directory in that snapshot.
 */
status_t snapshot_ls(repo_t *repo, uint32_t snap_id, const char *dir_path,
                     int recursive, char type_filter, const char *name_glob);

/*
 * Structured ls result — core logic separated from presentation.
 */
typedef struct {
    char   *name;             /* display name (relative to queried dir) */
    node_t  node;             /* copy of the node metadata */
    char    symlink_target[256];  /* empty if not symlink */
} ls_entry_t;

typedef struct {
    ls_entry_t *entries;
    uint32_t    count;
} ls_result_t;

status_t snapshot_ls_collect(repo_t *repo, uint32_t snap_id, const char *dir_path,
                              int recursive, char type_filter, const char *name_glob,
                              ls_result_t **out);
void     ls_result_free(ls_result_t *r);

/*
 * Fast filename search: case-insensitive substring match on dirent names.
 * Only builds full paths for matches.  Returns results up to max_results.
 */
typedef struct {
    char   *path;       /* full repo-relative path */
    node_t  node;
} search_hit_t;

typedef struct {
    search_hit_t *hits;
    uint32_t      count;
    uint32_t      total;      /* total matches before cap */
} search_result_t;

status_t snapshot_search(repo_t *repo, uint32_t snap_id,
                          const char *query, uint32_t max_results,
                          search_result_t **out);
void     search_result_free(search_result_t *r);

/*
 * Batch multi-snapshot search: shares dirent parsing across snapshots
 * with identical dirent data.  Calls hit_cb for each match; the callback's
 * hit->path is borrowed and must be copied if retained.
 */
status_t snapshot_search_multi(repo_t *repo, const uint32_t *snap_ids,
                                uint32_t n_snaps, const char *query,
                                uint32_t max_results,
                                void (*hit_cb)(uint32_t snap_id,
                                               const search_hit_t *hit,
                                               void *ctx),
                                void *ctx);
