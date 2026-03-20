#pragma once

#include "error.h"
#include <stddef.h>

/*
 * repo_t is an opaque handle to an open repository.
 * Use repo_open() / repo_close().
 */
typedef struct repo repo_t;

status_t repo_init(const char *path);
status_t repo_open(const char *path, repo_t **out);
void     repo_close(repo_t *repo);

/* Low-level helpers used by other modules. */
int         repo_fd(const repo_t *repo);   /* fd of repo root dir */
const char *repo_path(const repo_t *repo);

/* Pack index cache — opaque storage used by pack.c. */
void   repo_set_pack_cache(repo_t *repo, void *data, size_t cnt);
void  *repo_pack_cache_data(const repo_t *repo);
size_t repo_pack_cache_count(const repo_t *repo);

/*
 * Acquire / release an exclusive advisory lock on the repository.
 * repo_lock() fails immediately (ERR_IO) if another process holds the lock.
 * The lock is released automatically by repo_close().
 */
status_t repo_lock(repo_t *repo);
void     repo_unlock(repo_t *repo);
