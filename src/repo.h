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

/* Pack .dat file handle cache — avoids repeated open/close on bulk lookups. */
#include <stdio.h>
#include <stdint.h>
FILE  *repo_dat_cache_checkout(repo_t *repo, uint32_t pack_num);
void   repo_dat_cache_return(repo_t *repo, uint32_t pack_num, FILE *fp);
void   repo_dat_cache_flush(repo_t *repo);

/*
 * Exclusive lock — held during all write operations (run, prune, gc, pack).
 * Fails immediately (ERR_IO) if another writer holds the lock.
 * The lock is released automatically by repo_close().
 */
status_t repo_lock(repo_t *repo);
void     repo_unlock(repo_t *repo);

/*
 * Shared lock — held during read-only operations (restore, list, diff, verify,
 * stats, ls).  Blocks until any exclusive writer finishes.  Non-fatal: if the
 * lock cannot be acquired the operation proceeds with a warning.
 */
status_t repo_lock_shared(repo_t *repo);
