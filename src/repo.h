#pragma once

#include "error.h"

/*
 * repo_t is an opaque handle to an open repository.
 * Use repo_open() / repo_close().
 */
typedef struct repo repo_t;

status_t repo_init(const char *path);
status_t repo_open(const char *path, repo_t **out);
void     repo_close(repo_t *repo);

/* Low-level helpers used by other modules. */
int      repo_fd(const repo_t *repo);   /* fd of repo root dir */
const char *repo_path(const repo_t *repo);
