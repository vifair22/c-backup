#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

status_t tag_set(repo_t *repo, const char *name, uint32_t snap_id);
status_t tag_get(repo_t *repo, const char *name, uint32_t *out_id);
status_t tag_delete(repo_t *repo, const char *name);
status_t tag_list(repo_t *repo);   /* prints name -> snap_id to stdout */

/*
 * Resolve either a decimal snap ID string or a tag name.
 * Tries numeric parse first; if that yields 0 tries tag lookup.
 */
status_t tag_resolve(repo_t *repo, const char *arg, uint32_t *out_id);
