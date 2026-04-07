#pragma once

#include "error.h"
#include "repo.h"
#include <stddef.h>
#include <stdint.h>

/*
 * Tag file format (repo/tags/<name>):
 *   id = <snap_id>
 *   preserve = true|false
 *
 * Tags with preserve=true are skipped (with a warning) by prune operations.
 */

status_t tag_set(repo_t *repo, const char *name, uint32_t snap_id, int preserve);
status_t tag_get(repo_t *repo, const char *name, uint32_t *out_id);
status_t tag_delete(repo_t *repo, const char *name);
status_t tag_list(repo_t *repo);   /* prints name -> snap_id [preserved] to stdout */

/*
 * Returns 1 if snap_id is protected by at least one preserved tag, and writes
 * the first matching tag name into name_out (if non-NULL).  Returns 0 otherwise.
 */
int tag_snap_is_preserved(repo_t *repo, uint32_t snap_id,
                          char *name_out, size_t name_sz);

/*
 * Resolve either a decimal snap ID string, the literal HEAD, or a tag name.
 * Tries numeric parse first; if that yields 0 tries tag lookup.
 */
status_t tag_resolve(repo_t *repo, const char *arg, uint32_t *out_id);
