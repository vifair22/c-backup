#pragma once

/*
 * Repo identity and registration.
 *
 * A repo directory is identified by a .cbackup marker holding a UUID. That
 * UUID, not the path, is what the daemon DB joins against, so a repo can be
 * moved on disk and re-registered without losing its history.
 *
 * See daemonization-spec.md sections 5.1 and 5.2.
 */

#include "error.h"
#include <cutils/db.h>
#include <stdint.h>

#define CBACKUP_MARKER      ".cbackup"
#define CBACKUP_FORMAT      "cbackup-repo"
#define CBACKUP_FORMAT_VER  1
#define CBACKUP_UUID_LEN    37   /* RFC-4122 text plus NUL */

typedef struct {
    char uuid[CBACKUP_UUID_LEN];
    int  version;
} repo_marker_t;

/* Read the marker from a repo directory. ERR_NOT_FOUND if absent,
 * ERR_CORRUPT if present but malformed or of an unknown format. */
status_t repo_marker_read(const char *path, repo_marker_t *out);

/* Write a fresh marker with a newly generated UUID. Refuses to overwrite. */
status_t repo_marker_write(const char *path, repo_marker_t *out);

/*
 * Register a repo directory, per section 5.2:
 *
 *   missing or empty  -> create the skeleton, write a marker, insert a row
 *   valid marker      -> re-link by UUID, inserting a row if the DB has none
 *                        (this is the DB-restore-with-path-drift case)
 *   non-empty, no marker -> refuse
 *
 * The last case matters: registering someone else's directory would mean
 * scribbling objects into it, and the marker is the only thing that
 * distinguishes "our repo" from "a directory that happens to be here".
 */
status_t repo_register(cutils_db_t *db, const char *path, const char *label,
                       int64_t *out_repo_id);

status_t repo_deregister(cutils_db_t *db, int64_t repo_id, int purge);
