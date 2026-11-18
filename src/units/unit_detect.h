#pragma once

/*
 * Database detection for units outside a backup (docs/db-detection.md 7.4,
 * 8.1): units.probe, and POST /repos/<id>/units/<p>/<u>/detect, which runs
 * the detector over a unit's included mounts now, without a capture.
 *
 * The detect run scans each TREE item where a live read of it would (the
 * item's host path), records what it finds at the recorded paths, and
 * reads only the first bytes of each regular file, through the same
 * beneath-the-root open the store uses (open_scanned.h). It writes
 * nothing in the repository and takes nothing on the unit: no pause, no
 * snapshot. Coverage is judged against the capture the unit's next backup
 * would make (a snapshot-domain tree atomic, a tree on no domain paused or
 * live), which is a prediction and is said to be one.
 */

#include "db_detect.h"
#include "error.h"
#include "provider.h"

#include <cutils/db.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/*
 * units.probe for detection: NULL when detection is off, the provider has
 * no probe, or it failed. A failure is logged against key and never
 * passed on: detection then proceeds on file signals alone (principle 5).
 */
sp_unit_probe_t *unit_detect_probe(const sp_provider_ops_t *ops, sp_ctx_t *ctx,
                                   const char *unit_id, const char *key);

/*
 * db_detect_nextcloud_unread's reader: the recorded path mapped onto the
 * read root of the tree holding it (rec[i] is read at rd[i]), opened
 * without following a symlink in its last component, and its first
 * DB_HEAD_MORE_BYTES handed to the detector. A file that cannot be read
 * is skipped.
 */
void unit_detect_reread(db_detector_t *d, const char *const *rec, const char *const *rd,
                        size_t n);

typedef struct {
    char             unit_key[SP_PROVIDER_MAX + SP_UNIT_ID_MAX + 2];
    char             name[256];
    db_findings_t   *findings;      /* the caller frees */
    uint64_t         files_read;    /* heads read */
    uint64_t         entries;       /* scanned */
    int              probed;        /* units.probe answered */
    unsigned         heads_kept;    /* file_cache.db_head rows filled */
} unit_detect_result_t;

/*
 * The detect run for one unit of provider prov, named by id or name. The
 * findings are judged, linked (db_findings_link_nextcloud) and returned,
 * not recorded: the caller records them with db_findings_record_detect.
 * Classifications of files the file cache already knows, unchanged since
 * (size, mtime and ctime as cached), are kept in file_cache.db_head so the
 * next backup need not read them again.
 */
status_t unit_detect_run(cutils_db_t *db, int64_t repo_id, const char *prov, const char *unit,
                         atomic_int *cancel,
                         void (*progress)(const char *phase, uint64_t done, uint64_t total, void *ctx),
                         void *progress_ctx, unit_detect_result_t *out);
