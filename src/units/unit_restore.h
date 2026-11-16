#pragma once

/*
 * Restoring a unit snapshot without a provider (source-providers.md 12):
 *
 *   files  the ordinary restore of the snapshot (or a subtree) into a
 *          directory: each disk becomes a sparse raw file under
 *          .cbackup-unit/disks/, each tree lands at its recorded path
 *          under the destination
 *   item   one item: a disk or blob into one file, written to
 *          <dest>.cbk-partial with its holes as holes, synced and renamed
 *          into place, so a failed restore never leaves a plausible image
 *          (vm-backup.md 7.1, raw); a tree into a directory
 *
 * qcow2, block devices and defining the unit again need a provider and
 * come with it.
 */

#include "error.h"
#include "restore.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

/* 0 when snap_id is a unit snapshot, with its unit key; ERR_NOT_FOUND
 * (and why) when it is a file snapshot or unknown. */
status_t unit_snapshot_key(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           char *key, size_t ksz);

typedef struct {
    uint64_t bytes;           /* logical size of what was written */
    char     path[4096];      /* the file or directory written */
} unit_restore_item_result_t;

/* The content of a disk (block) item, for the NBD export: its content
 * hash and logical size. ERR_NOT_FOUND for an unknown item, ERR_INVALID
 * for one that is not a disk. */
status_t unit_item_content(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           const char *item_id, uint8_t hash[OBJECT_HASH_SIZE], uint64_t *size);

/* One item by id. dest must not exist. */
status_t unit_restore_item(store_t *store, cutils_db_t *db, int64_t repo_id,
                           int64_t snap_id, const char *item_id, const char *dest,
                           const restore_opts_t *ropts, unit_restore_item_result_t *out);
