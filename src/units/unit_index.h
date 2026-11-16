#pragma once

/*
 * snapshot_units and snapshot_unit_items: the derived index of which
 * snapshot is which unit's (docs/source-providers.md 7.4). Written by the
 * unit assembler in the snapshot row's transaction and rebuilt by reindex
 * from .cbackup-unit/unit.json in each snapshot root, so the database never
 * holds anything the repository does not say.
 */

#include "error.h"
#include "store.h"
#include "unit_json.h"

#include <cutils/db.h>
#include <stdint.h>

/* Replace the rows for one snapshot. Inside the caller's transaction. */
status_t unit_index_put(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                        const unit_json_t *u);

/* Drop them (a snapshot pruned, or found to be a file snapshot). */
status_t unit_index_delete(cutils_db_t *db, int64_t repo_id, int64_t snap_id);

/*
 * Classify one snapshot from its root tree, for reindex, inside its
 * transaction: no .cbackup-unit at the root is a file snapshot (rows
 * dropped); a readable unit.json gives the unit's rows; a .cbackup-unit
 * whose unit.json is missing, unreadable or of an unknown version is
 * indexed as unit_key 'unknown:<snap id>', which retention never prunes.
 * *out_kind (may be NULL) gets 0 file, 1 unit, 2 unknown. Only a failure
 * to read the root itself is an error.
 */
status_t unit_index_from_tree(store_t *store, cutils_db_t *db, int64_t repo_id,
                              int64_t snap_id, const uint8_t root[OBJECT_HASH_SIZE],
                              int *out_kind);

/* The unit key, "<provider>:<unit_id>", into out. */
void unit_key_format(char *out, size_t sz, const char *provider, const char *unit_id);
#define UNIT_KEY_MAX (SP_PROVIDER_MAX + SP_UNIT_ID_MAX + 1)
