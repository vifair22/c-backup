#pragma once

/*
 * Incremental tokens (docs/vm-backup.md 5.1, 5.4, 5.8;
 * docs/source-providers.md 7.4): which checkpoint describes which
 * snapshot's content for which disk, and whether a unit may read
 * incrementally at all.
 *
 * Daemon-only state. A token binds only while its snapshot still exists,
 * still names the recorded content at the disk's path, and the disk is
 * still the same disk (identity). Anything else is a full read with a
 * reason, never an incremental against the wrong base. A lost table costs
 * one full read per disk.
 */

#include "error.h"
#include "provider.h"
#include "store.h"
#include "types.h"

#include <cutils/db.h>
#include <limits.h>
#include <stdint.h>
#include <time.h>

#define UNIT_TOKEN_MAX     128
#define UNIT_IDENTITY_MAX  (PATH_MAX + 128)

typedef struct {
    char     item_id[SP_ITEM_ID_MAX];
    char     token[UNIT_TOKEN_MAX];
    int64_t  snap_id;
    uint8_t  root[OBJECT_HASH_SIZE];
    char     identity[UNIT_IDENTITY_MAX];
} unit_token_t;

/*
 * A disk's identity as the token records it: the resolved source path, the
 * format, the virtual size and the image file's inode, "|"-separated. A
 * source that cannot be resolved or stat'ed is recorded as it is with "-"
 * for the inode, the same way every time, so only a real change differs.
 */
void unit_identity(const sp_item_desc_t *it, char *buf, size_t sz);

/* Which part of two identities differs: "item-resized" when the size does,
 * "item-source-changed" for the path, format or inode, NULL when equal. */
const char *unit_identity_diff(const char *was, const char *now);

/* The row for one disk; *found 0 when there is none. */
status_t unit_token_get(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                        const char *item_id, unit_token_t *out, int *found);

/* Insert or replace. Runs inside the caller's transaction when it has one
 * (the snapshot row's, through the index hook). */
status_t unit_token_put(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                        const char *item_id, const char *token, const char *blob,
                        int64_t snap_id, const uint8_t root[OBJECT_HASH_SIZE],
                        const char *identity);

/* Every row of the unit, or every row but those for the items listed. */
status_t unit_tokens_drop(cutils_db_t *db, int64_t repo_id, const char *unit_key);
/* Every row of the unit in every repository: its disks were replaced in
 * place (B251), so no checkpoint of any chain describes them any more. */
status_t unit_tokens_drop_everywhere(cutils_db_t *db, const char *unit_key);
status_t unit_tokens_drop_except(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                                 const char *const *keep, size_t n_keep);

/* Rows of every unit of a provider, for the start-up redefine step. cb gets
 * the unit id (the part after "<provider>:") and the token; anything but
 * OK stops and is returned. */
status_t unit_tokens_each(cutils_db_t *db, const char *provider,
                          status_t (*cb)(void *ctx, int64_t repo_id, const char *unit_id,
                                         const char *token),
                          void *ctx);

/*
 * Whether a token binds for this disk now (5.4, the token row layer). *ok
 * 1, or 0 with *reason from the read-mode vocabulary: base-pruned (the
 * snapshot is gone, or no longer names the recorded content at the disk's
 * path), item-resized or item-source-changed (identity).
 */
status_t unit_token_binds(store_t *store, cutils_db_t *db, int64_t repo_id,
                          const unit_token_t *t, const char *identity_now,
                          int *ok, const char **reason);

/* ---- per unit ---- */

typedef struct {
    int64_t last_audit_at;
    char    disabled_reason[512];
    int64_t disabled_at;
} unit_incr_state_t;

status_t unit_incr_get(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                       unit_incr_state_t *out);
status_t unit_incr_set_audit(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                             int64_t when);
/* Incrementals off for the unit. Its rows stay, so the checkpoint they
 * name is handed over to be retired, but no base is taken while it is off. */
status_t unit_incr_disable(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                           const char *reason);
/* reset-incremental: the reason cleared and every token made unusable
 * (no snapshot), so the next session is a full read that starts a fresh
 * chain and retires the old checkpoint. */
status_t unit_incr_reset(cutils_db_t *db, int64_t repo_id, const char *unit_key);
