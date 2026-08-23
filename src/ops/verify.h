#pragma once

/*
 * Verify: prove the repository still holds what it claims to.
 *
 * Every object carries a parity trailer (XOR over the header, CRC32C plus
 * Reed-Solomon over the payload) and the load path already uses it to correct
 * damage. But it corrects in memory, only for objects something happens to
 * read, and never writes the correction back. Rot in a pack nobody has touched
 * since it was written is invisible, and stays invisible until a restore needs
 * it. This is the operation that goes looking.
 *
 * Levels exist because the cost difference is three orders of magnitude and a
 * server wants a cheap check often and a full scrub rarely.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef enum {
    /* Does every registered object still resolve to a location? Catches
     * index drift and objects whose bytes have gone missing entirely.
     * No payload is read. */
    VERIFY_LEVEL_INDEX = 0,
    /* Additionally parse each object's header. Confirms the record is
     * structurally intact and readable without pulling its payload. */
    VERIFY_LEVEL_STRUCTURE,
    /* Read every payload and verify its SHA-256. This is the only check
     * that can actually prove the bytes are right; everything above it is
     * a cheaper proxy. */
    VERIFY_LEVEL_DEEP,
} verify_level_t;

typedef struct {
    uint64_t checked;
    uint64_t ok;
    /* Damaged on disk and corrected from parity. With repair enabled the
     * correction was written back; without it, the object was readable but
     * the disk copy is still wrong. */
    uint64_t repaired;
    /* Damaged beyond what parity can reconstruct. These are data loss. */
    uint64_t unrecoverable;
    /* Registered but absent: no loose file, no pack entry. */
    uint64_t missing;
    uint64_t bytes_read;
    /* Blast radius of everything unrecoverable or missing. */
    uint64_t affected_files;
    uint64_t affected_snapshots;
    int64_t  run_id;
} verify_result_t;

/*
 * repair writes corrections back to disk. The caller must hold the repo
 * write lock when it does, and may hold only the read lock when it does not.
 */
status_t verify_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    verify_level_t level, int repair, volatile int *cancel,
                    void (*progress)(const char *phase, uint64_t done,
                                     uint64_t total, void *ctx),
                    void *progress_ctx,
                    verify_result_t *out);

/* Parse a level name from the API. Unknown names return -1. */
int verify_level_from_string(const char *s, verify_level_t *out);
