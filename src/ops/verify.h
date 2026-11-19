#pragma once

/*
 * Verify: prove the repository still holds what it claims to.
 *
 * Every object sits in a pack under Reed-Solomon parity striped over the
 * pack's body, with a CRC per entry, and the load path already uses it to
 * correct damage. But it corrects in memory, only for objects something happens to
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

#define VERIFY_DROPPED_LIST_MAX 16u

typedef struct {
    uint64_t checked;
    uint64_t ok;
    /* Damaged on disk, corrected from parity, and the correction written
     * back; or beyond parity and the index moved to a proven duplicate
     * (NIT-31). Only a repairing run can count anything here. Includes
     * pack_structures_repaired and packs_restored (NIT-30). */
    uint64_t repaired;
    /* Damaged on disk and readable only through parity, with the disk copy
     * still wrong: repair was off, or the write-back failed; or beyond
     * parity with a proven duplicate the index does not name yet. Not loss
     * yet, but one more bad byte away from it. Includes
     * pack_structures_recoverable. */
    uint64_t recoverable;
    /* Damaged beyond what parity can reconstruct. These are data loss. */
    uint64_t unrecoverable;
    /* Registered but absent: no pack entry, or its pack file is gone. */
    uint64_t missing;
    uint64_t bytes_read;
    /* Blast radius of everything unrecoverable or missing: distinct file
     * paths, and distinct snapshots holding any of them. */
    uint64_t affected_files;
    uint64_t affected_snapshots;
    /* Packs whose entry headers cannot be walked even with parity, which gc
     * leaves untouched and reports (B119): a finding at every level, and a
     * failed run, until a repairing run restores them from the index. */
    uint64_t packs_unwalkable;
    /* Packs whose own header, tail copies or CRC table disagreed with their
     * redundancy (B136): rewritten by a repairing run, otherwise counted as
     * recoverable. Not loss. Each is also one item in checked and in
     * repaired or recoverable, which is what the run record keeps. */
    uint64_t pack_structures_repaired;
    uint64_t pack_structures_recoverable;
    /* Packs a repairing run made walkable again by rebuilding headers. */
    uint64_t packs_restored;
    /* Packs a repairing run could not rebuild - a damaged header had no
     * index row - and salvaged instead: every indexed entry copied into a
     * new pack, damaged ones as they were (still reported as loss by a deep
     * walk), and the bytes no row covers dropped. Those are unreferenced,
     * garbage gc had swept; their ranges in the old pack are listed. */
    uint64_t packs_salvaged;
    /* Of packs_unwalkable, those only the key can unblock: the repository
     * is encrypted, its key is not loaded, and salvage decrypts. */
    uint64_t packs_need_key;
    uint64_t unreferenced_dropped_bytes;
    uint64_t unreferenced_dropped_ranges;
    uint32_t dropped_listed;
    struct { uint32_t pack; uint64_t offset, length; } dropped[VERIFY_DROPPED_LIST_MAX];
    /* File entries stored while still changing (OWN-45-LIVE), reported as a
     * notice finding. Not loss: they never fail the run. */
    uint64_t unsettled_files;
    int64_t  run_id;
} verify_result_t;

/*
 * repair writes corrections back to disk. The caller must hold the repo
 * write lock when it does, and may hold only the read lock when it does not.
 *
 * Returns ERR_CORRUPT when the walk finished but found unrecoverable or
 * missing objects, or a pack that cannot be walked. The run record and findings are written first, and *out
 * is filled, exactly as for a clean run.
 */
status_t verify_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    verify_level_t level, int repair, atomic_int *cancel,
                    void (*progress)(const char *phase, uint64_t done,
                                     uint64_t total, void *ctx),
                    void *progress_ctx,
                    verify_result_t *out);

/*
 * Clear a repository's storage sync latch (sync_latch.h) once its disk has
 * been fixed: drop the repository's clean pages from the page cache, so
 * what follows reads the disk and not what a failed sync left in memory,
 * run a deep verify that writes nothing, and clear the latch only if it
 * found nothing at all - no loss, nothing recoverable, no pack structure
 * to fix, no unwalkable pack. Anything found leaves the latch in place and
 * fails, saying so: a repairing verify comes first. ERR_INVALID when the
 * repository is not latched; the daemon-wide latch refuses outright. *out
 * is the verify's result. The caller holds the repo write lock.
 */
status_t repo_sync_repair(store_t *store, cutils_db_t *db, int64_t repo_id, atomic_int *cancel,
                          void (*progress)(const char *phase, uint64_t done,
                                           uint64_t total, void *ctx),
                          void *progress_ctx, verify_result_t *out);

/* Parse a level name from the API. Unknown names return -1. */
int verify_level_from_string(const char *s, verify_level_t *out);

/*
 * The node rules every level runs, as WHERE clauses over "nodes n" with the
 * repo id as ?1. Exposed so a test can hold their query plans to what keeps
 * them linear (B268): neither may visit node rows that do not match.
 */
typedef enum {
    VERIFY_RULE_CONTENTLESS = 0,
    VERIFY_RULE_UNSETTLED,
} verify_rule_t;

const char *verify_rule_condition(verify_rule_t rule);
