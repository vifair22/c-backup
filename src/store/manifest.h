#pragma once

/*
 * Manifest trees: the repo-side, self-describing record of what a snapshot
 * contains. One content-addressed tree object per directory, stored through
 * store_put_object like any other object, so it inherits compression, parity
 * and dedup from the existing engine.
 *
 * This replaces the .snap container that snapshot.c wrote. The daemon DB is a
 * derived index over these trees and can be rebuilt from them at any time,
 * which is what makes the repo recoverable without a DB backup.
 *
 * Phase 0 declares the surface. Phase 4 implements it.
 *
 * CRITICAL: serialisation must be deterministic or dedup silently degrades to
 * a full manifest rewrite per snapshot, with no visible failure. Measured at
 * 3.3% new tree objects per incremental snapshot (PERF.md 2026-08-19); if that
 * number ever climbs toward 100%, determinism has regressed. The rules, and
 * the reasoning, are in daemonization-spec.md section 5.4. In short: entries
 * sorted by memcmp byte order, fixed-width little-endian fields, and no
 * snapshot-scoped identifiers anywhere in a tree (no node_id, no parent_node,
 * no backup wall-clock time) since those change every run and would give every
 * directory a fresh hash.
 */

#include "store.h"
#include "types.h"
#include <stdint.h>

/* One entry in a directory's tree object. */
typedef struct {
    const char *name;
    uint16_t    name_len;
    uint8_t     type;               /* NODE_TYPE_* */
    uint32_t    mode, uid, gid;
    uint64_t    size, mtime_sec, mtime_nsec;
    uint64_t    inode_identity;     /* hardlink identity; not content identity */
    uint8_t     target_hash[OBJECT_HASH_SIZE];  /* content hash, or subtree hash for dirs */
    uint8_t     xattr_hash[OBJECT_HASH_SIZE];
    uint8_t     acl_hash[OBJECT_HASH_SIZE];
} manifest_entry_t;

/* Serialise one directory and store it. Entries are sorted internally; the
 * caller need not pre-sort, but must not rely on a particular input order. */
int manifest_write_tree(store_t *s, const manifest_entry_t *entries, uint32_t count,
                        uint8_t out_hash[OBJECT_HASH_SIZE]);

/* Read a tree object back. Used by reindex and by GC's mark phase. */
int manifest_read_tree(store_t *s, const uint8_t hash[OBJECT_HASH_SIZE],
                       manifest_entry_t **out_entries, uint32_t *out_count);
void manifest_free_entries(manifest_entry_t *entries, uint32_t count);
