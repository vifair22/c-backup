#pragma once

/*
 * Manifest trees: the repo-side, self-describing record of what a snapshot
 * contains. One content-addressed tree object per directory, stored through
 * the object store like any other object, so it inherits compression, parity
 * and dedup from the existing engine.
 *
 * This replaces the .snap container. The daemon DB is a derived index over
 * these trees and can be rebuilt from them at any time, which is what makes
 * a repository recoverable without a database backup.
 *
 * CRITICAL: serialisation must be deterministic or dedup silently degrades to
 * a full manifest rewrite per snapshot, with no visible failure. Measured at
 * 3.3% new tree objects per incremental snapshot; if that ever climbs toward
 * 100%, determinism has regressed. The rules are in daemonization-spec.md
 * section 5.4 and enforced by tests/test_manifest_tree.c.
 */

#include "error.h"
#include "store.h"
#include "types.h"

#include <stdint.h>

/*
 * One entry in a directory's tree object.
 *
 * Note what is absent: no node_id, no parent_node_id, no backup timestamp.
 * Those are per-snapshot values, and including any of them would give every
 * directory a fresh hash on every run and destroy dedup outright. Children
 * are referenced by content hash only; node ids are assigned by the indexer
 * and live in the database.
 */
typedef struct {
    const char *name;
    uint16_t    name_len;
    uint8_t     type;                   /* NODE_TYPE_* */
    uint32_t    mode, uid, gid;
    uint64_t    size, mtime_sec, mtime_nsec;
    /* Hard-link identity, not content identity: two paths sharing an inode
     * are the same file, which is different from two files that happen to
     * hold the same bytes. Stable across backups of the same filesystem. */
    uint64_t    inode_identity;
    /* Character and block devices only. A device node has no content: the
     * pair of numbers is the whole file, so without them a restore can
     * recreate the name and the mode and nothing that makes it a device.
     * Zero for every other type, including on trees written before the
     * format carried them. */
    uint32_t    dev_major, dev_minor;
    uint8_t     target_hash[OBJECT_HASH_SIZE];  /* content hash; subtree hash for dirs */
    uint8_t     xattr_hash[OBJECT_HASH_SIZE];
    uint8_t     acl_hash[OBJECT_HASH_SIZE];
    const char *link_target;            /* symlinks only; NULL otherwise */
    uint16_t    link_target_len;
} manifest_entry_t;

/*
 * Serialise one directory and store it. Entries are sorted internally, so the
 * caller need not pre-sort and must not depend on input order surviving.
 */
status_t manifest_write_tree(store_t *store,
                             manifest_entry_t *entries, uint32_t count,
                             uint8_t out_hash[OBJECT_HASH_SIZE]);

/* Read a tree back. Used by reindex and by GC's mark phase. The returned
 * entries own their strings; free with manifest_free_entries. */
/*
 * Parse a serialised tree. Split out from manifest_read_tree so it can be
 * driven with arbitrary bytes: everything it reads came off a disk, and the
 * hash check that would notice corruption runs after it, not before.
 */
status_t manifest_parse(const void *buf, size_t len,
                        manifest_entry_t **out_entries, uint32_t *out_count);

status_t manifest_read_tree(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            manifest_entry_t **out_entries, uint32_t *out_count);

void manifest_free_entries(manifest_entry_t *entries, uint32_t count);

/* Serialise without storing. Exposed so tests can assert byte-level
 * determinism directly rather than inferring it from hashes. */
status_t manifest_serialise(manifest_entry_t *entries, uint32_t count,
                            uint8_t **out_buf, size_t *out_len);
