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

#include "cdc.h"
#include "chunklist.h"
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
    uint64_t    size;
    /* Signed, because a file can predate 1970. On disk it is the same u64
     * field it always was, holding the two's complement bit pattern, so a
     * non-negative time serialises to exactly the bytes it always did. */
    int64_t     mtime_sec;
    uint64_t    mtime_nsec;
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
    /* MANIFEST_FLAG_*. A property of what was stored under this entry,
     * never of the snapshot, so the tree stays a function of the content. */
    uint8_t     flags;
} manifest_entry_t;

/*
 * The file never settled while the backup read it (OWN-45-LIVE): it was
 * still changing on the second read, which is what was stored. Those bytes
 * may mix two versions of the file, so restore warns and verify notes it.
 * Regular files only; the only flag there is.
 */
#define MANIFEST_FLAG_UNSETTLED 0x01u
#define MANIFEST_FLAGS_KNOWN    MANIFEST_FLAG_UNSETTLED

/*
 * Directories of any size (OWN-34).
 *
 * A directory's tree is one OBJECT_TYPE_TREE object while its serialised
 * form is at most MANIFEST_FRAG_MIN bytes, which is every directory most
 * trees hold (about 1,500 entries). A larger one is cut into fragments at
 * entry boundaries, each a complete TREE object in the same format holding
 * a run of the sorted entries, and the directory's hash names a
 * OBJECT_TYPE_CHUNKLIST whose leaves are those fragments in order, each
 * entry's offset and length its position in their concatenation. If the
 * cutting yields one fragment, that fragment is the directory.
 *
 * Cuts are content-defined so adding a file to a 200,000-entry directory
 * rewrites one fragment and the list, not the directory: once a fragment
 * holds MANIFEST_FRAG_MIN bytes, it ends after an entry whose
 * cdc_name_hash(name) is 0 modulo MANIFEST_FRAG_DIV, and it always ends
 * before an entry that would take it past MANIFEST_FRAG_MAX. Every
 * directory object is therefore bounded, and gc, reindex and restore read
 * any directory one fragment at a time.
 */
#define MANIFEST_FRAG_MIN ((size_t)256 * 1024)
#define MANIFEST_FRAG_MAX ((size_t)1024 * 1024)
#define MANIFEST_FRAG_DIV 1024u

typedef struct {
    uint64_t objects;        /* TREE fragments and lists handed to the sink */
    uint64_t objects_new;
    uint64_t phys_new;
    uint64_t fragments;      /* TREE objects: 1 for an ordinary directory */
} manifest_store_stats_t;

/*
 * Serialise one directory and store it through sink, fragmenting as above.
 * Entries are sorted in place. cp supplies the name hash (keyed in an
 * encrypted repository, like the chunker). stats may be NULL.
 */
status_t manifest_store_tree(manifest_entry_t *entries, uint32_t count,
                             const cdc_params_t *cp, const obj_sink_t *sink,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             manifest_store_stats_t *stats);

/*
 * manifest_store_tree into store's loose objects with the unkeyed default
 * parameters. Entries are sorted internally, so the caller need not
 * pre-sort and must not depend on input order surviving.
 */
status_t manifest_write_tree(store_t *store,
                             manifest_entry_t *entries, uint32_t count,
                             uint8_t out_hash[OBJECT_HASH_SIZE]);

/*
 * Read a directory one fragment at a time, whichever form it has.
 *
 * Entries come in name order; an entry is valid until the next call.
 * on_object (may be NULL) is told every object the directory consists of -
 * the root, every list, every fragment - as it is loaded, which is what gc
 * marks. Fragments are checked as they come: contiguous, each non-empty,
 * every fragment's first name after the previous one's last, and the list's
 * length matching what was read.
 */
typedef void (*manifest_object_fn)(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint8_t type);
typedef struct manifest_iter manifest_iter_t;

status_t manifest_iter_open(const obj_loader_t *loader,
                            const uint8_t hash[OBJECT_HASH_SIZE],
                            manifest_object_fn on_object, void *ctx,
                            manifest_iter_t **out);
status_t manifest_iter_next(manifest_iter_t *it, const manifest_entry_t **out,
                            int *done);
void     manifest_iter_close(manifest_iter_t *it);

/*
 * Parse one serialised TREE object (a whole small directory, or one
 * fragment). Split out so it can be driven with arbitrary bytes: everything
 * it reads came off a disk, and the hash check that would notice corruption
 * runs after it, not before.
 */
status_t manifest_parse(const void *buf, size_t len,
                        manifest_entry_t **out_entries, uint32_t *out_count);

/* Read a whole directory back, fragments joined, into one array. The
 * returned entries own their strings; free with manifest_free_entries.
 * Holds every entry in memory: prefer manifest_iter for a directory that
 * may be large. */
status_t manifest_read_tree(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            manifest_entry_t **out_entries, uint32_t *out_count);

void manifest_free_entries(manifest_entry_t *entries, uint32_t count);

/* Serialise without storing. Exposed so tests can assert byte-level
 * determinism directly rather than inferring it from hashes. */
status_t manifest_serialise(manifest_entry_t *entries, uint32_t count,
                            uint8_t **out_buf, size_t *out_len);
