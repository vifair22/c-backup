#pragma once

/*
 * Manifest trees from a scan, bottom-up.
 *
 * A directory's tree names its children by hash, so it can only be
 * serialised once every child directory has been, and the whole build has to
 * stay linear in entries (design constraint 3). Both are this module's job;
 * storing a serialised tree, recording it and reporting progress are the
 * caller's, through the callbacks below. Splitting it that way also lets a
 * test drive the real build against a synthetic scan with no store and no
 * database behind it.
 *
 * Every directory in the scan gets a tree, including one whose parent chain
 * does not reach a source root, and the synthetic root at the end ties
 * together the entries whose parent is 0. An entry whose parent id names
 * nothing in the scan, or names something that is not a directory, belongs
 * to no tree.
 */

#include "error.h"
#include "scan.h"
#include "types.h"

#include <stddef.h>
#include <stdint.h>

/* The index put() is given for the synthetic root. */
#define TREE_BUILD_ROOT UINT32_MAX

typedef struct {
    /*
     * Called before each directory with the number already built, and once
     * more with done == total before the synthetic root. Anything but OK
     * stops the build and is what tree_build returns.
     */
    status_t (*step)(void *ctx, uint32_t done, uint32_t total);
    /*
     * Store one serialised tree and name it into out_hash. index is the
     * directory's scan index, or TREE_BUILD_ROOT. For a directory out_hash
     * points into the hashes array, so the name is in place when put returns.
     */
    status_t (*put)(void *ctx, uint32_t index, const uint8_t *buf, size_t len,
                    uint8_t out_hash[OBJECT_HASH_SIZE]);
    void *ctx;
    /*
     * Where entry i's tree hash lives: hashes + i * stride, OBJECT_HASH_SIZE
     * bytes, written for directories only. Strided so the caller can keep
     * the hash inside a per-entry struct of its own instead of in a second
     * array of the scan's length.
     */
    uint8_t *hashes;
    size_t   stride;
} tree_build_ops_t;

typedef struct {
    /* Entries copied into a tree, summed over every tree built. Equal to
     * the number of entries that have a tree to belong to: each is looked
     * at once, which is the property the old per-directory rescan of the
     * whole scan lacked. */
    uint64_t entries_visited;
    /* Trees serialised, the synthetic root included. */
    uint32_t trees;
} tree_build_stats_t;

/* stats may be NULL. */
status_t tree_build(const scan_result_t *scan, const tree_build_ops_t *ops,
                    uint8_t root_hash[OBJECT_HASH_SIZE],
                    tree_build_stats_t *stats);
