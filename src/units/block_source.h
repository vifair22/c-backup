#pragma once

/*
 * A BLOCK item's extent stream as a chunker source (source-providers.md 4.2).
 *
 * Spans are defined by content, not by the allocation map (vm-backup.md
 * 5.2): a hole is a maximal run of zero bytes at least CDC_HOLE_MIN (64 KiB)
 * long, and everything else is data, chunked span by span. The extent map is
 * used only as a shortcut: a ZERO extent is known to read as zeroes and,
 * with trust_zero, is not read at all. Where a zero run starts and ends is
 * found by looking at the bytes, so the same guest content gives the same
 * chunk list whether it was read through extents or in full, raw or qcow2,
 * sparse or preallocated.
 *
 * Memory is one read window, whatever the size of the disk or of its
 * extent map (design constraints 1 to 3): spans reach the chunker in pieces
 * (cdc_source_t.next_piece), so no span end has to be found before its
 * bytes are handed over.
 */

#include "chunklist.h"
#include "error.h"
#include "provider.h"

#include <stdint.h>

typedef struct {
    /* Walk the extent map; 0 reads every byte as if all were DATA ("read
     * full", which is also what an audit does). */
    int use_extents;
    /* With use_extents: ZERO extents are not read. 0 reads them too, and
     * they are still classified by their bytes. */
    int trust_zero;
    /* Read window; 0 for the default (4 MiB). */
    size_t window;
    /*
     * Optional: every byte of the disk as it is classified, in order and
     * contiguous from 0 (buf NULL for a range known to be zero), for the
     * audit (vm-backup.md 5.4). Only meaningful for a forward read with no
     * seek; a non-OK return fails the read.
     */
    status_t (*tap)(void *ctx, uint64_t off, const uint8_t *buf, size_t len);
    void      *tap_ctx;
} block_source_opts_t;

typedef struct {
    uint64_t bytes_read;      /* fetched from the stream */
    uint64_t zero_skipped;    /* known zero from the map, never read */
    uint64_t hole_bytes;      /* in holes, by the zero-run rule */
} block_source_stats_t;

typedef struct block_source block_source_t;

status_t block_source_open(const sp_block_stream_t *bs, uint64_t size,
                           const block_source_opts_t *opts, block_source_t **out);
/* The chunker interface over it; valid until close. Its seek starts the
 * zero-run rule afresh at the offset given, which is what an incremental
 * store restarting at an old chunk start needs (vm-backup.md 5.3). */
cdc_source_t block_source_iface(block_source_t *b);
void         block_source_stats(const block_source_t *b, block_source_stats_t *out);
/* Frees the adapter. Does not close the stream. */
void         block_source_close(block_source_t *b);
