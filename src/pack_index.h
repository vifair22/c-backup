#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stdint.h>

/* ------------------------------------------------------------------ */
/* Global pack index: merges all per-pack .idx files into a single    */
/* mmap'd file with fanout table for O(1) range narrowing.            */
/* Supplements (does not replace) per-pack .idx files.                */
/* ------------------------------------------------------------------ */

#define PACK_INDEX_MAGIC   0x42504D49u  /* "BPMI" */
#define PACK_INDEX_VERSION 1u

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t entry_count;
    uint32_t pack_count;
} pack_index_hdr_t;  /* 16 bytes */

typedef struct __attribute__((packed)) {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint32_t pack_num;                 /*  4 */
    uint64_t dat_offset;               /*  8 */
    uint32_t pack_version;             /*  4 */
    uint32_t entry_index;              /*  4 — UINT32_MAX for pre-v3 packs */
} pack_index_entry_t;  /* 52 bytes */

/* Mmap'd global index handle */
typedef struct {
    void                       *map;
    size_t                      map_size;
    const pack_index_hdr_t     *hdr;
    const uint32_t             *fanout;   /* 256 entries */
    const pack_index_entry_t   *entries;
} pack_index_t;

/* Open the global pack index via mmap.  Returns NULL if missing,
 * stale, or corrupt (caller should fall back to legacy .idx scan). */
pack_index_t *pack_index_open(repo_t *repo);

/* Close and munmap. */
void pack_index_close(pack_index_t *idx);

/* Lookup a hash in the global index.  Returns pointer into mmap'd
 * entries on hit, NULL on miss.  O(log(n/256)) via fanout. */
const pack_index_entry_t *pack_index_lookup(
    const pack_index_t *idx, const uint8_t hash[OBJECT_HASH_SIZE]);

/* Rebuild the global index from all per-pack .idx files.
 * Writes atomically (tmp + fsync + rename).  Includes CRC32C +
 * RS parity trailer + parity_footer_t. */
status_t pack_index_rebuild(repo_t *repo);
