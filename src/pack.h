#pragma once

#include "error.h"
#include "repo.h"
#include "object.h"
#include <stdint.h>

#define PACK_DAT_MAGIC   0x42504b44u   /* "BPKD" */
#define PACK_IDX_MAGIC   0x42504b49u   /* "BPKI" */
#define PACK_VERSION_V1  1u            /* compressed_size was uint32_t */
#define PACK_VERSION_V2  2u            /* compressed_size is uint64_t  */
#define PACK_VERSION     3u            /* parity trailers + idx entry_index */

/*
 * Pack all loose objects into a new pack file.
 * *out_packed (may be NULL) receives the number of objects packed.
 */
status_t repo_pack(repo_t *repo, uint32_t *out_packed);

/*
 * Complete any interrupted pack installs from a previous crash.
 * Call before repo_pack() to recover staging directories.
 */
void pack_resume_installing(repo_t *repo);

/* --- used by object.c -------------------------------------------- */

/*
 * Search all pack index files for hash.  If found, decompress and
 * return the payload in *out_data / *out_size.  Returns OK,
 * ERR_NOT_FOUND (not in any pack), ERR_TOO_LARGE (uncompressed object
 * exceeds STREAM_CHUNK — use pack_object_load_stream instead),
 * or ERR_IO / ERR_CORRUPT.
 */
status_t pack_object_load(repo_t *repo,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type);

/*
 * Stream a packed object to out_fd without buffering the full payload.
 * Safe for large COMPRESS_NONE objects that would OOM pack_object_load.
 */
status_t pack_object_load_stream(repo_t *repo,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 int out_fd,
                                 uint64_t *out_size,
                                 uint8_t *out_type);

/* Returns 1 if hash lives in any pack file, 0 otherwise. */
int pack_object_exists(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]);

/* Return packed physical bytes (entry header + compressed payload). */
status_t pack_object_physical_size(repo_t *repo,
                                   const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint64_t *out_bytes);

/* Return uncompressed size and type for a packed object without loading payload. */
status_t pack_object_get_info(repo_t *repo,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_uncompressed_size,
                              uint8_t *out_type);

/* Drop the cached pack index so it is reloaded on next access. */
void pack_cache_invalidate(repo_t *repo);

/*
 * Attempt to repair a packed object's entry in-place via pwrite().
 * Returns: >0 = bytes corrected, 0 = no corruption found, -1 = error.
 */
int pack_object_repair(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Rewrite any pack files that contain unreferenced objects, keeping only
 * entries whose hash appears in the sorted refs array (refs_cnt entries of
 * OBJECT_HASH_SIZE bytes each).  Called by repo_gc after collecting refs.
 * *out_kept / *out_deleted (may be NULL) receive aggregate counts.
 */
status_t pack_gc(repo_t *repo,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted);

/* ------------------------------------------------------------------ */
/* On-disk pack structures (shared between pack.c and json_api.c)      */
/* ------------------------------------------------------------------ */

/* Pack data file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_dat_hdr_t;

/* Per-object header inside the .dat body — v2/v3 (current) */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint64_t compressed_size;          /*  8 */
} __attribute__((packed)) pack_dat_entry_hdr_t;  /* 50 bytes */

/* V1 entry header — used only when reading existing v1 packs */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint32_t compressed_size;          /*  4 */
} __attribute__((packed)) pack_dat_entry_hdr_v1_t;  /* 46 bytes */

/* Pack index file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_idx_hdr_t;

/* On-disk index entry v2 (40 bytes, sorted by hash) */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint64_t dat_offset;               /*  8 */
} __attribute__((packed)) pack_idx_disk_entry_v2_t;

/* On-disk index entry v3 (44 bytes, sorted by hash) — current write format */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint64_t dat_offset;               /*  8 */
    uint32_t entry_index;              /*  4 */
} __attribute__((packed)) pack_idx_disk_entry_t;

/* ------------------------------------------------------------------ */
/* Pack enumeration (read-only iteration)                              */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;
    uint8_t  compression;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint64_t payload_offset;
} pack_entry_info_t;

typedef void (*pack_dat_entry_cb)(const pack_entry_info_t *info, void *ctx);

status_t pack_enumerate_dat(repo_t *repo, const char *dat_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_dat_entry_cb cb, void *ctx);

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t dat_offset;
    uint32_t entry_index;
} pack_idx_info_t;

typedef void (*pack_idx_entry_cb)(const pack_idx_info_t *info, void *ctx);

status_t pack_enumerate_idx(repo_t *repo, const char *idx_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_idx_entry_cb cb, void *ctx);
