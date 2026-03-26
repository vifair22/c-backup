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
