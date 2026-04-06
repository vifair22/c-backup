#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>

/*
 * Hash data and store it as an object in the repo.
 * out_hash must point to OBJECT_HASH_SIZE bytes.
 * Returns OK if newly written or already existed.
 */
status_t object_store(repo_t *repo, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]);

status_t object_store_ex(repo_t *repo, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes);

/*
 * Store a regular file, automatically detecting sparse regions.
 * Stores as OBJECT_TYPE_SPARSE if the file has holes, OBJECT_TYPE_FILE
 * otherwise.  Caller passes the fd (already open O_RDONLY) and file size.
 */
status_t object_store_file(repo_t *repo, int fd, uint64_t file_size,
                           uint8_t out_hash[OBJECT_HASH_SIZE]);

status_t object_store_file_ex(repo_t *repo, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes);

/* Per-chunk progress callback invoked from the write path after each ~16 MiB
 * chunk is written.  chunk_bytes is the bytes in that chunk; ctx is caller-
 * supplied.  May be NULL (no callback). */
typedef void (*xfer_progress_fn)(uint64_t chunk_bytes, void *ctx);

/* Like object_store_file_ex but fires cb(chunk_bytes, cb_ctx) per chunk.
 * Useful for sub-second progress updates during large-file transfers.
 * If src_path is non-NULL the function takes ownership of fd (closes it)
 * and may reopen the file on transient read errors to recover mid-stream. */
status_t object_store_file_cb(repo_t *repo, int fd, const char *src_path,
                              uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes,
                              xfer_progress_fn cb, void *cb_ctx);

/*
 * Load object data by hash.  Caller must free(*out_data).
 * out_size is the uncompressed payload size.
 * out_type (may be NULL) receives the object type byte.
 */
status_t object_load(repo_t *repo,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type);

/* Check whether an object already exists (avoids re-hashing). */
int object_exists(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Stream object content to out_fd without buffering the entire payload in RAM.
 * Safe for large OBJECT_TYPE_FILE objects that would fail object_load with
 * ERR_TOO_LARGE.  out_size and out_type may be NULL.
 */
status_t object_load_stream(repo_t *repo,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             int out_fd,
                             uint64_t *out_size,
                             uint8_t *out_type);

/*
 * Load at most max_bytes of an object's uncompressed content.
 * Avoids decompressing the entire payload for large objects.
 * *out_full_size receives the total uncompressed size.
 * *out_size receives the bytes actually returned (≤ max_bytes).
 * Hash verification is skipped (partial data can't be hashed).
 */
status_t object_load_prefix(repo_t *repo,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             size_t max_bytes,
                             void **out_data, size_t *out_size,
                             uint64_t *out_full_size,
                             uint8_t *out_type);

/* Hex-encode a hash.  buf must be at least OBJECT_HASH_SIZE*2+1 bytes. */
void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf);

/*
 * Read object size and type from the header without loading the payload.
 * Works for both loose and packed objects.
 */
status_t object_get_info(repo_t *repo,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint64_t *out_size, uint8_t *out_type);

/*
 * Stream `size` bytes from src_fd into the object store as type `type`,
 * stored uncompressed (COMPRESS_NONE).  Verifies SHA-256 against expected_hash.
 * Used by the xfer importer for large objects that exceed INT_MAX.
 */
status_t object_store_fd(repo_t *repo, uint8_t type, int src_fd, uint64_t size,
                          const uint8_t expected_hash[OBJECT_HASH_SIZE]);

/*
 * Return on-disk physical bytes used by a single object payload record.
 * For loose objects: object_header_t + compressed payload bytes.
 * For packed objects: pack_dat_entry_hdr_t + compressed payload bytes.
 */
status_t object_physical_size(repo_t *repo,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes);

/*
 * Attempt to repair a loose object file in-place using its parity data.
 * Reads the file, applies XOR/RS parity correction, writes corrected bytes
 * back via pwrite.  Only works on version-2 loose objects with parity trailers.
 * Returns: >0 = bytes corrected, 0 = no corruption found, <0 = error or not repairable.
 */
int object_repair(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]);
