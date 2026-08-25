#pragma once

#include "error.h"
#include "store.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>

/*
 * Hash data and store it as an object in the store.
 * out_hash must point to OBJECT_HASH_SIZE bytes.
 * Returns OK if newly written or already existed.
 */
status_t object_store(store_t *store, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]);

status_t object_store_ex(store_t *store, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes);

/*
 * Store a regular file, automatically detecting sparse regions.
 * Stores as OBJECT_TYPE_SPARSE if the file has holes, OBJECT_TYPE_FILE
 * otherwise.  Caller passes the fd (already open O_RDONLY) and file size.
 */
status_t object_store_file(store_t *store, int fd, uint64_t file_size,
                           uint8_t out_hash[OBJECT_HASH_SIZE]);

status_t object_store_file_ex(store_t *store, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes);

/*
 * What the store actually wrote, for the index to record.
 *
 * payload_size is the object header's uncompressed_size, and it is not always
 * the file's logical size. A sparse object's payload is its region table plus
 * the bytes that are not holes, so a 64 MiB file holding 1 MiB of data has a
 * payload of about 1 MiB. The logical size is a property of the manifest node
 * rather than of the object, because one object can legitimately back files of
 * different lengths: two files with the same data and different trailing holes
 * produce the same region table, the same payload, and therefore the same hash.
 * Recording the logical size against the object made stats and summary report
 * different totals before and after a pack, which rewrites the column from the
 * object header and so disagreed with what backup had put there.
 */
typedef struct {
    int      is_new;
    uint64_t phys_bytes;    /* bytes on disk: header, payload, parity trailer */
    uint64_t payload_size;  /* the object header's uncompressed_size */
} object_write_t;

status_t object_store_file_info(store_t *store, int fd, uint64_t file_size,
                                uint8_t out_hash[OBJECT_HASH_SIZE],
                                object_write_t *out_info);

/* Per-chunk progress callback invoked from the write path after each ~16 MiB
 * chunk is written.  chunk_bytes is the bytes in that chunk; ctx is caller-
 * supplied.  May be NULL (no callback). */
typedef void (*xfer_progress_fn)(uint64_t chunk_bytes, void *ctx);

/* Like object_store_file_ex but fires cb(chunk_bytes, cb_ctx) per chunk.
 * Useful for sub-second progress updates during large-file transfers.
 * If src_path is non-NULL the function takes ownership of fd (closes it)
 * and may reopen the file on transient read errors to recover mid-stream. */
status_t object_store_file_cb(store_t *store, int fd, const char *src_path,
                              uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes,
                              xfer_progress_fn cb, void *cb_ctx);

/*
 * Load object data by hash.  Caller must free(*out_data).
 * out_size is the uncompressed payload size.
 * out_type (may be NULL) receives the object type byte.
 */
status_t object_load(store_t *store,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type);

/* Check whether an object already exists (avoids re-hashing). */
int object_exists(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Stream object content to out_fd without buffering the entire payload in RAM.
 * Safe for large OBJECT_TYPE_FILE objects that would fail object_load with
 * ERR_TOO_LARGE.  out_size and out_type may be NULL.
 */
status_t object_load_stream(store_t *store,
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
status_t object_load_prefix(store_t *store,
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
status_t object_get_info(store_t *store,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint64_t *out_size, uint8_t *out_type);

/*
 * Stream `size` bytes from src_fd into the object store as type `type`,
 * stored uncompressed (COMPRESS_NONE).  Verifies SHA-256 against expected_hash.
 * Used by the xfer importer for large objects that exceed INT_MAX.
 */
status_t object_store_fd(store_t *store, uint8_t type, int src_fd, uint64_t size,
                          const uint8_t expected_hash[OBJECT_HASH_SIZE]);

/*
 * Return on-disk physical bytes used by a single object payload record.
 * For loose objects: object_header_t + compressed payload bytes.
 * For packed objects: pack_dat_entry_hdr_t + compressed payload bytes.
 *
 * out_header_bytes, when non-NULL, receives the size of whichever of those
 * two headers was counted. Callers that report the header separately need it
 * from here rather than assuming: the two differ (64 against 51), and an
 * introspection route that showed the loose header beside a packed object's
 * footprint was describing a layout the object does not have.
 */
status_t object_physical_size(store_t *store,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes,
                              uint32_t *out_header_bytes);

/*
 * What an object looks like on disk, without decoding it.
 *
 * Everything a reader needs to interpret the payload, which is what the
 * loose header and the pack entry header both carry - the two forms differ
 * in layout, not in content.
 */
typedef struct {
    uint8_t  type;
    uint8_t  compression;
    uint8_t  encryption;
    uint64_t uncompressed_size;
} object_stored_info_t;

/*
 * Read an object without decrypting or decompressing it.
 *
 * The payload comes back exactly as stored: compressed, then encrypted, with
 * the nonce and tag included. Replication uses this so that a copy holds
 * what the source holds and a target which never receives the key can still
 * store, verify and repair it - parity covers ciphertext for that reason.
 *
 * The hash is not checked, and cannot be: it covers the plaintext, which is
 * the one thing this function deliberately does not produce. A caller
 * handing these bytes to something else is moving a sealed box, not
 * vouching for what is inside it. Whoever eventually decrypts it checks.
 *
 * Caller frees *out_payload.
 */
status_t object_read_stored(store_t *store,
                            const uint8_t hash[OBJECT_HASH_SIZE],
                            object_stored_info_t *out_info,
                            void **out_payload, size_t *out_len);

/*
 * Attempt to repair a loose object file in-place using its parity data.
 * Reads the file, applies XOR/RS parity correction, writes corrected bytes
 * back via pwrite.  Only works on version-2 loose objects with parity trailers.
 * Returns: >0 = bytes corrected, 0 = no corruption found, <0 = error or not repairable.
 */
int object_repair(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);
