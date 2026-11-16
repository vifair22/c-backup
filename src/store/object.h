#pragma once

/*
 * Objects: naming, storing and loading (repository format 3).
 *
 * Every object lives in a pack and is at most OBJECT_MAX_BYTES. Its name is
 * SHA-256(type || payload) - object_name below, the only implementation -
 * so the same bytes stored as two types are two objects (OWN-38), and a
 * load checks the type along with the bytes.
 *
 * These are the primitives the chunker and the tree writer build on: they
 * hand a typed buffer to a store (directly, or through an obj_sink_t over
 * object_store_ex) and get its name back, and load by name. Nothing outside
 * this layer computes a name.
 */

#include "error.h"
#include "store.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>

/* The name of an object: SHA-256 of the type byte, then the payload. */
void object_name(uint8_t type, const void *data, size_t len,
                 uint8_t out[OBJECT_HASH_SIZE]);

/*
 * Name data and store it as an object of `type`, through the store's own
 * pack writer: staged into a pack that is sealed and installed when it
 * fills, when the store is closed, when object_store_flush is called, or
 * when anything reads an object it holds. OK whether the object was new or
 * already present (*out_is_new says which; *out_phys_bytes is the stored
 * size of a new one, 0 otherwise). ERR_TOO_LARGE past OBJECT_MAX_BYTES,
 * ERR_INVALID for a type this build does not know. Safe to call from
 * several threads; they share the one writer.
 *
 * out_hash must point to OBJECT_HASH_SIZE bytes.
 */
status_t object_store(store_t *store, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]);

status_t object_store_ex(store_t *store, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes);

/* Seal and install whatever object_store_ex has staged, so a walk over the
 * packs (gc, verify, a test reading the files) sees it. */
status_t object_store_flush(store_t *store);

/* What the store wrote, for the index to record. payload_size is the
 * object's uncompressed size. */
typedef struct {
    int      is_new;
    uint64_t phys_bytes;    /* stored bytes of the entry, header excluded */
    uint64_t payload_size;
} object_write_t;

/*
 * Store into a caller's open pack writer (a backup's), or through the
 * store's own writer when pack_w is NULL.
 */
struct pack_backup_writer;
status_t object_store_packed(store_t *store, uint8_t type,
                             const void *data, size_t len,
                             struct pack_backup_writer *pack_w,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             object_write_t *out_info);

/*
 * Store file_size bytes of fd as file content, through the chunker
 * (cdc_store_fd) with the store's chunking parameters: one OBJECT_TYPE_FILE
 * object when there is no cut and no hole, a chunk list otherwise, exactly
 * what a backup stores for the same file. Every chunk goes through
 * object_store_packed. Reads with pread; fd's offset is untouched. A file
 * that turns out shorter than file_size is ERR_IO. out_info's payload_size
 * is the file's length.
 */
status_t object_store_file_packed(store_t *store, int fd, uint64_t file_size,
                                  struct pack_backup_writer *pack_w,
                                  uint8_t out_hash[OBJECT_HASH_SIZE],
                                  object_write_t *out_info);

/*
 * Load an object by name. Caller frees *out_data. out_size is the payload
 * size; out_type (may be NULL) receives the type. The object is corrected
 * from parity where it needs it, decrypted and decompressed whole, and
 * checked against its name - type included - before anything is returned.
 */
status_t object_load(store_t *store,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type);

/* Is an object of this name present (installed, or staged in the store's
 * own writer)? */
int object_exists(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);

/* Hex-encode a hash.  buf must be at least OBJECT_HASH_SIZE*2+1 bytes. */
void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf);

/* Size and type from the entry header, without loading the payload. */
status_t object_get_info(store_t *store,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint64_t *out_size, uint8_t *out_type);

/*
 * Bytes the object's entry occupies in its pack: the 51-byte entry header
 * plus the stored payload. *out_header_bytes (may be NULL) gets the header
 * size, for a caller that reports it separately.
 */
status_t object_physical_size(store_t *store,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes,
                              uint32_t *out_header_bytes);

/* What an object looks like as stored, without decoding it. */
typedef struct {
    uint8_t  type;
    uint8_t  compression;
    uint8_t  encryption;
    uint64_t uncompressed_size;
} object_stored_info_t;

/*
 * Read an object without decrypting or decompressing it: the form
 * replication moves, so a target that never receives the key can still
 * store, verify and repair it. The bytes are corrected from parity first;
 * *out_damaged (may be NULL) is set when they carry damage beyond it, which
 * a caller moving them must report (OWN-31). The name is not checked, and
 * without the key cannot be. Every object is bounded, so the payload comes
 * back whole. Caller frees *out_payload.
 */
status_t object_read_stored(store_t *store,
                            const uint8_t hash[OBJECT_HASH_SIZE],
                            object_stored_info_t *out_info,
                            void **out_payload, size_t *out_len,
                            int *out_damaged);
