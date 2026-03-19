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

/*
 * Load object data by hash.  Caller must free(*out_data).
 * out_size is the uncompressed size.
 */
status_t object_load(repo_t *repo,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size);

/* Check whether an object already exists (avoids re-hashing). */
int object_exists(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]);

/* Hex-encode a hash.  buf must be at least OBJECT_HASH_SIZE*2+1 bytes. */
void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf);
