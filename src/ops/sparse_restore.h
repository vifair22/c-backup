#pragma once

#include "error.h"
#include "store.h"
#include "types.h"
#include <stdint.h>

/*
 * Streaming sparse-file reconstruction.
 *
 * Extracted verbatim from the pre-daemon restore.c during Phase 0 because it
 * is one of the few pieces of the old ops layer worth keeping: it reconstructs
 * a sparse object without ever holding the whole file, which the "files exceed
 * RAM" constraint requires.
 *
 * Phase 2 swaps store_t for store_t when the storage engine boundary lands.
 */
status_t restore_sparse_streaming(store_t *store,
                                  const uint8_t hash[OBJECT_HASH_SIZE],
                                  int out_fd, uint64_t file_size,
                                  const char *full);
