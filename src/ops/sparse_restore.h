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
/*
 * Streams the object through a pipe and lays it out as it arrives, so what
 * reaches out_fd is not verified until this returns OK: the tag and hash
 * are checked at the end of the read. out_fd must be somewhere a failure
 * can be discarded, never a file's final path. Restore itself uses
 * restore_sparse_from_fd below; this remains for the fuzz harness.
 */
status_t restore_sparse_streaming(store_t *store,
                                  const uint8_t hash[OBJECT_HASH_SIZE],
                                  int out_fd, uint64_t file_size,
                                  const char *full);

/*
 * Lay out a sparse object's payload that has already been read, and so
 * verified and if need be corrected from parity, into payload_fd. Restore
 * streams the object into a temporary file first and scatters from that:
 * the payload is read from the repository once, and a damaged loose object
 * is corrected on the way, which a pipe cannot do because it cannot be
 * rewound for the correcting pass.
 */
status_t restore_sparse_from_fd(int payload_fd, int out_fd, uint64_t file_size,
                                const char *full);

/*
 * Region table validation, exposed so it can be driven with arbitrary bytes.
 * These values become lseek offsets and write lengths against a file on the
 * machine being restored to, so they are checked before any of them is used.
 */
status_t sparse_header_check(const sparse_hdr_t *h, uint64_t file_size);
status_t sparse_regions_check(const sparse_region_t *r, uint32_t count,
                              uint64_t file_size);
