#pragma once

/*
 * Restore's read-ahead: the kernel is asked for the stored bytes of the
 * objects a restore will load next, a bounded window ahead of the one it is
 * loading.
 *
 * The restore is one thread that loads an object, decodes it and writes
 * it, then asks for the next. Each load was a synchronous read of the pack
 * with nothing else queued, so on a cold cache the device served one
 * request at a time and sat idle while the thread decrypted, hashed and
 * wrote. The hints (object_prefetch, POSIX_FADV_WILLNEED) queue the next
 * objects' reads so the device works while the thread does; every byte is
 * still read, authenticated and checked against its name by the load that
 * uses it, exactly as before. Nothing here decides what is restored or
 * how, and a hint that misses costs a read, never a wrong file.
 *
 * The window walks the same rows the restore will, in the same order, and
 * the restore says where it is: at the start of each file it restores
 * (restore_prefetch_at) and as each object of it is written
 * (restore_prefetch_wrote). The window is counted in content bytes, so a
 * file of many chunks and many small files both keep about the same amount
 * in flight.
 */

#include "store.h"
#include "object.h"

#include <stdint.h>

/*
 * Whether row will be restored from repository content, and if so its
 * content hash and size. Called once per row, in order, ahead of the
 * restore reaching it; a row it accepts that the restore then skips costs
 * only a wasted hint.
 */
typedef int (*restore_prefetch_target_fn)(void *ctx, int row,
                                          uint8_t hash[OBJECT_HASH_SIZE],
                                          uint64_t *size);

typedef struct restore_prefetch restore_prefetch_t;

/* NULL when it cannot be allocated: the restore runs without read-ahead. */
restore_prefetch_t *restore_prefetch_new(store_t *store, int nrows,
                                         restore_prefetch_target_fn target,
                                         void *ctx);
void restore_prefetch_free(restore_prefetch_t *p);

/* The restore is about to load row's content. */
void restore_prefetch_at(restore_prefetch_t *p, int row);

/* The restore has written n more content bytes of the current row. */
void restore_prefetch_wrote(restore_prefetch_t *p, uint64_t n);

/* Hints issued and content bytes they covered, for tests and benches. */
void restore_prefetch_counts(const restore_prefetch_t *p, uint64_t *hints,
                             uint64_t *bytes);
