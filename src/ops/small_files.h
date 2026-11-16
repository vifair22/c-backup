#pragma once

/*
 * Small files read, chunked and sealed on worker threads, many to a work
 * item, and appended to the backup's pack on the backup thread.
 *
 * A backup of many small files used to store them one at a time on the
 * backup thread: open, read, SHA-256, the LZ4 probe and compression, AEAD,
 * CRC and Reed-Solomon parity, then the pack append and the index rows,
 * about 18 us a file once the database stopped dominating (PERF.md, the
 * many-small profile, proposal 5). Everything up to the append needs nothing
 * from the pack writer or the database, so it moves here.
 *
 * The split is deliberate and narrow:
 *
 *   - the backup thread collects files the file cache could not vouch for
 *     into a batch (up to SMALL_BATCH_FILES files or SMALL_BATCH_BYTES of
 *     content) and submits it;
 *   - a worker opens each file and stores it with cdc_store_fd into a sink
 *     that only prepares (pack_backup_prepare: name, probe, LZ4, AEAD, CRC,
 *     parity) and keeps the prepared objects, with a private copy of each
 *     object's bytes, in the order the chunker produced them;
 *   - the backup thread takes batches back in submission order and appends
 *     every prepared object with pack_backup_add_prepared, which is where
 *     dedup against the repository and the open pack happens, then writes
 *     the rows and the cache entry. Packs, rows and the cache stay on one
 *     thread.
 *
 * The result does not depend on the number of workers: a file's content
 * hash is the chunker's, from its own bytes, and the objects reach the pack
 * in file order, as they did when the backup thread stored them itself.
 *
 * Memory is bounded by the number of batches in flight (submitted and not
 * yet applied) times their content, each held to the daemon's read budget:
 * the backup thread acquires a batch's bytes when it submits it, the slot
 * travels with the batch, and the backup thread releases it once applied.
 * It keeps what its batches hold to half the budget, so the slot its own
 * serial path takes for a chunk can always be had without waiting for a
 * batch only it can apply.
 */

#include "cdc.h"
#include "error.h"
#include "pack.h"
#include "store.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/* Files at most this large are batched; larger ones keep their paths. */
#define SMALL_FILE_MAX     ((uint64_t)1024 * 1024)
#define SMALL_BATCH_FILES  256u
#define SMALL_BATCH_BYTES  ((uint64_t)4 * 1024 * 1024)
/*
 * Workers, at most. The append, the rows and the cache entries stay on the
 * backup thread, which is the limit well before this: on 100,000 files of
 * 2-6 KiB the workers together are busy for under a third of its time.
 * More only adds threads contending for the scheduler (and, had they any
 * database work, for the one handle's mutex).
 */
#define SMALL_POOL_MAX     4u

/* One object a worker prepared, with the bytes it points into. */
typedef struct {
    pack_prepared_t pp;
    uint8_t        *bytes;      /* owned: pp's data and payload point here */
    size_t          len;
} small_obj_t;

typedef struct {
    /* In */
    uint32_t     index;         /* the scan entry */
    const char  *path;          /* the scan entry's, valid for the backup */
    uint64_t     size;          /* the scan's size */
    /* Out */
    int          ran;           /* a worker got to it (not cancelled first) */
    status_t     st;
    int          skipped;       /* opened or found changed: warn, not fail */
    char         err[ERR_MSG_MAX];
    uint8_t      hash[OBJECT_HASH_SIZE];
    small_obj_t *objs;
    size_t       nobjs, capobjs;
} small_file_t;

typedef struct small_batch {
    struct small_batch *next;   /* submission order */
    struct small_batch *qnext;  /* the workers' queue */
    small_file_t       *files;
    uint32_t            n, cap;
    uint64_t            bytes;
    uint64_t            taken;  /* read budget, set by the submitter and
                                 * released by whoever frees the batch */
    status_t            st;     /* ERR_BUSY when cancelled, else OK */
    int                 done;
} small_batch_t;

typedef struct small_pool small_pool_t;

/*
 * Start n workers. cancel (may be NULL) and the store's own flag stop a
 * worker between files. inflight caps batches submitted and not yet taken.
 */
status_t small_pool_start(store_t *store, const cdc_params_t *cp, unsigned n,
                          unsigned inflight, atomic_int *cancel,
                          small_pool_t **out);

/* A new, empty batch, or NULL on allocation failure. */
small_batch_t *small_batch_new(void);
/* Add one file. Returns 1 when the batch is full and should be submitted. */
int  small_batch_add(small_batch_t *b, uint32_t index, const char *path,
                     uint64_t size);
void small_batch_free(small_batch_t *b);

/* Hand a batch over; the pool owns it until small_pool_take returns it. */
void small_pool_submit(small_pool_t *pool, small_batch_t *b);
/* Batches submitted and not yet taken. */
unsigned small_pool_outstanding(const small_pool_t *pool);
unsigned small_pool_capacity(const small_pool_t *pool);
/*
 * The oldest batch, once finished: waits up to wait_ms for it (0: do not
 * wait). NULL if there is none, or it is not finished in time.
 */
small_batch_t *small_pool_take(small_pool_t *pool, unsigned wait_ms);

/* Time the workers spent, summed, in ns. */
uint64_t small_pool_busy_ns(const small_pool_t *pool);

/* Stop and join. Batches not taken are freed (their budget released). */
void small_pool_stop(small_pool_t *pool);
