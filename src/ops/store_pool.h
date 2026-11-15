#pragma once

/*
 * Large files stored on worker threads, so one backup is not one core (B086).
 *
 * A file's SHA-256 is inherently serial: the object identity is the hash of
 * the whole plaintext, and splitting it would need a tree hash, which is a
 * format change. So one file can go no faster than one core hashes. What the
 * pool buys is several files at once. On the tester's machine the eight 2 GiB
 * files of a 16 GiB backup were stored strictly one after another at about
 * 203 MB/s each, with eleven of twelve threads idle.
 *
 * The split of work is deliberate and narrow:
 *
 *   - the backup thread opens each file over PACK_STREAM_THRESHOLD that the
 *     file cache could not vouch for, and hands the descriptor over;
 *   - a worker stores it loose with object_store_file_loose, which is what
 *     the serial path does for a file that size anyway, and closes it;
 *   - the backup thread takes the result back and does everything else:
 *     the index row, the cache row, the counters, the entry's hash.
 *
 * Small files, the backup pack, and every write to the database stay on the
 * backup thread, inside the transaction it already holds. The one database
 * access a worker makes is the store's existence check after hashing, which
 * goes through the resolver: a single indexed read per large file, on the
 * shared handle's mutex, with no transaction of its own. Forwarding it to the
 * backup thread would have meant servicing a request channel from every
 * point at which that thread can block.
 *
 * Nothing about the repository depends on which worker finishes first. A
 * result lands in the scan entry it came from, and trees are only built once
 * every result is in, so the manifest is byte-identical to a serial run's.
 */

#include "error.h"
#include "object.h"
#include "phase_timer.h"
#include "store.h"

#include <stddef.h>
#include <stdint.h>

typedef struct store_pool store_pool_t;

/* One file, from submission to the backup thread taking it back. */
typedef struct store_pool_job {
    /* In: set by the submitter. fd belongs to the pool once submitted. */
    uint32_t index;          /* the scan entry, for the caller */
    int      fd;
    uint64_t size;

    /* Out: set by the worker, read only after store_pool_take returns it. */
    status_t       st;
    object_write_t w;
    uint8_t        hash[OBJECT_HASH_SIZE];
    char           err[ERR_MSG_MAX];
    /* Bytes this file added to the pool's running count; the taker
     * subtracts them when it counts the whole file instead. */
    uint64_t       reported;

    struct store_pool_job *next;
} store_pool_job_t;

/*
 * How many workers to run and how much RS parity each may hold in memory.
 *
 * requested: 0 for automatic, otherwise the operator's number, which is
 * honoured exactly except that more workers than candidate files would only
 * sit idle. Automatic is two thirds of the CPUs this process may run on,
 * then bounded by a memory budget of an eighth of physical RAM (between
 * 256 MiB and 2 GiB) at no less than 40 MiB a worker. Each object writer
 * already runs a hasher and a writer thread beside its reader, so a third of
 * the CPUs is left for them, the backup thread and the pack workers.
 *
 * n_large: files that may go to the pool (over PACK_STREAM_THRESHOLD; the
 * cache may still vouch for some of them).
 *
 * The parity cap divides what the budget leaves after the 8 MiB of pipeline
 * buffers per worker, between 16 MiB and the serial default of 256 MiB.
 * Parity past the cap spills to the repository's tmp/, which costs a write
 * and a read of it, not correctness.
 *
 * A result of one worker means: do not start a pool, store serially.
 */
void store_pool_plan(unsigned requested, uint64_t n_large,
                     unsigned *out_workers, size_t *out_parity_cap);

/* The inputs store_pool_plan reads from the machine, for tests and logs. */
unsigned store_pool_cpus(void);
uint64_t store_pool_mem_budget(void);

/*
 * Start `workers` threads storing into `store`. When `timed` is set each
 * worker attributes to its own phase_timers_t, folded into the caller's by
 * store_pool_finish.
 */
status_t store_pool_start(store_t *store, unsigned workers, size_t parity_cap,
                          int timed, store_pool_t **out);

/* Jobs that may be outstanding (queued, running, or finished and not yet
 * taken) before store_pool_submit has to wait: two per worker, so a worker
 * finishing never waits for the backup thread to hand it the next file. */
unsigned store_pool_capacity(const store_pool_t *pool);
unsigned store_pool_outstanding(const store_pool_t *pool);

/* Queue one file. Never blocks; the caller checks capacity first. The pool
 * owns fd from here, whatever happens. */
status_t store_pool_submit(store_pool_t *pool, uint32_t index, int fd,
                           uint64_t size);

/* A finished job, or NULL. Non-blocking. The caller frees it with
 * store_pool_job_free. */
store_pool_job_t *store_pool_take(store_pool_t *pool);
void store_pool_job_free(store_pool_job_t *job);

/* Wait up to timeout_ms for a job to finish. Returns at once if one already
 * has, or if nothing is outstanding. */
void store_pool_wait(store_pool_t *pool, unsigned timeout_ms);

/* Bytes read so far by the workers from files not yet taken. Read from the
 * backup thread for progress; workers only add. */
uint64_t store_pool_bytes(const store_pool_t *pool);

/* Workers the pool runs. */
unsigned store_pool_workers(const store_pool_t *pool);

/*
 * Stop and free the pool. With `abort` set, the workers are told to stop
 * through their thread cancel flag (store_set_thread_cancel) - a file being
 * stored fails within one read and its temp file is removed - queued files
 * are closed without being read, and finished results are discarded.
 * Without it every outstanding job must already have been taken.
 *
 * Joins every thread before returning and, when `timers` is given, folds
 * the workers' phases into it. NULL is a no-op.
 */
void store_pool_finish(store_pool_t *pool, int abort, phase_timers_t *timers);
