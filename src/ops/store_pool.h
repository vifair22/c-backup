#pragma once

/*
 * Large files stored on worker threads, so one backup is not one core (B086).
 *
 * Format 3 cuts every file into content-defined chunks whose CPU work runs
 * on chunk workers (cdc.h), but one file is still bound by the thread that
 * reads it and finds its boundaries. What this pool buys is several files
 * at once: on the tester's machine the eight 2 GiB files of a 16 GiB backup
 * were stored strictly one after another, with most threads idle.
 *
 * The split of work is deliberate and narrow:
 *
 *   - the backup thread opens each file over PACK_STREAM_THRESHOLD that the
 *     file cache could not vouch for, and hands the descriptor over;
 *   - a worker stores it through the caller's ops (store_pool_ops_t): a
 *     backup's cut it into a pack writer of the worker's own and seal that
 *     writer's packs, whose commits create the objects' index rows;
 *   - the backup thread takes the result back and does everything else:
 *     the cache row, the counters, the entry's hash.
 *
 * Small files, the backup's own pack, and every other database write stay
 * on the backup thread, inside the transaction it already holds; it lets
 * that go whenever a worker's seal waits for the write lock.
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

    /* Out: set by the worker, read only after store_pool_take returns it.
     * out is whatever the store hook made of the file; the pool frees it
     * with the ops' out_free. */
    status_t       st;
    void          *out;
    char           err[ERR_MSG_MAX];
    /* Bytes this file added to the pool's running count; the taker
     * subtracts them when it counts the whole file instead. */
    uint64_t       reported;

    void         (*out_free)(void *);
    struct store_pool_job *next;
} store_pool_job_t;

/*
 * What each worker does (format 3: a backup's large files, chunked, each
 * worker into its own pack writer). begin runs on the worker thread before
 * its first file and end after its last, with abort set when the pool is
 * being stopped; store runs per file. All three run on the worker thread,
 * with the pool's cancel flag as that thread's (store_set_thread_cancel)
 * and the store's byte-progress hook counting into the pool.
 */
typedef struct {
    status_t (*begin)(void *ctx, void **wstate);
    status_t (*store)(void *ctx, void *wstate, store_pool_job_t *job);
    status_t (*end)(void *ctx, void *wstate, int abort);
    void     (*out_free)(void *out);
    void      *ctx;
} store_pool_ops_t;

/*
 * How many workers to run.
 *
 * requested: 0 for automatic, otherwise the operator's number, which is
 * honoured exactly except that more workers than candidate files would only
 * sit idle. Automatic is two thirds of the CPUs this process may run on,
 * then bounded by a memory budget (store_pool_budget_for: an eighth of
 * physical RAM or of the cgroup's limit, whichever is smaller, between
 * 256 MiB and 2 GiB) at no less than 40 MiB a worker. Each object writer
 * already runs a hasher and a writer thread beside its reader, so a third of
 * the CPUs is left for them, the backup thread and the pack workers.
 *
 * n_large: files that may go to the pool (over PACK_STREAM_THRESHOLD; the
 * cache may still vouch for some of them).
 *
 * Parity is no longer a per-worker cost here: it is each pack writer's,
 * charged to the daemon's buffer budget when the writer opens (pack.h,
 * pack_writer_budget_bytes), which is what bounds how many large files a
 * backup stores at once (backup.c, store_content).
 *
 * A result of one worker means: do not start a pool, store serially.
 */
void store_pool_plan(unsigned requested, uint64_t n_large, unsigned *out_workers);

/* The inputs store_pool_plan reads from the machine, for tests and logs. */
unsigned store_pool_cpus(void);
uint64_t store_pool_mem_budget(void);

/*
 * The memory budget from its two inputs: physical RAM (0 if unknown) and
 * the cgroup's memory limit (0 for none), whichever is smaller. An eighth of
 * it, between 256 MiB and 2 GiB, and never more than a quarter of it.
 *
 * Physical RAM alone is the wrong figure in a container: a daemon under a
 * 1 GiB memory.max on a 64 GiB host would have planned for 64 GiB and been
 * OOM-killed partway through a backup of large files.
 */
uint64_t store_pool_budget_for(uint64_t phys, uint64_t cgroup_limit);

/*
 * The cgroup v2 memory limit that applies to this process: the smallest
 * memory.max from its own cgroup up to the root, read through `proc_cgroup`
 * (normally /proc/self/cgroup) under `cgroup_root` (normally
 * /sys/fs/cgroup). 0 when there is none, when only cgroup v1 is mounted, or
 * when anything cannot be read. Paths are parameters so a test can point
 * them at fixture files.
 */
uint64_t store_pool_cgroup_limit(const char *proc_cgroup, const char *cgroup_root);

/* One memory.max file: its byte count, or 0 for "max", absent or garbage. */
uint64_t store_pool_read_memory_max(const char *path);

/*
 * Start `workers` threads storing into `store`. When `timed` is set each
 * worker attributes to its own phase_timers_t, folded into the caller's by
 * store_pool_finish.
 */
status_t store_pool_start(store_t *store, unsigned workers,
                          const store_pool_ops_t *ops, int timed,
                          store_pool_t **out);

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
 * the workers' phases into it. Returns the first failure of a worker's
 * end hook (its last pack's seal, for a backup); OK for NULL.
 */
status_t store_pool_finish(store_pool_t *pool, int abort, phase_timers_t *timers);
