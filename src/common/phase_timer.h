#pragma once

/*
 * Where a backup actually spends its time, attributed per component.
 *
 * The existing timer reported "scan+store" as one number, which is the bucket
 * holding almost everything worth knowing - and its label was wrong besides,
 * since the clock started after the scan had already run. That left every
 * question about throughput answerable only by argument. Two such arguments
 * were made and measured wrong in one afternoon.
 *
 * Left on in production deliberately. A run on a developer's tmpfs is exactly
 * the run whose numbers do not matter: bench_phases reports 51,033 files/s for
 * a backup the same machine performs at 175 files/s against real storage,
 * because the benchmark keeps the index in RAM and the file count small. The
 * interesting profile only exists on the deployment, so the profiler has to
 * live there too.
 *
 * The cost of that is one clock_gettime pair per measured span. It is a vDSO
 * read on Linux, tens of nanoseconds, and the spans are per file rather than
 * per byte: about six probes across 622,000 files is under a tenth of a second
 * against a run measured in minutes. If a span is ever added inside a
 * per-block loop, this stops being free and the decision has to be revisited.
 */

/* clock_gettime and CLOCK_MONOTONIC need POSIX 2008, which each translation
 * unit in this tree enables for itself. Including this from a file that has
 * not is a compile error rather than a silent fallback, which is the right
 * way round. */
#include <stdint.h>
#include <time.h>

typedef enum {
    PHASE_SCAN = 0,      /* lstat walk, xattr and ACL collection */
    PHASE_CACHE_LOOKUP,  /* file_cache SELECT, plus the objects existence check */
    PHASE_READ_HASH,     /* open, read, SHA-256, and the object write itself */
    PHASE_INDEX_PUT,     /* store_index_put_loose */
    PHASE_CACHE_PUT,     /* file_cache_put */
    PHASE_META_BLOB,     /* xattr and ACL blobs */
    PHASE_TREES,         /* build_trees: manifest serialisation and dedup */
    PHASE_INDEX_NODES,   /* nodes and node_fts inserts */
    PHASE_CACHE_SWEEP,   /* dropping cache rows for paths the scan no longer sees */
    /* Inside PHASE_READ_HASH, which on real storage is 82% of a full backup
     * and far too coarse to act on. These break that bucket apart. */
    PHASE_OBJ_SETUP,     /* mkstemp, header write, parity stream init */
    PHASE_OBJ_IO,        /* the read -> hash -> write -> parity-feed loop */
    PHASE_OBJ_FINISH,    /* parity finish and trailer write */
    PHASE_OBJ_SYNC,      /* async_writeback */
    PHASE_OBJ_INSTALL,   /* rename into objects/, and the existence check */
    /* Inside obj:install, which turned out to be 59% of a full backup. */
    PHASE_OBJ_CLOSE,     /* close of the written temp file */
    PHASE_OBJ_MKDIR,     /* mkdirat + openat of the fanout directory */
    PHASE_OBJ_RENAME,    /* rename into place */
    PHASE_OBJ_LOOSESET,  /* store_loose_set_insert */
    PHASE__COUNT
} phase_id_t;

typedef struct {
    uint64_t ns[PHASE__COUNT];
    uint64_t count[PHASE__COUNT];
} phase_timers_t;

/*
 * The timer set the current thread is attributing to.
 *
 * Layers below the backup pipeline - the object writer in particular - need to
 * record spans without a phase_timers_t threaded through signatures that have
 * nothing else to do with profiling. A thread-local avoids that, and is correct
 * here because a backup runs on one job worker while pack workers have their
 * own. NULL means nobody is measuring, which phase_add treats as a no-op.
 */
extern _Thread_local phase_timers_t *phase_current;

static inline uint64_t phase_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Adds the span from `start` to now. Taking the start rather than the duration
 * keeps the call sites to one line and one local. */
static inline void phase_add(phase_timers_t *t, phase_id_t id, uint64_t start)
{
    if (!t) return;
    t->ns[id]    += phase_now() - start;
    t->count[id] += 1;
}

/*
 * Log one line per phase, widest first, with a per-call figure.
 *
 * Sorted by cost because the only question this exists to answer is which
 * component to attack, and a fixed order buries that under whichever phase
 * happens to be declared first.
 */
void phase_report(const phase_timers_t *t, const char *what, uint64_t total_ns);
