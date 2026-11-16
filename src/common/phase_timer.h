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
    PHASE_INDEX_PUT,     /* store_index_put_packed */
    PHASE_CACHE_PUT,     /* file_cache_put */
    PHASE_META_BLOB,     /* xattr and ACL blobs */
    PHASE_TREES,         /* build_trees: manifest serialisation and dedup */
    PHASE_INDEX_NODES,   /* nodes and node_fts inserts */
    PHASE_CACHE_SWEEP,   /* dropping cache rows for paths the scan no longer sees */
    PHASE_SETUP,         /* dropping stale staged rows, opening the backup pack */
    PHASE_PACK_SEAL,     /* sealing a full backup pack, and the last one */
    PHASE_SNAPSHOT,      /* snapshot id, the record file and its row */
    PHASE_STORE_WAIT,    /* the backup thread blocked on the file workers */
    /*
     * Nested spans: recorded from inside whichever phase is running, and
     * subtracted from it by phase_add_excl, so the top-level rows stay
     * disjoint and still sum to the wall clock.
     *
     * The transaction rows are recorded by db_txn itself, for every
     * transaction this thread takes: the store phase's batches, the trees
     * batch, index_nodes, and the pack commit callback a seal runs. On a
     * deployment whose storage syncs slowly txn-commit is where the time
     * goes, and before these existed it was "elsewhere" (OWN-16).
     */
    PHASE_TXN_WAIT,      /* the ticket lock and BEGIN */
    PHASE_TXN_COMMIT,    /* COMMIT or ROLLBACK, WAL sync included */
    PHASE_PROGRESS,      /* the job's progress callbacks: a database write each */
    /* Not accounted: txn-commit again, by which phase took the transaction.
     * Set through phase_timers_t.txn_site. */
    PHASE_COMMIT_STORE,
    PHASE_COMMIT_TREES,
    PHASE_COMMIT_INDEX,
    PHASE_COMMIT_PACK,
    /* Not accounted either: the file workers' own time storing files, summed
     * over them, so it can exceed the wall clock. */
    PHASE_STORE_WORKERS,
    PHASE__COUNT
} phase_id_t;

typedef struct {
    uint64_t ns[PHASE__COUNT];
    uint64_t count[PHASE__COUNT];
    /* Total of every nested span so far (PHASE_TXN_*, PHASE_PROGRESS); a
     * top-level span subtracts what accrued while it ran. */
    uint64_t nested_ns;
    /* Which commit:* row a transaction commit is also charged to, or 0. */
    phase_id_t txn_site;
    /* File workers the backup ran (store_pool.h); 0 or 1 when none. */
    unsigned store_threads;
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

/* The nested total to hand phase_add_excl when a top-level span starts. */
static inline uint64_t phase_nested(const phase_timers_t *t)
{
    return t ? t->nested_ns : 0;
}

/*
 * A top-level span, less the nested spans recorded while it ran. Every
 * top-level phase that can contain a transaction or a progress write is
 * recorded this way, or the same second would be counted twice and the
 * profile would claim more time than the run took.
 */
static inline void phase_add_excl(phase_timers_t *t, phase_id_t id,
                                  uint64_t start, uint64_t nested_start)
{
    if (!t) return;
    uint64_t span = phase_now() - start;
    uint64_t inner = t->nested_ns - nested_start;
    t->ns[id]    += span > inner ? span - inner : 0;
    t->count[id] += 1;
}

/* A nested span: counted under its own row and in nested_ns. */
static inline void phase_add_nested(phase_timers_t *t, phase_id_t id, uint64_t start)
{
    if (!t) return;
    uint64_t span = phase_now() - start;
    t->ns[id]     += span;
    t->count[id]  += 1;
    t->nested_ns  += span;
}

/* A commit or rollback: txn-commit, and the commit:* row of the phase that
 * took the transaction, when one has said so. */
static inline void phase_add_commit(phase_timers_t *t, uint64_t start)
{
    if (!t) return;
    uint64_t span = phase_now() - start;
    t->ns[PHASE_TXN_COMMIT]    += span;
    t->count[PHASE_TXN_COMMIT] += 1;
    t->nested_ns               += span;
    if (t->txn_site) {
        t->ns[t->txn_site]    += span;
        t->count[t->txn_site] += 1;
    }
}

/*
 * This thread's CPU clock, for the io:* stages.
 *
 * Not a vDSO read: it is a system call, a few hundred nanoseconds. The stages
 * take it twice per 1 MiB chunk, which is under a millisecond per gigabyte,
 * so the rule in the header comment still holds - but only because the spans
 * are per chunk. Per block would not be free.
 */
static inline uint64_t phase_cpu_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Adds a duration measured elsewhere: a stage thread keeps its own sums and
 * hands them over once joined, so no counter is ever shared between threads. */
static inline void phase_add_ns(phase_timers_t *t, phase_id_t id,
                                uint64_t ns, uint64_t count)
{
    if (!t) return;
    t->ns[id]    += ns;
    t->count[id] += count;
}

/* Folds a joined worker's timers into the backup's. The caller owns both: the
 * worker has exited, so nothing else is writing src. */
void phase_merge(phase_timers_t *dst, const phase_timers_t *src);

/*
 * Log one line per phase, widest first, with a per-call figure.
 *
 * Sorted by cost because the only question this exists to answer is which
 * component to attack, and a fixed order buries that under whichever phase
 * happens to be declared first.
 */
void phase_report(const phase_timers_t *t, const char *what, uint64_t total_ns);
