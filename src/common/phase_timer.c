#define _POSIX_C_SOURCE 200809L

#include "phase_timer.h"

#include <cutils/log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

_Thread_local phase_timers_t *phase_current = NULL;

static const char *const phase_names[PHASE__COUNT] = {
    [PHASE_SCAN]         = "scan",
    [PHASE_CACHE_LOOKUP] = "cache-lookup",
    [PHASE_READ_HASH]    = "read+hash+write",
    [PHASE_INDEX_PUT]    = "index-put",
    [PHASE_CACHE_PUT]    = "cache-put",
    [PHASE_META_BLOB]    = "meta-blob",
    [PHASE_TREES]        = "trees",
    [PHASE_INDEX_NODES]  = "index-nodes",
    [PHASE_CACHE_SWEEP]  = "cache-sweep",
    [PHASE_SETUP]        = "setup",
    [PHASE_PACK_SEAL]    = "pack-seal",
    [PHASE_SNAPSHOT]     = "snapshot-record",
    [PHASE_STORE_WAIT]   = "file-wait",
    [PHASE_TXN_WAIT]     = "txn-wait",
    [PHASE_TXN_COMMIT]   = "txn-commit",
    [PHASE_PROGRESS]     = "progress-cb",
    [PHASE_COMMIT_STORE] = "  commit:store",
    [PHASE_COMMIT_TREES] = "  commit:trees",
    [PHASE_COMMIT_INDEX] = "  commit:index",
    [PHASE_COMMIT_PACK]  = "  commit:pack",
    [PHASE_STORE_WORKERS] = "  file-workers",
};

void phase_merge(phase_timers_t *dst, const phase_timers_t *src)
{
    if (!dst || !src) return;
    for (int i = 0; i < PHASE__COUNT; i++) {
        dst->ns[i]    += src->ns[i];
        dst->count[i] += src->count[i];
    }
    /* nested_ns is not merged: it is this thread's running total for its
     * own open spans, and a worker's spans were never inside them. */
}

/* The disjoint rows that sum to the wall clock. Everything else breaks one
 * of them down further, or recounts it from another angle. */
static int phase_accounted(int i)
{
    return i < PHASE_COMMIT_STORE;
}

struct row {
    const char *name;
    uint64_t    ns;
    uint64_t    count;
};

static int by_ns_desc(const void *a, const void *b)
{
    const struct row *x = a, *y = b;
    if (x->ns < y->ns) return 1;
    if (x->ns > y->ns) return -1;
    return 0;
}

void phase_report(const phase_timers_t *t, const char *what, uint64_t total_ns)
{
    if (!t) return;

    struct row rows[PHASE__COUNT];
    size_t n = 0;
    uint64_t accounted = 0;

    for (int i = 0; i < PHASE__COUNT; i++) {
        if (t->count[i] == 0) continue;
        rows[n].name  = phase_names[i] ? phase_names[i] : "?";
        rows[n].ns    = t->ns[i];
        rows[n].count = t->count[i];
        /* Sub-phases of read+hash+write, and the commit:* recount of
         * txn-commit, are already counted in their parent. Adding them
         * again would double-count and make the percentages lie. */
        if (phase_accounted(i)) accounted += t->ns[i];
        n++;
    }
    if (n == 0) return;

    qsort(rows, n, sizeof(rows[0]), by_ns_desc);

    log_info("%s profile: %.2fs measured of %.2fs total", what ? what : "backup",
             (double)accounted / 1e9, (double)total_ns / 1e9);

    for (size_t i = 0; i < n; i++) {
        /*
         * Percentages are of the measured total rather than the wall clock.
         * The two differ by whatever sits between the spans, and reporting
         * that gap explicitly below is more honest than quietly inflating
         * every phase to make the column sum to a hundred.
         */
        double pct = accounted ? (double)rows[i].ns * 100.0 / (double)accounted : 0.0;
        double per_call_us = rows[i].count
            ? (double)rows[i].ns / 1000.0 / (double)rows[i].count : 0.0;
        log_info("  %-16s %8.2fs  %5.1f%%  %10llu calls  %8.1f us/call",
                 rows[i].name, (double)rows[i].ns / 1e9, pct,
                 (unsigned long long)rows[i].count, per_call_us);
    }


    /* Said once rather than left for the reader to infer: file-workers is
     * summed across threads and can exceed the wall clock. The backup
     * thread's own wait for them is file-wait. */
    if (t->store_threads > 1)
        log_info("  (files stored on %u workers at once: file-workers is wall "
                 "time summed over them)", t->store_threads);

    if (total_ns > accounted) {
        double gap = (double)(total_ns - accounted) / 1e9;
        log_info("  %-16s %8.2fs  %5.1f%%  (unattributed)", "elsewhere", gap,
                 (double)(total_ns - accounted) * 100.0 / (double)total_ns);
    }
}
