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
    [PHASE_STORE_WAIT]   = "large-file-wait",
    [PHASE_SETUP]        = "setup",
    [PHASE_PACK_SEAL]    = "pack-seal",
    [PHASE_SNAPSHOT]     = "snapshot-record",
    [PHASE_TXN_WAIT]     = "txn-wait",
    [PHASE_TXN_COMMIT]   = "txn-commit",
    [PHASE_PROGRESS]     = "progress-cb",
    [PHASE_COMMIT_STORE] = "  commit:store",
    [PHASE_COMMIT_TREES] = "  commit:trees",
    [PHASE_COMMIT_INDEX] = "  commit:index",
    [PHASE_COMMIT_PACK]  = "  commit:pack",
    [PHASE_OBJ_SETUP]    = "  obj:setup",
    [PHASE_OBJ_IO]       = "  obj:io-loop",
    [PHASE_OBJ_FINISH]   = "  obj:finish",
    [PHASE_OBJ_SYNC]     = "  obj:sync",
    [PHASE_OBJ_INSTALL]  = "  obj:install",
    [PHASE_OBJ_CLOSE]    = "    inst:close",
    [PHASE_OBJ_MKDIR]    = "    inst:mkdir+open",
    [PHASE_OBJ_RENAME]   = "    inst:rename",
    [PHASE_OBJ_LOOSESET] = "    inst:loose-set",
    [PHASE_STORE_WORKERS] = "  large-files",
    [PHASE_IO_READ]      = "    io:read cpu",
    [PHASE_IO_HASH]      = "    io:hash cpu",
    [PHASE_IO_ENCRYPT]   = "    io:encrypt cpu",
    [PHASE_IO_WRITE]     = "    io:write cpu",
    [PHASE_IO_PARITY]    = "    io:parity cpu",
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

    /*
     * Said once rather than left for the reader to infer: with parallel
     * workers the obj:* and large-files rows are summed across threads and
     * can each exceed the wall clock, and the io:* rows are CPU time. The
     * backup thread's own wait for the workers is large-file-wait, which is
     * what the percentages above count.
     */
    if (t->store_threads > 1)
        log_info("  (large files stored on %u threads: large-files and obj:* are "
                 "wall time summed over them, io:* is thread CPU time)",
                 t->store_threads);
    else if (t->ns[PHASE_IO_HASH] || t->ns[PHASE_IO_READ])
        log_info("  (io:* is thread CPU time over the reader, hasher and writer "
                 "threads of each object; obj:* is wall time)");

    if (total_ns > accounted) {
        double gap = (double)(total_ns - accounted) / 1e9;
        log_info("  %-16s %8.2fs  %5.1f%%  (unattributed)", "elsewhere", gap,
                 (double)(total_ns - accounted) * 100.0 / (double)total_ns);
    }
}
