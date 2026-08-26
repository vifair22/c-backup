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
    [PHASE_OBJ_SETUP]    = "  obj:setup",
    [PHASE_OBJ_IO]       = "  obj:io-loop",
    [PHASE_OBJ_FINISH]   = "  obj:finish",
    [PHASE_OBJ_SYNC]     = "  obj:sync",
    [PHASE_OBJ_INSTALL]  = "  obj:install",
    [PHASE_OBJ_CLOSE]    = "    inst:close",
    [PHASE_OBJ_MKDIR]    = "    inst:mkdir+open",
    [PHASE_OBJ_RENAME]   = "    inst:rename",
    [PHASE_OBJ_LOOSESET] = "    inst:loose-set",
};

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
        /* Sub-phases of read+hash+write, already counted in their parent.
         * Adding them again would double-count and make the percentages lie. */
        if (i < PHASE_OBJ_SETUP) accounted += t->ns[i];
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

    if (total_ns > accounted) {
        double gap = (double)(total_ns - accounted) / 1e9;
        log_info("  %-16s %8.2fs  %5.1f%%  (unattributed)", "elsewhere", gap,
                 (double)(total_ns - accounted) * 100.0 / (double)total_ns);
    }
}
