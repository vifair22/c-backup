#define _POSIX_C_SOURCE 200809L

#include "io_stall.h"
#include "phase_timer.h"

#include <cutils/log.h>

#include <stdatomic.h>

static atomic_ullong g_count[IO_STALL_STEPS];
static atomic_ullong g_total[IO_STALL_STEPS];
static atomic_ullong g_max[IO_STALL_STEPS];

#define IO_STALL_LOG_NS 1000000000ULL

const char *io_stall_name(io_stall_step_t step)
{
    switch (step) {
    case IO_STALL_TAG_WRITE:       return "tag_write";
    case IO_STALL_TAG_FILE_SYNC:   return "tag_file_sync";
    case IO_STALL_TAG_DIR_SYNC:    return "tag_dir_sync";
    case IO_STALL_DURABLE_BARRIER: return "durable_barrier";
    case IO_STALL_DURABLE_SYNC:    return "durable_sync";
    case IO_STALL_STEPS:           break;
    }
    return "unknown";
}

void io_stall_add(io_stall_step_t step, uint64_t start_ns)
{
    if ((unsigned)step >= IO_STALL_STEPS) return;
    uint64_t now = phase_now();
    uint64_t d = now > start_ns ? now - start_ns : 0;
    atomic_fetch_add_explicit(&g_count[step], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_total[step], d, memory_order_relaxed);
    unsigned long long cur = atomic_load_explicit(&g_max[step], memory_order_relaxed);
    while (d > cur &&
           !atomic_compare_exchange_weak_explicit(&g_max[step], &cur, d,
                                                  memory_order_relaxed, memory_order_relaxed))
        ;
    if (d >= IO_STALL_LOG_NS)
        log_warn("slow sync: %s waited %.3f s", io_stall_name(step), (double)d / 1e9);
}

void io_stall_get(io_stall_step_t step, io_stall_stat_t *out)
{
    if (!out) return;
    if ((unsigned)step >= IO_STALL_STEPS) { *out = (io_stall_stat_t){ 0 }; return; }
    out->count    = atomic_load_explicit(&g_count[step], memory_order_relaxed);
    out->total_ns = atomic_load_explicit(&g_total[step], memory_order_relaxed);
    out->max_ns   = atomic_load_explicit(&g_max[step], memory_order_relaxed);
}
