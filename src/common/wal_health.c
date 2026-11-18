#define _POSIX_C_SOURCE 200809L

#include "wal_health.h"

#include <cutils/log.h>

#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define REPEAT_LOG_S 60

static const char *const g_names[WAL_HEALTH_NSRC] = {
    "checkpointer", "wal-index reader", "WAL sync",
};

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static atomic_int      g_failing[WAL_HEALTH_NSRC];
static atomic_ulong    g_total;
static struct {
    char    msg[512];
    int64_t since;       /* unix time of the first failure in this run of them */
    int64_t last_log;    /* monotonic seconds */
    unsigned long repeats;
} g_src[WAL_HEALTH_NSRC];

static int64_t mono_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (int64_t)t.tv_sec;
}

void wal_health_fail(wal_health_src_t src, const char *fmt, ...)
{
    if ((unsigned)src >= WAL_HEALTH_NSRC) return;
    char msg[sizeof(g_src[0].msg)];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    atomic_fetch_add(&g_total, 1);
    int64_t now = mono_s();
    pthread_mutex_lock(&g_mu);
    int first = !atomic_load(&g_failing[src]);
    if (first) {
        g_src[src].since = (int64_t)time(NULL);
        g_src[src].repeats = 0;
    }
    snprintf(g_src[src].msg, sizeof(g_src[src].msg), "%s", msg);
    int log_now = first || now - g_src[src].last_log >= REPEAT_LOG_S;
    unsigned long repeats = g_src[src].repeats;
    if (log_now) {
        g_src[src].last_log = now;
        g_src[src].repeats = 0;
    } else {
        g_src[src].repeats++;
    }
    atomic_store(&g_failing[src], 1);
    pthread_mutex_unlock(&g_mu);

    if (!log_now) return;
    if (first)
        log_error("state.db: %s failed: %s; the WAL is not being copied back into the "
                  "database and will grow until this is fixed", g_names[src], msg);
    else
        log_error("state.db: %s still failing (%lu more since the last report): %s",
                  g_names[src], repeats, msg);
}

void wal_health_ok(wal_health_src_t src)
{
    if ((unsigned)src >= WAL_HEALTH_NSRC) return;
    if (!atomic_load_explicit(&g_failing[src], memory_order_relaxed)) return;
    pthread_mutex_lock(&g_mu);
    int was = atomic_exchange(&g_failing[src], 0);
    int64_t since = g_src[src].since;
    pthread_mutex_unlock(&g_mu);
    if (was)
        log_info("state.db: %s recovered (failing since unix time %lld)", g_names[src],
                 (long long)since);
}

unsigned long wal_health_failures(void)
{
    return atomic_load(&g_total);
}

int wal_health_failing(char *msg, size_t msz, int64_t *since)
{
    int found = -1;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < WAL_HEALTH_NSRC; i++) {
        if (!atomic_load(&g_failing[i])) continue;
        if (found < 0 || g_src[i].since < g_src[found].since) found = i;
    }
    if (found >= 0) {
        if (msg && msz) snprintf(msg, msz, "%s: %s", g_names[found], g_src[found].msg);
        if (since) *since = g_src[found].since;
    }
    pthread_mutex_unlock(&g_mu);
    return found >= 0;
}

void wal_health_reset(void)
{
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < WAL_HEALTH_NSRC; i++) {
        atomic_store(&g_failing[i], 0);
        memset(&g_src[i], 0, sizeof(g_src[i]));
    }
    atomic_store(&g_total, 0);
    pthread_mutex_unlock(&g_mu);
}
