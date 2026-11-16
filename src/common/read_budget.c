#define _POSIX_C_SOURCE 200809L

#include "read_budget.h"

#include <pthread.h>

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv = PTHREAD_COND_INITIALIZER;
static uint64_t g_total;      /* 0 = unlimited */
static uint64_t g_used;
static uint64_t g_peak;
static uint64_t g_nested;

/* What this thread holds and has not handed off. */
static _Thread_local uint64_t t_held;

status_t read_budget_init(uint64_t bytes)
{
    if (bytes != 0 && bytes < READ_BUDGET_MIN)
        return set_error(ERR_INVALID, "read budget %llu bytes is under one "
                         "largest object (%llu)", (unsigned long long)bytes,
                         (unsigned long long)READ_BUDGET_MIN);
    pthread_mutex_lock(&g_mu);
    g_total = bytes;
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
    return OK;
}

uint64_t read_budget_total(void)
{
    pthread_mutex_lock(&g_mu);
    uint64_t t = g_total;
    pthread_mutex_unlock(&g_mu);
    return t;
}

static void note_use_locked(void)
{
    if (g_used > g_peak) g_peak = g_used;
}

uint64_t read_budget_acquire(uint64_t n)
{
    pthread_mutex_lock(&g_mu);
    if (g_total && n > g_total) n = g_total;
    if (t_held > 0) {
        /* A holder waiting for more is how a small budget deadlocks; take
         * it without waiting, and say so. */
        g_nested++;
    } else {
        while (g_total && g_used + n > g_total) pthread_cond_wait(&g_cv, &g_mu);
    }
    g_used += n;
    note_use_locked();
    pthread_mutex_unlock(&g_mu);
    t_held += n;
    return n;
}

static void give_back(uint64_t taken)
{
    pthread_mutex_lock(&g_mu);
    g_used = taken <= g_used ? g_used - taken : 0;
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
}

void read_budget_release(uint64_t taken)
{
    t_held = taken <= t_held ? t_held - taken : 0;
    give_back(taken);
}

void read_budget_handoff(uint64_t taken)
{
    t_held = taken <= t_held ? t_held - taken : 0;
}

void read_budget_release_foreign(uint64_t taken)
{
    give_back(taken);
}

uint64_t read_budget_in_use(void)
{
    pthread_mutex_lock(&g_mu);
    uint64_t u = g_used;
    pthread_mutex_unlock(&g_mu);
    return u;
}

uint64_t read_budget_peak(void)
{
    pthread_mutex_lock(&g_mu);
    uint64_t p = g_peak;
    pthread_mutex_unlock(&g_mu);
    return p;
}

uint64_t read_budget_nested(void)
{
    pthread_mutex_lock(&g_mu);
    uint64_t v = g_nested;
    pthread_mutex_unlock(&g_mu);
    return v;
}

void read_budget_reset_stats(void)
{
    pthread_mutex_lock(&g_mu);
    g_peak = g_used;
    g_nested = 0;
    pthread_mutex_unlock(&g_mu);
}
