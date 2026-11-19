#define _POSIX_C_SOURCE 200809L

#include "shutdown_wait.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <time.h>

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv;
static pthread_once_t  g_cv_once = PTHREAD_ONCE_INIT;
static int             g_stop;      /* g_mu */

/* CLOCK_MONOTONIC, so setting the wall clock neither ends a wait early nor
 * stretches it. */
static void cv_init(void)
{
    pthread_condattr_t a;
    pthread_condattr_init(&a);
    pthread_condattr_setclock(&a, CLOCK_MONOTONIC);
    pthread_cond_init(&g_cv, &a);
    pthread_condattr_destroy(&a);
}

static struct timespec deadline_in(unsigned ms)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec  += (time_t)(ms / 1000u);
    t.tv_nsec += (long)(ms % 1000u) * 1000000L;
    if (t.tv_nsec >= 1000000000L) { t.tv_sec++; t.tv_nsec -= 1000000000L; }
    return t;
}

void stop_request(void)
{
    pthread_once(&g_cv_once, cv_init);
    pthread_mutex_lock(&g_mu);
    g_stop = 1;
    pthread_cond_broadcast(&g_cv);
    pthread_mutex_unlock(&g_mu);
}

int stop_requested(void)
{
    pthread_mutex_lock(&g_mu);
    int s = g_stop;
    pthread_mutex_unlock(&g_mu);
    return s;
}

int stop_wait(unsigned ms)
{
    pthread_once(&g_cv_once, cv_init);
    struct timespec until = deadline_in(ms);
    pthread_mutex_lock(&g_mu);
    while (!g_stop && pthread_cond_timedwait(&g_cv, &g_mu, &until) == 0) {}
    int s = g_stop;
    pthread_mutex_unlock(&g_mu);
    return s;
}

void stop_reset_for_test(void)
{
    pthread_mutex_lock(&g_mu);
    g_stop = 0;
    pthread_mutex_unlock(&g_mu);
}

/*
 * Shared by the caller and the thread; whichever lets go last frees it, so
 * a caller that gave up never frees what a late fn's thread still uses.
 */
typedef struct {
    void          (*fn)(void);
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             done;
    atomic_int      refs;
} within_t;

static void within_put(within_t *w)
{
    if (atomic_fetch_sub(&w->refs, 1) == 1) {
        pthread_cond_destroy(&w->cv);
        pthread_mutex_destroy(&w->mu);
        free(w);
    }
}

static void *within_thread(void *arg)
{
    within_t *w = arg;
    w->fn();
    pthread_mutex_lock(&w->mu);
    w->done = 1;
    pthread_cond_signal(&w->cv);
    pthread_mutex_unlock(&w->mu);
    within_put(w);
    return NULL;
}

int run_within(void (*fn)(void), unsigned ms)
{
    within_t *w = calloc(1, sizeof(*w));
    if (!w) { fn(); return 0; }
    w->fn = fn;
    atomic_init(&w->refs, 2);
    pthread_mutex_init(&w->mu, NULL);
    pthread_condattr_t a;
    pthread_condattr_init(&a);
    pthread_condattr_setclock(&a, CLOCK_MONOTONIC);
    pthread_cond_init(&w->cv, &a);
    pthread_condattr_destroy(&a);

    pthread_attr_t ta;
    pthread_t t;
    pthread_attr_init(&ta);
    pthread_attr_setdetachstate(&ta, PTHREAD_CREATE_DETACHED);
    int rc = pthread_create(&t, &ta, within_thread, w);
    pthread_attr_destroy(&ta);
    if (rc != 0) {
        pthread_cond_destroy(&w->cv);
        pthread_mutex_destroy(&w->mu);
        free(w);
        fn();
        return 0;
    }

    struct timespec until = deadline_in(ms);
    pthread_mutex_lock(&w->mu);
    while (!w->done && pthread_cond_timedwait(&w->cv, &w->mu, &until) == 0) {}
    int done = w->done;
    pthread_mutex_unlock(&w->mu);
    within_put(w);
    return done ? 0 : -1;
}
