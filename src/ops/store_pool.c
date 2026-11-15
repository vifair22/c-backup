#define _GNU_SOURCE

#include "store_pool.h"

#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MIB ((uint64_t)1024 * 1024)

/* What each object writer holds besides parity: STREAM_PIPE_DEPTH chunks of
 * STREAM_PIPE_CHUNK in object.c. */
#define POOL_BUFFERS_PER_WORKER (8 * MIB)
/* Below this a worker would spill nearly all of its parity, so the memory
 * budget stops adding workers rather than starving each of them. */
#define POOL_MIN_PER_WORKER     (40 * MIB)
#define POOL_PARITY_MIN         (16 * MIB)
#define POOL_PARITY_MAX         (256 * MIB)
#define POOL_BUDGET_MIN         (256 * MIB)
#define POOL_BUDGET_MAX         (2048 * MIB)
/* The ceiling on an explicit request, matching the config key's range. */
#define POOL_MAX_WORKERS        64u

typedef struct worker {
    struct store_pool *pool;
    pthread_t          tid;
    int                started;
    phase_timers_t     timers;
    store_pool_job_t  *job;     /* the file this worker is storing, or NULL */
} worker_t;

struct store_pool {
    store_t        *store;
    size_t          parity_cap;
    int             timed;

    pthread_mutex_t mu;
    pthread_cond_t  cv_work;    /* a job was queued, or shutdown */
    pthread_cond_t  cv_done;    /* a job finished */
    store_pool_job_t *q_head, *q_tail;   /* queued, not yet picked up */
    store_pool_job_t *d_head, *d_tail;   /* finished, not yet taken */
    unsigned        outstanding;         /* submitted and not yet taken */
    int             shutdown;

    /* The workers' thread cancel flag; see store_set_thread_cancel. */
    volatile int    stop;
    /* Bytes read by workers from files not yet taken. Atomic: each worker
     * adds, the backup thread reads and subtracts. */
    uint64_t        bytes;

    unsigned        n_workers;
    worker_t       *workers;
};

unsigned store_pool_cpus(void)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    /* The affinity mask, not the machine: a container pinned to four cores
     * of a thirty-two core host should plan for four. */
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        int n = CPU_COUNT(&set);
        if (n > 0) return (unsigned)n;
    }
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (unsigned)n : 1u;
}

uint64_t store_pool_mem_budget(void)
{
    long pages = sysconf(_SC_PHYS_PAGES);
    long psize = sysconf(_SC_PAGESIZE);
    uint64_t phys = (pages > 0 && psize > 0)
                  ? (uint64_t)pages * (uint64_t)psize : 2048 * MIB;
    uint64_t b = phys / 8;
    if (b < POOL_BUDGET_MIN) b = POOL_BUDGET_MIN;
    if (b > POOL_BUDGET_MAX) b = POOL_BUDGET_MAX;
    return b;
}

void store_pool_plan(unsigned requested, uint64_t n_large,
                     unsigned *out_workers, size_t *out_parity_cap)
{
    uint64_t budget = store_pool_mem_budget();
    unsigned w;
    if (requested) {
        w = requested > POOL_MAX_WORKERS ? POOL_MAX_WORKERS : requested;
    } else {
        w = store_pool_cpus() * 2 / 3;
        uint64_t by_mem = budget / POOL_MIN_PER_WORKER;
        if (w > by_mem) w = (unsigned)by_mem;
        if (w > POOL_MAX_WORKERS) w = POOL_MAX_WORKERS;
    }
    if ((uint64_t)w > n_large) w = (unsigned)n_large;
    if (w < 1) w = 1;

    uint64_t share = budget / w;
    uint64_t cap = share > POOL_BUFFERS_PER_WORKER ? share - POOL_BUFFERS_PER_WORKER : 0;
    if (cap < POOL_PARITY_MIN) cap = POOL_PARITY_MIN;
    if (cap > POOL_PARITY_MAX) cap = POOL_PARITY_MAX;

    *out_workers = w;
    *out_parity_cap = (size_t)cap;
}

/*
 * Bytes read by this worker, from inside the store. Capped at the file's
 * size, as the serial path's counter is, so a reread on recovery cannot
 * carry progress past the total.
 */
static void pool_io(uint64_t bytes, void *ctx)
{
    worker_t *wk = ctx;
    store_pool_job_t *j = wk->job;
    if (!j) return;
    uint64_t room = j->size > j->reported ? j->size - j->reported : 0;
    if (bytes > room) bytes = room;
    if (!bytes) return;
    j->reported += bytes;
    __atomic_fetch_add(&wk->pool->bytes, bytes, __ATOMIC_RELAXED);
}

static void *worker_main(void *arg)
{
    worker_t *wk = arg;
    store_pool_t *pool = wk->pool;

    store_set_thread_cancel(&pool->stop);
    store_set_io_progress(pool->store, pool_io, wk);
    phase_current = pool->timed ? &wk->timers : NULL;

    for (;;) {
        pthread_mutex_lock(&pool->mu);
        while (!pool->q_head && !pool->shutdown)
            pthread_cond_wait(&pool->cv_work, &pool->mu);
        store_pool_job_t *j = pool->q_head;
        if (!j) { pthread_mutex_unlock(&pool->mu); break; }
        pool->q_head = j->next;
        if (!pool->q_head) pool->q_tail = NULL;
        j->next = NULL;
        pthread_mutex_unlock(&pool->mu);

        wk->job = j;
        if (__atomic_load_n(&pool->stop, __ATOMIC_ACQUIRE)) {
            j->st = set_error(ERR_BUSY, "cancelled before storing");
        } else {
            err_clear();
            uint64_t t0 = phase_now();
            j->st = object_store_file_loose(pool->store, j->fd, j->size,
                                            pool->parity_cap, j->hash, &j->w);
            phase_add(phase_current, PHASE_STORE_WORKERS, t0);
        }
        if (j->st != OK) snprintf(j->err, sizeof(j->err), "%s", err_msg());
        close(j->fd);
        j->fd = -1;
        wk->job = NULL;

        pthread_mutex_lock(&pool->mu);
        if (pool->d_tail) pool->d_tail->next = j; else pool->d_head = j;
        pool->d_tail = j;
        pthread_cond_signal(&pool->cv_done);
        pthread_mutex_unlock(&pool->mu);
    }

    /* Thread-locals outlive nothing here, but leaving phase_current aimed at
     * memory the pool is about to free is how OWN-12 happened. */
    phase_current = NULL;
    store_set_io_progress(pool->store, NULL, NULL);
    store_set_thread_cancel(NULL);
    return NULL;
}

static void free_list(store_pool_job_t *j)
{
    while (j) {
        store_pool_job_t *next = j->next;
        if (j->fd >= 0) close(j->fd);
        free(j);
        j = next;
    }
}

static void pool_destroy(store_pool_t *pool)
{
    free_list(pool->q_head);
    free_list(pool->d_head);
    pthread_cond_destroy(&pool->cv_work);
    pthread_cond_destroy(&pool->cv_done);
    pthread_mutex_destroy(&pool->mu);
    free(pool->workers);
    free(pool);
}

static void pool_join(store_pool_t *pool)
{
    pthread_mutex_lock(&pool->mu);
    pool->shutdown = 1;
    pthread_cond_broadcast(&pool->cv_work);
    pthread_mutex_unlock(&pool->mu);
    for (unsigned i = 0; i < pool->n_workers; i++)
        if (pool->workers[i].started) pthread_join(pool->workers[i].tid, NULL);
}

status_t store_pool_start(store_t *store, unsigned workers, size_t parity_cap,
                          int timed, store_pool_t **out)
{
    *out = NULL;
    if (!store || workers < 1)
        return set_error(ERR_INVALID, "store_pool_start: no store or no workers");

    store_pool_t *pool = calloc(1, sizeof(*pool));
    if (!pool) return set_error(ERR_NOMEM, "store pool");
    pool->workers = calloc(workers, sizeof(*pool->workers));
    if (!pool->workers) { free(pool); return set_error(ERR_NOMEM, "store pool workers"); }
    pool->store      = store;
    pool->parity_cap = parity_cap;
    pool->timed      = timed;
    pool->n_workers  = workers;
    pthread_mutex_init(&pool->mu, NULL);
    pthread_cond_init(&pool->cv_work, NULL);
    pthread_cond_init(&pool->cv_done, NULL);

    for (unsigned i = 0; i < workers; i++) {
        worker_t *wk = &pool->workers[i];
        wk->pool = pool;
        int rc = pthread_create(&wk->tid, NULL, worker_main, wk);
        if (rc != 0) {
            pool_join(pool);
            pool_destroy(pool);
            errno = rc;
            return set_error_errno(ERR_IO, "store pool: pthread_create");
        }
        wk->started = 1;
    }
    *out = pool;
    return OK;
}

unsigned store_pool_capacity(const store_pool_t *pool)
{
    return pool->n_workers * 2;
}

unsigned store_pool_outstanding(const store_pool_t *pool)
{
    /* Written only by the backup thread, which is the only caller. */
    return pool->outstanding;
}

unsigned store_pool_workers(const store_pool_t *pool)
{
    return pool ? pool->n_workers : 0;
}

status_t store_pool_submit(store_pool_t *pool, uint32_t index, int fd,
                           uint64_t size)
{
    store_pool_job_t *j = calloc(1, sizeof(*j));
    if (!j) { close(fd); return set_error(ERR_NOMEM, "store pool job"); }
    j->index = index;
    j->fd    = fd;
    j->size  = size;

    pthread_mutex_lock(&pool->mu);
    if (pool->q_tail) pool->q_tail->next = j; else pool->q_head = j;
    pool->q_tail = j;
    pool->outstanding++;
    pthread_cond_signal(&pool->cv_work);
    pthread_mutex_unlock(&pool->mu);
    return OK;
}

store_pool_job_t *store_pool_take(store_pool_t *pool)
{
    pthread_mutex_lock(&pool->mu);
    store_pool_job_t *j = pool->d_head;
    if (j) {
        pool->d_head = j->next;
        if (!pool->d_head) pool->d_tail = NULL;
        j->next = NULL;
        pool->outstanding--;
    }
    pthread_mutex_unlock(&pool->mu);
    if (j && j->reported)
        __atomic_fetch_sub(&pool->bytes, j->reported, __ATOMIC_RELAXED);
    return j;
}

void store_pool_job_free(store_pool_job_t *job)
{
    if (!job) return;
    if (job->fd >= 0) close(job->fd);
    free(job);
}

void store_pool_wait(store_pool_t *pool, unsigned timeout_ms)
{
    struct timespec until;
    clock_gettime(CLOCK_REALTIME, &until);
    until.tv_sec  += (time_t)(timeout_ms / 1000);
    until.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }

    pthread_mutex_lock(&pool->mu);
    while (!pool->d_head && pool->outstanding > 0) {
        if (pthread_cond_timedwait(&pool->cv_done, &pool->mu, &until) != 0) break;
    }
    pthread_mutex_unlock(&pool->mu);
}

uint64_t store_pool_bytes(const store_pool_t *pool)
{
    return pool ? __atomic_load_n(&pool->bytes, __ATOMIC_RELAXED) : 0;
}

void store_pool_finish(store_pool_t *pool, int abort, phase_timers_t *timers)
{
    if (!pool) return;
    if (abort) {
        __atomic_store_n(&pool->stop, 1, __ATOMIC_RELEASE);
        /* Queued files are never started: close them now rather than let
         * each worker pick one up only to refuse it. */
        pthread_mutex_lock(&pool->mu);
        store_pool_job_t *q = pool->q_head;
        pool->q_head = pool->q_tail = NULL;
        pthread_mutex_unlock(&pool->mu);
        free_list(q);
    }
    pool_join(pool);

    if (timers) {
        for (unsigned i = 0; i < pool->n_workers; i++)
            phase_merge(timers, &pool->workers[i].timers);
        if (pool->n_workers > timers->store_threads)
            timers->store_threads = pool->n_workers;
    }
    pool_destroy(pool);
}
