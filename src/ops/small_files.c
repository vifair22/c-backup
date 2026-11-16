#define _POSIX_C_SOURCE 200809L

#include "small_files.h"

#include "chunklist.h"
#include "phase_timer.h"
#include "read_budget.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct small_pool {
    store_t            *store;
    const cdc_params_t *cp;
    atomic_int         *cancel;

    pthread_mutex_t     mu;
    pthread_cond_t      work;     /* a batch was queued, or stop */
    pthread_cond_t      fin;      /* a batch finished */
    small_batch_t      *qhead, *qtail;      /* waiting for a worker */
    small_batch_t      *ohead, *otail;      /* submitted, not yet taken */
    unsigned            outstanding, inflight;
    int                 stop;

    pthread_t          *th;
    unsigned            n, started;
    _Atomic uint64_t    busy_ns;
};

/* ------------------------------------------------------------------ */
/* Batches                                                             */
/* ------------------------------------------------------------------ */

small_batch_t *small_batch_new(void)
{
    small_batch_t *b = calloc(1, sizeof(*b));
    if (!b) return NULL;
    b->files = calloc(SMALL_BATCH_FILES, sizeof(*b->files));
    if (!b->files) { free(b); return NULL; }
    b->cap = SMALL_BATCH_FILES;
    return b;
}

int small_batch_add(small_batch_t *b, uint32_t index, const char *path,
                    uint64_t size)
{
    small_file_t *f = &b->files[b->n++];
    memset(f, 0, sizeof(*f));
    f->index = index;
    f->path = path;
    f->size = size;
    b->bytes += size;
    return b->n >= b->cap || b->bytes >= SMALL_BATCH_BYTES;
}

static void file_free_objs(small_file_t *f)
{
    for (size_t k = 0; k < f->nobjs; k++) {
        pack_prepared_free(&f->objs[k].pp);
        free(f->objs[k].bytes);
    }
    free(f->objs);
    f->objs = NULL;
    f->nobjs = f->capobjs = 0;
}

void small_batch_free(small_batch_t *b)
{
    if (!b) return;
    for (uint32_t i = 0; i < b->n; i++) file_free_objs(&b->files[i]);
    if (b->taken) read_budget_release_foreign(b->taken);
    free(b->files);
    free(b);
}

/* ------------------------------------------------------------------ */
/* The worker's sink: prepare and keep, append nothing                 */
/* ------------------------------------------------------------------ */

typedef struct {
    store_t      *store;
    small_file_t *f;
} keep_ctx_t;

static status_t keep_put(void *ctx, uint8_t type, const void *data, size_t len,
                         obj_put_t *out)
{
    keep_ctx_t *k = ctx;
    small_file_t *f = k->f;
    if (f->nobjs == f->capobjs) {
        size_t nc = f->capobjs ? f->capobjs * 2 : 2;
        small_obj_t *t = realloc(f->objs, nc * sizeof(*t));
        if (!t) return set_error(ERR_NOMEM, "small files: object list");
        f->objs = t;
        f->capobjs = nc;
    }
    /* The chunker reuses its buffer once put returns, and the prepared
     * entry points into whatever it was prepared from. */
    uint8_t *copy = malloc(len ? len : 1);
    if (!copy) return set_error(ERR_NOMEM, "small files: object copy");
    if (len) memcpy(copy, data, len);
    small_obj_t *o = &f->objs[f->nobjs];
    status_t st = pack_backup_prepare(k->store, type, copy, len, &o->pp);
    if (st != OK) { free(copy); return st; }
    o->bytes = copy;
    o->len = len;
    f->nobjs++;
    memcpy(out->hash, o->pp.hash, OBJECT_HASH_SIZE);
    /* Whether it is new is the append's to say, on the backup thread. */
    out->is_new = 0;
    out->phys_bytes = 0;
    return OK;
}

static int pool_cancelled(void *ctx)
{
    small_pool_t *pool = ctx;
    return (pool->cancel && atomic_load(pool->cancel)) || store_cancelled(pool->store);
}

static void store_one(small_pool_t *pool, small_file_t *f)
{
    f->ran = 1;
    int fd = open(f->path, O_RDONLY);
    if (fd == -1) {
        f->skipped = 1;
        snprintf(f->err, sizeof(f->err), "cannot open %s, skipping", f->path);
        return;
    }
    /* Replaced by something else since the scan; see the serial path. */
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        close(fd);
        f->skipped = 1;
        snprintf(f->err, sizeof(f->err), "%s is no longer a regular file, skipping",
                 f->path);
        return;
    }
    keep_ctx_t k = { pool->store, f };
    obj_sink_t sink = { .put = keep_put, .ctx = &k };
    cdc_opts_t o;
    memset(&o, 0, sizeof(o));
    o.cancelled = pool_cancelled;
    o.cancelled_ctx = pool;
    /* The batch holds its slot for all of its bytes (small_batch_t.taken);
     * a second acquisition per chunk would be nested. */
    o.unbudgeted = 1;
    cdc_result_t r;
    err_clear();
    f->st = cdc_store_fd(pool->cp, fd, f->size, &sink, &o, &r);
    close(fd);
    if (f->st == OK) memcpy(f->hash, r.hash, OBJECT_HASH_SIZE);
    else {
        snprintf(f->err, sizeof(f->err), "%s", err_msg());
        file_free_objs(f);
    }
}

static void run_batch(small_pool_t *pool, small_batch_t *b)
{
    uint64_t t0 = phase_now();
    b->st = OK;
    for (uint32_t i = 0; i < b->n; i++) {
        if (pool_cancelled(pool)) { b->st = ERR_BUSY; break; }
        store_one(pool, &b->files[i]);
    }
    atomic_fetch_add_explicit(&pool->busy_ns, phase_now() - t0, memory_order_relaxed);
}

static void *worker_main(void *arg)
{
    small_pool_t *pool = arg;
    pthread_mutex_lock(&pool->mu);
    for (;;) {
        while (!pool->qhead && !pool->stop) pthread_cond_wait(&pool->work, &pool->mu);
        if (!pool->qhead) break;
        small_batch_t *b = pool->qhead;
        pool->qhead = b->qnext;
        if (!pool->qhead) pool->qtail = NULL;
        pthread_mutex_unlock(&pool->mu);

        run_batch(pool, b);

        pthread_mutex_lock(&pool->mu);
        b->done = 1;
        pthread_cond_broadcast(&pool->fin);
    }
    pthread_mutex_unlock(&pool->mu);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* The pool                                                            */
/* ------------------------------------------------------------------ */

status_t small_pool_start(store_t *store, const cdc_params_t *cp, unsigned n,
                          unsigned inflight, atomic_int *cancel,
                          small_pool_t **out)
{
    *out = NULL;
    if (!store || !cp || n == 0 || inflight == 0)
        return set_error(ERR_INVALID, "small_pool_start: bad arguments");
    small_pool_t *pool = calloc(1, sizeof(*pool));
    if (!pool) return set_error(ERR_NOMEM, "small files: pool");
    pool->th = calloc(n, sizeof(*pool->th));
    if (!pool->th) { free(pool); return set_error(ERR_NOMEM, "small files: threads"); }
    pool->store = store;
    pool->cp = cp;
    pool->cancel = cancel;
    pool->n = n;
    pool->inflight = inflight;
    pthread_mutex_init(&pool->mu, NULL);
    pthread_cond_init(&pool->work, NULL);
    pthread_cond_init(&pool->fin, NULL);
    for (unsigned i = 0; i < n; i++) {
        if (pthread_create(&pool->th[i], NULL, worker_main, pool) != 0) break;
        pool->started++;
    }
    if (pool->started == 0) {
        small_pool_stop(pool);
        return set_error(ERR_IO, "small files: cannot start a worker");
    }
    *out = pool;
    return OK;
}

void small_pool_submit(small_pool_t *pool, small_batch_t *b)
{
    pthread_mutex_lock(&pool->mu);
    b->next = NULL;
    b->qnext = NULL;
    if (pool->otail) pool->otail->next = b; else pool->ohead = b;
    pool->otail = b;
    if (pool->qtail) pool->qtail->qnext = b; else pool->qhead = b;
    pool->qtail = b;
    pool->outstanding++;
    pthread_cond_signal(&pool->work);
    pthread_mutex_unlock(&pool->mu);
}

unsigned small_pool_outstanding(const small_pool_t *pool)
{
    return pool ? pool->outstanding : 0;
}

unsigned small_pool_capacity(const small_pool_t *pool)
{
    return pool ? pool->inflight : 0;
}

small_batch_t *small_pool_take(small_pool_t *pool, unsigned wait_ms)
{
    pthread_mutex_lock(&pool->mu);
    small_batch_t *b = pool->ohead;
    if (b && !b->done && wait_ms) {
        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_sec  += wait_ms / 1000;
        dl.tv_nsec += (long)(wait_ms % 1000) * 1000000L;
        if (dl.tv_nsec >= 1000000000L) { dl.tv_sec++; dl.tv_nsec -= 1000000000L; }
        while (!b->done) {
            if (pthread_cond_timedwait(&pool->fin, &pool->mu, &dl) != 0) break;
        }
    }
    if (!b || !b->done) {
        pthread_mutex_unlock(&pool->mu);
        return NULL;
    }
    pool->ohead = b->next;
    if (!pool->ohead) pool->otail = NULL;
    pool->outstanding--;
    pthread_mutex_unlock(&pool->mu);
    b->next = NULL;
    return b;
}

uint64_t small_pool_busy_ns(const small_pool_t *pool)
{
    return pool ? atomic_load_explicit(&pool->busy_ns, memory_order_relaxed) : 0;
}

void small_pool_stop(small_pool_t *pool)
{
    if (!pool) return;
    pthread_mutex_lock(&pool->mu);
    pool->stop = 1;
    /* Batches no worker has started are dropped; one a worker is on is
     * finished first (the workers stop between batches, and between files
     * once cancelled). */
    small_batch_t *q = pool->qhead;
    pool->qhead = pool->qtail = NULL;
    for (; q; q = q->qnext) q->done = 1;
    pthread_cond_broadcast(&pool->work);
    pthread_mutex_unlock(&pool->mu);
    for (unsigned i = 0; i < pool->started; i++) pthread_join(pool->th[i], NULL);
    small_batch_t *b = pool->ohead;
    while (b) {
        small_batch_t *next = b->next;
        small_batch_free(b);
        b = next;
    }
    pthread_cond_destroy(&pool->work);
    pthread_cond_destroy(&pool->fin);
    pthread_mutex_destroy(&pool->mu);
    free(pool->th);
    free(pool);
}
