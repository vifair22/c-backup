#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "cdc.h"
#include "read_budget.h"
#include "types.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <linux/magic.h>
#include <unistd.h>

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/params.h>

/* ------------------------------------------------------------------ */
/* Parameters                                                          */
/* ------------------------------------------------------------------ */

/*
 * HKDF-SHA256, extract then expand, 2,048 bytes = 256 little-endian u64.
 * The salt and info strings are the domain separation: no other use of the
 * repository key derives with them, so the table says nothing about the key
 * that the AEAD uses, and vice versa. They are part of the format.
 */
#define CDC_HKDF_SALT "c-backup v3 cdc"
#define CDC_HKDF_INFO "gear table"

static status_t derive_gear(uint64_t gear[256], const uint8_t *key)
{
    static const uint8_t zero_key[32];
    const uint8_t *ikm = key ? key : zero_key;
    uint8_t out[256 * 8];

    EVP_KDF *kdf = EVP_KDF_fetch(NULL, "HKDF", NULL);
    if (!kdf) return set_error(ERR_IO, "cdc: HKDF unavailable");
    EVP_KDF_CTX *ctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!ctx) return set_error(ERR_NOMEM, "cdc: HKDF context");

    char digest[] = "SHA256";
    char salt[] = CDC_HKDF_SALT;
    char info[] = CDC_HKDF_INFO;
    uint8_t ikm_copy[32];
    memcpy(ikm_copy, ikm, sizeof(ikm_copy));
    OSSL_PARAM params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, digest, 0),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, ikm_copy,
                                          sizeof(ikm_copy)),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, salt,
                                          sizeof(salt) - 1),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, info,
                                          sizeof(info) - 1),
        OSSL_PARAM_construct_end(),
    };
    int rc = EVP_KDF_derive(ctx, out, sizeof(out), params);
    EVP_KDF_CTX_free(ctx);
    OPENSSL_cleanse(ikm_copy, sizeof(ikm_copy));
    if (rc != 1) return set_error(ERR_IO, "cdc: HKDF derive failed");

    for (int i = 0; i < 256; i++) {
        uint64_t v = 0;
        for (int b = 0; b < 8; b++) v |= (uint64_t)out[i * 8 + b] << (8 * b);
        gear[i] = v;
    }
    OPENSSL_cleanse(out, sizeof(out));
    return OK;
}

static unsigned log2_exact(uint32_t v)
{
    unsigned b = 0;
    while ((1u << b) < v) b++;
    return b;
}

/*
 * `bits` bits set immediately below the most significant: bits 62 down to
 * 63 - bits. Bit 63 is left out so the scanner can take two bytes per step
 * (FastCDC 2020's rolling two bytes): after the first byte of a pair it
 * holds 2 * fp, and testing that against mask << 1 is exactly testing fp
 * against mask only when the mask does not need the bit the doubling
 * shifted out. Bit 62 still depends on the last 63 bytes.
 */
static uint64_t cut_mask(unsigned bits)
{
    return bits == 0 ? 0 : ((~(uint64_t)0) << (64 - bits)) >> 1;
}

status_t cdc_params_init(cdc_params_t *p, uint32_t min_size, uint32_t avg_size,
                         uint32_t max_size, const uint8_t *key)
{
    if (!p) return set_error(ERR_INVALID, "cdc_params_init: null");
    if (min_size < CDC_MIN_FLOOR || max_size > CDC_MAX_CEILING ||
        !(min_size < avg_size && avg_size < max_size))
        return set_error(ERR_INVALID, "cdc: sizes %u/%u/%u outside %u <= min "
                         "< avg < max <= %u", min_size, avg_size, max_size,
                         CDC_MIN_FLOOR, CDC_MAX_CEILING);
    if ((avg_size & (avg_size - 1)) != 0)
        return set_error(ERR_INVALID, "cdc: average %u is not a power of two",
                         avg_size);
    memset(p, 0, sizeof(*p));
    p->min_size = min_size;
    p->avg_size = avg_size;
    p->max_size = max_size;
    unsigned bits = log2_exact(avg_size);
    p->mask_s = cut_mask(bits + 2);
    p->mask_l = cut_mask(bits - 2);
    p->keyed = key != NULL;
    status_t st = derive_gear(p->gear, key);
    for (int i = 0; i < 256; i++) p->gear_ls[i] = p->gear[i] << 1;
    return st;
}

void cdc_params_default(cdc_params_t *p)
{
    /* Cannot fail for the defaults short of OpenSSL missing HKDF, which
     * every supported build has; a zeroed table would still chunk
     * deterministically, just at max_size. */
    if (cdc_params_init(p, CDC_MIN_DEFAULT, CDC_AVG_DEFAULT, CDC_MAX_DEFAULT,
                        NULL) != OK)
        err_clear();
}

uint64_t cdc_name_hash(const cdc_params_t *p, const void *data, size_t len)
{
    const uint8_t *b = data;
    uint64_t h = 0;
    for (size_t i = 0; i < len; i++) h = (h << 1) + p->gear[b[i]];
    /* The shift-add leaves short names' low bits poorly mixed; the
     * splitmix64 finaliser spreads every input bit over the word. */
    h ^= (uint64_t)len;
    h ^= h >> 30; h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27; h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return h;
}

/* ------------------------------------------------------------------ */
/* Scanner                                                             */
/* ------------------------------------------------------------------ */

size_t cdc_scan(const cdc_params_t *p, cdc_state_t *s,
                const uint8_t *buf, size_t len, int *cut)
{
    size_t i = 0;
    uint32_t n = s->n;
    uint64_t fp = s->fp;
    const uint64_t *gear = p->gear;

    /* Cut-point skipping: nothing before min_size can be a boundary, so
     * those bytes are not hashed at all. */
    if (n < p->min_size) {
        size_t skip = p->min_size - n;
        if (skip >= len) {
            s->n = n + (uint32_t)len;
            *cut = 0;
            return len;
        }
        i = skip;
        n = p->min_size;
    }

    /*
     * Two bytes per step where two are available inside the region, which
     * shortens the dependency chain from shift-add per byte to
     * shift-add-add per pair: (fp << 2) + (gear[b0] << 1) is 2 * fp1, tested
     * against mask << 1 (see cut_mask), then + gear[b1] is fp2. The result
     * is bit-for-bit the one-byte recurrence; the one-byte loops finish a
     * region's odd byte.
     */
    const uint64_t *gls = p->gear_ls;
    uint64_t m = p->mask_s, m2 = m << 1;
    const uint32_t avg = p->avg_size, max = p->max_size;
    while (i + 2 <= len && n + 2 <= avg) {
        fp = (fp << 2) + gls[buf[i]];
        if (!(fp & m2)) { i += 1; goto found; }
        fp += gear[buf[i + 1]];
        i += 2; n += 2;
        if (!(fp & m)) goto found;
    }
    while (i < len && n < avg) {
        fp = (fp << 1) + gear[buf[i]];
        i++; n++;
        if (!(fp & m)) goto found;
    }
    m = p->mask_l; m2 = m << 1;
    while (i + 2 <= len && n + 2 <= max) {
        fp = (fp << 2) + gls[buf[i]];
        if (!(fp & m2)) { i += 1; goto found; }
        fp += gear[buf[i + 1]];
        i += 2; n += 2;
        if (!(fp & m)) goto found;
    }
    while (i < len && n < max) {
        fp = (fp << 1) + gear[buf[i]];
        i++; n++;
        if (!(fp & m)) goto found;
    }
    if (n >= max) goto found;

    s->n = n;
    s->fp = fp;
    *cut = 0;
    return i;

found:
    cdc_reset(s);
    *cut = 1;
    return i;
}

/* ------------------------------------------------------------------ */
/* Workers                                                             */
/* ------------------------------------------------------------------ */

typedef struct cdc_job {
    struct cdc_job   *next;     /* the file's submission order */
    struct cdc_job   *qnext;    /* the workers' queue */
    const obj_sink_t *sink;
    uint8_t          *data;
    size_t            len;
    uint64_t          offset;
    obj_put_t         put;
    status_t          st;
    int               done;
    size_t            cap;      /* data's allocated size, for the pool */
    uint64_t          taken;    /* read budget, given back by the worker */
    char              err[ERR_MSG_MAX];
} cdc_job_t;

struct cdc_workers {
    pthread_mutex_t mu;
    pthread_cond_t  work;   /* a job was queued, or stop */
    pthread_cond_t  fin;    /* a job finished */
    cdc_job_t      *head, *tail;
    int             stop;
    unsigned        n, started;
    pthread_t      *th;
    /*
     * Chunk buffers, recycled. A fresh max_size buffer per chunk is a fresh
     * mapping the kernel faults in and zeroes page by page, which cost one
     * file as much as the copies the buffers exist to avoid (1.4 GB/s
     * against 1.8). Held here rather than per file so a backup of many
     * large files reuses them throughout. All one size: a request for
     * another size bypasses the pool.
     */
    uint8_t       **pool;
    unsigned        npool, pool_cap;
    size_t          pool_size;
};

/* In-flight chunks per file, per worker. Bounds memory by count as well as
 * bytes, since every buffer in flight is max_size. */
#define CDC_JOBS_PER_WORKER 3u

/* poolable: a full max_size buffer, the one size the pool keeps. Smaller
 * ones (a span shorter than max_size) are allocated and freed as needed. */
static uint8_t *buf_get(cdc_workers_t *w, size_t size, int poolable)
{
    uint8_t *b = NULL;
    if (w && poolable) {
        pthread_mutex_lock(&w->mu);
        if (!w->pool_size) w->pool_size = size;
        if (w->npool && w->pool_size == size) b = w->pool[--w->npool];
        pthread_mutex_unlock(&w->mu);
    }
    return b ? b : malloc(size);
}

static void buf_put(cdc_workers_t *w, uint8_t *b, size_t size)
{
    if (!b) return;
    if (w) {
        pthread_mutex_lock(&w->mu);
        int kept = w->pool_size == size && w->npool < w->pool_cap;
        if (kept) w->pool[w->npool++] = b;
        pthread_mutex_unlock(&w->mu);
        if (kept) return;
    }
    free(b);
}

static void *worker_main(void *arg)
{
    cdc_workers_t *w = arg;
    pthread_mutex_lock(&w->mu);
    for (;;) {
        while (!w->head && !w->stop) pthread_cond_wait(&w->work, &w->mu);
        if (!w->head) break;            /* stop, and nothing left */
        cdc_job_t *j = w->head;
        w->head = j->qnext;
        if (!w->head) w->tail = NULL;
        pthread_mutex_unlock(&w->mu);

        err_clear();
        obj_put_t put;
        memset(&put, 0, sizeof(put));
        status_t st = j->sink->put(j->sink->ctx, OBJECT_TYPE_FILE,
                                   j->data, j->len, &put);
        if (j->taken) read_budget_release_foreign(j->taken);

        pthread_mutex_lock(&w->mu);
        j->put = put;
        j->st = st;
        if (st != OK) snprintf(j->err, sizeof(j->err), "%s", err_msg());
        j->done = 1;
        pthread_cond_broadcast(&w->fin);
    }
    pthread_mutex_unlock(&w->mu);
    return NULL;
}

status_t cdc_workers_start(unsigned n, cdc_workers_t **out)
{
    if (!out || n == 0) return set_error(ERR_INVALID, "cdc_workers_start: n=0");
    cdc_workers_t *w = calloc(1, sizeof(*w));
    if (!w) return set_error(ERR_NOMEM, "cdc_workers_start");
    w->th = calloc(n, sizeof(*w->th));
    if (!w->th) { free(w); return set_error(ERR_NOMEM, "cdc_workers_start"); }
    /* Enough for one file's jobs in flight plus its two reader buffers. */
    w->pool_cap = n * CDC_JOBS_PER_WORKER + 2;
    w->pool = calloc(w->pool_cap, sizeof(*w->pool));
    if (!w->pool) { free(w->th); free(w); return set_error(ERR_NOMEM, "cdc_workers_start"); }
    pthread_mutex_init(&w->mu, NULL);
    pthread_cond_init(&w->work, NULL);
    pthread_cond_init(&w->fin, NULL);
    w->n = n;
    for (unsigned i = 0; i < n; i++) {
        if (pthread_create(&w->th[i], NULL, worker_main, w) != 0) {
            cdc_workers_stop(w);
            return set_error(ERR_IO, "cdc: cannot start worker %u", i);
        }
        w->started++;
    }
    *out = w;
    return OK;
}

unsigned cdc_workers_count(const cdc_workers_t *w)
{
    return w ? w->n : 0;
}

void cdc_workers_stop(cdc_workers_t *w)
{
    if (!w) return;
    pthread_mutex_lock(&w->mu);
    w->stop = 1;
    pthread_cond_broadcast(&w->work);
    pthread_mutex_unlock(&w->mu);
    for (unsigned i = 0; i < w->started; i++) pthread_join(w->th[i], NULL);
    pthread_cond_destroy(&w->fin);
    pthread_cond_destroy(&w->work);
    pthread_mutex_destroy(&w->mu);
    for (unsigned i = 0; i < w->npool; i++) free(w->pool[i]);
    free(w->pool);
    free(w->th);
    free(w);
}

/* ------------------------------------------------------------------ */
/* Data spans                                                          */
/* ------------------------------------------------------------------ */

/*
 * The file's data as a sequence of spans, holes under CDC_HOLE_MIN absorbed
 * into the data around them. Walked one span at a time, never collected:
 * a file with millions of extents costs no memory here.
 */
typedef struct {
    int      fd;
    uint64_t size;
    uint64_t pos;       /* everything before this has been returned */
    int      done;
    int      no_seek;   /* SEEK_DATA unsupported: one span, the whole file */
} spans_t;

static off_t seek_data(int fd, uint64_t from, int *nodata)
{
    *nodata = 0;
    off_t r = lseek(fd, (off_t)from, SEEK_DATA);
    if (r == (off_t)-1 && errno == ENXIO) *nodata = 1;
    return r;
}

static uint64_t seek_hole(int fd, uint64_t from, uint64_t size)
{
    off_t r = lseek(fd, (off_t)from, SEEK_HOLE);
    if (r == (off_t)-1 || (uint64_t)r > size) return size;
    return (uint64_t)r;
}

/* OK with *got set and [*s, *e) the next span, or *got 0 at the end. */
static status_t next_span(spans_t *sp, uint64_t *s, uint64_t *e, int *got)
{
    *got = 0;
    if (sp->done || sp->pos >= sp->size) { sp->done = 1; return OK; }
    /* A file shorter than CDC_HOLE_MIN cannot hold a hole that counts, so
     * it is one span by definition and the probes are skipped: two
     * syscalls per file is most of what a small file costs. */
    if (sp->pos == 0 && sp->size < CDC_HOLE_MIN) sp->no_seek = 1;
    if (sp->no_seek) {
        *s = sp->pos; *e = sp->size; *got = 1;
        sp->pos = sp->size; sp->done = 1;
        return OK;
    }

    int nodata = 0;
    off_t ds = seek_data(sp->fd, sp->pos, &nodata);
    if (ds == (off_t)-1) {
        if (!nodata) {
            /* The filesystem cannot answer (EINVAL and the like): all data. */
            sp->no_seek = 1;
            return next_span(sp, s, e, got);
        }
        if (sp->pos == 0) {
            /*
             * No data anywhere. Either the file really is all hole, or a
             * FUSE layer that does not implement SEEK_DATA is saying ENXIO
             * for everything; treating real data as a hole would lose it.
             * The block count tells them apart (as object.c always has).
             */
            struct stat fst;
            if (fstat(sp->fd, &fst) != 0)
                return set_error_errno(ERR_IO, "cdc: fstat");
            if (fst.st_blocks != 0) {
                sp->no_seek = 1;
                return next_span(sp, s, e, got);
            }
        }
        sp->done = 1;
        return OK;
    }

    uint64_t start = (uint64_t)ds;
    if (start >= sp->size) { sp->done = 1; return OK; }
    /* A short leading hole is data. Later short holes never get here: the
     * absorbing loop below has already swallowed them into the span before. */
    if (sp->pos == 0 && start < CDC_HOLE_MIN) start = 0;

    uint64_t end = seek_hole(sp->fd, start, sp->size);
    while (end < sp->size) {
        off_t nd = seek_data(sp->fd, end, &nodata);
        uint64_t next = nd == (off_t)-1 ? sp->size : (uint64_t)nd;
        if (next > sp->size) next = sp->size;
        if (next - end >= CDC_HOLE_MIN) break;      /* a real hole */
        if (next >= sp->size) { end = sp->size; break; }   /* short tail hole */
        end = seek_hole(sp->fd, next, sp->size);
    }

    *s = start;
    *e = end;
    *got = 1;
    sp->pos = end;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Storing a file                                                      */
/* ------------------------------------------------------------------ */

/* Small, so what is read past a cut and has to be copied is small. */
#define CDC_READ_STEP ((size_t)256 * 1024)

typedef struct {
    const cdc_params_t *p;
    const obj_sink_t   *sink;
    const cdc_opts_t   *o;
    cdc_workers_t      *w;
    chunklist_builder_t *b;
    cdc_result_t       *r;
    uint8_t             first[OBJECT_HASH_SIZE];
    uint64_t            first_off, first_len;
    /* Parallel: this file's jobs in submission order. */
    cdc_job_t          *ohead, *otail;
    size_t              cur_cap, spare_cap;   /* the reader's two buffers */
    size_t              inflight, inflight_max;
    unsigned            jobs, jobs_max;
    status_t            err;            /* first failure, sticky */
    char                errmsg[ERR_MSG_MAX];
} drv_t;

static void drv_fail(drv_t *d, status_t st, const char *msg)
{
    if (d->err != OK) return;
    d->err = st;
    snprintf(d->errmsg, sizeof(d->errmsg), "%s", msg);
}

static status_t consume(drv_t *d, uint64_t offset, size_t len, const obj_put_t *put)
{
    cdc_result_t *r = d->r;
    if (put->is_new) {
        r->chunks_new++;
        r->chunk_bytes_new += len;
        r->phys_new += put->phys_bytes;
    }
    /*
     * The builder (and its 786 KB of scratch) only once there is a second
     * chunk: most files are one chunk and never need a list. The first is
     * held and replayed into it.
     */
    if (r->chunks++ == 0) {
        memcpy(d->first, put->hash, OBJECT_HASH_SIZE);
        d->first_off = offset;
        d->first_len = len;
        return OK;
    }
    if (!d->b) {
        status_t st = chunklist_builder_new(OBJECT_TYPE_FILE, d->sink, &d->b);
        if (st == OK) st = chunklist_builder_add(d->b, d->first_off, d->first_len, d->first);
        if (st != OK) return st;
    }
    return chunklist_builder_add(d->b, offset, len, put->hash);
}

/* Take finished jobs off the front of this file's order, waiting for the
 * front one while more than keep_bytes or keep_jobs are in flight. Called
 * with no lock. */
static void drain(drv_t *d, size_t keep_bytes, unsigned keep_jobs)
{
    cdc_workers_t *w = d->w;
    pthread_mutex_lock(&w->mu);
    while (d->ohead) {
        cdc_job_t *j = d->ohead;
        if (!j->done) {
            if (d->inflight <= keep_bytes && d->jobs <= keep_jobs) break;
            pthread_cond_wait(&w->fin, &w->mu);
            continue;
        }
        d->ohead = j->next;
        if (!d->ohead) d->otail = NULL;
        d->inflight -= j->len;
        d->jobs--;
        pthread_mutex_unlock(&w->mu);

        if (j->st != OK) {
            drv_fail(d, j->st, j->err);
        } else if (d->err == OK) {
            status_t st = consume(d, j->offset, j->len, &j->put);
            if (st != OK) drv_fail(d, st, err_msg());
        }
        buf_put(w, j->data, j->cap);
        free(j);
        pthread_mutex_lock(&w->mu);
    }
    pthread_mutex_unlock(&w->mu);
}

/*
 * Hand one chunk on. Serial: put it from the caller's buffer, which stays the
 * caller's. Pooled: the job takes the buffer itself and *bufp becomes NULL;
 * the reader never copies a chunk it has cut, and the buffer goes back to
 * the pool when the job is taken back.
 */
static status_t emit_chunk(drv_t *d, uint8_t **bufp, size_t len,
                           uint64_t offset)
{
    /*
     * A cut chunk is an object buffer in flight until the sink has staged
     * it: it takes its size from the read budget. On the pool the slot
     * travels with the job and the worker gives it back, so this thread,
     * which waits for workers, never holds one while it does.
     */
    if (!d->w) {
        obj_put_t put;
        memset(&put, 0, sizeof(put));
        int budget = !(d->o && d->o->unbudgeted);
        uint64_t taken = budget ? read_budget_acquire(len ? len : 1) : 0;
        status_t st = d->sink->put(d->sink->ctx, OBJECT_TYPE_FILE, *bufp, len, &put);
        if (budget) read_budget_release(taken);
        if (st != OK) return st;
        return consume(d, offset, len, &put);
    }

    cdc_job_t *j = calloc(1, sizeof(*j));
    if (!j) return set_error(ERR_NOMEM, "cdc: chunk job");
    if (!(d->o && d->o->unbudgeted)) {
        j->taken = read_budget_acquire(len ? len : 1);
        read_budget_handoff(j->taken);
    }
    j->sink = d->sink;
    j->data = *bufp;
    j->cap = d->cur_cap;
    *bufp = NULL;
    j->len = len;
    j->offset = offset;

    cdc_workers_t *w = d->w;
    pthread_mutex_lock(&w->mu);
    if (d->otail) d->otail->next = j; else d->ohead = j;
    d->otail = j;
    d->inflight += len;
    d->jobs++;
    if (w->tail) w->tail->qnext = j; else w->head = j;
    w->tail = j;
    pthread_cond_signal(&w->work);
    pthread_mutex_unlock(&w->mu);

    drain(d, d->inflight_max, d->jobs_max);
    if (d->err != OK) return set_error(d->err, "%s", d->errmsg);
    return OK;
}

static status_t read_full_at(int fd, uint8_t *buf, size_t n, uint64_t off)
{
    while (n > 0) {
        ssize_t r = pread(fd, buf, n, (off_t)off);
        if (r < 0) {
            if (errno == EINTR) continue;
            return set_error_errno(ERR_IO, "cdc: read at %llu",
                                   (unsigned long long)off);
        }
        if (r == 0)
            return set_error(ERR_IO, "cdc: file ended at %llu, short of its "
                             "scanned size (changed while being read)",
                             (unsigned long long)off);
        buf += r;
        n -= (size_t)r;
        off += (uint64_t)r;
    }
    return OK;
}

/*
 * Chunk one span.
 *
 * Reads go straight into the buffer of the chunk being cut (max_size bytes),
 * a read step at a time. At a cut, only what was read past it - less than
 * one step - is copied, into the next chunk's buffer. Reading into a window
 * and copying each chunk out cost every byte two extra copies, which
 * capped one file at about 1.8 GB/s however many workers hashed.
 */
static status_t chunk_span(drv_t *d, const cdc_source_t *src,
                           uint64_t s, uint64_t e,
                           uint8_t **cur, uint8_t **spare)
{
    const size_t max = d->p->max_size;
    cdc_state_t cs;
    cdc_reset(&cs);
    size_t blen = 0, scanned = 0;
    uint64_t pos = s, chunk_abs = s;

    /* No chunk of this span can be longer than the span: a small file gets
     * a small buffer, not a max_size mapping made and torn down for it. */
    size_t need = (e - s) < (uint64_t)max ? (size_t)(e - s) : max;
    if (*cur && d->cur_cap < need) {
        buf_put(d->w, *cur, d->cur_cap);
        *cur = NULL;
    }
    if (!*cur) {
        if (!(*cur = buf_get(d->w, need, need == max)))
            return set_error(ERR_NOMEM, "cdc: chunk buffer");
        d->cur_cap = need;
    }

    for (;;) {
        if (scanned == blen) {
            if (pos == e) break;
            if (d->o && (cancel_requested(d->o->cancel) ||
                         (d->o->cancelled && d->o->cancelled(d->o->cancelled_ctx))))
                return set_error(ERR_BUSY, "cancelled");
            /* No cut yet means blen < max and < the span, so there is room. */
            size_t n = d->cur_cap - blen;
            if (n > CDC_READ_STEP) n = CDC_READ_STEP;
            if ((uint64_t)n > e - pos) n = (size_t)(e - pos);
            status_t st = src->read(src->ctx, *cur + blen, n, pos);
            if (st != OK) return st;
            blen += n;
            pos += n;
            d->r->data_bytes += n;
            if (d->o && d->o->progress) d->o->progress(n, d->o->progress_ctx);
        }
        int cut = 0;
        scanned += cdc_scan(d->p, &cs, *cur + scanned, blen - scanned, &cut);
        if (!cut) continue;

        size_t spill = blen - scanned;
        if (*spare && d->spare_cap < need) {
            buf_put(d->w, *spare, d->spare_cap);
            *spare = NULL;
        }
        if (!*spare) {
            if (!(*spare = buf_get(d->w, need, need == max)))
                return set_error(ERR_NOMEM, "cdc: chunk buffer");
            d->spare_cap = need;
        }
        memcpy(*spare, *cur + scanned, spill);
        status_t st = emit_chunk(d, cur, scanned, chunk_abs);
        if (st != OK) return st;
        uint8_t *t = *cur;          /* NULL when the job took it */
        size_t tc = d->cur_cap;
        *cur = *spare;
        d->cur_cap = d->spare_cap;
        *spare = t;
        d->spare_cap = t ? tc : 0;
        chunk_abs += scanned;
        blen = spill;
        scanned = 0;
    }
    /* The span's end is a boundary of its own. */
    if (blen > 0) return emit_chunk(d, cur, blen, chunk_abs);
    return OK;
}

/*
 * The file source: SEEK_DATA spans, and reads that a thread of its own has
 * usually already made.
 *
 * Reads on the cutting thread are synchronous, so on a cold disk each one
 * waited its full latency while nothing else happened, and a file read at
 * half the disk's speed. For a span of 16 MiB or more a read-ahead thread
 * fills a ring of 1 MiB slots in order ahead of the cutter, which copies out
 * of them: the disk and the scan overlap, as object.c's reader thread made
 * them for the format 2 writer. Smaller spans are read directly. What has
 * been read is released from the page cache behind, as that writer did, so
 * a backup does not push the machine's working set out.
 */
#define RA_MIN    ((uint64_t)16 * 1024 * 1024)
#define RA_SLOTS  8u
#define RA_SLOT   ((size_t)1024 * 1024)

typedef struct {
    uint8_t *buf;
    uint64_t off;
    size_t   len;
    int      state;        /* 0 free, 1 filling, 2 ready */
    status_t st;
    char     err[ERR_MSG_MAX];
} ra_slot_t;

typedef struct {
    spans_t  sp;
    int      fd;
    uint64_t dropped;      /* page cache released below here */

    /* Read-ahead for the current span, when it is long enough and the
     * file is on something that has a latency to hide. */
    int             ra_ok;
    int             ra_on;
    pthread_t       ra_th;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    ra_slot_t       slot[RA_SLOTS];
    uint64_t        ra_next, ra_end;   /* the thread's next read, span end */
    unsigned        ra_fill;           /* slot it fills next */
    int             ra_stop;
} fd_src_t;

static void *ra_main(void *arg)
{
    fd_src_t *f = arg;
    pthread_mutex_lock(&f->mu);
    for (;;) {
        ra_slot_t *sl = &f->slot[f->ra_fill];
        while (!f->ra_stop && (sl->state != 0 || f->ra_next >= f->ra_end))
            pthread_cond_wait(&f->cv, &f->mu);
        if (f->ra_stop) break;
        uint64_t off = f->ra_next;
        size_t n = f->ra_end - off < RA_SLOT ? (size_t)(f->ra_end - off) : RA_SLOT;
        sl->state = 1;
        sl->off = off;
        sl->len = n;
        f->ra_next += n;
        f->ra_fill = (f->ra_fill + 1) % RA_SLOTS;
        pthread_mutex_unlock(&f->mu);
        err_clear();
        status_t st = read_full_at(f->fd, sl->buf, n, off);
        pthread_mutex_lock(&f->mu);
        sl->st = st;
        if (st != OK) snprintf(sl->err, sizeof(sl->err), "%s", err_msg());
        sl->state = 2;
        pthread_cond_broadcast(&f->cv);
    }
    pthread_mutex_unlock(&f->mu);
    return NULL;
}

static void ra_stop(fd_src_t *f)
{
    if (!f->ra_on) return;
    pthread_mutex_lock(&f->mu);
    f->ra_stop = 1;
    pthread_cond_broadcast(&f->cv);
    pthread_mutex_unlock(&f->mu);
    pthread_join(f->ra_th, NULL);
    for (unsigned i = 0; i < RA_SLOTS; i++) {
        free(f->slot[i].buf);
        f->slot[i].buf = NULL;
    }
    pthread_cond_destroy(&f->cv);
    pthread_mutex_destroy(&f->mu);
    f->ra_on = 0;
}

static status_t ra_start(fd_src_t *f, uint64_t s, uint64_t e)
{
    memset(f->slot, 0, sizeof(f->slot));
    for (unsigned i = 0; i < RA_SLOTS; i++)
        if (!(f->slot[i].buf = malloc(RA_SLOT))) {
            for (unsigned k = 0; k < i; k++) free(f->slot[k].buf);
            return ERR_NOMEM;   /* read directly instead */
        }
    pthread_mutex_init(&f->mu, NULL);
    pthread_cond_init(&f->cv, NULL);
    f->ra_next = s;
    f->ra_end = e;
    f->ra_fill = 0;
    f->ra_stop = 0;
    if (pthread_create(&f->ra_th, NULL, ra_main, f) != 0) {
        for (unsigned i = 0; i < RA_SLOTS; i++) free(f->slot[i].buf);
        pthread_cond_destroy(&f->cv);
        pthread_mutex_destroy(&f->mu);
        return ERR_IO;
    }
    f->ra_on = 1;
    return OK;
}

static status_t fd_next_span(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    fd_src_t *f = ctx;
    ra_stop(f);
    status_t st = next_span(&f->sp, s, e, got);
    if (st == OK && *got && f->ra_ok && *e - *s >= RA_MIN) (void)ra_start(f, *s, *e);
    return st;
}

static void drop_behind(fd_src_t *f, uint64_t off)
{
    if (off > f->dropped + 2 * RA_MIN) {
        (void)posix_fadvise(f->fd, (off_t)f->dropped, (off_t)(off - f->dropped),
                            POSIX_FADV_DONTNEED);
        f->dropped = off;
    }
}

static status_t fd_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    fd_src_t *f = ctx;
    if (!f->ra_on) {
        status_t st = read_full_at(f->fd, buf, n, off);
        if (st == OK) drop_behind(f, off);
        return st;
    }
    /* The cutter reads in order, so the slot it wants is the oldest one;
     * a slot is given back once it has been read to its end. */
    pthread_mutex_lock(&f->mu);
    status_t st = OK;
    while (n > 0 && st == OK) {
        ra_slot_t *sl = NULL;
        for (unsigned i = 0; i < RA_SLOTS; i++) {
            ra_slot_t *c = &f->slot[i];
            if (c->state != 0 && off >= c->off && off < c->off + c->len) { sl = c; break; }
        }
        if (!sl || sl->state != 2) {
            pthread_cond_wait(&f->cv, &f->mu);
            continue;
        }
        if (sl->st != OK) { st = set_error(sl->st, "%s", sl->err); break; }
        size_t k = (size_t)(sl->off + sl->len - off);
        if (k > n) k = n;
        memcpy(buf, sl->buf + (off - sl->off), k);
        buf += k; n -= k; off += k;
        if (off >= sl->off + sl->len) {
            sl->state = 0;
            pthread_cond_broadcast(&f->cv);
        }
    }
    pthread_mutex_unlock(&f->mu);
    if (st == OK) drop_behind(f, off);
    return st;
}

status_t cdc_store_fd(const cdc_params_t *p, int fd, uint64_t size,
                      const obj_sink_t *sink, const cdc_opts_t *opts,
                      cdc_result_t *out)
{
    /*
     * On the heap: the read-ahead ring holds an error message per slot, and
     * at ERR_MSG_MAX (8 KiB since B121) eight of them alone took this frame
     * past the 64 KiB stack limit (make analyze).
     */
    fd_src_t *f = calloc(1, sizeof(*f));
    if (!f) return set_error(ERR_NOMEM, "cdc: file source");
    f->fd = fd;
    f->sp.fd = fd;
    f->sp.size = size;
    if (size >= CDC_HOLE_MIN) (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    /* Memory-backed: every read is a copy already, and read-ahead would add
     * another one for nothing (a quarter slower, measured on tmpfs). */
    struct statfs sfs;
    f->ra_ok = !(fstatfs(fd, &sfs) == 0 &&
                 (sfs.f_type == TMPFS_MAGIC || sfs.f_type == RAMFS_MAGIC));
    cdc_source_t src = { .next_span = fd_next_span, .read = fd_read, .ctx = f };
    status_t st = cdc_store_source(p, &src, size, sink, opts, out);
    ra_stop(f);
    free(f);
    return st;
}

status_t cdc_store_source(const cdc_params_t *p, const cdc_source_t *src,
                          uint64_t size, const obj_sink_t *sink,
                          const cdc_opts_t *opts, cdc_result_t *out)
{
    if (!p || !src || !src->next_span || !src->read || !sink || !sink->put || !out)
        return set_error(ERR_INVALID, "cdc_store_source: bad arguments");
    memset(out, 0, sizeof(*out));

    if (size == 0) {
        obj_put_t put;
        memset(&put, 0, sizeof(put));
        static const uint8_t empty = 0;
        status_t st = sink->put(sink->ctx, OBJECT_TYPE_FILE, &empty, 0, &put);
        if (st != OK) return st;
        memcpy(out->hash, put.hash, OBJECT_HASH_SIZE);
        out->type = OBJECT_TYPE_FILE;
        out->chunks = 1;
        if (put.is_new) { out->chunks_new = 1; out->phys_new = put.phys_bytes; }
        return OK;
    }

    drv_t d;
    memset(&d, 0, sizeof(d));
    d.p = p;
    d.sink = sink;
    d.o = opts;
    d.w = opts ? opts->workers : NULL;
    d.r = out;
    d.inflight_max = (opts && opts->inflight_max) ? opts->inflight_max
                                                  : (size_t)8 * p->max_size;
    d.jobs_max = d.w ? cdc_workers_count(d.w) * CDC_JOBS_PER_WORKER : 0;

    uint8_t *cur = NULL, *spare = NULL;

    uint64_t covered = 0;
    unsigned nspans = 0;
    int whole = 1;          /* one span covering [0, size) */
    status_t st = OK;
    for (;;) {
        uint64_t s = 0, e = 0;
        int got = 0;
        st = src->next_span(src->ctx, &s, &e, &got);
        if (st != OK || !got) break;
        if (e <= s || e > size) {
            st = set_error(ERR_INVALID, "cdc: span [%llu, %llu) outside %llu bytes",
                           (unsigned long long)s, (unsigned long long)e,
                           (unsigned long long)size);
            break;
        }
        if (nspans++ > 0 || s != 0) whole = 0;
        covered += e - s;
        st = chunk_span(&d, src, s, e, &cur, &spare);
        if (st != OK) break;
    }
    if (covered != size) whole = 0;

    /* Every job this file put out has to be back before anything it points
     * at is freed, failure or not. */
    if (d.w) {
        char keep[ERR_MSG_MAX];
        snprintf(keep, sizeof(keep), "%s", err_msg());
        drain(&d, 0, 0);
        if (st == OK && d.err != OK) st = set_error(d.err, "%s", d.errmsg);
        else if (st != OK) set_error(st, "%s", keep);
    }
    buf_put(d.w, cur, d.cur_cap);
    buf_put(d.w, spare, d.spare_cap);

    if (st == OK && whole && out->chunks == 1) {
        /* One chunk, no holes: the chunk is the file. Nothing was stored
         * for the builder, which only stores when a run closes. */
        memcpy(out->hash, d.first, OBJECT_HASH_SIZE);
        out->type = OBJECT_TYPE_FILE;
    } else if (st == OK) {
        if (!d.b) {
            /* One chunk with holes around it, or none at all. */
            st = chunklist_builder_new(OBJECT_TYPE_FILE, sink, &d.b);
            if (st == OK && out->chunks == 1)
                st = chunklist_builder_add(d.b, d.first_off, d.first_len, d.first);
        }
        if (st == OK) st = chunklist_builder_finish(d.b, size, out->hash);
        if (st == OK) {
            out->type = OBJECT_TYPE_CHUNKLIST;
            chunklist_builder_stats(d.b, &out->lists);
            out->phys_new += out->lists.phys_new;
        }
    }
    chunklist_builder_free(d.b);
    return st;
}
