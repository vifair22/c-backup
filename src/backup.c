#define _POSIX_C_SOURCE 200809L
#include "backup.h"
#include "scan.h"
#include "object.h"
#include "snapshot.h"
#include "util.h"
#include "../vendor/log.h"

#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

static void fmt_bytes(uint64_t n, char *buf, size_t sz) {
    if      (n >= (uint64_t)1024*1024*1024)
        snprintf(buf, sz, "%.1f GB", (double)n / (1024.0*1024*1024));
    else if (n >= 1024*1024)
        snprintf(buf, sz, "%.1f MB", (double)n / (1024.0*1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1f KB", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%" PRIu64 " B", n);
}

static const char *status_name(status_t st) {
    switch (st) {
    case OK: return "OK";
    case ERR_IO: return "ERR_IO";
    case ERR_CORRUPT: return "ERR_CORRUPT";
    case ERR_NOMEM: return "ERR_NOMEM";
    case ERR_INVALID: return "ERR_INVALID";
    case ERR_NOT_FOUND: return "ERR_NOT_FOUND";
    default: return "ERR_UNKNOWN";
    }
}

static size_t g_phase_line_len = 0;
static int ts_ge(const struct timespec *a, const struct timespec *b);
static struct timespec ts_add_ms(struct timespec t, long ms);

static int phase_ui_enabled(const backup_opts_t *opts) {
    return (!opts || !opts->quiet) &&
           (getenv("CBACKUP_PROGRESS") || isatty(STDERR_FILENO));
}

static void phase_line_setf(const char *fmt, ...) {
    char msg[256];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    if (n < 0) return;
    size_t len = (size_t)n;
    if (len >= sizeof(msg)) len = sizeof(msg) - 1;
    msg[len] = '\0';
    progress_line_set(&g_phase_line_len, msg);
}

#define phase_line_clear() progress_line_clear(&g_phase_line_len)

typedef struct {
    struct timespec next_update;
} scan_progress_state_t;

typedef struct {
    struct timespec next_update;
} phase2_progress_state_t;

static void scan_progress_clear_cb(void *ctx) {
    (void)ctx;
    phase_line_clear();
}

static void scan_progress_cb(uint32_t scanned_entries, void *ctx) {
    scan_progress_state_t *st = ctx;
    if (st) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (scanned_entries != 1 && !ts_ge(&now, &st->next_update)) return;
        st->next_update = ts_add_ms(now, 1000);
    }
    phase_line_setf("Phase 1: scanning (%u entries)", scanned_entries);
}

static void phase2_progress_cb(uint32_t done, uint32_t total, void *ctx) {
    phase2_progress_state_t *st = ctx;
    if (st) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (done != 0 && done != total && !ts_ge(&now, &st->next_update)) return;
        st->next_update = ts_add_ms(now, 1000);
    }
    phase_line_setf("Phase 2: loading previous snapshot (%u/%u)", done, total);
}

/* elapsed_sec: uses elapsed_sec from util.h */

static double elapsed_between(const struct timespec *a, const struct timespec *b) {
    time_t ds = b->tv_sec - a->tv_sec;
    long dn = b->tv_nsec - a->tv_nsec;
    return (double)ds + (double)dn / 1000000000.0;
}

static int ts_ge(const struct timespec *a, const struct timespec *b) {
    if (a->tv_sec != b->tv_sec) return a->tv_sec > b->tv_sec;
    return a->tv_nsec >= b->tv_nsec;
}

static struct timespec ts_add_ms(struct timespec t, long ms) {
    t.tv_sec += ms / 1000;
    t.tv_nsec += (ms % 1000) * 1000000L;
    if (t.tv_nsec >= 1000000000L) {
        t.tv_sec += 1;
        t.tv_nsec -= 1000000000L;
    }
    return t;
}

/* fmt_eta: uses fmt_eta from util.h */

/* Change classification */
#define CHANGE_UNCHANGED     0
#define CHANGE_CREATED       1
#define CHANGE_MODIFIED      2
#define CHANGE_METADATA_ONLY 3

/* Per-worker context for the per-chunk progress callback. */
typedef struct {
    /* Pool's shared in-flight counter.  Incremented per chunk, then
     * decremented by the same amount when the file write loop finishes. */
    _Atomic uint64_t *bytes_in_flight;
    uint64_t          accumulated;   /* how much this callback chain added */
} store_cb_ctx_t;

static void store_chunk_cb(uint64_t chunk_bytes, void *ctx) {
    store_cb_ctx_t *c = ctx;
    atomic_fetch_add(c->bytes_in_flight, chunk_bytes);
    c->accumulated += chunk_bytes;
}

/* Store a file's content using sparse-aware storage (with per-chunk callback). */
static status_t store_file_content_cb(repo_t *repo, const char *path,
                                      uint64_t file_size,
                                      uint8_t out_hash[OBJECT_HASH_SIZE],
                                      uint64_t *out_phys_new,
                                      xfer_progress_fn cb, void *cb_ctx) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) return set_error_errno(ERR_IO, "store: open(%s)", path);
    int is_new = 0;
    uint64_t phys = 0;
    status_t st = object_store_file_cb(repo, fd, file_size, out_hash,
                                       &is_new, &phys, cb, cb_ctx);
    close(fd);
    if (out_phys_new) *out_phys_new = is_new ? phys : 0;
    return st;
}

typedef struct {
    uint32_t count;
} deleted_ctx_t;

typedef struct {
    uint64_t *ids;
    uint32_t  count;
    uint32_t  cap;
} skipped_ids_t;

static int skipped_cmp(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static int skipped_has(const skipped_ids_t *s, uint64_t id) {
    if (s->count == 0) return 0;
    return bsearch(&id, s->ids, s->count, sizeof(uint64_t), skipped_cmp) != NULL;
}

static status_t skipped_add(skipped_ids_t *s, uint64_t id) {
    if (s->count == s->cap) {
        uint32_t nc = s->cap ? s->cap * 2 : 16;
        uint64_t *tmp = realloc(s->ids, nc * sizeof(*tmp));
        if (!tmp) return set_error(ERR_NOMEM, "skipped_add: realloc failed");
        s->ids = tmp;
        s->cap = nc;
    }
    s->ids[s->count++] = id;
    return OK;
}

static void deleted_cb(const char *path, const node_t *node, void *ctx_) {
    (void)path;
    (void)node;
    deleted_ctx_t *ctx = ctx_;
    ctx->count++;
}

/* ------------------------------------------------------------------ */
/* Parallel content storage (Phase 3b)                                */
/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t idx;
    int      has_prev;
    node_t   prev;
} store_task_t;

typedef struct {
    repo_t        *repo;
    scan_entry_t  *entries;    /* full scan array */
    store_task_t  *tasks;      /* entries needing content store */
    uint32_t       queue_len;
    uint32_t       next;       /* next queue slot to claim */
    uint32_t       done;       /* number of queue items fully processed */
    uint64_t       total_bytes;
    uint64_t       bytes_done;
    uint64_t       phys_new_bytes;
    int            show_progress;
    struct timespec started_at;
    uint32_t       skipped_transient;
    char           first_skipped_path[PATH_MAX];
    int            first_skipped_errno;
    char           first_error_path[PATH_MAX];
    int            first_error_errno;
    pthread_mutex_t  mu;
    status_t         first_error;
    /* ---- decoupled progress thread ---- */
    _Atomic uint64_t bytes_in_flight;  /* per-chunk, no lock needed */
    _Atomic uint32_t files_active;     /* workers currently mid-file */
    _Atomic int      progress_stop;
    pthread_t        progress_thr;
} store_pool_t;

/* Dedicated 1-second progress thread for Phase 3.
 * Samples bytes_done (mutex) + bytes_in_flight (atomic) every second,
 * computing an EMA rate and ETA independently of when files complete. */
static void *phase3_progress_fn(void *arg) {
    store_pool_t *pool = arg;
    uint64_t      last_seen = 0;
    uint32_t      last_done = 0;
    struct timespec last_t;
    clock_gettime(CLOCK_MONOTONIC, &last_t);
    double ema_bps = 0.0, ema_speed = 0.0;

    for (;;) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
        if (atomic_load(&pool->progress_stop)) break;

        /* Sample completed bytes under lock + in-flight bytes atomically. */
        pthread_mutex_lock(&pool->mu);
        uint64_t bd       = pool->bytes_done;
        uint32_t done_now = pool->done;
        pthread_mutex_unlock(&pool->mu);
        uint64_t seen    = bd + (uint64_t)atomic_load(&pool->bytes_in_flight);
        uint32_t active  = (uint32_t)atomic_load(&pool->files_active);

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double dt = elapsed_between(&last_t, &now);
        if (dt > 0.0) {
            double inst_bps   = (double)(seen    - last_seen) / dt;
            double inst_speed = (double)(done_now - last_done) / dt;
            if (ema_bps <= 0.0)   ema_bps   = inst_bps;
            else                   ema_bps   = 0.12 * inst_bps   + 0.88 * ema_bps;
            if (ema_speed <= 0.0) ema_speed = inst_speed;
            else                   ema_speed = 0.12 * inst_speed + 0.88 * ema_speed;
        }
        last_seen = seen;
        last_done = done_now;
        last_t    = now;

        double rem = (ema_bps > 0.0 && pool->total_bytes > seen)
                   ? (double)(pool->total_bytes - seen) / ema_bps
                   : 0.0;
        char eta[32];
        fmt_eta(rem, eta, sizeof(eta));
        if (active > 0)
            phase_line_setf("Phase 3: %u/%u objects  %u writing  %.1f/%.1f GiB  %.1f MiB/s  ETA %s",
                            done_now, pool->queue_len, active,
                            (double)seen              / (1024.0 * 1024.0 * 1024.0),
                            (double)pool->total_bytes / (1024.0 * 1024.0 * 1024.0),
                            ema_bps / (1024.0 * 1024.0), eta);
        else
            phase_line_setf("Phase 3: %u/%u objects  %.1f/%.1f GiB  %.1f MiB/s  ETA %s",
                            done_now, pool->queue_len,
                            (double)seen              / (1024.0 * 1024.0 * 1024.0),
                            (double)pool->total_bytes / (1024.0 * 1024.0 * 1024.0),
                            ema_bps / (1024.0 * 1024.0), eta);
    }
    return NULL;
}

static void *store_worker_fn(void *arg) {
    store_pool_t *pool = arg;
    for (;;) {
        pthread_mutex_lock(&pool->mu);
        uint32_t qi = pool->next++;
        pthread_mutex_unlock(&pool->mu);
        if (qi >= pool->queue_len) break;

        store_task_t *t = &pool->tasks[qi];
        scan_entry_t *e = &pool->entries[t->idx];
        uint64_t bytes_for_task = e->node.size;
        uint64_t phys_new = 0;

        atomic_fetch_add(&pool->files_active, 1);

        /* Per-call context tracks how many bytes this callback chain adds to
         * bytes_in_flight so we can retire exactly that amount on completion. */
        store_cb_ctx_t cb_ctx = {
            .bytes_in_flight = &pool->bytes_in_flight,
            .accumulated     = 0,
        };
        status_t st = store_file_content_cb(pool->repo, e->path,
                                            e->node.size, e->node.content_hash,
                                            &phys_new,
                                            store_chunk_cb, &cb_ctx);
        if (st != OK) {
            int err = errno;
            if (err == ENOENT || err == EACCES || err == EPERM || err == ESTALE || err == EIO) {
                pthread_mutex_lock(&pool->mu);
                pool->skipped_transient++;
                if (pool->first_skipped_path[0] == '\0') {
                    if (snprintf(pool->first_skipped_path, sizeof(pool->first_skipped_path),
                                 "%s", e->path) >= (int)sizeof(pool->first_skipped_path)) {
                        pool->first_skipped_path[0] = '\0';
                    }
                    pool->first_skipped_errno = err;
                }
                pthread_mutex_unlock(&pool->mu);

                if (t->has_prev) {
                    e->node = t->prev;
                } else {
                    e->node.type = 0;
                }
                st = OK;
            }

            if (st != OK) {
                pthread_mutex_lock(&pool->mu);
                if (pool->first_error == OK) {
                    pool->first_error = st;
                    pool->first_error_errno = err;
                    if (snprintf(pool->first_error_path, sizeof(pool->first_error_path),
                                 "%s", e->path) >= (int)sizeof(pool->first_error_path)) {
                        pool->first_error_path[0] = '\0';
                    }
                }
                pthread_mutex_unlock(&pool->mu);
                return (void *)1;
            }
        }

        /* Retire in-flight bytes and mark this file no longer active. */
        atomic_fetch_sub(&pool->bytes_in_flight, cb_ctx.accumulated);
        atomic_fetch_sub(&pool->files_active, 1);

        pthread_mutex_lock(&pool->mu);
        pool->done++;
        pool->bytes_done += bytes_for_task;
        pool->phys_new_bytes += phys_new;
        pthread_mutex_unlock(&pool->mu);
    }
    return (void *)0;
}

static status_t store_parallel(repo_t *repo, scan_entry_t *entries,
                                 store_task_t *tasks, uint32_t queue_len,
                                 int show_progress, uint32_t *out_skipped,
                                 uint64_t *out_phys_new_bytes) {
    if (queue_len == 0) {
        if (out_skipped) *out_skipped = 0;
        if (out_phys_new_bytes) *out_phys_new_bytes = 0;
        return OK;
    }

    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;
    {
        const char *env = getenv("CBACKUP_STORE_THREADS");
        if (env && *env) {
            char *end = NULL;
            long v = strtol(env, &end, 10);
            if (end != env && *end == '\0' && v > 0) nthreads = (int)v;
        }
    }
    if (nthreads > 256) nthreads = 256;
    if ((uint32_t)nthreads > queue_len) nthreads = (int)queue_len;

    store_pool_t pool = {
        .repo          = repo,
        .entries       = entries,
        .tasks         = tasks,
        .queue_len     = queue_len,
        .next          = 0,
        .done          = 0,
        .total_bytes   = 0,
        .bytes_done    = 0,
        .show_progress = show_progress,
        .first_error   = OK,
    };
    for (uint32_t i = 0; i < queue_len; i++)
        pool.total_bytes += entries[tasks[i].idx].node.size;
    clock_gettime(CLOCK_MONOTONIC, &pool.started_at);
    atomic_init(&pool.bytes_in_flight, 0);
    atomic_init(&pool.files_active,    0);
    atomic_init(&pool.progress_stop,   0);
    pthread_mutex_init(&pool.mu, NULL);

    pthread_t *threads = malloc((size_t)nthreads * sizeof(pthread_t));
    if (!threads) { pthread_mutex_destroy(&pool.mu); return set_error(ERR_NOMEM, "store_parallel: alloc threads failed"); }

    /* Spawn the decoupled progress thread before the workers so it catches
     * activity from the very first chunk. */
    int progress_thr_started = 0;
    if (pool.show_progress) {
        if (pthread_create(&pool.progress_thr, NULL, phase3_progress_fn, &pool) == 0)
            progress_thr_started = 1;
    }

    int n_started = 0;
    for (int i = 0; i < nthreads; i++) {
        if (pthread_create(&threads[i], NULL, store_worker_fn, &pool) != 0) break;
        n_started++;
    }

    if (n_started == 0) {
        if (progress_thr_started) {
            atomic_store(&pool.progress_stop, 1);
            pthread_join(pool.progress_thr, NULL);
        }
        phase_line_clear();
        set_error(ERR_IO, "failed to start store worker thread");
        free(threads);
        pthread_mutex_destroy(&pool.mu);
        return ERR_IO;
    }
    if (n_started < nthreads) {
        if (progress_thr_started) {
            atomic_store(&pool.progress_stop, 1);
            pthread_join(pool.progress_thr, NULL);
        }
        phase_line_clear();
        set_error(ERR_IO, "failed to start all requested store workers");
        for (int i = 0; i < n_started; i++) {
            (void)pthread_join(threads[i], NULL);
        }
        free(threads);
        pthread_mutex_destroy(&pool.mu);
        return ERR_IO;
    }

    for (int i = 0; i < n_started; i++) {
        void *ret = NULL;
        if (pthread_join(threads[i], &ret) != 0) {
            if (pool.first_error == OK) pool.first_error = ERR_IO;
            set_error(ERR_IO, "store worker thread join failed");
            continue;
        }
        if (ret != 0 && pool.first_error == OK) {
            pool.first_error = ERR_IO;
        }
    }

    /* Stop the progress thread before touching the display for error messages. */
    if (progress_thr_started) {
        atomic_store(&pool.progress_stop, 1);
        pthread_join(pool.progress_thr, NULL);
    }

    if (pool.done < queue_len && pool.first_error == OK) {
        pool.first_error = ERR_IO;
        phase_line_clear();
        set_error(ERR_IO, "store worker exited early before queue completion");
    }

    if (pool.first_error != OK) {
        phase_line_clear();
        if (pool.first_error_path[0]) {
            if (pool.first_error_errno != 0) {
                set_error(pool.first_error,
                          "store failed at path '%s': %s (%s)",
                          pool.first_error_path, status_name(pool.first_error),
                          strerror(pool.first_error_errno));
            } else {
                set_error(pool.first_error,
                          "store failed at path '%s': %s",
                          pool.first_error_path, status_name(pool.first_error));
            }
        } else if (_err_buf[0] == '\0') {
            set_error(pool.first_error, "store failed: %s",
                      status_name(pool.first_error));
        }
    }

    if (out_skipped) *out_skipped = pool.skipped_transient;
    if (out_phys_new_bytes) *out_phys_new_bytes = pool.phys_new_bytes;

    if (pool.skipped_transient > 0 && pool.first_skipped_path[0]) {
        char msg[PATH_MAX + 192];
        snprintf(msg, sizeof(msg),
                 "skipped %u transiently unavailable file(s), first: '%s' (%s)",
                 pool.skipped_transient,
                 pool.first_skipped_path,
                 strerror(pool.first_skipped_errno));
        log_msg("WARN", msg);
    }

    if (pool.show_progress) {
        double sec = elapsed_sec(&pool.started_at);
        double bps = sec > 0.0 ? (double)pool.bytes_done / sec : 0.0;
        char eta[32];
        fmt_eta(0.0, eta, sizeof(eta));
        phase_line_setf("Phase 3: %u/%u objects  %.1f/%.1f GiB  %.1f MiB/s  ETA %s",
                        pool.done, pool.queue_len,
                        (double)pool.bytes_done   / (1024.0 * 1024.0 * 1024.0),
                        (double)pool.total_bytes  / (1024.0 * 1024.0 * 1024.0),
                        bps / (1024.0 * 1024.0), eta);
    }

    free(threads);
    pthread_mutex_destroy(&pool.mu);
    return pool.first_error;
}

/* ------------------------------------------------------------------ */

status_t backup_run(repo_t *repo, const char **source_paths, int path_count) {
    return backup_run_opts(repo, source_paths, path_count, NULL);
}

status_t backup_run_opts(repo_t *repo, const char **source_paths, int path_count,
                         const backup_opts_t *opts) {
    if (!repo || !source_paths || path_count <= 0) return set_error(ERR_INVALID, "backup: invalid arguments");

    int tui = phase_ui_enabled(opts);
    const char *fail_ctx = "startup";

    /* ----------------------------------------------------------------
     * Phase 1: Scan source trees (shared imap across all roots)
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 1: scanning");

    scan_imap_t *imap = scan_imap_new();
    if (!imap) return set_error(ERR_NOMEM, "backup: alloc inode map failed");

    /* Build scan_opts_t from backup_opts_t */
    scan_opts_t sopts = {0};
    scan_progress_state_t scan_prog = {0};
    if (opts) {
        sopts.exclude = opts->exclude;
        sopts.n_exclude = opts->n_exclude;
        sopts.verbose = opts->verbose;
    }
    int strict_meta = (opts && opts->strict_meta);
    sopts.collect_meta = strict_meta ? 1 : 0;
    if (tui) {
        clock_gettime(CLOCK_MONOTONIC, &scan_prog.next_update);
        sopts.progress_cb = scan_progress_cb;
        sopts.progress_clear_cb = scan_progress_clear_cb;
        sopts.progress_ctx = &scan_prog;
        sopts.progress_every = 1;
        phase_line_setf("Phase 1: scanning");
    }
    const scan_opts_t *sp = &sopts;

    scan_result_t *scan = NULL;
    for (int i = 0; i < path_count; i++) {
        scan_result_t *partial = NULL;
        fail_ctx = "scan";
        status_t st = scan_tree(source_paths[i], imap, sp, &partial);
        if (st != OK) { scan_imap_free(imap); scan_result_free(scan); return st; }
        if (!scan) {
            scan = partial;
        } else {
            for (uint32_t j = 0; j < partial->count; j++) {
                if (scan->count == scan->capacity) {
                    if (scan->capacity > UINT32_MAX / 2) {
                        scan_imap_free(imap);
                        scan_result_free(scan); scan_result_free(partial);
                        return set_error(ERR_NOMEM, "backup: scan capacity overflow");
                    }
                    uint32_t nc = scan->capacity * 2;
                    scan_entry_t *tmp = realloc(scan->entries, (size_t)nc * sizeof(*tmp));
                    if (!tmp) {
                        scan_imap_free(imap);
                        scan_result_free(scan); scan_result_free(partial);
                        return set_error(ERR_NOMEM, "backup: realloc scan entries failed");
                    }
                    scan->entries  = tmp;
                    scan->capacity = nc;
                }
                scan->entries[scan->count++] = partial->entries[j];
                memset(&partial->entries[j], 0, sizeof(partial->entries[j]));
            }
            /* Merge warnings from partial into scan */
            for (uint32_t w = 0; w < partial->warn_count; w++) {
                if (scan->warn_count == scan->warn_cap) {
                    uint32_t nc = scan->warn_cap ? scan->warn_cap * 2 : 16;
                    char **tmp = realloc(scan->warnings, (size_t)nc * sizeof(char *));
                    if (!tmp) break; /* drop warnings on OOM — non-fatal */
                    scan->warnings = tmp;
                    scan->warn_cap = nc;
                }
                scan->warnings[scan->warn_count++] = partial->warnings[w];
                partial->warnings[w] = NULL; /* transferred ownership */
            }
            partial->count = 0;
            scan_result_free(partial);
        }
    }
    scan_imap_free(imap);
    if (!scan) return set_error(ERR_INVALID, "backup: no scan results");

    /* Print scan summary */
    {
        if (tui) phase_line_clear();
        uint64_t total_bytes = 0;
        for (uint32_t i = 0; i < scan->count; i++)
            if (scan->entries[i].node.type == NODE_TYPE_REG)
                total_bytes += scan->entries[i].node.size;
        char sz[32];
        fmt_bytes(total_bytes, sz, sizeof(sz));
        if (!opts || !opts->quiet)
            fprintf(stderr, "scan:    %u file(s)  %s\n", scan->count, sz);
        if (scan->warn_count > 0 && (!opts || !opts->quiet)) {
            fprintf(stderr, "scan:    %u warning(s):\n", scan->warn_count);
            for (uint32_t w = 0; w < scan->warn_count; w++)
                fprintf(stderr, "  %s\n", scan->warnings[w]);
        }
    }

    /* ----------------------------------------------------------------
     * Phase 2: Load previous snapshot + build path map
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 2: loading previous snapshot");

    uint32_t   prev_id   = 0;
    snapshot_t *prev_snap = NULL;
    pathmap_t  *prev_map  = NULL;
    phase2_progress_state_t phase2_prog = {0};
    if (tui) {
        clock_gettime(CLOCK_MONOTONIC, &phase2_prog.next_update);
        phase_line_setf("Phase 2: loading previous snapshot");
    }

    if (snapshot_read_head(repo, &prev_id) == OK && prev_id > 0) {
        fail_ctx = "load previous snapshot";
        if (snapshot_load(repo, prev_id, &prev_snap) == OK) {
            if (pathmap_build_progress(prev_snap, &prev_map,
                                       tui ? phase2_progress_cb : NULL,
                                       tui ? &phase2_prog : NULL) != OK) {
                if (tui) phase_line_clear();
                log_msg("WARN", "could not build previous path map; treating all as new");
                prev_map = NULL;
            }
        }
    }
    if (tui) phase_line_clear();

    /* ----------------------------------------------------------------
     * Phase 3a: Classify entries; store xattr/acl/symlink objects;
     *           queue regular files for parallel content storage.
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 3: compare and store");

    status_t st = OK;

    skipped_ids_t skipped = {0};
    store_task_t *store_queue = malloc(scan->count * sizeof(*store_queue));
    if (!store_queue) { st = ERR_NOMEM; goto done; }
    uint32_t store_qlen = 0;
    uint32_t n_transient_skipped_store = 0;

    uint32_t n_new = 0, n_modified = 0, n_unchanged = 0, n_meta = 0, n_deleted = 0;
    uint32_t n_unreadable = 0;
    uint64_t phys_new_bytes = 0;

    struct timespec cmp_tick = {0};
    if (tui) clock_gettime(CLOCK_MONOTONIC, &cmp_tick);

    for (uint32_t i = 0; i < scan->count; i++) {
        scan_entry_t *e = &scan->entries[i];
        int skip_entry = 0;

        if (tui && tick_due(&cmp_tick))
            phase_line_setf("Phase 3: comparing %u/%u entries", i + 1, scan->count);

        /* Repo-relative path */
        const char *rel = e->path + e->strip_prefix_len;

        /* Hard link secondaries share the primary's objects — nothing to store */
        if (e->hardlink_to_node_id != 0) continue;

        /* --- Step A: look up in previous snapshot --- */
        const node_t *prev = prev_map ? pathmap_lookup(prev_map, rel) : NULL;
        if (prev) pathmap_mark_seen(prev_map, rel);

        /* --- Step B: fast change classification --- */
        int change;
        int content_same = 0;
        int meta_basic_same = 0;
        if (!prev) {
            change = CHANGE_CREATED;
        } else {
            if (e->node.type == NODE_TYPE_REG) {
                content_same = (e->node.size       == prev->size       &&
                                e->node.mtime_sec  == prev->mtime_sec  &&
                                e->node.mtime_nsec == prev->mtime_nsec &&
                                e->node.inode_identity == prev->inode_identity);
            } else {
                content_same = 1;
            }
            meta_basic_same = (e->node.mode == prev->mode &&
                               e->node.uid  == prev->uid  &&
                               e->node.gid  == prev->gid);

            if (!strict_meta && content_same && meta_basic_same) {
                memcpy(e->node.content_hash, prev->content_hash, OBJECT_HASH_SIZE);
                memcpy(e->node.xattr_hash, prev->xattr_hash, OBJECT_HASH_SIZE);
                memcpy(e->node.acl_hash, prev->acl_hash, OBJECT_HASH_SIZE);
                change = CHANGE_UNCHANGED;
            } else if (content_same) {
                change = CHANGE_METADATA_ONLY;
            } else {
                change = CHANGE_MODIFIED;
            }
        }

        /* --- Step C: collect/store xattr+ACL unless fast-unchanged --- */
        if (strict_meta || !(prev && content_same && meta_basic_same)) {
            fail_ctx = "collect metadata";
            st = scan_entry_collect_metadata(e);
            if (st != OK) goto done;

            if (e->xattr_len > 0) {
                fail_ctx = "store xattrs";
                int is_new = 0;
                uint64_t phys = 0;
                st = object_store_ex(repo, OBJECT_TYPE_XATTR,
                                     e->xattr_data, e->xattr_len, e->node.xattr_hash,
                                     &is_new, &phys);
                if (st != OK) goto done;
                if (is_new) phys_new_bytes += phys;
            }
            if (e->acl_len > 0) {
                fail_ctx = "store ACLs";
                int is_new = 0;
                uint64_t phys = 0;
                st = object_store_ex(repo, OBJECT_TYPE_ACL,
                                     e->acl_data, e->acl_len, e->node.acl_hash,
                                     &is_new, &phys);
                if (st != OK) goto done;
                if (is_new) phys_new_bytes += phys;
            }
            free(e->xattr_data);
            e->xattr_data = NULL;
            e->xattr_len = 0;
            free(e->acl_data);
            e->acl_data = NULL;
            e->acl_len = 0;

            if (prev && content_same) {
                int meta_same = (e->node.mode == prev->mode &&
                                 e->node.uid  == prev->uid  &&
                                 e->node.gid  == prev->gid  &&
                                 memcmp(e->node.xattr_hash, prev->xattr_hash,
                                        OBJECT_HASH_SIZE) == 0 &&
                                 memcmp(e->node.acl_hash, prev->acl_hash,
                                        OBJECT_HASH_SIZE) == 0);
                change = meta_same ? CHANGE_UNCHANGED : CHANGE_METADATA_ONLY;
            }
        }

        /* --- Step D: handle content ---
         * Regular files: inherit hash if unchanged, or queue for parallel store.
         * Symlinks: store target inline (tiny — no benefit from parallelising). */
        if (e->node.type == NODE_TYPE_REG) {
            if (change == CHANGE_UNCHANGED || change == CHANGE_METADATA_ONLY) {
                memcpy(e->node.content_hash, prev->content_hash, OBJECT_HASH_SIZE);
            } else if (e->node.size > 0) {
                store_queue[store_qlen].idx = i;
                if (prev) {
                    store_queue[store_qlen].has_prev = 1;
                    store_queue[store_qlen].prev = *prev;
                } else {
                    store_queue[store_qlen].has_prev = 0;
                    memset(&store_queue[store_qlen].prev, 0, sizeof(node_t));
                }
                store_qlen++;
            }
        } else if (e->node.type == NODE_TYPE_SYMLINK && e->symlink_target) {
            if (change == CHANGE_UNCHANGED || change == CHANGE_METADATA_ONLY) {
                memcpy(e->node.content_hash, prev->content_hash, OBJECT_HASH_SIZE);
            } else {
                size_t tlen = strlen(e->symlink_target) + 1;
                int is_new = 0;
                uint64_t phys = 0;
                st = object_store_ex(repo, OBJECT_TYPE_FILE,
                                     e->symlink_target, tlen, e->node.content_hash,
                                     &is_new, &phys);
                if (st != OK) goto done;
                if (is_new) phys_new_bytes += phys;
            }
            free(e->symlink_target);
            e->symlink_target = NULL;
        }

        /* --- Step E: classify counters --- */
        if (skip_entry) continue;
        switch (change) {
        case CHANGE_CREATED:
            n_new++;
            break;
        case CHANGE_MODIFIED:
            n_modified++;
            break;
        case CHANGE_METADATA_ONLY:
            n_meta++;
            break;
        default:
            n_unchanged++;
            break;
        }
    }

    /* Find deleted entries (in prev but unseen during this scan) */
    if (prev_map) {
        deleted_ctx_t dctx = {0};
        pathmap_foreach_unseen(prev_map, deleted_cb, &dctx);
        n_deleted = dctx.count;
    }

    if (tui) phase_line_clear();

    if (!opts || !opts->quiet) {
        fprintf(stderr, "changes: %u new  %u modified  %u unchanged  %u meta-only  %u deleted\n",
                n_new, n_modified, n_unchanged, n_meta, n_deleted);
    }

    if (n_unreadable > 0) {
        if (opts && opts->verbose) {
            fprintf(stderr, "warning: skipped %u unreadable file(s)\n", n_unreadable);
        } else {
            fprintf(stderr,
                    "warning: skipped %u unreadable file(s) (use --verbose to list paths)\n",
                    n_unreadable);
        }
    }

    /* No changes since last backup — skip snapshot creation entirely. */
    if (prev_id > 0 && n_new == 0 && n_modified == 0 && n_meta == 0 && n_deleted == 0) {
        if (!opts || !opts->quiet)
            fprintf(stderr, "no changes since snapshot %u, skipping\n", prev_id);
        goto done;
    }

    /* ----------------------------------------------------------------
     * Phase 3b: Parallel file content storage
     * ---------------------------------------------------------------- */
    if (store_qlen > 0 && (!opts || !opts->quiet)) {
        if (tui) phase_line_setf("Phase 3: storing 0/%u", store_qlen);
        else fprintf(stderr, "storing: %u new object(s)...\n", store_qlen);
    }
    fail_ctx = "store file contents";
    st = store_parallel(repo, scan->entries, store_queue, store_qlen,
                        tui, &n_transient_skipped_store, &phys_new_bytes);
    if (st != OK) goto done;
    if (tui) phase_line_clear();

    if (n_transient_skipped_store > 0) {
        fprintf(stderr,
                "warning: skipped %u file(s) that changed/disappeared during store\n",
                n_transient_skipped_store);
    }

    /* Populate skipped set: new primary entries whose storage failed.
     * Their hardlink secondaries must be excluded from the dirent table. */
    for (uint32_t i = 0; i < store_qlen; i++) {
        if (!store_queue[i].has_prev &&
            scan->entries[store_queue[i].idx].node.type == 0) {
            uint64_t nid = scan->entries[store_queue[i].idx].node.node_id;
            if (nid) {
                st = skipped_add(&skipped, nid);
                if (st != OK) goto done;
            }
        }
    }
    qsort(skipped.ids, skipped.count, sizeof(*skipped.ids), skipped_cmp);

    /* ----------------------------------------------------------------
     * Phase 4 & 5: Build new snapshot
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 4/5: building snapshot");

    snapshot_t *new_snap = calloc(1, sizeof(*new_snap));
    if (!new_snap) { st = ERR_NOMEM; goto done; }
    new_snap->snap_id     = prev_id + 1;
    new_snap->created_sec = (uint64_t)time(NULL);
    new_snap->phys_new_bytes = phys_new_bytes;

    /* Node table: primary nodes only (skip hard link secondaries) */
    uint32_t primary_count = 0;
    for (uint32_t i = 0; i < scan->count; i++)
        if (scan->entries[i].node.type != 0 &&
            scan->entries[i].hardlink_to_node_id == 0) primary_count++;
    new_snap->node_count = primary_count;
    new_snap->nodes      = malloc(primary_count * sizeof(node_t));
    if (!new_snap->nodes) { free(new_snap); st = ERR_NOMEM; goto done; }
    {
        uint32_t ni = 0;
        for (uint32_t i = 0; i < scan->count; i++)
            if (scan->entries[i].node.type != 0 &&
                scan->entries[i].hardlink_to_node_id == 0)
                new_snap->nodes[ni++] = scan->entries[i].node;
    }

    /* Build dirent table: all entries; secondaries reference the primary node_id */
    size_t dirent_buf_sz = 0;
    uint32_t dirent_count = 0;
    for (uint32_t i = 0; i < scan->count; i++) {
        if (scan->entries[i].node.type == 0) continue;
        if (scan->entries[i].hardlink_to_node_id != 0 &&
            skipped_has(&skipped, scan->entries[i].hardlink_to_node_id)) continue;
        const char *name = strrchr(scan->entries[i].path, '/');
        name = name ? name + 1 : scan->entries[i].path;
        dirent_buf_sz += sizeof(dirent_rec_t) + strlen(name);
        dirent_count++;
    }
    new_snap->dirent_data     = malloc(dirent_buf_sz ? dirent_buf_sz : 1);
    new_snap->dirent_data_len = dirent_buf_sz;
    new_snap->dirent_count    = dirent_count;

    if (!new_snap->dirent_data) {
        snapshot_free(new_snap); st = ERR_NOMEM; goto done;
    }

    uint8_t *dp = new_snap->dirent_data;
    for (uint32_t i = 0; i < scan->count; i++) {
        const scan_entry_t *e = &scan->entries[i];
        if (e->node.type == 0) continue;
        if (e->hardlink_to_node_id != 0 && skipped_has(&skipped, e->hardlink_to_node_id))
            continue;
        const char *name = strrchr(e->path, '/');
        name = name ? name + 1 : e->path;
        uint16_t nlen = (uint16_t)strlen(name);
        uint64_t nid  = e->hardlink_to_node_id ? e->hardlink_to_node_id
                                                : e->node.node_id;
        dirent_rec_t dr = {
            .parent_node = e->parent_node_id,
            .node_id     = nid,
            .name_len    = nlen,
        };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, name, nlen);      dp += nlen;
    }

    /* ----------------------------------------------------------------
     * Phase 6: Commit (objects → snapshot → HEAD)
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 6: committing");

    fail_ctx = "write snapshot";
    st = snapshot_write(repo, new_snap);
    if (st != OK) { snapshot_free(new_snap); goto done; }

    fail_ctx = "update HEAD";
    st = snapshot_write_head(repo, new_snap->snap_id);
    if (st == OK) {
        if (!opts || !opts->quiet)
            fprintf(stderr, "snapshot %u committed  (%u entries, %u change(s))\n",
                    new_snap->snap_id, scan->count,
                    n_new + n_modified + n_meta + n_deleted);

        /* ----------------------------------------------------------------
         * Phase 7 (optional): verify every object referenced by the new
         * snapshot actually landed in the object store.
         * ---------------------------------------------------------------- */
        if (opts && opts->verify_after) {
            log_msg("INFO", "Phase 7: verifying stored objects");
            uint32_t missing = 0;
            struct timespec v_tick = {0};
            if (tui) clock_gettime(CLOCK_MONOTONIC, &v_tick);
            for (uint32_t i = 0; i < new_snap->node_count; i++) {
                const node_t *nd = &new_snap->nodes[i];
                int j;
                int c_zero = 1, x_zero = 1, a_zero = 1;
                for (j = 0; j < OBJECT_HASH_SIZE; j++) {
                    if (nd->content_hash[j]) c_zero = 0;
                    if (nd->xattr_hash[j])   x_zero = 0;
                    if (nd->acl_hash[j])     a_zero = 0;
                }
                if (!c_zero && !object_exists(repo, nd->content_hash)) {
                    if (tui) phase_line_clear();
                    missing++;
                    fprintf(stderr, "error: missing content object for node %u\n", i);
                }
                if (!x_zero && !object_exists(repo, nd->xattr_hash)) {
                    if (tui) phase_line_clear();
                    missing++;
                    fprintf(stderr, "error: missing xattr object for node %u\n", i);
                }
                if (!a_zero && !object_exists(repo, nd->acl_hash)) {
                    if (tui) phase_line_clear();
                    missing++;
                    fprintf(stderr, "error: missing acl object for node %u\n", i);
                }
                if (tui && tick_due(&v_tick))
                    phase_line_setf("Phase 7: verifying (%u/%u nodes)",
                                    i + 1, new_snap->node_count);
            }
            if (tui) phase_line_clear();
            if (missing > 0) {
                fprintf(stderr, "error: %u object(s) missing — "
                        "repository may be corrupt\n", missing);
                st = ERR_CORRUPT;
            } else if (!opts->quiet) {
                fprintf(stderr, "verify: all objects present\n");
            }
        }
    }

    snapshot_free(new_snap);

done:
    if (tui) phase_line_clear();
    if (st != OK && _err_buf[0] == '\0') {
        set_error(st, "backup failed in %s: %s", fail_ctx, status_name(st));
    }
    free(skipped.ids);
    free(store_queue);
    pathmap_free(prev_map);
    snapshot_free(prev_snap);
    scan_result_free(scan);

    return st;
}
