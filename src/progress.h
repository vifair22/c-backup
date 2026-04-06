#pragma once

/*
 * Unified decoupled progress display.
 *
 * Spawns a 1 Hz thread that samples atomic counters and renders a
 * single-line progress display on stderr.  Workers just do
 * atomic_fetch_add(&prog.items, 1) / atomic_fetch_add(&prog.bytes, n).
 *
 * Usage:
 *   progress_t prog = {
 *       .label       = "verify:",
 *       .unit        = "objects",
 *       .total_items = n,
 *       .total_bytes = total,
 *   };
 *   progress_start(&prog);
 *   // ... atomic_fetch_add(&prog.items, 1); ...
 *   progress_end(&prog);
 */

#include "util.h"
#include <pthread.h>
#include <stdatomic.h>

typedef struct progress progress_t;

/* Custom format callback.  If non-NULL, called instead of the default
 * line formatter.  Must write a single-line string into buf. */
typedef void (*progress_fmt_fn)(char *buf, size_t bufsz,
                                const progress_t *p, void *ctx);

struct progress {
    /* ---- Configuration (set before progress_start) ---- */
    const char      *label;        /* prefix, e.g. "verify:" */
    const char      *unit;         /* counter noun, e.g. "objects"; NULL=omit */
    uint64_t         total_items;  /* 0 = unknown total */
    uint64_t         total_bytes;  /* 0 = skip byte-based ETA + done/total GiB */
    progress_fmt_fn  fmt;          /* NULL = default format */
    void            *fmt_ctx;      /* passed to custom fmt callback */

    /* ---- Counters (update via atomic_fetch_add) ---- */
    _Atomic uint64_t items;
    _Atomic uint64_t bytes;
    _Atomic int      stop;

    /* ---- Computed by progress thread (readable by custom fmt) ---- */
    double           ema_bps;      /* bytes/sec EMA */
    double           ema_ips;      /* items/sec EMA */
    int              samples;      /* EMA sample count */

    /* ---- Private ---- */
    uint64_t         _last_bytes;
    uint64_t         _last_items;
    struct timespec  _last_t;
    size_t           _line_len;
    pthread_t        _thread;
    int              _started;
};

/* ---- Default formatter ----
 * "label: done/total unit  X.X/Y.Y GiB  Z.Z MiB/s  ETA Wm"
 * Sections are omitted when data is unavailable. */
static inline void progress_default_fmt_(char *buf, size_t sz,
                                          const progress_t *p,
                                          uint64_t ci, uint64_t cb) {
    int off = 0;

    /* Label + item counter */
    if (p->total_items > 0 && p->unit)
        off = snprintf(buf, sz, "%s %llu/%llu %s", p->label,
                       (unsigned long long)ci,
                       (unsigned long long)p->total_items, p->unit);
    else if (p->total_items > 0)
        off = snprintf(buf, sz, "%s %llu/%llu", p->label,
                       (unsigned long long)ci,
                       (unsigned long long)p->total_items);
    else if (p->unit)
        off = snprintf(buf, sz, "%s %llu %s", p->label,
                       (unsigned long long)ci, p->unit);
    else
        off = snprintf(buf, sz, "%s %llu", p->label,
                       (unsigned long long)ci);
    if (off < 0 || (size_t)off >= sz) return;

    /* GiB: show done/total when total_bytes known, done-only otherwise */
    if (cb > 0) {
        int n;
        if (p->total_bytes > 0)
            n = snprintf(buf + off, sz - (size_t)off, "  %.1f/%.1f GiB",
                         (double)cb              / (1024.0*1024.0*1024.0),
                         (double)p->total_bytes  / (1024.0*1024.0*1024.0));
        else
            n = snprintf(buf + off, sz - (size_t)off, "  %.1f GiB",
                         (double)cb / (1024.0*1024.0*1024.0));
        if (n > 0) off += n;
    }

    /* Throughput (after 3-sample warmup) */
    if (p->samples >= 3 && p->ema_bps > 0.0) {
        int n = snprintf(buf + off, sz - (size_t)off, "  %.1f MiB/s",
                         p->ema_bps / (1024.0*1024.0));
        if (n > 0) off += n;
    }

    /* ETA: prefer byte-based, fall back to item-based */
    if (p->samples >= ETA_WARMUP_SAMPLES) {
        double rem = -1.0;
        if (p->total_bytes > 0 && p->ema_bps > 0.0 && p->total_bytes > cb)
            rem = (double)(p->total_bytes - cb) / p->ema_bps;
        else if (p->total_items > 0 && p->ema_ips > 0.0 && p->total_items > ci)
            rem = (double)(p->total_items - ci) / p->ema_ips;
        if (rem >= 0.0) {
            char eta[32];
            fmt_eta(rem, eta, sizeof(eta));
            snprintf(buf + off, sz - (size_t)off, "  ETA %s", eta);
        }
    }
}

/* ---- Progress thread (1 Hz sampling) ---- */
static inline void *progress_thread_fn_(void *arg) {
    progress_t *p = arg;
    for (;;) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
        if (atomic_load(&p->stop)) break;

        uint64_t ci = atomic_load(&p->items);
        uint64_t cb = atomic_load(&p->bytes);

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double dt = (double)(now.tv_sec  - p->_last_t.tv_sec) +
                    (double)(now.tv_nsec - p->_last_t.tv_nsec) / 1e9;
        if (dt > 0.0) {
            double ibps = (double)(cb - p->_last_bytes) / dt;
            double iips = (double)(ci - p->_last_items) / dt;
            if (p->ema_bps <= 0.0) p->ema_bps = ibps;
            else                    p->ema_bps = 0.3 * ibps + 0.7 * p->ema_bps;
            if (p->ema_ips <= 0.0) p->ema_ips = iips;
            else                    p->ema_ips = 0.3 * iips + 0.7 * p->ema_ips;
            p->samples++;
        }
        p->_last_bytes = cb;
        p->_last_items = ci;
        p->_last_t     = now;

        char line[192];
        if (p->fmt)
            p->fmt(line, sizeof(line), p, p->fmt_ctx);
        else
            progress_default_fmt_(line, sizeof(line), p, ci, cb);
        progress_line_set(&p->_line_len, line);
    }
    return NULL;
}

/* Spawn the 1 Hz progress thread.  No-op if stderr is not a tty. */
static inline void progress_start(progress_t *p) {
    atomic_store(&p->items, 0);
    atomic_store(&p->bytes, 0);
    atomic_store(&p->stop, 0);
    p->ema_bps     = 0.0;
    p->ema_ips     = 0.0;
    p->samples     = 0;
    p->_last_bytes = 0;
    p->_last_items = 0;
    p->_line_len   = 0;
    p->_started    = 0;
    clock_gettime(CLOCK_MONOTONIC, &p->_last_t);
    if (progress_enabled()) {
        if (pthread_create(&p->_thread, NULL, progress_thread_fn_, p) == 0)
            p->_started = 1;
    }
}

/* Stop the progress thread and clear the display line. */
static inline void progress_end(progress_t *p) {
    if (!p->_started) return;
    atomic_store(&p->stop, 1);
    pthread_join(p->_thread, NULL);
    progress_line_clear(&p->_line_len);
    p->_started = 0;
}
