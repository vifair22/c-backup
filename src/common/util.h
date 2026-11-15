#pragma once

/*
 * Shared inline utilities used across multiple modules.
 *
 * All functions are static inline to avoid link-time symbol conflicts
 * while eliminating code duplication.
 */

#include "types.h"

#include <errno.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Retry-on-EINTR I/O helpers                                         */
/* ------------------------------------------------------------------ */

static inline int io_read_full(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t done = 0;
    while (done < n) {
        ssize_t r = read(fd, p + done, n - done);
        if (r == 0) return -1;
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        done += (size_t)r;
    }
    return 0;
}

static inline int io_write_full(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, p + done, n - done);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Hash helpers                                                       */
/* ------------------------------------------------------------------ */

static inline int hash_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

static inline int hex_decode(const char *hex, size_t hexlen, uint8_t *out) {
    if (hexlen != OBJECT_HASH_SIZE * 2) return -1;
    for (size_t i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned hi, lo;
        char hc = hex[i * 2], lc = hex[i * 2 + 1];
        if      (hc >= '0' && hc <= '9') hi = (unsigned)(hc - '0');
        else if (hc >= 'a' && hc <= 'f') hi = (unsigned)(hc - 'a') + 10u;
        else return -1;
        if      (lc >= '0' && lc <= '9') lo = (unsigned)(lc - '0');
        else if (lc >= 'a' && lc <= 'f') lo = (unsigned)(lc - 'a') + 10u;
        else return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Media detection                                                    */
/* ------------------------------------------------------------------ */

/* Detect if a path resides on a rotational (HDD) device.
 * Returns 1 for HDD, 0 for SSD, -1 for unknown (FUSE, NFS, etc). */
#ifdef __linux__
#include <sys/stat.h>
#include <sys/sysmacros.h>
static inline int path_is_rotational(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    unsigned int maj = major(st.st_dev);
    unsigned int min = minor(st.st_dev);
    char syspath[128];
    FILE *f;
    /* Try partition's own queue first */
    snprintf(syspath, sizeof(syspath),
             "/sys/dev/block/%u:%u/queue/rotational", maj, min);
    f = fopen(syspath, "r");
    if (!f) {
        /* Try parent device (partition → whole disk) */
        snprintf(syspath, sizeof(syspath),
                 "/sys/dev/block/%u:%u/../queue/rotational", maj, min);
        f = fopen(syspath, "r");
    }
    if (!f) return -1;  /* FUSE, NFS, or other virtual FS */
    int val = 0;
    if (fscanf(f, "%d", &val) != 1) val = 0;
    fclose(f);
    return val == 1;
}
#else
static inline int path_is_rotational(const char *path) { (void)path; return -1; }
#endif

/* ------------------------------------------------------------------ */
/* ------------------------------------------------------------------ */
/* Path helpers                                                       */
/* ------------------------------------------------------------------ */

/*
 * Lexical normalisation of an absolute path: repeated slashes collapse, "."
 * components vanish, ".." removes the component before it and stops at the
 * root, and no trailing slash survives except on "/" itself.
 *
 * Lexical on purpose. It touches no filesystem, so it works on paths that do
 * not exist here - a snapshot's contents, an exclude pattern, a restore
 * destination not yet created - at the price of not resolving symlinks.
 * Callers that need to know where a path really lands use realpath on the
 * part that exists.
 *
 * Returns 0, or -1 when the input is not absolute or the result does not fit.
 */
static inline int path_normalize(const char *in, char *out, size_t outsz) {
    if (!in || in[0] != '/' || !out || outsz < 2) return -1;
    size_t len = 0;
    out[len++] = '/';
    out[len] = '\0';
    const char *p = in;
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != '/') p++;
        size_t n = (size_t)(p - s);
        if (n == 1 && s[0] == '.') continue;
        if (n == 2 && s[0] == '.' && s[1] == '.') {
            while (len > 1 && out[len - 1] != '/') len--;
            if (len > 1) len--;
            out[len] = '\0';
            continue;
        }
        if (len > 1) {
            if (len + 1 >= outsz) return -1;
            out[len++] = '/';
        }
        if (len + n >= outsz) return -1;
        memcpy(out + len, s, n);
        len += n;
        out[len] = '\0';
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Time helpers                                                       */
/* ------------------------------------------------------------------ */

/*
 * File times as one signed count of nanoseconds since the epoch.
 *
 * Signed because a file can predate 1970: `touch -d 1960-01-01` is a
 * legitimate timestamp, and the unsigned version of this turned it into
 * 1.8e19, which SQLite then stored as a REAL and the restore read back as 0.
 * An int64 of nanoseconds spans 1678 to 2262; anything outside that is
 * clamped rather than allowed to overflow, since a wrong-but-bounded time
 * on an absurd timestamp beats undefined behaviour.
 */
static inline int64_t time_to_ns(int64_t sec, int64_t nsec) {
    const int64_t NS = 1000000000;
    if (sec > (INT64_MAX - nsec) / NS) return INT64_MAX;
    if (sec < (INT64_MIN + NS) / NS)   return INT64_MIN;
    return sec * NS + nsec;
}

/* The inverse, flooring so tv_nsec stays in [0, 1e9) as utimensat requires:
 * -0.5 s is { -1, 500000000 }, not { 0, -500000000 }. */
static inline struct timespec ns_to_timespec(int64_t ns) {
    const int64_t NS = 1000000000;
    int64_t sec = ns / NS, rem = ns % NS;
    if (rem < 0) { rem += NS; sec--; }
    struct timespec ts;
    ts.tv_sec  = (time_t)sec;
    ts.tv_nsec = (long)rem;
    return ts;
}

