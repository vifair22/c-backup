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

