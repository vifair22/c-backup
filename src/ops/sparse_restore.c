/*
 * Streaming sparse-file reconstruction.
 *
 * Extracted from restore.c during Phase 0 of the daemon rewrite. The rest of
 * restore.c was deleted; this survived because reconstructing a sparse file
 * without materialising it in RAM is a constraint the new implementation still
 * has to meet, and this code already does it correctly.
 *
 * Unchanged from the original apart from being made non-static.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "sparse_restore.h"

#include "object.h"
#include "util.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * Header and region table validation, split out so arbitrary bytes can be
 * thrown at them directly.
 *
 * A sparse object's region table is a list of offsets and lengths that a
 * restore turns straight into lseek and write against a file it just
 * created. Every value in it came off a disk. Getting this wrong does not
 * corrupt an object somewhere - it writes to arbitrary offsets in a file on
 * the machine being restored to, which is why the checks are here and up
 * front rather than interleaved with the writing.
 *
 * The rules, and why each one:
 *   - the magic must match, or this is not a region table at all
 *   - a zero region count means a sparse object with no data, which the
 *     writer never produces
 *   - region_count is bounded by what the table could physically describe,
 *     so a corrupt count cannot ask for a multi-gigabyte allocation
 *   - regions ascend and do not overlap, so the file is written once
 *     forwards rather than seeking backwards over itself
 *   - every region lies inside the file, computed without overflowing
 */

/* One region per byte of file is already absurd; this is a sanity ceiling
 * that keeps region_count * sizeof(region) from overflowing a size_t. */
#define SPARSE_MAX_REGIONS (1u << 24)

status_t sparse_header_check(const sparse_hdr_t *h, uint64_t file_size)
{
    if (!h) return set_error(ERR_INVALID, "sparse: null header");
    if (h->magic != SPARSE_MAGIC)
        return set_error(ERR_CORRUPT, "sparse: bad magic");
    if (h->region_count == 0)
        return set_error(ERR_CORRUPT, "sparse: no regions");
    if (h->region_count > SPARSE_MAX_REGIONS)
        return set_error(ERR_CORRUPT, "sparse: %u regions is not plausible",
                         h->region_count);
    /* Each region needs at least one byte of file to describe. */
    if ((uint64_t)h->region_count > file_size)
        return set_error(ERR_CORRUPT,
                         "sparse: %u regions in a %llu byte file",
                         h->region_count, (unsigned long long)file_size);
    return OK;
}

status_t sparse_regions_check(const sparse_region_t *r, uint32_t count,
                              uint64_t file_size)
{
    if (!r && count) return set_error(ERR_INVALID, "sparse: null regions");

    uint64_t prev_end = 0;
    for (uint32_t i = 0; i < count; i++) {
        if (r[i].length == 0)
            return set_error(ERR_CORRUPT, "sparse: region %u is empty", i);
        if (r[i].offset < prev_end)
            return set_error(ERR_CORRUPT,
                             "sparse: region %u overlaps or precedes the last", i);
        if (r[i].offset > file_size)
            return set_error(ERR_CORRUPT, "sparse: region %u starts past EOF", i);
        /* Subtraction, not addition: offset <= file_size is already known,
         * so this cannot wrap the way offset + length can. */
        if (r[i].length > file_size - r[i].offset)
            return set_error(ERR_CORRUPT, "sparse: region %u runs past EOF", i);
        prev_end = r[i].offset + r[i].length;
    }
    return OK;
}

/* Streaming sparse restore: decompress a sparse object through a pipe,
 * parse the header + region table, and scatter data to the correct offsets
 * in out_fd.  Bounded-RAM — safe for sparse objects that exceed STREAM_LOAD_MAX. */
struct sparse_stream_arg {
    store_t *store;
    const uint8_t *hash;
    int pipe_wfd;
    status_t rc;
};

static void *sparse_stream_worker(void *a) {
    struct sparse_stream_arg *arg = a;
    uint64_t sz = 0; uint8_t type = 0;
    arg->rc = object_load_stream(arg->store, arg->hash, arg->pipe_wfd, &sz, &type);
    close(arg->pipe_wfd);
    return NULL;
}

static status_t read_pipe_full(int fd, void *buf, size_t n) {
    uint8_t *p = buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r == 0) return ERR_CORRUPT;
        if (r < 0) { if (errno == EINTR) continue; return ERR_IO; }
        p += r; n -= (size_t)r;
    }
    return OK;
}

status_t restore_sparse_streaming(store_t *store,
                                         const uint8_t hash[OBJECT_HASH_SIZE],
                                         int out_fd, uint64_t file_size,
                                         const char *full) {
    int pfd[2];
    if (pipe(pfd) == -1) return set_error_errno(ERR_IO, "restore sparse: pipe");

    struct sparse_stream_arg arg = { .store = store, .hash = hash,
                                     .pipe_wfd = pfd[1], .rc = OK };
    pthread_t tid;
    if (pthread_create(&tid, NULL, sparse_stream_worker, &arg) != 0) {
        close(pfd[0]); close(pfd[1]);
        return set_error_errno(ERR_IO, "restore sparse: pthread_create");
    }

    status_t st = OK;
    sparse_region_t *rgns = NULL;
    uint8_t *buf = NULL;
    sparse_hdr_t shdr;
    if (read_pipe_full(pfd[0], &shdr, sizeof(shdr)) != OK) {
        st = set_error(ERR_CORRUPT, "restore sparse: short header for %s", full);
        goto done;
    }
    st = sparse_header_check(&shdr, file_size);
    if (st != OK) goto done;

    size_t rt_sz = (size_t)shdr.region_count * sizeof(sparse_region_t);
    rgns = malloc(rt_sz);
    if (!rgns) { st = set_error(ERR_NOMEM, "restore sparse: regions"); goto done; }
    if (read_pipe_full(pfd[0], rgns, rt_sz) != OK) {
        st = set_error(ERR_CORRUPT, "restore sparse: short regions for %s", full);
        goto done;
    }
    st = sparse_regions_check(rgns, shdr.region_count, file_size);
    if (st != OK) goto done;
    if (ftruncate(out_fd, (off_t)file_size) == -1 && file_size > 0) {
        st = set_error_errno(ERR_IO, "restore sparse: ftruncate(%s)", full);
        goto done;
    }
    enum { SP_BUF = 1u << 20 };
    buf = malloc(SP_BUF);
    if (!buf) { st = set_error(ERR_NOMEM, "restore sparse: buf"); goto done; }

    for (uint32_t r = 0; r < shdr.region_count && st == OK; r++) {
        if (lseek(out_fd, (off_t)rgns[r].offset, SEEK_SET) == (off_t)-1) {
            st = set_error_errno(ERR_IO, "restore sparse: lseek(%s)", full); break;
        }
        uint64_t rem = rgns[r].length;
        while (rem > 0) {
            size_t want = (rem > SP_BUF) ? SP_BUF : (size_t)rem;
            ssize_t got = read(pfd[0], buf, want);
            if (got == 0) { st = set_error(ERR_CORRUPT, "restore sparse: short data for %s", full); break; }
            if (got < 0) { if (errno == EINTR) continue; st = set_error_errno(ERR_IO, "restore sparse: read pipe"); break; }
            if (io_write_full(out_fd, buf, (size_t)got) != 0) {
                st = set_error_errno(ERR_IO, "restore sparse: write(%s)", full); break;
            }
            rem -= (uint64_t)got;
        }
    }

done:
    free(buf);
    free(rgns);
    /* Drain pipe so the writer thread can finish cleanly. */
    uint8_t drain[4096];
    while (read(pfd[0], drain, sizeof(drain)) > 0) {}
    close(pfd[0]);
    pthread_join(tid, NULL);
    if (st == OK && arg.rc != OK) st = arg.rc;
    return st;
}
