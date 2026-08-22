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

/* Streaming sparse restore: decompress a sparse object through a pipe,
 * parse the header + region table, and scatter data to the correct offsets
 * in out_fd.  Bounded-RAM — safe for sparse objects that exceed STREAM_LOAD_MAX. */
struct sparse_stream_arg {
    repo_t *repo;
    const uint8_t *hash;
    int pipe_wfd;
    status_t rc;
};

static void *sparse_stream_worker(void *a) {
    struct sparse_stream_arg *arg = a;
    uint64_t sz = 0; uint8_t type = 0;
    arg->rc = object_load_stream(arg->repo, arg->hash, arg->pipe_wfd, &sz, &type);
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

status_t restore_sparse_streaming(repo_t *repo,
                                         const uint8_t hash[OBJECT_HASH_SIZE],
                                         int out_fd, uint64_t file_size,
                                         const char *full) {
    int pfd[2];
    if (pipe(pfd) == -1) return set_error_errno(ERR_IO, "restore sparse: pipe");

    struct sparse_stream_arg arg = { .repo = repo, .hash = hash,
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
    if (shdr.magic != SPARSE_MAGIC || shdr.region_count == 0) {
        st = set_error(ERR_CORRUPT, "restore sparse: bad header for %s", full);
        goto done;
    }
    size_t rt_sz = (size_t)shdr.region_count * sizeof(sparse_region_t);
    rgns = malloc(rt_sz);
    if (!rgns) { st = set_error(ERR_NOMEM, "restore sparse: regions"); goto done; }
    if (read_pipe_full(pfd[0], rgns, rt_sz) != OK) {
        st = set_error(ERR_CORRUPT, "restore sparse: short regions for %s", full);
        goto done;
    }
    if (ftruncate(out_fd, (off_t)file_size) == -1 && file_size > 0) {
        st = set_error_errno(ERR_IO, "restore sparse: ftruncate(%s)", full);
        goto done;
    }
    enum { SP_BUF = 1u << 20 };
    buf = malloc(SP_BUF);
    if (!buf) { st = set_error(ERR_NOMEM, "restore sparse: buf"); goto done; }

    uint64_t prev_end = 0;
    for (uint32_t r = 0; r < shdr.region_count && st == OK; r++) {
        if (rgns[r].offset < prev_end || rgns[r].offset > file_size ||
            rgns[r].length > file_size - rgns[r].offset) {
            st = set_error(ERR_CORRUPT, "restore sparse: region %u OOB for %s", r, full);
            break;
        }
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
        prev_end = rgns[r].offset + rgns[r].length;
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
