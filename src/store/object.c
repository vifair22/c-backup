#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "parity_stream.h"
#include "util.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <pthread.h>

#include <openssl/evp.h>
#include <openssl/sha.h>

/* ---- Async writeback for loose objects ----
 * Initiates async writeback without blocking.  On HDD this avoids the
 * 5-15ms platter rotation stall per fdatasync.  Safe because objects use
 * mkstemp→write→sync→rename: if power fails before rename, the temp file
 * is lost but no corruption occurs (re-stored on next backup).
 * Falls back to fdatasync on non-Linux or if sync_file_range fails. */
#ifdef __linux__
#include <linux/fs.h>
static int async_writeback(int fd) {
    int rc = (int)sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WRITE);
    return rc == -1 ? fdatasync(fd) : 0;
}
#else
static int async_writeback(int fd) { return fdatasync(fd); }
#endif

/* ------------------------------------------------------------------ */

static void hash_to_path(const uint8_t hash[OBJECT_HASH_SIZE],
                         char subdir[3], char fname[OBJECT_HASH_SIZE * 2 - 1]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    subdir[0] = hex[0]; subdir[1] = hex[1]; subdir[2] = '\0';
    memcpy(fname, hex + 2, OBJECT_HASH_SIZE * 2 - 2);
    fname[OBJECT_HASH_SIZE * 2 - 2] = '\0';
}

void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf) {
    static const char hx[] = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        buf[i * 2]     = hx[hash[i] >> 4];
        buf[i * 2 + 1] = hx[hash[i] & 0xf];
    }
    buf[OBJECT_HASH_SIZE * 2] = '\0';
}

static void sha256(const void *data, size_t len, uint8_t out[OBJECT_HASH_SIZE]) {
    SHA256((const unsigned char *)data, len, out);
}

static int obj_name_exists_at(int subfd, const char *fname) {
    return faccessat(subfd, fname, F_OK, 0) == 0;
}

int object_exists(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]) {
    /* Fast path: in-memory loose set (returns 0 if set not built) */
    if (repo_loose_set_contains(repo, hash)) return 1;

    /* On-disk check — only needed when loose set has not been built */
    if (!repo_loose_set_ready(repo)) {
        char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
        hash_to_path(hash, subdir, fname);
        int objfd = repo_objects_fd(repo);
        if (objfd != -1) {
            int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
            if (subfd != -1) {
                int exists = (faccessat(subfd, fname, F_OK, 0) == 0);
                close(subfd);
                if (exists) return 1;
            }
        }
    }
    return pack_object_exists(repo, hash);
}

status_t object_physical_size(repo_t *repo,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes) {
    if (!out_bytes) return set_error(ERR_INVALID, "object_physical_size: out_bytes is NULL");

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    struct stat st;
    if (stat(path, &st) == 0) {
        *out_bytes = (uint64_t)st.st_size;
        return OK;
    }
    if (errno != ENOENT)
        return set_error_errno(ERR_IO, "stat(%s)", path);

    return pack_object_physical_size(repo, hash, out_bytes);
}

/* Write a blob (already in memory) as an object. */
static status_t write_object(repo_t *repo, uint8_t type,
                             const void *data, size_t len,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             int *out_is_new, uint64_t *out_phys_bytes) {
    sha256(data, len, out_hash);
    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(repo, out_hash)) return OK;

    /* Always store uncompressed; the pack sweeper handles compression. */
    const void *payload = data;
    size_t payload_size = len;
    char *compressed = NULL;

    object_header_t hdr = {
        .magic             = OBJECT_MAGIC,
        .version           = OBJECT_HDR_VERSION,
        .type              = type,
        .compression       = COMPRESS_NONE,
        .pack_skip_ver     = 0,
        .uncompressed_size = (uint64_t)len,
        .compressed_size   = (uint64_t)payload_size,
    };
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = repo_objects_fd(repo);
    if (objfd == -1) { free(compressed); return set_error_errno(ERR_IO, "openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        free(compressed);
        return set_error_errno(ERR_IO, "mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    if (subfd == -1) { free(compressed); return set_error_errno(ERR_IO, "openat(%s)", subdir); }

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) {
        close(subfd); free(compressed); return set_error(ERR_IO, "write_object: tmppath too long");
    }
    int fd = mkstemp(tmppath);
    if (fd == -1) { close(subfd); free(compressed); return set_error_errno(ERR_IO, "mkstemp(%s)", tmppath); }

    status_t st = OK;
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        st = set_error_errno(ERR_IO, "write_object: write header");
        goto fail;
    }
    if (write(fd, payload, payload_size) != (ssize_t)payload_size) {
        if (errno == 0) errno = EIO;
        st = set_error_errno(ERR_IO, "write_object: write payload");
        goto fail;
    }

    /* ---- Parity trailer (version 2) ---- */
    parity_record_t hdr_par;
    parity_record_compute(&hdr, sizeof(hdr), &hdr_par);
    if (write(fd, &hdr_par, sizeof(hdr_par)) != (ssize_t)sizeof(hdr_par)) {
        st = set_error_errno(ERR_IO, "write_object: write parity header");
        goto fail;
    }

    size_t rs_sz = rs_parity_size(payload_size);
    uint8_t *rs_buf = NULL;
    if (rs_sz > 0) {
        rs_buf = malloc(rs_sz);
        if (!rs_buf) { st = set_error(ERR_NOMEM, "malloc(%zu)", rs_sz); goto fail; }
        rs_parity_encode(payload, payload_size, rs_buf);
        if (write(fd, rs_buf, rs_sz) != (ssize_t)rs_sz) {
            free(rs_buf);
            st = set_error_errno(ERR_IO, "write_object: write RS parity");
            goto fail;
        }
        free(rs_buf);
    }

    uint32_t pcrc = crc32c(payload, payload_size);
    if (write(fd, &pcrc, sizeof(pcrc)) != (ssize_t)sizeof(pcrc)) {
        st = set_error_errno(ERR_IO, "write_object: write CRC");
        goto fail;
    }
    /* Deprecated rs_data_len field — always 0, see parity.h. */
    uint32_t rs_data_len_dep = PARITY_RS_DATA_LEN_DEPRECATED;
    if (write(fd, &rs_data_len_dep, sizeof(rs_data_len_dep)) != (ssize_t)sizeof(rs_data_len_dep)) {
        st = set_error_errno(ERR_IO, "write_object: write rs_data_len (deprecated)");
        goto fail;
    }

    parity_footer_t pfooter = {
        .magic        = PARITY_FOOTER_MAGIC,
        .version      = PARITY_VERSION,
        .trailer_size = (uint32_t)(sizeof(hdr_par) + rs_sz + sizeof(pcrc)
                         + sizeof(rs_data_len_dep) + sizeof(pfooter)),
    };
    if (write(fd, &pfooter, sizeof(pfooter)) != (ssize_t)sizeof(pfooter)) {
        st = set_error_errno(ERR_IO, "write_object: write parity footer");
        goto fail;
    }
    /* ---- End parity trailer ---- */

    if (async_writeback(fd) == -1) { st = set_error_errno(ERR_IO, "write_object: fdatasync"); goto fail; }
    close(fd); fd = -1;

    uint64_t total_phys = (uint64_t)sizeof(hdr) + (uint64_t)payload_size
                        + (uint64_t)pfooter.trailer_size;

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", repo_path(repo), subdir, fname)
        >= (int)sizeof(dstpath)) { st = set_error(ERR_IO, "write_object: dstpath too long"); goto fail; }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            unlink(tmppath);
            close(subfd); free(compressed);
            return OK;
        }
        st = set_error_errno(ERR_IO, "rename(%s, %s)", tmppath, dstpath);
        goto fail;
    }
    close(subfd); free(compressed);
    repo_loose_set_insert(repo, out_hash);
    if (out_is_new) *out_is_new = 1;
    if (out_phys_bytes) *out_phys_bytes = total_phys;
    return st;
fail:
    if (fd >= 0) close(fd);
    unlink(tmppath);
    close(subfd); free(compressed);
    return st;
}

/* ------------------------------------------------------------------ */
/* Streaming file → object store.  Single-threaded read → hash → write */
/* loop.  Uses smaller chunks on FUSE to avoid overwhelming userspace. */
/* ------------------------------------------------------------------ */

#define STREAM_CHUNK      ((size_t)(128 * 1024 * 1024)) /* 128 MiB write buffer */
#define STREAM_CHUNK_FUSE ((size_t)(  1 * 1024 * 1024)) /*   1 MiB on FUSE */
#define STREAM_LOAD_MAX   ((size_t)( 16 * 1024 * 1024)) /*  16 MiB load threshold */

#ifndef FUSE_SUPER_MAGIC
#define FUSE_SUPER_MAGIC 0x65735546
#endif

/* Detect whether fd lives on a FUSE filesystem. */
static int fd_is_fuse(int fd) {
    struct statfs sfs;
    if (fstatfs(fd, &sfs) != 0) return 0;
    return sfs.f_type == (__typeof__(sfs.f_type))FUSE_SUPER_MAGIC;
}

/* Probe size and compression threshold for LZ4 frame decision */
#define PROBE_SIZE        (64  * 1024)          /* 64 KiB sample          */
#define PROBE_RATIO_MAX   0.98                  /* skip LZ4 if > 98% size */

#define STREAM_REOPEN_MAX 5   /* max seek/reopen recovery attempts per file */

/* ------------------------------------------------------------------ */
/* FUSE readahead ring — overlaps FUSE reads with disk writes.        */
/*                                                                    */
/* A single reader thread fills a small bounded ring of 1 MiB buffers */
/* while the main thread processes (hash + write + CRC + RS).  The    */
/* ring is small (4 slots) so the FUSE daemon never has more than one */
/* outstanding read, avoiding the flooding that caused ENXIO before.  */
/* ------------------------------------------------------------------ */

#define FUSE_RING_SLOTS  4
#define FUSE_RING_CHUNK  ((size_t)(4 * 1024 * 1024))   /* 4 MiB per slot */

typedef struct {
    /* Ring state */
    uint8_t *bufs[FUSE_RING_SLOTS];   /* per-slot buffers, each FUSE_RING_CHUNK */
    size_t   lens[FUSE_RING_SLOTS];   /* bytes valid in each slot */
    int      head;                     /* next slot to fill (reader) */
    int      tail;                     /* next slot to consume (main) */
    int      count;                    /* slots currently filled */

    /* Synchronization */
    pthread_mutex_t mu;
    pthread_cond_t  not_full;          /* reader waits when ring is full */
    pthread_cond_t  not_empty;         /* main waits when ring is empty */

    /* Reader thread state */
    int      src_fd;
    char    *src_path;                 /* for reopen recovery (NULL if no path) */
    uint64_t file_size;
    uint64_t read_offset;              /* how far the reader has gotten */

    /* Completion / error signaling */
    int      done;                     /* reader finished (EOF or error) */
    int      error;                    /* errno from reader, 0 if none */
    int      stop;                     /* main thread requests early stop */
    int      recoveries;              /* shared recovery count */
} fuse_ring_t;

static int fuse_ring_init(fuse_ring_t *r, int src_fd, const char *src_path,
                          uint64_t file_size)
{
    memset(r, 0, sizeof(*r));
    r->src_fd    = src_fd;
    r->src_path  = src_path ? strdup(src_path) : NULL;
    r->file_size = file_size;

    pthread_mutex_init(&r->mu, NULL);
    pthread_cond_init(&r->not_full, NULL);
    pthread_cond_init(&r->not_empty, NULL);

    for (int i = 0; i < FUSE_RING_SLOTS; i++) {
        r->bufs[i] = malloc(FUSE_RING_CHUNK);
        if (!r->bufs[i]) {
            for (int j = 0; j < i; j++) free(r->bufs[j]);
            free(r->src_path);
            return -1;
        }
    }
    return 0;
}

static void fuse_ring_destroy(fuse_ring_t *r)
{
    for (int i = 0; i < FUSE_RING_SLOTS; i++)
        free(r->bufs[i]);
    free(r->src_path);
    pthread_mutex_destroy(&r->mu);
    pthread_cond_destroy(&r->not_full);
    pthread_cond_destroy(&r->not_empty);
}

/* Reader thread: fills ring slots with FUSE reads, one at a time. */
static void *fuse_reader_thread(void *arg)
{
    fuse_ring_t *r = (fuse_ring_t *)arg;

    while (r->read_offset < r->file_size) {
        size_t want = FUSE_RING_CHUNK;
        if ((uint64_t)want > r->file_size - r->read_offset)
            want = (size_t)(r->file_size - r->read_offset);

        /* Wait for a free slot */
        pthread_mutex_lock(&r->mu);
        while (r->count == FUSE_RING_SLOTS && !r->stop)
            pthread_cond_wait(&r->not_full, &r->mu);
        if (r->stop) {
            r->done = 1;
            pthread_cond_signal(&r->not_empty);
            pthread_mutex_unlock(&r->mu);
            return NULL;
        }
        int slot = r->head;
        pthread_mutex_unlock(&r->mu);

        /* Read into this slot (outside the lock) */
        size_t got = 0;
        int retries = 0;
        int last_was_seek = 0;

        while (got < want) {
            ssize_t rd = read(r->src_fd, r->bufs[slot] + got, want - got);
            if (rd > 0) { got += (size_t)rd; retries = 0; last_was_seek = 0; continue; }
            if (rd == -1 && errno == EINTR) continue;
            if (rd == -1 && retries < 3 &&
                (errno == EIO || errno == ENXIO || errno == EAGAIN)) {
                retries++;
                struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
                nanosleep(&ts, NULL);
                continue;
            }
            if (rd == 0) errno = ESTALE;

            /* Recovery: seek or reopen */
            int saved_errno = errno;

            if (r->recoveries >= STREAM_REOPEN_MAX) {
                pthread_mutex_lock(&r->mu);
                r->error = saved_errno ? saved_errno : EIO;
                r->done = 1;
                pthread_cond_signal(&r->not_empty);
                pthread_mutex_unlock(&r->mu);
                return NULL;
            }

            /* Tier 1: seek on existing fd */
            if (!last_was_seek) {
                off_t target = (off_t)(r->read_offset + got);
                if (lseek(r->src_fd, target, SEEK_SET) == target) {
                    r->recoveries++;
                    last_was_seek = 1;
                    continue;
                }
            }

            /* Tier 2: reopen by path */
            if (r->src_path) {
                off_t target = (off_t)(r->read_offset + got);
                int new_fd = open(r->src_path, O_RDONLY);
                if (new_fd >= 0 && lseek(new_fd, target, SEEK_SET) == target) {
                    close(r->src_fd);
                    r->src_fd = new_fd;
                    r->recoveries++;
                    last_was_seek = 0;
                    continue;
                }
                if (new_fd >= 0) close(new_fd);
            }

            /* Unrecoverable */
            pthread_mutex_lock(&r->mu);
            r->error = saved_errno ? saved_errno : EIO;
            r->done = 1;
            pthread_cond_signal(&r->not_empty);
            pthread_mutex_unlock(&r->mu);
            return NULL;
        }

        /* Slot filled — publish it */
        pthread_mutex_lock(&r->mu);
        r->lens[slot] = got;
        r->head = (r->head + 1) % FUSE_RING_SLOTS;
        r->count++;
        r->read_offset += got;
        pthread_cond_signal(&r->not_empty);
        pthread_mutex_unlock(&r->mu);
    }

    /* EOF — signal done */
    pthread_mutex_lock(&r->mu);
    r->done = 1;
    pthread_cond_signal(&r->not_empty);
    pthread_mutex_unlock(&r->mu);
    return NULL;
}

/* Pull the next filled buffer from the ring.
 * Returns pointer to buffer and sets *out_len.
 * Returns NULL on error (sets *out_errno) or when done (*out_len = 0).
 *
 * IMPORTANT: The returned buffer remains owned by the ring.  The caller
 * must call fuse_ring_release() after it is done processing the buffer.
 * Until release, the reader thread cannot reuse this slot. */
static uint8_t *fuse_ring_pull(fuse_ring_t *r, size_t *out_len, int *out_errno)
{
    pthread_mutex_lock(&r->mu);
    while (r->count == 0 && !r->done)
        pthread_cond_wait(&r->not_empty, &r->mu);

    if (r->count == 0) {
        /* Ring empty and done */
        if (r->error) {
            *out_errno = r->error;
            pthread_mutex_unlock(&r->mu);
            return NULL;
        }
        *out_len = 0;
        pthread_mutex_unlock(&r->mu);
        return NULL;
    }

    int slot = r->tail;
    *out_len = r->lens[slot];
    uint8_t *buf = r->bufs[slot];
    /* Advance tail but do NOT decrement count yet — the buffer is still
     * in use by the caller.  count is decremented in fuse_ring_release(). */
    r->tail = (r->tail + 1) % FUSE_RING_SLOTS;
    pthread_mutex_unlock(&r->mu);

    *out_errno = 0;
    return buf;
}

/* Release a previously pulled buffer back to the ring.
 * Must be called after the caller is done processing the buffer
 * returned by fuse_ring_pull(). */
static void fuse_ring_release(fuse_ring_t *r)
{
    pthread_mutex_lock(&r->mu);
    r->count--;
    pthread_cond_signal(&r->not_full);
    pthread_mutex_unlock(&r->mu);
}

static status_t write_object_file_stream(repo_t *repo, int src_fd,
                                         const char *src_path,
                                         uint64_t file_size,
                                         uint8_t out_hash[OBJECT_HASH_SIZE],
                                         int *out_is_new, uint64_t *out_phys_bytes,
                                         xfer_progress_fn progress_cb,
                                         void *progress_ctx) {
    /* Always store uncompressed; the pack sweeper handles compression. */
    if (lseek(src_fd, 0, SEEK_SET) == (off_t)-1) {
        if (src_path) close(src_fd);
        return set_error_errno(ERR_IO, "lseek(src_fd)");
    }

    /* ----------------------------------------------------------------
     * Create temp file and write placeholder header.
     * compressed_size will be patched after writing.
     * ---------------------------------------------------------------- */
    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) {
        if (src_path) close(src_fd);
        return set_error(ERR_IO, "stream: tmppath too long");
    }

    int tfd = mkstemp(tmppath);
    if (tfd == -1) {
        if (src_path) close(src_fd);
        return set_error_errno(ERR_IO, "mkstemp(%s)", tmppath);
    }

    object_header_t hdr = {
        .magic             = OBJECT_MAGIC,
        .version           = OBJECT_HDR_VERSION,
        .type              = OBJECT_TYPE_FILE,
        .compression       = COMPRESS_NONE,
        .pack_skip_ver     = 0,
        .uncompressed_size = file_size,
        .compressed_size   = 0,   /* patched below */
    };
    if (write(tfd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        if (src_path) close(src_fd);
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "stream: write header");
    }

    uint64_t total_written = 0;

    /* ----------------------------------------------------------------
     * Hash context + read buffer
     * ---------------------------------------------------------------- */
    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx || EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(mdctx);
        if (src_path) close(src_fd);
        close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "EVP_MD_CTX_new");
    }

    int is_fuse = fd_is_fuse(src_fd);

    /* Non-FUSE path uses a single large buffer; FUSE path uses ring buffers */
    uint8_t *buf = NULL;
    if (!is_fuse) {
        buf = malloc(STREAM_CHUNK);
        if (!buf) {
            EVP_MD_CTX_free(mdctx);
            if (src_path) close(src_fd);
            close(tfd); unlink(tmppath);
            return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_CHUNK);
        }
    }

    /* On FUSE, POSIX_FADV_SEQUENTIAL triggers aggressive kernel readahead
     * that floods the userspace daemon — use conservative readahead instead. */
    if (!is_fuse)
        posix_fadvise(src_fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    /* ----------------------------------------------------------------
     * RS parity accumulator — bounded-RAM streaming, spills to temp file.
     * ---------------------------------------------------------------- */
    char ps_tmp_dir[PATH_MAX];
    snprintf(ps_tmp_dir, sizeof(ps_tmp_dir), "%s/tmp", repo_path(repo));
    rs_parity_stream_t ps;
    int ps_ok = rs_parity_stream_init(&ps, 256 * 1024 * 1024, ps_tmp_dir);
    if (ps_ok != 0) {
        free(buf);
        EVP_MD_CTX_free(mdctx);
        if (src_path) close(src_fd);
        close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "stream: rs parity stream init");
    }

    /* ----------------------------------------------------------------
     * Main loop: read → hash → write → accumulate CRC & RS parity
     * ---------------------------------------------------------------- */
    status_t st     = OK;
    uint32_t stream_crc = 0;
    uint64_t offset = 0;

    /* Helper: push the progress line down before logging so our messages
     * aren't overwritten by the \r-based progress display. */
    #define STREAM_LOG(level, msg) do { \
        fprintf(stderr, "\n"); fflush(stderr); \
        log_msg((level), (msg)); \
    } while (0)

    if (is_fuse) {
        /* ---- FUSE path: reader thread fills ring, main thread processes ---- */
        fuse_ring_t ring;
        if (fuse_ring_init(&ring, src_fd, src_path, file_size) != 0) {
            free(buf);
            rs_parity_stream_destroy(&ps);
            EVP_MD_CTX_free(mdctx);
            close(tfd); unlink(tmppath);
            return set_error(ERR_NOMEM, "stream: fuse ring init");
        }

        pthread_t reader_thr;
        if (pthread_create(&reader_thr, NULL, fuse_reader_thread, &ring) != 0) {
            fuse_ring_destroy(&ring);
            free(buf);
            rs_parity_stream_destroy(&ps);
            EVP_MD_CTX_free(mdctx);
            close(tfd); unlink(tmppath);
            return set_error(ERR_IO, "stream: pthread_create reader");
        }

        while (st == OK) {
            size_t got = 0;
            int pull_errno = 0;
            uint8_t *chunk_buf = fuse_ring_pull(&ring, &got, &pull_errno);

            if (!chunk_buf && got == 0 && pull_errno == 0)
                break;  /* EOF */

            if (!chunk_buf || pull_errno) {
                errno = pull_errno ? pull_errno : EIO;
                st = set_error_errno(ERR_IO,
                    "stream: FUSE read error at offset %llu/%llu (%.1f%%)",
                    (unsigned long long)offset,
                    (unsigned long long)file_size,
                    file_size > 0 ? 100.0 * (double)offset / (double)file_size : 0.0);
                break;
            }

            if (EVP_DigestUpdate(mdctx, chunk_buf, got) != 1) {
                st = set_error(ERR_IO, "stream: SHA256 update failed");
                fuse_ring_release(&ring); break;
            }
            if (write(tfd, chunk_buf, got) != (ssize_t)got) {
                if (errno == 0) errno = EIO;
                st = set_error_errno(ERR_IO, "stream: write payload chunk");
                fuse_ring_release(&ring); break;
            }
            stream_crc = crc32c_update(stream_crc, chunk_buf, got);
            if (rs_parity_stream_feed(&ps, chunk_buf, got) != 0) {
                st = set_error_errno(ERR_IO, "stream: RS parity feed failed (spill)");
                fuse_ring_release(&ring); break;
            }

            /* Done with buffer — release slot back to reader */
            fuse_ring_release(&ring);

            total_written += got;
            offset += (uint64_t)got;
            if (progress_cb) progress_cb((uint64_t)got, progress_ctx);
        }

        /* Signal reader to stop (in case it's blocked on not_full) */
        pthread_mutex_lock(&ring.mu);
        ring.stop = 1;
        pthread_cond_signal(&ring.not_full);
        pthread_mutex_unlock(&ring.mu);

        pthread_join(reader_thr, NULL);

        /* Reader thread may have reopened src_fd — update for cleanup. */
        if (ring.src_fd != src_fd)
            src_fd = ring.src_fd;
        fuse_ring_destroy(&ring);

        goto post_loop;

    } else {
        /* ---- Non-FUSE path: direct read with recovery ---- */
        int recoveries = 0;
        int last_was_seek = 0;

        while (offset < file_size) {
            size_t want = STREAM_CHUNK;
            if ((uint64_t)want > file_size - offset)
                want = (size_t)(file_size - offset);

            /* Read with EINTR + transient-error retry */
            size_t got = 0;
            int    retries = 0;
            while (got < want) {
                ssize_t r = read(src_fd, buf + got, want - got);
                if (r > 0) { got += (size_t)r; retries = 0; last_was_seek = 0; continue; }
                if (r == -1 && errno == EINTR) continue;
                if (r == -1 && retries < 3 &&
                    (errno == EIO || errno == ENXIO || errno == EAGAIN)) {
                    retries++;
                    char rmsg[256];
                    snprintf(rmsg, sizeof(rmsg),
                             "stream: transient read error (%s) at offset %llu/%llu (%.1f%%), retry %d/3",
                             strerror(errno),
                             (unsigned long long)(offset + got),
                             (unsigned long long)file_size,
                             file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0,
                             retries);
                    STREAM_LOG("WARN", rmsg);
                    struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
                    nanosleep(&ts, NULL);
                    continue;
                }
                if (r == 0) errno = ESTALE;

                /* --- Recovery: seek on same fd, or reopen + seek --- */
                int saved_errno = errno;
                char rmsg[256];

                if (recoveries >= STREAM_REOPEN_MAX) {
                    snprintf(rmsg, sizeof(rmsg),
                             "stream: read failed (%s) at offset %llu/%llu (%.1f%%), all %d recovery attempts exhausted",
                             strerror(saved_errno),
                             (unsigned long long)(offset + got),
                             (unsigned long long)file_size,
                             file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0,
                             STREAM_REOPEN_MAX);
                    STREAM_LOG("ERROR", rmsg);
                    errno = saved_errno;
                    st = set_error_errno(ERR_IO, "%s", rmsg);
                    goto done;
                }

                /* Tier 1: lseek on existing fd (skip if last attempt was also a seek,
                 * since that means the fd is dead and lseek just lies about success) */
                if (!last_was_seek) {
                    off_t seek_target = (off_t)(offset + got);
                    if (lseek(src_fd, seek_target, SEEK_SET) == seek_target) {
                        recoveries++;
                        last_was_seek = 1;
                        snprintf(rmsg, sizeof(rmsg),
                                 "stream: read error (%s) at offset %llu/%llu (%.1f%%), trying seek recovery (%d/%d)",
                                 strerror(saved_errno),
                                 (unsigned long long)(offset + got),
                                 (unsigned long long)file_size,
                                 file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0,
                                 recoveries, STREAM_REOPEN_MAX);
                        STREAM_LOG("WARN", rmsg);
                        errno = saved_errno;
                        continue;
                    }
                    snprintf(rmsg, sizeof(rmsg),
                             "stream: seek recovery failed (%s) at offset %llu/%llu (%.1f%%), escalating to reopen",
                             strerror(errno),
                             (unsigned long long)(offset + got),
                             (unsigned long long)file_size,
                             file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0);
                    STREAM_LOG("WARN", rmsg);
                }

                /* Tier 2: close + reopen by path + seek */
                if (src_path) {
                    off_t seek_target = (off_t)(offset + got);
                    snprintf(rmsg, sizeof(rmsg),
                             "stream: attempting reopen recovery at offset %llu/%llu (%.1f%%), attempt %d/%d",
                             (unsigned long long)(offset + got),
                             (unsigned long long)file_size,
                             file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0,
                             recoveries + 1, STREAM_REOPEN_MAX);
                    STREAM_LOG("WARN", rmsg);

                    int new_fd = open(src_path, O_RDONLY);
                    if (new_fd < 0) {
                        snprintf(rmsg, sizeof(rmsg),
                                 "stream: reopen failed (%s), giving up",
                                 strerror(errno));
                        STREAM_LOG("ERROR", rmsg);
                        errno = saved_errno;
                        st = set_error_errno(ERR_IO,
                            "stream: read failed at offset %llu/%llu (%.1f%%), reopen failed",
                            (unsigned long long)(offset + got),
                            (unsigned long long)file_size,
                            file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0);
                        goto done;
                    }
                    if (lseek(new_fd, seek_target, SEEK_SET) != seek_target) {
                        snprintf(rmsg, sizeof(rmsg),
                                 "stream: reopen seek failed (%s), giving up",
                                 strerror(errno));
                        STREAM_LOG("ERROR", rmsg);
                        close(new_fd);
                        errno = saved_errno;
                        st = set_error_errno(ERR_IO,
                            "stream: read failed at offset %llu/%llu (%.1f%%), reopen seek failed",
                            (unsigned long long)(offset + got),
                            (unsigned long long)file_size,
                            file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0);
                        goto done;
                    }
                    close(src_fd);
                    src_fd = new_fd;
                    recoveries++;
                    last_was_seek = 0;
                    snprintf(rmsg, sizeof(rmsg),
                             "stream: reopened fd at offset %llu/%llu (%.1f%%), recovery %d/%d",
                             (unsigned long long)(offset + got),
                             (unsigned long long)file_size,
                             file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0,
                             recoveries, STREAM_REOPEN_MAX);
                    STREAM_LOG("WARN", rmsg);
                    posix_fadvise(src_fd, 0, 0, POSIX_FADV_SEQUENTIAL);
                    errno = saved_errno;
                    continue;
                }

                /* No src_path — can't reopen, give up */
                snprintf(rmsg, sizeof(rmsg),
                         "stream: read failed (%s) at offset %llu/%llu (%.1f%%), no path for reopen",
                         strerror(saved_errno),
                         (unsigned long long)(offset + got),
                         (unsigned long long)file_size,
                         file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0);
                STREAM_LOG("ERROR", rmsg);
                errno = saved_errno;
                st = set_error_errno(ERR_IO, "%s", rmsg);
                goto done;
            }

            /* Drop source pages from cache */
            posix_fadvise(src_fd, (off_t)offset, (off_t)want, POSIX_FADV_DONTNEED);

            /* Hash */
            if (EVP_DigestUpdate(mdctx, buf, got) != 1) {
                st = set_error(ERR_IO, "stream: SHA256 update failed"); goto done;
            }

            /* Write raw bytes (always uncompressed) */
            if (write(tfd, buf, got) != (ssize_t)got) {
                if (errno == 0) errno = EIO;
                st = set_error_errno(ERR_IO, "stream: write payload chunk"); goto done;
            }
            stream_crc = crc32c_update(stream_crc, buf, got);

            /* Accumulate RS parity inline via bounded-RAM stream */
            if (rs_parity_stream_feed(&ps, buf, got) != 0) {
                st = set_error_errno(ERR_IO, "stream: RS parity feed failed (spill)");
                goto done;
            }

            total_written += got;
            offset += (uint64_t)got;
            if (progress_cb) progress_cb((uint64_t)got, progress_ctx);
        }
    }

post_loop:
    #undef STREAM_LOG

done:
    free(buf);
    if (src_path) close(src_fd);  /* only close if we own the fd */

    /* (No LZ4 frame end mark — write path always uses COMPRESS_NONE) */

    /* Flush final partial RS group */
    if (st == OK) {
        if (rs_parity_stream_finish(&ps) != 0)
            st = set_error_errno(ERR_IO, "stream: RS parity finish failed");
    }

    if (st != OK) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return st;
    }

    unsigned int dlen = 0;
    if (EVP_DigestFinal_ex(mdctx, out_hash, &dlen) != 1 || dlen != OBJECT_HASH_SIZE) {
        EVP_MD_CTX_free(mdctx); rs_parity_stream_destroy(&ps);
        close(tfd); unlink(tmppath); return set_error(ERR_IO, "stream: SHA256 finalize failed");
    }
    EVP_MD_CTX_free(mdctx);

    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(repo, out_hash)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath); return OK;
    }

    /* Patch header: fill in sizes and hash now that we know them */
    hdr.uncompressed_size = file_size;
    hdr.compressed_size   = total_written;
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);
    if (pwrite(tfd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "stream: pwrite header");
    }

    /* ---- Parity trailer (version 2) ----
     * CRC and RS parity were computed inline during the write loop.
     */
    {
        parity_record_t shdr_par;
        parity_record_compute(&hdr, sizeof(hdr), &shdr_par);
        if (lseek(tfd, 0, SEEK_END) == (off_t)-1 ||
            write(tfd, &shdr_par, sizeof(shdr_par)) != (ssize_t)sizeof(shdr_par)) {
            rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
            return set_error_errno(ERR_IO, "stream: write parity header");
        }

        size_t rs_sz = rs_parity_stream_total(&ps);

        if (rs_sz > 0) {
            if (rs_parity_stream_replay_fd(&ps, tfd) != 0) {
                if (errno == 0) errno = EIO;
                rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
                return set_error_errno(ERR_IO,
                    "stream: write RS parity (%zu bytes)", rs_sz);
            }
        }
        rs_parity_stream_destroy(&ps);

        if (write(tfd, &stream_crc, sizeof(stream_crc)) != (ssize_t)sizeof(stream_crc)) {
            close(tfd); unlink(tmppath);
            return set_error_errno(ERR_IO, "stream: write CRC");
        }
        /* Deprecated rs_data_len field — always 0, see parity.h. */
        uint32_t rs_data_len_s = PARITY_RS_DATA_LEN_DEPRECATED;
        if (write(tfd, &rs_data_len_s, sizeof(rs_data_len_s)) != (ssize_t)sizeof(rs_data_len_s)) {
            close(tfd); unlink(tmppath);
            return set_error_errno(ERR_IO, "stream: write rs_data_len (deprecated)");
        }

        parity_footer_t spfooter = {
            .magic        = PARITY_FOOTER_MAGIC,
            .version      = PARITY_VERSION,
            .trailer_size = (uint32_t)(sizeof(shdr_par) + rs_sz + sizeof(stream_crc)
                             + sizeof(rs_data_len_s) + sizeof(spfooter)),
        };
        if (write(tfd, &spfooter, sizeof(spfooter)) != (ssize_t)sizeof(spfooter)) {
            close(tfd); unlink(tmppath);
            return set_error_errno(ERR_IO, "stream: write parity footer");
        }
    }
    /* ---- End parity trailer ---- */

    /* Evict payload pages — no parity re-read needed */
    posix_fadvise(tfd, (off_t)sizeof(hdr), (off_t)total_written, POSIX_FADV_DONTNEED);

    if (async_writeback(tfd) == -1) { close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: fdatasync"); }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = repo_objects_fd(repo);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "stream: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        unlink(tmppath); return set_error_errno(ERR_IO, "stream: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    if (subfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "stream: openat(%s)", subdir); }

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s",
                 repo_path(repo), subdir, fname) >= (int)sizeof(dstpath)) {
        close(subfd); unlink(tmppath); return set_error(ERR_IO, "stream: dstpath too long");
    }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            close(subfd); unlink(tmppath); return OK;
        }
        close(subfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: rename(%s, %s)", tmppath, dstpath);
    }

    close(subfd);
    repo_loose_set_insert(repo, out_hash);
    if (out_is_new) *out_is_new = 1;
    if (out_phys_bytes) *out_phys_bytes = (uint64_t)sizeof(object_header_t) + total_written;
    return OK;
}

status_t object_store(repo_t *repo, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return object_store_ex(repo, type, data, len, out_hash, NULL, NULL);
}

status_t object_store_ex(repo_t *repo, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes) {
    return write_object(repo, type, data, len, out_hash, out_is_new, out_phys_bytes);
}

/* ------------------------------------------------------------------ */
/* Sparse-aware file storage                                           */
/* ------------------------------------------------------------------ */

status_t object_store_file(repo_t *repo, int fd, uint64_t file_size,
                           uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return object_store_file_ex(repo, fd, file_size, out_hash, NULL, NULL);
}

static status_t object_store_file_ex_cb(repo_t *repo, int fd,
                                        const char *src_path,
                                        uint64_t file_size,
                                        uint8_t out_hash[OBJECT_HASH_SIZE],
                                        int *out_is_new, uint64_t *out_phys_bytes,
                                        xfer_progress_fn progress_cb,
                                        void *progress_ctx);

/* Streaming writer for sparse objects. Writes [sparse_hdr_t][regions[]][data]
 * to a new object without ever materializing the full payload in RAM.
 * Takes ownership of fd (closes on return). */
static status_t write_sparse_object_stream(repo_t *repo, int fd,
                                           const char *src_path,
                                           uint64_t file_size,
                                           const sparse_region_t *regions,
                                           uint32_t n_regions,
                                           uint8_t out_hash[OBJECT_HASH_SIZE],
                                           int *out_is_new,
                                           uint64_t *out_phys_bytes) {
    (void)file_size;  /* uncompressed_size is the sparse payload size, not file size */

    /* Compute total payload size (header + regions table + region data) */
    uint64_t data_bytes = 0;
    for (uint32_t i = 0; i < n_regions; i++) {
        if (regions[i].length > UINT64_MAX - data_bytes) {
            if (src_path) close(fd);
            return set_error(ERR_TOO_LARGE, "sparse: region data_bytes overflow");
        }
        data_bytes += regions[i].length;
    }
    uint64_t payload_sz = (uint64_t)sizeof(sparse_hdr_t)
                        + (uint64_t)n_regions * sizeof(sparse_region_t)
                        + data_bytes;

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) {
        if (src_path) close(fd);
        return set_error(ERR_IO, "sparse_stream: tmppath too long");
    }
    int tfd = mkstemp(tmppath);
    if (tfd == -1) {
        if (src_path) close(fd);
        return set_error_errno(ERR_IO, "mkstemp(%s)", tmppath);
    }

    object_header_t hdr = {
        .magic             = OBJECT_MAGIC,
        .version           = OBJECT_HDR_VERSION,
        .type              = OBJECT_TYPE_SPARSE,
        .compression       = COMPRESS_NONE,
        .pack_skip_ver     = 0,
        .uncompressed_size = payload_sz,
        .compressed_size   = 0,   /* patched below */
    };
    if (write(tfd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        if (src_path) close(fd);
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: write header");
    }

    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx || EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(mdctx);
        if (src_path) close(fd);
        close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "sparse_stream: EVP init");
    }

    char ps_tmp_dir[PATH_MAX];
    snprintf(ps_tmp_dir, sizeof(ps_tmp_dir), "%s/tmp", repo_path(repo));
    rs_parity_stream_t ps;
    if (rs_parity_stream_init(&ps, 256 * 1024 * 1024, ps_tmp_dir) != 0) {
        EVP_MD_CTX_free(mdctx);
        if (src_path) close(fd);
        close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "sparse_stream: rs init");
    }

    uint32_t crc = 0;
    uint64_t total_written = 0;
    status_t st = OK;

    /* Helper macro: hash + crc + rs + write to tfd */
    #define SP_EMIT(buf, n) do {                                               \
        if (EVP_DigestUpdate(mdctx, (buf), (n)) != 1) {                        \
            st = set_error(ERR_IO, "sparse_stream: SHA256 update"); goto sp_done; \
        }                                                                      \
        if (io_write_full(tfd, (buf), (n)) != 0) {                             \
            st = set_error_errno(ERR_IO, "sparse_stream: write tfd"); goto sp_done; \
        }                                                                      \
        crc = crc32c_update(crc, (buf), (n));                                  \
        if (rs_parity_stream_feed(&ps, (buf), (n)) != 0) {                     \
            st = set_error_errno(ERR_IO, "sparse_stream: rs feed"); goto sp_done; \
        }                                                                      \
        total_written += (uint64_t)(n);                                        \
    } while (0)

    /* Write sparse header */
    sparse_hdr_t shdr = { .magic = SPARSE_MAGIC, .region_count = n_regions };
    SP_EMIT(&shdr, sizeof(shdr));

    /* Write regions table */
    SP_EMIT(regions, (size_t)n_regions * sizeof(sparse_region_t));

    /* Stream each region's data */
    uint8_t *buf = malloc(STREAM_CHUNK);
    if (!buf) { st = set_error(ERR_NOMEM, "sparse_stream: buf alloc"); goto sp_done; }

    for (uint32_t r = 0; r < n_regions && st == OK; r++) {
        if (lseek(fd, (off_t)regions[r].offset, SEEK_SET) == (off_t)-1) {
            st = set_error_errno(ERR_IO, "sparse_stream: lseek region %u", r); break;
        }
        uint64_t remaining = regions[r].length;
        while (remaining > 0) {
            size_t want = (remaining > STREAM_CHUNK) ? STREAM_CHUNK : (size_t)remaining;
            ssize_t got = read(fd, buf, want);
            if (got <= 0) {
                if (got == -1 && errno == EINTR) continue;
                st = set_error_errno(ERR_IO, "sparse_stream: read region %u", r);
                break;
            }
            SP_EMIT(buf, (size_t)got);
            remaining -= (uint64_t)got;
        }
    }
    free(buf);

    #undef SP_EMIT

sp_done:
    if (src_path) close(fd);

    if (st != OK) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return st;
    }

    if (total_written != payload_sz) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return set_error(ERR_IO, "sparse_stream: payload size mismatch %llu != %llu",
                         (unsigned long long)total_written, (unsigned long long)payload_sz);
    }

    if (rs_parity_stream_finish(&ps) != 0) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: rs finish");
    }

    unsigned int dlen = 0;
    if (EVP_DigestFinal_ex(mdctx, out_hash, &dlen) != 1 || dlen != OBJECT_HASH_SIZE) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return set_error(ERR_IO, "sparse_stream: SHA256 finalize");
    }
    EVP_MD_CTX_free(mdctx);

    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(repo, out_hash)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return OK;
    }

    hdr.compressed_size = total_written;
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);
    if (pwrite(tfd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: pwrite header");
    }

    /* Parity trailer (V2 object, parity V1 footer format) */
    parity_record_t hpar;
    parity_record_compute(&hdr, sizeof(hdr), &hpar);
    if (lseek(tfd, 0, SEEK_END) == (off_t)-1 ||
        write(tfd, &hpar, sizeof(hpar)) != (ssize_t)sizeof(hpar)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: write hpar");
    }
    size_t rs_sz = rs_parity_stream_total(&ps);
    if (rs_sz > 0 && rs_parity_stream_replay_fd(&ps, tfd) != 0) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: write rs parity");
    }
    rs_parity_stream_destroy(&ps);

    if (write(tfd, &crc, sizeof(crc)) != (ssize_t)sizeof(crc)) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: write crc");
    }
    /* Deprecated rs_data_len field — always 0, see parity.h. */
    uint32_t rs_data_len = PARITY_RS_DATA_LEN_DEPRECATED;
    if (write(tfd, &rs_data_len, sizeof(rs_data_len)) != (ssize_t)sizeof(rs_data_len)) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: write rs_data_len (deprecated)");
    }
    parity_footer_t pftr = {
        .magic = PARITY_FOOTER_MAGIC,
        .version = PARITY_VERSION,
        .trailer_size = (uint32_t)(sizeof(hpar) + rs_sz + sizeof(crc)
                                   + sizeof(rs_data_len) + sizeof(pftr)),
    };
    if (write(tfd, &pftr, sizeof(pftr)) != (ssize_t)sizeof(pftr)) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: write footer");
    }

    if (async_writeback(tfd) != 0) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: fdatasync");
    }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);
    int objfd = repo_objects_fd(repo);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "sparse_stream: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        unlink(tmppath); return set_error_errno(ERR_IO, "sparse_stream: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    if (subfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "sparse_stream: openat(%s)", subdir); }
    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s",
                 repo_path(repo), subdir, fname) >= (int)sizeof(dstpath)) {
        close(subfd); unlink(tmppath); return set_error(ERR_IO, "sparse_stream: dstpath too long");
    }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            close(subfd); unlink(tmppath); return OK;
        }
        close(subfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: rename(%s, %s)", tmppath, dstpath);
    }
    close(subfd);
    repo_loose_set_insert(repo, out_hash);
    if (out_is_new) *out_is_new = 1;
    if (out_phys_bytes) *out_phys_bytes = (uint64_t)sizeof(object_header_t) + total_written;
    return OK;
}

status_t object_store_file_ex(repo_t *repo, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes) {
    return object_store_file_ex_cb(repo, fd, NULL, file_size, out_hash,
                                   out_is_new, out_phys_bytes, NULL, NULL);
}

static status_t object_store_file_ex_cb(repo_t *repo, int fd,
                                        const char *src_path,
                                        uint64_t file_size,
                                        uint8_t out_hash[OBJECT_HASH_SIZE],
                                        int *out_is_new, uint64_t *out_phys_bytes,
                                        xfer_progress_fn progress_cb,
                                        void *progress_ctx) {
    if (file_size == 0) {
        if (src_path) close(fd);
        uint8_t empty = 0;
        return write_object(repo, OBJECT_TYPE_FILE, &empty, 0, out_hash,
                            out_is_new, out_phys_bytes);
    }

    /* Discover data regions using SEEK_DATA / SEEK_HOLE */
    sparse_region_t *regions = NULL;
    uint32_t n_regions = 0, regions_cap = 0;
    int is_sparse = 0;

    off_t pos = lseek(fd, 0, SEEK_DATA);
    if (pos == -1 && errno == ENXIO && file_size == 0) {
        /* Empty file, shouldn't reach here but be safe */
        is_sparse = 0;
    } else if (pos == -1 && errno == ENXIO) {
        /* SEEK_DATA returns ENXIO for two reasons:
         * 1. File is entirely a hole (all zeroes) — legitimate sparse
         * 2. Filesystem (e.g. FUSE/shfs) doesn't support SEEK_DATA
         *
         * Distinguish by attempting a probe read.  A real all-zero sparse
         * file will read zeroes; a FUSE that doesn't support SEEK_DATA
         * will also read real data.  Either way we fall through to the
         * non-sparse streaming path which handles both correctly. */
        char probe;
        if (pread(fd, &probe, 1, 0) == 1) {
            /* File is readable — SEEK_DATA unsupported or has real data.
             * Treat as non-sparse to avoid misclassifying. */
            is_sparse = 0;
        } else {
            /* Can't even read the file — real I/O error */
            if (src_path) close(fd);
            return set_error_errno(ERR_IO, "store: read probe failed");
        }
    } else if (pos != -1) {
        /* Check if first data region starts at 0 — if not, file is sparse */
        if (pos != 0) is_sparse = 1;

        while (pos >= 0 && (uint64_t)pos < file_size) {
            off_t data_start = pos;
            off_t hole_start = lseek(fd, data_start, SEEK_HOLE);
            if (hole_start == -1) hole_start = (off_t)file_size;
            if ((uint64_t)hole_start > file_size) hole_start = (off_t)file_size;

            if (hole_start > data_start) {
                if (n_regions == regions_cap) {
                    uint32_t nc = regions_cap ? regions_cap * 2 : 8;
                    sparse_region_t *tmp = realloc(regions, nc * sizeof(*tmp));
                    if (!tmp) { free(regions); if (src_path) close(fd); return set_error(ERR_NOMEM, "realloc sparse regions"); }
                    regions = tmp; regions_cap = nc;
                }
                regions[n_regions].offset = (uint64_t)data_start;
                regions[n_regions].length = (uint64_t)(hole_start - data_start);
                n_regions++;
                if (hole_start < data_start) break;  /* shouldn't happen */
                if ((uint64_t)hole_start < file_size) is_sparse = 1;
            }

            /* advance past the hole */
            pos = lseek(fd, hole_start, SEEK_DATA);
            if (pos == -1) break;  /* no more data */
        }
    }

    /* If the file isn't sparse (or the FS doesn't report holes), read it whole.
     * write_object_file_stream takes ownership of fd (closes it on return). */
    if (!is_sparse || n_regions == 0) {
        free(regions);
        return write_object_file_stream(repo, fd, src_path, file_size, out_hash,
                                        out_is_new, out_phys_bytes,
                                        progress_cb, progress_ctx);
    }

    /* Stream sparse object — bounded RAM regardless of file size. */
    status_t st = write_sparse_object_stream(repo, fd, src_path, file_size,
                                             regions, n_regions,
                                             out_hash, out_is_new, out_phys_bytes);
    free(regions);
    return st;
}

status_t object_store_file_cb(repo_t *repo, int fd, const char *src_path,
                              uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes,
                              xfer_progress_fn cb, void *cb_ctx) {
    if (file_size == 0) {
        if (src_path) close(fd);  /* we own fd when src_path is set */
        uint8_t empty = 0;
        return write_object(repo, OBJECT_TYPE_FILE, &empty, 0, out_hash,
                            out_is_new, out_phys_bytes);
    }
    /* Sparse-aware path handles both sparse and non-sparse files.
     * For non-sparse files it falls through to write_object_file_stream.
     * The progress callback is only used on the streaming (non-sparse) path. */
    return object_store_file_ex_cb(repo, fd, src_path, file_size, out_hash,
                                   out_is_new, out_phys_bytes, cb, cb_ctx);
}

/* ------------------------------------------------------------------ */
/* Load                                                                */
/* ------------------------------------------------------------------ */

status_t object_load(repo_t *repo,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not a loose object — try pack files */
        return pack_object_load(repo, hash, out_data, out_size, out_type);
    }

    object_header_t hdr;
    if (io_read_full(fd, &hdr, sizeof(hdr)) != 0) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd); return set_error(ERR_CORRUPT, "object %s: truncated header", hex);
    }
    if (hdr.magic != OBJECT_MAGIC) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd); return set_error(ERR_CORRUPT, "object %s: bad magic", hex);
    }
    if (hdr.version != 1 && hdr.version != 2) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd); return set_error(ERR_CORRUPT, "object %s: unsupported version %u", hex, hdr.version);
    }

    /* Large uncompressed objects must be loaded via object_load_stream. */
    if (hdr.compression == COMPRESS_NONE && hdr.compressed_size > STREAM_LOAD_MAX) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd);
        return set_error(ERR_TOO_LARGE, "object %s: %lu bytes, use streaming load", hex, (unsigned long)hdr.compressed_size);
    }

    char *cpayload = malloc((size_t)hdr.compressed_size);
    if (!cpayload) { close(fd); return set_error(ERR_NOMEM, "malloc(%zu)", (size_t)hdr.compressed_size); }
    if (io_read_full(fd, cpayload, (size_t)hdr.compressed_size) != 0) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        free(cpayload); close(fd); return set_error(ERR_CORRUPT, "object %s: truncated payload", hex);
    }

    /* ---- Parity check/repair for version 2 objects ---- */
    if (hdr.version == 2) {
        /* Read the entire parity trailer in one sequential read — the file
         * position is already right after the payload, which is the trailer
         * start.  This avoids 4-5 extra seeks per loose object on HDD. */
        size_t rs_sz = rs_parity_size((size_t)hdr.compressed_size);
        size_t trailer_sz = sizeof(parity_record_t) + rs_sz
                          + sizeof(uint32_t) * 2 + sizeof(parity_footer_t);
        uint8_t *trailer = malloc(trailer_sz);
        if (trailer && io_read_full(fd, trailer, trailer_sz) == 0) {
            /* Parse footer at the end */
            parity_footer_t pftr;
            memcpy(&pftr, trailer + trailer_sz - sizeof(pftr), sizeof(pftr));
            if (pftr.magic == PARITY_FOOTER_MAGIC &&
                pftr.version == PARITY_VERSION &&
                pftr.trailer_size == (uint32_t)trailer_sz) {
                /* Header parity record */
                parity_record_t hdr_par;
                memcpy(&hdr_par, trailer, sizeof(hdr_par));
                int hrc = parity_record_check(&hdr, sizeof(hdr), &hdr_par);
                if (hrc == 1) {
                    log_msg("WARN", "object: repaired corrupt header via parity");
                    parity_stats_add_repaired(1);
                } else if (hrc < 0) {
                    log_msg("WARN", "object: header parity check failed (uncorrectable)");
                    parity_stats_add_uncorrectable(1);
                }

                /* Payload CRC + RS repair */
                uint32_t stored_crc;
                memcpy(&stored_crc,
                       trailer + sizeof(hdr_par) + rs_sz,
                       sizeof(stored_crc));
                uint32_t cur_crc = crc32c(cpayload, (size_t)hdr.compressed_size);
                if (cur_crc != stored_crc && rs_sz > 0) {
                    int rrc = rs_parity_decode(cpayload, (size_t)hdr.compressed_size,
                                               trailer + sizeof(hdr_par));
                    if (rrc > 0) {
                        log_msg("WARN", "object: RS repaired corrupt payload bytes");
                        parity_stats_add_repaired(1);
                    } else if (rrc < 0) {
                        log_msg("WARN", "object: RS repair failed (uncorrectable)");
                        parity_stats_add_uncorrectable(1);
                    }
                }
            }
        }
        free(trailer);
    }
    close(fd);

    void *data;
    size_t data_sz;
    if (hdr.compression == COMPRESS_NONE) {
        if (hdr.uncompressed_size != hdr.compressed_size) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            free(cpayload);
            return set_error(ERR_CORRUPT, "object %s: size mismatch (uncompressed)", hex);
        }
        data    = cpayload;
        data_sz = (size_t)hdr.uncompressed_size;
    } else {
        /* Loose objects are always COMPRESS_NONE — compression happens at
         * pack time.  Any other value indicates corruption. */
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        free(cpayload);
        return set_error(ERR_CORRUPT, "object %s: unexpected compression %u in loose object", hex, hdr.compression);
    }

    /* verify hash */
    uint8_t got[OBJECT_HASH_SIZE];
    sha256(data, data_sz, got);
    if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        free(data);
        return set_error(ERR_CORRUPT, "object %s: hash mismatch", hex);
    }

    *out_data = data;
    *out_size = data_sz;
    if (out_type) *out_type = hdr.type;
    return OK;
}

status_t object_load_stream(repo_t *repo,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             int out_fd,
                             uint64_t *out_size,
                             uint8_t *out_type)
{
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not a loose object — try pack files (streaming to handle large objects). */
        uint64_t psz = 0; uint8_t ptype = 0;
        status_t st = pack_object_load_stream(repo, hash, out_fd, &psz, &ptype);
        if (st != OK) return st;
        if (out_size) *out_size = psz;
        if (out_type) *out_type = ptype;
        return OK;
    }

    object_header_t hdr;
    if (io_read_full(fd, &hdr, sizeof(hdr)) != 0) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd); return set_error(ERR_CORRUPT, "object %s: truncated header", hex);
    }
    if (hdr.magic != OBJECT_MAGIC) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd); return set_error(ERR_CORRUPT, "object %s: bad magic", hex);
    }
    if (hdr.version != 1 && hdr.version != 2) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd); return set_error(ERR_CORRUPT, "object %s: unsupported version %u", hex, hdr.version);
    }

    /* For version 2 objects, attempt header repair via parity.
     * Use pread at known trailer offset (header + payload) to avoid seeking. */
    if (hdr.version == 2) {
        off_t trailer_off = (off_t)sizeof(hdr) + (off_t)hdr.compressed_size;
        parity_record_t hpar;
        parity_footer_t pftr;
        /* Read header parity record + footer in two preads (no seek) */
        if (pread(fd, &hpar, sizeof(hpar), trailer_off) == (ssize_t)sizeof(hpar)) {
            size_t rs_sz = rs_parity_size((size_t)hdr.compressed_size);
            off_t ftr_off = trailer_off + (off_t)sizeof(hpar) + (off_t)rs_sz
                          + (off_t)(sizeof(uint32_t) * 2);
            if (pread(fd, &pftr, sizeof(pftr), ftr_off) == (ssize_t)sizeof(pftr) &&
                pftr.magic == PARITY_FOOTER_MAGIC && pftr.version == PARITY_VERSION) {
                int hrc = parity_record_check(&hdr, sizeof(hdr), &hpar);
                if (hrc == 1) {
                    log_msg("WARN", "object: repaired corrupt header via parity (stream)");
                    parity_stats_add_repaired(1);
                } else if (hrc < 0) {
                    log_msg("WARN", "object: header parity uncorrectable (stream)");
                    parity_stats_add_uncorrectable(1);
                }
            }
        }
        /* File position is still at sizeof(hdr) — no seek needed. */
    }

    if (out_type) *out_type = hdr.type;
    if (out_size) *out_size = hdr.uncompressed_size;

    if (hdr.compression == COMPRESS_NONE) {
        /* Stream payload to out_fd in chunks, hashing as we go. */
        EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
        if (!mdctx) { close(fd); return set_error(ERR_NOMEM, "EVP_MD_CTX_new"); }
        if (EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
            EVP_MD_CTX_free(mdctx); close(fd); return set_error(ERR_IO, "load_stream: SHA256 init failed");
        }

        uint8_t *buf = malloc(STREAM_LOAD_MAX);
        if (!buf) { EVP_MD_CTX_free(mdctx); close(fd); return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_LOAD_MAX); }

        uint64_t remaining = hdr.compressed_size;
        status_t st = OK;
        while (remaining > 0 && st == OK) {
            size_t want = (remaining > STREAM_LOAD_MAX) ? STREAM_LOAD_MAX : (size_t)remaining;
            if (io_read_full(fd, buf, want) != 0)           { st = set_error(ERR_CORRUPT, "load_stream: truncated payload"); break; }
            if (EVP_DigestUpdate(mdctx, buf, want) != 1) { st = set_error(ERR_IO, "load_stream: SHA256 update failed"); break; }
            if (io_write_full(out_fd, buf, want) != 0)      { st = set_error_errno(ERR_IO, "load_stream: write to out_fd"); break; }
            remaining -= want;
        }
        free(buf);
        close(fd);

        if (st == OK) {
            uint8_t got[OBJECT_HASH_SIZE];
            unsigned dlen = 0;
            if (EVP_DigestFinal_ex(mdctx, got, &dlen) != 1 ||
                dlen != OBJECT_HASH_SIZE ||
                memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
                char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
                st = set_error(ERR_CORRUPT, "object %s: hash mismatch (stream)", hex);
            }
        }
        EVP_MD_CTX_free(mdctx);
        return st;

    } else {
        /* Loose objects are always COMPRESS_NONE — compression happens at
         * pack time.  Any other value indicates corruption. */
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd);
        return set_error(ERR_CORRUPT, "object %s: unexpected compression %u in loose object", hex, hdr.compression);
    }
}

status_t object_load_prefix(repo_t *repo,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             size_t max_bytes,
                             void **out_data, size_t *out_size,
                             uint64_t *out_full_size,
                             uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not loose — try pack files */
        return pack_object_load_prefix(repo, hash, max_bytes,
                                        out_data, out_size, out_full_size, out_type);
    }

    object_header_t hdr;
    if (io_read_full(fd, &hdr, sizeof(hdr)) != 0) {
        close(fd); return set_error(ERR_CORRUPT, "object_load_prefix: truncated header");
    }
    if (hdr.magic != OBJECT_MAGIC || (hdr.version != 1 && hdr.version != 2)) {
        close(fd); return set_error(ERR_CORRUPT, "object_load_prefix: bad header");
    }

    /* Loose objects are always uncompressed — read just the prefix */
    if (out_full_size) *out_full_size = hdr.uncompressed_size;
    if (out_type) *out_type = hdr.type;

    size_t want = max_bytes;
    if (want > hdr.uncompressed_size) want = (size_t)hdr.uncompressed_size;

    char *buf = malloc(want);
    if (!buf) { close(fd); return set_error(ERR_NOMEM, "object_load_prefix: alloc"); }
    if (io_read_full(fd, buf, want) != 0) {
        free(buf); close(fd);
        return set_error(ERR_CORRUPT, "object_load_prefix: short read");
    }
    close(fd);

    *out_data = buf;
    *out_size = want;
    return OK;
}

status_t object_get_info(repo_t *repo,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint64_t *out_size, uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1)
        return pack_object_get_info(repo, hash, out_size, out_type);

    object_header_t hdr;
    int rc = io_read_full(fd, &hdr, sizeof(hdr));
    close(fd);
    if (rc != 0) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        return set_error(ERR_CORRUPT, "object %s: truncated header", hex);
    }
    if (hdr.magic != OBJECT_MAGIC) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        return set_error(ERR_CORRUPT, "object %s: bad magic", hex);
    }

    if (out_size) *out_size = hdr.uncompressed_size;
    if (out_type) *out_type = hdr.type;
    return OK;
}

/*
 * Stream `size` bytes from src_fd into the object store as object type `type`.
 * The data is stored uncompressed (COMPRESS_NONE).  The SHA-256 of the raw
 * bytes is computed while streaming and verified against expected_hash.
 * Returns OK, ERR_NOMEM, ERR_IO, or ERR_CORRUPT (hash mismatch).
 */
status_t object_store_fd(repo_t *repo, uint8_t type, int src_fd, uint64_t size,
                          const uint8_t expected_hash[OBJECT_HASH_SIZE]) {
    /* Check existence first to avoid redundant work. */
    if (object_exists(repo, expected_hash)) return OK;

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) return set_error(ERR_IO, "store_fd: tmppath too long");

    int tfd = mkstemp(tmppath);
    if (tfd == -1) return set_error_errno(ERR_IO, "mkstemp(%s)", tmppath);

    object_header_t hdr = {
        .magic             = OBJECT_MAGIC,
        .version           = OBJECT_HDR_VERSION,
        .type              = type,
        .compression       = COMPRESS_NONE,
        .pack_skip_ver     = 0,
        .uncompressed_size = size,
        .compressed_size   = size,
    };
    memcpy(hdr.hash, expected_hash, OBJECT_HASH_SIZE);
    if (write(tfd, &hdr, sizeof(hdr)) != (ssize_t)sizeof(hdr)) {
        close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: write header");
    }

    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx) { close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "EVP_MD_CTX_new"); }
    if (EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return set_error(ERR_IO, "store_fd: SHA256 init failed");
    }

    uint8_t *buf = malloc(STREAM_LOAD_MAX);
    if (!buf) {
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_LOAD_MAX);
    }

    uint64_t remaining = size;
    status_t st = OK;
    while (remaining > 0) {
        size_t want = (remaining < STREAM_LOAD_MAX) ? (size_t)remaining : STREAM_LOAD_MAX;
        ssize_t r = read(src_fd, buf, want);
        if (r <= 0) { st = set_error_errno(ERR_IO, "store_fd: read src_fd"); break; }
        if (EVP_DigestUpdate(mdctx, buf, (size_t)r) != 1) { st = set_error(ERR_IO, "store_fd: SHA256 update failed"); break; }
        if (io_write_full(tfd, buf, (size_t)r) != 0) { st = set_error_errno(ERR_IO, "store_fd: write payload"); break; }
        remaining -= (uint64_t)r;
    }
    free(buf);

    if (st == OK) {
        uint8_t got[OBJECT_HASH_SIZE];
        unsigned int dlen = OBJECT_HASH_SIZE;
        if (EVP_DigestFinal_ex(mdctx, got, &dlen) != 1 ||
            dlen != OBJECT_HASH_SIZE ||
            memcmp(got, expected_hash, OBJECT_HASH_SIZE) != 0) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(expected_hash, hex);
            st = set_error(ERR_CORRUPT, "object %s: hash mismatch (store_fd)", hex);
        }
    }
    EVP_MD_CTX_free(mdctx);

    if (st != OK) { close(tfd); unlink(tmppath); return st; }
    if (async_writeback(tfd) != 0) { close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: fdatasync"); }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(expected_hash, subdir, fname);
    int objfd = repo_objects_fd(repo);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", repo_path(repo), subdir, fname)
        >= (int)sizeof(dstpath)) { unlink(tmppath); return set_error(ERR_IO, "store_fd: dstpath too long"); }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && object_exists(repo, expected_hash)) {
            unlink(tmppath);
            return OK;
        }
        unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: rename(%s, %s)", tmppath, dstpath);
    }

    repo_loose_set_insert(repo, expected_hash);
    return OK;
}

int object_repair(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    int fd = open(path, O_RDWR);
    if (fd == -1) return -1;  /* not a loose object or not writable */

    object_header_t hdr;
    if (io_read_full(fd, &hdr, sizeof(hdr)) != 0) { close(fd); return -1; }
    if (hdr.magic != OBJECT_MAGIC || hdr.version != 2) { close(fd); return 0; }

    /* Read parity footer */
    parity_footer_t pftr;
    off_t file_end = lseek(fd, 0, SEEK_END);
    if (file_end < (off_t)sizeof(pftr)) { close(fd); return 0; }
    if (pread(fd, &pftr, sizeof(pftr), file_end - (off_t)sizeof(pftr))
        != (ssize_t)sizeof(pftr) ||
        pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION) {
        close(fd); return 0;
    }

    off_t trailer_start = file_end - (off_t)pftr.trailer_size;
    if (trailer_start < (off_t)(sizeof(hdr) + hdr.compressed_size)) {
        close(fd); return 0;
    }

    int total_fixed = 0;

    /* Header: XOR parity repair and write back */
    parity_record_t hdr_par;
    if (pread(fd, &hdr_par, sizeof(hdr_par), trailer_start) == (ssize_t)sizeof(hdr_par)) {
        object_header_t hdr_copy = hdr;
        int hrc = parity_record_check(&hdr_copy, sizeof(hdr_copy), &hdr_par);
        if (hrc == 1) {
            /* Write corrected header back */
            if (pwrite(fd, &hdr_copy, sizeof(hdr_copy), 0) == (ssize_t)sizeof(hdr_copy))
                total_fixed++;
        }
    }

    /* Payload: CRC check then RS repair */
    if (hdr.compressed_size > 0 && hdr.compressed_size <= (128ull * 1024 * 1024 * 1024)) {
        size_t csize = (size_t)hdr.compressed_size;
        uint32_t stored_crc;
        off_t crc_off = file_end - (off_t)sizeof(pftr)
                        - (off_t)sizeof(uint32_t) - (off_t)sizeof(uint32_t);
        if (pread(fd, &stored_crc, sizeof(stored_crc), crc_off) == (ssize_t)sizeof(stored_crc)) {
            char *cpayload = malloc(csize);
            if (cpayload) {
                if (pread(fd, cpayload, csize, (off_t)sizeof(hdr)) == (ssize_t)csize) {
                    uint32_t cur_crc = crc32c(cpayload, csize);
                    if (cur_crc != stored_crc) {
                        size_t rs_sz = rs_parity_size(csize);
                        if (rs_sz > 0) {
                            uint8_t *rs_par = malloc(rs_sz);
                            if (rs_par) {
                                off_t rs_off = trailer_start + (off_t)sizeof(hdr_par);
                                if (pread(fd, rs_par, rs_sz, rs_off) == (ssize_t)rs_sz) {
                                    rs_init();
                                    int rrc = rs_parity_decode(cpayload, csize, rs_par);
                                    if (rrc > 0) {
                                        /* Write corrected payload back */
                                        if (pwrite(fd, cpayload, csize, (off_t)sizeof(hdr))
                                            == (ssize_t)csize)
                                            total_fixed += rrc;
                                    }
                                }
                                free(rs_par);
                            }
                        }
                    }
                }
                free(cpayload);
            }
        }
    }

    if (total_fixed > 0)
        fsync(fd);
    close(fd);
    return total_fixed;
}
