#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "util.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <pthread.h>

#include <lz4.h>
#include <lz4frame.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

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
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd != -1) {
        int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
        close(objfd);
        if (subfd != -1) {
            int exists = (faccessat(subfd, fname, F_OK, 0) == 0);
            close(subfd);
            if (exists) return 1;
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
    int fd = open(path, O_RDONLY);
    if (fd != -1) {
        struct stat st;
        if (fstat(fd, &st) == 0) {
            close(fd);
            *out_bytes = (uint64_t)st.st_size;
            return OK;
        }
        close(fd);
        return set_error_errno(ERR_IO, "fstat(%s)", path);
    }

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

    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd == -1) { free(compressed); return set_error_errno(ERR_IO, "openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        close(objfd);
        free(compressed);
        return set_error_errno(ERR_IO, "mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
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
    uint32_t rs_data_len = (uint32_t)payload_size;
    if (write(fd, &rs_data_len, sizeof(rs_data_len)) != (ssize_t)sizeof(rs_data_len)) {
        st = set_error_errno(ERR_IO, "write_object: write rs_data_len");
        goto fail;
    }

    parity_footer_t pfooter = {
        .magic        = PARITY_FOOTER_MAGIC,
        .version      = PARITY_VERSION,
        .trailer_size = (uint32_t)(sizeof(hdr_par) + rs_sz + sizeof(pcrc)
                         + sizeof(rs_data_len) + sizeof(pfooter)),
    };
    if (write(fd, &pfooter, sizeof(pfooter)) != (ssize_t)sizeof(pfooter)) {
        st = set_error_errno(ERR_IO, "write_object: write parity footer");
        goto fail;
    }
    /* ---- End parity trailer ---- */

    if (fsync(fd) == -1) { st = set_error_errno(ERR_IO, "write_object: fsync"); goto fail; }
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
    {
        char dirpath[PATH_MAX];
        if (snprintf(dirpath, sizeof(dirpath), "%s/objects/%s", repo_path(repo), subdir)
            >= (int)sizeof(dirpath)) { st = set_error(ERR_IO, "write_object: dirpath too long"); goto fail; }
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    close(subfd); free(compressed);
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
/* Double-buffered streaming: reader thread fills one slot while the   */
/* processor (main thread) hashes + writes the other.                 */
/* ------------------------------------------------------------------ */

#define STREAM_CHUNK ((size_t)(16 * 1024 * 1024))  /* 16 MiB per slot */

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cond;
    int             src_fd;
    uint64_t        file_size;
    uint8_t        *bufs[2];
    size_t          lens[2];
    int             is_last[2];
    int             state[2];   /* 0 = empty (reader fills), 1 = full (processor reads) */
    status_t        reader_st;
    int             abort;
} dbl_buf_t;

static void *dbl_reader_fn(void *arg) {
    dbl_buf_t *d      = arg;
    uint64_t   offset = 0;
    int        slot   = 0;

    posix_fadvise(d->src_fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    while (offset < d->file_size) {
        size_t want = STREAM_CHUNK;
        if ((uint64_t)want > d->file_size - offset)
            want = (size_t)(d->file_size - offset);

        /* Wait for slot to become empty */
        pthread_mutex_lock(&d->mu);
        while (d->state[slot] != 0 && !d->abort)
            pthread_cond_wait(&d->cond, &d->mu);
        if (d->abort) { pthread_mutex_unlock(&d->mu); return NULL; }
        pthread_mutex_unlock(&d->mu);

        /* Read without the lock held */
        size_t got = 0;
        while (got < want) {
            ssize_t r = read(d->src_fd, d->bufs[slot] + got, want - got);
            if (r <= 0) {
                if (r == 0) errno = ESTALE;
                pthread_mutex_lock(&d->mu);
                d->reader_st = set_error_errno(ERR_IO, "dbl_reader: read(src_fd)");
                d->abort     = 1;
                pthread_cond_broadcast(&d->cond);
                pthread_mutex_unlock(&d->mu);
                return NULL;
            }
            got += (size_t)r;
        }

        /* Source pages are now in our buffer; no need to keep them in cache */
        posix_fadvise(d->src_fd, (off_t)offset, (off_t)want, POSIX_FADV_DONTNEED);
        offset += (uint64_t)got;

        pthread_mutex_lock(&d->mu);
        d->lens[slot]    = got;
        d->is_last[slot] = (offset >= d->file_size);
        d->state[slot]   = 1;
        pthread_cond_broadcast(&d->cond);
        pthread_mutex_unlock(&d->mu);

        slot ^= 1;
    }
    return NULL;
}

/* Probe size and compression threshold for LZ4 frame decision */
#define PROBE_SIZE        (64  * 1024)          /* 64 KiB sample          */
#define PROBE_RATIO_MAX   0.98                  /* skip LZ4 if > 98% size */

static status_t write_object_file_stream(repo_t *repo, int src_fd,
                                         uint64_t file_size,
                                         uint8_t out_hash[OBJECT_HASH_SIZE],
                                         int *out_is_new, uint64_t *out_phys_bytes,
                                         xfer_progress_fn progress_cb,
                                         void *progress_ctx) {
    /* Always store uncompressed; the pack sweeper handles compression. */
    if (lseek(src_fd, 0, SEEK_SET) == (off_t)-1) return set_error_errno(ERR_IO, "lseek(src_fd)");

    /* ----------------------------------------------------------------
     * Create temp file and write placeholder header.
     * compressed_size will be patched after writing.
     * ---------------------------------------------------------------- */
    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) return set_error(ERR_IO, "stream: tmppath too long");

    int tfd = mkstemp(tmppath);
    if (tfd == -1) return set_error_errno(ERR_IO, "mkstemp(%s)", tmppath);

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
        close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: write header");
    }

    uint64_t total_written = 0;

    /* ----------------------------------------------------------------
     * Hash context + double-buffered reader thread
     * ---------------------------------------------------------------- */
    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx || EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(mdctx);
        close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "EVP_MD_CTX_new");
    }

    dbl_buf_t dbl;
    dbl.src_fd     = src_fd;
    dbl.file_size  = file_size;
    dbl.reader_st  = OK;
    dbl.abort      = 0;
    dbl.lens[0]    = 0;   dbl.lens[1]    = 0;
    dbl.is_last[0] = 0;   dbl.is_last[1] = 0;
    dbl.state[0]   = 0;   dbl.state[1]   = 0;
    dbl.bufs[0]    = malloc(STREAM_CHUNK);
    dbl.bufs[1]    = malloc(STREAM_CHUNK);
    if (!dbl.bufs[0] || !dbl.bufs[1]) {
        free(dbl.bufs[0]); free(dbl.bufs[1]);
        EVP_MD_CTX_free(mdctx);
        close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_CHUNK);
    }
    pthread_mutex_init(&dbl.mu, NULL);
    pthread_cond_init(&dbl.cond, NULL);

    pthread_t reader_thr;
    if (pthread_create(&reader_thr, NULL, dbl_reader_fn, &dbl) != 0) {
        free(dbl.bufs[0]); free(dbl.bufs[1]);
        pthread_mutex_destroy(&dbl.mu);
        pthread_cond_destroy(&dbl.cond);
        EVP_MD_CTX_free(mdctx);
        close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "pthread_create(reader)");
    }

    /* ----------------------------------------------------------------
     * Main loop: hash + (compress +) write each buffer slot
     * ---------------------------------------------------------------- */
    status_t st            = OK;
    int      slot      = 0;
    off_t    write_off = (off_t)sizeof(hdr) + (off_t)total_written;

    for (;;) {
        pthread_mutex_lock(&dbl.mu);
        while (dbl.state[slot] != 1 && !dbl.abort)
            pthread_cond_wait(&dbl.cond, &dbl.mu);
        int      do_abort = dbl.abort;
        status_t rdr_st   = dbl.reader_st;
        size_t   len      = dbl.lens[slot];
        int      is_last  = dbl.is_last[slot];
        pthread_mutex_unlock(&dbl.mu);

        if (do_abort) { st = (rdr_st != OK) ? rdr_st : set_error(ERR_IO, "stream: reader thread aborted"); break; }

        /* Hash the uncompressed bytes */
        if (EVP_DigestUpdate(mdctx, dbl.bufs[slot], len) != 1) {
            st = set_error(ERR_IO, "stream: SHA256 update failed"); goto abort_reader;
        }

        /* Write raw bytes (always uncompressed) */
        if (write(tfd, dbl.bufs[slot], len) != (ssize_t)len) {
            if (errno == 0) errno = EIO;
            st = set_error_errno(ERR_IO, "stream: write payload chunk"); goto abort_reader;
        }
        posix_fadvise(tfd, write_off, (off_t)len, POSIX_FADV_DONTNEED);
        write_off     += (off_t)len;
        total_written += len;
        if (progress_cb) progress_cb((uint64_t)len, progress_ctx);

        pthread_mutex_lock(&dbl.mu);
        dbl.state[slot] = 0;
        pthread_cond_broadcast(&dbl.cond);
        pthread_mutex_unlock(&dbl.mu);

        if (is_last) break;
        slot ^= 1;
        continue;

abort_reader:
        pthread_mutex_lock(&dbl.mu);
        dbl.abort = 1;
        pthread_cond_broadcast(&dbl.cond);
        pthread_mutex_unlock(&dbl.mu);
        break;
    }

    pthread_join(reader_thr, NULL);
    if (st == OK && dbl.reader_st != OK) st = dbl.reader_st;
    pthread_cond_destroy(&dbl.cond);
    pthread_mutex_destroy(&dbl.mu);
    free(dbl.bufs[1]);
    free(dbl.bufs[0]);

    /* (No LZ4 frame end mark — write path always uses COMPRESS_NONE) */

    if (st != OK) {
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return st;
    }

    unsigned int dlen = 0;
    if (EVP_DigestFinal_ex(mdctx, out_hash, &dlen) != 1 || dlen != OBJECT_HASH_SIZE) {
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return set_error(ERR_IO, "stream: SHA256 finalize failed");
    }
    EVP_MD_CTX_free(mdctx);

    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(repo, out_hash)) {
        close(tfd); unlink(tmppath); return OK;
    }

    /* Patch header: fill in sizes and hash now that we know them */
    hdr.uncompressed_size = file_size;
    hdr.compressed_size   = total_written;
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);
    if (pwrite(tfd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: pwrite header");
    }

    /* ---- Parity trailer (version 2) ----
     * Read payload back from temp file (in page cache) to compute RS parity.
     */
    {
        parity_record_t shdr_par;
        parity_record_compute(&hdr, sizeof(hdr), &shdr_par);
        if (lseek(tfd, 0, SEEK_END) == (off_t)-1 ||
            write(tfd, &shdr_par, sizeof(shdr_par)) != (ssize_t)sizeof(shdr_par)) {
            close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: write parity header");
        }

        /* Compute CRC and RS parity by reading payload back in groups */
        size_t payload_len = (size_t)total_written;
        size_t rs_sz = rs_parity_size(payload_len);
        uint8_t *rs_par_buf = NULL;
        if (rs_sz > 0) {
            rs_par_buf = malloc(rs_sz);
            if (!rs_par_buf) { close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "malloc(%zu)", rs_sz); }
        }

        uint32_t stream_crc = 0;
        size_t remaining_p = payload_len;
        off_t read_pos = (off_t)sizeof(hdr);
        size_t rs_par_off = 0;

        while (remaining_p > 0) {
            size_t group_data = RS_K * (size_t)RS_INTERLEAVE;  /* 15296 */
            size_t chunk = remaining_p < group_data ? remaining_p : group_data;
            uint8_t *tmp_buf = malloc(chunk);
            if (!tmp_buf) { free(rs_par_buf); close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "malloc(%zu)", chunk); }
            if (pread(tfd, tmp_buf, chunk, read_pos) != (ssize_t)chunk) {
                free(tmp_buf); free(rs_par_buf); close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: pread parity chunk");
            }
            stream_crc = crc32c_update(stream_crc, tmp_buf, chunk);
            if (rs_par_buf) {
                size_t grp_par = rs_parity_size(chunk);
                rs_parity_encode(tmp_buf, chunk, rs_par_buf + rs_par_off);
                rs_par_off += grp_par;
            }
            free(tmp_buf);
            read_pos += (off_t)chunk;
            remaining_p -= chunk;
        }

        if (rs_par_buf && rs_sz > 0) {
            if (write(tfd, rs_par_buf, rs_sz) != (ssize_t)rs_sz) {
                free(rs_par_buf); close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: write RS parity");
            }
            free(rs_par_buf);
        }

        if (write(tfd, &stream_crc, sizeof(stream_crc)) != (ssize_t)sizeof(stream_crc)) {
            close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: write CRC");
        }
        uint32_t rs_data_len_s = (uint32_t)payload_len;
        if (write(tfd, &rs_data_len_s, sizeof(rs_data_len_s)) != (ssize_t)sizeof(rs_data_len_s)) {
            close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: write rs_data_len");
        }

        parity_footer_t spfooter = {
            .magic        = PARITY_FOOTER_MAGIC,
            .version      = PARITY_VERSION,
            .trailer_size = (uint32_t)(sizeof(shdr_par) + rs_sz + sizeof(stream_crc)
                             + sizeof(rs_data_len_s) + sizeof(spfooter)),
        };
        if (write(tfd, &spfooter, sizeof(spfooter)) != (ssize_t)sizeof(spfooter)) {
            close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: write parity footer");
        }
    }
    /* ---- End parity trailer ---- */

    if (fsync(tfd) == -1) { close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: fsync"); }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "stream: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        close(objfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
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

    fsync(subfd);
    close(subfd);
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

status_t object_store_file_ex(repo_t *repo, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes) {
    if (file_size == 0) {
        /* empty file */
        uint8_t empty = 0;
        return write_object(repo, OBJECT_TYPE_FILE, &empty, 0, out_hash,
                            out_is_new, out_phys_bytes);
    }

    /* Discover data regions using SEEK_DATA / SEEK_HOLE */
    sparse_region_t *regions = NULL;
    uint32_t n_regions = 0, regions_cap = 0;
    int is_sparse = 0;

    off_t pos = lseek(fd, 0, SEEK_DATA);
    if (pos == -1 && errno == ENXIO) {
        /* File is entirely a hole (all zeroes) */
        is_sparse = 1;
        /* no regions at all */
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
                    if (!tmp) { free(regions); return set_error(ERR_NOMEM, "realloc sparse regions"); }
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

    /* If the file isn't sparse (or the FS doesn't report holes), read it whole */
    if (!is_sparse || n_regions == 0) {
        free(regions);
        return write_object_file_stream(repo, fd, file_size, out_hash,
                                        out_is_new, out_phys_bytes, NULL, NULL);
    }

    /* Build sparse payload: [sparse_hdr][regions][data bytes] */
    size_t data_bytes = 0;
    for (uint32_t i = 0; i < n_regions; i++) data_bytes += (size_t)regions[i].length;

    size_t payload_sz = sizeof(sparse_hdr_t)
                      + n_regions * sizeof(sparse_region_t)
                      + data_bytes;
    uint8_t *payload = malloc(payload_sz);
    if (!payload) { free(regions); return set_error(ERR_NOMEM, "malloc(%zu)", payload_sz); }

    uint8_t *p = payload;
    sparse_hdr_t shdr = { .magic = SPARSE_MAGIC, .region_count = n_regions };
    memcpy(p, &shdr, sizeof(shdr)); p += sizeof(shdr);
    memcpy(p, regions, n_regions * sizeof(sparse_region_t));
    p += n_regions * sizeof(sparse_region_t);

    /* Read each data region */
    for (uint32_t i = 0; i < n_regions; i++) {
        lseek(fd, (off_t)regions[i].offset, SEEK_SET);
        size_t remaining = (size_t)regions[i].length;
        while (remaining > 0) {
            ssize_t r = read(fd, p, remaining);
            if (r <= 0) {
                free(regions);
                free(payload);
                errno = (r == 0) ? ESTALE : errno;
                return set_error_errno(ERR_IO, "sparse: read region %u", i);
            }
            p += r; remaining -= (size_t)r;
        }
    }

    free(regions);
    status_t st = write_object(repo, OBJECT_TYPE_SPARSE, payload, payload_sz, out_hash,
                               out_is_new, out_phys_bytes);
    free(payload);
    return st;
}

status_t object_store_file_cb(repo_t *repo, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes,
                              xfer_progress_fn cb, void *cb_ctx) {
    if (file_size == 0) {
        uint8_t empty = 0;
        return write_object(repo, OBJECT_TYPE_FILE, &empty, 0, out_hash,
                            out_is_new, out_phys_bytes);
    }
    /* Sparse-aware: detect holes and fall back to write_object for sparse files
     * (sparse files are in-memory; callback fires only for the streaming path). */
    off_t pos = lseek(fd, 0, SEEK_DATA);
    int is_sparse = (pos != -1 && pos != 0) ||
                    (pos == -1 && errno == ENXIO);
    if (!is_sparse && pos != -1) {
        /* Check whether any holes exist inside the file */
        off_t hole = lseek(fd, pos == -1 ? 0 : pos, SEEK_HOLE);
        if (hole != -1 && (uint64_t)hole < file_size) is_sparse = 1;
    }
    if (is_sparse) {
        /* Delegate to the full sparse-aware path (no per-chunk callback). */
        return object_store_file_ex(repo, fd, file_size, out_hash,
                                    out_is_new, out_phys_bytes);
    }
    return write_object_file_stream(repo, fd, file_size, out_hash,
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
    if (hdr.compression == COMPRESS_NONE && hdr.compressed_size > STREAM_CHUNK) {
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
        /* Read parity footer from end of file */
        parity_footer_t pftr;
        off_t file_end = lseek(fd, 0, SEEK_END);
        if (file_end >= (off_t)sizeof(pftr)) {
            if (pread(fd, &pftr, sizeof(pftr),
                      file_end - (off_t)sizeof(pftr)) == (ssize_t)sizeof(pftr) &&
                pftr.magic == PARITY_FOOTER_MAGIC && pftr.version == PARITY_VERSION) {
                /* Read full trailer */
                off_t trailer_start = file_end - (off_t)pftr.trailer_size;
                if (trailer_start >= (off_t)(sizeof(hdr) + hdr.compressed_size)) {
                    /* Repair header if needed */
                    parity_record_t hdr_par;
                    if (pread(fd, &hdr_par, sizeof(hdr_par), trailer_start)
                        == (ssize_t)sizeof(hdr_par)) {
                        int hrc = parity_record_check(&hdr, sizeof(hdr), &hdr_par);
                        if (hrc == 1) {
                            log_msg("WARN", "object: repaired corrupt header via parity");
                            parity_stats_add_repaired(1);
                        } else if (hrc < 0) {
                            log_msg("WARN", "object: header parity check failed (uncorrectable)");
                            parity_stats_add_uncorrectable(1);
                        }
                    }

                    /* Check payload CRC, RS-repair if needed */
                    uint32_t stored_crc;
                    off_t crc_off = file_end - (off_t)sizeof(pftr)
                                    - (off_t)sizeof(uint32_t) - (off_t)sizeof(uint32_t);
                    if (pread(fd, &stored_crc, sizeof(stored_crc), crc_off)
                        == (ssize_t)sizeof(stored_crc)) {
                        uint32_t cur_crc = crc32c(cpayload, (size_t)hdr.compressed_size);
                        if (cur_crc != stored_crc) {
                            /* Try RS repair */
                            size_t rs_sz = rs_parity_size((size_t)hdr.compressed_size);
                            if (rs_sz > 0) {
                                uint8_t *rs_par = malloc(rs_sz);
                                if (rs_par) {
                                    off_t rs_off = trailer_start + (off_t)sizeof(hdr_par);
                                    if (pread(fd, rs_par, rs_sz, rs_off) == (ssize_t)rs_sz) {
                                        int rrc = rs_parity_decode(cpayload, (size_t)hdr.compressed_size, rs_par);
                                        if (rrc > 0) {
                                            log_msg("WARN", "object: RS repaired corrupt payload bytes");
                                            parity_stats_add_repaired(1);
                                        } else if (rrc < 0) {
                                            log_msg("WARN", "object: RS repair failed (uncorrectable)");
                                            parity_stats_add_uncorrectable(1);
                                        }
                                    }
                                    free(rs_par);
                                }
                            }
                        }
                    }
                }
            }
        }
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
    } else if (hdr.compression == COMPRESS_LZ4_FRAME) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        free(cpayload);
        return set_error(ERR_TOO_LARGE, "object %s: LZ4 frame requires streaming load", hex);
    } else if (hdr.compression == COMPRESS_LZ4) {
        if (hdr.compressed_size > (uint64_t)INT_MAX ||
            hdr.uncompressed_size > (uint64_t)INT_MAX) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            free(cpayload);
            return set_error(ERR_TOO_LARGE, "object %s: exceeds LZ4 INT_MAX limit", hex);
        }
        char *out = malloc((size_t)hdr.uncompressed_size);
        if (!out) { free(cpayload); return set_error(ERR_NOMEM, "malloc(%zu)", (size_t)hdr.uncompressed_size); }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)hdr.compressed_size,
                                    (int)hdr.uncompressed_size);
        free(cpayload);
        if (r < 0 || (uint64_t)r != hdr.uncompressed_size) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(hash, hex);
            free(out);
            return set_error(ERR_CORRUPT, "object %s: lz4 decompress failed", hex);
        }
        data    = out;
        data_sz = (size_t)hdr.uncompressed_size;
    } else {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        free(cpayload); return set_error(ERR_CORRUPT, "object %s: unknown compression %u", hex, hdr.compression);
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

    /* For version 2 objects, attempt header repair via parity */
    if (hdr.version == 2) {
        parity_footer_t pftr;
        off_t fend = lseek(fd, 0, SEEK_END);
        if (fend >= (off_t)sizeof(pftr) &&
            pread(fd, &pftr, sizeof(pftr), fend - (off_t)sizeof(pftr)) == (ssize_t)sizeof(pftr) &&
            pftr.magic == PARITY_FOOTER_MAGIC && pftr.version == PARITY_VERSION) {
            off_t tstart = fend - (off_t)pftr.trailer_size;
            parity_record_t hpar;
            if (pread(fd, &hpar, sizeof(hpar), tstart) == (ssize_t)sizeof(hpar)) {
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
        /* Seek back to payload start for the read loops below */
        if (lseek(fd, (off_t)sizeof(hdr), SEEK_SET) == (off_t)-1) {
            close(fd); return set_error_errno(ERR_IO, "load_stream: lseek to payload");
        }
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

        uint8_t *buf = malloc(STREAM_CHUNK);
        if (!buf) { EVP_MD_CTX_free(mdctx); close(fd); return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_CHUNK); }

        uint64_t remaining = hdr.compressed_size;
        status_t st = OK;
        while (remaining > 0 && st == OK) {
            size_t want = (remaining > STREAM_CHUNK) ? STREAM_CHUNK : (size_t)remaining;
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

    } else if (hdr.compression == COMPRESS_LZ4) {
        /* LZ4 API is limited to INT_MAX — objects larger than that cannot be LZ4-compressed. */
        if (hdr.compressed_size > (uint64_t)INT_MAX ||
            hdr.uncompressed_size > (uint64_t)INT_MAX) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            close(fd);
            return set_error(ERR_TOO_LARGE, "object %s: exceeds LZ4 INT_MAX limit", hex);
        }
        char *cpayload = malloc((size_t)hdr.compressed_size);
        if (!cpayload) { close(fd); return set_error(ERR_NOMEM, "malloc(%zu)", (size_t)hdr.compressed_size); }
        if (io_read_full(fd, cpayload, (size_t)hdr.compressed_size) != 0) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            free(cpayload); close(fd); return set_error(ERR_CORRUPT, "object %s: truncated LZ4 payload", hex);
        }
        close(fd);

        char *out = malloc((size_t)hdr.uncompressed_size);
        if (!out) { free(cpayload); return set_error(ERR_NOMEM, "malloc(%zu)", (size_t)hdr.uncompressed_size); }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)hdr.compressed_size,
                                    (int)hdr.uncompressed_size);
        free(cpayload);
        if (r < 0 || (uint64_t)r != hdr.uncompressed_size) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            free(out); return set_error(ERR_CORRUPT, "object %s: LZ4 decompress failed", hex);
        }

        uint8_t got[OBJECT_HASH_SIZE];
        sha256(out, (size_t)hdr.uncompressed_size, got);
        if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            free(out); return set_error(ERR_CORRUPT, "object %s: hash mismatch (LZ4)", hex);
        }

        int wr = io_write_full(out_fd, out, (size_t)hdr.uncompressed_size);
        free(out);
        return wr != 0 ? set_error_errno(ERR_IO, "load_stream: write LZ4 output") : OK;

    } else if (hdr.compression == COMPRESS_LZ4_FRAME) {
        /* Stream-decompress LZ4 frame payload, verify hash over decompressed bytes. */
        uint8_t *src = malloc(STREAM_CHUNK);
        uint8_t *dst = malloc(STREAM_CHUNK);
        LZ4F_dctx *dctx = NULL;
        if (!src || !dst || LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION))) {
            free(src); free(dst);
            if (dctx) LZ4F_freeDecompressionContext(dctx);
            close(fd); return set_error(ERR_NOMEM, "LZ4F decompression context");
        }

        EVP_MD_CTX *fmdctx = EVP_MD_CTX_new();
        if (!fmdctx || EVP_DigestInit_ex(fmdctx, EVP_sha256(), NULL) != 1) {
            EVP_MD_CTX_free(fmdctx);
            LZ4F_freeDecompressionContext(dctx);
            free(src); free(dst);
            close(fd); return set_error(ERR_NOMEM, "EVP_MD_CTX_new (LZ4F)");
        }

        uint64_t remaining = hdr.compressed_size;
        status_t fst = OK;
        while (remaining > 0 && fst == OK) {
            size_t want = (remaining > STREAM_CHUNK) ? STREAM_CHUNK : (size_t)remaining;
            if (io_read_full(fd, src, want) != 0) { fst = set_error(ERR_CORRUPT, "load_stream: truncated LZ4F payload"); break; }
            size_t src_left = want;
            uint8_t *srcp = src;
            while (src_left > 0 && fst == OK) {
                size_t dst_sz  = STREAM_CHUNK;
                size_t src_sz  = src_left;
                size_t ret = LZ4F_decompress(dctx, dst, &dst_sz, srcp, &src_sz, NULL);
                if (LZ4F_isError(ret)) { fst = set_error(ERR_CORRUPT, "load_stream: LZ4F decompress error"); break; }
                if (dst_sz > 0) {
                    if (EVP_DigestUpdate(fmdctx, dst, dst_sz) != 1 ||
                        io_write_full(out_fd, dst, dst_sz) != 0)
                        fst = set_error_errno(ERR_IO, "load_stream: write LZ4F output");
                }
                srcp     += src_sz;
                src_left -= src_sz;
            }
            remaining -= want;
        }
        close(fd);
        free(src); free(dst);
        LZ4F_freeDecompressionContext(dctx);

        if (fst == OK) {
            uint8_t got[OBJECT_HASH_SIZE];
            unsigned int dl = OBJECT_HASH_SIZE;
            if (EVP_DigestFinal_ex(fmdctx, got, &dl) != 1 ||
                dl != OBJECT_HASH_SIZE ||
                memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
                char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
                fst = set_error(ERR_CORRUPT, "object %s: hash mismatch (LZ4F)", hex);
            }
        }
        EVP_MD_CTX_free(fmdctx);
        return fst;

    } else {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        close(fd);
        return set_error(ERR_CORRUPT, "object %s: unknown compression %u", hex, hdr.compression);
    }
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

    uint8_t *buf = malloc(STREAM_CHUNK);
    if (!buf) {
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_CHUNK);
    }

    uint64_t remaining = size;
    status_t st = OK;
    while (remaining > 0) {
        size_t want = (remaining < STREAM_CHUNK) ? (size_t)remaining : STREAM_CHUNK;
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
    if (fsync(tfd) != 0) { close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: fsync"); }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(expected_hash, subdir, fname);
    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        close(objfd); unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    close(objfd);

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

    /* fsync the object directory */
    char dirpath[PATH_MAX];
    if (snprintf(dirpath, sizeof(dirpath), "%s/objects/%s", repo_path(repo), subdir)
        < (int)sizeof(dirpath)) {
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
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
