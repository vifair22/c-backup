#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "object.h"
#include "object_crypt.h"
#include "pack.h"
#include "parity.h"
#include "parity_stream.h"
#include "util.h"
#include "phase_timer.h"
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>


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

/* A loose trailer's bytes besides the RS parity: header parity record,
 * payload CRC, the deprecated rs_data_len and the footer. */
#define LOOSE_TRAILER_FIXED \
    (sizeof(parity_record_t) + 2 * sizeof(uint32_t) + sizeof(parity_footer_t))
_Static_assert(LOOSE_TRAILER_FIXED <= PARITY_TRAILER_FIXED_MAX,
               "the loose trailer must sit under the shared threshold");

/*
 * The write is never refused - design constraint 1 is arbitrary size - but
 * an object whose trailer_size wraps has parity no repair will use, and the
 * operator should hear that when it is written rather than when it is
 * damaged. `what` names the writer, since the hash is not always known yet.
 */
static void warn_if_parity_unusable(uint64_t stored_size, const char *what)
{
    if (parity_fields_fit(stored_size)) return;
    log_warn("%s: stored size %llu is too large for 32-bit parity fields; "
             "its parity cannot be used for repair until the format is fixed",
             what, (unsigned long long)stored_size);
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


/*
 * Read and validate an object header. One layout, one size: there are no
 * older repositories to accommodate, so there is nothing to branch on.
 * Leaves the file positioned at the start of the payload.
 */
/*
 * Read and sanity-check an object header.
 *
 * compressed_size is eight bytes that decide how much memory the reader is
 * about to allocate, and they came off a disk. Checking the magic and the
 * version is not enough: a header whose codec byte is neither of the three
 * we write slips past the "too large for an in-memory load" guard further
 * down, which only applies to COMPRESS_NONE, and reaches malloc with
 * whatever the field says. A fuzzer found that in under a minute - a
 * corrupt header asking for 3.6 exabytes. malloc returns NULL for that and
 * the load fails cleanly, so nothing was ever unsafe, but the check that
 * matters is cheaper and more honest than relying on the allocator to
 * refuse: the payload cannot be bigger than the file holding it.
 *
 * The codecs are checked here too, so exactly one place decides what a
 * plausible header looks like. So is the type: a type this build does not
 * know is refused by name rather than handed to a caller that would treat
 * it as file bytes (OWN-39).
 */
/*
 * A type byte this build does not know may be damage rather than a newer
 * format, and the header parity record can tell: if it reconstructs a
 * header whose type is known, that is the header. Checked here, before the
 * refusal, because every reader refuses at this point and the parity pass
 * the two loaders run afterwards would otherwise never see the header.
 * pread, so the descriptor stays just past the header for the caller.
 */
static int header_parity_fixes_type(int fd, object_header_t *hdr)
{
    off_t trailer_off = (off_t)sizeof(*hdr) + (off_t)hdr->compressed_size;
    parity_record_t hpar;
    parity_footer_t pftr;
    size_t rs_sz = rs_parity_size((size_t)hdr->compressed_size);
    off_t ftr_off = trailer_off + (off_t)sizeof(hpar) + (off_t)rs_sz
                  + (off_t)(sizeof(uint32_t) * 2);
    if (pread(fd, &hpar, sizeof(hpar), trailer_off) != (ssize_t)sizeof(hpar) ||
        pread(fd, &pftr, sizeof(pftr), ftr_off) != (ssize_t)sizeof(pftr) ||
        pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION)
        return 0;
    object_header_t copy = *hdr;
    /* The trailer was found through compressed_size, so a reconstruction
     * that changes it located its own parity through a wrong field. */
    if (parity_record_check(&copy, sizeof(copy), &hpar) != 1 ||
        !object_type_known(copy.type) ||
        copy.compressed_size != hdr->compressed_size)
        return 0;
    log_warn("object: repaired a damaged type byte (%u -> %u) via header parity",
             hdr->type, copy.type);
    parity_stats_add_repaired(1);
    *hdr = copy;
    return 1;
}

static status_t read_object_header(int fd, object_header_t *hdr)
{
    if (io_read_full(fd, hdr, sizeof(*hdr)) != 0)
        return set_error(ERR_CORRUPT, "object: truncated header");
    if (hdr->magic != OBJECT_MAGIC)
        return set_error(ERR_CORRUPT, "object: bad magic");
    if (hdr->version != OBJECT_HDR_VERSION)
        return set_error(ERR_CORRUPT, "object: unsupported version %u", hdr->version);
    if (hdr->compression != COMPRESS_NONE &&
        hdr->compression != COMPRESS_LZ4 &&
        hdr->compression != COMPRESS_LZ4_FRAME)
        return set_error(ERR_CORRUPT, "object: unknown compression codec %u",
                         hdr->compression);
    if (hdr->encryption != ENCRYPT_NONE && hdr->encryption != ENCRYPT_CHACHA20)
        return set_error(ERR_CORRUPT, "object: unknown encryption codec %u",
                         hdr->encryption);

    struct stat sb;
    if (fstat(fd, &sb) != 0)
        return set_error_errno(ERR_IO, "object: fstat");
    /* The payload starts after the header and the parity trailer follows it,
     * so the file is at least header + payload. Anything larger than that is
     * a lie the allocator should not be asked to believe. */
    uint64_t avail = (uint64_t)sb.st_size > sizeof(*hdr)
                     ? (uint64_t)sb.st_size - sizeof(*hdr) : 0;
    if (hdr->compressed_size > avail)
        return set_error(ERR_CORRUPT,
                         "object: header claims %llu payload bytes, file holds %llu",
                         (unsigned long long)hdr->compressed_size,
                         (unsigned long long)avail);
    if (!object_type_known(hdr->type) && !header_parity_fixes_type(fd, hdr))
        return set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, hdr->type);
    return OK;
}

static int obj_name_exists_at(int subfd, const char *fname) {
    return faccessat(subfd, fname, F_OK, 0) == 0;
}

int object_exists(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]) {
    /* Fast path: in-memory loose set (returns 0 if set not built) */
    if (store_loose_set_contains(store, hash)) return 1;

    /* On-disk check — only needed when loose set has not been built */
    if (!store_loose_set_ready(store)) {
        char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
        hash_to_path(hash, subdir, fname);
        int objfd = store_objects_fd(store);
        if (objfd != -1) {
            int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
            if (subfd != -1) {
                int exists = (faccessat(subfd, fname, F_OK, 0) == 0);
                close(subfd);
                if (exists) return 1;
            }
        }
    }
    return pack_object_exists(store, hash);
}

uint64_t object_loose_phys_size(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    int objfd = store_objects_fd(store);
    if (objfd == -1) return 0;
    char rel[3 + OBJECT_HASH_SIZE * 2];
    snprintf(rel, sizeof(rel), "%s/%s", subdir, fname);
    struct stat sb;
    if (fstatat(objfd, rel, &sb, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISREG(sb.st_mode))
        return 0;
    return (uint64_t)sb.st_size;
}

status_t object_physical_size(store_t *store,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes,
                              uint32_t *out_header_bytes) {
    if (!out_bytes) return set_error(ERR_INVALID, "object_physical_size: out_bytes is NULL");

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);
    struct stat st;
    if (stat(path, &st) == 0) {
        *out_bytes = (uint64_t)st.st_size;
        if (out_header_bytes) *out_header_bytes = (uint32_t)sizeof(object_header_t);
        return OK;
    }
    if (errno != ENOENT)
        return set_error_errno(ERR_IO, "stat(%s)", path);

    status_t st_pack = pack_object_physical_size(store, hash, out_bytes);
    if (st_pack == OK && out_header_bytes)
        *out_header_bytes = (uint32_t)sizeof(pack_dat_entry_hdr_t);
    return st_pack;
}

/* Write a blob (already in memory) as an object. */
static status_t write_object(store_t *store, uint8_t type,
                             const void *data, size_t len,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             int *out_is_new, uint64_t *out_phys_bytes) {
    /* Every reader refuses a type outside 1-5 (OWN-39), so writing one
     * would store an object nothing can open. */
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "write_object: " OBJECT_TYPE_UNKNOWN_FMT, type);
    if (store_needs_key(store))
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to write a plaintext object");

    sha256(data, len, out_hash);
    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(store, out_hash)) {
        if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(store, out_hash);
        return OK;
    }

    /* Always store uncompressed; the pack sweeper handles compression. */
    const void *payload = data;
    size_t payload_size = len;
    char *compressed = NULL;

    /*
     * Encrypt after compression and before parity, so the trailer protects
     * what is actually on disk and a scrub can verify an offsite copy without
     * the key.
     */
    uint8_t encryption = ENCRYPT_NONE;
    uint8_t *ciphertext = NULL;
    const repo_key_t *key = store_key(store);
    if (key) {
        ciphertext = malloc(payload_size + OBJECT_CRYPT_OVERHEAD);
        if (!ciphertext) return set_error(ERR_NOMEM, "write_object: cipher buffer");
        status_t est = object_encrypt(key, payload, payload_size, ciphertext);
        if (est != OK) { free(ciphertext); return est; }
        payload = ciphertext;
        payload_size += OBJECT_CRYPT_OVERHEAD;
        encryption = ENCRYPT_CHACHA20;
    }

    object_header_t hdr = {
        .magic             = OBJECT_MAGIC,
        .version           = OBJECT_HDR_VERSION,
        .type              = type,
        .compression       = COMPRESS_NONE,
        .encryption        = encryption,
        .pack_skip_ver     = 0,
        .uncompressed_size = (uint64_t)len,
        .compressed_size   = (uint64_t)payload_size,
    };
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = store_objects_fd(store);
    if (objfd == -1) { free(compressed); free(ciphertext); return set_error_errno(ERR_IO, "openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        free(compressed); free(ciphertext);
        return set_error_errno(ERR_IO, "mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    if (subfd == -1) { free(compressed); free(ciphertext); return set_error_errno(ERR_IO, "openat(%s)", subdir); }

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", store_root(store))
        >= (int)sizeof(tmppath)) {
        close(subfd); free(compressed); free(ciphertext); return set_error(ERR_IO, "write_object: tmppath too long");
    }
    int fd = mkstemp(tmppath);
    if (fd == -1) { close(subfd); free(compressed); free(ciphertext); return set_error_errno(ERR_IO, "mkstemp(%s)", tmppath); }

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
    warn_if_parity_unusable(payload_size, "write_object");
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
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", store_root(store), subdir, fname)
        >= (int)sizeof(dstpath)) { st = set_error(ERR_IO, "write_object: dstpath too long"); goto fail; }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            unlink(tmppath);
            close(subfd); free(compressed); free(ciphertext);
            store_count_write(store, STORE_WR_LOOSE_DISCARD, total_phys);
            if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(store, out_hash);
            return OK;
        }
        st = set_error_errno(ERR_IO, "rename(%s, %s)", tmppath, dstpath);
        goto fail;
    }
    close(subfd); free(compressed); free(ciphertext);
    store_count_write(store, STORE_WR_LOOSE, (uint64_t)sizeof(hdr) + (uint64_t)payload_size);
    store_count_write(store, STORE_WR_LOOSE_PARITY, (uint64_t)pfooter.trailer_size);
    store_loose_set_insert(store, out_hash);
    if (out_is_new) *out_is_new = 1;
    if (out_phys_bytes) *out_phys_bytes = total_phys;
    return st;
fail:
    if (fd >= 0) close(fd);
    unlink(tmppath);
    close(subfd); free(compressed); free(ciphertext);
    return st;
}

/* ------------------------------------------------------------------ */
/* Streaming file -> object store: read and hash on the caller's thread, */
/* encrypt, write and parity on a second one.                         */
/* ------------------------------------------------------------------ */

#define STREAM_CHUNK      ((size_t)(128 * 1024 * 1024)) /* 128 MiB write buffer */
#define STREAM_LOAD_MAX   ((size_t)( 16 * 1024 * 1024)) /*  16 MiB load threshold */
/* The most one read may take between cancellation checks. 8 MiB is tens of
 * milliseconds on a disk and a few on an SSD; small enough that a cancel is
 * prompt, large enough that the check costs nothing. */
#define CANCEL_READ_MAX   ((size_t)(  8 * 1024 * 1024))

/* Probe size and compression threshold for LZ4 frame decision */
#define PROBE_SIZE        (64  * 1024)          /* 64 KiB sample          */
#define PROBE_RATIO_MAX   0.98                  /* skip LZ4 if > 98% size */

#define STREAM_REOPEN_MAX 5   /* max seek/reopen recovery attempts per file */

/*
 * The plain stream's chunk and pipeline (B086).
 *
 * The stream used to read 128 MiB, then hash it, then write it, then feed it
 * to parity, all on one thread. Every pass over a buffer that size came from
 * DRAM rather than cache, and nothing overlapped: a first backup of large
 * files ran at about half the speed `cp` managed on the same disks, and with
 * the repository on tmpfs it was barely faster, which is what a CPU-bound
 * loop looks like.
 *
 * Now each 1 MiB chunk passes through three stages, each on its own thread:
 * the caller reads it (and owns the read-recovery ladder), a hasher feeds it
 * to SHA-256, and a writer encrypts it in place, writes it, and feeds the CRC
 * and the Reed-Solomon stream. Encryption has to wait for the hash, because
 * the hash is over plaintext, and that is the order the ring enforces. SHA-256
 * is serial and is the slowest stage, so a deeper split would only queue
 * behind it. Each stage sees chunks in file order and the writer is the only
 * thread touching the temp file, the cipher and the parity stream, so the
 * bytes on disk are identical to what the single-threaded loop wrote.
 *
 * Memory is bounded at STREAM_PIPE_DEPTH chunks, 8 MiB, where the old buffer
 * was 128 MiB. Files no larger than STREAM_PIPE_MIN run every stage inline on
 * the caller: two threads per small object would cost more than they overlap.
 */
#define STREAM_PIPE_CHUNK ((size_t)(1024 * 1024))
#define STREAM_PIPE_DEPTH 8u
#define STREAM_PIPE_MIN   ((uint64_t)(4 * 1024 * 1024))

/* Which writer step failed, so the reading thread can say so with the
 * messages the single-threaded loop used. Error state is thread-local, so a
 * worker records what happened and the caller raises it. */
enum { SINK_OK = 0, SINK_ENCRYPT, SINK_WRITE, SINK_PARITY };

typedef struct {
    /* Stage state. The hasher alone touches md and the writer alone the rest
     * while they run; the caller has all of it before start and after join. */
    EVP_MD_CTX             *md;
    int                     tfd;
    object_crypt_stream_t  *cs;
    rs_parity_stream_t     *ps;
    uint32_t                crc;
    uint64_t                written;
    int                     hash_fail;
    int                     fail_stage;
    int                     fail_errno;

    /* Stage CPU time, for the io:* phases. Each sum has one writer - the
     * hasher, or the writer - and is read by the caller only after the join,
     * so none of it needs to be atomic. Zero `timed` skips the clock reads
     * when nobody is profiling. */
    int                     timed;
    uint64_t                ns_hash, ns_encrypt, ns_write, ns_parity;
    uint64_t                n_hash, n_consume;

    /* The ring. Chunk i lives in slot i % DEPTH and moves read -> hashed ->
     * written; each count only grows. The reader may fill a slot once the
     * writer has released it, so at most DEPTH chunks are in flight. */
    uint8_t                *buf[STREAM_PIPE_DEPTH];
    size_t                  len[STREAM_PIPE_DEPTH];
    uint64_t                n_read, n_hashed, n_written;
    int                     closing;    /* no more chunks are coming */
    int                     abandon;    /* discard what is queued */
    int                     failed;     /* a stage failed; set under mu */
    int                     threaded;
    pthread_t               hasher, writer;
    pthread_mutex_t         mu;
    pthread_cond_t          cv;
} stream_sink_t;

static void sink_hash(stream_sink_t *s, const uint8_t *b, size_t n)
{
    if (s->hash_fail) return;
    uint64_t t0 = s->timed ? phase_cpu_now() : 0;
    if (EVP_DigestUpdate(s->md, b, n) != 1) s->hash_fail = 1;
    if (s->timed) { s->ns_hash += phase_cpu_now() - t0; s->n_hash++; }
}

static void sink_consume(stream_sink_t *s, uint8_t *b, size_t n)
{
    if (s->fail_stage) return;
    uint64_t t0 = s->timed ? phase_cpu_now() : 0, t1 = t0;
    /* Hashed as plaintext, stored as ciphertext, encrypted in place: the
     * chunk is the writer's until it is released. */
    if (s->cs) {
        if (object_crypt_stream_update(s->cs, b, n, b) != OK) {
            s->fail_stage = SINK_ENCRYPT; s->fail_errno = EIO; return;
        }
        if (s->timed) { t1 = phase_cpu_now(); s->ns_encrypt += t1 - t0; }
    }
    if (io_write_full(s->tfd, b, n) != 0) {
        s->fail_stage = SINK_WRITE; s->fail_errno = errno ? errno : EIO; return;
    }
    uint64_t t2 = s->timed ? phase_cpu_now() : 0;
    if (s->timed) s->ns_write += t2 - t1;
    s->crc = crc32c_update(s->crc, b, n);
    if (rs_parity_stream_feed(s->ps, b, n) != 0) {
        s->fail_stage = SINK_PARITY; s->fail_errno = errno ? errno : EIO; return;
    }
    if (s->timed) { s->ns_parity += phase_cpu_now() - t2; s->n_consume++; }
    s->written += n;
}

/* Hand the stage sums to whoever is profiling this thread, once the stages
 * have been joined. */
static void sink_account(const stream_sink_t *s, uint64_t ns_read, uint64_t n_read)
{
    if (!s->timed || !phase_current) return;
    phase_add_ns(phase_current, PHASE_IO_READ,  ns_read, n_read);
    phase_add_ns(phase_current, PHASE_IO_HASH,  s->ns_hash, s->n_hash);
    if (s->ns_encrypt)
        phase_add_ns(phase_current, PHASE_IO_ENCRYPT, s->ns_encrypt, s->n_consume);
    phase_add_ns(phase_current, PHASE_IO_WRITE,  s->ns_write,  s->n_consume);
    phase_add_ns(phase_current, PHASE_IO_PARITY, s->ns_parity, s->n_consume);
}

static void *sink_hasher(void *arg)
{
    stream_sink_t *s = arg;
    pthread_mutex_lock(&s->mu);
    for (;;) {
        while (s->n_hashed == s->n_read && !s->closing)
            pthread_cond_wait(&s->cv, &s->mu);
        if (s->n_hashed == s->n_read) break;
        unsigned slot = (unsigned)(s->n_hashed % STREAM_PIPE_DEPTH);
        int skip = s->abandon || s->failed;
        pthread_mutex_unlock(&s->mu);

        if (!skip) sink_hash(s, s->buf[slot], s->len[slot]);

        pthread_mutex_lock(&s->mu);
        if (s->hash_fail) s->failed = 1;
        s->n_hashed++;
        pthread_cond_broadcast(&s->cv);
    }
    pthread_mutex_unlock(&s->mu);
    return NULL;
}

static void *sink_writer(void *arg)
{
    stream_sink_t *s = arg;
    pthread_mutex_lock(&s->mu);
    for (;;) {
        /* The hasher exits only once everything read is hashed, so caught up
         * with it, with nothing more coming, is the end. */
        while (s->n_written == s->n_hashed &&
               !(s->closing && s->n_hashed == s->n_read))
            pthread_cond_wait(&s->cv, &s->mu);
        if (s->n_written == s->n_hashed) break;
        unsigned slot = (unsigned)(s->n_written % STREAM_PIPE_DEPTH);
        int skip = s->abandon || s->failed;
        pthread_mutex_unlock(&s->mu);

        if (!skip) sink_consume(s, s->buf[slot], s->len[slot]);

        pthread_mutex_lock(&s->mu);
        if (s->fail_stage) s->failed = 1;
        s->n_written++;
        pthread_cond_broadcast(&s->cv);
    }
    pthread_mutex_unlock(&s->mu);
    return NULL;
}

/* Allocation happens here, on the calling thread, so the fault-injection
 * countdowns (thread-local) still see every buffer the stream takes. */
static int sink_init(stream_sink_t *s, EVP_MD_CTX *md, int tfd,
                     object_crypt_stream_t *cs, rs_parity_stream_t *ps,
                     uint32_t crc, uint64_t written, uint64_t file_size)
{
    memset(s, 0, sizeof(*s));
    s->md = md; s->tfd = tfd; s->cs = cs; s->ps = ps;
    s->crc = crc; s->written = written;
    s->timed = phase_current != NULL;
    unsigned n = file_size > STREAM_PIPE_MIN ? STREAM_PIPE_DEPTH : 1u;
    for (unsigned i = 0; i < n; i++) {
        s->buf[i] = malloc(STREAM_PIPE_CHUNK);
        if (!s->buf[i]) {
            for (unsigned j = 0; j < i; j++) { free(s->buf[j]); s->buf[j] = NULL; }
            return -1;
        }
    }
    if (n == 1) return 0;
    if (pthread_mutex_init(&s->mu, NULL) != 0) goto nothread;
    if (pthread_cond_init(&s->cv, NULL) != 0) {
        pthread_mutex_destroy(&s->mu); goto nothread;
    }
    if (pthread_create(&s->hasher, NULL, sink_hasher, s) != 0) {
        pthread_cond_destroy(&s->cv); pthread_mutex_destroy(&s->mu);
        goto nothread;
    }
    if (pthread_create(&s->writer, NULL, sink_writer, s) != 0) {
        pthread_mutex_lock(&s->mu);
        s->closing = 1;
        pthread_cond_broadcast(&s->cv);
        pthread_mutex_unlock(&s->mu);
        pthread_join(s->hasher, NULL);
        pthread_cond_destroy(&s->cv); pthread_mutex_destroy(&s->mu);
        goto nothread;
    }
    s->threaded = 1;
    return 0;
nothread:
    /* No threads is not an error: the inline path writes the same bytes. */
    for (unsigned i = 1; i < n; i++) { free(s->buf[i]); s->buf[i] = NULL; }
    return 0;
}

static int sink_failed(const stream_sink_t *s)
{
    return s->hash_fail || s->fail_stage;
}

/* The next free chunk, or NULL once a stage has failed. */
static uint8_t *sink_slot(stream_sink_t *s)
{
    if (!s->threaded) return sink_failed(s) ? NULL : s->buf[0];
    pthread_mutex_lock(&s->mu);
    while (s->n_read - s->n_written == STREAM_PIPE_DEPTH && !s->failed)
        pthread_cond_wait(&s->cv, &s->mu);
    uint8_t *b = s->failed ? NULL : s->buf[s->n_read % STREAM_PIPE_DEPTH];
    pthread_mutex_unlock(&s->mu);
    return b;
}

/* Queue the chunk sink_slot returned, now holding n bytes. */
static void sink_push(stream_sink_t *s, size_t n)
{
    if (!s->threaded) {
        sink_hash(s, s->buf[0], n);
        if (!s->hash_fail) sink_consume(s, s->buf[0], n);
        return;
    }
    pthread_mutex_lock(&s->mu);
    s->len[s->n_read % STREAM_PIPE_DEPTH] = n;
    s->n_read++;
    pthread_cond_broadcast(&s->cv);
    pthread_mutex_unlock(&s->mu);
}

/* Drain (or, with abandon, discard) what is queued, stop the workers and free
 * the chunks. Afterwards every piece of stage state is the caller's again. */
static void sink_close(stream_sink_t *s, int abandon)
{
    if (s->threaded) {
        pthread_mutex_lock(&s->mu);
        s->closing = 1;
        if (abandon) s->abandon = 1;
        pthread_cond_broadcast(&s->cv);
        pthread_mutex_unlock(&s->mu);
        pthread_join(s->hasher, NULL);
        pthread_join(s->writer, NULL);
        pthread_cond_destroy(&s->cv);
        pthread_mutex_destroy(&s->mu);
        s->threaded = 0;
    }
    for (unsigned i = 0; i < STREAM_PIPE_DEPTH; i++) { free(s->buf[i]); s->buf[i] = NULL; }
}

/* Raise a stage failure on the calling thread. */
static status_t sink_error(const stream_sink_t *s)
{
    if (s->hash_fail) return set_error(ERR_IO, "stream: SHA256 update failed");
    errno = s->fail_errno;
    switch (s->fail_stage) {
    case SINK_ENCRYPT: return set_error(ERR_IO, "stream: encryption failed");
    case SINK_WRITE:   return set_error_errno(ERR_IO, "stream: write payload chunk");
    case SINK_PARITY:  return set_error_errno(ERR_IO, "stream: RS parity feed failed (spill)");
    default:           return OK;
    }
}

static status_t write_object_file_stream(store_t *store, int src_fd,
                                         const char *src_path,
                                         uint64_t file_size,
                                         size_t parity_cap,
                                         uint8_t out_hash[OBJECT_HASH_SIZE],
                                         int *out_is_new, uint64_t *out_phys_bytes,
                                         xfer_progress_fn progress_cb,
                                         void *progress_ctx) {
    uint64_t _pt_setup = phase_now();
    if (store_needs_key(store)) {
        if (src_path) close(src_fd);
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to write a plaintext object");
    }

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
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", store_root(store))
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

    /*
     * Encryption, when the repository has a key.
     *
     * The nonce is drawn now and written ahead of the ciphertext, so a reader
     * has it before the first byte it needs to decrypt. Chunks are encrypted
     * in place: the plaintext is hashed first, and the chunk is the
     * consumer's until it is written, so a second buffer per chunk would be
     * memory spent on nothing.
     */
    const repo_key_t *enc_key = store_key(store);
    object_crypt_stream_t *cs = NULL;
    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    if (enc_key) {
        status_t cst = object_crypt_stream_begin(&cs, enc_key, nonce, 1);
        if (cst != OK) {
            if (src_path) close(src_fd);
            close(tfd); unlink(tmppath);
            return cst;
        }
        if (write(tfd, nonce, sizeof(nonce)) != (ssize_t)sizeof(nonce)) {
            object_crypt_stream_free(cs);
            if (src_path) close(src_fd);
            close(tfd); unlink(tmppath);
            return set_error_errno(ERR_IO, "stream: write nonce");
        }
        total_written += sizeof(nonce);
    }

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

    posix_fadvise(src_fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    /* ----------------------------------------------------------------
     * RS parity accumulator — bounded-RAM streaming, spills to temp file.
     * ---------------------------------------------------------------- */
    char ps_tmp_dir[PATH_MAX];
    snprintf(ps_tmp_dir, sizeof(ps_tmp_dir), "%s/tmp", store_root(store));
    rs_parity_stream_t ps;
    int ps_ok = rs_parity_stream_init(&ps, parity_cap, ps_tmp_dir);
    if (ps_ok != 0) {
        EVP_MD_CTX_free(mdctx);
        if (src_path) close(src_fd);
        close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "stream: rs parity stream init");
    }
    ps.spill_counter = store_write_counter(store, STORE_WR_PARITY_SPILL);

    phase_add(phase_current, PHASE_OBJ_SETUP, _pt_setup);

    /* ----------------------------------------------------------------
     * Main loop: read → hash → write → accumulate CRC & RS parity
     * ---------------------------------------------------------------- */
    uint64_t _pt_io = phase_now();
    status_t st     = OK;
    uint32_t stream_crc = 0;
    uint64_t offset = 0;

    /*
     * Parity protects what is stored, and the nonce is stored.
     *
     * This fed back the first twelve bytes of the file for one commit, which
     * is the object header, not the nonce - the nonce follows it. CRC and
     * parity then described a payload that did not exist, so every whole-file
     * load of an encrypted object failed its CRC and ran an RS decode against
     * parity computed over the wrong bytes. The streaming reader never
     * noticed because it checks the hash rather than the CRC, which is why a
     * clean large-object round trip proved nothing.
     */
    if (cs) {
        stream_crc = crc32c_update(stream_crc, nonce, sizeof(nonce));
        if (rs_parity_stream_feed(&ps, nonce, sizeof(nonce)) != 0)
            st = set_error_errno(ERR_IO, "stream: RS parity feed failed (nonce)");
    }

    /* From here until sink_close the consumer thread owns tfd, cs, ps, the
     * CRC and the byte count; see STREAM_PIPE_CHUNK. */
    stream_sink_t sink;
    if (sink_init(&sink, mdctx, tfd, cs, &ps, stream_crc, total_written, file_size) != 0 &&
        st == OK)
        st = set_error(ERR_NOMEM, "malloc(%zu)", STREAM_PIPE_CHUNK);

    #define STREAM_LOG(logfn, msg) logfn("%s", (msg))

    /* The reader's share of the io:* phases; the other stages keep theirs in
     * the sink. */
    uint64_t io_read_ns = 0, io_reads = 0;

    {
        /* Direct read, with recovery for sources that fail transiently. */
        int recoveries = 0;
        int last_was_seek = 0;

        while (st == OK && offset < file_size) {
            size_t want = STREAM_PIPE_CHUNK;
            if ((uint64_t)want > file_size - offset)
                want = (size_t)(file_size - offset);

            uint8_t *buf = sink_slot(&sink);
            if (!buf) { st = sink_error(&sink); goto done; }

            /* Read with EINTR + transient-error retry */
            size_t got = 0;
            int    retries = 0;
            while (got < want) {
                /*
                 * Cancellation is checked per read, and a read is capped, so
                 * a cancel lands within one CANCEL_READ_MAX of I/O rather
                 * than at the end of the file. A pipeline chunk is smaller
                 * than the cap today; the cap is what keeps that true.
                 */
                if (store_cancelled(store)) {
                    st = set_error(ERR_BUSY, "cancelled while storing an object");
                    goto done;
                }
                size_t ask = want - got;
                if (ask > CANCEL_READ_MAX) ask = CANCEL_READ_MAX;
                uint64_t _rc = sink.timed ? phase_cpu_now() : 0;
                ssize_t r = read(src_fd, buf + got, ask);
                if (sink.timed) { io_read_ns += phase_cpu_now() - _rc; io_reads++; }
                if (r > 0) {
                    got += (size_t)r; retries = 0; last_was_seek = 0;
                    store_io_progress(store, (uint64_t)r);
                    continue;
                }
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
                    STREAM_LOG(log_warn, rmsg);
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
                    STREAM_LOG(log_error, rmsg);
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
                        STREAM_LOG(log_warn, rmsg);
                        errno = saved_errno;
                        continue;
                    }
                    snprintf(rmsg, sizeof(rmsg),
                             "stream: seek recovery failed (%s) at offset %llu/%llu (%.1f%%), escalating to reopen",
                             strerror(errno),
                             (unsigned long long)(offset + got),
                             (unsigned long long)file_size,
                             file_size > 0 ? 100.0 * (double)(offset + got) / (double)file_size : 0.0);
                    STREAM_LOG(log_warn, rmsg);
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
                    STREAM_LOG(log_warn, rmsg);

                    int new_fd = open(src_path, O_RDONLY);
                    if (new_fd < 0) {
                        snprintf(rmsg, sizeof(rmsg),
                                 "stream: reopen failed (%s), giving up",
                                 strerror(errno));
                        STREAM_LOG(log_error, rmsg);
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
                        STREAM_LOG(log_error, rmsg);
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
                    STREAM_LOG(log_warn, rmsg);
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
                STREAM_LOG(log_error, rmsg);
                errno = saved_errno;
                st = set_error_errno(ERR_IO, "%s", rmsg);
                goto done;
            }

            /* Drop source pages from cache */
            posix_fadvise(src_fd, (off_t)offset, (off_t)want, POSIX_FADV_DONTNEED);

            /* Hashed, then encrypted in place, by the stages behind this
             * one: the chunk is not ours after this. */
            sink_push(&sink, got);

            offset += (uint64_t)got;
            if (progress_cb) progress_cb((uint64_t)got, progress_ctx);
        }
    }

    #undef STREAM_LOG

done:
    sink_close(&sink, st != OK);
    sink_account(&sink, io_read_ns, io_reads);
    if (st == OK && sink_failed(&sink)) st = sink_error(&sink);
    stream_crc    = sink.crc;
    total_written = sink.written;
    if (src_path) close(src_fd);  /* only close if we own the fd */

    /* (No LZ4 frame end mark — write path always uses COMPRESS_NONE) */

    /*
     * Close the cipher and store its tag with the payload. It has to land
     * before the parity trailer, because parity covers what is stored and
     * the tag is part of that.
     */
    if (st == OK && cs) {
        uint8_t tag[OBJECT_CRYPT_TAG_BYTES];
        st = object_crypt_stream_finish(cs, tag, NULL);
        if (st == OK) {
            if (write(tfd, tag, sizeof(tag)) != (ssize_t)sizeof(tag)) {
                st = set_error_errno(ERR_IO, "stream: write tag");
            } else {
                total_written += sizeof(tag);
                stream_crc = crc32c_update(stream_crc, tag, sizeof(tag));
                if (rs_parity_stream_feed(&ps, tag, sizeof(tag)) != 0)
                    st = set_error_errno(ERR_IO, "stream: RS parity feed failed (tag)");
            }
        }
    }
    object_crypt_stream_free(cs);
    cs = NULL;
    phase_add(phase_current, PHASE_OBJ_IO, _pt_io);

    /* Flush final partial RS group */
    uint64_t _pt_fin = phase_now();
    if (st == OK) {
        if (rs_parity_stream_finish(&ps) != 0)
            st = set_error_errno(ERR_IO, "stream: RS parity finish failed");
    }
    phase_add(phase_current, PHASE_OBJ_FINISH, _pt_fin);

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
    if (object_exists(store, out_hash)) {
        /* The whole payload is already in tmp/ by now: a duplicate large
         * file costs its full size in writes, and this is where it shows. */
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        store_count_write(store, STORE_WR_LOOSE_DISCARD,
                          (uint64_t)sizeof(hdr) + total_written);
        if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(store, out_hash);
        return OK;
    }

    /* Patch header: fill in sizes and hash now that we know them */
    hdr.uncompressed_size = file_size;
    hdr.compressed_size   = total_written;   /* nonce, ciphertext and tag */
    hdr.encryption        = enc_key ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);
    if (pwrite(tfd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "stream: pwrite header");
    }

    /* ---- Parity trailer (version 2) ----
     * CRC and RS parity were computed inline during the write loop.
     */
    uint64_t stream_trailer_sz = 0;
    {
        parity_record_t shdr_par;
        parity_record_compute(&hdr, sizeof(hdr), &shdr_par);
        if (lseek(tfd, 0, SEEK_END) == (off_t)-1 ||
            write(tfd, &shdr_par, sizeof(shdr_par)) != (ssize_t)sizeof(shdr_par)) {
            rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
            return set_error_errno(ERR_IO, "stream: write parity header");
        }

        size_t rs_sz = rs_parity_stream_total(&ps);
        warn_if_parity_unusable(hdr.compressed_size, "stream");

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
        stream_trailer_sz = spfooter.trailer_size;
    }
    /* ---- End parity trailer ---- */

    /*
     * Evict payload pages - no parity re-read needed.
     *
     * Timed with the writeback below because that is what it costs: the
     * pages are still dirty, so DONTNEED starts their writeback rather than
     * dropping them, and on btrfs that was 0.6-0.9 s of a 3 GiB backup spent
     * here (B086). Dropping both calls did not make the backup faster; the
     * same work then landed on the writer as dirty-page throttling. It stays
     * for what it was for: a large backup not evicting the page cache.
     */
    uint64_t _pt_sync = phase_now();
    posix_fadvise(tfd, (off_t)sizeof(hdr), (off_t)total_written, POSIX_FADV_DONTNEED);

    if (async_writeback(tfd) == -1) { close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: fdatasync"); }
    phase_add(phase_current, PHASE_OBJ_SYNC, _pt_sync);
    uint64_t _pt_inst = phase_now();
    uint64_t _pt_cl = phase_now();
    close(tfd);
    phase_add(phase_current, PHASE_OBJ_CLOSE, _pt_cl);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    uint64_t _pt_md = phase_now();
    int objfd = store_objects_fd(store);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "stream: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        unlink(tmppath); return set_error_errno(ERR_IO, "stream: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    if (subfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "stream: openat(%s)", subdir); }
    phase_add(phase_current, PHASE_OBJ_MKDIR, _pt_md);

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s",
                 store_root(store), subdir, fname) >= (int)sizeof(dstpath)) {
        close(subfd); unlink(tmppath); return set_error(ERR_IO, "stream: dstpath too long");
    }
    uint64_t _pt_rn = phase_now();
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            close(subfd); unlink(tmppath);
            store_count_write(store, STORE_WR_LOOSE_DISCARD,
                              (uint64_t)sizeof(hdr) + total_written + stream_trailer_sz);
            if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(store, out_hash);
            return OK;
        }
        close(subfd); unlink(tmppath); return set_error_errno(ERR_IO, "stream: rename(%s, %s)", tmppath, dstpath);
    }
    phase_add(phase_current, PHASE_OBJ_RENAME, _pt_rn);

    close(subfd);
    store_count_write(store, STORE_WR_LOOSE, (uint64_t)sizeof(hdr) + total_written);
    store_count_write(store, STORE_WR_LOOSE_PARITY, stream_trailer_sz);
    uint64_t _pt_ls = phase_now();
    store_loose_set_insert(store, out_hash);
    phase_add(phase_current, PHASE_OBJ_LOOSESET, _pt_ls);
    if (out_is_new) *out_is_new = 1;
    /* The parity trailer is on disk whether or not the caller asked about it,
     * so it belongs in the physical size. Leaving it out here while the
     * small-object path counted it made the figure disagree with itself
     * between objects in one repo, and understated it worst for the large
     * files that dominate a repository. */
    if (out_phys_bytes)
        *out_phys_bytes = (uint64_t)sizeof(object_header_t) + total_written
                        + stream_trailer_sz;
    phase_add(phase_current, PHASE_OBJ_INSTALL, _pt_inst);
    return OK;
}

status_t object_store(store_t *store, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return object_store_ex(store, type, data, len, out_hash, NULL, NULL);
}

status_t object_store_ex(store_t *store, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes) {
    return write_object(store, type, data, len, out_hash, out_is_new, out_phys_bytes);
}

/* ------------------------------------------------------------------ */
/* Sparse-aware file storage                                           */
/* ------------------------------------------------------------------ */

status_t object_store_file(store_t *store, int fd, uint64_t file_size,
                           uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return object_store_file_ex(store, fd, file_size, out_hash, NULL, NULL);
}

/* How much RS parity one streamed object keeps in memory before spilling it
 * to tmp/. 256 MiB covers the parity of a file of about 3.8 GiB. */
#define STREAM_PARITY_CAP_DEFAULT ((size_t)256 * 1024 * 1024)

static status_t object_store_file_ex_cb(store_t *store, int fd,
                                        const char *src_path,
                                        uint64_t file_size,
                                        uint8_t out_hash[OBJECT_HASH_SIZE],
                                        int *out_is_new, uint64_t *out_phys_bytes,
                                        uint64_t *out_payload_size,
                                        pack_backup_writer_t *pack_w,
                                        size_t parity_cap,
                                        xfer_progress_fn progress_cb,
                                        void *progress_ctx);

/*
 * One place where a sparse payload turns into stored bytes.
 *
 * This was a macro that hashed, wrote, CRC'd and fed parity in four lines.
 * Encryption does not fit that shape: the hash covers the plaintext and
 * everything after it covers the ciphertext, and two of the three callers
 * hand over memory they do not own (a stack struct and the caller's region
 * table), so encrypting in place is not available the way it is on the
 * ordinary streaming path. A scratch buffer and a real function cost one
 * copy per chunk and make the ordering explicit.
 */
typedef struct {
    int                    tfd;
    EVP_MD_CTX            *mdctx;
    rs_parity_stream_t    *ps;
    object_crypt_stream_t *cs;    /* NULL when the repository has no key */
    uint8_t               *cbuf;  /* STREAM_CHUNK ciphertext scratch, cs only */
    uint32_t               crc;
    uint64_t               total_written;
} sparse_emit_t;

/* Store n bytes: hash the plaintext, encrypt if the repository has a key,
 * then write and cover what was written with CRC and parity. */
static status_t sp_emit(sparse_emit_t *e, const void *data, size_t n)
{
    if (n == 0) return OK;
    if (EVP_DigestUpdate(e->mdctx, data, n) != 1)
        return set_error(ERR_IO, "sparse_stream: SHA256 update");

    const uint8_t *p = data;
    size_t left = n;
    while (left > 0) {
        size_t chunk = left;
        const uint8_t *out = p;
        if (e->cs) {
            if (chunk > STREAM_CHUNK) chunk = STREAM_CHUNK;
            status_t cst = object_crypt_stream_update(e->cs, p, chunk, e->cbuf);
            if (cst != OK) return cst;
            out = e->cbuf;
        }
        if (io_write_full(e->tfd, out, chunk) != 0)
            return set_error_errno(ERR_IO, "sparse_stream: write tfd");
        e->crc = crc32c_update(e->crc, out, chunk);
        if (rs_parity_stream_feed(e->ps, out, chunk) != 0)
            return set_error_errno(ERR_IO, "sparse_stream: rs feed");
        e->total_written += chunk;
        p += chunk;
        left -= chunk;
    }
    return OK;
}

/* Streaming writer for sparse objects. Writes [sparse_hdr_t][regions[]][data]
 * to a new object without ever materializing the full payload in RAM.
 * Takes ownership of fd (closes on return). */
static status_t write_sparse_object_stream(store_t *store, int fd,
                                           const char *src_path,
                                           uint64_t file_size,
                                           size_t parity_cap,
                                           uint64_t *out_payload_size,
                                           const sparse_region_t *regions,
                                           uint32_t n_regions,
                                           uint8_t out_hash[OBJECT_HASH_SIZE],
                                           int *out_is_new,
                                           uint64_t *out_phys_bytes) {
    if (store_needs_key(store)) {
        if (src_path) close(fd);
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to write a plaintext object");
    }

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
    /* Reported before any early return below can fail: the caller records this
     * against the object, and the object header carries the same number, so the
     * two must not be able to disagree. */
    if (out_payload_size) *out_payload_size = payload_sz;

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", store_root(store))
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
    snprintf(ps_tmp_dir, sizeof(ps_tmp_dir), "%s/tmp", store_root(store));
    rs_parity_stream_t ps;
    if (rs_parity_stream_init(&ps, parity_cap, ps_tmp_dir) != 0) {
        EVP_MD_CTX_free(mdctx);
        if (src_path) close(fd);
        close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "sparse_stream: rs init");
    }
    ps.spill_counter = store_write_counter(store, STORE_WR_PARITY_SPILL);

    status_t st = OK;
    sparse_emit_t em = { .tfd = tfd, .mdctx = mdctx, .ps = &ps };
    uint8_t tag[OBJECT_CRYPT_TAG_BYTES];

    /*
     * The nonce goes down first, before the sparse header, so a reader has it
     * before the first byte it has to decrypt. It is part of what is stored,
     * so CRC and parity cover it; it is not part of what is hashed, because
     * the hash names the payload and two stores of the same payload draw
     * different nonces.
     */
    const repo_key_t *enc_key = store_key(store);
    if (enc_key) {
        uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
        st = object_crypt_stream_begin(&em.cs, enc_key, nonce, 1);
        if (st == OK) {
            em.cbuf = malloc(STREAM_CHUNK);
            if (!em.cbuf) st = set_error(ERR_NOMEM, "sparse_stream: cipher buf");
        }
        if (st == OK) {
            if (io_write_full(tfd, nonce, sizeof(nonce)) != 0) {
                st = set_error_errno(ERR_IO, "sparse_stream: write nonce");
            } else {
                em.crc = crc32c_update(em.crc, nonce, sizeof(nonce));
                if (rs_parity_stream_feed(&ps, nonce, sizeof(nonce)) != 0)
                    st = set_error_errno(ERR_IO, "sparse_stream: rs feed (nonce)");
                em.total_written += sizeof(nonce);
            }
        }
        if (st != OK) goto sp_done;
    }

    /* Sparse header, then the regions table, then each region's data. */
    sparse_hdr_t shdr = { .magic = SPARSE_MAGIC, .region_count = n_regions };
    st = sp_emit(&em, &shdr, sizeof(shdr));
    if (st != OK) goto sp_done;

    st = sp_emit(&em, regions, (size_t)n_regions * sizeof(sparse_region_t));
    if (st != OK) goto sp_done;

    uint8_t *buf = malloc(STREAM_CHUNK);
    if (!buf) { st = set_error(ERR_NOMEM, "sparse_stream: buf alloc"); goto sp_done; }

    for (uint32_t r = 0; r < n_regions && st == OK; r++) {
        if (lseek(fd, (off_t)regions[r].offset, SEEK_SET) == (off_t)-1) {
            st = set_error_errno(ERR_IO, "sparse_stream: lseek region %u", r); break;
        }
        uint64_t remaining = regions[r].length;
        while (remaining > 0) {
            /* Same bound as the plain stream: see CANCEL_READ_MAX. */
            if (store_cancelled(store)) {
                st = set_error(ERR_BUSY, "cancelled while storing an object");
                break;
            }
            size_t want = (remaining > CANCEL_READ_MAX) ? CANCEL_READ_MAX : (size_t)remaining;
            ssize_t got = read(fd, buf, want);
            if (got <= 0) {
                if (got == -1 && errno == EINTR) continue;
                st = set_error_errno(ERR_IO, "sparse_stream: read region %u", r);
                break;
            }
            st = sp_emit(&em, buf, (size_t)got);
            if (st != OK) break;
            store_io_progress(store, (uint64_t)got);
            remaining -= (uint64_t)got;
        }
    }
    free(buf);

    /* The tag closes the payload and lands before the parity trailer, so
     * parity covers it like everything else that is stored. */
    if (st == OK && em.cs) {
        st = object_crypt_stream_finish(em.cs, tag, NULL);
        if (st == OK) {
            if (io_write_full(tfd, tag, sizeof(tag)) != 0) {
                st = set_error_errno(ERR_IO, "sparse_stream: write tag");
            } else {
                em.crc = crc32c_update(em.crc, tag, sizeof(tag));
                if (rs_parity_stream_feed(&ps, tag, sizeof(tag)) != 0)
                    st = set_error_errno(ERR_IO, "sparse_stream: rs feed (tag)");
                em.total_written += sizeof(tag);
            }
        }
    }

sp_done:
    object_crypt_stream_free(em.cs);
    free(em.cbuf);
    uint32_t crc = em.crc;
    uint64_t total_written = em.total_written;
    if (src_path) close(fd);

    if (st != OK) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return st;
    }

    uint64_t expect_written = payload_sz + (enc_key ? OBJECT_CRYPT_OVERHEAD : 0);
    if (total_written != expect_written) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return set_error(ERR_IO, "sparse_stream: payload size mismatch %llu != %llu",
                         (unsigned long long)total_written,
                         (unsigned long long)expect_written);
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
    if (object_exists(store, out_hash)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        store_count_write(store, STORE_WR_LOOSE_DISCARD,
                          (uint64_t)sizeof(object_header_t) + total_written);
        if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(store, out_hash);
        return OK;
    }

    /* compressed_size counts what is stored - nonce and tag included - so
     * every trailer offset and parity length stays a plain byte count. */
    hdr.compressed_size = total_written;
    hdr.encryption      = enc_key ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
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
    warn_if_parity_unusable(hdr.compressed_size, "sparse_stream");
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
    int objfd = store_objects_fd(store);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "sparse_stream: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        unlink(tmppath); return set_error_errno(ERR_IO, "sparse_stream: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    if (subfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "sparse_stream: openat(%s)", subdir); }
    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s",
                 store_root(store), subdir, fname) >= (int)sizeof(dstpath)) {
        close(subfd); unlink(tmppath); return set_error(ERR_IO, "sparse_stream: dstpath too long");
    }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            close(subfd); unlink(tmppath);
            store_count_write(store, STORE_WR_LOOSE_DISCARD,
                              (uint64_t)sizeof(object_header_t) + total_written
                              + pftr.trailer_size);
            if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(store, out_hash);
            return OK;
        }
        close(subfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "sparse_stream: rename(%s, %s)", tmppath, dstpath);
    }
    close(subfd);
    store_count_write(store, STORE_WR_LOOSE, (uint64_t)sizeof(object_header_t) + total_written);
    store_count_write(store, STORE_WR_LOOSE_PARITY, pftr.trailer_size);
    store_loose_set_insert(store, out_hash);
    if (out_is_new) *out_is_new = 1;
    /* Trailer included, same reasoning as the streaming path above. */
    if (out_phys_bytes)
        *out_phys_bytes = (uint64_t)sizeof(object_header_t) + total_written
                        + pftr.trailer_size;
    return OK;
}

status_t object_store_file_ex(store_t *store, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes) {
    return object_store_file_ex_cb(store, fd, NULL, file_size, out_hash,
                                   out_is_new, out_phys_bytes, NULL, NULL, 0,
                                   NULL, NULL);
}

status_t object_store_packed(store_t *store, uint8_t type,
                             const void *data, size_t len,
                             pack_backup_writer_t *pack_w,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             object_write_t *out_info) {
    object_write_t scratch;
    object_write_t *w = out_info ? out_info : &scratch;
    memset(w, 0, sizeof(*w));
    w->payload_size = len;

    if (pack_w && (uint64_t)len <= PACK_STREAM_THRESHOLD)
        return pack_backup_add(pack_w, type, data, len, out_hash,
                               &w->is_new, &w->phys_bytes, &w->payload_size);

    return object_store_ex(store, type, data, len, out_hash,
                           &w->is_new, &w->phys_bytes);
}

status_t object_store_file_packed(store_t *store, int fd, uint64_t file_size,
                                  pack_backup_writer_t *pack_w,
                                  uint8_t out_hash[OBJECT_HASH_SIZE],
                                  object_write_t *out_info) {
    object_write_t scratch;
    object_write_t *w = out_info ? out_info : &scratch;
    memset(w, 0, sizeof(*w));
    return object_store_file_ex_cb(store, fd, NULL, file_size, out_hash,
                                   &w->is_new, &w->phys_bytes, &w->payload_size,
                                   pack_w, 0, NULL, NULL);
}

status_t object_store_file_loose(store_t *store, int fd, uint64_t file_size,
                                 size_t parity_cap,
                                 uint8_t out_hash[OBJECT_HASH_SIZE],
                                 object_write_t *out_info) {
    object_write_t scratch;
    object_write_t *w = out_info ? out_info : &scratch;
    memset(w, 0, sizeof(*w));
    return object_store_file_ex_cb(store, fd, NULL, file_size, out_hash,
                                   &w->is_new, &w->phys_bytes, &w->payload_size,
                                   NULL, parity_cap, NULL, NULL);
}

status_t object_store_file_info(store_t *store, int fd, uint64_t file_size,
                                uint8_t out_hash[OBJECT_HASH_SIZE],
                                object_write_t *out_info) {
    object_write_t scratch;
    object_write_t *w = out_info ? out_info : &scratch;
    memset(w, 0, sizeof(*w));
    return object_store_file_ex_cb(store, fd, NULL, file_size, out_hash,
                                   &w->is_new, &w->phys_bytes, &w->payload_size,
                                   NULL, 0, NULL, NULL);
}

static status_t object_store_file_ex_cb(store_t *store, int fd,
                                        const char *src_path,
                                        uint64_t file_size,
                                        uint8_t out_hash[OBJECT_HASH_SIZE],
                                        int *out_is_new, uint64_t *out_phys_bytes,
                                        uint64_t *out_payload_size,
                                        pack_backup_writer_t *pack_w,
                                        size_t parity_cap,
                                        xfer_progress_fn progress_cb,
                                        void *progress_ctx) {
    if (parity_cap == 0) parity_cap = STREAM_PARITY_CAP_DEFAULT;
    if (file_size == 0) {
        if (src_path) close(fd);
        uint8_t empty = 0;
        if (out_payload_size) *out_payload_size = 0;
        return write_object(store, OBJECT_TYPE_FILE, &empty, 0, out_hash,
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
        /*
         * No data at or after offset 0. That is what an all-hole file
         * (`truncate -s 100M f`) reports, and storing it whole turned 0
         * allocated blocks into 100M of zeroes on restore.
         *
         * It is also what some FUSE layers return when they do not
         * implement SEEK_DATA, and treating a file full of real data as one
         * big hole would lose it. The block count tells the two apart: a
         * file with no allocated blocks has nothing to read but zeroes,
         * whatever lseek says. Anything else keeps the old, safe behaviour
         * of storing the file whole.
         */
        struct stat fst;
        if (fstat(fd, &fst) == 0 && fst.st_blocks == 0) {
            is_sparse = 1;      /* zero regions: the whole file is a hole */
        } else {
            char probe;
            if (pread(fd, &probe, 1, 0) != 1) {
                /* Can't even read the file: a real I/O error */
                if (src_path) close(fd);
                return set_error_errno(ERR_IO, "store: read probe failed");
            }
            is_sparse = 0;
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
     * write_object_file_stream takes ownership of fd (closes it on return).
     * A sparse file with no regions is all hole, and is stored as a header
     * and an empty region table rather than as that many zeroes. */
    if (!is_sparse) {
        free(regions);
        /* The plain stream writes uncompressed_size = file_size, so for a file
         * with no holes the payload and the logical size are the same number. */
        if (out_payload_size) *out_payload_size = file_size;

        /*
         * Straight into the open pack, when there is one and the file is small
         * enough to hash before its entry header is written.
         *
         * The decision sits here rather than in the caller because it depends
         * on whether the file has holes, and that is what the code above has
         * just worked out. A sparse object hashes over its region table rather
         * than its contents, so routing one down this path would give it a
         * different hash from the same file stored loose and quietly split the
         * repository's deduplication in two.
         */
        if (pack_w && file_size <= PACK_STREAM_THRESHOLD) {
            uint8_t *buf = malloc((size_t)file_size);
            if (!buf) {
                if (src_path) close(fd);
                return set_error(ERR_NOMEM, "store: pack staging buffer");
            }
            /*
             * Read a chunk at a time, reporting each, rather than in one call.
             * Progress is also where a backup notices a writer queued for the
             * database lock (OWN-16), and up to 16 MiB in one read is long
             * enough on a slow source to hold that writer for seconds.
             */
            int rerr = lseek(fd, 0, SEEK_SET) == (off_t)-1;
            for (uint64_t off = 0; !rerr && off < file_size; ) {
                size_t n = (file_size - off) < STREAM_PIPE_CHUNK
                         ? (size_t)(file_size - off) : STREAM_PIPE_CHUNK;
                if (io_read_full(fd, buf + off, n) != 0) { rerr = 1; break; }
                off += n;
                store_io_progress(store, (uint64_t)n);
            }
            if (rerr) {
                free(buf);
                if (src_path) close(fd);
                return set_error_errno(ERR_IO, "store: read for pack staging");
            }
            if (src_path) close(fd);
            status_t pst = pack_backup_add(pack_w, OBJECT_TYPE_FILE, buf,
                                           (size_t)file_size, out_hash,
                                           out_is_new, out_phys_bytes,
                                           out_payload_size);
            free(buf);
            return pst;
        }

        return write_object_file_stream(store, fd, src_path, file_size,
                                        parity_cap, out_hash,
                                        out_is_new, out_phys_bytes,
                                        progress_cb, progress_ctx);
    }

    /* Stream sparse object — bounded RAM regardless of file size. */
    status_t st = write_sparse_object_stream(store, fd, src_path, file_size,
                                             parity_cap, out_payload_size,
                                             regions, n_regions,
                                             out_hash, out_is_new, out_phys_bytes);
    free(regions);
    return st;
}

status_t object_store_file_cb(store_t *store, int fd, const char *src_path,
                              uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes,
                              xfer_progress_fn cb, void *cb_ctx) {
    if (file_size == 0) {
        if (src_path) close(fd);  /* we own fd when src_path is set */
        uint8_t empty = 0;
        return write_object(store, OBJECT_TYPE_FILE, &empty, 0, out_hash,
                            out_is_new, out_phys_bytes);
    }
    /* Sparse-aware path handles both sparse and non-sparse files.
     * For non-sparse files it falls through to write_object_file_stream.
     * The progress callback is only used on the streaming (non-sparse) path. */
    return object_store_file_ex_cb(store, fd, src_path, file_size, out_hash,
                                   out_is_new, out_phys_bytes, NULL, NULL, 0,
                                   cb, cb_ctx);
}

/* ------------------------------------------------------------------ */
/* Load                                                                */
/* ------------------------------------------------------------------ */

status_t object_load(store_t *store,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not a loose object — try pack files */
        return pack_object_load(store, hash, out_data, out_size, out_type);
    }

    object_header_t hdr;
    if (read_object_header(fd, &hdr) != OK) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        close(fd);
        return set_error(ERR_CORRUPT, "object %s: %s", hex, why);
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

    /* ---- Parity check and repair ---- */
    if (hdr.version >= 2) {
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
                    log_warn("object: repaired corrupt header via parity");
                    parity_stats_add_repaired(1);
                } else if (hrc < 0) {
                    log_warn("object: header parity check failed (uncorrectable)");
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
                                               trailer + sizeof(hdr_par), rs_sz);
                    if (rrc > 0) {
                        log_warn("object: RS repaired corrupt payload bytes");
                        parity_stats_add_repaired(1);
                    } else if (rrc < 0) {
                        log_warn("object: RS repair failed (uncorrectable)");
                        parity_stats_add_uncorrectable(1);
                    }
                }
            }
        }
        free(trailer);
    }
    close(fd);

    /*
     * Decrypt before anything looks at the payload. compressed_size counts
     * the tag, so the plaintext is that much shorter; decrypting in place
     * keeps the buffer the caller already owns.
     */
    uint8_t codec = hdr.compression;
    if (hdr.encryption != ENCRYPT_NONE) {
        const repo_key_t *key = store_key(store);
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        if (!key) {
            free(cpayload);
            return set_error(ERR_INVALID,
                             "object %s is encrypted and no key is loaded", hex);
        }
        if (hdr.compressed_size < OBJECT_CRYPT_OVERHEAD) {
            free(cpayload);
            return set_error(ERR_CORRUPT, "object %s: truncated ciphertext", hex);
        }
        size_t ptlen = (size_t)hdr.compressed_size - OBJECT_CRYPT_OVERHEAD;
        status_t dst = object_decrypt(key, cpayload, ptlen, cpayload);
        if (dst != OK) { free(cpayload); return dst; }
        hdr.compressed_size = ptlen;
    }

    void *data;
    size_t data_sz;
    if (codec == COMPRESS_NONE) {
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


/* ------------------------------------------------------------------ */
/* Streaming payload repair                                            */
/*                                                                     */
/* An object larger than STREAM_LOAD_MAX cannot be loaded whole, so it */
/* is streamed - and until now the streaming path checked the SHA-256  */
/* and nothing else. The Reed-Solomon parity sitting in the same file  */
/* went unused, so a single flipped byte made a large object           */
/* unreadable even though the bytes to rebuild it were right there.    */
/* Large incompressible objects stay loose permanently by design, so   */
/* the exposure was exactly the video, audio and image files that      */
/* dominate a real backup.                                             */
/*                                                                     */
/* Detection is free: the hash is already computed, and a clean object */
/* costs nothing extra. Only a mismatch triggers a second, corrective  */
/* pass, group by group, in bounded memory.                            */
/* ------------------------------------------------------------------ */

/* One RS interleave group: RS_K * RS_INTERLEAVE data bytes protected by
 * RS_2T * RS_INTERLEAVE parity bytes. Groups are independent, which is what
 * makes correction streamable at all. */
#define RS_STREAM_GROUP_DATA ((size_t)RS_K * RS_INTERLEAVE)
#define RS_STREAM_GROUP_PAR  ((size_t)RS_2T * RS_INTERLEAVE)

/*
 * Can the destination be rewound so a corrective pass may replace what the
 * first pass already wrote?
 *
 * A regular file can be truncated back to where this object started. A
 * character device - /dev/null, which is what verify streams into - keeps
 * nothing, so there is nothing to undo. A pipe cannot unsend: a damaged
 * object read into one fails, and an explicit repair fixes it on disk.
 */
static int out_rewind_point(int out_fd, off_t *pos)
{
    struct stat sb;
    if (fstat(out_fd, &sb) != 0) return 0;
    if (S_ISCHR(sb.st_mode)) { *pos = -1; return 1; }
    if (!S_ISREG(sb.st_mode)) return 0;
    off_t p = lseek(out_fd, 0, SEEK_CUR);
    if (p < 0) return 0;
    *pos = p;
    return 1;
}

static int out_rewind(int out_fd, off_t pos)
{
    if (pos < 0) return 0;   /* a device: nothing persisted */
    if (lseek(out_fd, pos, SEEK_SET) == (off_t)-1) return -1;
    return ftruncate(out_fd, pos);
}

/*
 * Locate the RS parity block in a loose object's trailer without reading it.
 *
 * The block is proportional to the payload - about 1 KiB per 15 KiB of data,
 * so 144 MiB for a 2 GiB object and several gigabytes for the large ones this
 * path exists to serve. Loading it whole to repair a stream would reintroduce
 * the unbounded allocation one level down. Groups are read a window at a
 * time from this offset instead.
 *
 * Returns 0 with the offset and size of the parity block when the footer
 * agrees with the layout, -1 when there is nothing usable.
 */
static int locate_loose_rs_parity(int fd, const object_header_t *hdr,
                                  off_t *out_off, size_t *out_sz)
{
    *out_off = 0;
    *out_sz = 0;
    if (hdr->version < 2) return -1;

    size_t rs_sz = rs_parity_size((size_t)hdr->compressed_size);
    if (rs_sz == 0) return -1;

    off_t trailer_off = (off_t)sizeof(*hdr) + (off_t)hdr->compressed_size;
    off_t ftr_off = trailer_off + (off_t)sizeof(parity_record_t) + (off_t)rs_sz
                  + (off_t)(sizeof(uint32_t) * 2);
    parity_footer_t pftr;
    if (pread(fd, &pftr, sizeof(pftr), ftr_off) != (ssize_t)sizeof(pftr) ||
        pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION)
        return -1;

    *out_off = trailer_off + (off_t)sizeof(parity_record_t);
    *out_sz  = rs_sz;
    return 0;
}

/*
 * OWN-30: a loose trailer's trailer_size is a uint32, and parity is 1,024
 * bytes per 15,296 stored, so the field wraps at about 59.75 GiB of stored
 * data. The streamed read locates parity from the header rather than from
 * that field, but repair is held to the same line as every other repair
 * path: past it nothing is corrected, so one rule covers every reader.
 *
 * TODO(OWN-30): switch to the shared parity_fields_fit helper from
 * fix/own30-repair-guard when it lands; this is the equivalent check.
 */
#define LOOSE_PARITY_FIT_DEFAULT \
    ((uint64_t)((UINT64_C(1) << 32) / RS_STREAM_GROUP_PAR) * (uint64_t)RS_STREAM_GROUP_DATA)
static _Atomic uint64_t loose_parity_fit_limit = LOOSE_PARITY_FIT_DEFAULT;

void object_test_set_parity_fit_limit(uint64_t bytes)
{
    atomic_store_explicit(&loose_parity_fit_limit,
                          bytes ? bytes : LOOSE_PARITY_FIT_DEFAULT,
                          memory_order_relaxed);
}

static int loose_parity_fits(uint64_t stored_size)
{
    return stored_size < atomic_load_explicit(&loose_parity_fit_limit,
                                              memory_order_relaxed);
}

/* RS groups a streamed read holds at once: 1,096 groups is 16,764,416 bytes
 * of payload, just under STREAM_LOAD_MAX, and 1.1 MiB of parity when
 * repairing. Windows are whole groups so that each one can be corrected
 * on its own. */
#define LOOSE_WIN_GROUPS 1096u

static int pread_full(int fd, void *buf, size_t n, off_t off)
{
    uint8_t *p = buf;
    while (n > 0) {
        ssize_t r = pread(fd, p, n, off);
        if (r == 0) return -1;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        p += r; n -= (size_t)r; off += r;
    }
    return 0;
}

/*
 * The stored payload of an encrypted object is nonce, ciphertext, tag. This
 * splits a window of stored bytes into those three, decrypting the
 * ciphertext in place, so that a window can be corrected from parity first:
 * parity covers the stored bytes, nonce and tag included.
 */
typedef struct {
    int                    encrypted;
    const repo_key_t      *key;
    object_crypt_stream_t *cs;
    uint8_t                nonce[OBJECT_CRYPT_NONCE_BYTES];
    size_t                 nonce_have;
    uint8_t                tag[OBJECT_CRYPT_TAG_BYTES];
    size_t                 tag_have;
    uint64_t               pos, total;
} loose_sink_t;

static status_t loose_sink_push(loose_sink_t *sk, uint8_t *buf, size_t n,
                                uint8_t **out, size_t *out_n)
{
    if (!sk->encrypted) {
        sk->pos += n;
        *out = buf; *out_n = n;
        return OK;
    }

    const uint64_t nb = OBJECT_CRYPT_NONCE_BYTES;
    const uint64_t tb = OBJECT_CRYPT_TAG_BYTES;
    uint64_t start = sk->pos, end = start + n;
    uint64_t ct_end = sk->total - tb;

    uint64_t ns = start < nb ? start : nb;
    uint64_t ne = end   < nb ? end   : nb;
    if (ne > ns) {
        memcpy(sk->nonce + sk->nonce_have, buf + (ns - start), (size_t)(ne - ns));
        sk->nonce_have += (size_t)(ne - ns);
        if (sk->nonce_have == nb) {
            status_t st = object_crypt_stream_begin(&sk->cs, sk->key, sk->nonce, 0);
            if (st != OK) return st;
        }
    }

    uint8_t *pp = buf;
    size_t   pn = 0;
    uint64_t cs0 = start > nb     ? start : nb;
    uint64_t ce0 = end   < ct_end ? end   : ct_end;
    if (ce0 > cs0) {
        if (!sk->cs) return set_error(ERR_CORRUPT, "load_stream: ciphertext before nonce");
        pp = buf + (cs0 - start);
        pn = (size_t)(ce0 - cs0);
        status_t st = object_crypt_stream_update(sk->cs, pp, pn, pp);
        if (st != OK) return st;
    }

    uint64_t ts = start > ct_end ? start : ct_end;
    if (end > ts) {
        size_t take = (size_t)(end - ts);
        if (sk->tag_have + take > sizeof(sk->tag))
            return set_error(ERR_CORRUPT, "load_stream: oversized tag");
        memcpy(sk->tag + sk->tag_have, buf + (ts - start), take);
        sk->tag_have += take;
    }

    sk->pos = end;
    *out = pp; *out_n = pn;
    return OK;
}

static status_t loose_sink_finish(loose_sink_t *sk)
{
    if (!sk->encrypted) return OK;
    if (!sk->cs || sk->tag_have != OBJECT_CRYPT_TAG_BYTES)
        return set_error(ERR_CORRUPT, "load_stream: truncated ciphertext");
    return object_crypt_stream_finish(sk->cs, NULL, sk->tag);
}

/*
 * One pass over a loose object's stored payload, a window at a time:
 * correct the window from parity (repair), decrypt, hash and emit. Ends by
 * checking the tag and then the hash, so ERR_CORRUPT from here means the
 * bytes emitted are not the object, whichever check said so.
 *
 * *out_fixed counts bytes parity corrected; a group parity could not
 * correct ends the pass with ERR_CORRUPT.
 */
static status_t loose_stream_pass(store_t *store, int fd, const object_header_t *hdr,
                                  const uint8_t hash[OBJECT_HASH_SIZE], int out_fd,
                                  int repair, off_t par_off, int *out_fixed)
{
    const size_t win = (size_t)LOOSE_WIN_GROUPS * RS_STREAM_GROUP_DATA;
    *out_fixed = 0;

    uint8_t    *buf = malloc(win);
    uint8_t    *par = repair ? malloc((size_t)LOOSE_WIN_GROUPS * RS_STREAM_GROUP_PAR) : NULL;
    EVP_MD_CTX *md  = EVP_MD_CTX_new();
    loose_sink_t sk;
    memset(&sk, 0, sizeof(sk));
    sk.encrypted = hdr->encryption != ENCRYPT_NONE;
    sk.key       = store_key(store);
    sk.total     = hdr->compressed_size;

    status_t st = OK;
    if (!buf || (repair && !par) || !md || EVP_DigestInit_ex(md, EVP_sha256(), NULL) != 1) {
        st = set_error(ERR_NOMEM, "load_stream: window alloc failed");
        goto out;
    }

    uint64_t done = 0;
    off_t    pay_off = (off_t)sizeof(*hdr);
    if (repair) rs_init();
    while (done < hdr->compressed_size && st == OK) {
        /* A restore of one large file is cancellable inside it. */
        if (store_cancelled(store)) {
            st = set_error(ERR_BUSY, "cancelled while reading an object"); break;
        }
        uint64_t left = hdr->compressed_size - done;
        size_t want = left > win ? win : (size_t)left;
        if (pread_full(fd, buf, want, pay_off + (off_t)done) != 0) {
            st = set_error(ERR_CORRUPT, "load_stream: truncated payload"); break;
        }
        if (repair) {
            size_t plen = rs_parity_size(want);
            off_t  poff = par_off + (off_t)((done / RS_STREAM_GROUP_DATA) * RS_STREAM_GROUP_PAR);
            if (pread_full(fd, par, plen, poff) != 0) {
                st = set_error(ERR_CORRUPT, "load_stream: truncated parity"); break;
            }
            int rc = rs_parity_decode(buf, want, par, plen);
            if (rc < 0) {
                st = set_error(ERR_CORRUPT, "load_stream: damage beyond what parity can correct");
                break;
            }
            *out_fixed += rc;
        }
        done += want;

        uint8_t *pp = NULL;
        size_t   pn = 0;
        st = loose_sink_push(&sk, buf, want, &pp, &pn);
        if (st != OK) break;
        if (pn == 0) continue;
        if (EVP_DigestUpdate(md, pp, pn) != 1) {
            st = set_error(ERR_IO, "load_stream: SHA256 update failed"); break;
        }
        if (io_write_full(out_fd, pp, pn) != 0) {
            st = set_error_errno(ERR_IO, "load_stream: write to out_fd"); break;
        }
        store_io_progress(store, (uint64_t)pn);
    }

    if (st == OK) st = loose_sink_finish(&sk);
    if (st == OK) {
        uint8_t got[OBJECT_HASH_SIZE];
        unsigned dlen = 0;
        if (EVP_DigestFinal_ex(md, got, &dlen) != 1 || dlen != OBJECT_HASH_SIZE ||
            memcmp(got, hash, OBJECT_HASH_SIZE) != 0)
            st = set_error(ERR_CORRUPT, "load_stream: hash mismatch");
    }

out:
    object_crypt_stream_free(sk.cs);
    EVP_MD_CTX_free(md);
    free(buf);
    free(par);
    return st;
}

/*
 * Stream with parity applied, paying for parity only when the object is
 * damaged. The first pass trusts the stored bytes; a failure that says they
 * are wrong - the tag, the hash, a truncation - rewinds the destination and
 * runs a correcting pass, which decrypts and authenticates again.
 *
 * A failed tag is treated exactly like a hash mismatch. It used to end the
 * read before repair was considered, so an encrypted loose object with one
 * flipped byte was unreadable although its parity could rebuild it (OWN-37):
 * the ciphertext is what parity covers, so the correction applies to it
 * unchanged.
 */
static status_t loose_stream(store_t *store, int fd, const object_header_t *hdr,
                             const uint8_t hash[OBJECT_HASH_SIZE], int out_fd)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);

    off_t start = -1;
    int   rewindable = out_rewind_point(out_fd, &start);
    int   fixed = 0;

    status_t st = loose_stream_pass(store, fd, hdr, hash, out_fd, 0, 0, &fixed);
    if (st != ERR_CORRUPT) return st;

    char first[ERR_MSG_MAX];
    snprintf(first, sizeof(first), "%s", err_msg());

    off_t  par_off = 0;
    size_t rs_sz   = 0;
    if (locate_loose_rs_parity(fd, hdr, &par_off, &rs_sz) != 0) {
        log_warn("object %s: damaged and has no usable parity", hex);
        parity_stats_add_uncorrectable(1);
        return set_error(ERR_CORRUPT, "object %s: %s (no usable parity)", hex, first);
    }
    if (!loose_parity_fits(hdr->compressed_size)) {
        log_warn("object %s: %llu stored bytes is past where its 32-bit parity "
                 "sizes can be trusted; parity not applied (OWN-30)", hex,
                 (unsigned long long)hdr->compressed_size);
        parity_stats_add_uncorrectable(1);
        return set_error(ERR_CORRUPT, "object %s: %s (parity not applied)", hex, first);
    }
    if (!rewindable) {
        log_warn("object %s: damaged, and the destination cannot be rewound "
                 "to rewrite it; run a repairing verify", hex);
        return set_error(ERR_CORRUPT, "object %s: %s (stream)", hex, first);
    }
    if (out_rewind(out_fd, start) != 0)
        return set_error_errno(ERR_IO, "object %s: cannot rewind the destination "
                               "to rewrite a damaged object", hex);

    err_clear();
    st = loose_stream_pass(store, fd, hdr, hash, out_fd, 1, par_off, &fixed);
    if (st == OK) {
        log_warn("object %s: %d byte(s) corrected from parity while streaming",
                 hex, fixed);
        parity_stats_add_repaired(1);
        return OK;
    }
    if (st != ERR_CORRUPT) return st;
    parity_stats_add_uncorrectable(1);
    char second[ERR_MSG_MAX];
    snprintf(second, sizeof(second), "%s", err_msg());
    return set_error(ERR_CORRUPT, "object %s: %s; after parity: %s", hex, first, second);
}

status_t object_load_stream(store_t *store,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             int out_fd,
                             uint64_t *out_size,
                             uint8_t *out_type)
{
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not a loose object — try pack files (streaming to handle large objects). */
        uint64_t psz = 0; uint8_t ptype = 0;
        status_t st = pack_object_load_stream(store, hash, out_fd, &psz, &ptype);
        if (st != OK) return st;
        if (out_size) *out_size = psz;
        if (out_type) *out_type = ptype;
        return OK;
    }

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);

    object_header_t hdr;
    if (read_object_header(fd, &hdr) != OK) {
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        close(fd);
        return set_error(ERR_CORRUPT, "object %s: %s", hex, why);
    }

    /* For version 2 objects, attempt header repair via parity.
     * Use pread at known trailer offset (header + payload) to avoid seeking. */
    if (hdr.version >= 2) {
        off_t trailer_off = (off_t)sizeof(hdr) + (off_t)hdr.compressed_size;
        parity_record_t hpar;
        parity_footer_t pftr;
        if (pread(fd, &hpar, sizeof(hpar), trailer_off) == (ssize_t)sizeof(hpar)) {
            size_t rs_sz = rs_parity_size((size_t)hdr.compressed_size);
            off_t ftr_off = trailer_off + (off_t)sizeof(hpar) + (off_t)rs_sz
                          + (off_t)(sizeof(uint32_t) * 2);
            if (pread(fd, &pftr, sizeof(pftr), ftr_off) == (ssize_t)sizeof(pftr) &&
                pftr.magic == PARITY_FOOTER_MAGIC && pftr.version == PARITY_VERSION) {
                int hrc = parity_record_check(&hdr, sizeof(hdr), &hpar);
                if (hrc == 1) {
                    log_warn("object: repaired corrupt header via parity (stream)");
                    parity_stats_add_repaired(1);
                } else if (hrc < 0) {
                    log_warn("object: header parity uncorrectable (stream)");
                    parity_stats_add_uncorrectable(1);
                }
            }
        }
    }

    if (hdr.compression != COMPRESS_NONE) {
        /* Loose objects are always COMPRESS_NONE — compression happens at
         * pack time.  Any other value indicates corruption. */
        close(fd);
        return set_error(ERR_CORRUPT, "object %s: unexpected compression %u in loose object",
                         hex, hdr.compression);
    }
    if (hdr.encryption != ENCRYPT_NONE) {
        if (!store_key(store)) {
            close(fd);
            return set_error(ERR_INVALID, "object %s is encrypted and no key is loaded", hex);
        }
        if (hdr.compressed_size < OBJECT_CRYPT_OVERHEAD) {
            close(fd);
            return set_error(ERR_CORRUPT, "object %s: truncated ciphertext", hex);
        }
    }

    if (out_type) *out_type = hdr.type;
    if (out_size) *out_size = hdr.uncompressed_size;

    /*
     * Encrypted payloads are stored as nonce, ciphertext, tag, and the tag
     * is only reached at the end - so a streamed read cannot know the
     * payload is authentic until it has emitted all of it. The same was
     * always true of the hash. A caller that must not act on unverified
     * bytes streams into something it can discard: restore writes a
     * temporary file and renames it into place only on OK, and grep holds
     * a file's matches until the read returns.
     */
    status_t st = loose_stream(store, fd, &hdr, hash, out_fd);
    close(fd);
    return st;
}

status_t object_load_prefix(store_t *store,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             size_t max_bytes,
                             void **out_data, size_t *out_size,
                             uint64_t *out_full_size,
                             uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not loose — try pack files */
        return pack_object_load_prefix(store, hash, max_bytes,
                                        out_data, out_size, out_full_size, out_type);
    }

    object_header_t hdr;
    if (read_object_header(fd, &hdr) != OK) {
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        close(fd); return set_error(ERR_CORRUPT, "object_load_prefix: %s", why);
    }

    if (out_full_size) *out_full_size = hdr.uncompressed_size;
    if (out_type) *out_type = hdr.type;

    /*
     * A loose payload is never compressed - packing is the only thing that
     * compresses - so the prefix is a plain read from the front. It can be
     * encrypted, though, and for a while this function did not know that:
     * it read the bytes off disk and handed the caller ciphertext, with an
     * OK status and the right length. Nothing downstream would have
     * noticed, because a prefix is precisely the thing that skips the
     * SHA-256 check.
     *
     * Assert the codec rather than assume it. If loose objects ever start
     * being compressed, this returns compressed bytes just as silently.
     */
    if (hdr.compression != COMPRESS_NONE) {
        close(fd);
        return set_error(ERR_CORRUPT,
                         "object_load_prefix: loose object has codec %u, expected none",
                         hdr.compression);
    }

    size_t want = max_bytes;
    if (want > hdr.uncompressed_size) want = (size_t)hdr.uncompressed_size;

    /*
     * ChaCha20 is a stream cipher, so a prefix decrypts on its own without
     * the rest of the ciphertext. The Poly1305 tag sits at the far end and
     * is neither read nor checked, which is the same bargain this function
     * already makes with the hash it never gets to verify.
     */
    object_crypt_stream_t *cs = NULL;
    if (hdr.encryption != ENCRYPT_NONE) {
        const repo_key_t *key = store_key(store);
        if (!key) {
            close(fd);
            char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
            return set_error(ERR_INVALID,
                             "object %s is encrypted and no key is loaded", hex);
        }
        if (hdr.compressed_size < OBJECT_CRYPT_OVERHEAD) {
            close(fd);
            return set_error(ERR_CORRUPT, "object_load_prefix: truncated ciphertext");
        }
        uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
        if (io_read_full(fd, nonce, sizeof(nonce)) != 0) {
            close(fd);
            return set_error(ERR_CORRUPT, "object_load_prefix: truncated nonce");
        }
        status_t cst = object_crypt_stream_begin(&cs, key, nonce, 0);
        if (cst != OK) { close(fd); return cst; }

        uint64_t avail = hdr.compressed_size - OBJECT_CRYPT_OVERHEAD;
        if (want > avail) want = (size_t)avail;
    }

    char *buf = malloc(want ? want : 1);
    if (!buf) {
        object_crypt_stream_free(cs); close(fd);
        return set_error(ERR_NOMEM, "object_load_prefix: alloc");
    }
    if (want && io_read_full(fd, buf, want) != 0) {
        free(buf); object_crypt_stream_free(cs); close(fd);
        return set_error(ERR_CORRUPT, "object_load_prefix: short read");
    }
    if (cs && want && object_crypt_stream_update(cs, buf, want, buf) != OK) {
        free(buf); object_crypt_stream_free(cs); close(fd);
        return set_error(ERR_CORRUPT, "object_load_prefix: decryption failed");
    }
    object_crypt_stream_free(cs);
    close(fd);

    *out_data = buf;
    *out_size = want;
    return OK;
}

/*
 * The object exactly as it sits on disk, loose or packed.
 *
 * No parity check, no decrypt, no decompress, no hash comparison - see the
 * note in object.h. The parity trailer is skipped rather than verified
 * because it covers the stored bytes, and the target computes its own over
 * whatever it stores.
 */
status_t object_read_stored(store_t *store,
                            const uint8_t hash[OBJECT_HASH_SIZE],
                            object_stored_info_t *out_info,
                            void **out_payload, size_t *out_len) {
    if (!store || !out_info || !out_payload || !out_len)
        return set_error(ERR_INVALID, "object_read_stored: null argument");

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1)
        return pack_object_read_stored(store, hash, &out_info->type,
                                       &out_info->compression,
                                       &out_info->encryption,
                                       &out_info->uncompressed_size,
                                       out_payload, out_len);

    object_header_t hdr;
    status_t hst = read_object_header(fd, &hdr);
    if (hst != OK) { close(fd); return hst; }

    /* read_object_header has already refused a payload longer than the file,
     * so this cannot be asked for more than the object occupies. */
    void *buf = malloc(hdr.compressed_size ? (size_t)hdr.compressed_size : 1);
    if (!buf) {
        close(fd);
        return set_error(ERR_NOMEM, "object_read_stored: alloc %llu",
                         (unsigned long long)hdr.compressed_size);
    }
    if (hdr.compressed_size && io_read_full(fd, buf, (size_t)hdr.compressed_size) != 0) {
        free(buf);
        close(fd);
        return set_error(ERR_CORRUPT, "object_read_stored: short read");
    }
    close(fd);

    out_info->type              = hdr.type;
    out_info->compression       = hdr.compression;
    out_info->encryption        = hdr.encryption;
    out_info->uncompressed_size = hdr.uncompressed_size;
    *out_payload = buf;
    *out_len     = (size_t)hdr.compressed_size;
    return OK;
}

/*
 * A stored-form reader, loose or packed, that never passes on damage parity
 * can correct.
 *
 * Replication streams what this hands over straight into the target, which
 * computes fresh parity over it; a damaged byte shipped as it stood would
 * be protected there as if it were right. Bytes already handed over cannot
 * be recalled, so open CRC-scans the payload first and, only on a mismatch
 * with the trailer, serves every read from a group corrected from parity.
 * The packed half lives in pack.c (pack_object_open_stored); the loose half
 * is here, one 15 KiB group at a time so a loose transfer stays inside its
 * chunk.
 */
struct object_stored_reader {
    FILE    *f;          /* loose, plain: positioned at the next payload byte */
    pack_stored_reader_t *p;   /* packed */
    uint64_t remaining;

    /* Loose, repair mode: groups read by pread and corrected. */
    int      fd;
    int      repair;
    uint64_t len;
    off_t    par_off;
    size_t   rs_sz;
    uint8_t *gbuf;
    uint8_t *gpar;
    uint64_t g_off;
    size_t   g_len;      /* 0: no group loaded */
    int64_t  fixed;
    uint64_t bad;
    uint8_t  hash[OBJECT_HASH_SIZE];
};

/* CRC the loose payload; on a mismatch with the trailer switch r to repair
 * mode. Returns OK whether or not there was damage. */
static status_t loose_stored_scan(object_stored_reader_t *r, const object_header_t *hdr)
{
    off_t  par_off = 0;
    size_t rs_sz   = 0;
    if (r->len == 0 || locate_loose_rs_parity(r->fd, hdr, &par_off, &rs_sz) != 0)
        return OK;   /* no parity to apply: read as it is */

    uint32_t stored_crc = 0;
    if (pread(r->fd, &stored_crc, sizeof(stored_crc), par_off + (off_t)rs_sz)
        != (ssize_t)sizeof(stored_crc))
        return OK;

    r->gbuf = malloc(RS_STREAM_GROUP_DATA);
    if (!r->gbuf) return set_error(ERR_NOMEM, "object_stored_open: group buffer");
    uint32_t crc = 0;
    for (uint64_t o = 0; o < r->len; ) {
        size_t want = r->len - o < RS_STREAM_GROUP_DATA ? (size_t)(r->len - o)
                                                         : RS_STREAM_GROUP_DATA;
        if (pread(r->fd, r->gbuf, want, (off_t)sizeof(*hdr) + (off_t)o) != (ssize_t)want)
            return set_error(ERR_CORRUPT, "object_stored_open: short read");
        crc = crc32c_update(crc, r->gbuf, want);
        o += want;
    }
    if (crc == stored_crc) {
        free(r->gbuf);
        r->gbuf = NULL;
        return OK;
    }
    r->gpar = malloc(RS_STREAM_GROUP_PAR);
    if (!r->gpar) return set_error(ERR_NOMEM, "object_stored_open: parity buffer");
    r->repair  = 1;
    r->par_off = par_off;
    r->rs_sz   = rs_sz;
    return OK;
}

static void object_stored_free(object_stored_reader_t *r)
{
    if (r->f) fclose(r->f);
    else if (r->fd >= 0) close(r->fd);
    if (r->p) pack_stored_close(r->p);
    free(r->gbuf);
    free(r->gpar);
    free(r);
}

status_t object_stored_open(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            object_stored_info_t *out_info, uint64_t *out_len,
                            object_stored_reader_t **out)
{
    if (!store || !hash || !out_info || !out_len || !out)
        return set_error(ERR_INVALID, "object_stored_open: null argument");

    object_stored_reader_t *r = calloc(1, sizeof(*r));
    if (!r) return set_error(ERR_NOMEM, "object_stored_open: alloc");
    r->fd = -1;
    memcpy(r->hash, hash, OBJECT_HASH_SIZE);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        status_t st = pack_object_open_stored(store, hash, &out_info->type,
                                              &out_info->compression,
                                              &out_info->encryption,
                                              &out_info->uncompressed_size,
                                              &r->remaining, &r->p);
        if (st != OK) { free(r); return st; }
        *out_len = r->remaining;
        *out = r;
        return OK;
    }
    r->fd = fd;

    /* read_object_header refuses a payload longer than the file, so the
     * reads below cannot be sent past the object. */
    object_header_t hdr;
    status_t hst = read_object_header(fd, &hdr);
    if (hst != OK) { object_stored_free(r); return hst; }
    r->remaining = r->len = hdr.compressed_size;

    hst = loose_stored_scan(r, &hdr);
    if (hst != OK) { object_stored_free(r); return hst; }

    if (!r->repair) {
        /* The scan used pread, so the descriptor is still just past the header. */
        r->f = fdopen(fd, "rb");
        if (!r->f) {
            status_t st = set_error_errno(ERR_IO, "object_stored_open: fdopen(%s)", path);
            object_stored_free(r);
            return st;
        }
    }

    out_info->type              = hdr.type;
    out_info->compression       = hdr.compression;
    out_info->encryption        = hdr.encryption;
    out_info->uncompressed_size = hdr.uncompressed_size;
    *out_len = r->remaining;
    *out = r;
    return OK;
}

/* Serve n loose payload bytes from groups corrected from parity. */
static status_t loose_stored_read_repaired(object_stored_reader_t *r, uint8_t *out, size_t n)
{
    uint64_t pos = r->len - r->remaining;
    while (n > 0) {
        if (r->g_len == 0 || pos < r->g_off || pos >= r->g_off + r->g_len) {
            r->g_off = pos - pos % RS_STREAM_GROUP_DATA;
            uint64_t left = r->len - r->g_off;
            r->g_len = left < RS_STREAM_GROUP_DATA ? (size_t)left : RS_STREAM_GROUP_DATA;
            if (pread(r->fd, r->gbuf, r->g_len, (off_t)sizeof(object_header_t) + (off_t)r->g_off)
                != (ssize_t)r->g_len) {
                r->g_len = 0;
                return set_error(ERR_CORRUPT, "object_stored_read: short read");
            }
            size_t pstart = (size_t)(r->g_off / RS_STREAM_GROUP_DATA) * RS_STREAM_GROUP_PAR;
            if (pstart + RS_STREAM_GROUP_PAR <= r->rs_sz &&
                pread(r->fd, r->gpar, RS_STREAM_GROUP_PAR, r->par_off + (off_t)pstart)
                    == (ssize_t)RS_STREAM_GROUP_PAR) {
                int rc = rs_parity_decode(r->gbuf, r->g_len, r->gpar, RS_STREAM_GROUP_PAR);
                if (rc > 0)      r->fixed += rc;
                else if (rc < 0) r->bad++;
            } else {
                r->bad++;
            }
        }
        size_t at   = (size_t)(pos - r->g_off);
        size_t take = r->g_len - at < n ? r->g_len - at : n;
        memcpy(out, r->gbuf + at, take);
        out += take;
        n   -= take;
        pos += take;
        r->remaining -= take;
    }
    return OK;
}

status_t object_stored_read(object_stored_reader_t *r, void *buf, size_t n)
{
    if (!r || (!buf && n)) return set_error(ERR_INVALID, "object_stored_read: null argument");
    if ((uint64_t)n > r->remaining)
        return set_error(ERR_INVALID, "object_stored_read: %zu bytes asked for, %llu left",
                         n, (unsigned long long)r->remaining);
    if (r->p) {
        status_t st = pack_stored_read(r->p, buf, n);
        if (st == OK) r->remaining -= n;
        return st;
    }
    if (r->repair) return loose_stored_read_repaired(r, buf, n);
    if (n && fread(buf, 1, n, r->f) != n)
        return set_error(ERR_CORRUPT, "object_stored_read: short read");
    r->remaining -= n;
    return OK;
}

int object_stored_damaged(const object_stored_reader_t *r)
{
    if (!r) return 0;
    if (r->p) return pack_stored_damaged(r->p);
    return r->bad > 0;
}

void object_stored_close(object_stored_reader_t *r)
{
    if (!r) return;
    if (r->repair) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(r->hash, hex);
        if (r->bad > 0) {
            log_warn("object %s: damaged beyond its parity (%llu group(s)); read as stored",
                     hex, (unsigned long long)r->bad);
            parity_stats_add_uncorrectable(1);
        } else if (r->fixed > 0) {
            log_warn("object %s: corrected from parity while reading it as stored "
                     "(%lld byte(s))", hex, (long long)r->fixed);
            parity_stats_add_repaired(1);
        }
    }
    object_stored_free(r);
}

status_t object_get_info(store_t *store,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint64_t *out_size, uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);

    int fd = open(path, O_RDONLY);
    if (fd == -1)
        return pack_object_get_info(store, hash, out_size, out_type);

    object_header_t hdr;
    status_t hst = read_object_header(fd, &hdr);
    close(fd);
    if (hst != OK) {
        char hex[OBJECT_HASH_SIZE * 2 + 1]; object_hash_to_hex(hash, hex);
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        return set_error(ERR_CORRUPT, "object %s: %s", hex, why);
    }

    if (out_size) *out_size = hdr.uncompressed_size;
    if (out_type) *out_type = hdr.type;
    return OK;
}

/*
 * Stream `size` bytes from src_fd into the object store as object type `type`.
 * The SHA-256 of the raw bytes is computed while streaming and verified
 * against expected_hash. Returns OK, ERR_NOMEM, ERR_IO, or ERR_CORRUPT.
 *
 * This is the import half of xfer, which Phase 0 deferred and which has not
 * been rebuilt, so nothing in the daemon calls it. It is kept because an
 * import path will want it, and it was finished rather than left refusing an
 * encrypted repository: for two commits it was also the only writer here
 * that emitted no parity trailer, so every object it produced was the one
 * kind this store does not know how to repair. A dead path that is quietly
 * wrong is worse than one that is merely unused.
 */
status_t object_store_fd(store_t *store, uint8_t type, int src_fd, uint64_t size,
                          const uint8_t expected_hash[OBJECT_HASH_SIZE]) {
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "store_fd: " OBJECT_TYPE_UNKNOWN_FMT, type);
    /* Check existence first to avoid redundant work. */
    if (object_exists(store, expected_hash)) return OK;

    if (store_needs_key(store))
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to write a plaintext object");

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", store_root(store))
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
        .compressed_size   = 0,   /* patched below */
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

    char ps_tmp_dir[PATH_MAX];
    snprintf(ps_tmp_dir, sizeof(ps_tmp_dir), "%s/tmp", store_root(store));
    rs_parity_stream_t ps;
    if (rs_parity_stream_init(&ps, 256 * 1024 * 1024, ps_tmp_dir) != 0) {
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath);
        return set_error(ERR_NOMEM, "store_fd: rs parity stream init");
    }

    uint8_t *buf = malloc(STREAM_LOAD_MAX);
    if (!buf) {
        rs_parity_stream_destroy(&ps);
        EVP_MD_CTX_free(mdctx); close(tfd); unlink(tmppath); return set_error(ERR_NOMEM, "malloc(%zu)", STREAM_LOAD_MAX);
    }

    uint32_t crc = 0;
    uint64_t total_written = 0;
    status_t st = OK;

    /* Nonce first, then ciphertext, then tag - the layout every other writer
     * here uses. All three are stored, so CRC and parity cover all three. */
    const repo_key_t *enc_key = store_key(store);
    object_crypt_stream_t *cs = NULL;
    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    if (enc_key) {
        st = object_crypt_stream_begin(&cs, enc_key, nonce, 1);
        if (st == OK && io_write_full(tfd, nonce, sizeof(nonce)) != 0)
            st = set_error_errno(ERR_IO, "store_fd: write nonce");
        if (st == OK) {
            crc = crc32c_update(crc, nonce, sizeof(nonce));
            if (rs_parity_stream_feed(&ps, nonce, sizeof(nonce)) != 0)
                st = set_error_errno(ERR_IO, "store_fd: rs feed (nonce)");
            total_written += sizeof(nonce);
        }
    }

    uint64_t remaining = size;
    while (remaining > 0 && st == OK) {
        size_t want = (remaining < STREAM_LOAD_MAX) ? (size_t)remaining : STREAM_LOAD_MAX;
        ssize_t r = read(src_fd, buf, want);
        if (r <= 0) {
            if (r == -1 && errno == EINTR) continue;
            st = set_error_errno(ERR_IO, "store_fd: read src_fd"); break;
        }
        /* Hash the plaintext, then encrypt in place: the buffer is ours until
         * it is written, so a second one would buy nothing. */
        if (EVP_DigestUpdate(mdctx, buf, (size_t)r) != 1) { st = set_error(ERR_IO, "store_fd: SHA256 update failed"); break; }
        if (cs) {
            st = object_crypt_stream_update(cs, buf, (size_t)r, buf);
            if (st != OK) break;
        }
        if (io_write_full(tfd, buf, (size_t)r) != 0) { st = set_error_errno(ERR_IO, "store_fd: write payload"); break; }
        crc = crc32c_update(crc, buf, (size_t)r);
        if (rs_parity_stream_feed(&ps, buf, (size_t)r) != 0) { st = set_error_errno(ERR_IO, "store_fd: rs feed"); break; }
        total_written += (uint64_t)r;
        remaining -= (uint64_t)r;
    }
    free(buf);

    if (st == OK && cs) {
        uint8_t tag[OBJECT_CRYPT_TAG_BYTES];
        st = object_crypt_stream_finish(cs, tag, NULL);
        if (st == OK && io_write_full(tfd, tag, sizeof(tag)) != 0)
            st = set_error_errno(ERR_IO, "store_fd: write tag");
        if (st == OK) {
            crc = crc32c_update(crc, tag, sizeof(tag));
            if (rs_parity_stream_feed(&ps, tag, sizeof(tag)) != 0)
                st = set_error_errno(ERR_IO, "store_fd: rs feed (tag)");
            total_written += sizeof(tag);
        }
    }
    object_crypt_stream_free(cs);

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

    if (st == OK && rs_parity_stream_finish(&ps) != 0)
        st = set_error_errno(ERR_IO, "store_fd: rs finish");

    if (st != OK) {
        rs_parity_stream_destroy(&ps);
        close(tfd); unlink(tmppath); return st;
    }

    hdr.compressed_size = total_written;
    hdr.encryption      = enc_key ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
    if (pwrite(tfd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "store_fd: pwrite header");
    }

    /* Parity trailer: header XOR record, RS payload parity, CRC, footer. */
    parity_record_t hpar;
    parity_record_compute(&hdr, sizeof(hdr), &hpar);
    if (lseek(tfd, 0, SEEK_END) == (off_t)-1 ||
        write(tfd, &hpar, sizeof(hpar)) != (ssize_t)sizeof(hpar)) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "store_fd: write hpar");
    }
    size_t rs_sz = rs_parity_stream_total(&ps);
    warn_if_parity_unusable(hdr.compressed_size, "store_fd");
    if (rs_sz > 0 && rs_parity_stream_replay_fd(&ps, tfd) != 0) {
        rs_parity_stream_destroy(&ps); close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "store_fd: write rs parity");
    }
    rs_parity_stream_destroy(&ps);

    if (write(tfd, &crc, sizeof(crc)) != (ssize_t)sizeof(crc)) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "store_fd: write crc");
    }
    /* Deprecated rs_data_len field - always 0, see parity.h. */
    uint32_t rs_data_len = PARITY_RS_DATA_LEN_DEPRECATED;
    if (write(tfd, &rs_data_len, sizeof(rs_data_len)) != (ssize_t)sizeof(rs_data_len)) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "store_fd: write rs_data_len");
    }
    parity_footer_t pftr = {
        .magic        = PARITY_FOOTER_MAGIC,
        .version      = PARITY_VERSION,
        .trailer_size = (uint32_t)(sizeof(hpar) + rs_sz + sizeof(crc)
                                   + sizeof(rs_data_len) + sizeof(parity_footer_t)),
    };
    if (write(tfd, &pftr, sizeof(pftr)) != (ssize_t)sizeof(pftr)) {
        close(tfd); unlink(tmppath);
        return set_error_errno(ERR_IO, "store_fd: write footer");
    }

    if (async_writeback(tfd) != 0) { close(tfd); unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: fdatasync"); }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(expected_hash, subdir, fname);
    int objfd = store_objects_fd(store);
    if (objfd == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: openat(objects)"); }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: mkdirat(%s)", subdir);
    }
    if (errno == EEXIST) errno = 0;

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", store_root(store), subdir, fname)
        >= (int)sizeof(dstpath)) { unlink(tmppath); return set_error(ERR_IO, "store_fd: dstpath too long"); }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && object_exists(store, expected_hash)) {
            unlink(tmppath);
            return OK;
        }
        unlink(tmppath); return set_error_errno(ERR_IO, "store_fd: rename(%s, %s)", tmppath, dstpath);
    }

    store_loose_set_insert(store, expected_hash);
    return OK;
}

/*
 * Whether a header that parity has just reconstructed describes this file.
 * Parity only promises the bytes match the CRC it recorded; this is the
 * check that the result is also an object header for these bytes, before
 * it is written over the old one.
 */
static int repaired_header_fits(const object_header_t *h,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                uint64_t stored_size)
{
    return h->magic == OBJECT_MAGIC &&
           h->version == OBJECT_HDR_VERSION &&
           (h->compression == COMPRESS_NONE || h->compression == COMPRESS_LZ4 ||
            h->compression == COMPRESS_LZ4_FRAME) &&
           (h->encryption == ENCRYPT_NONE || h->encryption == ENCRYPT_CHACHA20) &&
           h->compressed_size == stored_size &&
           memcmp(h->hash, hash, OBJECT_HASH_SIZE) == 0;
}

int object_repair(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", store_root(store), subdir, fname);
    int fd = open(path, O_RDWR);
    if (fd == -1) return -1;  /* not a loose object or not writable */

    /*
     * The header is read raw, not through read_object_header. Its fields
     * are what this function may be about to rewrite, so none of them is
     * allowed to decide whether or where repair looks: a damaged magic,
     * version or compressed_size used to end the repair before it began,
     * and a compressed_size that grew past the trailer's start was refused
     * by the sanity check meant to protect it (OWN-32).
     */
    object_header_t hdr;
    if (pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) { close(fd); return -1; }

    /* Read parity footer. A file without one (a version 1 object) has no
     * parity to repair from. */
    parity_footer_t pftr;
    off_t file_end = lseek(fd, 0, SEEK_END);
    if (file_end < (off_t)(sizeof(hdr) + LOOSE_TRAILER_FIXED)) { close(fd); return 0; }
    if (pread(fd, &pftr, sizeof(pftr), file_end - (off_t)sizeof(pftr))
        != (ssize_t)sizeof(pftr) ||
        pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION) {
        close(fd); return 0;
    }

    /*
     * OWN-30 stopgap: past the threshold trailer_size has wrapped, so the
     * trailer_start computed below points into the wrong bytes and every
     * "correction" read from there would be written into user data. Judged
     * from the file size alone, which is exact for one object and does not
     * depend on a header this function may rewrite.
     */
    if (!parity_trailer_fits_file((uint64_t)file_end, 1, sizeof(hdr), 0,
                                  LOOSE_TRAILER_FIXED, 0)) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        log_warn("object %s: not repaired: %lld-byte file (header claims "
                 "%llu stored) is " PARITY_TOO_LARGE_REASON, hex,
                 (long long)file_end, (unsigned long long)hdr.compressed_size);
        close(fd);
        return PARITY_REPAIR_REFUSED_TOO_LARGE;
    }

    /*
     * The layout comes from the file size and the footer: the trailer is
     * the last trailer_size bytes, the payload is everything between the
     * header and it, and the trailer must then be exactly what a payload of
     * that size is given. A footer that disagrees is itself damaged, and
     * nothing located through it is safe to write from.
     */
    off_t trailer_start = file_end - (off_t)pftr.trailer_size;
    if (trailer_start < (off_t)sizeof(hdr)) { close(fd); return 0; }
    uint64_t stored_size = (uint64_t)trailer_start - sizeof(hdr);
    if ((uint64_t)pftr.trailer_size
        != LOOSE_TRAILER_FIXED + (uint64_t)rs_parity_size((size_t)stored_size)) {
        close(fd); return 0;
    }

    int total_fixed = 0;

    /* Header: XOR parity repair, written back only when the result is a
     * header for this file. */
    parity_record_t hdr_par;
    if (pread(fd, &hdr_par, sizeof(hdr_par), trailer_start) == (ssize_t)sizeof(hdr_par)) {
        object_header_t hdr_copy = hdr;
        int hrc = parity_record_check(&hdr_copy, sizeof(hdr_copy), &hdr_par);
        if (hrc == 1) {
            if (repaired_header_fits(&hdr_copy, hash, stored_size)) {
                if (pwrite(fd, &hdr_copy, sizeof(hdr_copy), 0) == (ssize_t)sizeof(hdr_copy))
                    total_fixed++;
            } else {
                char hex[OBJECT_HASH_SIZE * 2 + 1];
                object_hash_to_hex(hash, hex);
                log_warn("object %s: header parity produced a header that does "
                         "not describe the file; header left as it is", hex);
            }
        }
    }

    /*
     * Payload: CRC pre-check, then correct group by group.
     *
     * This used to read the entire payload into memory and write it all
     * back, guarded only by a 128 GiB sanity limit - which is not a limit
     * on a machine with 32 GiB of RAM. Objects are allowed to exceed memory
     * (design constraint 2), and the ones that stay loose the longest are
     * the large incompressible ones, so the repair path was unusable
     * exactly where it was most needed. Groups are independent, so
     * correcting one at a time costs a 15 KiB buffer whatever the size.
     */
    /* Sized from the file, never from the header: the header may be the
     * damaged part, and the old code repaired it on disk and then walked
     * the payload with the damaged copy's length anyway (OWN-32). */
    if (stored_size > 0) {
        size_t   csize = (size_t)stored_size;
        size_t   rs_sz = rs_parity_size(csize);
        uint32_t stored_crc = 0;
        off_t crc_off = file_end - (off_t)sizeof(pftr)
                        - (off_t)sizeof(uint32_t) - (off_t)sizeof(uint32_t);

        if (rs_sz > 0 &&
            pread(fd, &stored_crc, sizeof(stored_crc), crc_off) == (ssize_t)sizeof(stored_crc)) {

            uint8_t *gbuf = malloc(RS_STREAM_GROUP_DATA);
            uint8_t *gpar = malloc(RS_STREAM_GROUP_PAR);
            if (gbuf && gpar) {
                /* One sequential pass to see whether anything is wrong at
                 * all: a clean object must not pay for syndrome work. */
                uint32_t cur_crc = 0;
                size_t   left = csize;
                off_t    off = (off_t)sizeof(hdr);
                int      readable = 1;
                while (left > 0) {
                    size_t want = left < RS_STREAM_GROUP_DATA ? left : RS_STREAM_GROUP_DATA;
                    if (pread(fd, gbuf, want, off) != (ssize_t)want) { readable = 0; break; }
                    cur_crc = crc32c_update(cur_crc, gbuf, want);
                    off  += (off_t)want;
                    left -= want;
                }

                if (readable && cur_crc != stored_crc) {
                    rs_init();
                    left = csize;
                    off  = (off_t)sizeof(hdr);
                    off_t par_off = trailer_start + (off_t)sizeof(hdr_par);
                    size_t par_left = rs_sz;

                    while (left > 0 && par_left >= RS_STREAM_GROUP_PAR) {
                        size_t want = left < RS_STREAM_GROUP_DATA ? left : RS_STREAM_GROUP_DATA;
                        if (pread(fd, gbuf, want, off) != (ssize_t)want) break;
                        if (pread(fd, gpar, RS_STREAM_GROUP_PAR, par_off)
                            != (ssize_t)RS_STREAM_GROUP_PAR) break;

                        int rrc = rs_parity_decode(gbuf, want, gpar, RS_STREAM_GROUP_PAR);
                        if (rrc > 0) {
                            if (pwrite(fd, gbuf, want, off) == (ssize_t)want)
                                total_fixed += rrc;
                        }
                        off      += (off_t)want;
                        par_off  += (off_t)RS_STREAM_GROUP_PAR;
                        par_left -= RS_STREAM_GROUP_PAR;
                        left     -= want;
                    }
                }
            }
            free(gbuf);
            free(gpar);
        }
    }

    if (total_fixed > 0)
        fsync(fd);
    close(fd);
    return total_fixed;
}
