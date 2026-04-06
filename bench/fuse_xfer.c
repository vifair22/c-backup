/*
 * FUSE-to-disk transfer benchmark.
 *
 * Tests 5 strategies for copying a file from a FUSE source to a real disk,
 * each with the full backup pipeline (SHA256 + CRC32C + RS parity).
 *
 * Usage:
 *   build/bench_fuse_xfer <source_file> <dest_dir>
 *
 *   source_file  — large file on FUSE (e.g. /mnt/user/share/big.rar)
 *   dest_dir     — directory on the repo disk (e.g. /mnt/disk5/tmp)
 *
 * Each test writes to dest_dir/bench_XXXX, cleaned up after.
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "parity.h"
#include "parity_stream.h"

/* ------------------------------------------------------------------ */
/* Timing                                                              */
/* ------------------------------------------------------------------ */

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static uint64_t file_size_of(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0) { perror(path); exit(1); }
    return (uint64_t)st.st_size;
}

static int open_src(const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); exit(1); }
    return fd;
}

static int open_dst(const char *dir)
{
    char path[4096];
    snprintf(path, sizeof(path), "%s/bench_xfer.XXXXXX", dir);
    int fd = mkstemp(path);
    if (fd < 0) { perror(path); exit(1); }
    unlink(path);
    return fd;
}

static void report(const char *name, uint64_t bytes, double elapsed)
{
    double mb = (double)bytes / (1024.0 * 1024.0);
    printf("  %-44s  %8.1f MiB/s  (%.1fs)\n", name, mb / elapsed, elapsed);
}

/* ------------------------------------------------------------------ */
/* Test 1: Serial 1 MiB — read(1M) → SHA256 → write → CRC → RS      */
/* Baseline: exactly what the old code did on FUSE.                   */
/* ------------------------------------------------------------------ */

static void test_serial_1m(const char *src_path, const char *dst_dir,
                           uint64_t fsize)
{
    int sfd = open_src(src_path);
    int dfd = open_dst(dst_dir);

    const size_t CHUNK = 1024 * 1024;
    uint8_t *buf = malloc(CHUNK);
    uint8_t hash[32];
    unsigned int dlen;
    uint32_t crc = 0;

    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);

    rs_parity_stream_t ps;
    rs_parity_stream_init(&ps, 256 * 1024 * 1024, "/tmp");

    double t0 = now_sec();
    uint64_t total = 0;

    while (total < fsize) {
        size_t want = CHUNK;
        if ((uint64_t)want > fsize - total) want = (size_t)(fsize - total);

        size_t got = 0;
        while (got < want) {
            ssize_t r = read(sfd, buf + got, want - got);
            if (r > 0) { got += (size_t)r; continue; }
            if (r == -1 && errno == EINTR) continue;
            perror("read"); goto out;
        }

        EVP_DigestUpdate(md, buf, got);
        if (write(dfd, buf, got) != (ssize_t)got) { perror("write"); goto out; }
        crc = crc32c_update(crc, buf, got);
        rs_parity_stream_feed(&ps, buf, got);
        total += got;
    }

out:;
    double elapsed = now_sec() - t0;
    EVP_DigestFinal_ex(md, hash, &dlen);
    rs_parity_stream_finish(&ps);

    report("serial_1M (read→sha→write→crc→rs)", total, elapsed);

    rs_parity_stream_destroy(&ps);
    EVP_MD_CTX_free(md);
    free(buf);
    close(dfd);
    close(sfd);
    (void)crc;
}

/* ------------------------------------------------------------------ */
/* Test 2: Serial 4 MiB — same pipeline but larger chunks             */
/* Tests whether bigger reads/writes help on FUSE.                    */
/* ------------------------------------------------------------------ */

static void test_serial_4m(const char *src_path, const char *dst_dir,
                           uint64_t fsize)
{
    int sfd = open_src(src_path);
    int dfd = open_dst(dst_dir);

    const size_t CHUNK = 4 * 1024 * 1024;
    uint8_t *buf = malloc(CHUNK);
    uint8_t hash[32];
    unsigned int dlen;
    uint32_t crc = 0;

    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);

    rs_parity_stream_t ps;
    rs_parity_stream_init(&ps, 256 * 1024 * 1024, "/tmp");

    double t0 = now_sec();
    uint64_t total = 0;

    while (total < fsize) {
        size_t want = CHUNK;
        if ((uint64_t)want > fsize - total) want = (size_t)(fsize - total);

        size_t got = 0;
        while (got < want) {
            ssize_t r = read(sfd, buf + got, want - got);
            if (r > 0) { got += (size_t)r; continue; }
            if (r == -1 && errno == EINTR) continue;
            perror("read"); goto out;
        }

        EVP_DigestUpdate(md, buf, got);
        if (write(dfd, buf, got) != (ssize_t)got) { perror("write"); goto out; }
        crc = crc32c_update(crc, buf, got);
        rs_parity_stream_feed(&ps, buf, got);
        total += got;
    }

out:;
    double elapsed = now_sec() - t0;
    EVP_DigestFinal_ex(md, hash, &dlen);
    rs_parity_stream_finish(&ps);

    report("serial_4M (bigger chunks)", total, elapsed);

    rs_parity_stream_destroy(&ps);
    EVP_MD_CTX_free(md);
    free(buf);
    close(dfd);
    close(sfd);
    (void)crc;
}

/* ------------------------------------------------------------------ */
/* Test 3: Ring reader (4 × 1 MiB) — current implementation           */
/* Reader thread fills ring, main thread processes + writes.           */
/* ------------------------------------------------------------------ */

#define RING_SLOTS 4
#define RING_CHUNK (1024 * 1024)

typedef struct {
    uint8_t *bufs[RING_SLOTS];
    size_t   lens[RING_SLOTS];
    int      head, tail, count;
    pthread_mutex_t mu;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;
    int      src_fd;
    uint64_t file_size;
    uint64_t read_offset;
    int      done;
    int      error;
    size_t   chunk_size;
} ring_t;

static void *ring_reader(void *arg)
{
    ring_t *r = (ring_t *)arg;
    while (r->read_offset < r->file_size) {
        size_t want = r->chunk_size;
        if ((uint64_t)want > r->file_size - r->read_offset)
            want = (size_t)(r->file_size - r->read_offset);

        pthread_mutex_lock(&r->mu);
        while (r->count == RING_SLOTS && !r->done)
            pthread_cond_wait(&r->not_full, &r->mu);
        if (r->done) { pthread_mutex_unlock(&r->mu); return NULL; }
        int slot = r->head;
        pthread_mutex_unlock(&r->mu);

        size_t got = 0;
        while (got < want) {
            ssize_t rd = read(r->src_fd, r->bufs[slot] + got, want - got);
            if (rd > 0) { got += (size_t)rd; continue; }
            if (rd == -1 && errno == EINTR) continue;
            pthread_mutex_lock(&r->mu);
            r->error = errno ? errno : EIO;
            r->done = 1;
            pthread_cond_signal(&r->not_empty);
            pthread_mutex_unlock(&r->mu);
            return NULL;
        }

        pthread_mutex_lock(&r->mu);
        r->lens[slot] = got;
        r->head = (r->head + 1) % RING_SLOTS;
        r->count++;
        r->read_offset += got;
        pthread_cond_signal(&r->not_empty);
        pthread_mutex_unlock(&r->mu);
    }

    pthread_mutex_lock(&r->mu);
    r->done = 1;
    pthread_cond_signal(&r->not_empty);
    pthread_mutex_unlock(&r->mu);
    return NULL;
}

static uint8_t *ring_pull(ring_t *r, size_t *out_len, int *out_err)
{
    pthread_mutex_lock(&r->mu);
    while (r->count == 0 && !r->done)
        pthread_cond_wait(&r->not_empty, &r->mu);
    if (r->count == 0) {
        *out_err = r->error;
        *out_len = 0;
        pthread_mutex_unlock(&r->mu);
        return NULL;
    }
    int slot = r->tail;
    *out_len = r->lens[slot];
    uint8_t *buf = r->bufs[slot];
    r->tail = (r->tail + 1) % RING_SLOTS;
    r->count--;
    *out_err = 0;
    pthread_cond_signal(&r->not_full);
    pthread_mutex_unlock(&r->mu);
    return buf;
}

static void test_ring_1m(const char *src_path, const char *dst_dir,
                         uint64_t fsize)
{
    int sfd = open_src(src_path);
    int dfd = open_dst(dst_dir);

    ring_t ring;
    memset(&ring, 0, sizeof(ring));
    ring.src_fd = sfd;
    ring.file_size = fsize;
    ring.chunk_size = RING_CHUNK;
    pthread_mutex_init(&ring.mu, NULL);
    pthread_cond_init(&ring.not_full, NULL);
    pthread_cond_init(&ring.not_empty, NULL);
    for (int i = 0; i < RING_SLOTS; i++)
        ring.bufs[i] = malloc(RING_CHUNK);

    uint8_t hash[32];
    unsigned int dlen;
    uint32_t crc = 0;
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);

    rs_parity_stream_t ps;
    rs_parity_stream_init(&ps, 256 * 1024 * 1024, "/tmp");

    pthread_t thr;
    pthread_create(&thr, NULL, ring_reader, &ring);

    double t0 = now_sec();
    uint64_t total = 0;

    for (;;) {
        size_t got = 0;
        int err = 0;
        uint8_t *buf = ring_pull(&ring, &got, &err);
        if (!buf) break;

        EVP_DigestUpdate(md, buf, got);
        if (write(dfd, buf, got) != (ssize_t)got) { perror("write"); break; }
        crc = crc32c_update(crc, buf, got);
        rs_parity_stream_feed(&ps, buf, got);
        total += got;
    }

    double elapsed = now_sec() - t0;
    pthread_join(thr, NULL);

    EVP_DigestFinal_ex(md, hash, &dlen);
    rs_parity_stream_finish(&ps);

    report("ring_4x1M (reader thread, 1M slots)", total, elapsed);

    rs_parity_stream_destroy(&ps);
    EVP_MD_CTX_free(md);
    for (int i = 0; i < RING_SLOTS; i++) free(ring.bufs[i]);
    pthread_mutex_destroy(&ring.mu);
    pthread_cond_destroy(&ring.not_full);
    pthread_cond_destroy(&ring.not_empty);
    close(dfd);
    close(sfd);
    (void)crc;
}

/* ------------------------------------------------------------------ */
/* Test 4: Ring reader (4 × 4 MiB) — larger ring slots                */
/* Maybe FUSE benefits from bigger reads even with a ring.             */
/* ------------------------------------------------------------------ */

#define RING_CHUNK_4M (4 * 1024 * 1024)

static void test_ring_4m(const char *src_path, const char *dst_dir,
                         uint64_t fsize)
{
    int sfd = open_src(src_path);
    int dfd = open_dst(dst_dir);

    ring_t ring;
    memset(&ring, 0, sizeof(ring));
    ring.src_fd = sfd;
    ring.file_size = fsize;
    ring.chunk_size = RING_CHUNK_4M;
    pthread_mutex_init(&ring.mu, NULL);
    pthread_cond_init(&ring.not_full, NULL);
    pthread_cond_init(&ring.not_empty, NULL);
    for (int i = 0; i < RING_SLOTS; i++)
        ring.bufs[i] = malloc(RING_CHUNK_4M);

    uint8_t hash[32];
    unsigned int dlen;
    uint32_t crc = 0;
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);

    rs_parity_stream_t ps;
    rs_parity_stream_init(&ps, 256 * 1024 * 1024, "/tmp");

    pthread_t thr;
    pthread_create(&thr, NULL, ring_reader, &ring);

    double t0 = now_sec();
    uint64_t total = 0;

    for (;;) {
        size_t got = 0;
        int err = 0;
        uint8_t *buf = ring_pull(&ring, &got, &err);
        if (!buf) break;

        EVP_DigestUpdate(md, buf, got);
        if (write(dfd, buf, got) != (ssize_t)got) { perror("write"); break; }
        crc = crc32c_update(crc, buf, got);
        rs_parity_stream_feed(&ps, buf, got);
        total += got;
    }

    double elapsed = now_sec() - t0;
    pthread_join(thr, NULL);

    EVP_DigestFinal_ex(md, hash, &dlen);
    rs_parity_stream_finish(&ps);

    report("ring_4x4M (reader thread, 4M slots)", total, elapsed);

    rs_parity_stream_destroy(&ps);
    EVP_MD_CTX_free(md);
    for (int i = 0; i < RING_SLOTS; i++) free(ring.bufs[i]);
    pthread_mutex_destroy(&ring.mu);
    pthread_cond_destroy(&ring.not_full);
    pthread_cond_destroy(&ring.not_empty);
    close(dfd);
    close(sfd);
    (void)crc;
}

/* ------------------------------------------------------------------ */
/* Test 5: Double ring — reader thread + writer thread                 */
/* Main thread only does CPU work (SHA256 + CRC + RS).                */
/* Reader fills read ring, main processes, writer drains write ring.   */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t *bufs[RING_SLOTS];
    size_t   lens[RING_SLOTS];
    int      head, tail, count;
    pthread_mutex_t mu;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;
    int      fd;
    int      done;
    int      stop;
    int      error;
} wring_t;

static void *write_thread(void *arg)
{
    wring_t *w = (wring_t *)arg;
    for (;;) {
        pthread_mutex_lock(&w->mu);
        while (w->count == 0 && !w->done)
            pthread_cond_wait(&w->not_empty, &w->mu);
        if (w->count == 0 && w->done) {
            pthread_mutex_unlock(&w->mu);
            return NULL;
        }
        int slot = w->tail;
        size_t len = w->lens[slot];
        uint8_t *buf = w->bufs[slot];
        pthread_mutex_unlock(&w->mu);

        /* Write outside the lock */
        const uint8_t *p = buf;
        while (len > 0) {
            ssize_t wr = write(w->fd, p, len);
            if (wr > 0) { p += wr; len -= (size_t)wr; continue; }
            if (wr == -1 && errno == EINTR) continue;
            pthread_mutex_lock(&w->mu);
            w->error = errno ? errno : EIO;
            w->done = 1;
            pthread_mutex_unlock(&w->mu);
            return NULL;
        }

        pthread_mutex_lock(&w->mu);
        w->tail = (w->tail + 1) % RING_SLOTS;
        w->count--;
        pthread_cond_signal(&w->not_full);
        pthread_mutex_unlock(&w->mu);
    }
}

static void test_double_ring(const char *src_path, const char *dst_dir,
                             uint64_t fsize)
{
    int sfd = open_src(src_path);
    int dfd = open_dst(dst_dir);

    /* Read ring */
    ring_t rring;
    memset(&rring, 0, sizeof(rring));
    rring.src_fd = sfd;
    rring.file_size = fsize;
    rring.chunk_size = RING_CHUNK;
    pthread_mutex_init(&rring.mu, NULL);
    pthread_cond_init(&rring.not_full, NULL);
    pthread_cond_init(&rring.not_empty, NULL);
    for (int i = 0; i < RING_SLOTS; i++)
        rring.bufs[i] = malloc(RING_CHUNK);

    /* Write ring — separate buffers so main thread can hand off */
    wring_t wring;
    memset(&wring, 0, sizeof(wring));
    wring.fd = dfd;
    pthread_mutex_init(&wring.mu, NULL);
    pthread_cond_init(&wring.not_full, NULL);
    pthread_cond_init(&wring.not_empty, NULL);
    for (int i = 0; i < RING_SLOTS; i++)
        wring.bufs[i] = malloc(RING_CHUNK);

    uint8_t hash[32];
    unsigned int dlen;
    uint32_t crc = 0;
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);

    rs_parity_stream_t ps;
    rs_parity_stream_init(&ps, 256 * 1024 * 1024, "/tmp");

    pthread_t rthr, wthr;
    pthread_create(&rthr, NULL, ring_reader, &rring);
    pthread_create(&wthr, NULL, write_thread, &wring);

    double t0 = now_sec();
    uint64_t total = 0;

    for (;;) {
        size_t got = 0;
        int err = 0;
        uint8_t *rbuf = ring_pull(&rring, &got, &err);
        if (!rbuf) break;

        /* CPU work on read buffer */
        EVP_DigestUpdate(md, rbuf, got);
        crc = crc32c_update(crc, rbuf, got);
        rs_parity_stream_feed(&ps, rbuf, got);

        /* Copy to write ring and submit */
        pthread_mutex_lock(&wring.mu);
        while (wring.count == RING_SLOTS && !wring.done)
            pthread_cond_wait(&wring.not_full, &wring.mu);
        if (wring.error) {
            pthread_mutex_unlock(&wring.mu);
            break;
        }
        int wslot = wring.head;
        memcpy(wring.bufs[wslot], rbuf, got);
        wring.lens[wslot] = got;
        wring.head = (wring.head + 1) % RING_SLOTS;
        wring.count++;
        pthread_cond_signal(&wring.not_empty);
        pthread_mutex_unlock(&wring.mu);

        total += got;
    }

    /* Signal writer to finish */
    pthread_mutex_lock(&wring.mu);
    wring.done = 1;
    pthread_cond_signal(&wring.not_empty);
    pthread_mutex_unlock(&wring.mu);

    pthread_join(rthr, NULL);
    pthread_join(wthr, NULL);

    double elapsed = now_sec() - t0;

    EVP_DigestFinal_ex(md, hash, &dlen);
    rs_parity_stream_finish(&ps);

    report("double_ring (read thr + write thr)", total, elapsed);

    rs_parity_stream_destroy(&ps);
    EVP_MD_CTX_free(md);
    for (int i = 0; i < RING_SLOTS; i++) {
        free(rring.bufs[i]);
        free(wring.bufs[i]);
    }
    pthread_mutex_destroy(&rring.mu);
    pthread_cond_destroy(&rring.not_full);
    pthread_cond_destroy(&rring.not_empty);
    pthread_mutex_destroy(&wring.mu);
    pthread_cond_destroy(&wring.not_full);
    pthread_cond_destroy(&wring.not_empty);
    close(dfd);
    close(sfd);
    (void)crc;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <source_file> <dest_dir>\n", argv[0]);
        fprintf(stderr, "  source_file  — file on FUSE mount\n");
        fprintf(stderr, "  dest_dir     — directory on repo disk\n");
        return 1;
    }

    const char *src_path = argv[1];
    const char *dst_dir  = argv[2];

    uint64_t fsize = file_size_of(src_path);

    rs_init();

    printf("\nFUSE transfer benchmark\n");
    printf("  source: %s (%.1f GiB)\n", src_path,
           (double)fsize / (1024.0 * 1024.0 * 1024.0));
    printf("  dest:   %s\n\n", dst_dir);
    printf("  %-44s  %14s\n", "STRATEGY", "THROUGHPUT");
    printf("  %-44s  %14s\n",
           "--------------------------------------------",
           "--------------");

    /* Drop page cache between tests so each starts cold */
    #define DROP_CACHE() do { \
        sync(); \
        FILE *_f = fopen("/proc/sys/vm/drop_caches", "w"); \
        if (_f) { fprintf(_f, "3\n"); fclose(_f); } \
        usleep(500000); \
    } while (0)

    DROP_CACHE();
    test_serial_1m(src_path, dst_dir, fsize);

    DROP_CACHE();
    test_serial_4m(src_path, dst_dir, fsize);

    DROP_CACHE();
    test_ring_1m(src_path, dst_dir, fsize);

    DROP_CACHE();
    test_ring_4m(src_path, dst_dir, fsize);

    DROP_CACHE();
    test_double_ring(src_path, dst_dir, fsize);

    printf("\n");
    return 0;
}
