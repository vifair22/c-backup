/*
 * Microbenchmark harness for c-backup hot paths.
 *
 * Usage:
 *   build/bench                  — run all benchmarks
 *   build/bench sha256           — run only benchmarks matching "sha256"
 *   build/bench sha256 crc32c    — run benchmarks matching either name
 *   build/bench -l               — list available benchmarks
 *
 * Each benchmark runs for at least BENCH_MIN_SECS seconds (default 1),
 * reports throughput in MiB/s or Mops/s.
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "parity.h"

/* ------------------------------------------------------------------ */
/* Timing helpers                                                      */
/* ------------------------------------------------------------------ */

#define BENCH_MIN_SECS  1.0
#define BENCH_WARMUP    2

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ------------------------------------------------------------------ */
/* Random-ish data buffer (deterministic, not crypto)                   */
/* ------------------------------------------------------------------ */

static uint8_t *g_buf;        /* 16 MiB scratch buffer */
static size_t   g_buf_sz;

static void init_buf(void)
{
    g_buf_sz = 16u * 1024 * 1024;
    g_buf = malloc(g_buf_sz);
    if (!g_buf) { perror("malloc"); exit(1); }
    /* Fill with pseudo-random bytes so compression/hash behave realistically */
    uint64_t s = 0xdeadbeefcafebabeULL;
    uint64_t *p = (uint64_t *)g_buf;
    for (size_t i = 0; i < g_buf_sz / 8; i++) {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17;
        p[i] = s;
    }
}

/* ------------------------------------------------------------------ */
/* Benchmark registry                                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *name;
    const char *unit;        /* "MiB/s" or "Mops/s" */
    void (*fn)(double *throughput);
} bench_entry_t;

static bench_entry_t g_benches[64];
static int g_nbench;

#define REGISTER_BENCH(nm, un, func) \
    __attribute__((constructor)) static void reg_##func(void) { \
        g_benches[g_nbench].name = nm; \
        g_benches[g_nbench].unit = un; \
        g_benches[g_nbench].fn   = func; \
        g_nbench++; \
    }

/* ------------------------------------------------------------------ */
/* Reporting                                                           */
/* ------------------------------------------------------------------ */

static void report(const char *name, const char *unit,
                   double throughput, uint64_t iters, double elapsed)
{
    printf("  %-36s %10.1f %-6s  (%lu iters, %.2fs)\n",
           name, throughput, unit, (unsigned long)iters, elapsed);
}

/* ------------------------------------------------------------------ */
/* SHA-256 benchmarks                                                  */
/* ------------------------------------------------------------------ */

static void bench_sha256_4k(double *tp)
{
    uint8_t hash[32];
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) SHA256(g_buf, 4096, hash);
    double t0 = now_sec(), elapsed;
    do { SHA256(g_buf, 4096, hash); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = ((double)iters * 4096.0) / (1024.0 * 1024.0) / elapsed;
    report("sha256_oneshot_4K", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("sha256_oneshot_4K", "MiB/s", bench_sha256_4k);

static void bench_sha256_64k(double *tp)
{
    uint8_t hash[32];
    size_t sz = 64 * 1024;
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) SHA256(g_buf, sz, hash);
    double t0 = now_sec(), elapsed;
    do { SHA256(g_buf, sz, hash); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = ((double)iters * (double)sz) / (1024.0 * 1024.0) / elapsed;
    report("sha256_oneshot_64K", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("sha256_oneshot_64K", "MiB/s", bench_sha256_64k);

static void bench_sha256_1m(double *tp)
{
    uint8_t hash[32];
    size_t sz = 1024 * 1024;
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) SHA256(g_buf, sz, hash);
    double t0 = now_sec(), elapsed;
    do { SHA256(g_buf, sz, hash); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = ((double)iters * (double)sz) / (1024.0 * 1024.0) / elapsed;
    report("sha256_oneshot_1M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("sha256_oneshot_1M", "MiB/s", bench_sha256_1m);

static void bench_sha256_16m(double *tp)
{
    uint8_t hash[32];
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) SHA256(g_buf, g_buf_sz, hash);
    double t0 = now_sec(), elapsed;
    do { SHA256(g_buf, g_buf_sz, hash); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = ((double)iters * (double)g_buf_sz) / (1024.0 * 1024.0) / elapsed;
    report("sha256_oneshot_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("sha256_oneshot_16M", "MiB/s", bench_sha256_16m);

/* EVP streaming: N x Update calls of chunk_sz, then Final */
static void bench_sha256_evp_stream(double *tp)
{
    /* Simulate object.c's streaming path: 16 MiB buffer, EVP update */
    uint8_t hash[32];
    unsigned int dlen;
    uint64_t iters = 0;
    size_t chunk = g_buf_sz;

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    /* warmup */
    for (int i = 0; i < BENCH_WARMUP; i++) {
        EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(ctx, g_buf, chunk);
        EVP_DigestFinal_ex(ctx, hash, &dlen);
    }
    double t0 = now_sec(), elapsed;
    do {
        EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(ctx, g_buf, chunk);
        EVP_DigestFinal_ex(ctx, hash, &dlen);
        iters++;
        elapsed = now_sec() - t0;
    } while (elapsed < BENCH_MIN_SECS);
    EVP_MD_CTX_free(ctx);
    *tp = ((double)iters * (double)chunk) / (1024.0 * 1024.0) / elapsed;
    report("sha256_evp_stream_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("sha256_evp_stream_16M", "MiB/s", bench_sha256_evp_stream);

/* EVP per-object overhead: many tiny objects (256 bytes) */
static void bench_sha256_evp_small(double *tp)
{
    uint8_t hash[32];
    unsigned int dlen;
    size_t obj_sz = 256;
    uint64_t iters = 0;

    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    for (int i = 0; i < BENCH_WARMUP; i++) {
        EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(ctx, g_buf, obj_sz);
        EVP_DigestFinal_ex(ctx, hash, &dlen);
    }
    double t0 = now_sec(), elapsed;
    do {
        EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
        EVP_DigestUpdate(ctx, g_buf, obj_sz);
        EVP_DigestFinal_ex(ctx, hash, &dlen);
        iters++;
        elapsed = now_sec() - t0;
    } while (elapsed < BENCH_MIN_SECS);
    EVP_MD_CTX_free(ctx);
    *tp = (double)iters / 1e6 / elapsed;
    report("sha256_evp_small_256B", "Mops/s", *tp, iters, elapsed);
}
REGISTER_BENCH("sha256_evp_small_256B", "Mops/s", bench_sha256_evp_small);

/* ------------------------------------------------------------------ */
/* CRC-32C benchmarks                                                  */
/* ------------------------------------------------------------------ */

static void bench_crc32c_4k(double *tp)
{
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) crc32c(g_buf, 4096);
    double t0 = now_sec(), elapsed;
    do { crc32c(g_buf, 4096); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = ((double)iters * 4096.0) / (1024.0 * 1024.0) / elapsed;
    report("crc32c_4K", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("crc32c_4K", "MiB/s", bench_crc32c_4k);

static void bench_crc32c_16m(double *tp)
{
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) crc32c(g_buf, g_buf_sz);
    double t0 = now_sec(), elapsed;
    do { crc32c(g_buf, g_buf_sz); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = ((double)iters * (double)g_buf_sz) / (1024.0 * 1024.0) / elapsed;
    report("crc32c_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("crc32c_16M", "MiB/s", bench_crc32c_16m);

/* ------------------------------------------------------------------ */
/* RS parity benchmarks                                                */
/* ------------------------------------------------------------------ */

static void bench_rs_encode_single(double *tp)
{
    uint8_t data[RS_K], par[RS_2T];
    memcpy(data, g_buf, RS_K);
    rs_init();
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) rs_encode(data, par);
    double t0 = now_sec(), elapsed;
    do { rs_encode(data, par); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    *tp = (double)iters / 1e6 / elapsed;
    report("rs_encode_single", "Mops/s", *tp, iters, elapsed);
}
REGISTER_BENCH("rs_encode_single", "Mops/s", bench_rs_encode_single);

static void bench_rs_parity_encode_16m(double *tp)
{
    size_t par_sz = rs_parity_size(g_buf_sz);
    uint8_t *par = malloc(par_sz);
    if (!par) { *tp = 0; return; }
    rs_init();
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) rs_parity_encode(g_buf, g_buf_sz, par);
    double t0 = now_sec(), elapsed;
    do { rs_parity_encode(g_buf, g_buf_sz, par); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    free(par);
    *tp = ((double)iters * (double)g_buf_sz) / (1024.0 * 1024.0) / elapsed;
    report("rs_parity_encode_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("rs_parity_encode_16M", "MiB/s", bench_rs_parity_encode_16m);

static void bench_rs_parity_decode_clean_16m(double *tp)
{
    size_t par_sz = rs_parity_size(g_buf_sz);
    uint8_t *par = malloc(par_sz);
    uint8_t *data = malloc(g_buf_sz);
    if (!par || !data) { free(par); free(data); *tp = 0; return; }
    memcpy(data, g_buf, g_buf_sz);
    rs_init();
    rs_parity_encode(data, g_buf_sz, par);
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++) rs_parity_decode(data, g_buf_sz, par);
    double t0 = now_sec(), elapsed;
    do { rs_parity_decode(data, g_buf_sz, par); iters++; elapsed = now_sec() - t0; }
    while (elapsed < BENCH_MIN_SECS);
    free(par); free(data);
    *tp = ((double)iters * (double)g_buf_sz) / (1024.0 * 1024.0) / elapsed;
    report("rs_parity_decode_clean_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("rs_parity_decode_clean_16M", "MiB/s", bench_rs_parity_decode_clean_16m);

/* ------------------------------------------------------------------ */
/* LZ4 benchmarks                                                      */
/* ------------------------------------------------------------------ */

#include <lz4.h>
#include <lz4frame.h>

static void bench_lz4_compress_16m(double *tp)
{
    int bound = LZ4_compressBound((int)g_buf_sz);
    char *dst = malloc((size_t)bound);
    if (!dst) { *tp = 0; return; }
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++)
        LZ4_compress_default((const char *)g_buf, dst, (int)g_buf_sz, bound);
    double t0 = now_sec(), elapsed;
    do {
        LZ4_compress_default((const char *)g_buf, dst, (int)g_buf_sz, bound);
        iters++;
        elapsed = now_sec() - t0;
    } while (elapsed < BENCH_MIN_SECS);
    free(dst);
    *tp = ((double)iters * (double)g_buf_sz) / (1024.0 * 1024.0) / elapsed;
    report("lz4_compress_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("lz4_compress_16M", "MiB/s", bench_lz4_compress_16m);

static void bench_lz4_decompress_16m(double *tp)
{
    int bound = LZ4_compressBound((int)g_buf_sz);
    char *comp = malloc((size_t)bound);
    char *decomp = malloc(g_buf_sz);
    if (!comp || !decomp) { free(comp); free(decomp); *tp = 0; return; }
    int comp_sz = LZ4_compress_default((const char *)g_buf, comp,
                                       (int)g_buf_sz, bound);
    uint64_t iters = 0;
    for (int i = 0; i < BENCH_WARMUP; i++)
        LZ4_decompress_safe(comp, decomp, comp_sz, (int)g_buf_sz);
    double t0 = now_sec(), elapsed;
    do {
        LZ4_decompress_safe(comp, decomp, comp_sz, (int)g_buf_sz);
        iters++;
        elapsed = now_sec() - t0;
    } while (elapsed < BENCH_MIN_SECS);
    free(comp); free(decomp);
    *tp = ((double)iters * (double)g_buf_sz) / (1024.0 * 1024.0) / elapsed;
    report("lz4_decompress_16M", "MiB/s", *tp, iters, elapsed);
}
REGISTER_BENCH("lz4_decompress_16M", "MiB/s", bench_lz4_decompress_16m);

/* ------------------------------------------------------------------ */
/* Main: filter by name, run matching benchmarks                       */
/* ------------------------------------------------------------------ */

static int matches_filter(const char *name, int argc, char **argv)
{
    if (argc <= 1) return 1;  /* no filter = run all */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0) continue;
        if (strstr(name, argv[i]) != NULL) return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    /* -l flag: list and exit */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0) {
            printf("Available benchmarks:\n");
            for (int b = 0; b < g_nbench; b++)
                printf("  %s\n", g_benches[b].name);
            return 0;
        }
    }

    init_buf();

    printf("bench: %d benchmark(s) registered\n", g_nbench);
    printf("  %-36s %10s %-6s\n", "NAME", "RESULT", "UNIT");
    printf("  %-36s %10s %-6s\n",
           "------------------------------------",
           "----------", "------");

    int ran = 0;
    for (int b = 0; b < g_nbench; b++) {
        if (!matches_filter(g_benches[b].name, argc, argv))
            continue;
        double tp = 0;
        g_benches[b].fn(&tp);
        ran++;
    }

    if (ran == 0)
        printf("  (no benchmarks matched filter)\n");

    free(g_buf);
    return 0;
}
