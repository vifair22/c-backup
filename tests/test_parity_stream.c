/*
 * Unit tests for rs_parity_stream_t — bounded-RAM streaming RS parity.
 *
 * Verifies that streaming parity output is byte-identical to
 * rs_parity_encode() on the same input across various sizes,
 * including data that forces spill-to-disk.
 */
#define _POSIX_C_SOURCE 200809L

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include "parity.h"
#include "parity_stream.h"

#define TMP_DIR  "/tmp/c_backup_ps_test"

/* RS group constants for readability */
#define GROUP_DATA  ((size_t)(RS_K * RS_INTERLEAVE))   /* 15296 */
#define GROUP_PAR   ((size_t)(RS_2T * RS_INTERLEAVE))  /* 1024  */

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static void ensure_tmp_dir(void)
{
    int rc = system("mkdir -p " TMP_DIR);
    (void)rc;
}

static void cleanup_tmp_dir(void)
{
    int rc = system("rm -rf " TMP_DIR);
    (void)rc;
}

/*
 * Generate deterministic test data.  Uses a simple LCG so different
 * offsets produce different byte patterns.
 */
static void fill_pattern(uint8_t *buf, size_t len, uint32_t seed)
{
    for (size_t i = 0; i < len; i++) {
        seed = seed * 1103515245u + 12345u;
        buf[i] = (uint8_t)(seed >> 16);
    }
}

/*
 * Compare streaming parity against reference rs_parity_encode().
 * Feeds data to the stream in chunks of feed_chunk_size.
 */
static void verify_stream_vs_reference(size_t data_len,
                                       size_t mem_cap,
                                       size_t feed_chunk_size)
{
    if (data_len == 0) {
        /* Special case: zero-length input should produce zero parity */
        rs_parity_stream_t ps;
        assert_int_equal(rs_parity_stream_init(&ps, mem_cap, TMP_DIR), 0);
        rs_parity_stream_finish(&ps);
        assert_int_equal(rs_parity_stream_total(&ps), 0);
        rs_parity_stream_destroy(&ps);
        return;
    }

    /* Generate test data */
    uint8_t *data = malloc(data_len);
    assert_non_null(data);
    fill_pattern(data, data_len, (uint32_t)data_len);

    /* Reference: one-shot rs_parity_encode */
    size_t ref_par_sz = rs_parity_size(data_len);
    uint8_t *ref_par = malloc(ref_par_sz);
    assert_non_null(ref_par);
    rs_parity_encode(data, data_len, ref_par);

    /* Streaming path */
    rs_parity_stream_t ps;
    assert_int_equal(rs_parity_stream_init(&ps, mem_cap, TMP_DIR), 0);

    size_t off = 0;
    while (off < data_len) {
        size_t chunk = data_len - off;
        if (chunk > feed_chunk_size) chunk = feed_chunk_size;
        rs_parity_stream_feed(&ps, data + off, chunk);
        off += chunk;
    }
    rs_parity_stream_finish(&ps);

    assert_int_equal(rs_parity_stream_total(&ps), ref_par_sz);

    /* Replay to a temp file and compare */
    char replay_path[] = TMP_DIR "/replay.XXXXXX";
    int rfd = mkstemp(replay_path);
    assert_true(rfd >= 0);
    unlink(replay_path);

    assert_int_equal(rs_parity_stream_replay_fd(&ps, rfd), 0);

    /* Read back and compare */
    assert_true(lseek(rfd, 0, SEEK_SET) == 0);
    uint8_t *got_par = malloc(ref_par_sz);
    assert_non_null(got_par);

    size_t total_read = 0;
    while (total_read < ref_par_sz) {
        ssize_t r = read(rfd, got_par + total_read, ref_par_sz - total_read);
        assert_true(r > 0);
        total_read += (size_t)r;
    }

    assert_memory_equal(got_par, ref_par, ref_par_sz);

    close(rfd);
    free(got_par);
    rs_parity_stream_destroy(&ps);
    free(ref_par);
    free(data);
}

/*
 * Same but test replay_file path.
 */
static void verify_stream_replay_file(size_t data_len,
                                      size_t mem_cap,
                                      size_t feed_chunk_size)
{
    if (data_len == 0) return;

    uint8_t *data = malloc(data_len);
    assert_non_null(data);
    fill_pattern(data, data_len, (uint32_t)(data_len + 1));

    size_t ref_par_sz = rs_parity_size(data_len);
    uint8_t *ref_par = malloc(ref_par_sz);
    assert_non_null(ref_par);
    rs_parity_encode(data, data_len, ref_par);

    rs_parity_stream_t ps;
    assert_int_equal(rs_parity_stream_init(&ps, mem_cap, TMP_DIR), 0);

    size_t off = 0;
    while (off < data_len) {
        size_t chunk = data_len - off;
        if (chunk > feed_chunk_size) chunk = feed_chunk_size;
        rs_parity_stream_feed(&ps, data + off, chunk);
        off += chunk;
    }
    rs_parity_stream_finish(&ps);

    /* Replay via FILE* */
    char replay_path[] = TMP_DIR "/replay_f.XXXXXX";
    int tmpfd = mkstemp(replay_path);
    assert_true(tmpfd >= 0);
    FILE *f = fdopen(tmpfd, "w+b");
    assert_non_null(f);

    assert_int_equal(rs_parity_stream_replay_file(&ps, f), 0);
    fflush(f);

    /* Read back and compare */
    fseek(f, 0, SEEK_SET);
    uint8_t *got_par = malloc(ref_par_sz);
    assert_non_null(got_par);
    assert_int_equal(fread(got_par, 1, ref_par_sz, f), ref_par_sz);

    assert_memory_equal(got_par, ref_par, ref_par_sz);

    fclose(f);
    unlink(replay_path);
    free(got_par);
    rs_parity_stream_destroy(&ps);
    free(ref_par);
    free(data);
}

/* ------------------------------------------------------------------ */
/* Test cases                                                          */
/* ------------------------------------------------------------------ */

/* Zero-length input */
static void test_zero_length(void **state)
{
    (void)state;
    verify_stream_vs_reference(0, 1024 * 1024, 4096);
}

/* One byte */
static void test_one_byte(void **state)
{
    (void)state;
    verify_stream_vs_reference(1, 1024 * 1024, 4096);
}

/* Exactly one RS group (15296 bytes) */
static void test_exact_one_group(void **state)
{
    (void)state;
    verify_stream_vs_reference(GROUP_DATA, 1024 * 1024, 4096);
}

/* One group + 1 byte (partial second group) */
static void test_one_group_plus_one(void **state)
{
    (void)state;
    verify_stream_vs_reference(GROUP_DATA + 1, 1024 * 1024, 4096);
}

/* Multiple groups, no spill, feed in small chunks */
static void test_multi_group_small_chunks(void **state)
{
    (void)state;
    /* 10 groups = 152960 bytes, parity = 10240 bytes.  mem_cap 1 MiB, no spill. */
    verify_stream_vs_reference(GROUP_DATA * 10, 1024 * 1024, 1000);
}

/* Multiple groups, no spill, feed in large chunks */
static void test_multi_group_large_chunks(void **state)
{
    (void)state;
    verify_stream_vs_reference(GROUP_DATA * 10, 1024 * 1024, 100000);
}

/* Feed entire data in one call */
static void test_single_feed(void **state)
{
    (void)state;
    verify_stream_vs_reference(GROUP_DATA * 5, 1024 * 1024, GROUP_DATA * 5);
}

/* Force spill: tiny mem_cap, enough data to overflow */
static void test_spill_tiny_cap(void **state)
{
    (void)state;
    /* mem_cap = 1 group parity (1024).  Data = 50 groups → 50 KiB parity.
     * Will spill ~49 times. */
    verify_stream_vs_reference(GROUP_DATA * 50, GROUP_PAR, 4096);
}

/* Force spill: moderate data, cap smaller than total parity */
static void test_spill_moderate(void **state)
{
    (void)state;
    /* 4 MiB data → ~268 groups → ~268 KiB parity.  Cap at 64 KiB. */
    size_t data_len = 4 * 1024 * 1024;
    verify_stream_vs_reference(data_len, 64 * 1024, 65536);
}

/* Larger test: 32 MiB data, 16 KiB cap (extreme spill) */
static void test_spill_large_data(void **state)
{
    (void)state;
    size_t data_len = 32 * 1024 * 1024;
    verify_stream_vs_reference(data_len, 16 * 1024, 128 * 1024);
}

/* Test replay_file path */
static void test_replay_file_small(void **state)
{
    (void)state;
    verify_stream_replay_file(GROUP_DATA * 5, 1024 * 1024, 4096);
}

/* Test replay_file with spill */
static void test_replay_file_spill(void **state)
{
    (void)state;
    verify_stream_replay_file(GROUP_DATA * 50, GROUP_PAR, 8192);
}

/* Destroy without finish — should not crash */
static void test_destroy_without_finish(void **state)
{
    (void)state;
    rs_parity_stream_t ps;
    assert_int_equal(rs_parity_stream_init(&ps, 1024 * 1024, TMP_DIR), 0);

    uint8_t buf[1000];
    fill_pattern(buf, sizeof(buf), 42);
    rs_parity_stream_feed(&ps, buf, sizeof(buf));

    /* Destroy without calling finish — must not leak or crash */
    rs_parity_stream_destroy(&ps);
}

/* Double finish — should be idempotent */
static void test_double_finish(void **state)
{
    (void)state;
    rs_parity_stream_t ps;
    assert_int_equal(rs_parity_stream_init(&ps, 1024 * 1024, TMP_DIR), 0);

    uint8_t buf[GROUP_DATA];
    fill_pattern(buf, sizeof(buf), 99);
    rs_parity_stream_feed(&ps, buf, sizeof(buf));

    rs_parity_stream_finish(&ps);
    size_t t1 = rs_parity_stream_total(&ps);
    rs_parity_stream_finish(&ps);
    size_t t2 = rs_parity_stream_total(&ps);
    assert_int_equal(t1, t2);

    rs_parity_stream_destroy(&ps);
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(void)
{
    ensure_tmp_dir();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_zero_length),
        cmocka_unit_test(test_one_byte),
        cmocka_unit_test(test_exact_one_group),
        cmocka_unit_test(test_one_group_plus_one),
        cmocka_unit_test(test_multi_group_small_chunks),
        cmocka_unit_test(test_multi_group_large_chunks),
        cmocka_unit_test(test_single_feed),
        cmocka_unit_test(test_spill_tiny_cap),
        cmocka_unit_test(test_spill_moderate),
        cmocka_unit_test(test_spill_large_data),
        cmocka_unit_test(test_replay_file_small),
        cmocka_unit_test(test_replay_file_spill),
        cmocka_unit_test(test_destroy_without_finish),
        cmocka_unit_test(test_double_finish),
    };

    int rc = cmocka_run_group_tests(tests, NULL, NULL);
    cleanup_tmp_dir();
    return rc;
}
