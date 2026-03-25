#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#include "../src/parity.h"

/* ---- CRC-32C tests ---- */

static void test_crc32c_empty(void **state)
{
    (void)state;
    uint32_t c = crc32c("", 0);
    assert_int_equal(c, 0);
}

static void test_crc32c_known_vectors(void **state)
{
    (void)state;
    /* CRC-32C of "123456789" = 0xE3069283 */
    uint32_t c = crc32c("123456789", 9);
    assert_int_equal(c, 0xE3069283u);
}

static void test_crc32c_incremental(void **state)
{
    (void)state;
    const char *data = "Hello, World!";
    size_t len = strlen(data);
    uint32_t full = crc32c(data, len);

    uint32_t inc = crc32c_update(0, data, 5);
    inc = crc32c_update(inc, data + 5, len - 5);
    assert_int_equal(full, inc);
}

/* ---- XOR parity tests ---- */

static void test_xor_parity_clean(void **state)
{
    (void)state;
    /* 56-byte header (like object_header_t). */
    uint8_t hdr[56];
    for (int i = 0; i < 56; i++) hdr[i] = (uint8_t)(i * 7 + 13);

    parity_record_t rec;
    parity_record_compute(hdr, 56, &rec);
    int rc = parity_record_check(hdr, 56, &rec);
    assert_int_equal(rc, 0);
}

static void test_xor_parity_single_byte_repair(void **state)
{
    (void)state;
    uint8_t hdr[56];
    for (int i = 0; i < 56; i++) hdr[i] = (uint8_t)(i * 7 + 13);

    parity_record_t rec;
    parity_record_compute(hdr, 56, &rec);

    /* Corrupt one byte. */
    uint8_t saved = hdr[17];
    hdr[17] ^= 0xAB;
    assert_true(hdr[17] != saved);

    int rc = parity_record_check(hdr, 56, &rec);
    assert_int_equal(rc, 1);  /* Repaired. */
    assert_int_equal(hdr[17], saved);
}

static void test_xor_parity_every_position(void **state)
{
    (void)state;
    /* Test single-byte corruption at every position of a 56-byte header. */
    uint8_t orig[56];
    for (int i = 0; i < 56; i++) orig[i] = (uint8_t)(i * 3 + 99);

    parity_record_t rec;
    parity_record_compute(orig, 56, &rec);

    for (int pos = 0; pos < 56; pos++) {
        uint8_t hdr[56];
        memcpy(hdr, orig, 56);
        hdr[pos] ^= 0xFF;  /* Flip all bits in one byte. */

        int rc = parity_record_check(hdr, 56, &rec);
        assert_int_equal(rc, 1);
        assert_memory_equal(hdr, orig, 56);
    }
}

static void test_xor_parity_two_byte_uncorrectable(void **state)
{
    (void)state;
    uint8_t hdr[56];
    for (int i = 0; i < 56; i++) hdr[i] = (uint8_t)(i * 7 + 13);

    parity_record_t rec;
    parity_record_compute(hdr, 56, &rec);

    /* Corrupt two bytes in different columns → uncorrectable. */
    hdr[5] ^= 0x11;
    hdr[20] ^= 0x22;

    int rc = parity_record_check(hdr, 56, &rec);
    assert_int_equal(rc, -1);
}

/* ---- RS low-level tests ---- */

static void test_rs_no_errors(void **state)
{
    (void)state;
    rs_init();

    uint8_t data[RS_K];
    for (int i = 0; i < RS_K; i++) data[i] = (uint8_t)(i & 0xFF);

    uint8_t parity[RS_2T];
    rs_encode(data, parity);

    uint8_t cw[RS_N];
    memcpy(cw, data, RS_K);
    memcpy(cw + RS_K, parity, RS_2T);

    int rc = rs_decode(cw);
    assert_int_equal(rc, 0);
    assert_memory_equal(cw, data, RS_K);
}

static void test_rs_single_error(void **state)
{
    (void)state;
    rs_init();

    uint8_t data[RS_K];
    for (int i = 0; i < RS_K; i++) data[i] = (uint8_t)(i * 3 + 7);

    uint8_t parity[RS_2T];
    rs_encode(data, parity);

    uint8_t cw[RS_N];
    memcpy(cw, data, RS_K);
    memcpy(cw + RS_K, parity, RS_2T);

    /* Corrupt one data byte. */
    cw[42] ^= 0xAA;

    int rc = rs_decode(cw);
    assert_true(rc >= 1);
    assert_memory_equal(cw, data, RS_K);
}

static void test_rs_max_errors(void **state)
{
    (void)state;
    rs_init();

    uint8_t data[RS_K];
    for (int i = 0; i < RS_K; i++) data[i] = (uint8_t)(i ^ 0x55);

    uint8_t parity[RS_2T];
    rs_encode(data, parity);

    uint8_t cw[RS_N];
    memcpy(cw, data, RS_K);
    memcpy(cw + RS_K, parity, RS_2T);

    /* Corrupt exactly 8 bytes (the maximum correctable). */
    int positions[] = {0, 10, 50, 100, 150, 200, 230, 254};
    for (int i = 0; i < 8; i++)
        cw[positions[i]] ^= (uint8_t)(0x11 * (i + 1));

    int rc = rs_decode(cw);
    assert_true(rc >= 1);
    assert_memory_equal(cw, data, RS_K);
}

static void test_rs_too_many_errors(void **state)
{
    (void)state;
    rs_init();

    uint8_t data[RS_K];
    for (int i = 0; i < RS_K; i++) data[i] = (uint8_t)i;

    uint8_t parity[RS_2T];
    rs_encode(data, parity);

    uint8_t cw[RS_N];
    memcpy(cw, data, RS_K);
    memcpy(cw + RS_K, parity, RS_2T);

    /* Corrupt 9 bytes — beyond correction capacity. */
    for (int i = 0; i < 9; i++)
        cw[i * 25] ^= 0xFF;

    int rc = rs_decode(cw);
    assert_int_equal(rc, -1);
}

static void test_rs_parity_byte_error(void **state)
{
    (void)state;
    rs_init();

    uint8_t data[RS_K];
    for (int i = 0; i < RS_K; i++) data[i] = (uint8_t)(i + 100);

    uint8_t parity[RS_2T];
    rs_encode(data, parity);

    uint8_t cw[RS_N];
    memcpy(cw, data, RS_K);
    memcpy(cw + RS_K, parity, RS_2T);

    /* Corrupt a parity byte. */
    cw[RS_K + 3] ^= 0x77;

    int rc = rs_decode(cw);
    assert_true(rc >= 1);
    /* Data portion should still be intact after decode. */
    assert_memory_equal(cw, data, RS_K);
}

/* ---- Interleaved RS (high-level) tests ---- */

static void test_rs_parity_size(void **state)
{
    (void)state;
    assert_int_equal(rs_parity_size(0), 0);
    assert_int_equal(rs_parity_size(1), 1024);  /* 1 group */
    assert_int_equal(rs_parity_size(15296), 1024);  /* exactly 1 group */
    assert_int_equal(rs_parity_size(15297), 2048);  /* 2 groups */
}

static void test_rs_interleaved_clean(void **state)
{
    (void)state;
    size_t len = 4096;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    for (size_t i = 0; i < len; i++) data[i] = (uint8_t)(i & 0xFF);

    size_t psize = rs_parity_size(len);
    uint8_t *par = malloc(psize);
    assert_non_null(par);

    rs_parity_encode(data, len, par);
    int rc = rs_parity_decode(data, len, par);
    assert_int_equal(rc, 0);

    free(par);
    free(data);
}

static void test_rs_interleaved_single_error(void **state)
{
    (void)state;
    size_t len = 4096;
    uint8_t *data = malloc(len);
    uint8_t *orig = malloc(len);
    assert_non_null(data);
    assert_non_null(orig);
    for (size_t i = 0; i < len; i++) data[i] = (uint8_t)(i & 0xFF);
    memcpy(orig, data, len);

    size_t psize = rs_parity_size(len);
    uint8_t *par = malloc(psize);
    assert_non_null(par);

    rs_parity_encode(data, len, par);

    /* Corrupt one byte. */
    data[1000] ^= 0xCC;

    int rc = rs_parity_decode(data, len, par);
    assert_true(rc >= 1);
    assert_memory_equal(data, orig, len);

    free(par);
    free(orig);
    free(data);
}

static void test_rs_interleaved_burst_512(void **state)
{
    (void)state;
    /* 512-byte contiguous burst — the design target for sector corruption. */
    size_t len = 32768;
    uint8_t *data = malloc(len);
    uint8_t *orig = malloc(len);
    assert_non_null(data);
    assert_non_null(orig);
    for (size_t i = 0; i < len; i++) data[i] = (uint8_t)((i * 13 + 7) & 0xFF);
    memcpy(orig, data, len);

    size_t psize = rs_parity_size(len);
    uint8_t *par = malloc(psize);
    assert_non_null(par);

    rs_parity_encode(data, len, par);

    /* Corrupt a contiguous 512-byte region (simulating one bad disk sector). */
    size_t burst_start = 1024;
    for (size_t i = burst_start; i < burst_start + 512; i++)
        data[i] ^= 0xFF;

    int rc = rs_parity_decode(data, len, par);
    assert_true(rc > 0);
    assert_memory_equal(data, orig, len);

    free(par);
    free(orig);
    free(data);
}

static void test_rs_interleaved_multi_group(void **state)
{
    (void)state;
    /* Data spanning multiple interleave groups. */
    size_t len = 50000;  /* ~3.3 groups */
    uint8_t *data = malloc(len);
    uint8_t *orig = malloc(len);
    assert_non_null(data);
    assert_non_null(orig);
    for (size_t i = 0; i < len; i++) data[i] = (uint8_t)((i * 37) & 0xFF);
    memcpy(orig, data, len);

    size_t psize = rs_parity_size(len);
    uint8_t *par = malloc(psize);
    assert_non_null(par);

    rs_parity_encode(data, len, par);

    /* Corrupt scattered bytes across groups. */
    data[100] ^= 0x11;
    data[20000] ^= 0x22;
    data[40000] ^= 0x33;

    int rc = rs_parity_decode(data, len, par);
    assert_true(rc >= 3);
    assert_memory_equal(data, orig, len);

    free(par);
    free(orig);
    free(data);
}

static void test_rs_interleaved_uncorrectable(void **state)
{
    (void)state;
    /* Overload one codeword with 9 errors → uncorrectable. */
    size_t len = 4096;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    for (size_t i = 0; i < len; i++) data[i] = (uint8_t)(i & 0xFF);

    size_t psize = rs_parity_size(len);
    uint8_t *par = malloc(psize);
    assert_non_null(par);

    rs_parity_encode(data, len, par);

    /* Corrupt 9 bytes all mapping to codeword 0 (positions 0, 64, 128, ...).
     * Each of these maps to cw[0] at row 0,1,2,... — 9 errors in one codeword. */
    for (int i = 0; i < 9; i++)
        data[(size_t)i * RS_INTERLEAVE] ^= (uint8_t)(0x10 + i);

    int rc = rs_parity_decode(data, len, par);
    assert_int_equal(rc, -1);

    free(par);
    free(data);
}

static void test_rs_interleaved_small_data(void **state)
{
    (void)state;
    /* Very small data (< one codeword's share). */
    size_t len = 10;
    uint8_t data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    uint8_t orig[10];
    memcpy(orig, data, len);

    size_t psize = rs_parity_size(len);
    assert_true(psize > 0);
    uint8_t *par = malloc(psize);
    assert_non_null(par);

    rs_parity_encode(data, len, par);
    data[5] ^= 0xFF;

    int rc = rs_parity_decode(data, len, par);
    assert_true(rc >= 1);
    assert_memory_equal(data, orig, len);

    free(par);
}

/* ---- Footer structure tests ---- */

static void test_parity_footer_size(void **state)
{
    (void)state;
    assert_int_equal(sizeof(parity_footer_t), 12);
    assert_int_equal(sizeof(parity_record_t), 260);
}

/* ---- main ---- */

int main(void)
{
    const struct CMUnitTest tests[] = {
        /* CRC-32C */
        cmocka_unit_test(test_crc32c_empty),
        cmocka_unit_test(test_crc32c_known_vectors),
        cmocka_unit_test(test_crc32c_incremental),
        /* XOR parity */
        cmocka_unit_test(test_xor_parity_clean),
        cmocka_unit_test(test_xor_parity_single_byte_repair),
        cmocka_unit_test(test_xor_parity_every_position),
        cmocka_unit_test(test_xor_parity_two_byte_uncorrectable),
        /* RS low-level */
        cmocka_unit_test(test_rs_no_errors),
        cmocka_unit_test(test_rs_single_error),
        cmocka_unit_test(test_rs_max_errors),
        cmocka_unit_test(test_rs_too_many_errors),
        cmocka_unit_test(test_rs_parity_byte_error),
        /* Interleaved RS */
        cmocka_unit_test(test_rs_parity_size),
        cmocka_unit_test(test_rs_interleaved_clean),
        cmocka_unit_test(test_rs_interleaved_single_error),
        cmocka_unit_test(test_rs_interleaved_burst_512),
        cmocka_unit_test(test_rs_interleaved_multi_group),
        cmocka_unit_test(test_rs_interleaved_uncorrectable),
        cmocka_unit_test(test_rs_interleaved_small_data),
        /* Footer struct */
        cmocka_unit_test(test_parity_footer_size),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
