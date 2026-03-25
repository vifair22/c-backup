#pragma once

#include <stdint.h>
#include <stddef.h>

/* ---- CRC-32C ---- */

uint32_t crc32c(const void *data, size_t len);
uint32_t crc32c_update(uint32_t crc, const void *data, size_t len);

/* ---- XOR interleaved parity (headers, stride=256) ----
 *
 * For data up to 256 bytes (all on-disk headers), each stride column
 * contains exactly one data byte, guaranteeing single-byte correction.
 */

#define PARITY_STRIDE 256

typedef struct {
    uint32_t crc;
    uint8_t  parity[PARITY_STRIDE];
} __attribute__((packed)) parity_record_t;  /* 260 bytes */

void parity_record_compute(const void *data, size_t len, parity_record_t *out);

/* Returns: 0=OK, 1=repaired, -1=uncorrectable */
int  parity_record_check(void *data, size_t len, const parity_record_t *rec);

/* ---- Reed-Solomon RS(255,239) in GF(2^8) ---- */

#define RS_N          255   /* codeword length */
#define RS_K          239   /* data bytes per codeword */
#define RS_2T         16    /* parity bytes per codeword (corrects t=8) */
#define RS_INTERLEAVE 64    /* interleave depth — burst correction = 64×8 = 512 bytes */

/* One-time GF(2^8) table + generator polynomial setup.
 * Safe to call multiple times (idempotent). */
void rs_init(void);

/* Encode RS_K data bytes, produce RS_2T parity bytes. */
void rs_encode(const uint8_t data[RS_K], uint8_t parity_out[RS_2T]);

/* Decode a full RS_N codeword in-place.
 * Returns number of corrected bytes, or -1 if uncorrectable. */
int  rs_decode(uint8_t codeword[RS_N]);

/* ---- High-level interleaved payload parity ----
 *
 * Data is split into interleave groups of RS_K × RS_INTERLEAVE = 15296 bytes.
 * Each group produces RS_INTERLEAVE × RS_2T = 1024 parity bytes.
 * A contiguous 512-byte burst on disk spans at most 8 bytes in any single
 * codeword (512/64 = 8), which is exactly RS(255,239) correction capacity.
 */

/* Returns parity byte count for a given data length. */
size_t rs_parity_size(size_t data_len);

/* Encode parity for data. parity_out must be rs_parity_size(len) bytes. */
void   rs_parity_encode(const void *data, size_t len, void *parity_out);

/* Decode/repair data using its parity block.
 * Returns: 0=clean, >0=bytes corrected, -1=uncorrectable */
int    rs_parity_decode(void *data, size_t len, const void *parity);

/* ---- Global parity statistics (atomic, process-wide) ---- */

typedef struct {
    uint64_t repaired;       /* bytes/headers successfully corrected */
    uint64_t uncorrectable;  /* corruption detected but not fixable */
} parity_stats_t;

/* Get current stats snapshot. */
parity_stats_t parity_stats_get(void);

/* Reset counters to zero. */
void parity_stats_reset(void);

/* Increment counters (called internally by parity_record_check / rs_parity_decode). */
void parity_stats_add_repaired(uint64_t n);
void parity_stats_add_uncorrectable(uint64_t n);

/* ---- On-disk parity footer ---- */

#define PARITY_FOOTER_MAGIC  0x50415249u  /* "PARI" */
#define PARITY_VERSION       1u

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t trailer_size;  /* total trailer INCLUDING this footer */
} __attribute__((packed)) parity_footer_t;  /* 12 bytes */
