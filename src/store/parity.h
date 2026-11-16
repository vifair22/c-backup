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
/*
 * Repair data in place using parity. Returns bytes corrected, 0 if clean,
 * -1 if uncorrectable or if parity_len is too short to cover len.
 *
 * parity_len is not redundant with rs_parity_size(len). Inside a pack, the
 * payload length comes from the entry header and the parity length comes
 * from the trailer's block boundaries, and those are separate parts of the
 * file that a single bad sector can put out of step. When they disagreed,
 * this function walked rs_parity_size(len) bytes of a buffer that was
 * shorter than that - a heap overread reachable from any corrupt pack, and
 * one that no caller could have prevented without being told the length.
 */
int    rs_parity_decode(void *data, size_t len, const void *parity,
                        size_t parity_len);

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

/* ---- Deprecated trailer field: rs_data_len ----
 *
 * Every parity trailer historically contained a 4-byte `rs_data_len` field
 * between the payload CRC and the footer.  It was intended to let a
 * standalone verifier RS-decode a payload without trusting the entry/object
 * header for its length, as part of a header-corruption recovery story.
 *
 * That recovery path was never built: rs_data_len alone is insufficient
 * (you also need the compression codec, uncompressed size, type, and
 * expected hash — none of which are in the trailer), and every existing
 * reader trusts the header for the length anyway.  Meanwhile the field
 * capped per-entry payloads at 4 GiB because it was only 32 bits wide.
 *
 * The field is now officially dead weight: writers must store 0, readers
 * must ignore any value they find there.  The 4 bytes are retained on disk
 * so the trailer layout is unchanged and old repositories remain readable
 * without a format bump.  If header-corruption recovery is ever revisited,
 * the right design is to duplicate the entire header in the trailer, not
 * to resurrect this single field.
 */
#define PARITY_RS_DATA_LEN_DEPRECATED 0u

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t trailer_size;  /* total trailer INCLUDING this footer */
} __attribute__((packed)) parity_footer_t;  /* 12 bytes */

/* ---- 32-bit parity size fields (OWN-30 stopgap) ----
 *
 * The footer's trailer_size and a pack entry's parity block size are both
 * uint32. RS parity costs 1024 bytes per 15296 stored bytes, so past about
 * 59.75 GiB of stored data the size written into those fields wraps. Reads
 * are unaffected - they locate parity from the header - but every repair
 * path finds the trailer through trailer_size, and with a wrapped value it
 * reads parity from the wrong place and can write a wrong "correction" into
 * user data. Until the format grows wider fields, repair refuses anything
 * whose fields may have wrapped, and writers say so when they produce one.
 *
 * The exact limit: a trailer of rs_parity_size(n) plus its fixed bytes must
 * fit in a uint32. The fixed bytes are 280 for a loose object, 272 for one
 * pack entry block and 300 for a single-entry pack's whole trailer. All of
 * them sit inside the same 1024-byte remainder (UINT32_MAX % 1024 == 1023),
 * so they share one threshold: at most UINT32_MAX / 1024 = 4194303 parity
 * groups, which is 4194303 * 15296 = 64,156,058,688 stored bytes. One byte
 * more needs a 4194304th group and wraps every one of those fields.
 */
#define PARITY_GROUP_DATA       ((uint64_t)RS_K * RS_INTERLEAVE)   /* 15296 */
#define PARITY_GROUP_PAR        ((uint64_t)RS_2T * RS_INTERLEAVE)  /* 1024 */
#define PARITY_TRAILER_FIXED_MAX 300u  /* largest fixed overhead of any trailer */
#define PARITY_FIELDS_MAX_STORED \
    (((uint64_t)UINT32_MAX / PARITY_GROUP_PAR) * PARITY_GROUP_DATA)

_Static_assert(PARITY_TRAILER_FIXED_MAX <= (uint64_t)UINT32_MAX % PARITY_GROUP_PAR,
               "every trailer layout must share one group-count threshold");
_Static_assert(PARITY_FIELDS_MAX_STORED == 64156058688ull,
               "the documented threshold is the computed one");

/* What a refused repair says, so the reason reads the same everywhere. */
#define PARITY_TOO_LARGE_REASON \
    "too large for 32-bit parity fields; repair disabled pending format fix"

/* Returned by object_repair and pack_object_repair when they refuse. */
#define PARITY_REPAIR_REFUSED_TOO_LARGE (-2)

/* The largest trailer the size fields can carry: UINT32_MAX, unless a test
 * has lowered it. */
uint64_t parity_trailer_limit(void);

/* 1 when an object of stored_size bytes has a trailer whose size fields
 * cannot wrap, 0 when its parity must not be used for repair. */
int parity_fields_fit(uint64_t stored_size);

/*
 * 1 when a file holding `entries` parity-protected payloads cannot have a
 * trailer larger than parity_trailer_limit(). The file is assumed to be
 *
 *   body_fixed + entries * body_per_entry + payload bytes      (body)
 *   + trailer_fixed + entries * trailer_per_entry + RS parity  (trailer)
 *
 * and only its size is trusted, never a header's length, because the header
 * is exactly what a repair may be about to rewrite. The bound is exact for a
 * single entry and conservative by at most entries - 1 groups otherwise.
 */
int parity_trailer_fits_file(uint64_t file_size, uint64_t entries,
                             uint64_t body_fixed, uint64_t body_per_entry,
                             uint64_t trailer_fixed, uint64_t trailer_per_entry);

/* The bound itself: the largest trailer such a file can hold. Exposed so
 * tests can pin it against real trailers. */
uint64_t parity_trailer_bound(uint64_t file_size, uint64_t entries,
                              uint64_t body_fixed, uint64_t body_per_entry,
                              uint64_t trailer_fixed, uint64_t trailer_per_entry);

/* Test hook: treat any object stored in more groups than max_stored needs
 * as too large, so a few-MiB object exercises the refusal. 0 restores the
 * real limit. Never called outside tests. */
void parity_fields_set_test_limit(uint64_t max_stored);
