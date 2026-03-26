#include "parity.h"

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#if defined(__x86_64__) && defined(__SSE4_2__)
#include <immintrin.h>
#define CRC32C_HW 1
#define RS_SIMD    1
#endif

/* ========================================================================
 * Global parity statistics (atomic counters)
 * ======================================================================== */

static atomic_uint_fast64_t g_parity_repaired;
static atomic_uint_fast64_t g_parity_uncorrectable;

parity_stats_t parity_stats_get(void)
{
    parity_stats_t s;
    s.repaired      = atomic_load(&g_parity_repaired);
    s.uncorrectable = atomic_load(&g_parity_uncorrectable);
    return s;
}

void parity_stats_reset(void)
{
    atomic_store(&g_parity_repaired, 0);
    atomic_store(&g_parity_uncorrectable, 0);
}

void parity_stats_add_repaired(uint64_t n)
{
    atomic_fetch_add(&g_parity_repaired, n);
}

void parity_stats_add_uncorrectable(uint64_t n)
{
    atomic_fetch_add(&g_parity_uncorrectable, n);
}

/* ========================================================================
 * CRC-32C (Castagnoli) — software lookup table
 * Polynomial: 0x1EDC6F41
 * ======================================================================== */

#if !CRC32C_HW
static uint32_t crc32c_table[256];
static int crc32c_table_ready;

static void crc32c_init_table(void)
{
    if (crc32c_table_ready) return;
    for (unsigned i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c >> 1) ^ ((c & 1) ? 0x82F63B78u : 0);
        crc32c_table[i] = c;
    }
    crc32c_table_ready = 1;
}
#endif

uint32_t crc32c_update(uint32_t crc, const void *data, size_t len)
{
#if CRC32C_HW
    /* Hardware-accelerated CRC32C via SSE4.2 crc32 instruction.
     * Processes 8 bytes at a time (~20 GB/s vs ~500 MiB/s software). */
    const uint8_t *p = (const uint8_t *)data;
    uint64_t c = ~(uint64_t)crc;

    /* Align to 8 bytes */
    while (len > 0 && ((uintptr_t)p & 7)) {
        c = _mm_crc32_u8((uint32_t)c, *p++);
        len--;
    }

    /* Process 8 bytes at a time */
    while (len >= 8) {
        uint64_t val;
        memcpy(&val, p, 8);
        c = _mm_crc32_u64(c, val);
        p += 8;
        len -= 8;
    }

    /* Remaining bytes */
    while (len > 0) {
        c = _mm_crc32_u8((uint32_t)c, *p++);
        len--;
    }

    return ~(uint32_t)c;
#else
    crc32c_init_table();
    const uint8_t *p = (const uint8_t *)data;
    crc = ~crc;
    for (size_t i = 0; i < len; i++)
        crc = crc32c_table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
#endif
}

uint32_t crc32c(const void *data, size_t len)
{
    return crc32c_update(0, data, len);
}

/* ========================================================================
 * XOR Interleaved Parity (stride = 256)
 *
 * For data up to 256 bytes, each stride position has exactly one
 * contributing byte, enabling guaranteed single-byte correction.
 *
 * The parity record stores:
 *   - CRC-32C of the original data (for detection)
 *   - 256-byte XOR parity array
 * ======================================================================== */

void parity_record_compute(const void *data, size_t len, parity_record_t *out)
{
    const uint8_t *d = (const uint8_t *)data;
    memset(out, 0, sizeof(*out));
    out->crc = crc32c(data, len);
    for (size_t i = 0; i < len; i++)
        out->parity[i % PARITY_STRIDE] ^= d[i];
}

int parity_record_check(void *data, size_t len, const parity_record_t *rec)
{
    uint8_t *d = (uint8_t *)data;

    /* Fast path: CRC matches → data is good. */
    uint32_t cur = crc32c(data, len);
    if (cur == rec->crc)
        return 0;

    /* Recompute parity to find the corrupted column. */
    uint8_t recomputed[PARITY_STRIDE];
    memset(recomputed, 0, sizeof(recomputed));
    for (size_t i = 0; i < len; i++)
        recomputed[i % PARITY_STRIDE] ^= d[i];

    /* XOR with stored parity gives syndrome per column. */
    uint8_t syndrome[PARITY_STRIDE];
    int bad_cols = 0;
    int last_bad = -1;
    for (int i = 0; i < PARITY_STRIDE; i++) {
        syndrome[i] = recomputed[i] ^ rec->parity[i];
        if (syndrome[i] != 0) {
            bad_cols++;
            last_bad = i;
        }
    }

    if (bad_cols == 0)
        return -1;  /* CRC mismatch but parity clean — multi-byte in same column */

    if (bad_cols == 1 && len <= PARITY_STRIDE) {
        /* Single column, one byte per column → exact correction. */
        d[last_bad] ^= syndrome[last_bad];
        /* Verify the repair. */
        if (crc32c(d, len) == rec->crc)
            return 1;
        /* Repair failed — revert. */
        d[last_bad] ^= syndrome[last_bad];
        return -1;
    }

    /* For data > PARITY_STRIDE: multiple bytes share a column.
     * A single bad column could be any byte in that column — try each. */
    if (bad_cols == 1) {
        size_t col = (size_t)last_bad;
        for (size_t off = col; off < len; off += PARITY_STRIDE) {
            d[off] ^= syndrome[col];
            if (crc32c(d, len) == rec->crc)
                return 1;
            d[off] ^= syndrome[col];
        }
        return -1;
    }

    return -1;  /* Multiple bad columns → uncorrectable by XOR parity. */
}

/* ========================================================================
 * GF(2^8) Arithmetic
 *
 * Field polynomial: x^8 + x^4 + x^3 + x^2 + 1 (0x11D)
 * Primitive element: α = 2
 * ======================================================================== */

#define GF_POLY 0x11D  /* x^8 + x^4 + x^3 + x^2 + 1 */

static uint8_t gf_exp[512];  /* α^i for i=0..510 (doubled to avoid modular reduce) */
static uint8_t gf_log[256];  /* log_α(i) for i=1..255; gf_log[0] unused */
static int gf_ready;
static pthread_once_t rs_once = PTHREAD_ONCE_INIT;

static void gf_init(void)
{
    unsigned x = 1;
    for (int i = 0; i < 255; i++) {
        gf_exp[i] = (uint8_t)x;
        gf_log[x] = (uint8_t)i;
        x <<= 1;
        if (x & 0x100)
            x ^= GF_POLY;
    }
    /* Extend exp table for easy modular access. */
    for (int i = 255; i < 512; i++)
        gf_exp[i] = gf_exp[i - 255];
    gf_ready = 1;
}

static inline uint8_t gf_mul(uint8_t a, uint8_t b)
{
    if (a == 0 || b == 0) return 0;
    return gf_exp[gf_log[a] + gf_log[b]];
}

static inline uint8_t gf_inv(uint8_t a)
{
    /* a must be non-zero. */
    return gf_exp[255 - gf_log[a]];
}

/* gf_pow is not currently used but may be needed for future parity operations.
static inline uint8_t gf_pow(uint8_t a, int n)
{
    if (a == 0) return 0;
    return gf_exp[(gf_log[a] * n) % 255];
}
*/

/* ========================================================================
 * RS(255,239) Encoder — systematic encoding
 *
 * Generator polynomial: g(x) = ∏(x - α^i) for i=0..15
 * The 16 parity bytes are the remainder of data(x) * x^16 mod g(x).
 * ======================================================================== */

static uint8_t rs_gen[RS_2T + 1];  /* generator polynomial coefficients */

/* Precomputed multiplication table: gen_mul[j][x] = gf_mul(rs_gen[j], x).
 * Replaces 3 table lookups + branch per GF multiply with a single lookup.
 * 16 × 256 = 4 KiB — fits entirely in L1 cache. */
static uint8_t rs_gen_mul[RS_2T][256];

#if RS_SIMD
/* SIMD vector lookup table: rs_gen_fb_vec[f] is a 128-bit vector where
 * byte j = gf_mul(rs_gen[j], f).  One 16-byte load replaces the entire
 * 16-element inner loop of the LFSR.  256 × 16 = 4 KiB, fits in L1. */
static __m128i rs_gen_fb_vec[256] __attribute__((aligned(16)));

/* Split-nibble tables for AVX2 batch encoding.  For each generator
 * coefficient j, gen_lo[j] and gen_hi[j] are 256-bit vpshufb tables:
 *   gen_lo[j][nibble] = gf_mul(rs_gen[j], nibble)        (nibble 0..15)
 *   gen_hi[j][nibble] = gf_mul(rs_gen[j], nibble << 4)   (nibble 0..15)
 * Broadcast to both 128-bit lanes so vpshufb works across the full __m256i.
 * 16 × 2 × 32 = 1 KiB. */
#ifdef __AVX2__
static __m256i rs_gen_lo[RS_2T] __attribute__((aligned(32)));
static __m256i rs_gen_hi[RS_2T] __attribute__((aligned(32)));
#endif
#endif

static int rs_gen_ready;

static void rs_gen_build(void)
{
    if (rs_gen_ready) return;

    /* Start with g(x) = 1. */
    memset(rs_gen, 0, sizeof(rs_gen));
    rs_gen[0] = 1;
    int deg = 0;

    for (int i = 0; i < RS_2T; i++) {
        /* Multiply g(x) by (x - α^i). */
        uint8_t root = gf_exp[i];  /* α^i */
        for (int j = deg + 1; j > 0; j--)
            rs_gen[j] = rs_gen[j - 1] ^ gf_mul(rs_gen[j], root);
        rs_gen[0] = gf_mul(rs_gen[0], root);
        deg++;
    }

    /* Build the multiplication lookup table */
    for (int j = 0; j < RS_2T; j++)
        for (int x = 0; x < 256; x++)
            rs_gen_mul[j][x] = gf_mul(rs_gen[j], (uint8_t)x);

#if RS_SIMD
    /* Build SIMD vector table: rs_gen_fb_vec[f][j] = gf_mul(rs_gen[j], f) */
    for (int f = 0; f < 256; f++) {
        uint8_t v[16];
        for (int j = 0; j < RS_2T; j++)
            v[j] = rs_gen_mul[j][f];
        rs_gen_fb_vec[f] = _mm_loadu_si128((const __m128i *)v);
    }

#ifdef __AVX2__
    /* Build split-nibble tables for AVX2 batch encoding */
    for (int j = 0; j < RS_2T; j++) {
        uint8_t lo[16], hi[16];
        for (int n = 0; n < 16; n++) {
            lo[n] = gf_mul(rs_gen[j], (uint8_t)n);
            hi[n] = gf_mul(rs_gen[j], (uint8_t)(n << 4));
        }
        __m128i lo128 = _mm_loadu_si128((const __m128i *)lo);
        __m128i hi128 = _mm_loadu_si128((const __m128i *)hi);
        rs_gen_lo[j] = _mm256_broadcastsi128_si256(lo128);
        rs_gen_hi[j] = _mm256_broadcastsi128_si256(hi128);
    }
#endif
#endif

    rs_gen_ready = 1;
}

static void rs_init_internal(void)
{
    gf_init();
    rs_gen_build();
}

void rs_init(void)
{
    pthread_once(&rs_once, rs_init_internal);
}

void rs_encode(const uint8_t data[RS_K], uint8_t parity_out[RS_2T])
{
    rs_init();

#if RS_SIMD
    /* SIMD LFSR: the entire 16-element inner loop becomes 3 instructions:
     *   1. Extract byte 15 (feedback high byte)
     *   2. Load precomputed gen*feedback vector (single 16-byte load)
     *   3. Byte-shift register left by 1, XOR with gen*feedback
     *
     * This replaces 16 scalar GF multiplies + 16 XORs per data byte
     * with one vector shift + one vector XOR + one table load.
     * ~20× speedup per codeword. */
    __m128i reg = _mm_setzero_si128();

    for (int i = 0; i < RS_K; i++) {
        uint8_t feedback = data[i] ^ (uint8_t)_mm_extract_epi8(reg, 15);
        __m128i gen_fb = rs_gen_fb_vec[feedback];
        reg = _mm_xor_si128(_mm_slli_si128(reg, 1), gen_fb);
    }

    /* Reverse byte order for output: parity_out[0] = reg[15], etc. */
    __m128i rev = _mm_set_epi8(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);
    _mm_storeu_si128((__m128i *)parity_out, _mm_shuffle_epi8(reg, rev));
#else
    /* Scalar fallback */
    uint8_t reg[RS_2T];
    memset(reg, 0, sizeof(reg));

    for (int i = 0; i < RS_K; i++) {
        uint8_t feedback = data[i] ^ reg[RS_2T - 1];
        for (int j = RS_2T - 1; j > 0; j--)
            reg[j] = reg[j - 1] ^ rs_gen_mul[j][feedback];
        reg[0] = rs_gen_mul[0][feedback];
    }

    for (int i = 0; i < RS_2T; i++)
        parity_out[i] = reg[RS_2T - 1 - i];
#endif
}

/* ========================================================================
 * RS(255,239) Decoder
 *
 * Steps:
 *   1. Syndrome computation
 *   2. Berlekamp-Massey → error locator polynomial
 *   3. Chien search → error positions
 *   4. Forney algorithm → error magnitudes
 * ======================================================================== */

int rs_decode(uint8_t codeword[RS_N])
{
    rs_init();

    /* Convention matching the encoder:
     *   cw[0] = coefficient of x^{N-1} (highest power)
     *   cw[j] = coefficient of x^{N-1-j}
     *   cw[254] = coefficient of x^0
     *
     * Syndromes via Horner: S_i = Σ cw[j] * α^{i*(N-1-j)}
     * Error at cw[j] has polynomial position p = N-1-j = 254-j
     * Error locator: X = α^p = α^{254-j}
     * X^{-1} = α^{j-254} = α^{(j+1) mod 255}
     */

    /* 1. Syndrome computation (Horner — matches encoder polynomial order) */
    uint8_t syn[RS_2T];
    int has_error = 0;
    for (int i = 0; i < RS_2T; i++) {
        uint8_t s = 0;
        for (int j = 0; j < RS_N; j++)
            s = gf_mul(s, gf_exp[i]) ^ codeword[j];
        syn[i] = s;
        if (s != 0) has_error = 1;
    }

    if (!has_error)
        return 0;

    /* 2. Berlekamp-Massey */
    uint8_t sigma[RS_2T + 1];
    uint8_t B[RS_2T + 1];
    uint8_t T[RS_2T + 1];
    memset(sigma, 0, sizeof(sigma));
    memset(B, 0, sizeof(B));
    sigma[0] = 1;
    B[0] = 1;
    int L = 0;
    uint8_t b = 1;

    for (int n = 0; n < RS_2T; n++) {
        uint8_t delta = syn[n];
        for (int i = 1; i <= L; i++)
            delta ^= gf_mul(sigma[i], syn[n - i]);

        if (delta == 0) {
            memmove(B + 1, B, RS_2T);
            B[0] = 0;
        } else if (2 * L <= n) {
            memcpy(T, sigma, sizeof(T));
            uint8_t coeff = gf_mul(delta, gf_inv(b));
            for (int i = RS_2T; i >= 1; i--)
                sigma[i] ^= gf_mul(coeff, B[i - 1]);
            memcpy(B, T, sizeof(B));
            L = n + 1 - L;
            b = delta;
        } else {
            uint8_t coeff = gf_mul(delta, gf_inv(b));
            for (int i = RS_2T; i >= 1; i--)
                sigma[i] ^= gf_mul(coeff, B[i - 1]);
            memmove(B + 1, B, RS_2T);
            B[0] = 0;
        }
    }

    if (L > RS_2T / 2)
        return -1;

    /* 3. Chien search
     *
     * We need σ(X^{-1}) = 0 where X = α^{254-j} for cw index j.
     * So X^{-1} = α^{(j+1) mod 255}.
     * Try m = 0..254: if σ(α^m) = 0, then (j+1) ≡ m (mod 255),
     * so j = (m + 254) mod 255. */
    int err_pos[RS_2T / 2];
    int nerr = 0;
    for (int m = 0; m < RS_N; m++) {
        uint8_t xi = gf_exp[m];
        uint8_t val = 0;
        uint8_t xi_pow = 1;
        for (int i = 0; i <= L; i++) {
            val ^= gf_mul(sigma[i], xi_pow);
            xi_pow = gf_mul(xi_pow, xi);
        }
        if (val == 0) {
            int j = (m + 254) % 255;  /* codeword array index */
            if (nerr >= RS_2T / 2)
                return -1;
            err_pos[nerr++] = j;
        }
    }

    if (nerr != L)
        return -1;

    /* 4. Forney algorithm
     *
     * Ω(x) = S(x) · σ(x) mod x^{2t}
     * e = X · Ω(X^{-1}) / σ'(X^{-1})   (fcr=0, GF(2) so -1=1)
     * where X = α^{254-j}, X^{-1} = α^{(j+1) mod 255}
     */
    uint8_t omega[RS_2T];
    memset(omega, 0, sizeof(omega));
    for (int i = 0; i < RS_2T; i++) {
        for (int j = 0; j <= i && j <= L; j++)
            omega[i] ^= gf_mul(syn[i - j], sigma[j]);
    }

    for (int k = 0; k < nerr; k++) {
        int j = err_pos[k];
        int p = (254 - j) % 255;       /* polynomial position, but for j=254: (254-254)%255 = 0 ✓ */
        uint8_t X = gf_exp[p];          /* α^p = error locator */
        int inv_exp = (j + 1) % 255;
        uint8_t Xi = gf_exp[inv_exp];   /* X^{-1} = α^{(j+1) mod 255} */

        /* Ω(X^{-1}) */
        uint8_t omega_val = 0;
        uint8_t xp = 1;
        for (int i = 0; i < RS_2T; i++) {
            omega_val ^= gf_mul(omega[i], xp);
            xp = gf_mul(xp, Xi);
        }

        /* σ'(X^{-1}): formal derivative in GF(2), only odd-index terms survive */
        uint8_t sigma_deriv = 0;
        xp = 1;
        for (int i = 1; i <= L; i += 2) {
            sigma_deriv ^= gf_mul(sigma[i], xp);
            xp = gf_mul(xp, gf_mul(Xi, Xi));
        }

        if (sigma_deriv == 0)
            return -1;

        uint8_t err_val = gf_mul(gf_mul(X, omega_val), gf_inv(sigma_deriv));
        codeword[j] ^= err_val;
    }

    return nerr;
}

/* ========================================================================
 * High-level interleaved RS parity
 *
 * Data is partitioned into interleave groups. Each group is D=64 codewords
 * of RS_K=239 data bytes each (15296 bytes total). Data is interleaved
 * column-by-column: byte[i] goes to codeword (i % D), row (i / D).
 *
 * The last group may be shorter (padded with zeros for encoding).
 *
 * Parity blob: for each group, D × RS_2T = 1024 bytes of parity.
 * ======================================================================== */

#define RS_GROUP_DATA  ((size_t)RS_K * RS_INTERLEAVE)   /* 15296 */
#define RS_GROUP_PAR   ((size_t)RS_2T * RS_INTERLEAVE)  /* 1024  */

size_t rs_parity_size(size_t data_len)
{
    if (data_len == 0) return 0;
    size_t ngroups = (data_len + RS_GROUP_DATA - 1) / RS_GROUP_DATA;
    return ngroups * RS_GROUP_PAR;
}

#if defined(RS_SIMD) && defined(__AVX2__)
/* Encode 32 codewords simultaneously using AVX2 vpshufb GF multiplication.
 *
 * The interleaved data layout is naturally SIMD-friendly: for data position i,
 * bytes src[i*64 + 0..31] are position i of codewords 0-31 (contiguous 32-byte
 * AVX2 load).  The LFSR inner loop becomes:
 *
 *   feedback = data ^ reg[15]                       (32 bytes at once)
 *   gen*fb   = vpshufb(gen_lo, fb_lo) ^ vpshufb(gen_hi, fb_hi)  (GF multiply)
 *   reg[j]   = reg[j-1] ^ gen*fb[j]                (shift + XOR)
 *
 * This processes 32 codewords × 16 coefficients with 3 SIMD ops per coefficient
 * instead of 1 scalar GF multiply each — a ~32× throughput improvement over
 * calling rs_encode 32 times.
 */
static void rs_encode_batch32(const uint8_t *src, size_t group_len,
                              uint8_t *par, int batch_offset) {
    __m256i reg[RS_2T];
    for (int j = 0; j < RS_2T; j++)
        reg[j] = _mm256_setzero_si256();

    __m256i mask_lo = _mm256_set1_epi8(0x0F);

    for (size_t i = 0; i < (size_t)RS_K; i++) {
        /* Load 32 data bytes: position i for codewords in this batch.
         * Located at src[i*64 + batch_offset .. i*64 + batch_offset + 31].
         * Must iterate RS_K times (not just ceil(group_len/64)) because
         * the LFSR feedback is non-zero even when data bytes are zero. */
        __m256i data_v;
        size_t offset = i * (size_t)RS_INTERLEAVE + (size_t)batch_offset;
        if (offset + 32 <= group_len) {
            data_v = _mm256_loadu_si256((const __m256i *)(src + offset));
        } else {
            /* Partial or fully beyond group — zero-pad */
            uint8_t tmp[32];
            memset(tmp, 0, sizeof(tmp));
            size_t avail = (offset < group_len) ? group_len - offset : 0;
            if (avail > 32) avail = 32;
            if (avail > 0) memcpy(tmp, src + offset, avail);
            data_v = _mm256_loadu_si256((const __m256i *)tmp);
        }

        /* feedback[d] = data[d] ^ reg[15][d] for d=0..31 */
        __m256i feedback = _mm256_xor_si256(data_v, reg[RS_2T - 1]);

        /* Split feedback into nibbles for vpshufb GF multiply */
        __m256i fb_lo = _mm256_and_si256(feedback, mask_lo);
        __m256i fb_hi = _mm256_and_si256(_mm256_srli_epi16(feedback, 4), mask_lo);

        /* LFSR shift + multiply for all 16 parity positions */
        for (int j = RS_2T - 1; j > 0; j--) {
            __m256i mul = _mm256_xor_si256(
                _mm256_shuffle_epi8(rs_gen_lo[j], fb_lo),
                _mm256_shuffle_epi8(rs_gen_hi[j], fb_hi));
            reg[j] = _mm256_xor_si256(reg[j - 1], mul);
        }
        __m256i mul0 = _mm256_xor_si256(
            _mm256_shuffle_epi8(rs_gen_lo[0], fb_lo),
            _mm256_shuffle_epi8(rs_gen_hi[0], fb_hi));
        reg[0] = mul0;
    }

    /* Store parity: need reg[j] byte d → par[d * RS_2T + (15-j)].
     * Reverse the register order, then scatter bytes to output. */
    for (int d = 0; d < 32; d++) {
        uint8_t out[RS_2T];
        for (int j = 0; j < RS_2T; j++) {
            /* Extract byte d from reg[15-j] */
            uint8_t tmp[32];
            _mm256_storeu_si256((__m256i *)tmp, reg[RS_2T - 1 - j]);
            out[j] = tmp[d];
        }
        int cw = batch_offset + d;
        memcpy(par + cw * RS_2T, out, RS_2T);
    }
}
#endif

void rs_parity_encode(const void *data, size_t len, void *parity_out)
{
    rs_init();

    const uint8_t *src = (const uint8_t *)data;
    uint8_t *par = (uint8_t *)parity_out;
    size_t remaining = len;

    while (remaining > 0) {
        size_t group_len = remaining < RS_GROUP_DATA ? remaining : RS_GROUP_DATA;

#if defined(RS_SIMD) && defined(__AVX2__)
        /* AVX2 batch path: process 32 codewords at a time.
         * Two batches cover all 64 codewords in the interleave group. */
        size_t active_cw = group_len < (size_t)RS_INTERLEAVE
                           ? group_len : (size_t)RS_INTERLEAVE;

        /* Batch 1: codewords 0-31 */
        if (active_cw > 0)
            rs_encode_batch32(src, group_len, par, 0);
        /* Zero out parity for inactive codewords in batch 1 */
        for (size_t d = active_cw; d < 32 && d < (size_t)RS_INTERLEAVE; d++)
            memset(par + d * RS_2T, 0, RS_2T);

        /* Batch 2: codewords 32-63 */
        if (active_cw > 32)
            rs_encode_batch32(src, group_len, par, 32);
        else {
            /* All codewords 32-63 are inactive — zero their parity */
            for (size_t d = 32; d < (size_t)RS_INTERLEAVE; d++)
                memset(par + d * RS_2T, 0, RS_2T);
        }
#else
        /* De-interleave into D codewords. */
        uint8_t cw_data[RS_INTERLEAVE][RS_K];
        memset(cw_data, 0, sizeof(cw_data));
        for (size_t i = 0; i < group_len; i++)
            cw_data[i % RS_INTERLEAVE][i / RS_INTERLEAVE] = src[i];

        size_t active_cw = group_len < (size_t)RS_INTERLEAVE
                           ? group_len : (size_t)RS_INTERLEAVE;

        for (size_t d = 0; d < (size_t)RS_INTERLEAVE; d++) {
            if (d < active_cw) {
                rs_encode(cw_data[d], par + d * RS_2T);
            } else {
                memset(par + d * RS_2T, 0, RS_2T);
            }
        }
#endif

        src += group_len;
        par += RS_GROUP_PAR;
        remaining -= group_len;
    }
}

int rs_parity_decode(void *data, size_t len, const void *parity)
{
    rs_init();

    uint8_t *dst = (uint8_t *)data;
    const uint8_t *par = (const uint8_t *)parity;
    size_t remaining = len;
    int total_corrected = 0;

    while (remaining > 0) {
        size_t group_len = remaining < RS_GROUP_DATA ? remaining : RS_GROUP_DATA;

        /* De-interleave into D codewords (data + parity). */
        uint8_t cw[RS_INTERLEAVE][RS_N];
        memset(cw, 0, sizeof(cw));

        /* Fill data portion. */
        for (size_t i = 0; i < group_len; i++)
            cw[i % RS_INTERLEAVE][i / RS_INTERLEAVE] = dst[i];

        /* Fill parity portion. */
        for (size_t d = 0; d < (size_t)RS_INTERLEAVE; d++)
            memcpy(&cw[d][RS_K], par + d * RS_2T, RS_2T);

        /* Determine active codewords. */
        size_t active_cw = group_len < (size_t)RS_INTERLEAVE
                           ? group_len : (size_t)RS_INTERLEAVE;

        /* Decode each codeword. */
        for (size_t d = 0; d < active_cw; d++) {
            int rc = rs_decode(cw[d]);
            if (rc < 0)
                return -1;
            total_corrected += rc;
        }

        /* Re-interleave corrected data back. */
        if (total_corrected > 0 || 1) {
            /* Always write back — simpler and handles any corrections. */
            for (size_t i = 0; i < group_len; i++)
                dst[i] = cw[i % RS_INTERLEAVE][i / RS_INTERLEAVE];
        }

        dst += group_len;
        par += RS_GROUP_PAR;
        remaining -= group_len;
    }

    return total_corrected;
}
